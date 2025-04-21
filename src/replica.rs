use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam::{channel::tick, select};
use thiserror::Error;

use crate::{
    Message,
    network::{
        CommitMessage, PrepareMessage, PrepareOkMessage, ReplicaNetwork, ReplyMessage,
        RequestMessage,
    },
};

pub trait Service {
    type Input;
    type Output;

    fn execute(&self, input: &Self::Input) -> Self::Output;
}

const PERIODIC_INTERVAL: Duration = Duration::from_millis(20);
const COMMIT_TIMEOUT_MS: Duration = Duration::from_millis(200);

/// A single replica
pub struct Replica<S, I, O> {
    /// The current replica number given the configuration
    replica_number: usize,

    /// Internal view number of the replica
    pub(crate) view: usize,

    /// The total amount of replicas
    total: usize,

    /// Internal replica state to understand availability
    status: ReplicaStatus,

    /// Most recently received request, initially 0
    operation_number: usize,

    /// Acknowledgements table with the operations that have been acknowledged by
    /// a given amount of replicas.
    /// Key is the operation number, value is the replica numbers that acknowledged
    /// this operation.
    acks: HashMap<usize, Ack>,

    /// Log entries of size "operation_number" containing the requests
    /// that have been received so far in their assigned order.
    log: Log<I>,

    /// The operation number of the most recently committed operation
    commit_number: usize,

    /// The timestamp of the last committed operation.
    committed_at: Instant,

    /// For each client, the number of its most recent request, plus,
    /// if the request has been executed, the result sent for that request.
    client_table: HashMap<usize, ClientTableEntry<O>>,

    /// Service container used by the replica to execute client requests.
    service: Arc<S>,

    /// Network of the replica containing connections to all the replicas
    /// and the client
    pub(crate) network: ReplicaNetwork<I, O>,
}

impl<S, I, O> Replica<S, I, O>
where
    S: Service<Input = I, Output = O>,
    I: Clone + Send,
    O: Clone + Send,
{
    pub(crate) fn new(
        replica_number: usize,
        total: usize,
        service: S,
        network: ReplicaNetwork<I, O>,
    ) -> Self {
        Replica {
            replica_number,
            view: 0,
            total,
            status: ReplicaStatus::Normal,
            operation_number: 0,
            log: Log::new(),
            commit_number: 0,
            committed_at: Instant::now(),
            acks: HashMap::default(),
            client_table: HashMap::default(),
            network,
            service: Arc::new(service),
        }
    }

    fn handle_message(&mut self, message: Message<I>) -> Result<(), ReplicaError> {
        match message {
            Message::Request(request) => self.handle_request(request),
            Message::Prepare(prepare) => self.handle_prepare(prepare),
            Message::PrepareOk(prepare_ok) => self.handle_prepare_ok(prepare_ok),
            Message::Commit(commit) => self.handle_commit(commit),
            Message::StartViewChange(start_view_change) => todo!(),
            Message::DoViewChange(do_view_change) => todo!(),
            Message::StartView(start_view) => todo!(),
        }
    }

    fn handle_request(&mut self, request: RequestMessage<I>) -> Result<(), ReplicaError> {
        // Backup replicas ignore client request message.
        if !self.is_primary() {
            return Ok(());
        }

        // The replica is not ready to receive calls, the client must wait.
        if self.status != ReplicaStatus::Normal {
            return Err(ReplicaError::NotReady);
        }

        // If the request-number isn’t bigger than the information in the client table it drops the
        // request, but it will re-send the response if the request is the most recent one from this
        // client and it has already been executed.
        if let Some(most_recent_request) = self.client_table.get(&request.client_id) {
            if most_recent_request.request_number > request.request_number {
                return Ok(());
            }

            if most_recent_request.request_number == request.request_number {
                if let Some(result) = most_recent_request.response.as_ref() {
                    let message = ReplyMessage {
                        view: request.view,
                        request_number: most_recent_request.request_number,
                        result: result.clone(),
                    };
                    return self.network.client.send_out(message, request.client_id);
                }
            }
        }

        // Advance the operation number adds the request to the end of the log, and updates the
        // information for this client in the client-table to contain the new request number
        assert!(self.operation_number == self.log.size());

        self.operation_number += 1;
        self.log.append(
            request.operation.clone(),
            request.request_number,
            request.client_id,
        );
        self.client_table.insert(
            request.client_id,
            ClientTableEntry {
                request_number: request.request_number,
                response: None,
            },
        );

        // Broadcast message to the backup replicas
        self.network.broadcast(Message::Prepare(PrepareMessage {
            view: request.view,
            operation_number: self.operation_number,
            commit_number: self.commit_number,
            request,
        }))
    }

    fn handle_prepare(&mut self, prepare: PrepareMessage<I>) -> Result<(), ReplicaError> {
        // `Prepare` messages must only be received by backup replicas
        assert!(!self.is_primary());

        // Update the commit number from the primary committed in a previous request.
        self.commit_number = prepare.commit_number;
        self.committed_at = Instant::now();

        // The operation has already been processed by the replica. The message is duplicate and will be ignored.
        if self.operation_number + 1 > prepare.operation_number {
            return Ok(());
        }

        // Replica is missing some operations, we should initiate state transfer
        if self.operation_number + 1 < prepare.operation_number {
            // TODO: implement state transfer protocol
            return Ok(());
        }

        // Advance the operation number
        assert!(self.operation_number + 1 == prepare.operation_number);
        assert!(self.operation_number == self.log.size());

        self.operation_number = prepare.operation_number;

        // Add the request to the end of the log
        self.log.append(
            prepare.request.operation.clone(),
            prepare.request.request_number,
            prepare.request.client_id,
        );

        // Send a prepare ok message to the primary
        self.network.send(
            Message::PrepareOk(PrepareOkMessage {
                view: prepare.view,
                operation_number: prepare.operation_number,
                replica_number: self.replica_number,
                client_id: prepare.request.client_id,
            }),
            &self.view,
        )
    }

    fn handle_prepare_ok(&mut self, prepare_ok: PrepareOkMessage) -> Result<(), ReplicaError> {
        // `PrepareOk` messages must only be received by the primary
        assert!(self.is_primary());

        // The primary waits for at least a quorum amount of `PrepareOk` messages
        // from different backups
        let ack = self
            .acks
            .entry(prepare_ok.operation_number)
            .or_insert(Ack::Waiting {
                replicas: HashSet::with_capacity(self.total),
            });

        let ack_replicas = match ack {
            Ack::Waiting { replicas } => replicas,
            Ack::Executed => return Ok(()),
        };

        ack_replicas.insert(prepare_ok.replica_number);

        // Quorum not reached yet. Primary waits for more `PrepareOk` messages
        if ack_replicas.len() < quorum(self.total) {
            return Ok(());
        };

        // Execute the query
        let operation = self
            .log
            .get_operation(prepare_ok.operation_number)
            .expect("Operation not found in log");

        let result = self.service.execute(operation);

        // Increment commit_number
        self.commit_number += 1;
        self.committed_at = Instant::now();

        self.acks.insert(prepare_ok.operation_number, Ack::Executed);

        let request = self.client_table.get_mut(&prepare_ok.client_id)
            .expect("Client must be present in the client table since the request message has been already received");

        // Send reply message to the client
        self.network.client.send_out(
            ReplyMessage {
                view: prepare_ok.view,
                request_number: request.request_number,
                result: result.clone(),
            },
            prepare_ok.client_id,
        )?;

        // The primary also updates the client's entry in the client-table to contain the result
        request.response = Some(result);

        Ok(())
    }

    fn handle_commit(&mut self, commit: CommitMessage) -> Result<(), ReplicaError> {
        // Replica is not prepared to handle operations, the commit message is ignored.
        if self.status != ReplicaStatus::Normal {
            return Ok(());
        }

        // `Commit` messages must only be received by backup replicas
        assert!(!self.is_primary());
        assert!(self.view == commit.view);

        // If the replica has a higher or equal operation number, the operation
        // was already commited. We'll update our latest commit operation but nothing
        // to execute.
        if self.commit_number >= commit.operation_number {
            self.committed_at = Instant::now();
            return Ok(());
        }

        let has_log = self.log.get_operation(commit.operation_number).is_some();
        if !has_log {
            todo!("Implement state transfer")
        }

        // Execute earlier and current operation by checking the log after state
        // transfer was successful
        for operation_number in self.operation_number..=commit.operation_number {
            let Some(operation) = self.log.get_operation(operation_number) else {
                // Log state is incorrect. Intermediate operations are missing
                return Err(ReplicaError::InvalidState);
            };

            self.service.execute(operation);
        }

        self.commit_number = commit.operation_number;
        self.committed_at = Instant::now();

        Ok(())
    }

    fn periodic(&self) -> Result<(), ReplicaError> {
        // Primary periodic tasks
        if self.is_primary() {
            // If the last committed was sent
            if self.committed_at.elapsed() >= COMMIT_TIMEOUT_MS {
                self.send_commit(self.view, self.operation_number)?;
            }
        }

        Ok(())
    }

    fn send_commit(&self, view: usize, operation_number: usize) -> Result<(), ReplicaError> {
        // `Commit` messages must only be received by the primary
        assert!(self.replica_number == view);

        self.network.broadcast(Message::Commit(CommitMessage {
            view,
            operation_number,
        }))
    }

    fn is_primary(&self) -> bool {
        self.view == self.replica_number
    }

    pub fn run(mut self) {
        let ticker = tick(PERIODIC_INTERVAL);

        loop {
            select! {
                recv(self.network.client.incoming.1) -> message => match message {
                    Ok(message) => self.handle_message(Message::Request(message)).unwrap(),
                    Err(_) => panic!("something went wrong")
                },
                recv(self.network.incoming) -> message => match message {
                    Ok(message) => self.handle_message(message).unwrap(),
                    Err(_) => panic!("something went wrong")
                },
                recv(ticker) -> _ => self.periodic().unwrap()
            }
        }
    }
}

pub(crate) fn quorum(total: usize) -> usize {
    // total = 2 * f + 1
    // f = (total - 1) / 2
    (total - 1) / 2
}

#[derive(Debug, Clone)]
pub(crate) struct Log<I> {
    /// A queue of log entries for each request sent to the replica.
    entries: Vec<LogEntry<I>>,
}

impl<I> Log<I>
where
    I: Clone,
{
    fn new() -> Self {
        Log {
            entries: Vec::new(),
        }
    }

    fn append(&mut self, operation: I, request_number: usize, client_id: usize) {
        self.entries.push(LogEntry {
            request_number,
            client_id,
            operation,
        });
    }

    fn get_operation(&self, operation_number: usize) -> Option<&I> {
        // The operation number is a 1-based counter. Thus, we need to subtract 1 to get
        // the associated index
        self.entries
            .get(operation_number - 1)
            .map(|entry| &entry.operation)
    }

    fn size(&self) -> usize {
        self.entries.len()
    }
}

#[derive(Debug, Clone)]
struct LogEntry<T> {
    /// The request number of the log.
    request_number: usize,
    /// The client ID that triggered the request.
    client_id: usize,
    /// Operation of the request
    operation: T,
}

#[derive(Debug)]
struct ClientTableEntry<O> {
    request_number: usize,
    response: Option<O>,
}

#[derive(Debug, PartialEq)]
enum ReplicaStatus {
    Normal,
    ViewChange,
    Recovering,
}

enum Ack {
    Waiting { replicas: HashSet<usize> },
    Executed,
}

#[derive(Error, Debug)]
pub enum ReplicaError {
    #[error("A message could not be sent or received")]
    NetworkError,
    #[error("A replica is not ready to receive requests")]
    NotReady,
    #[error("A replica has an invalid state")]
    InvalidState,
}
