use std::collections::{HashMap, HashSet, VecDeque};

use thiserror::Error;

use crate::{
    Message,
    network::{
        ClientMessage, CommitMessage, PrepareMessage, PrepareOkMessage, ReplicaNetwork,
        ReplyMessage, RequestMessage,
    },
};

/// A single replica
pub(crate) struct Replica {
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

    /// Log entries of size "operation_number" containing the requests
    /// that have been received so far in their assigned order.
    log: Log,

    /// The operation number of the most recently committed operation
    commit_number: usize,

    /// Acknowledgements table with the operations that have been acknowledged by
    /// a given amount of replicas.
    /// Key is the operation number, value is the replica numbers that acknowledged
    /// this operation.
    acks: HashMap<usize, Ack>,

    /// For each client, the number of its most recent request, plus,
    /// if the request has been executed, the result sent for that request.
    client_table: HashMap<usize, ClientTableEntry>,

    pub(crate) network: ReplicaNetwork,
}

impl Replica {
    pub(crate) fn new(replica_number: usize, total: usize, network: ReplicaNetwork) -> Self {
        Replica {
            replica_number,
            view: 0,
            total,
            status: ReplicaStatus::Normal,
            operation_number: 0,
            log: Log::default(),
            commit_number: 0,
            acks: HashMap::default(),
            client_table: HashMap::default(),
            network,
        }
    }

    fn handle_message(&mut self, message: Message) -> Result<(), ReplicaError> {
        println!(
            "Received message {:?} in replica {}",
            message, self.replica_number
        );

        match message {
            Message::Request(request) => self.handle_request(request),
            Message::Prepare(prepare) => self.handle_prepare(prepare),
            Message::PrepareOk(prepare_ok) => self.handle_prepare_ok(prepare_ok),
            Message::Commit(commit) => self.handle_commit(commit),
            Message::Reply { .. } => {
                unreachable!("Reply messages should be sent to the client")
            }
        }
    }

    fn handle_request(&mut self, request: RequestMessage) -> Result<(), ReplicaError> {
        // Backup replicas ignore client request message.
        if self.replica_number != request.view {
            return Ok(());
        }

        // If the request-number isn’t bigger than the information in the client table it drops the
        // request, but it will re-send the response if the request is the most recent one from this
        // client and it has already been executed.
        if let Some(most_recent_request) = self.client_table.get(&request.client_id) {
            if most_recent_request.request_number > request.request_number {
                return Ok(());
            }

            if most_recent_request.request_number == request.request_number {
                if let Some(result) = most_recent_request.response {
                    let message = Message::Reply(ReplyMessage {
                        view: request.view,
                        request_number: most_recent_request.request_number,
                        result,
                    });
                    return self.network.client.send_out(message, request.client_id);
                }
            }
        }

        // Advance the operation number adds the request to the end of the log, and updates the
        // information for this client in the client-table to contain the new request number
        self.operation_number += 1;
        self.log.append(request.request_number);
        self.client_table.insert(
            request.client_id,
            ClientTableEntry {
                request_number: request.request_number,
                response: None,
            },
        );

        // Broadcast message to the backup replicas
        let message = Message::Prepare(PrepareMessage {
            view: request.view,
            operation_number: self.operation_number,
            commit_number: self.commit_number,
            request,
        });

        self.network.broadcast(&message)
    }

    fn handle_prepare(&mut self, prepare: PrepareMessage) -> Result<(), ReplicaError> {
        // `Prepare` messages must only be received by backup replicas
        assert!(self.replica_number != prepare.view);

        // Update the commit number from the primary committed in a previous request.
        self.commit_number = prepare.commit_number;

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
        self.operation_number = prepare.operation_number;

        // Add the request to the end of the log
        self.log.append(prepare.request.request_number);

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
        assert!(self.replica_number == prepare_ok.view);

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
        let result = self.execute();

        // Increment commit_number
        self.commit_number += 1;
        self.acks.insert(prepare_ok.operation_number, Ack::Executed);

        let request = self.client_table.get_mut(&prepare_ok.client_id)
            .expect("Client must be present in the client table since the request message has been already received");

        // Send reply message to the client
        self.network.client.send_out(
            Message::Reply(ReplyMessage {
                view: prepare_ok.view,
                request_number: request.request_number,
                result,
            }),
            prepare_ok.client_id,
        )?;

        // The primary also updates the client's entry in the client-table to contain the result
        request.response = Some(result);

        Ok(())
    }

    fn handle_commit(&mut self, message: CommitMessage) -> Result<(), ReplicaError> {
        self.commit_number = message.operation_number;

        Ok(())
    }

    fn execute(&self) -> () {
        // TODO: here we run the service logic and build response
        ()
    }

    fn send_commit(&self, view: usize, operation_number: usize) -> Result<(), ReplicaError> {
        // `Commit` messages must only be received by the primary
        assert!(self.replica_number == view);

        let message = Message::Commit(CommitMessage {
            view,
            operation_number,
        });

        self.network.broadcast(&message)
    }

    pub(crate) fn tick(&mut self) {
        while !self.network.client.incoming.1.is_empty() {
            let message = match self.network.client.incoming.1.recv().unwrap() {
                ClientMessage::Request(request) => Message::Request(request),
                ClientMessage::Response { .. } => {
                    unreachable!("Replica should not receive a client response message")
                }
            };

            self.handle_message(message).unwrap();
        }

        while !self.network.incoming.is_empty() {
            let message = self.network.incoming.recv().unwrap();
            self.handle_message(message).unwrap();
        }
    }
}

pub(crate) fn quorum(total: usize) -> usize {
    // total = 2 * f + 1
    // f = (total - 1) / 2
    (total - 1) / 2
}

#[derive(Debug, Default)]
struct Log {
    /// A queue of log entries for each request sent to the replica.
    entries: VecDeque<LogEntry>,
}

impl Log {
    fn new() -> Self {
        Log::default()
    }

    fn append(&mut self, request_number: usize) {
        let entry = LogEntry { request_number };
        self.entries.push_front(entry);
    }
}

#[derive(Debug)]
struct LogEntry {
    /// The request number of the log.
    request_number: usize,
    // The client ID that triggered the request.
    // client_id: usize,
}

#[derive(Debug)]
struct ClientTableEntry {
    request_number: usize,
    response: Option<()>,
}

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
}
