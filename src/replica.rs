use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam::{channel::tick, select};
use thiserror::Error;

use crate::{
    Message,
    network::{
        CommitMessage, DoViewChangeMessage, GetStateMessage, Network, NewStateMessage,
        PrepareMessage, PrepareOkMessage, RecoveryMessage, RecoveryPrimaryResponse,
        RecoveryResponseMessage, ReplicaNetwork, ReplyMessage, RequestMessage,
        StartViewChangeMessage, StartViewMessage,
    },
};

pub trait Service {
    type Input;
    type Output;

    fn execute(&self, input: &Self::Input) -> Self::Output;
}

const PERIODIC_INTERVAL: Duration = Duration::from_millis(20);
const COMMIT_TIMEOUT_MS: Duration = Duration::from_millis(200);
const IDLE_TIMEOUT_MS: Duration = Duration::from_millis(2000);

/// A single replica
pub struct Replica<S, N, I, O> {
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
    /// a given amount of replicas for a client request.
    /// Key is the operation number, value is the replica numbers that acknowledged
    /// this operation.
    prepare_acks: HashMap<usize, PrepareAck>,

    /// Heap with the view change acknowledgements sorted by latest view and
    /// operation number
    view_acks: BinaryHeap<ViewAck<I>>,

    recovery_acks: Vec<RecoveryAck<I>>,

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
    service: S,

    /// Network of the replica containing connections to all the replicas
    /// and the client
    pub(crate) network: N,
}

impl<S, I, N, O> Replica<S, N, I, O>
where
    S: Service<Input = I, Output = O>,
    N: Network<Input = I, Output = O>,
    I: Clone + Send,
    O: Clone + Send,
{
    pub(crate) fn new(replica_number: usize, total: usize, service: S, network: N) -> Self {
        Replica {
            replica_number,
            view: 0,
            total,
            status: ReplicaStatus::Normal,
            operation_number: 0,
            log: Log::new(),
            commit_number: 0,
            committed_at: Instant::now(),
            prepare_acks: HashMap::default(),
            view_acks: BinaryHeap::new(),
            recovery_acks: Vec::new(),
            client_table: HashMap::default(),
            network,
            service,
        }
    }

    fn handle_message(&mut self, message: Message<I>) -> Result<(), ReplicaError> {
        match message {
            Message::Request(request) => self.handle_request(request),
            Message::Prepare(prepare) => self.handle_prepare(prepare),
            Message::PrepareOk(prepare_ok) => self.handle_prepare_ok(prepare_ok),
            Message::Commit(commit) => self.handle_commit(commit),
            Message::GetState(get_state) => self.handle_get_state(get_state),
            Message::NewState(new_state) => self.handle_new_state(new_state),
            Message::StartViewChange(start_view_change) => {
                self.handle_start_view_change(start_view_change)
            }
            Message::DoViewChange(do_view_change) => self.handle_do_view_change(do_view_change),
            Message::StartView(start_view) => self.handle_start_view(start_view),
            Message::Recovery(recovery) => self.handle_recovery(recovery),
            Message::RecoveryResponse(recovery_response) => {
                self.handle_recovery_response(recovery_response)
            }
        }
    }

    fn handle_request(&mut self, request: RequestMessage<I>) -> Result<(), ReplicaError> {
        // Backup replicas ignore client request message.
        if !self.is_primary() {
            return Ok(());
        }

        // The replica and message should message
        if self.view != request.view {
            return Err(ReplicaError::MessageViewMismatch);
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
                    return self.network.send_client(message, request.client_id);
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

        // The replica and message should message
        if self.view != prepare.view {
            return Err(ReplicaError::MessageViewMismatch);
        }

        // The operation has already been processed by the replica. The message is duplicate and will be ignored.
        if self.operation_number + 1 > prepare.operation_number {
            return Ok(());
        }

        // Replica is missing some operations, we should initiate state transfer
        if self.operation_number + 1 < prepare.operation_number {
            return self.trigger_state_transfer();
        }

        // Advance the operation number, update the log and the client table
        assert!(self.operation_number + 1 == prepare.operation_number);
        assert!(self.operation_number == self.log.size());

        self.operation_number = prepare.operation_number;
        self.log.append(
            prepare.request.operation.clone(),
            prepare.request.request_number,
            prepare.request.client_id,
        );
        self.client_table.insert(
            prepare.request.client_id,
            ClientTableEntry {
                request_number: prepare.request.request_number,
                response: None,
            },
        );

        // Execute previous committed operations by checking the log
        for commit_number in (self.commit_number + 1)..=prepare.commit_number {
            let Some(operation) = self.log.get_operation(commit_number) else {
                // Log state is incorrect. Intermediate operations are missing
                return Err(ReplicaError::InvalidState);
            };

            self.service.execute(operation);
        }
        self.commit_number = prepare.commit_number;
        self.committed_at = Instant::now();

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

        // The replica and message should message
        if self.view != prepare_ok.view {
            return Err(ReplicaError::MessageViewMismatch);
        }

        // The primary waits for at least a quorum amount of `PrepareOk` messages
        // from different backups
        let ack = self
            .prepare_acks
            .entry(prepare_ok.operation_number)
            .or_insert(PrepareAck::Waiting {
                replicas: HashSet::with_capacity(self.total),
            });

        let ack_replicas = match ack {
            PrepareAck::Waiting { replicas } => replicas,
            PrepareAck::Executed => return Ok(()),
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

        self.prepare_acks
            .insert(prepare_ok.operation_number, PrepareAck::Executed);

        let client_request = self.client_table.get_mut(&prepare_ok.client_id)
            .expect("Client must be present in the client table since the request message has been already received");

        // Send reply message to the client
        self.network.send_client(
            ReplyMessage {
                view: prepare_ok.view,
                request_number: client_request.request_number,
                result: result.clone(),
            },
            prepare_ok.client_id,
        )?;

        // The primary also updates the client's entry in the client-table to contain the result
        client_request.response = Some(result);

        Ok(())
    }

    fn handle_commit(&mut self, commit: CommitMessage) -> Result<(), ReplicaError> {
        // Replica is not prepared to handle operations, the commit message is ignored.
        if self.status != ReplicaStatus::Normal {
            return Ok(());
        }

        // `Commit` messages must only be received by backup replicas
        assert!(!self.is_primary());

        // The replica and message should message
        if self.view != commit.view {
            return Err(ReplicaError::MessageViewMismatch);
        }

        // If the replica has a higher or equal operation number, the operation
        // was already commited. We'll update our latest commit operation but nothing
        // to execute.
        if self.commit_number >= commit.operation_number {
            self.committed_at = Instant::now();
            return Ok(());
        }

        let has_log = self.log.get_operation(commit.operation_number).is_some();
        if !has_log {
            return self.trigger_state_transfer();
        }

        // Execute uncommitted operations by checking the log
        for commit_number in (self.commit_number + 1)..=commit.operation_number {
            let Some(operation) = self.log.get_operation(commit_number) else {
                // Log state is incorrect. Intermediate operations are missing
                return Err(ReplicaError::InvalidState);
            };

            self.service.execute(operation);
        }

        self.commit_number = commit.operation_number;
        self.committed_at = Instant::now();

        Ok(())
    }

    fn trigger_state_transfer(&self) -> Result<(), ReplicaError> {
        let message = Message::GetState(GetStateMessage {
            view: self.view,
            operation_number: self.operation_number,
            replica_number: self.replica_number,
        });

        // Send message to the next replica in the circle
        let next_replica = self.next_replica(self.replica_number);
        self.network.send(message, &next_replica)
    }

    fn handle_get_state(&mut self, get_state: GetStateMessage) -> Result<(), ReplicaError> {
        assert!(self.status == ReplicaStatus::Normal);
        assert!(self.view == get_state.view);

        let log_after_operation = self.log.since(get_state.operation_number);

        let message = Message::NewState(NewStateMessage {
            view: self.view,
            log_after_operation,
            operation_number: self.operation_number,
            commit_number: self.commit_number,
        });

        self.network.send(message, &get_state.replica_number)
    }

    fn handle_new_state(&mut self, new_state: NewStateMessage<I>) -> Result<(), ReplicaError> {
        assert!(self.view == new_state.view);

        self.log.merge(new_state.log_after_operation);
        self.operation_number = new_state.operation_number;

        // Execute previous committed operations by checking the log
        for commit_number in (self.commit_number + 1)..=new_state.commit_number {
            let Some(operation) = self.log.get_operation(commit_number) else {
                // Log state is incorrect. Intermediate operations are missing
                return Err(ReplicaError::InvalidState);
            };

            self.service.execute(operation);
        }
        self.commit_number = new_state.commit_number;
        self.committed_at = Instant::now();

        Ok(())
    }

    fn handle_start_view_change(
        &mut self,
        start_view_change: StartViewChangeMessage,
    ) -> Result<(), ReplicaError> {
        let old_view = self.view;

        // New backup replica acknowledges a view change and waits for the new primary
        // to send a `StartView` message
        if self.view != start_view_change.new_view {
            self.view = start_view_change.new_view;
            self.status = ReplicaStatus::ViewChange;
        }

        // The current primary won't send a `DoViewChange` message to itself.
        // It will acknowledge the view change and wait for `DoViewChange` messages.
        if self.is_primary() {
            self.view_acks.push(ViewAck {
                old_view,
                operation_number: self.operation_number,
                commit_number: self.commit_number,
                log: self.log.clone(),
            });
            return Ok(());
        }

        // Send a `DoViewChange` to the new primary replica to mark the current replica
        // ready for a view change.
        let message = Message::DoViewChange(DoViewChangeMessage {
            old_view,
            new_view: self.view,
            log: self.log.clone(),
            operation_number: self.operation_number,
            commit_number: self.commit_number,
            replica_number: self.replica_number,
        });

        self.network.send(message, &self.view)
    }

    fn handle_do_view_change(
        &mut self,
        do_view_change: DoViewChangeMessage<I>,
    ) -> Result<(), ReplicaError> {
        // When the primary received `quorum + 1` `DoViewChange` messages from different replicas
        // (including itself), it sets its view number to that in the messages as selects as the
        // new log the one contained in the message with the largest view.
        self.view_acks.push(ViewAck {
            old_view: do_view_change.old_view,
            operation_number: do_view_change.operation_number,
            commit_number: do_view_change.commit_number,
            log: do_view_change.log,
        });

        if self.view_acks.len() < quorum(self.total) {
            return Ok(());
        }

        if self.view != do_view_change.new_view {
            self.view = do_view_change.new_view;
            self.status = ReplicaStatus::ViewChange;
        }

        let new_view = self.view_acks.drain().next().expect(
            "View acknowledgements are more than the quorum. At least 1 entry must be present.",
        );

        self.status = ReplicaStatus::Normal;

        self.log = new_view.log;
        self.operation_number = new_view.operation_number;

        // In case any uncommitted operations are present
        if self.operation_number > self.commit_number {
            let mut executed_requests = HashMap::new();

            // Iterate over all the uncommitted operations in the log in order
            for operation_number in new_view.commit_number..=self.operation_number {
                let log_entry = self.log.get_entry(operation_number).expect(
                    "Log entry must be present if the operation number is higher than the log size",
                );

                // Execute the operation and send reply to the client
                let result = self.service.execute(&log_entry.operation);

                self.network.send_client(
                    ReplyMessage {
                        view: self.view,
                        request_number: log_entry.request_number,
                        result: result.clone(),
                    },
                    log_entry.client_id,
                )?;

                executed_requests.insert(
                    log_entry.client_id,
                    ClientTableEntry {
                        request_number: log_entry.request_number,
                        response: Some(result),
                    },
                );
            }

            // Update client table with the executed requests
            self.client_table.extend(executed_requests);
        }

        self.commit_number = new_view.commit_number;
        self.committed_at = Instant::now();

        self.view_acks.clear();

        let message = Message::StartView(StartViewMessage {
            view: self.view,
            log: self.log.clone(),
            operation_number: self.operation_number,
            commit_number: self.commit_number,
        });

        self.network.broadcast(message)?;

        Ok(())
    }

    fn handle_start_view(&mut self, start_view: StartViewMessage<I>) -> Result<(), ReplicaError> {
        // Refresh internal state with the new view
        self.status = ReplicaStatus::Normal;
        self.view = start_view.view;
        self.log = start_view.log;
        self.operation_number = start_view.operation_number;

        // Iterate over all the uncommitted operations in the log in order
        if self.operation_number > self.commit_number {
            let mut executed_requests = HashMap::new();
            for operation_number in start_view.commit_number..=self.operation_number {
                let log_entry = self.log.get_entry(operation_number).expect(
                    "Log entry must be present if the operation number is higher than the log size",
                );

                // Execute the operation and send a `PrepareOk` to the new primary
                let result = self.service.execute(&log_entry.operation);

                self.network.send(
                    Message::PrepareOk(PrepareOkMessage {
                        view: self.view,
                        operation_number,
                        replica_number: self.replica_number,
                        client_id: log_entry.client_id,
                    }),
                    &self.view,
                )?;

                executed_requests.insert(
                    log_entry.client_id,
                    ClientTableEntry {
                        request_number: log_entry.request_number,
                        response: Some(result),
                    },
                );
            }

            // Update client table with the executed requests
            self.client_table.extend(executed_requests);
        }

        self.commit_number = start_view.commit_number;
        self.committed_at = Instant::now();

        Ok(())
    }

    fn handle_recovery(&mut self, recovery: RecoveryMessage) -> Result<(), ReplicaError> {
        // A replica won't respond to a recovery if it's not operational
        assert!(self.status == ReplicaStatus::Normal);

        let response = if self.is_primary() {
            RecoveryResponseMessage {
                view: self.view,
                nonce: recovery.nonce,
                primary: Some(RecoveryPrimaryResponse {
                    log: self.log.clone(),
                    operation_number: self.operation_number,
                    commit_number: self.commit_number,
                }),
            }
        } else {
            RecoveryResponseMessage {
                view: self.view,
                nonce: recovery.nonce,
                primary: None,
            }
        };

        self.network.send(
            Message::RecoveryResponse(response),
            &recovery.replica_number,
        )
    }

    fn handle_recovery_response(
        &mut self,
        recovery_response: RecoveryResponseMessage<I>,
    ) -> Result<(), ReplicaError> {
        match self.status {
            ReplicaStatus::Recovering { nonce } => assert!(nonce == recovery_response.nonce),
            _ => panic!("Replica must be in recovering status to handle recovery responses"),
        }

        // Attach recovery acknowledgements and check if quorum is reached
        self.recovery_acks
            .push(RecoveryAck::from_message(recovery_response));

        if self.recovery_acks.len() < (quorum(self.total) + 1) {
            return Ok(());
        };

        // Read the primary acknowledgements of the latest view. If no acknowledgement from the
        // primary replicas has been received, the recovering replica must wait.
        let mut ready_ack: Option<&RecoveryPrimaryAck<I>> = None;
        for recovery_ack in &self.recovery_acks {
            if let Some(primary_ack) = recovery_ack.primary.as_ref() {
                if let Some(current) = ready_ack {
                    if current.view > primary_ack.view {
                        ready_ack = Some(current);
                    } else {
                        ready_ack = Some(primary_ack);
                    }
                } else {
                    ready_ack = Some(primary_ack);
                }
            }
        }

        // The recovering replica must wait for the primary to acknowledge the recovery
        let Some(RecoveryPrimaryAck {
            log,
            operation_number,
            commit_number,
            view,
        }) = ready_ack
        else {
            return Ok(());
        };

        self.view = *view;
        self.operation_number = *operation_number;
        self.log = log.clone();

        // Execute previous committed operations by checking the log
        let recovery_commit_number = *commit_number;
        let mut executed_requests = HashMap::new();
        for commit_number in (self.commit_number + 1)..=recovery_commit_number {
            let Some(log_entry) = self.log.get_entry(commit_number) else {
                // Log state is incorrect. Intermediate operations are missing
                return Err(ReplicaError::InvalidState);
            };

            let result = self.service.execute(&log_entry.operation);

            executed_requests.insert(
                log_entry.client_id,
                ClientTableEntry {
                    request_number: log_entry.request_number,
                    response: Some(result),
                },
            );
        }

        // Refresh commit and client table
        self.commit_number = recovery_commit_number;
        self.committed_at = Instant::now();
        self.client_table.extend(executed_requests);

        // Replica is available for handling messages
        self.status = ReplicaStatus::Normal;

        // Clear acks
        self.recovery_acks.clear();

        Ok(())
    }

    fn periodic(&mut self) -> Result<(), ReplicaError> {
        // Primary periodic tasks
        if self.is_primary() {
            // If the primary is idle (no requests sent)
            if self.committed_at.elapsed() >= COMMIT_TIMEOUT_MS {
                self.trigger_commit(self.view, self.operation_number)?;
            }
        } else {
            // If the backup is idle (no requests received)
            if self.committed_at.elapsed() >= IDLE_TIMEOUT_MS {
                self.trigger_view_change()?;
            }
        }

        Ok(())
    }

    fn trigger_commit(&self, view: usize, operation_number: usize) -> Result<(), ReplicaError> {
        // `Commit` messages must only be received by the primary
        assert!(self.replica_number == view);

        self.network.broadcast(Message::Commit(CommitMessage {
            view,
            operation_number,
        }))
    }

    fn trigger_view_change(&mut self) -> Result<(), ReplicaError> {
        // If the replica is not running a view change already, or it's in recovery
        // we won't start a new change.
        if self.status != ReplicaStatus::Normal {
            return Ok(());
        }

        self.view = self.next_replica(self.view);
        self.status = ReplicaStatus::ViewChange;

        let message = Message::StartViewChange(StartViewChangeMessage {
            new_view: self.view,
            replica_number: self.replica_number,
        });

        self.network.broadcast(message)
    }

    fn trigger_recovery(&mut self) -> Result<(), ReplicaError> {
        let nonce = nonce();
        self.status = ReplicaStatus::Recovering { nonce };

        let message = Message::Recovery(RecoveryMessage {
            replica_number: self.replica_number,
            nonce,
        });

        self.network.broadcast(message)
    }

    fn is_primary(&self) -> bool {
        (self.view % self.total) == self.replica_number
    }

    fn next_replica(&self, replica_number: usize) -> usize {
        replica_number + 1
    }
}

impl<S, I, O> Replica<S, ReplicaNetwork<I, O>, I, O>
where
    S: Service<Input = I, Output = O>,
    I: Clone + Send,
    O: Clone + Send,
{
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

/// Create a nonce ("number used once") using the current system time
/// and the UNIX epoch time.
fn nonce() -> u64 {
    let now = SystemTime::now();
    now.duration_since(UNIX_EPOCH).unwrap().as_secs()
}

#[derive(Debug, Clone, PartialEq)]
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
        self.get_entry(operation_number)
            .map(|entry| &entry.operation)
    }

    fn get_entry(&self, operation_number: usize) -> Option<&LogEntry<I>> {
        // The operation number is a 1-based counter. Thus, we need to subtract 1 to get
        // the associated index
        self.entries.get(operation_number - 1)
    }

    fn since(&self, operation_number: usize) -> Log<I> {
        if operation_number >= self.entries.len() {
            return Log::new();
        }

        let mut entries = Vec::new();
        for i in operation_number..self.size() {
            let entry = self
                .entries
                .get(i)
                .expect("Log entry must be inside boundaries");

            entries.push(entry.clone());
        }

        Log { entries }
    }

    fn merge(&mut self, log: Log<I>) {
        for entry in log.entries {
            self.entries.push(entry)
        }
    }

    fn size(&self) -> usize {
        self.entries.len()
    }
}

#[derive(Debug, Clone, PartialEq)]
struct LogEntry<T> {
    /// The request number of the log.
    request_number: usize,
    /// The client ID that triggered the request.
    client_id: usize,
    /// Operation of the request
    operation: T,
}

#[derive(Debug, PartialEq)]
struct ClientTableEntry<O> {
    request_number: usize,
    response: Option<O>,
}

#[derive(Debug, PartialEq)]
enum ReplicaStatus {
    Normal,
    ViewChange,
    Recovering { nonce: u64 },
}

#[derive(Debug, PartialEq)]
enum PrepareAck {
    Waiting { replicas: HashSet<usize> },
    Executed,
}

#[derive(Debug)]
struct ViewAck<I> {
    old_view: usize,
    operation_number: usize,
    commit_number: usize,
    log: Log<I>,
}

impl<I> PartialEq for ViewAck<I> {
    fn eq(&self, other: &Self) -> bool {
        self.old_view == other.old_view && self.operation_number == other.operation_number
    }
}

impl<I> Eq for ViewAck<I> {}

impl<I> PartialOrd for ViewAck<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> Ord for ViewAck<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.old_view.cmp(&other.old_view).then_with(|| {
            self.operation_number
                .cmp(&other.operation_number)
                .then_with(|| self.commit_number.cmp(&other.commit_number))
        })
    }
}

#[derive(Debug, PartialEq)]
struct RecoveryAck<I> {
    primary: Option<RecoveryPrimaryAck<I>>,
    view: usize,
}

impl<I> RecoveryAck<I> {
    fn from_message(message: RecoveryResponseMessage<I>) -> Self {
        let primary = message.primary.map(|response| RecoveryPrimaryAck {
            log: response.log,
            operation_number: response.operation_number,
            commit_number: response.commit_number,
            view: message.view,
        });

        RecoveryAck {
            primary,
            view: message.view,
        }
    }
}

#[derive(Debug, PartialEq)]
struct RecoveryPrimaryAck<I> {
    log: Log<I>,
    operation_number: usize,
    commit_number: usize,
    view: usize,
}

#[derive(Error, Debug, PartialEq)]
pub enum ReplicaError {
    #[error("A message could not be sent or received")]
    NetworkError,
    #[error("A replica is not ready to receive requests")]
    NotReady,
    #[error("A replica has an invalid state")]
    InvalidState,
    #[error("View mismatch between client and replica")]
    MessageViewMismatch,
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        collections::{BinaryHeap, HashMap},
        time::Instant,
    };

    use crate::{
        network::{
            CommitMessage, DoViewChangeMessage, GetStateMessage, Message, Network, NewStateMessage,
            PrepareMessage, PrepareOkMessage, RecoveryMessage, RecoveryPrimaryResponse,
            RecoveryResponseMessage, ReplyMessage, RequestMessage, StartViewChangeMessage,
            StartViewMessage,
        },
        replica::{
            ClientTableEntry, IDLE_TIMEOUT_MS, Log, LogEntry, PrepareAck, RecoveryAck,
            RecoveryPrimaryAck, ReplicaError, ReplicaStatus, ViewAck,
        },
    };

    use super::{Replica, Service};

    struct MockNetwork {
        incoming: RefCell<HashMap<usize, Message<usize>>>,
        outgoing: RefCell<HashMap<usize, ReplyMessage<usize>>>,
        connected_replicas: Vec<usize>,
    }

    impl MockNetwork {
        fn new(connected_replicas: Vec<usize>) -> Self {
            MockNetwork {
                incoming: RefCell::new(HashMap::new()),
                outgoing: RefCell::new(HashMap::new()),
                connected_replicas,
            }
        }
    }

    impl Network for MockNetwork {
        type Input = usize;
        type Output = usize;

        fn send(
            &self,
            message: Message<Self::Input>,
            replica: &usize,
        ) -> Result<(), super::ReplicaError> {
            let mut incoming = self.incoming.borrow_mut();
            incoming.insert(*replica, message);
            Ok(())
        }

        fn broadcast(&self, message: Message<Self::Input>) -> Result<(), super::ReplicaError> {
            let mut incoming = self.incoming.borrow_mut();
            for replica in &self.connected_replicas {
                incoming.insert(*replica, message.clone());
            }
            Ok(())
        }

        fn send_client(
            &self,
            message: ReplyMessage<Self::Output>,
            client_id: usize,
        ) -> Result<(), super::ReplicaError> {
            let mut outgoing = self.outgoing.borrow_mut();
            outgoing.insert(client_id, message);

            Ok(())
        }
    }

    struct MockService;

    impl Service for MockService {
        type Input = usize;
        type Output = usize;

        fn execute(&self, input: &Self::Input) -> Self::Output {
            input + 1
        }
    }

    fn create_primary(total: usize) -> Replica<MockService, MockNetwork, usize, usize> {
        create_replica(0, total)
    }

    fn create_backup(total: usize) -> Replica<MockService, MockNetwork, usize, usize> {
        create_replica(1, total)
    }

    fn create_replica(
        replica_number: usize,
        total: usize,
    ) -> Replica<MockService, MockNetwork, usize, usize> {
        assert!(total > 0);

        let connected = (0..total)
            .filter(|i| i != &replica_number)
            .collect::<Vec<usize>>();

        let service = MockService;
        let network = MockNetwork::new(connected);

        Replica::new(replica_number, total, service, network)
    }

    #[test]
    fn handles_request() {
        // given
        let request = Message::Request(RequestMessage {
            view: 0,
            request_number: 1,
            client_id: 1,
            operation: 0,
        });
        let mut replica = create_primary(3);

        // when
        let result = replica.handle_message(request.clone());

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.replica_number, 0);
        assert_eq!(replica.view, 0);
        assert_eq!(replica.total, 3);
        assert_eq!(replica.operation_number, 1);
        assert_eq!(replica.commit_number, 0);

        // log is correct
        let mut log = Log::new();
        log.append(0, 1, 1);
        assert_eq!(replica.log, log);

        // client table is correct
        let mut client_table = HashMap::new();
        client_table.insert(
            1,
            ClientTableEntry {
                request_number: 1,
                response: None,
            },
        );
        assert_eq!(replica.client_table, client_table);

        // prepare messages are sent
        let prepare = Message::Prepare(PrepareMessage {
            view: 0,
            operation_number: 1,
            commit_number: 0,
            request: RequestMessage {
                view: 0,
                request_number: 1,
                client_id: 1,
                operation: 0,
            },
        });
        assert_eq!(
            *replica.network.incoming.borrow(),
            HashMap::from_iter([(1, prepare.clone()), (2, prepare.clone())])
        );
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_request_returns_most_recent_response() {
        // given
        let request = Message::Request(RequestMessage {
            view: 0,
            request_number: 1,
            client_id: 1,
            operation: 0,
        });
        let mut replica = create_primary(3);
        replica.client_table = HashMap::from_iter([(
            1,
            ClientTableEntry {
                request_number: 1,
                response: Some(1),
            },
        )]);

        // when
        let result = replica.handle_message(request.clone());

        // then
        assert_eq!(result, Ok(()));
        assert_eq!(
            *replica.network.outgoing.borrow(),
            HashMap::from_iter([(
                1,
                ReplyMessage {
                    view: 0,
                    request_number: 1,
                    result: 1
                }
            )])
        );
    }

    #[test]
    fn handles_request_backup_replica_ignores() {
        // given
        let request = Message::Request(RequestMessage {
            view: 1,
            request_number: 1,
            client_id: 1,
            operation: 0,
        });
        let mut replica = create_backup(3);

        // when
        let result = replica.handle_message(request);

        // then
        assert_eq!(result, Ok(()));
        assert_eq!(*replica.network.incoming.borrow(), HashMap::new());
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_request_invalid_view_sent_by_client() {
        // given
        let request = Message::Request(RequestMessage {
            view: 1,
            request_number: 1,
            client_id: 1,
            operation: 0,
        });
        let mut replica = create_primary(3);

        // when
        let result = replica.handle_message(request.clone());

        // then
        assert_eq!(result, Err(ReplicaError::MessageViewMismatch));
    }

    #[test]
    fn handles_prepare() {
        // given
        let prepare = Message::Prepare(PrepareMessage {
            view: 0,
            operation_number: 1,
            commit_number: 0,
            request: RequestMessage {
                view: 0,
                request_number: 1,
                client_id: 1,
                operation: 0,
            },
        });
        let mut replica = create_backup(3);

        // when
        let result = replica.handle_message(prepare);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.operation_number, 1);
        assert_eq!(replica.commit_number, 0);

        // log is correct
        let mut log = Log::new();
        log.append(0, 1, 1);
        assert_eq!(replica.log, log);

        // client table is correct
        let mut client_table = HashMap::new();
        client_table.insert(
            1,
            ClientTableEntry {
                request_number: 1,
                response: None,
            },
        );
        assert_eq!(replica.client_table, client_table);

        // prepare_ok messages are sent
        let prepare_ok = Message::PrepareOk(PrepareOkMessage {
            view: 0,
            operation_number: 1,
            replica_number: 1,
            client_id: 1,
        });
        assert_eq!(
            *replica.network.incoming.borrow(),
            HashMap::from_iter([(0, prepare_ok)])
        );
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_prepare_triggers_state_transfer() {
        // given
        let prepare = Message::Prepare(PrepareMessage {
            view: 0,
            operation_number: 2,
            commit_number: 1,
            request: RequestMessage {
                view: 0,
                request_number: 2,
                client_id: 1,
                operation: 1,
            },
        });
        let mut replica = create_backup(3);
        replica.log.append(0, 1, 1);

        // when
        let result = replica.handle_message(prepare);

        // then
        assert_eq!(result, Ok(()));

        // get_state message sent
        let get_state = Message::GetState(GetStateMessage {
            view: 0,
            operation_number: 0,
            replica_number: 1,
        });
        assert_eq!(
            *replica.network.incoming.borrow(),
            HashMap::from_iter([(2, get_state)])
        );
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_prepare_ok() {
        // given
        let prepare_ok = Message::PrepareOk(PrepareOkMessage {
            view: 0,
            operation_number: 1,
            replica_number: 1,
            client_id: 1,
        });
        let mut replica = create_primary(3);
        let operation = 0;
        replica.log.append(operation, 1, 1);
        replica.client_table.insert(
            1,
            ClientTableEntry {
                request_number: 1,
                response: None,
            },
        );
        replica.prepare_acks.insert(
            1,
            PrepareAck::Waiting {
                replicas: [2].into(),
            },
        );

        // when
        let result = replica.handle_message(prepare_ok);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.commit_number, 1);
        assert_eq!(
            replica.prepare_acks,
            HashMap::from_iter([(1, PrepareAck::Executed)])
        );

        // client table is correct
        let mut client_table = HashMap::new();
        client_table.insert(
            1,
            ClientTableEntry {
                request_number: 1,
                response: Some(MockService.execute(&operation)),
            },
        );
        assert_eq!(replica.client_table, client_table);

        // client response sent
        let reply_message = ReplyMessage {
            view: 0,
            request_number: 1,
            result: MockService.execute(&operation),
        };
        assert_eq!(*replica.network.incoming.borrow(), HashMap::new());
        assert_eq!(
            *replica.network.outgoing.borrow(),
            HashMap::from_iter([(1, reply_message)])
        );
    }

    #[test]
    fn handles_prepare_ok_waits_for_quorum() {
        // given
        let prepare_ok = Message::PrepareOk(PrepareOkMessage {
            view: 0,
            operation_number: 1,
            replica_number: 1,
            client_id: 1,
        });
        let mut replica = create_primary(5);
        let operation = 0;
        replica.log.append(operation, 1, 1);
        replica.client_table.insert(
            1,
            ClientTableEntry {
                request_number: 1,
                response: None,
            },
        );

        // when
        let result = replica.handle_message(prepare_ok);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(
            replica.prepare_acks,
            HashMap::from_iter([(
                1,
                PrepareAck::Waiting {
                    replicas: [1].into()
                }
            )])
        );

        // no message sent
        assert_eq!(*replica.network.incoming.borrow(), HashMap::new());
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_commit() {
        // given
        let commit = Message::Commit(CommitMessage {
            view: 0,
            operation_number: 2,
        });
        let mut replica = create_backup(3);
        replica.operation_number = 2;
        replica.commit_number = 0;
        replica.log.append(0, 1, 1);
        replica.log.append(1, 2, 1);

        // when
        let result = replica.handle_message(commit);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.commit_number, 2);

        // no message sent
        assert_eq!(*replica.network.incoming.borrow(), HashMap::new());
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_commit_trigger_state_transfer() {
        // given
        let commit = Message::Commit(CommitMessage {
            view: 0,
            operation_number: 2,
        });
        let mut replica = create_backup(3);
        replica.operation_number = 0;
        replica.commit_number = 0;

        // when
        let result = replica.handle_message(commit);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.commit_number, 0);

        // get_state message sent
        let get_state = Message::GetState(GetStateMessage {
            view: 0,
            operation_number: 0,
            replica_number: 1,
        });
        assert_eq!(
            *replica.network.incoming.borrow(),
            HashMap::from_iter([(2, get_state)])
        );
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_get_state() {
        // given
        let get_state = Message::GetState(GetStateMessage {
            view: 0,
            operation_number: 1,
            replica_number: 2,
        });
        let mut replica = create_backup(3);
        replica.operation_number = 3;
        replica.commit_number = 2;
        replica.log.append(0, 1, 1);
        replica.log.append(1, 2, 1);
        replica.log.append(2, 3, 1);

        // when
        let result = replica.handle_message(get_state);

        // then
        assert_eq!(result, Ok(()));

        // new_state message sent
        let new_state = Message::NewState(NewStateMessage {
            view: 0,
            log_after_operation: Log {
                entries: Vec::from([
                    LogEntry {
                        request_number: 2,
                        client_id: 1,
                        operation: 1,
                    },
                    LogEntry {
                        request_number: 3,
                        client_id: 1,
                        operation: 2,
                    },
                ]),
            },
            operation_number: 3,
            commit_number: 2,
        });
        assert_eq!(
            *replica.network.incoming.borrow(),
            HashMap::from_iter([(2, new_state)])
        );
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_new_state() {
        // given
        let new_state = Message::NewState(NewStateMessage {
            view: 0,
            log_after_operation: Log {
                entries: Vec::from([
                    LogEntry {
                        request_number: 2,
                        client_id: 1,
                        operation: 1,
                    },
                    LogEntry {
                        request_number: 3,
                        client_id: 1,
                        operation: 2,
                    },
                ]),
            },
            operation_number: 3,
            commit_number: 2,
        });
        let mut replica = create_backup(3);
        replica.log.append(0, 1, 1);
        replica.operation_number = 1;

        // when
        let result = replica.handle_message(new_state);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.operation_number, 3);
        assert_eq!(replica.commit_number, 2);

        // log is correct
        let mut log = Log::new();
        log.append(0, 1, 1);
        log.append(1, 2, 1);
        log.append(2, 3, 1);
        assert_eq!(replica.log, log);

        // no message sent
        assert_eq!(*replica.network.incoming.borrow(), HashMap::new());
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn triggers_view_change() {
        // given
        let mut replica = create_backup(3);
        replica.committed_at = Instant::now() - (IDLE_TIMEOUT_MS * 2);

        // when
        let result = replica.periodic();

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.status, ReplicaStatus::ViewChange);

        // start_view_change messages sent
        let start_view_change = Message::StartViewChange(StartViewChangeMessage {
            new_view: 1,
            replica_number: 1,
        });
        assert_eq!(
            *replica.network.incoming.borrow(),
            HashMap::from_iter([
                (0, start_view_change.clone()),
                (2, start_view_change.clone())
            ])
        );
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn triggers_view_change_respects_replica_status() {
        // given
        let mut replica = create_backup(3);
        replica.view = 0;
        replica.committed_at = Instant::now() - (IDLE_TIMEOUT_MS * 2);
        replica.status = ReplicaStatus::ViewChange;

        // when
        let result = replica.periodic();

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.status, ReplicaStatus::ViewChange);

        // no message sent
        assert_eq!(*replica.network.incoming.borrow(), HashMap::new());
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_start_view_change() {
        // given
        let start_view_change = Message::StartViewChange(StartViewChangeMessage {
            new_view: 1,
            replica_number: 1,
        });
        let mut replica = create_primary(3);
        replica.operation_number = 1;
        replica.log.append(0, 1, 1);

        // when
        let result = replica.handle_message(start_view_change);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.view, 1);
        assert_eq!(replica.status, ReplicaStatus::ViewChange);

        // do_view_change message sent
        let do_view_change = Message::DoViewChange(DoViewChangeMessage {
            old_view: 0,
            new_view: 1,
            log: Log {
                entries: vec![LogEntry {
                    request_number: 1,
                    client_id: 1,
                    operation: 0,
                }],
            },
            operation_number: 1,
            commit_number: 0,
            replica_number: 0,
        });
        assert_eq!(
            *replica.network.incoming.borrow(),
            HashMap::from_iter([(1, do_view_change)])
        );
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_start_view_change_new_primary_acknowledges() {
        // given
        let start_view_change = Message::StartViewChange(StartViewChangeMessage {
            new_view: 1,
            replica_number: 1,
        });
        let mut replica = create_backup(3);
        replica.view = 0;
        replica.operation_number = 1;
        replica.log.append(0, 1, 1);

        // when
        let result = replica.handle_message(start_view_change);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.view, 1);
        assert_eq!(replica.status, ReplicaStatus::ViewChange);

        // acks are set
        let view_acks = replica.view_acks.as_slice();
        assert_eq!(
            view_acks,
            &[ViewAck {
                old_view: 0,
                operation_number: 1,
                commit_number: 0,
                log: Log {
                    entries: vec![LogEntry {
                        request_number: 1,
                        client_id: 1,
                        operation: 0,
                    }],
                },
            }]
        );

        // no message sent
        assert_eq!(*replica.network.incoming.borrow(), HashMap::new());
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_do_view_change() {
        // given
        let mut message_log = Log::new();
        message_log.append(0, 1, 1);
        message_log.append(1, 2, 1);
        let do_view_change = Message::DoViewChange(DoViewChangeMessage {
            old_view: 0,
            new_view: 1,
            log: message_log.clone(),
            operation_number: 2,
            commit_number: 1,
            replica_number: 0,
        });

        let mut replica = create_backup(3);
        replica.view_acks = BinaryHeap::from([ViewAck {
            old_view: 0,
            operation_number: 1,
            commit_number: 0,
            log: Log {
                entries: vec![LogEntry {
                    request_number: 1,
                    client_id: 1,
                    operation: 0,
                }],
            },
        }]);
        replica.operation_number = 1;
        replica.log.append(0, 1, 1);

        // when
        let result = replica.handle_message(do_view_change);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.operation_number, 2);
        assert_eq!(replica.log, message_log.clone());
        assert_eq!(replica.commit_number, 1);
        assert_eq!(
            replica.client_table,
            HashMap::from_iter([(
                1,
                ClientTableEntry {
                    request_number: 2,
                    response: Some(2)
                }
            )])
        );

        // start_view messages sent
        let start_view = Message::StartView(StartViewMessage {
            view: 1,
            log: message_log,
            operation_number: 2,
            commit_number: 1,
        });
        assert_eq!(
            *replica.network.incoming.borrow(),
            HashMap::from_iter([(0, start_view.clone()), (2, start_view.clone())])
        );

        // reply messages sent
        assert_eq!(
            *replica.network.outgoing.borrow(),
            HashMap::from_iter([(
                1,
                ReplyMessage {
                    view: 1,
                    request_number: 2,
                    result: 2
                }
            )])
        );
    }

    #[test]
    fn handles_do_view_change_waits_for_quorum() {
        // given
        let mut message_log = Log::new();
        message_log.append(0, 1, 1);
        message_log.append(1, 2, 1);
        let do_view_change = Message::DoViewChange(DoViewChangeMessage {
            old_view: 0,
            new_view: 1,
            log: message_log.clone(),
            operation_number: 2,
            commit_number: 1,
            replica_number: 0,
        });

        let mut replica = create_backup(5);

        // when
        let result = replica.handle_message(do_view_change);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(
            replica.view_acks.as_slice(),
            &[ViewAck {
                old_view: 0,
                operation_number: 2,
                commit_number: 0,
                log: message_log.clone()
            }]
        );

        // no messages sent
        assert_eq!(*replica.network.incoming.borrow(), HashMap::new());
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_start_view() {
        // given
        let mut message_log = Log::new();
        message_log.append(0, 1, 1);
        message_log.append(1, 2, 1);
        let start_view = Message::StartView(StartViewMessage {
            view: 1,
            log: message_log.clone(),
            operation_number: 2,
            commit_number: 1,
        });

        let mut replica = create_primary(3);
        replica.view = 0;
        replica.operation_number = 1;
        replica.log.append(0, 1, 1);

        // when
        let result = replica.handle_message(start_view);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.status, ReplicaStatus::Normal);
        assert_eq!(replica.view, 1);
        assert_eq!(replica.log, message_log.clone());
        assert_eq!(replica.operation_number, 2);
        assert_eq!(replica.commit_number, 1);
        assert_eq!(
            replica.client_table,
            HashMap::from_iter([(
                1,
                ClientTableEntry {
                    request_number: 2,
                    response: Some(2)
                }
            )])
        );

        // prepare_ok message is sent to new replica
        assert_eq!(
            *replica.network.incoming.borrow(),
            HashMap::from_iter([(
                1,
                Message::PrepareOk(PrepareOkMessage {
                    view: 1,
                    operation_number: 2,
                    replica_number: 0,
                    client_id: 1,
                })
            )])
        );
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn triggers_recovery() {
        // given
        let mut replica = create_backup(3);

        // when
        let result = replica.trigger_recovery();

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert!(matches!(replica.status, ReplicaStatus::Recovering { .. }));

        // prepare_ok message is sent to new replica
        let incoming_messages = replica.network.incoming.borrow();
        assert_eq!(incoming_messages.len(), 2);

        let Message::Recovery(RecoveryMessage { replica_number, .. }) =
            incoming_messages.get(&0).unwrap()
        else {
            panic!("unexpected message")
        };
        assert_eq!(replica_number, &1);
        let Message::Recovery(RecoveryMessage { replica_number, .. }) =
            incoming_messages.get(&2).unwrap()
        else {
            panic!("unexpected message")
        };
        assert_eq!(replica_number, &1);

        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_recovery() {
        // given
        let recovery = Message::Recovery(RecoveryMessage {
            replica_number: 1,
            nonce: 1234,
        });
        let mut replica = create_primary(3);
        replica.view = 0;
        replica.commit_number = 1;
        replica.operation_number = 2;
        replica.log.append(0, 1, 1);
        replica.log.append(1, 2, 1);

        // when
        let result = replica.handle_message(recovery);

        // then
        assert_eq!(result, Ok(()));

        // recovery_response message is sent to recovering replica
        assert_eq!(
            *replica.network.incoming.borrow(),
            HashMap::from_iter([(
                1,
                Message::RecoveryResponse(RecoveryResponseMessage {
                    view: 0,
                    nonce: 1234,
                    primary: Some(RecoveryPrimaryResponse {
                        log: replica.log.clone(),
                        operation_number: 2,
                        commit_number: 1
                    })
                })
            )])
        );
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_recovery_backup_replica() {
        // given
        let recovery = Message::Recovery(RecoveryMessage {
            replica_number: 1,
            nonce: 1234,
        });
        let mut replica = create_backup(3);
        replica.view = 0;
        replica.commit_number = 1;
        replica.operation_number = 2;
        replica.log.append(0, 1, 1);
        replica.log.append(1, 2, 1);

        // when
        let result = replica.handle_message(recovery);

        // then
        assert_eq!(result, Ok(()));

        // recovery_response message is sent to recovering replica
        assert_eq!(
            *replica.network.incoming.borrow(),
            HashMap::from_iter([(
                1,
                Message::RecoveryResponse(RecoveryResponseMessage {
                    view: 0,
                    nonce: 1234,
                    primary: None
                })
            )])
        );
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_recovery_response() {
        // given
        let mut log = Log::new();
        log.append(0, 1, 1);
        log.append(1, 2, 1);

        let recovery_response = Message::RecoveryResponse(RecoveryResponseMessage {
            view: 2,
            nonce: 1234,
            primary: Some(RecoveryPrimaryResponse {
                log: log.clone(),
                operation_number: 2,
                commit_number: 1,
            }),
        });
        let mut replica = create_backup(3);
        replica.status = ReplicaStatus::Recovering { nonce: 1234 };
        replica.recovery_acks.push(RecoveryAck {
            primary: Some(RecoveryPrimaryAck {
                log: Log {
                    entries: vec![LogEntry {
                        request_number: 1,
                        client_id: 1,
                        operation: 0,
                    }],
                },
                operation_number: 1,
                commit_number: 0,
                view: 1,
            }),
            view: 1,
        });
        replica.recovery_acks.push(RecoveryAck {
            primary: None,
            view: 2,
        });

        // when
        let result = replica.handle_message(recovery_response);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.view, 2);
        assert_eq!(replica.operation_number, 2);
        assert_eq!(replica.log, log);
        assert_eq!(replica.commit_number, 1);
        assert_eq!(
            replica.client_table,
            HashMap::from_iter([(
                1,
                ClientTableEntry {
                    request_number: 1,
                    response: Some(1)
                }
            )])
        );
        assert_eq!(replica.status, ReplicaStatus::Normal);

        // no message sent
        assert_eq!(*replica.network.incoming.borrow(), HashMap::new());
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_recovery_response_waits_for_quorum() {
        // given
        let mut log = Log::new();
        log.append(0, 1, 1);
        log.append(1, 2, 1);

        let recovery_response = Message::RecoveryResponse(RecoveryResponseMessage {
            view: 2,
            nonce: 1234,
            primary: Some(RecoveryPrimaryResponse {
                log: log.clone(),
                operation_number: 2,
                commit_number: 1,
            }),
        });
        let mut replica = create_backup(3);
        replica.status = ReplicaStatus::Recovering { nonce: 1234 };

        // when
        let result = replica.handle_message(recovery_response);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.operation_number, 0);
        assert_eq!(
            replica.recovery_acks,
            vec![RecoveryAck {
                primary: Some(RecoveryPrimaryAck {
                    log: log.clone(),
                    operation_number: 2,
                    commit_number: 1,
                    view: 2,
                }),
                view: 2,
            }]
        );

        // no message sent
        assert_eq!(*replica.network.incoming.borrow(), HashMap::new());
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }

    #[test]
    fn handles_recovery_response_waits_for_primary_ack() {
        // given
        let mut log = Log::new();
        log.append(0, 1, 1);
        log.append(1, 2, 1);

        let recovery_response = Message::RecoveryResponse(RecoveryResponseMessage {
            view: 2,
            nonce: 1234,
            primary: None,
        });
        let mut replica = create_backup(3);
        replica.status = ReplicaStatus::Recovering { nonce: 1234 };
        replica.recovery_acks.push(RecoveryAck {
            primary: None,
            view: 2,
        });

        // when
        let result = replica.handle_message(recovery_response);

        // then
        assert_eq!(result, Ok(()));

        // state is correct
        assert_eq!(replica.operation_number, 0);
        assert_eq!(
            replica.recovery_acks,
            vec![
                RecoveryAck {
                    primary: None,
                    view: 2
                },
                RecoveryAck {
                    primary: None,
                    view: 2
                }
            ]
        );

        // no message sent
        assert_eq!(*replica.network.incoming.borrow(), HashMap::new());
        assert_eq!(*replica.network.outgoing.borrow(), HashMap::new());
    }
}
