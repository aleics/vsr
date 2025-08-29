use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    net::SocketAddr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bincode::{Decode, Encode};
use thiserror::Error;

use crate::{
    ClientId, Operation, ReplicaId, Service, ServiceError,
    bus::ReplicaMessageBus,
    clock::Timeout,
    io::IOError,
    message::{
        CommitMessage, CommitMessageBody, DoViewChangeMessage, DoViewChangeMessageBody,
        GetStateMessage, GetStateMessageBody, Header, Message, NewStateMessage,
        NewStateMessageBody, PrepareMessage, PrepareMessageBody, PrepareOkMessage,
        PrepareOkMessageBody, RecoveryMessage, RecoveryMessageBody, RecoveryPrimaryResponse,
        RecoveryResponseMessage, RecoveryResponseMessageBody, ReplyMessage, ReplyMessageBody,
        RequestMessage, StartViewChangeMessage, StartViewChangeMessageBody, StartViewMessage,
        StartViewMessageBody,
    },
    storage::LogStorage,
};

const HEALTH_ID: &str = "replica_health";
const HEALTH_TICK_COUNT: u64 = 100;
const COMMIT_TIMEOUT_MS: Duration = Duration::from_millis(200);
const IDLE_TIMEOUT_MS: Duration = Duration::from_millis(2000);

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReplicaConfig {
    pub(crate) seed: u64,

    /// The current replica number given the configuration.
    pub(crate) replica: ReplicaId,

    /// The total amount of replicas.
    pub(crate) total: u8,

    /// The socket addresses of all the replicas (including itself).
    pub(crate) addresses: Vec<SocketAddr>,
}

/// A single replica
pub struct Replica<S, ST, IO: crate::io::IO> {
    /// The current replica number given the configuration.
    replica_number: ReplicaId,

    /// Internal view number of the replica.
    pub(crate) view: ReplicaId,

    /// The total amount of replicas.
    total: u8,

    /// Internal replica state to understand availability.
    status: ReplicaStatus,

    /// Most recently received request, initially 0.
    operation_number: usize,

    /// Acknowledgements table with the operations that have been acknowledged by
    /// a given amount of replicas for a client request.
    /// Key is the operation number, value is the replica numbers that acknowledged
    /// this operation.
    prepare_acks: HashMap<usize, PrepareAck>,

    /// Heap with the view change acknowledgements sorted by latest view and
    /// operation number.
    view_acks: BinaryHeap<ViewAck>,

    /// An unsorted set of recovery acknowledgements coming from other replicas.
    recovery_acks: Vec<RecoveryAck>,

    /// Log entries of size `operation_number` containing the requests
    /// that have been received so far in their assigned order.
    log: LogStore<ST>,

    /// The operation number of the most recently committed operation.
    commit_number: usize,

    /// The timestamp of the last committed operation.
    committed_at: Instant,

    /// For each client, the number of its most recent request, plus,
    /// if the request has been executed, the result sent for that request.
    client_table: ClientTable,

    /// Service container used by the replica to execute client requests.
    service: S,

    /// Network of the replica containing connections to all the replicas
    /// and the client.
    pub(crate) bus: ReplicaMessageBus<IO>,

    /// A health timeout used to periodically check the health of the replica.
    /// Meaning, it can communicate with the rest of the replicas.
    health: Timeout,
}

impl<S, ST, I, IO, O> Replica<S, ST, IO>
where
    S: Service<Input = I, Output = O>,
    ST: LogStorage,
    I: Decode<()>,
    O: Encode,
    IO: crate::io::IO,
{
    pub(crate) fn new(config: &ReplicaConfig, service: S, storage: ST, io: IO) -> Self {
        let bus = ReplicaMessageBus::new(config, io);
        let health = Timeout::new(HEALTH_ID, HEALTH_TICK_COUNT);

        Replica {
            replica_number: config.replica,
            view: 0,
            total: config.addresses.len() as u8,
            status: ReplicaStatus::Normal,
            operation_number: 0,
            log: LogStore::new(storage),
            commit_number: 0,
            committed_at: Instant::now(),
            prepare_acks: HashMap::default(),
            view_acks: BinaryHeap::new(),
            recovery_acks: Vec::new(),
            client_table: ClientTable::new(),
            bus,
            service,
            health,
        }
    }

    pub fn init(&mut self) -> Result<(), ReplicaError> {
        self.bus.init()?;
        self.health.start();
        Ok(())
    }

    pub fn run(&mut self) -> Result<(), ReplicaError> {
        loop {
            self.tick()?;
        }
    }

    pub fn tick(&mut self) -> Result<(), ReplicaError> {
        if self.health.fired() {
            let output = self.periodic()?;
            self.health.reset();

            self.handle_output(output)?;
        }

        self.health.tick();

        for message in self.bus.tick()? {
            if let Err(err) = self
                .handle_message(message)
                .and_then(|output| self.handle_output(output))
            {
                self.handle_err(&err);
            }
        }

        Ok(())
    }

    fn handle_output(&mut self, output: HandleOutput) -> Result<(), ReplicaError> {
        match output {
            HandleOutput::DoNothing => Ok(()),
            HandleOutput::Actions(actions) => {
                for action in actions {
                    match action {
                        OutputAction::Broadcast { message } => self.broadcast(&message)?,
                        OutputAction::Send { message, replica } => {
                            let sent = self.bus.send_to_replica(&message, replica)?;
                            if !sent {
                                tracing::error!(
                                    "[Replica::handle_output] Message could not be sent replica (from: {}, to: {})",
                                    self.replica_number,
                                    replica
                                );
                            }
                        }
                        OutputAction::SendClient { reply, client_id } => {
                            let sent = self.bus.send_to_client(reply, client_id)?;
                            if !sent {
                                tracing::error!(
                                    "[Replica::handle_output] Message could not be sent client (replica: {}, client: {})",
                                    self.replica_number,
                                    client_id
                                );
                            }
                        }
                    }
                }

                Ok(())
            }
        }
    }

    fn broadcast(&mut self, message: &Message) -> Result<(), ReplicaError> {
        // For all the replicas except itself
        for i in 0..self.total {
            if i != self.replica_number {
                self.bus.send_to_replica(message, i)?;
            }
        }

        Ok(())
    }

    fn handle_err(&mut self, err: &ReplicaError) {
        match err {
            ReplicaError::NotReady | ReplicaError::MessageViewMismatch | ReplicaError::IO(_) => {
                tracing::warn!(
                    "[Replica::handle_err] Error occurred \"{}\" (replica: {})",
                    err,
                    self.replica_number
                );
            }
            ReplicaError::InvalidState | ReplicaError::ServiceExecution => {
                let output = self.trigger_recovery();
                self.handle_output(output)
                    .unwrap_or_else(|err| self.handle_err(&err));
            }
        }
    }

    fn execute_operation(&self, input: &Operation) -> Result<Option<Operation>, ReplicaError> {
        let result = self.service.execute_bytes(input);

        match result {
            Ok(output) => Ok(Some(output)),
            Err(err) => match err {
                ServiceError::Unrecoverable(err) => {
                    tracing::error!(err);
                    Err(ReplicaError::ServiceExecution)
                }
                ServiceError::Recoverable(err) => {
                    tracing::warn!(err);
                    Ok(None)
                }
                ServiceError::IO(err) => {
                    tracing::error!("IO error occurred while executing operation: {}", err);
                    Err(ReplicaError::ServiceExecution)
                }
            },
        }
    }

    fn handle_message(&mut self, message: Message) -> Result<HandleOutput, ReplicaError> {
        tracing::debug!(
            "[Replica::handle_message] Received message (message: {:?}, replica: {})",
            message,
            self.replica_number
        );

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
            Message::Reply(..) => unreachable!(),
        }
    }

    fn handle_request(&mut self, request: RequestMessage) -> Result<HandleOutput, ReplicaError> {
        // Backup replicas ignore client request message.
        if !self.is_primary() {
            return Ok(HandleOutput::DoNothing);
        }

        // The replica and message should message
        if self.view != request.header.view {
            return Err(ReplicaError::MessageViewMismatch);
        }

        // The replica is not ready to receive calls, the client must wait.
        if self.status != ReplicaStatus::Normal {
            return Err(ReplicaError::NotReady);
        }

        // If the request-number isn’t bigger than the information in the client table it drops the
        // request, but it will re-send the response if the request is the most recent one from this
        // client and it has already been executed.
        if let Some(most_recent_request) = self.client_table.read(&request.body.client_id) {
            if most_recent_request.request_number > request.body.request_number {
                return Ok(HandleOutput::DoNothing);
            }

            if most_recent_request.request_number == request.body.request_number
                && let Some(result) = &most_recent_request.response
            {
                let message = ReplyMessage {
                    header: Header {
                        view: request.header.view,
                    },
                    body: ReplyMessageBody {
                        request_number: most_recent_request.request_number,
                        result: result.clone(),
                    },
                };
                return Ok(HandleOutput::send_client(message, request.body.client_id));
            }
        }

        // Advance the operation number adds the request to the end of the log, and updates the
        // information for this client in the client-table to contain the new request number
        assert!(self.operation_number == self.log.size());

        self.operation_number += 1;
        self.log.append(
            request.body.operation.clone(),
            request.body.request_number,
            request.body.client_id,
        );
        self.client_table.add(
            request.body.client_id,
            ClientTableEntry {
                request_number: request.body.request_number,
                response: None,
            },
        );

        // Broadcast message to the backup replicas
        Ok(HandleOutput::broadcast(Message::Prepare(PrepareMessage {
            header: Header {
                view: request.header.view,
            },
            body: PrepareMessageBody {
                operation_number: self.operation_number,
                commit_number: self.commit_number,
                request,
            },
        })))
    }

    fn handle_prepare(&mut self, prepare: PrepareMessage) -> Result<HandleOutput, ReplicaError> {
        // `Prepare` messages must only be received by backup replicas
        assert!(!self.is_primary());

        // The replica and message should message
        if self.view != prepare.header.view {
            return Err(ReplicaError::MessageViewMismatch);
        }

        // The operation has already been processed by the replica. The message is duplicate and will be ignored.
        if self.operation_number + 1 > prepare.body.operation_number {
            return Ok(HandleOutput::DoNothing);
        }

        // Replica is missing some operations, we should initiate state transfer
        if self.operation_number + 1 < prepare.body.operation_number {
            return self.trigger_state_transfer();
        }

        // Advance the operation number, update the log and the client table
        assert!(self.operation_number + 1 == prepare.body.operation_number);
        assert!(self.operation_number == self.log.size());

        self.operation_number = prepare.body.operation_number;
        self.log.append(
            prepare.body.request.body.operation,
            prepare.body.request.body.request_number,
            prepare.body.request.body.client_id,
        );
        self.client_table.add(
            prepare.body.request.body.client_id,
            ClientTableEntry {
                request_number: prepare.body.request.body.request_number,
                response: None,
            },
        );

        // Execute previous committed operations by checking the log
        for commit_number in (self.commit_number + 1)..=prepare.body.commit_number {
            let Some(operation) = self.log.get_operation(commit_number) else {
                // Log state is incorrect. Intermediate operations are missing
                return Err(ReplicaError::InvalidState);
            };

            self.execute_operation(&operation)?;
        }

        self.commit_number = prepare.body.commit_number;
        self.committed_at = Instant::now();

        // Send a prepare ok message to the primary
        Ok(HandleOutput::send(
            Message::PrepareOk(PrepareOkMessage {
                header: Header {
                    view: prepare.header.view,
                },
                body: PrepareOkMessageBody {
                    operation_number: prepare.body.operation_number,
                    replica: self.replica_number,
                    client_id: prepare.body.request.body.client_id,
                },
            }),
            self.view,
        ))
    }

    fn handle_prepare_ok(
        &mut self,
        prepare_ok: PrepareOkMessage,
    ) -> Result<HandleOutput, ReplicaError> {
        // `PrepareOk` messages must only be received by the primary
        assert!(self.is_primary());

        // The replica and message should message
        if self.view != prepare_ok.header.view {
            return Err(ReplicaError::MessageViewMismatch);
        }

        // The primary waits for at least a quorum amount of `PrepareOk` messages
        // from different backups
        let ack = self
            .prepare_acks
            .entry(prepare_ok.body.operation_number)
            .or_insert(PrepareAck::Waiting {
                replicas: HashSet::with_capacity(self.total as usize),
            });

        let ack_replicas = match ack {
            PrepareAck::Waiting { replicas } => replicas,
            PrepareAck::Executed => return Ok(HandleOutput::DoNothing),
        };

        ack_replicas.insert(prepare_ok.body.replica);

        // Quorum not reached yet. Primary waits for more `PrepareOk` messages
        if ack_replicas.len() < quorum(self.total) {
            return Ok(HandleOutput::DoNothing);
        }

        // Execute the query
        let operation = self
            .log
            .get_operation(prepare_ok.body.operation_number)
            .expect("Operation not found in log");

        let Some(result) = self.execute_operation(&operation)? else {
            return Ok(HandleOutput::DoNothing);
        };

        // Increment commit_number
        self.commit_number += 1;
        self.committed_at = Instant::now();

        self.prepare_acks
            .insert(prepare_ok.body.operation_number, PrepareAck::Executed);

        // The primary also updates the client's entry in the client-table to contain the result
        let client_request = self.client_table.update_response(&prepare_ok.body.client_id, result.clone())
            .expect("Client must be present in the client table since the request message has been already received");

        // Send reply message to the client
        Ok(HandleOutput::send_client(
            ReplyMessage {
                header: Header {
                    view: prepare_ok.header.view,
                },
                body: ReplyMessageBody {
                    request_number: client_request.request_number,
                    result,
                },
            },
            prepare_ok.body.client_id,
        ))
    }

    fn handle_commit(&mut self, commit: CommitMessage) -> Result<HandleOutput, ReplicaError> {
        // Replica is not prepared to handle operations, the commit message is ignored.
        if self.status != ReplicaStatus::Normal {
            return Ok(HandleOutput::DoNothing);
        }

        // `Commit` messages must only be received by backup replicas
        assert!(!self.is_primary());

        // The replica and message should message
        if self.view != commit.header.view {
            return Err(ReplicaError::MessageViewMismatch);
        }

        // If the replica has a higher or equal operation number, the operation
        // was already commited. We'll update our latest commit operation but nothing
        // to execute.
        if self.commit_number >= commit.body.operation_number {
            self.committed_at = Instant::now();
            return Ok(HandleOutput::DoNothing);
        }

        let has_log = self
            .log
            .get_operation(commit.body.operation_number)
            .is_some();
        if !has_log {
            return self.trigger_state_transfer();
        }

        // Execute uncommitted operations by checking the log
        for commit_number in (self.commit_number + 1)..=commit.body.operation_number {
            let Some(operation) = self.log.get_operation(commit_number) else {
                // Log state is incorrect. Intermediate operations are missing
                return Err(ReplicaError::InvalidState);
            };

            self.execute_operation(&operation)?;
        }

        self.commit_number = commit.body.operation_number;
        self.committed_at = Instant::now();

        Ok(HandleOutput::DoNothing)
    }

    fn trigger_state_transfer(&self) -> Result<HandleOutput, ReplicaError> {
        let message = Message::GetState(GetStateMessage {
            header: Header { view: self.view },
            body: GetStateMessageBody {
                operation_number: self.operation_number,
                replica: self.replica_number,
            },
        });

        // Send message to the next replica in the circle
        let next_replica = next_replica(self.replica_number);
        Ok(HandleOutput::send(message, next_replica))
    }

    fn handle_get_state(
        &mut self,
        get_state: GetStateMessage,
    ) -> Result<HandleOutput, ReplicaError> {
        assert!(self.status == ReplicaStatus::Normal);
        assert!(self.view == get_state.header.view);

        let log_after_operation = self.log.since(get_state.body.operation_number);

        let message = Message::NewState(NewStateMessage {
            header: Header { view: self.view },
            body: NewStateMessageBody {
                replica: self.replica_number,
                log_after_operation,
                operation_number: self.operation_number,
                commit_number: self.commit_number,
            },
        });

        Ok(HandleOutput::send(message, get_state.body.replica))
    }

    fn handle_new_state(
        &mut self,
        new_state: NewStateMessage,
    ) -> Result<HandleOutput, ReplicaError> {
        assert!(self.view == new_state.header.view);

        self.log.merge(new_state.body.log_after_operation);
        self.operation_number = new_state.body.operation_number;

        // Execute previous committed operations by checking the log
        for commit_number in (self.commit_number + 1)..=new_state.body.commit_number {
            let Some(operation) = self.log.get_operation(commit_number) else {
                // Log state is incorrect. Intermediate operations are missing
                return Err(ReplicaError::InvalidState);
            };

            self.execute_operation(&operation)?;
        }
        self.commit_number = new_state.body.commit_number;
        self.committed_at = Instant::now();

        Ok(HandleOutput::DoNothing)
    }

    fn handle_start_view_change(
        &mut self,
        start_view_change: StartViewChangeMessage,
    ) -> Result<HandleOutput, ReplicaError> {
        let old_view = self.view;

        // New backup replica acknowledges a view change and waits for the new primary
        // to send a `StartView` message
        if self.view != start_view_change.body.new_view {
            self.view = start_view_change.body.new_view;
            self.status = ReplicaStatus::ViewChange;
        }

        let log = self.log.read_all();

        // The current primary won't send a `DoViewChange` message to itself.
        // It will acknowledge the view change and wait for `DoViewChange` messages.
        if self.is_primary() {
            self.view_acks.push(ViewAck {
                old_view,
                operation_number: self.operation_number,
                commit_number: self.commit_number,
                log,
            });
            return Ok(HandleOutput::DoNothing);
        }

        // Send a `DoViewChange` to the new primary replica to mark the current replica
        // ready for a view change.
        let message = Message::DoViewChange(DoViewChangeMessage {
            header: Header { view: self.view },
            body: DoViewChangeMessageBody {
                old_view,
                new_view: self.view,
                log,
                operation_number: self.operation_number,
                commit_number: self.commit_number,
                replica: self.replica_number,
            },
        });

        Ok(HandleOutput::send(message, self.view))
    }

    fn handle_do_view_change(
        &mut self,
        do_view_change: DoViewChangeMessage,
    ) -> Result<HandleOutput, ReplicaError> {
        // When the primary received `quorum + 1` `DoViewChange` messages from different replicas
        // (including itself), it sets its view number to that in the messages as selects as the
        // new log the one contained in the message with the largest view.
        self.view_acks.push(ViewAck {
            old_view: do_view_change.body.old_view,
            operation_number: do_view_change.body.operation_number,
            commit_number: do_view_change.body.commit_number,
            log: do_view_change.body.log,
        });

        if self.view_acks.len() < quorum(self.total) {
            return Ok(HandleOutput::DoNothing);
        }

        if self.view != do_view_change.body.new_view {
            self.view = do_view_change.body.new_view;
            self.status = ReplicaStatus::ViewChange;
        }

        let new_view = self.view_acks.drain().next().expect(
            "View acknowledgements are more than the quorum. At least 1 entry must be present.",
        );

        self.status = ReplicaStatus::Normal;

        self.log.overwrite(new_view.log);
        self.operation_number = new_view.operation_number;

        let mut actions = Vec::new();

        // In case any uncommitted operations are present
        if self.operation_number > self.commit_number {
            let mut executed_requests = HashMap::new();

            // Iterate over all the uncommitted operations in the log in order
            for operation_number in (new_view.commit_number + 1)..=self.operation_number {
                let log_entry = self.log.get_entry(operation_number).expect(
                    "Log entry must be present if the operation number is higher than the log size",
                );

                // Execute the operation and send reply to the client
                let Some(result) = self.execute_operation(&log_entry.operation)? else {
                    return Ok(HandleOutput::DoNothing);
                };

                actions.push(OutputAction::SendClient {
                    reply: ReplyMessage {
                        header: Header { view: self.view },
                        body: ReplyMessageBody {
                            request_number: log_entry.request_number,
                            result: result.clone(),
                        },
                    },
                    client_id: log_entry.client_id,
                });

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
            header: Header { view: self.view },
            body: StartViewMessageBody {
                replica: self.replica_number,
                log: self.log.read_all(),
                operation_number: self.operation_number,
                commit_number: self.commit_number,
            },
        });

        actions.push(OutputAction::Broadcast { message });

        Ok(HandleOutput::Actions(actions))
    }

    fn handle_start_view(
        &mut self,
        start_view: StartViewMessage,
    ) -> Result<HandleOutput, ReplicaError> {
        // Refresh internal state with the new view
        self.status = ReplicaStatus::Normal;
        self.view = start_view.header.view;
        self.log.overwrite(start_view.body.log);
        self.operation_number = start_view.body.operation_number;

        let mut actions = Vec::new();

        // Iterate over all the uncommitted operations in the log in order
        if self.operation_number > self.commit_number {
            let mut executed_requests = HashMap::new();
            for operation_number in (start_view.body.commit_number + 1)..=self.operation_number {
                let log_entry = self.log.get_entry(operation_number).expect(
                    "Log entry must be present if the operation number is higher than the log size",
                );

                // Execute the operation and send a `PrepareOk` to the new primary
                let Some(result) = self.execute_operation(&log_entry.operation)? else {
                    return Ok(HandleOutput::DoNothing);
                };

                actions.push(OutputAction::Send {
                    message: Message::PrepareOk(PrepareOkMessage {
                        header: Header { view: self.view },
                        body: PrepareOkMessageBody {
                            operation_number,
                            replica: self.replica_number,
                            client_id: log_entry.client_id,
                        },
                    }),
                    replica: self.view,
                });

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

        self.commit_number = start_view.body.commit_number;
        self.committed_at = Instant::now();

        Ok(HandleOutput::Actions(actions))
    }

    fn handle_recovery(&mut self, recovery: RecoveryMessage) -> Result<HandleOutput, ReplicaError> {
        // A replica won't respond to a recovery if it's not operational
        assert!(self.status == ReplicaStatus::Normal);

        let response = if self.is_primary() {
            RecoveryResponseMessage {
                header: Header { view: self.view },
                body: RecoveryResponseMessageBody {
                    replica: self.replica_number,
                    nonce: recovery.body.nonce,
                    primary: Some(RecoveryPrimaryResponse {
                        log: self.log.read_all(),
                        operation_number: self.operation_number,
                        commit_number: self.commit_number,
                    }),
                },
            }
        } else {
            RecoveryResponseMessage {
                header: Header { view: self.view },
                body: RecoveryResponseMessageBody {
                    replica: self.replica_number,
                    nonce: recovery.body.nonce,
                    primary: None,
                },
            }
        };

        Ok(HandleOutput::send(
            Message::RecoveryResponse(response),
            recovery.body.replica,
        ))
    }

    fn handle_recovery_response(
        &mut self,
        recovery_response: RecoveryResponseMessage,
    ) -> Result<HandleOutput, ReplicaError> {
        match self.status {
            ReplicaStatus::Recovering { nonce } => assert!(nonce == recovery_response.body.nonce),
            _ => panic!("Replica must be in recovering status to handle recovery responses"),
        }

        // Attach recovery acknowledgements and check if quorum is reached
        self.recovery_acks
            .push(RecoveryAck::from_message(recovery_response));

        if self.recovery_acks.len() < (quorum(self.total) + 1) {
            return Ok(HandleOutput::DoNothing);
        }

        // Read the primary acknowledgements of the latest view. If no acknowledgement from the
        // primary replicas has been received, the recovering replica must wait.
        let mut ready_ack: Option<&RecoveryPrimaryAck> = None;
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
            return Ok(HandleOutput::DoNothing);
        };

        self.view = *view;
        self.operation_number = *operation_number;
        self.log.overwrite(log.clone());

        // Execute previous committed operations by checking the log
        let recovery_commit_number = *commit_number;
        let mut executed_requests = HashMap::new();
        for commit_number in (self.commit_number + 1)..=recovery_commit_number {
            let Some(log_entry) = self.log.get_entry(commit_number) else {
                // Log state is incorrect. Intermediate operations are missing
                return Err(ReplicaError::InvalidState);
            };

            let Some(result) = self.execute_operation(&log_entry.operation)? else {
                return Ok(HandleOutput::DoNothing);
            };

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

        Ok(HandleOutput::DoNothing)
    }

    fn periodic(&mut self) -> Result<HandleOutput, ReplicaError> {
        // Primary periodic tasks
        if self.is_primary() {
            // If the primary is idle (no requests sent)
            if self.committed_at.elapsed() >= COMMIT_TIMEOUT_MS {
                return self.trigger_commit(self.view, self.operation_number);
            }
        } else {
            // If the backup is idle (no requests received)
            if self.committed_at.elapsed() >= IDLE_TIMEOUT_MS {
                return self.trigger_view_change();
            }
        }

        Ok(HandleOutput::DoNothing)
    }

    fn trigger_commit(
        &self,
        view: ReplicaId,
        operation_number: usize,
    ) -> Result<HandleOutput, ReplicaError> {
        // `Commit` messages must only be received by the primary
        assert!(self.replica_number == view);

        let message = Message::Commit(CommitMessage {
            header: Header { view },
            body: CommitMessageBody {
                replica: self.replica_number,
                operation_number,
            },
        });

        Ok(HandleOutput::broadcast(message))
    }

    fn trigger_view_change(&mut self) -> Result<HandleOutput, ReplicaError> {
        // If the replica is not running a view change already, or it's in recovery
        // we won't start a new change.
        if self.status != ReplicaStatus::Normal {
            return Ok(HandleOutput::DoNothing);
        }

        self.view = next_replica(self.view);
        self.status = ReplicaStatus::ViewChange;

        let message = Message::StartViewChange(StartViewChangeMessage {
            header: Header { view: self.view },
            body: StartViewChangeMessageBody {
                new_view: self.view,
                replica: self.replica_number,
            },
        });

        Ok(HandleOutput::broadcast(message))
    }

    fn trigger_recovery(&mut self) -> HandleOutput {
        let nonce = nonce();
        self.status = ReplicaStatus::Recovering { nonce };

        let message = Message::Recovery(RecoveryMessage {
            header: Header { view: self.view },
            body: RecoveryMessageBody {
                replica: self.replica_number,
                nonce,
            },
        });

        HandleOutput::broadcast(message)
    }

    fn is_primary(&self) -> bool {
        (self.view % self.total) == self.replica_number
    }
}

fn next_replica(replica_number: ReplicaId) -> ReplicaId {
    replica_number + 1
}

#[derive(Debug, Clone, PartialEq)]
enum HandleOutput {
    Actions(Vec<OutputAction>),
    DoNothing,
}

impl HandleOutput {
    fn broadcast(message: Message) -> Self {
        HandleOutput::Actions(vec![OutputAction::Broadcast { message }])
    }

    fn send(message: Message, replica: ReplicaId) -> Self {
        HandleOutput::Actions(vec![OutputAction::Send { message, replica }])
    }

    fn send_client(reply: ReplyMessage, client_id: ClientId) -> Self {
        HandleOutput::Actions(vec![OutputAction::SendClient { reply, client_id }])
    }
}

#[derive(Debug, Clone, PartialEq)]
enum OutputAction {
    Broadcast {
        message: Message,
    },
    Send {
        message: Message,
        replica: ReplicaId,
    },
    SendClient {
        reply: ReplyMessage,
        client_id: ClientId,
    },
}

pub(crate) fn quorum(total: u8) -> usize {
    // total = 2 * f + 1
    // f = (total - 1) / 2
    (total as usize - 1) / 2
}

/// Create a nonce ("number used once") using the current system time
/// and the UNIX epoch time.
fn nonce() -> u64 {
    let now = SystemTime::now();
    now.duration_since(UNIX_EPOCH).unwrap().as_secs()
}

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub(crate) struct Log {
    entries: Vec<LogEntry>,
}

#[derive(Debug)]
struct LogStore<S> {
    /// A queue of log entries for each request sent to the replica.
    storage: S,
    size: usize,
}

impl<ST: LogStorage> LogStore<ST> {
    fn new(storage: ST) -> Self {
        LogStore { storage, size: 0 }
    }

    fn append(&mut self, operation: Operation, request_number: u32, client_id: ClientId) {
        let key = self.size;
        let value = LogEntry {
            request_number,
            client_id,
            operation,
        };

        self.add(key, value);
    }

    fn add(&mut self, operation_number: usize, entry: LogEntry) {
        self.storage.write(operation_number, entry); // TODO: handle this
        self.size += 1;
    }

    fn add_next(&mut self, entry: LogEntry) {
        let operation_number = self.size;

        self.storage.write(operation_number, entry); // TODO: handle this
        self.size += 1;
    }

    fn get_operation(&self, operation_number: usize) -> Option<Operation> {
        self.get_entry(operation_number)
            .map(|entry| entry.operation)
    }

    fn get_entry(&self, operation_number: usize) -> Option<LogEntry> {
        // The operation number is a 1-based counter. Thus, we need to subtract 1 to get
        // the associated index
        let key = operation_number - 1;
        self.storage.read(key)
    }

    fn since(&self, operation_number: usize) -> Log {
        if operation_number >= self.size {
            return Log::default();
        }

        let mut entries = Vec::new();
        for i in operation_number..self.size() {
            let entry = self
                .storage
                .read(i)
                .expect("Log entry must be inside boundaries");

            entries.push(entry.clone());
        }

        Log { entries }
    }

    fn merge(&mut self, log: Log) {
        for value in log.entries {
            self.add_next(value);
        }
    }

    fn size(&self) -> usize {
        self.size
    }

    fn read_all(&self) -> Log {
        let mut entries = Vec::new();
        for i in 0..self.size() {
            if let Some(entry) = self.storage.read(i) {
                entries.push(entry.clone());
            }
        }
        Log { entries }
    }

    fn overwrite(&mut self, log: Log) {
        self.size = log.entries.len();

        let values = log.entries.into_iter().enumerate();
        self.storage.write_all(values); // TODO: handle this
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct LogEntry {
    /// The request number of the log.
    request_number: u32,
    /// The client ID that triggered the request.
    client_id: ClientId,
    /// Operation of the request
    operation: Operation,
}

#[derive(Debug, PartialEq)]
struct ClientTable {
    entries: HashMap<u128, ClientTableEntry>,
}

impl ClientTable {
    pub fn new() -> Self {
        ClientTable {
            entries: HashMap::new(),
        }
    }

    fn add(&mut self, client_id: u128, entry: ClientTableEntry) {
        self.entries.insert(client_id, entry);
    }

    fn read(&self, client_id: &u128) -> Option<&ClientTableEntry> {
        self.entries.get(client_id)
    }

    fn update_response(
        &mut self,
        client_id: &u128,
        operation: Operation,
    ) -> Option<&ClientTableEntry> {
        let entry = self.entries.get_mut(client_id)?;
        entry.response = Some(operation);
        Some(entry)
    }

    fn extend(&mut self, entries: HashMap<u128, ClientTableEntry>) {
        self.entries.extend(entries);
    }
}

impl FromIterator<(u128, ClientTableEntry)> for ClientTable {
    fn from_iter<T: IntoIterator<Item = (u128, ClientTableEntry)>>(iter: T) -> Self {
        let mut table = ClientTable::new();
        table.extend(iter.into_iter().collect());
        table
    }
}

#[derive(Debug, PartialEq)]
struct ClientTableEntry {
    request_number: u32,
    response: Option<Operation>,
}

#[derive(Debug, PartialEq)]
enum ReplicaStatus {
    Normal,
    ViewChange,
    Recovering { nonce: u64 },
}

#[derive(Debug, PartialEq)]
enum PrepareAck {
    Waiting { replicas: HashSet<u8> },
    Executed,
}

#[derive(Debug)]
struct ViewAck {
    old_view: ReplicaId,
    operation_number: usize,
    commit_number: usize,
    log: Log,
}

impl PartialEq for ViewAck {
    fn eq(&self, other: &Self) -> bool {
        self.old_view == other.old_view && self.operation_number == other.operation_number
    }
}

impl Eq for ViewAck {}

impl PartialOrd for ViewAck {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ViewAck {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.old_view.cmp(&other.old_view).then_with(|| {
            self.operation_number
                .cmp(&other.operation_number)
                .then_with(|| self.commit_number.cmp(&other.commit_number))
        })
    }
}

#[derive(Debug, PartialEq)]
struct RecoveryAck {
    primary: Option<RecoveryPrimaryAck>,
    view: ReplicaId,
}

impl RecoveryAck {
    fn from_message(message: RecoveryResponseMessage) -> Self {
        let primary = message.body.primary.map(|response| RecoveryPrimaryAck {
            log: response.log,
            operation_number: response.operation_number,
            commit_number: response.commit_number,
            view: message.header.view,
        });

        RecoveryAck {
            primary,
            view: message.header.view,
        }
    }
}

#[derive(Debug, PartialEq)]
struct RecoveryPrimaryAck {
    log: Log,
    operation_number: usize,
    commit_number: usize,
    view: ReplicaId,
}

#[derive(Error, Debug)]
pub enum ReplicaError {
    #[error(transparent)]
    IO(#[from] IOError),
    #[error("A replica is not ready to receive requests")]
    NotReady,
    #[error("A replica has an invalid state")]
    InvalidState,
    #[error("View mismatch between client and replica")]
    MessageViewMismatch,
    #[error("Service execution has failed")]
    ServiceExecution,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BinaryHeap, HashMap},
        net::SocketAddr,
        time::Instant,
    };

    use bytes::{Bytes, BytesMut};

    use crate::{
        Operation, ReplicaId, ReplicaOptions, encode_operation,
        io::{AcceptedConnection, IO, SocketLink},
        message::{
            CommitMessage, CommitMessageBody, DoViewChangeMessage, DoViewChangeMessageBody,
            GetStateMessage, GetStateMessageBody, Header, Message, NewStateMessage,
            NewStateMessageBody, PrepareMessage, PrepareMessageBody, PrepareOkMessage,
            PrepareOkMessageBody, RecoveryMessage, RecoveryMessageBody, RecoveryPrimaryResponse,
            RecoveryResponseMessage, RecoveryResponseMessageBody, ReplyMessage, ReplyMessageBody,
            RequestMessage, RequestMessageBody, StartViewChangeMessage, StartViewChangeMessageBody,
            StartViewMessage, StartViewMessageBody,
        },
        replica::{
            ClientTable, ClientTableEntry, HandleOutput, IDLE_TIMEOUT_MS, Log, LogEntry, LogStore,
            OutputAction, PrepareAck, RecoveryAck, RecoveryPrimaryAck, ReplicaStatus, ViewAck,
        },
        storage::InMemoryStorage,
    };

    use super::{Replica, Service, ServiceError};

    struct MockIncomingSocket;

    impl SocketLink for MockIncomingSocket {
        fn peer_addr(&self) -> Result<SocketAddr, std::io::Error> {
            unreachable!()
        }
    }

    struct MockIO;

    impl IO for MockIO {
        type Local = ();
        type Link = MockIncomingSocket;

        fn open_tcp(&self, _: SocketAddr) -> Result<(), crate::io::IOError> {
            unreachable!()
        }

        fn connect(
            &mut self,
            _: SocketAddr,
            _: usize,
        ) -> Result<Option<Self::Link>, crate::io::IOError> {
            unreachable!()
        }

        fn accept(
            &mut self,
            _: &(),
            _: usize,
        ) -> Result<Vec<AcceptedConnection<Self::Link>>, crate::io::IOError> {
            unreachable!()
        }

        fn close(&self, _: &mut Self::Link) -> Result<(), crate::io::IOError> {
            unreachable!()
        }

        fn recv(&self, _: &mut Self::Link, _: &mut BytesMut) -> Result<bool, crate::io::IOError> {
            unreachable!()
        }

        fn write(
            &self,
            _: &mut Self::Link,
            _: &Bytes,
        ) -> Result<Option<usize>, crate::io::IOError> {
            unreachable!()
        }

        fn send(&self, _: &mut Self::Link, _: usize) -> Result<(), crate::io::IOError> {
            unreachable!()
        }

        fn run(
            &mut self,
            _: std::time::Duration,
        ) -> Result<Vec<crate::io::Completion>, crate::io::IOError> {
            unreachable!()
        }
    }

    struct MockService;

    impl Service for MockService {
        type Input = usize;
        type Output = usize;

        fn execute(&self, input: Self::Input) -> Result<Self::Output, ServiceError> {
            Ok(input + 1)
        }
    }

    struct MockReplicaBuilder {
        replica_number: ReplicaId,
        addresses: Vec<String>,
    }

    impl MockReplicaBuilder {
        fn new() -> Self {
            MockReplicaBuilder::default()
        }

        fn backup(mut self) -> Self {
            self.replica_number = 1;
            self
        }

        fn addresses(mut self, addresses: Vec<String>) -> Self {
            self.addresses = addresses;
            self
        }

        fn build(self) -> Replica<MockService, InMemoryStorage, MockIO> {
            let total = self.addresses.len();
            assert!(total > 0);

            let options = ReplicaOptions {
                seed: 1234,
                current: self.replica_number,
                addresses: self.addresses,
            };

            let config = options.parse().unwrap();

            Replica::new(&config, MockService, InMemoryStorage::new(), MockIO)
        }
    }

    impl Default for MockReplicaBuilder {
        fn default() -> Self {
            MockReplicaBuilder {
                replica_number: 0,
                addresses: vec![
                    "127.0.0.1:3000".to_string(),
                    "127.0.0.1:3001".to_string(),
                    "127.0.0.1:3002".to_string(),
                ],
            }
        }
    }

    fn usize_as_bytes(value: usize) -> Operation {
        encode_operation(value).unwrap()
    }

    #[test]
    fn test_handle_request() {
        // given
        let operation = usize_as_bytes(0);
        let request = Message::Request(RequestMessage {
            header: Header { view: 0 },
            body: RequestMessageBody {
                request_number: 1,
                client_id: 1,
                operation: operation.clone(),
            },
        });
        let mut replica = MockReplicaBuilder::new().build();

        // when
        let result = replica.handle_message(request.clone()).unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::broadcast(Message::Prepare(PrepareMessage {
                header: Header { view: 0 },
                body: PrepareMessageBody {
                    operation_number: 1,
                    commit_number: 0,
                    request: RequestMessage {
                        header: Header { view: 0 },
                        body: RequestMessageBody {
                            request_number: 1,
                            client_id: 1,
                            operation: operation.clone()
                        },
                    },
                }
            }))
        );

        // state is correct
        assert_eq!(replica.replica_number, 0);
        assert_eq!(replica.view, 0);
        assert_eq!(replica.total, 3);
        assert_eq!(replica.operation_number, 1);
        assert_eq!(replica.commit_number, 0);

        // log is correct
        let mut log = LogStore::new(InMemoryStorage::new());
        log.append(operation, 1, 1);
        assert_eq!(replica.log.read_all(), log.read_all());

        // client table is correct
        let mut client_table = ClientTable::new();
        client_table.add(
            1,
            ClientTableEntry {
                request_number: 1,
                response: None,
            },
        );
        assert_eq!(replica.client_table, client_table);
    }

    #[test]
    fn test_handle_request_returns_most_recent_response() {
        // given
        let operation = usize_as_bytes(0);
        let request = Message::Request(RequestMessage {
            header: Header { view: 0 },
            body: RequestMessageBody {
                request_number: 1,
                client_id: 1,
                operation,
            },
        });
        let mut replica = MockReplicaBuilder::new().build();
        replica.client_table = ClientTable::from_iter([(
            1,
            ClientTableEntry {
                request_number: 1,
                response: Some(usize_as_bytes(1)),
            },
        )]);

        // when
        let result = replica.handle_message(request.clone()).unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::send_client(
                ReplyMessage {
                    header: Header { view: 0 },
                    body: ReplyMessageBody {
                        request_number: 1,
                        result: usize_as_bytes(1)
                    },
                },
                1
            )
        );
    }

    #[test]
    fn test_handle_request_backup_replica_ignores() {
        // given
        let operation = usize_as_bytes(0);
        let request = Message::Request(RequestMessage {
            header: Header { view: 1 },
            body: RequestMessageBody {
                request_number: 1,
                client_id: 1,
                operation,
            },
        });
        let mut replica = MockReplicaBuilder::new().backup().build();

        // when
        let result = replica.handle_message(request).unwrap();

        // then
        assert_eq!(result, HandleOutput::DoNothing);
    }

    #[test]
    fn test_handle_request_invalid_view_sent_by_client() {
        // given
        let operation = usize_as_bytes(0);
        let request = Message::Request(RequestMessage {
            header: Header { view: 1 },
            body: RequestMessageBody {
                request_number: 1,
                client_id: 1,
                operation,
            },
        });
        let mut replica = MockReplicaBuilder::new().build();

        // when
        let result = replica.handle_message(request.clone());

        // then
        assert!(result.is_err());
    }

    #[test]
    fn test_handle_prepare() {
        // given
        let operation = usize_as_bytes(0);
        let prepare = Message::Prepare(PrepareMessage {
            header: Header { view: 0 },
            body: PrepareMessageBody {
                operation_number: 1,
                commit_number: 0,
                request: RequestMessage {
                    header: Header { view: 0 },
                    body: RequestMessageBody {
                        request_number: 1,
                        client_id: 1,
                        operation: operation.clone(),
                    },
                },
            },
        });
        let mut replica = MockReplicaBuilder::new().backup().build();

        // when
        let result = replica.handle_message(prepare).unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::send(
                Message::PrepareOk(PrepareOkMessage {
                    header: Header { view: 0 },
                    body: PrepareOkMessageBody {
                        operation_number: 1,
                        replica: 1,
                        client_id: 1
                    },
                }),
                0
            )
        );

        // state is correct
        assert_eq!(replica.operation_number, 1);
        assert_eq!(replica.commit_number, 0);

        // log is correct
        let mut log = LogStore::new(InMemoryStorage::new());
        log.append(operation, 1, 1);
        assert_eq!(replica.log.read_all(), log.read_all());

        // client table is correct
        let mut client_table = ClientTable::new();
        client_table.add(
            1,
            ClientTableEntry {
                request_number: 1,
                response: None,
            },
        );
        assert_eq!(replica.client_table, client_table);
    }

    #[test]
    fn test_handle_prepare_triggers_state_transfer() {
        // given
        let operation = usize_as_bytes(1);
        let prepare = Message::Prepare(PrepareMessage {
            header: Header { view: 0 },
            body: PrepareMessageBody {
                operation_number: 2,
                commit_number: 1,
                request: RequestMessage {
                    header: Header { view: 0 },
                    body: RequestMessageBody {
                        request_number: 2,
                        client_id: 1,
                        operation: operation.clone(),
                    },
                },
            },
        });
        let mut replica = MockReplicaBuilder::new().backup().build();
        replica.log.append(operation, 1, 1);

        // when
        let result = replica.handle_message(prepare).unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::send(
                Message::GetState(GetStateMessage {
                    header: Header { view: 0 },
                    body: GetStateMessageBody {
                        operation_number: 0,
                        replica: 1
                    },
                }),
                2
            )
        );
    }

    #[test]
    fn test_handle_prepare_ok() {
        // given
        let prepare_ok = Message::PrepareOk(PrepareOkMessage {
            header: Header { view: 0 },
            body: PrepareOkMessageBody {
                operation_number: 1,
                replica: 1,
                client_id: 1,
            },
        });
        let mut replica = MockReplicaBuilder::new().build();
        let operation = usize_as_bytes(0);
        replica.log.append(operation, 1, 1);
        replica.client_table.add(
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
        let result = replica.handle_message(prepare_ok).unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::send_client(
                ReplyMessage {
                    header: Header { view: 0 },
                    body: ReplyMessageBody {
                        request_number: 1,
                        result: usize_as_bytes(1),
                    }
                },
                1
            )
        );

        // state is correct
        assert_eq!(replica.commit_number, 1);
        assert_eq!(
            replica.prepare_acks,
            HashMap::from_iter([(1, PrepareAck::Executed)])
        );

        // client table is correct
        let mut client_table = ClientTable::new();
        client_table.add(
            1,
            ClientTableEntry {
                request_number: 1,
                response: Some(usize_as_bytes(1)),
            },
        );
        assert_eq!(replica.client_table, client_table);
    }

    #[test]
    fn test_handle_prepare_ok_waits_for_quorum() {
        // given
        let prepare_ok = Message::PrepareOk(PrepareOkMessage {
            header: Header { view: 0 },
            body: PrepareOkMessageBody {
                operation_number: 1,
                replica: 1,
                client_id: 1,
            },
        });
        let mut replica = MockReplicaBuilder::new()
            .addresses(vec![
                "127.0.0.1:3000".to_string(),
                "127.0.0.1:3001".to_string(),
                "127.0.0.1:3002".to_string(),
                "127.0.0.1:3003".to_string(),
                "127.0.0.1:3004".to_string(),
            ])
            .build();
        let operation = usize_as_bytes(0);
        replica.log.append(operation, 1, 1);
        replica.client_table.add(
            1,
            ClientTableEntry {
                request_number: 1,
                response: None,
            },
        );

        // when
        let result = replica.handle_message(prepare_ok).unwrap();

        // then
        assert_eq!(result, HandleOutput::DoNothing);

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
    }

    #[test]
    fn test_handle_commit() {
        // given
        let mut replica = MockReplicaBuilder::new().backup().build();
        let commit = Message::Commit(CommitMessage {
            header: Header { view: 0 },
            body: CommitMessageBody {
                replica: replica.replica_number,
                operation_number: 2,
            },
        });
        replica.operation_number = 2;
        replica.commit_number = 0;
        replica.log.append(usize_as_bytes(0), 1, 1);
        replica.log.append(usize_as_bytes(1), 2, 1);

        // when
        let result = replica.handle_message(commit).unwrap();

        // then
        assert_eq!(result, HandleOutput::DoNothing);

        // state is correct
        assert_eq!(replica.commit_number, 2);
    }

    #[test]
    fn test_handle_commit_trigger_state_transfer() {
        // given
        let mut replica = MockReplicaBuilder::new().backup().build();
        let commit = Message::Commit(CommitMessage {
            header: Header { view: 0 },
            body: CommitMessageBody {
                replica: replica.replica_number,
                operation_number: 2,
            },
        });
        replica.operation_number = 0;
        replica.commit_number = 0;

        // when
        let result = replica.handle_message(commit).unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::send(
                Message::GetState(GetStateMessage {
                    header: Header { view: 0 },
                    body: GetStateMessageBody {
                        operation_number: 0,
                        replica: 1
                    },
                }),
                2
            )
        );

        // state is correct
        assert_eq!(replica.commit_number, 0);
    }

    #[test]
    fn test_handle_get_state() {
        // given
        let get_state = Message::GetState(GetStateMessage {
            header: Header { view: 0 },
            body: GetStateMessageBody {
                operation_number: 1,
                replica: 2,
            },
        });
        let mut replica = MockReplicaBuilder::new().backup().build();
        replica.operation_number = 3;
        replica.commit_number = 2;
        replica.log.append(usize_as_bytes(0), 1, 1);
        replica.log.append(usize_as_bytes(1), 2, 1);
        replica.log.append(usize_as_bytes(2), 3, 1);

        // when
        let result = replica.handle_message(get_state).unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::send(
                Message::NewState(NewStateMessage {
                    header: Header { view: 0 },
                    body: NewStateMessageBody {
                        replica: 1,
                        log_after_operation: Log {
                            entries: Vec::from([
                                LogEntry {
                                    request_number: 2,
                                    client_id: 1,
                                    operation: usize_as_bytes(1),
                                },
                                LogEntry {
                                    request_number: 3,
                                    client_id: 1,
                                    operation: usize_as_bytes(2),
                                },
                            ]),
                        },
                        operation_number: 3,
                        commit_number: 2,
                    }
                }),
                2
            )
        );
    }

    #[test]
    fn test_handle_new_state() {
        // given
        let new_state = Message::NewState(NewStateMessage {
            header: Header { view: 0 },
            body: NewStateMessageBody {
                replica: 0,
                log_after_operation: Log {
                    entries: Vec::from([
                        LogEntry {
                            request_number: 2,
                            client_id: 1,
                            operation: usize_as_bytes(1),
                        },
                        LogEntry {
                            request_number: 3,
                            client_id: 1,
                            operation: usize_as_bytes(2),
                        },
                    ]),
                },
                operation_number: 3,
                commit_number: 2,
            },
        });
        let mut replica = MockReplicaBuilder::new().backup().build();
        replica.log.append(usize_as_bytes(0), 1, 1);
        replica.operation_number = 1;

        // when
        let result = replica.handle_message(new_state).unwrap();

        // then
        assert_eq!(result, HandleOutput::DoNothing);

        // state is correct
        assert_eq!(replica.operation_number, 3);
        assert_eq!(replica.commit_number, 2);

        // log is correct
        let mut log = LogStore::new(InMemoryStorage::new());
        log.append(usize_as_bytes(0), 1, 1);
        log.append(usize_as_bytes(1), 2, 1);
        log.append(usize_as_bytes(2), 3, 1);
        assert_eq!(replica.log.read_all(), log.read_all());
    }

    #[test]
    fn triggers_view_change() {
        // given
        let mut replica = MockReplicaBuilder::new().backup().build();
        replica.committed_at = Instant::now() - (IDLE_TIMEOUT_MS * 2);

        // when
        let result = replica.periodic().unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::broadcast(Message::StartViewChange(StartViewChangeMessage {
                header: Header { view: 1 },
                body: StartViewChangeMessageBody {
                    new_view: 1,
                    replica: 1
                },
            }))
        );

        // state is correct
        assert_eq!(replica.status, ReplicaStatus::ViewChange);
    }

    #[test]
    fn triggers_view_change_respects_replica_status() {
        // given
        let mut replica = MockReplicaBuilder::new().backup().build();
        replica.view = 0;
        replica.committed_at = Instant::now() - (IDLE_TIMEOUT_MS * 2);
        replica.status = ReplicaStatus::ViewChange;

        // when
        let result = replica.periodic().unwrap();

        // then
        assert_eq!(result, HandleOutput::DoNothing);

        // state is correct
        assert_eq!(replica.status, ReplicaStatus::ViewChange);
    }

    #[test]
    fn test_handle_start_view_change() {
        // given
        let start_view_change = Message::StartViewChange(StartViewChangeMessage {
            header: Header { view: 1 },
            body: StartViewChangeMessageBody {
                new_view: 1,
                replica: 1,
            },
        });
        let mut replica = MockReplicaBuilder::new().build();
        replica.operation_number = 1;
        replica.log.append(usize_as_bytes(0), 1, 1);

        // when
        let result = replica.handle_message(start_view_change).unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::send(
                Message::DoViewChange(DoViewChangeMessage {
                    header: Header { view: 1 },
                    body: DoViewChangeMessageBody {
                        old_view: 0,
                        new_view: 1,
                        log: Log {
                            entries: vec![LogEntry {
                                request_number: 1,
                                client_id: 1,
                                operation: usize_as_bytes(0),
                            }],
                        },
                        operation_number: 1,
                        commit_number: 0,
                        replica: 0,
                    }
                }),
                1
            )
        );

        // state is correct
        assert_eq!(replica.view, 1);
        assert_eq!(replica.status, ReplicaStatus::ViewChange);
    }

    #[test]
    fn test_handle_start_view_change_new_primary_acknowledges() {
        // given
        let start_view_change = Message::StartViewChange(StartViewChangeMessage {
            header: Header { view: 1 },
            body: StartViewChangeMessageBody {
                new_view: 1,
                replica: 1,
            },
        });
        let mut replica = MockReplicaBuilder::new().backup().build();
        replica.view = 0;
        replica.operation_number = 1;
        replica.log.append(usize_as_bytes(0), 1, 1);

        // when
        let result = replica.handle_message(start_view_change).unwrap();

        // then
        assert_eq!(result, HandleOutput::DoNothing);

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
                        operation: usize_as_bytes(0),
                    }],
                },
            }]
        );
    }

    #[test]
    fn test_handle_do_view_change() {
        // given
        let message_log = Log {
            entries: vec![
                LogEntry {
                    request_number: 1,
                    client_id: 1,
                    operation: usize_as_bytes(0),
                },
                LogEntry {
                    request_number: 2,
                    client_id: 1,
                    operation: usize_as_bytes(1),
                },
            ],
        };
        let do_view_change = Message::DoViewChange(DoViewChangeMessage {
            header: Header { view: 1 },
            body: DoViewChangeMessageBody {
                old_view: 0,
                new_view: 1,
                log: message_log.clone(),
                operation_number: 2,
                commit_number: 1,
                replica: 0,
            },
        });

        let mut replica = MockReplicaBuilder::new().backup().build();
        replica.view_acks = BinaryHeap::from([ViewAck {
            old_view: 0,
            operation_number: 1,
            commit_number: 0,
            log: Log {
                entries: vec![LogEntry {
                    request_number: 1,
                    client_id: 1,
                    operation: usize_as_bytes(0),
                }],
            },
        }]);
        replica.operation_number = 1;
        replica.log.append(usize_as_bytes(0), 1, 1);

        // when
        let result = replica.handle_message(do_view_change).unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::Actions(vec![
                OutputAction::SendClient {
                    reply: ReplyMessage {
                        header: Header { view: 1 },
                        body: ReplyMessageBody {
                            request_number: 2,
                            result: usize_as_bytes(2),
                        }
                    },
                    client_id: 1
                },
                OutputAction::Broadcast {
                    message: Message::StartView(StartViewMessage {
                        header: Header { view: 1 },
                        body: StartViewMessageBody {
                            replica: 1,
                            log: message_log.clone(),
                            operation_number: 2,
                            commit_number: 1,
                        }
                    })
                }
            ])
        );

        // state is correct
        assert_eq!(replica.operation_number, 2);
        assert_eq!(replica.log.read_all(), message_log.clone());
        assert_eq!(replica.commit_number, 1);
        assert_eq!(
            replica.client_table,
            ClientTable::from_iter([(
                1,
                ClientTableEntry {
                    request_number: 2,
                    response: Some(usize_as_bytes(2))
                }
            )])
        );
    }

    #[test]
    fn test_handle_do_view_change_waits_for_quorum() {
        // given
        let message_log = Log {
            entries: vec![
                LogEntry {
                    request_number: 1,
                    client_id: 1,
                    operation: usize_as_bytes(0),
                },
                LogEntry {
                    request_number: 2,
                    client_id: 1,
                    operation: usize_as_bytes(1),
                },
            ],
        };
        let do_view_change = Message::DoViewChange(DoViewChangeMessage {
            header: Header { view: 1 },
            body: DoViewChangeMessageBody {
                old_view: 0,
                new_view: 1,
                log: message_log.clone(),
                operation_number: 2,
                commit_number: 1,
                replica: 0,
            },
        });

        let mut replica = MockReplicaBuilder::new()
            .backup()
            .addresses(vec![
                "127.0.0.1:3000".to_string(),
                "127.0.0.1:3001".to_string(),
                "127.0.0.1:3002".to_string(),
                "127.0.0.1:3003".to_string(),
                "127.0.0.1:3004".to_string(),
            ])
            .build();

        // when
        let result = replica.handle_message(do_view_change).unwrap();

        // then
        assert_eq!(result, HandleOutput::DoNothing);

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
    }

    #[test]
    fn test_handle_start_view() {
        // given
        let message_log = Log {
            entries: vec![
                LogEntry {
                    request_number: 1,
                    client_id: 1,
                    operation: usize_as_bytes(0),
                },
                LogEntry {
                    request_number: 2,
                    client_id: 1,
                    operation: usize_as_bytes(1),
                },
            ],
        };

        let mut replica = MockReplicaBuilder::new().build();
        replica.view = 0;
        replica.operation_number = 1;
        replica.log.append(usize_as_bytes(0), 1, 1);

        let start_view = Message::StartView(StartViewMessage {
            header: Header { view: 1 },
            body: StartViewMessageBody {
                replica: replica.replica_number,
                log: message_log.clone(),
                operation_number: 2,
                commit_number: 1,
            },
        });

        // when
        let result = replica.handle_message(start_view).unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::send(
                Message::PrepareOk(PrepareOkMessage {
                    header: Header { view: 1 },
                    body: PrepareOkMessageBody {
                        operation_number: 2,
                        replica: 0,
                        client_id: 1
                    },
                }),
                1
            )
        );

        // state is correct
        assert_eq!(replica.status, ReplicaStatus::Normal);
        assert_eq!(replica.view, 1);
        assert_eq!(replica.log.read_all(), message_log.clone());
        assert_eq!(replica.operation_number, 2);
        assert_eq!(replica.commit_number, 1);
        assert_eq!(
            replica.client_table,
            ClientTable::from_iter([(
                1,
                ClientTableEntry {
                    request_number: 2,
                    response: Some(usize_as_bytes(2))
                }
            )])
        );
    }

    #[test]
    fn test_trigger_recovery() {
        // given
        let mut replica = MockReplicaBuilder::new().backup().build();

        // when
        let result = replica.trigger_recovery();

        // then
        let HandleOutput::Actions(actions) = result else {
            panic!("Unexpected handle output")
        };
        assert_eq!(actions.len(), 1);

        let OutputAction::Broadcast { message } = actions.first().unwrap() else {
            panic!("unexpected actions")
        };

        let Message::Recovery(RecoveryMessage {
            body:
                RecoveryMessageBody {
                    replica: replica_number,
                    ..
                },
            ..
        }) = message
        else {
            panic!("unexpected message")
        };
        assert_eq!(replica_number, &1);

        // state is correct
        assert!(matches!(replica.status, ReplicaStatus::Recovering { .. }));
    }

    #[test]
    fn handles_recovery() {
        // given
        let recovery = Message::Recovery(RecoveryMessage {
            header: Header { view: 0 },
            body: RecoveryMessageBody {
                replica: 1,
                nonce: 1234,
            },
        });
        let mut replica = MockReplicaBuilder::new().build();
        replica.view = 0;
        replica.commit_number = 1;
        replica.operation_number = 2;
        replica.log.append(usize_as_bytes(0), 1, 1);
        replica.log.append(usize_as_bytes(1), 2, 1);

        // when
        let result = replica.handle_message(recovery).unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::send(
                Message::RecoveryResponse(RecoveryResponseMessage {
                    header: Header { view: 0 },
                    body: RecoveryResponseMessageBody {
                        replica: 0,
                        nonce: 1234,
                        primary: Some(RecoveryPrimaryResponse {
                            log: replica.log.read_all(),
                            operation_number: 2,
                            commit_number: 1
                        })
                    }
                }),
                1
            )
        );
    }

    #[test]
    fn test_handle_recovery_backup_replica() {
        // given
        let recovery = Message::Recovery(RecoveryMessage {
            header: Header { view: 0 },
            body: RecoveryMessageBody {
                replica: 1,
                nonce: 1234,
            },
        });
        let mut replica = MockReplicaBuilder::new().backup().build();
        replica.view = 0;
        replica.commit_number = 1;
        replica.operation_number = 2;
        replica.log.append(usize_as_bytes(0), 1, 1);
        replica.log.append(usize_as_bytes(1), 2, 1);

        // when
        let result = replica.handle_message(recovery).unwrap();

        // then
        assert_eq!(
            result,
            HandleOutput::send(
                Message::RecoveryResponse(RecoveryResponseMessage {
                    header: Header { view: 0 },
                    body: RecoveryResponseMessageBody {
                        replica: 1,
                        nonce: 1234,
                        primary: None
                    }
                }),
                1
            )
        );
    }

    #[test]
    fn test_handle_recovery_response() {
        // given
        let log = Log {
            entries: vec![
                LogEntry {
                    request_number: 1,
                    client_id: 1,
                    operation: usize_as_bytes(0),
                },
                LogEntry {
                    request_number: 2,
                    client_id: 1,
                    operation: usize_as_bytes(1),
                },
            ],
        };

        let recovery_response = Message::RecoveryResponse(RecoveryResponseMessage {
            header: Header { view: 2 },
            body: RecoveryResponseMessageBody {
                replica: 0,
                nonce: 1234,
                primary: Some(RecoveryPrimaryResponse {
                    log: log.clone(),
                    operation_number: 2,
                    commit_number: 1,
                }),
            },
        });
        let mut replica = MockReplicaBuilder::new().backup().build();
        replica.status = ReplicaStatus::Recovering { nonce: 1234 };
        replica.recovery_acks.push(RecoveryAck {
            primary: Some(RecoveryPrimaryAck {
                log: Log {
                    entries: vec![LogEntry {
                        request_number: 1,
                        client_id: 1,
                        operation: usize_as_bytes(0),
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
        let result = replica.handle_message(recovery_response).unwrap();

        // then
        assert_eq!(result, HandleOutput::DoNothing);

        // state is correct
        assert_eq!(replica.view, 2);
        assert_eq!(replica.operation_number, 2);
        assert_eq!(replica.log.read_all(), log);
        assert_eq!(replica.commit_number, 1);
        assert_eq!(
            replica.client_table,
            ClientTable::from_iter([(
                1,
                ClientTableEntry {
                    request_number: 1,
                    response: Some(usize_as_bytes(1))
                }
            )])
        );
        assert_eq!(replica.status, ReplicaStatus::Normal);
    }

    #[test]
    fn test_handle_recovery_response_waits_for_quorum() {
        // given
        let log = Log {
            entries: vec![
                LogEntry {
                    request_number: 1,
                    client_id: 1,
                    operation: usize_as_bytes(0),
                },
                LogEntry {
                    request_number: 2,
                    client_id: 1,
                    operation: usize_as_bytes(1),
                },
            ],
        };

        let recovery_response = Message::RecoveryResponse(RecoveryResponseMessage {
            header: Header { view: 2 },
            body: RecoveryResponseMessageBody {
                replica: 0,
                nonce: 1234,
                primary: Some(RecoveryPrimaryResponse {
                    log: log.clone(),
                    operation_number: 2,
                    commit_number: 1,
                }),
            },
        });
        let mut replica = MockReplicaBuilder::new().backup().build();
        replica.status = ReplicaStatus::Recovering { nonce: 1234 };

        // when
        let result = replica.handle_message(recovery_response).unwrap();

        // then
        assert_eq!(result, HandleOutput::DoNothing);

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
    }

    #[test]
    fn test_handle_recovery_response_waits_for_primary_ack() {
        // given
        let mut log = LogStore::new(InMemoryStorage::new());
        log.append(usize_as_bytes(0), 1, 1);
        log.append(usize_as_bytes(1), 2, 1);

        let recovery_response = Message::RecoveryResponse(RecoveryResponseMessage {
            header: Header { view: 2 },
            body: RecoveryResponseMessageBody {
                replica: 0,
                nonce: 1234,
                primary: None,
            },
        });
        let mut replica = MockReplicaBuilder::new().backup().build();
        replica.status = ReplicaStatus::Recovering { nonce: 1234 };
        replica.recovery_acks.push(RecoveryAck {
            primary: None,
            view: 2,
        });

        // when
        let result = replica.handle_message(recovery_response).unwrap();

        // then
        assert_eq!(result, HandleOutput::DoNothing);

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
    }
}
