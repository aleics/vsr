use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
};

use crossbeam::channel::{Receiver, Sender, unbounded};
use thiserror::Error;

/// VSR (Viewstamped Replication Revisited)
/// * Normal protocol
/// * View change protocol
/// * Recovery protocol
///
/// Primary is determined by the view number and the configuration.
/// The replicas are numbered starting with replica 1, which is the initial primary.
/// Replicas have an internal state to describe their availability.
/// State *normal* means the replica is available.
///
/// The client-proxy knows who the primary is and sends messages accordingly.
/// All requests sent by the client are given a number to understand ordering.
/// The client also sends its current known view number together with the message.
/// The primary replica can check that the view number matches with the one known by the client.
/// If the client is behind, the receiver drops the message.
/// If the client is ahead, the replica performs a "state transfer":
///     - it requests information it is missing from the other replicas and uses this information to bring itself up to date before processing the message.
///
/// Normal protocol
///  1. Client sends a message <Request, operation (with arguments), client_id, request_number>.
///  2. When the primary receives the message it compares the request number to the internal client_table.
///     a. If the request number is smaller, the table drops the request.
///     b. If it's equal it will re-send the same response that has been executed previously.
///     c. If the request number is bigger, (3)
///  3. The primary advances the operation_number, adds the request to the end of the log and updates the information for this client.
///     Then, it sends a <Prepare, view_number, message_client, request_operation_number, commit_number>
///  4. The backup replicas process the Prepare messages in order:
///     - A backup replica won't accept a prepare with an operation_number until it has entries for all earlier requests in its log.
///     Once all previous requests are available:
///     a. The operation_number is increased
///     b. The request is added to the end of its log
///     c. Updates the client's information in the client table
///     d. Sends a <PrepareOk, view_number, operation_number, replica_number> message to the primary to indicate that this operation has been processed.
///  5. Primary waits for `f`  PrepareOk messages from the different backup replicas. Once that happens, the operation is considered to be successful and thus marked as committed. Then, after it has executed all earlier operations
///     a. The primary executes the operation by making an up-call to the service code.
///     b. Increments its commit_number
///     c. Sends a <Reply, view_number, client_request_id, payload>.
///     d. The primary updates the client's table to contain the payload.
///  6. The primary informs the backup replicas about the commit when it sends the next Prepare message. However, if the primary does not receive a new client request in a while, it pro-actively informs the backup replicas about the latest commit with a message <Commit, view_number, commit_number>
///  7. When a backup learns of a commit, it waits until it has the request in its log and until it has executed all earlier operations. Then it executes the operation by performing the up-call to the service code, increments its commit-number, updates the client's entry in the client table, but does not send the reply to the client.
///  8. If a client doesn't receive a timeline response to a request, it re-sends the request to all replicas. This way if the grouped has moved to a later view, its message will reach the new primary. Backups ignore client requests; only the primary processes them.

/// A single replica
struct Replica {
    /// The current replica number given the configuration
    replica_number: usize,

    /// Internal view number of the replica
    view: usize,

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
    client_table: ClientTable,

    network: ReplicaNetwork,
}

impl Replica {
    fn new(replica_number: usize, total: usize, network: ReplicaNetwork) -> Self {
        Replica {
            replica_number,
            view: 0,
            total,
            status: ReplicaStatus::Normal,
            operation_number: 0,
            log: Log::default(),
            commit_number: 0,
            acks: HashMap::default(),
            client_table: ClientTable::default(),
            network,
        }
    }

    fn handle_message(&mut self, message: Message) -> Result<(), ReplicaError> {
        println!(
            "Received message {:?} in replica {}",
            message, self.replica_number
        );

        match message {
            Message::Request {
                view,
                request_number,
                client_id,
            } => self.handle_request(view, request_number, client_id),
            Message::Prepare {
                view,
                operation_number,
                client_id,
            } => self.handle_prepare(view, operation_number, client_id),
            Message::PrepareOk {
                view,
                operation_number,
                replica_number,
                client_id,
            } => self.handle_prepare_ok(view, operation_number, replica_number, client_id),
            Message::Commit {
                view,
                operation_number,
            } => self.handle_commit(view, operation_number),
            Message::Reply { .. } => Ok(()),
        }
    }

    fn handle_request(
        &mut self,
        view: usize,
        request_number: usize,
        client_id: usize,
    ) -> Result<(), ReplicaError> {
        // Backup replicas ignore client request message.
        if self.view != view {
            return Ok(());
        }

        // TODO: If the request-number s isn’t bigger than the information in the table it drops the
        // request, but it will re-send the response if the request is the most recent one from this
        // client and it has already been executed.

        let message = Message::Prepare {
            view,
            operation_number: self.operation_number,
            client_id,
        };

        // Advance the operation number and add the request to the end of the log
        self.operation_number += 1;
        self.log.append(request_number);

        // Broadcast message to the backup replicas
        self.network.broadcast(&message)
    }

    fn handle_prepare(
        &mut self,
        view: usize,
        operation_number: usize,
        client_id: usize,
    ) -> Result<(), ReplicaError> {
        if self.operation_number < operation_number {
            // TODO: waits until it has entries in its log for all earlier requests
            // (doing state transfer if necessary to get the missing information).
            return Ok(());
        }

        // The operation has already been processed by the replica. The message will be ignored.
        if self.operation_number > operation_number {
            return Ok(());
        }

        let message = Message::PrepareOk {
            view,
            operation_number,
            replica_number: self.replica_number,
            client_id,
        };

        // Advance the operation number and add the request to the end of the log
        self.operation_number += 1;
        // self.log.append(request_number); // TODO: we should have the request number in the prepare message?

        self.network.send(message, &self.view)
    }

    fn handle_prepare_ok(
        &mut self,
        view: usize,
        operation_number: usize,
        replica_number: usize,
        client_id: usize,
    ) -> Result<(), ReplicaError> {
        if self.view != view {
            return Ok(());
        }

        let ack = self.acks.entry(operation_number).or_insert(Ack::Waiting {
            replicas: HashSet::with_capacity(self.total),
        });

        let ack_replicas = match ack {
            Ack::Waiting { replicas } => replicas,
            Ack::Executed => return Ok(()),
        };

        ack_replicas.insert(replica_number);

        if ack_replicas.len() < quorum(self.total) {
            return Ok(());
        };

        self.execute(view, operation_number, client_id)?;

        self.acks.insert(operation_number, Ack::Executed);

        Ok(())
    }

    fn handle_commit(&mut self, view: usize, operation_number: usize) -> Result<(), ReplicaError> {
        self.commit_number = operation_number;

        Ok(())
    }

    fn execute(
        &mut self,
        view: usize,
        operation_number: usize,
        client_id: usize,
    ) -> Result<(), ReplicaError> {
        self.commit_number = operation_number;

        // TODO: here we run the service logic and build response

        let message = Message::Reply { view };

        self.network.client.send_out(message, client_id)

        // TODO: The primary also updates the client’s entry in the client-table to contain the result.
    }

    fn tick(&mut self) {
        while !self.network.client.incoming.1.is_empty() {
            let message = match self.network.client.incoming.1.recv().unwrap() {
                ClientMessage::Request {
                    view,
                    request_number,
                    client_id,
                } => Message::Request {
                    view,
                    request_number,
                    client_id,
                },
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

#[derive(Debug, Clone)]
enum Message {
    Request {
        view: usize,
        request_number: usize,
        client_id: usize,
    },
    Prepare {
        view: usize,
        operation_number: usize,
        client_id: usize,
    },
    PrepareOk {
        view: usize,
        operation_number: usize,
        replica_number: usize,
        client_id: usize,
    },
    Commit {
        view: usize,
        operation_number: usize,
    },
    Reply {
        view: usize,
    },
}

#[derive(Debug, Clone)]
enum ClientMessage {
    Request {
        view: usize,
        request_number: usize,
        client_id: usize,
    },
    Response {
        view: usize,
    },
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

#[derive(Debug, Default)]
struct ClientTable {
    /// A key-value map connecting a `client_id` to its most recent request.
    inner: HashMap<usize, ClientTableEntry>,
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

#[derive(Clone)]
struct ClientConnection {
    /// Incoming messages from the client to the replica. The sender is used by the client to connect to the replica.
    /// The receiver is used to receive messages from the client
    incoming: (Sender<ClientMessage>, Receiver<ClientMessage>),

    /// Outgoing messages from the replica to the client.
    outgoing: Vec<Sender<Message>>,
}

impl ClientConnection {
    fn new() -> Self {
        ClientConnection {
            incoming: unbounded(),
            outgoing: Vec::new(),
        }
    }

    fn attach(&mut self) -> (usize, (Sender<ClientMessage>, Receiver<Message>)) {
        let outgoing = unbounded();

        let client_id = self.outgoing.len();
        self.outgoing.push(outgoing.0);

        (client_id, (self.incoming.0.clone(), outgoing.1))
    }

    fn send_out(&self, message: Message, client_id: usize) -> Result<(), ReplicaError> {
        let Some(sender) = self.outgoing.get(client_id) else {
            return Err(ReplicaError::NetworkError);
        };

        sender.send(message).map_err(|_| ReplicaError::NetworkError)
    }
}

struct ReplicaNetwork {
    client: ClientConnection,
    incoming: Receiver<Message>,
    other: HashMap<usize, Sender<Message>>,
}

impl ReplicaNetwork {
    fn for_replica(
        replica: usize,
        client: ClientConnection,
        channels: &Vec<(Sender<Message>, Receiver<Message>)>,
    ) -> Self {
        let mut other = HashMap::with_capacity(channels.len() - 1);
        for (i, (tx, _)) in channels.iter().enumerate() {
            if i != replica {
                other.insert(i, tx.clone());
            }
        }

        let incoming = channels
            .get(replica)
            .expect("Replica index not found in the network channels")
            .1
            .clone();

        ReplicaNetwork {
            client,
            incoming,
            other,
        }
    }

    fn send(&self, message: Message, replica: &usize) -> Result<(), ReplicaError> {
        let sender = self
            .other
            .get(replica)
            .expect("Could not send message: replica not found in the network");

        sender.send(message).map_err(|_| ReplicaError::NetworkError)
    }

    fn broadcast(&self, message: &Message) -> Result<(), ReplicaError> {
        for (_, sender) in &self.other {
            sender
                .send(message.clone())
                .map_err(|_| ReplicaError::NetworkError)?;
        }
        Ok(())
    }
}

pub struct Config {
    pub addresses: Vec<String>,
}

pub struct Cluster {
    replicas: Vec<Replica>,
}

impl Cluster {
    pub fn new(config: &Config) -> Self {
        let total = config.addresses.len();

        let mut channels = Vec::with_capacity(total);
        for _ in &config.addresses {
            channels.push(unbounded::<Message>())
        }

        let client = ClientConnection::new();

        let mut replicas = Vec::with_capacity(total);
        for (i, _) in config.addresses.iter().enumerate() {
            replicas.push(Replica::new(
                i,
                total,
                ReplicaNetwork::for_replica(i, client.clone(), &channels),
            ));
        }

        println!("Cluster created {} replicas.", replicas.len());

        Cluster { replicas }
    }

    pub fn handshake(&mut self) -> Client {
        let primary = self.primary();

        let replica = self
            .replicas
            .get_mut(primary)
            .expect("Primary index not valid");

        let (client_id, channel) = replica.network.client.attach();

        Client::new(client_id, replica.view, channel)
    }

    fn primary(&self) -> usize {
        let total = self.replicas.len();
        let majority = quorum(total) + 1;

        assert!(!self.replicas.is_empty());

        let mut views = HashMap::<usize, usize>::new();
        for replica in &self.replicas {
            let count = views.entry(replica.view).or_default();
            *count += 1;
        }

        for (view, count) in views {
            if count >= majority {
                return view;
            }
        }

        panic!("Primary could not be found")
    }

    pub fn tick(&mut self) {
        for replica in &mut self.replicas {
            replica.tick();
        }
    }
}

fn quorum(total: usize) -> usize {
    // total = 2 * f + 1
    // f = (total - 1) / 2
    (total - 1) / 2
}

#[derive(Debug)]
pub struct Client {
    /// Client identification
    client_id: usize,

    /// Internal view number of the replica
    view: usize,

    /// Internal request number count
    next_request_number: RefCell<usize>,

    /// Communication channel between a client and the replicas
    channel: (Sender<ClientMessage>, Receiver<Message>),
}

impl Client {
    fn new(
        client_id: usize,
        view: usize,
        channel: (Sender<ClientMessage>, Receiver<Message>),
    ) -> Self {
        Client {
            client_id,
            view,
            channel,
            next_request_number: RefCell::new(0),
        }
    }

    pub fn send(&self) -> Result<(), ClientError> {
        let mut request_number = self.next_request_number.borrow_mut();

        self.channel
            .0
            .send(ClientMessage::Request {
                client_id: self.client_id,
                view: self.view,
                request_number: *request_number,
            })
            .map_err(|_| ClientError::NetworkError)?;

        *request_number += 1;

        Ok(())
    }

    pub fn recv(&self) -> Result<usize, ClientError> {
        let message = self
            .channel
            .1
            .recv()
            .map_err(|_| ClientError::NetworkError)?;

        if let Message::Reply { view } = message {
            return Ok(view);
        }

        unreachable!("Client received another message than a reply");
    }
}

#[derive(Error, Debug)]
pub enum ReplicaError {
    #[error("A message could not be sent or received")]
    NetworkError,
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("A message could not be sent or received")]
    NetworkError,
}

#[cfg(test)]
mod tests {
    use super::*;
    use lazy_static::lazy_static;

    lazy_static! {
        static ref CONFIG: Config = Config {
            addresses: vec!["ip-1".to_string(), "ip-2".to_string(), "ip-3".to_string()]
        };
    }

    #[test]
    fn it_connects_client() {
        // given
        let mut cluster = Cluster::new(&CONFIG);

        // when
        let client = cluster.handshake();

        // then
        assert_eq!(client.view, 0);
    }
}
