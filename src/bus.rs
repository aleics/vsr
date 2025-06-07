use bincode::config::Configuration;
use bytes::{Buf, Bytes, BytesMut};
use mio::net::{TcpListener, TcpStream};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::{cmp, collections::HashMap, time::Duration};

use crate::client::ClientConfig;
use crate::io::IOError;
use crate::message::{
    CommitMessage, DoViewChangeMessage, GetStateMessage, Message, NewStateMessage,
    PrepareOkMessage, RecoveryMessage, RecoveryResponseMessage, ReplyMessage,
    StartViewChangeMessage, StartViewMessage,
};
use crate::{
    ReplicaConfig,
    clock::Timeout,
    io::{Completion, IO},
};

/// The identifier used by the retry timeout used for connecting
/// to replicas / clients.
const CONNECT_RETRY_TIMEOUT_ID: &str = "connect_retry_timeout";

/// Minimum delay (ticks) used by the retry timeout.
const CONNECT_DELAY_MIN: u64 = 50;

/// Maximum delay (ticks) used by the retry timeout.
const CONNECT_DELAY_MAX: u64 = 1000;

/// Timeout (nanoseconds) for the IO tick iteration.
const TICK_TIMEOUT_NS: u64 = 200;

/// `RetryTimeout` defines a retry strategy and collects the timeout strategy,
/// as well as the amount of attempts for a given replica.
#[derive(Debug)]
struct RetryTimeout {
    /// Contains the amount of attempts for each replica, along with
    /// its associated timeout
    attempts: HashMap<usize, (u32, Timeout)>,
}

impl RetryTimeout {
    /// Create a new retry timeout. The `id` identifies the retry timeout.
    /// `after` defines the amount of ticks the timeout should use.
    /// `attempts` defines the
    fn new(id: &str, after: u64, replicas: Vec<usize>) -> Self {
        let attempts = replicas
            .into_iter()
            .map(|replica| {
                (
                    replica,
                    (0, Timeout::new(&format!("{id}_{replica}"), after)),
                )
            })
            .collect();

        RetryTimeout { attempts }
    }

    /// Start the retry timeout for all replicas
    fn init(&mut self) {
        for (_, timeout) in self.attempts.values_mut() {
            timeout.start();
        }
    }

    /// Tick the timeout for all replicas a single time.
    fn tick(&mut self) {
        for (_, timeout) in self.attempts.values_mut() {
            timeout.tick();
        }
    }

    /// Check if a given replica should try another attempt.
    /// In case no attempts have been tried yet, it returns `true`.
    fn should_retry(&self, replica: usize) -> bool {
        let Some((attempts, timeout)) = self.attempts.get(&replica) else {
            return false;
        };

        if attempts == &0 {
            return true;
        }

        timeout.fired()
    }

    /// Reset the retry timeout used by a given replica.
    fn reset(&mut self, replica: usize) {
        if let Some((attempts, timeout)) = self.attempts.get_mut(&replica) {
            *attempts = 0;
            timeout.reset();
        }
    }

    /// Increase the retry timeout of a given replica using exponential
    /// backoff with jitter defined by `rng`.
    fn increase(&mut self, replica: usize, rng: &mut ChaCha8Rng) {
        let Some((attempts, timeout)) = self.attempts.get_mut(&replica) else {
            return;
        };

        *attempts += 1;

        let after = compute_jittered_backoff(*attempts, CONNECT_DELAY_MIN, CONNECT_DELAY_MAX, rng);

        timeout.increase(after);
        timeout.reset();
    }
}

fn compute_jittered_backoff(attempt: u32, min: u64, max: u64, rng: &mut ChaCha8Rng) -> u64 {
    assert!(max > min);

    let base = u128::from(cmp::max(1, min));
    let exponent = attempt.min(63); // Saturate exponent to u6 equivalent (max 63)

    // backoff = min(cap, base * 2 ^ attempt)
    let power = 1u128
        .checked_shl(exponent)
        .expect("overflow in 1 << attempt");

    let backoff = base.saturating_mul(power).min(u128::from(max - min));
    let jitter = rng.random_range(0..=backoff) as u64;

    let result = min + jitter;

    assert!(result >= min);
    assert!(result <= max);

    result
}

/// The `OutgoingQueue` is used to collect all the messages meant
/// to be sent over to another replica / client.
#[derive(Debug, Default)]
struct OutgoingBuffer {
    pub(crate) send_queue: VecDeque<Bytes>,
}

impl OutgoingBuffer {
    fn new() -> Self {
        OutgoingBuffer::default()
    }

    /// Add a message in the outgoing buffer's queue.
    fn add(&mut self, message_bytes: Bytes) {
        self.send_queue.push_back(message_bytes);
    }

    /// Pop out the first message from the queue.
    fn pop(&mut self) -> Option<Bytes> {
        self.send_queue.pop_front()
    }
}

/// A `Connection` describes the connection between replicas / clients.
struct Connection {
    /// The target address of the connection.
    address: SocketAddr,

    /// The status of the connection.
    status: ConnectionStatus,

    /// The socket used to communicate to the target replica / client.
    socket: Option<TcpStream>,

    /// The connection peer identifying the target.
    peer: Option<ConnectionPeer>,

    /// The outgoing buffer of the connection where message to the target
    /// are queued.
    outgoing_buffer: OutgoingBuffer,

    /// The incoming buffer for message coming from the target.
    incoming_buffer: BytesMut,

    /// The `bincode` configuration used to encode / decode messages.
    encoding_config: Configuration,
}

impl Connection {
    /// Create a new connection without a peer.
    fn new(address: SocketAddr, config: Configuration) -> Self {
        Connection {
            address,
            status: ConnectionStatus::Free,
            socket: None,
            peer: None,
            outgoing_buffer: OutgoingBuffer::new(),
            incoming_buffer: BytesMut::new(),
            encoding_config: config,
        }
    }

    /// Sets the peer of the connection given a received message.
    fn set_message_peer(&mut self, message: &Message) -> &ConnectionPeer {
        let peer = ConnectionPeer::from_message(message);
        self.peer = Some(peer);

        self.peer.as_ref().unwrap()
    }

    /// Queue a new message into the outgoing buffer
    fn queue_message(&mut self, message: &Message) -> Result<(), IOError> {
        let buffer = message.encode(self.encoding_config)?;
        self.outgoing_buffer.add(buffer);

        Ok(())
    }
}

/// A `ConnectionPeer` describes what type of node is a connection connected to.
#[derive(Debug)]
enum ConnectionPeer {
    Client { id: usize },
    Replica { id: usize },
    Unknown,
}

impl ConnectionPeer {
    /// Guess what type of connection peer a message should be coming from.
    fn from_message(message: &Message) -> ConnectionPeer {
        match message {
            Message::Request(request) => ConnectionPeer::Client {
                id: request.client_id,
            },
            Message::Prepare(..) | Message::Reply(..) => ConnectionPeer::Unknown,
            Message::PrepareOk(PrepareOkMessage { replica_number, .. })
            | Message::Commit(CommitMessage { replica_number, .. })
            | Message::GetState(GetStateMessage { replica_number, .. })
            | Message::StartViewChange(StartViewChangeMessage { replica_number, .. })
            | Message::StartView(StartViewMessage { replica_number, .. })
            | Message::DoViewChange(DoViewChangeMessage { replica_number, .. })
            | Message::NewState(NewStateMessage { replica_number, .. })
            | Message::Recovery(RecoveryMessage { replica_number, .. })
            | Message::RecoveryResponse(RecoveryResponseMessage { replica_number, .. }) => {
                ConnectionPeer::Replica {
                    id: *replica_number,
                }
            }
        }
    }
}

/// A pool of connections available in a message bus.
#[derive(Default)]
struct ConnectionPool {
    connections: Vec<Connection>,
}

impl ConnectionPool {
    /// Create an empty connection pool
    fn new() -> Self {
        Self::default()
    }

    /// Determine what the next connection identifier must be
    /// given the existing connections.
    fn next_id(&self) -> usize {
        self.connections.len() + 1
    }

    /// Size of the connection pool
    fn size(&self) -> usize {
        self.connections.len()
    }

    /// Adds a new connection to the pool
    fn add(&mut self, connection: Connection) -> usize {
        let id = self.next_id();
        self.connections.push(connection);

        assert!(id == self.size());

        id
    }

    /// Get a connection given an identifier
    fn get(&self, connection_id: usize) -> Option<&Connection> {
        self.connections.get(connection_id - 1)
    }

    /// Get a mutable reference to a connection given an identifier
    fn get_mut(&mut self, connection_id: usize) -> Option<&mut Connection> {
        self.connections.get_mut(connection_id - 1)
    }

    /// Removes a connection from the pool given its identifier.
    fn remove(&mut self, connection_id: usize) -> Connection {
        self.connections.remove(connection_id - 1)
    }
}

/// The status of a connection.
#[derive(Debug, PartialEq)]
enum ConnectionStatus {
    Connected,
    Pending,
    Free,
}

/// The message bus used by the replicas. The bus is in charge of accepting and
/// closing connections to the source replica, as well as sending and receiving
/// messages from other replicas and clients.
pub(crate) struct ReplicaMessageBus<IO> {
    address: SocketAddr,

    /// The socket used to listen for messages.
    socket: Option<TcpListener>,

    /// The connection pool of the replica bus.
    connection_pool: ConnectionPool,

    /// The current replica's identifier.
    replica: usize,

    /// A key-value map identifying each connected replica to the associated
    /// connection identifier.
    replicas: HashMap<usize, usize>,

    /// A key-value map identifying each connected client to the associated
    /// connection identifier.
    clients: HashMap<usize, usize>,

    /// A retry timeout used for connecting to other replicas.
    connect_retry: RetryTimeout,

    /// A random generator used by the connect retry timeout.
    rng: ChaCha8Rng,

    /// The IO object used to read / write messages from the connections.
    io: IO,

    /// The `bincode` configuration used to encode / decode messages.
    encoding_config: Configuration,
}

impl<I: IO> ReplicaMessageBus<I> {
    /// Create a new instance of a message bus for the local replica.
    pub(crate) fn new(config: &ReplicaConfig, io: I) -> Self {
        let current_address = config.addresses.get(config.replica).unwrap();
        let encoding_config = bincode::config::standard();

        let mut connections = ConnectionPool::new();
        let mut replicas = HashMap::with_capacity(config.addresses.len() - 1);
        let mut connect_attempts = Vec::with_capacity(config.addresses.len() - config.replica + 1);

        // Create a fix amount of connection to the other replicas.
        // It will connect to the next replicas as defined in the configuration to prevent conflicts.
        for replica in (config.replica + 1)..config.addresses.len() {
            let address = config.addresses.get(replica).unwrap();
            let connection_id = connections.add(Connection::new(*address, encoding_config));
            replicas.insert(replica, connection_id);
            connect_attempts.push(replica);
        }

        ReplicaMessageBus {
            address: *current_address,
            socket: None,
            connection_pool: connections,
            replica: config.replica,
            replicas,
            clients: HashMap::new(),
            connect_retry: RetryTimeout::new(
                CONNECT_RETRY_TIMEOUT_ID,
                CONNECT_DELAY_MIN,
                connect_attempts,
            ),
            rng: ChaCha8Rng::seed_from_u64(config.seed),
            io,
            encoding_config,
        }
    }

    /// Initialize the bus and open a TCP address to allow incoming
    /// connections.
    pub(crate) fn init(&mut self) -> Result<(), IOError> {
        self.socket = Some(self.io.open_tcp(self.address)?);
        self.connect_retry.init();
        Ok(())
    }

    /// Run any pending IO operations with a given timeout in nanoseconds.
    /// All the operations will be executed and in case any incoming message
    /// has been received, it will be returned.
    fn run_for_ns(&mut self, nanos: u64) -> Result<Vec<Message>, IOError> {
        let duration = Duration::from_nanos(nanos);

        let mut messages = Vec::new();
        for completion in self.io.run(duration)? {
            match completion {
                Completion::Accept => self.accept()?,
                Completion::Recv { connection } => {
                    if let Some(message) = self.recv(connection)? {
                        messages.push(message);
                    }
                }
                Completion::Write { connection } => {
                    self.write(connection)?;
                }
            }
        }

        Ok(messages)
    }

    /// Connect to all the known replicas
    fn connect_to_other_replicas(&mut self) {
        for (replica, connection_id) in self.replicas.clone() {
            self.connect_to_replica(replica, connection_id);
        }
    }

    /// Connect to a replica using an associated connection identifier. In case the connection
    /// is successful, the connection identifier is returned.
    fn connect_to_replica(&mut self, replica: usize, connection_id: usize) -> Option<usize> {
        let connection = self.connection_pool.get_mut(connection_id)
            .unwrap_or_else(|| panic!("Connection must be initialized (replica: {replica}, connection_id: {connection_id})"));

        if let ConnectionStatus::Connected = connection.status {
            return Some(connection_id);
        }

        if !self.connect_retry.should_retry(replica) {
            return None;
        }

        if let ConnectionStatus::Pending = connection.status {
            let socket = connection
                .socket
                .as_ref()
                .unwrap_or_else(|| panic!("Connection that is pending must have already a socket assigned (replica: {replica}, connection_id: {connection_id})"));

            // In case the socket has an associated peer address, it means it's  already connected.
            if socket.peer_addr().is_ok() {
                connection.status = ConnectionStatus::Connected;
                Some(connection_id)
            } else {
                self.connect_retry.increase(replica, &mut self.rng);
                None
            }
        } else {
            // Try to connect to the connection's address in a non-blocking fashion.
            if let Ok(Some(socket)) = self.io.connect(connection.address, connection_id) {
                connection.socket = Some(socket);
                connection.peer = Some(ConnectionPeer::Replica { id: replica });
                connection.status = ConnectionStatus::Pending;

                self.connect_retry.reset(replica);

                None
            } else {
                self.connect_retry.increase(replica, &mut self.rng);
                None
            }
        }
    }

    /// Accept any incoming connections, either from other replicas or clients.
    fn accept(&mut self) -> Result<(), IOError> {
        let socket = self
            .socket
            .as_mut()
            .expect("Replica socket not available while accepting new connections");

        for accepted in self.io.accept(socket, self.connection_pool.next_id())? {
            let mut connection =
                Connection::new(accepted.socket.peer_addr()?, self.encoding_config);
            connection.status = ConnectionStatus::Connected;
            connection.socket = Some(accepted.socket);
            connection.peer = Some(ConnectionPeer::Unknown);

            let connection_id = self.connection_pool.add(connection);
            assert!(connection_id == accepted.connection_id);
            assert!(connection_id == self.connection_pool.size());
        }

        Ok(())
    }

    /// Receive any possible new message waiting to be read from a given connection.
    fn recv(&mut self, connection_id: usize) -> Result<Option<Message>, IOError> {
        let connection = self.connection_pool.get_mut(connection_id).unwrap_or_else(|| {
            panic!(
                "No connection matches for a received operation (connection_id: {connection_id})"
            )
        });

        assert!(connection.status == ConnectionStatus::Connected);

        let socket = connection.socket.as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (connection_id: {connection_id})"));

        let close = self.io.recv(socket, &mut connection.incoming_buffer)?;
        if close {
            self.close(connection_id)?;
            return Ok(None);
        }

        let Some(message) = Message::decode(&mut connection.incoming_buffer, self.encoding_config)?
        else {
            tracing::error!(
                "Message could not be decoded (connection_id = {}, replica = {})",
                connection_id,
                self.replica
            );
            return Ok(None);
        };

        // Given the type of message received, set the connection peer,
        // and refresh the associated known replica / client entry.
        match connection.set_message_peer(&message) {
            ConnectionPeer::Client { id } => {
                self.clients.insert(*id, connection_id);
            }
            ConnectionPeer::Replica { id } => {
                self.replicas.insert(*id, connection_id);
            }
            ConnectionPeer::Unknown => {}
        }

        Ok(Some(message))
    }

    /// Close a connection. It will be removed from the connection pool
    /// and from the entries of known replicas / clients.
    fn close(&mut self, connection_id: usize) -> Result<(), IOError> {
        let connection = self.connection_pool.remove(connection_id);
        let mut socket = connection.socket.unwrap_or_else(|| {
            panic!(
                "Connection being closed must be have a socket (connection_id: {}, replica: {})",
                connection_id, self.replica
            )
        });

        self.io.close(&mut socket)?;

        match connection.peer {
            Some(ConnectionPeer::Replica { id }) => {
                self.replicas.remove(&id);
            }
            Some(ConnectionPeer::Client { id }) => {
                self.clients.remove(&id);
            }
            Some(ConnectionPeer::Unknown) | None => {}
        }

        Ok(())
    }

    /// Send a message to another replica in a non-blocking fashion. The message will be queued
    /// to the connection's outgoing buffer to be ultimately sent.
    pub(crate) fn send_to_replica(
        &mut self,
        message: &Message,
        replica: usize,
    ) -> Result<bool, IOError> {
        let connection_id = self
            .replicas
            .get(&replica)
            .unwrap_or_else(|| panic!("Replica not found (from: {}, to: {replica})", self.replica));

        let connection = self.connection_pool.get_mut(*connection_id)
            .unwrap_or_else(|| panic!("Connection for replica not found (replica: {replica}, connection_id: {connection_id})"));

        if connection.status != ConnectionStatus::Connected {
            return Ok(false);
        }

        connection.queue_message(message)?;

        let socket = connection
            .socket
            .as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (replica: {replica}, connection_id: {connection_id})"));

        self.io.send(socket, *connection_id)?;

        Ok(true)
    }

    /// Write an outgoing message to the connection provided. A boolean is returned in case the
    /// message was written or not.
    fn write(&mut self, connection_id: usize) -> Result<bool, IOError> {
        let connection = self.connection_pool.get_mut(connection_id).unwrap_or_else(|| {
            panic!(
                "No connection matches for a received operation (connection_id: {connection_id})"
            )
        });

        if connection.status != ConnectionStatus::Connected {
            return Ok(false);
        }

        let socket = connection.socket.as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (connection_id: {connection_id})"));

        // Write the message from the outgoing buffer in chunks.
        while let Some(mut buffer) = connection.outgoing_buffer.pop() {
            while !buffer.is_empty() {
                let Some(written) = self.io.write(socket, &buffer)? else {
                    return Ok(true);
                };
                buffer.advance(written);
            }
        }

        Ok(false)
    }

    /// Send a message to a client in a non-blocking fashion. The message will be queued
    /// to the connection's outgoing buffer to be ultimately sent. A boolean is returned
    /// in case the message was sent to the client or not.
    pub(crate) fn send_to_client(
        &mut self,
        reply: ReplyMessage,
        client_id: usize,
    ) -> Result<bool, IOError> {
        let connection_id = self
            .clients
            .get(&client_id)
            .unwrap_or_else(|| panic!("Client not found (client: {client_id})"));

        let connection = self.connection_pool.get_mut(*connection_id)
            .unwrap_or_else(|| panic!("Connection for client not found (client: {client_id}, connection_id: {connection_id})"));

        // If the connection is not ready yet
        if connection.status != ConnectionStatus::Connected {
            return Ok(false);
        }

        let message = Message::Reply(reply);
        connection.queue_message(&message)?;

        let socket = connection
            .socket
            .as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (client: {client_id}, connection_id: {connection_id})"));

        self.io.send(socket, *connection_id)?;

        Ok(true)
    }

    /// Trigger a tick iteration for the bus. In case messages were received from
    /// the incoming buffer, they are returned so the local replica can process them.
    pub(crate) fn tick(&mut self) -> Result<Vec<Message>, IOError> {
        self.connect_to_other_replicas();
        let messages = self.run_for_ns(TICK_TIMEOUT_NS)?;
        self.connect_retry.tick();

        Ok(messages)
    }
}

/// The message bus used by the client. The bus is in charge of accepting and
/// closing connections to the client, as well as sending and receiving
/// messages from replicas.
pub(crate) struct ClientMessageBus<IO> {
    /// The client's identifier
    client_id: usize,

    /// The address of the current replica.
    address: SocketAddr,

    /// The socket used to listen for messages.
    socket: Option<TcpListener>,

    /// The connection pool of the replica bus.
    connection_pool: ConnectionPool,

    /// A key-value map identifying each connected replica to the associated
    /// connection identifier.
    replicas: HashMap<usize, usize>,

    /// A retry timeout used for connecting to the replicas.
    connect_retry: RetryTimeout,

    /// A random generator used by the connect retry timeout.
    rng: ChaCha8Rng,

    /// The IO object used to read / write messages from the connections.
    io: IO,

    /// The `bincode` configuration used to encode / decode messages.
    encoding_config: Configuration,
}

impl<I: IO> ClientMessageBus<I> {
    /// Create a new instance of a message bus for a client.
    pub(crate) fn new(config: &ClientConfig, io: I) -> Self {
        let encoding_config = bincode::config::standard();
        let mut connections = ConnectionPool::new();
        let mut replicas = HashMap::with_capacity(config.replicas.len() - 1);
        let mut connect_attempts = Vec::with_capacity(config.replicas.len());

        // Connect to all the known replicas.
        for (replica, address) in config.replicas.iter().enumerate() {
            let connection_id = connections.add(Connection::new(*address, encoding_config));
            replicas.insert(replica, connection_id);
            connect_attempts.push(replica);
        }

        ClientMessageBus {
            client_id: config.client_id,
            address: config.address,
            socket: None,
            connection_pool: connections,
            replicas,
            connect_retry: RetryTimeout::new(
                CONNECT_RETRY_TIMEOUT_ID,
                CONNECT_DELAY_MIN,
                connect_attempts,
            ),
            rng: ChaCha8Rng::seed_from_u64(config.seed),
            io,
            encoding_config,
        }
    }

    /// Initialize the bus and open a TCP address to allow incoming
    /// connections.
    pub(crate) fn init(&mut self) -> Result<(), IOError> {
        self.socket = Some(self.io.open_tcp(self.address)?);
        self.connect_retry.init();
        Ok(())
    }

    /// Check if all the known replicas have an associated connection ready to send messages to.
    pub(crate) fn is_connected(&self) -> bool {
        for connection_id in self.replicas.values() {
            let Some(connection) = self.connection_pool.get(*connection_id) else {
                return false;
            };

            if connection.status != ConnectionStatus::Connected {
                return false;
            }
        }

        true
    }

    /// Run any pending IO operations with a given timeout in nanoseconds.
    /// All the operations will be executed and in case any incoming message
    /// has been received, it will be returned.
    fn run_for_ns(&mut self, nanos: u64) -> Result<Vec<Message>, IOError> {
        let duration = Duration::from_nanos(nanos);

        let mut messages = Vec::new();
        for completion in self.io.run(duration)? {
            match completion {
                Completion::Accept => self.accept()?,
                Completion::Recv { connection } => {
                    if let Some(message) = self.recv(connection)? {
                        messages.push(message);
                    }
                }
                Completion::Write { connection } => {
                    self.write(connection)?;
                }
            }
        }

        Ok(messages)
    }

    /// Connect to all the known replicas
    fn connect_to_replicas(&mut self) {
        for (replica, connection_id) in self.replicas.clone() {
            self.connect_to_replica(replica, connection_id);
        }
    }

    /// Connect to a replica using an associated connection identifier. In case the connection
    /// is successful, the connection identifier is returned.
    fn connect_to_replica(&mut self, replica: usize, connection_id: usize) -> Option<usize> {
        let connection = self.connection_pool.get_mut(connection_id)
            .unwrap_or_else(|| panic!("Connection must be initialized (replica: {replica}, connection_id: {connection_id})"));

        if let ConnectionStatus::Connected = connection.status {
            return Some(connection_id);
        }

        if !self.connect_retry.should_retry(replica) {
            return None;
        }

        if let ConnectionStatus::Pending = connection.status {
            let socket = connection
                .socket
                .as_ref()
                .unwrap_or_else(|| panic!("Connection that is pending must have already a socket assigned (replica: {replica}, connection_id: {connection_id})"));

            // In case the socket has an associated peer address, it means it's  already connected.
            if socket.peer_addr().is_ok() {
                connection.status = ConnectionStatus::Connected;
                Some(connection_id)
            } else {
                self.connect_retry.increase(replica, &mut self.rng);
                None
            }
        } else if let Ok(Some(socket)) = self.io.connect(connection.address, connection_id) {
            connection.socket = Some(socket);
            connection.peer = Some(ConnectionPeer::Replica { id: replica });
            connection.status = ConnectionStatus::Pending;

            self.connect_retry.reset(replica);

            None
        } else {
            self.connect_retry.increase(replica, &mut self.rng);
            None
        }
    }

    /// Accept any incoming connections from any replica.
    fn accept(&mut self) -> Result<(), IOError> {
        let socket = self
            .socket
            .as_mut()
            .expect("Replica socket not available while accepting new connections");

        for accepted in self.io.accept(socket, self.connection_pool.next_id())? {
            let mut connection =
                Connection::new(accepted.socket.peer_addr()?, self.encoding_config);
            connection.status = ConnectionStatus::Connected;
            connection.socket = Some(accepted.socket);
            connection.peer = Some(ConnectionPeer::Unknown);

            let connection_id = self.connection_pool.add(connection);
            assert!(connection_id == accepted.connection_id);
            assert!(connection_id == self.connection_pool.size());
        }

        Ok(())
    }

    /// Receive any possible new message waiting to be read from a given connection.
    fn recv(&mut self, connection_id: usize) -> Result<Option<Message>, IOError> {
        let connection = self.connection_pool.get_mut(connection_id).unwrap_or_else(|| {
            panic!(
                "No connection matches for a received operation (connection_id: {connection_id})"
            )
        });

        assert!(connection.status == ConnectionStatus::Connected);

        let socket = connection.socket.as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (connection_id: {connection_id})"));

        let close = self.io.recv(socket, &mut connection.incoming_buffer)?;
        if close {
            self.close(connection_id)?;
            return Ok(None);
        }

        let Some(message) = Message::decode(&mut connection.incoming_buffer, self.encoding_config)?
        else {
            tracing::error!(
                "Message could not be decoded (connection_id = {}, client = {})",
                connection_id,
                self.client_id
            );
            return Ok(None);
        };

        // Given the type of message received, set the connection peer,
        // and refresh the associated known replica entry.
        match connection.set_message_peer(&message) {
            ConnectionPeer::Replica { id } => {
                self.replicas.insert(*id, connection_id);
            }
            ConnectionPeer::Client { .. } => unreachable!(),
            ConnectionPeer::Unknown => {}
        }

        Ok(Some(message))
    }

    /// Close a connection. It will be removed from the connection pool
    /// and from the entries of known replicas.
    fn close(&mut self, connection_id: usize) -> Result<(), IOError> {
        let connection = self.connection_pool.remove(connection_id);
        let mut socket = connection.socket.unwrap_or_else(|| {
            panic!(
                "Connection being closed must be have a socket (connection_id: {}, client: {})",
                connection_id, self.client_id
            )
        });

        self.io.close(&mut socket)?;

        if let Some(ConnectionPeer::Replica { id }) = connection.peer {
            self.replicas.remove(&id);
        }

        Ok(())
    }

    /// Send a message to a replica in a non-blocking fashion. The message will be queued
    /// to the connection's outgoing buffer to be ultimately sent.
    pub(crate) fn send_to_replica(
        &mut self,
        message: &Message,
        replica: usize,
    ) -> Result<(), IOError> {
        let connection_id = self
            .replicas
            .get(&replica)
            .unwrap_or_else(|| panic!("Replica not found (replica: {replica})"));

        let connection = self.connection_pool.get_mut(*connection_id)
            .unwrap_or_else(|| panic!("Connection for replica not found (replica: {replica}, connection_id: {connection_id})"));

        assert!(connection.status == ConnectionStatus::Connected);

        connection.queue_message(message)?;

        let socket = connection
            .socket
            .as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (replica: {replica}, connection_id: {connection_id})"));

        self.io.send(socket, *connection_id)?;

        Ok(())
    }

    /// Write an outgoing message to the connection provided. A boolean is returned in case the
    /// message was written or not.
    fn write(&mut self, connection_id: usize) -> Result<bool, IOError> {
        let connection = self.connection_pool.get_mut(connection_id).unwrap_or_else(|| {
            panic!(
                "No connection matches for a received operation (connection_id: {connection_id})"
            )
        });

        if connection.status != ConnectionStatus::Connected {
            return Ok(false);
        }

        let socket = connection.socket.as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (connection_id: {connection_id})"));

        // Write the message from the outgoing buffer in chunks.
        while let Some(mut buffer) = connection.outgoing_buffer.pop() {
            while !buffer.is_empty() {
                let Some(written) = self.io.write(socket, &buffer)? else {
                    return Ok(true);
                };
                buffer.advance(written);
            }
        }

        Ok(false)
    }

    /// Trigger a tick iteration for the bus. In case messages were received from
    /// the incoming buffer, they are returned so the client can process them.
    pub(crate) fn tick(&mut self) -> Result<Vec<Message>, IOError> {
        self.connect_to_replicas();
        let messages = self.run_for_ns(TICK_TIMEOUT_NS)?;
        self.connect_retry.tick();

        Ok(messages)
    }
}
