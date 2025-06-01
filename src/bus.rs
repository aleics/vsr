use bincode::config::Configuration;
use mio::net::{TcpListener, TcpStream};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::{cmp, collections::HashMap, time::Duration};

use crate::client::ClientConfig;
use crate::io::IOError;
use crate::network::{
    CommitMessage, DoViewChangeMessage, GetStateMessage, Message, NewStateMessage,
    PrepareOkMessage, RecoveryMessage, RecoveryResponseMessage, ReplyMessage,
    StartViewChangeMessage, StartViewMessage,
};
use crate::{
    ReplicaConfig,
    clock::Timeout,
    io::{Completion, IO},
};

const CONNECT_RETRY_TIMEOUT_ID: &str = "connect_retry_timeout";
const CONNECT_DELAY_MIN: u64 = 50;
const CONNECT_DELAY_MAX: u64 = 1000;

#[derive(Debug)]
struct RetryInfo {
    attempts: HashMap<usize, u32>,
    timeout: Timeout,
}

impl RetryInfo {
    fn start(id: &str, after: u64, attempts: HashMap<usize, u32>) -> Self {
        let mut timeout = Timeout::new(id, after);
        timeout.start();

        RetryInfo { attempts, timeout }
    }

    fn tick(&mut self) {
        self.timeout.tick();
    }

    fn retry(&self, replica: &usize) -> bool {
        let attempts = self.attempts.get(replica).copied().unwrap_or_default();
        if attempts == 0 {
            return true;
        }

        self.timeout.fired()
    }

    fn reset(&mut self, replica: usize) {
        let attempts = self.attempts.entry(replica).or_default();
        *attempts = 0;
        self.timeout.reset();
    }

    fn increase(&mut self, replica: usize, rng: &mut ChaCha8Rng) {
        let attempts = self.attempts.entry(replica).or_default();
        *attempts += 1;

        let after = compute_jittered_backoff(*attempts, CONNECT_DELAY_MIN, CONNECT_DELAY_MAX, rng);

        self.timeout.increase(after);
        self.timeout.reset();
    }
}

fn compute_jittered_backoff(attempt: u32, min: u64, max: u64, rng: &mut ChaCha8Rng) -> u64 {
    assert!(max > min);

    let base = cmp::max(1, min) as u128;
    let exponent = attempt.min(63); // Saturate exponent to u6 equivalent (max 63)

    // backoff = min(cap, base * 2 ^ attempt)
    let power = 1u128
        .checked_shl(exponent)
        .expect("overflow in 1 << attempt");

    let backoff = base.saturating_mul(power).min((max - min) as u128);
    let jitter = rng.random_range(0..=backoff) as u64;

    let result = min + jitter;

    assert!(result >= min);
    assert!(result <= max);

    result
}

fn encode_message(message: &Message, config: Configuration) -> Vec<u8> {
    let encoded = bincode::encode_to_vec(message, config).unwrap();
    let mut frame = (encoded.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&encoded);
    frame
}

fn decode_message(buf: &mut Vec<u8>) -> Option<Message> {
    if buf.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return None;
    }
    let msg_bytes = &buf[4..4 + len];
    let (msg, _) = bincode::decode_from_slice(msg_bytes, bincode::config::standard()).ok()?;
    buf.drain(..4 + len);
    Some(msg)
}

#[derive(Debug, Default)]
struct OutgoingBuffer {
    pub(crate) send_queue: VecDeque<Vec<u8>>,
}

impl OutgoingBuffer {
    fn new() -> Self {
        OutgoingBuffer::default()
    }
}

struct Connection {
    address: SocketAddr,
    status: ConnectionStatus,
    socket: Option<TcpStream>,
    peer: Option<ConnectionPeer>,
    outgoing_buffer: OutgoingBuffer,
    incoming_buffer: Vec<u8>,
    config: Configuration,
}

impl Connection {
    fn new(address: SocketAddr) -> Self {
        Connection {
            address,
            status: ConnectionStatus::Free,
            socket: None,
            peer: None,
            outgoing_buffer: OutgoingBuffer::new(),
            incoming_buffer: Vec::new(),
            config: bincode::config::standard(),
        }
    }

    fn set_message_peer(&mut self, message: &Message) -> &ConnectionPeer {
        let peer = ConnectionPeer::from_message(message);
        self.peer = Some(peer);

        self.peer.as_ref().unwrap()
    }

    fn queue_message(&mut self, message: &Message) {
        let buf = encode_message(message, self.config);
        self.outgoing_buffer.send_queue.push_back(buf);
    }
}

#[derive(Debug)]
enum ConnectionPeer {
    Client { id: usize },
    Replica { id: usize },
    Unknown,
}

impl ConnectionPeer {
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

#[derive(Default)]
struct ConnectionPool {
    connections: Vec<Connection>,
}

impl ConnectionPool {
    fn new() -> Self {
        Self::default()
    }

    fn next_id(&self) -> usize {
        self.connections.len() + 1
    }

    fn size(&self) -> usize {
        self.connections.len()
    }

    fn add(&mut self, connection: Connection) -> usize {
        let id = self.next_id();
        self.connections.push(connection);

        id
    }

    fn get(&self, connection_id: &usize) -> Option<&Connection> {
        self.connections.get(connection_id - 1)
    }

    fn get_mut(&mut self, connection_id: &usize) -> Option<&mut Connection> {
        self.connections.get_mut(connection_id - 1)
    }

    fn remove(&mut self, connection_id: usize) -> Connection {
        self.connections.remove(connection_id - 1)
    }
}

#[derive(Debug, PartialEq)]
enum ConnectionStatus {
    Connected,
    Pending,
    Free,
}

pub struct ReplicaMessageBus<I> {
    address: SocketAddr,
    socket: Option<TcpListener>, // TODO: Define this as a connection?
    connection_pool: ConnectionPool,
    current: usize,
    replicas: HashMap<usize, usize>,
    clients: HashMap<usize, usize>,
    connect_retry: RetryInfo,
    rng: ChaCha8Rng,
    io: I,
}

impl<I: IO> ReplicaMessageBus<I> {
    pub fn new(config: &ReplicaConfig, io: I) -> Self {
        let current_address = config.addresses.get(config.replica).unwrap();

        let mut connections = ConnectionPool::new();
        let mut replicas = HashMap::with_capacity(config.addresses.len() - 1);
        let mut connect_attempts = HashMap::with_capacity(config.addresses.len() - 1);

        for replica in (config.replica + 1)..config.addresses.len() {
            let address = config.addresses.get(replica).unwrap();
            let connection_id = connections.add(Connection::new(*address));
            replicas.insert(replica, connection_id);
            connect_attempts.insert(replica, 0);
        }

        ReplicaMessageBus {
            address: *current_address,
            socket: None,
            connection_pool: connections,
            current: config.replica,
            replicas,
            clients: HashMap::new(),
            connect_retry: RetryInfo::start(
                CONNECT_RETRY_TIMEOUT_ID,
                CONNECT_DELAY_MIN,
                connect_attempts,
            ),
            rng: ChaCha8Rng::seed_from_u64(config.seed),
            io,
        }
    }

    pub fn init(&mut self) -> Result<(), IOError> {
        self.socket = Some(self.io.open_tcp(self.address)?);
        Ok(())
    }

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

    fn connect_to_other_replicas(&mut self) -> Result<(), IOError> {
        let replicas = self.replicas.clone();
        for (replica, connection_id) in replicas.into_iter() {
            self.connect_to_replica(replica, connection_id);
        }

        Ok(())
    }

    fn connect_to_replica(&mut self, replica: usize, connection_id: usize) -> Option<usize> {
        let connection = self.connection_pool.get_mut(&connection_id)
            .unwrap_or_else(|| panic!("Connection must be initialized (replica: {replica}, connection_id: {connection_id})"));

        if let ConnectionStatus::Connected = connection.status {
            return Some(connection_id);
        }

        if !self.connect_retry.retry(&replica) {
            return None;
        }

        if let ConnectionStatus::Pending = connection.status {
            let socket = connection
                .socket
                .as_ref()
                .unwrap_or_else(|| panic!("Connection that is pending must have already a socket assigned (replica: {replica}, connection_id: {connection_id})"));

            match socket.peer_addr() {
                Ok(..) => {
                    connection.status = ConnectionStatus::Connected;
                    Some(connection_id)
                }
                Err(..) => {
                    self.connect_retry.increase(replica, &mut self.rng);
                    None
                }
            }
        } else {
            match self.io.connect(connection.address, connection_id) {
                Ok(Some(socket)) => {
                    connection.socket = Some(socket);
                    connection.peer = Some(ConnectionPeer::Replica { id: replica });
                    connection.status = ConnectionStatus::Pending;

                    self.connect_retry.reset(replica);

                    None
                }
                Ok(None) => {
                    self.connect_retry.increase(replica, &mut self.rng);
                    None
                }
                Err(..) => {
                    self.connect_retry.increase(replica, &mut self.rng);
                    None
                }
            }
        }
    }

    fn accept(&mut self) -> Result<(), IOError> {
        let socket = self
            .socket
            .as_mut()
            .expect("Replica socket not available while accepting new connections");

        let accepted_conns = self.io.accept(socket, self.connection_pool.next_id())?;

        for accepted_conn in accepted_conns {
            let mut connection = Connection::new(accepted_conn.socket.peer_addr()?);
            connection.status = ConnectionStatus::Connected;
            connection.socket = Some(accepted_conn.socket);
            connection.peer = Some(ConnectionPeer::Unknown);

            self.connection_pool.add(connection);
            assert!(accepted_conn.connection_id == self.connection_pool.size());
        }

        Ok(())
    }

    fn recv(&mut self, connection_id: usize) -> Result<Option<Message>, IOError> {
        let connection = self.connection_pool.get_mut(&connection_id).unwrap_or_else(|| {
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

        let Some(message) = decode_message(&mut connection.incoming_buffer) else {
            return Ok(None);
        };

        match connection.set_message_peer(&message) {
            ConnectionPeer::Client { id } => {
                self.clients.insert(*id, connection_id);
            }
            ConnectionPeer::Replica { id } => {
                self.replicas.insert(*id, connection_id);
            }
            ConnectionPeer::Unknown => {}
        };

        Ok(Some(message))
    }

    fn close(&mut self, connection_id: usize) -> Result<(), IOError> {
        let connection = self.connection_pool.remove(connection_id);
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

    pub fn send_to_replica(&mut self, message: Message, replica: &usize) -> Result<(), IOError> {
        let connection_id = self
            .replicas
            .get(replica)
            .unwrap_or_else(|| panic!("Replica not found (from: {}, to: {replica})", self.current));

        let connection = self.connection_pool.get_mut(connection_id)
            .unwrap_or_else(|| panic!("Connection for replica not found (replica: {replica}, connection_id: {connection_id})"));

        if connection.status != ConnectionStatus::Connected {
            return Ok(());
        }

        connection.queue_message(&message);

        let socket = connection
            .socket
            .as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (replica: {replica}, connection_id: {connection_id})"));

        self.io.send(socket, *connection_id)?;

        Ok(())
    }

    fn write(&mut self, connection_id: usize) -> Result<bool, IOError> {
        let connection = self.connection_pool.get_mut(&connection_id).unwrap_or_else(|| {
            panic!(
                "No connection matches for a received operation (connection_id: {connection_id})"
            )
        });

        if connection.status != ConnectionStatus::Connected {
            return Ok(false);
        }

        let socket = connection.socket.as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (connection_id: {connection_id})"));

        while let Some(buffer) = connection.outgoing_buffer.send_queue.front_mut() {
            while !buffer.is_empty() {
                let Some(written) = self.io.write(socket, buffer)? else {
                    return Ok(true);
                };

                buffer.drain(..written);
            }

            connection.outgoing_buffer.send_queue.pop_front();
        }

        Ok(false)
    }

    pub fn send_to_client(
        &mut self,
        message: ReplyMessage,
        client_id: &usize,
    ) -> Result<(), IOError> {
        let connection_id = self
            .clients
            .get(client_id)
            .unwrap_or_else(|| panic!("Client not found (client: {client_id})"));

        let connection = self.connection_pool.get_mut(connection_id)
            .unwrap_or_else(|| panic!("Connection for client not found (client: {client_id}, connection_id: {connection_id})"));

        if connection.status != ConnectionStatus::Connected {
            // TODO: If the client is not connected to the replica, then?
            return Ok(());
        }

        let message = Message::Reply(message);
        connection.queue_message(&message);

        let socket = connection
            .socket
            .as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (client: {client_id}, connection_id: {connection_id})"));

        self.io.send(socket, *connection_id)?;

        Ok(())
    }

    pub fn tick(&mut self) -> Result<Vec<Message>, IOError> {
        self.connect_to_other_replicas()?;
        let messages = self.run_for_ns(200)?;
        self.connect_retry.tick();

        Ok(messages)
    }
}

pub struct ClientMessageBus<I> {
    address: SocketAddr,
    socket: Option<TcpListener>, // TODO: Define this as a connection?
    connection_pool: ConnectionPool,
    replicas: HashMap<usize, usize>,
    connect_retry: RetryInfo,
    rng: ChaCha8Rng,
    io: I,
}

impl<I: IO> ClientMessageBus<I> {
    pub fn new(config: &ClientConfig, io: I) -> Self {
        let mut connections = ConnectionPool::new();
        let mut replicas = HashMap::with_capacity(config.replicas.len() - 1);
        let mut connect_attempts = HashMap::with_capacity(config.replicas.len() - 1);

        for (replica, address) in config.replicas.iter().enumerate() {
            let connection_id = connections.add(Connection::new(*address));
            replicas.insert(replica, connection_id);
            connect_attempts.insert(replica, 0);
        }

        ClientMessageBus {
            address: config.address,
            socket: None,
            connection_pool: connections,
            replicas,
            connect_retry: RetryInfo::start(
                CONNECT_RETRY_TIMEOUT_ID,
                CONNECT_DELAY_MIN,
                connect_attempts,
            ),
            rng: ChaCha8Rng::seed_from_u64(config.seed),
            io,
        }
    }

    pub fn init(&mut self) -> Result<(), IOError> {
        self.socket = Some(self.io.open_tcp(self.address)?);
        Ok(())
    }

    pub(crate) fn is_connected(&self) -> bool {
        let has_all_connections = self.connection_pool.size() == self.replicas.len();
        if !has_all_connections {
            return false;
        }

        for (_, connection_id) in self.replicas.iter() {
            let Some(connection) = self.connection_pool.get(connection_id) else {
                return false;
            };

            if connection.status != ConnectionStatus::Connected {
                return false;
            }
        }

        true
    }

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

    fn connect_to_replicas(&mut self) -> Result<(), IOError> {
        let replicas = self.replicas.clone();
        for (replica, connection_id) in replicas.into_iter() {
            self.connect_to_replica(replica, connection_id);
        }

        Ok(())
    }

    fn connect_to_replica(&mut self, replica: usize, connection_id: usize) -> Option<usize> {
        let connection = self.connection_pool.get_mut(&connection_id)
            .unwrap_or_else(|| panic!("Connection must be initialized (replica: {replica}, connection_id: {connection_id})"));

        if let ConnectionStatus::Connected = connection.status {
            return Some(connection_id);
        }

        if !self.connect_retry.retry(&replica) {
            return None;
        }

        if let ConnectionStatus::Pending = connection.status {
            let socket = connection
                .socket
                .as_ref()
                .unwrap_or_else(|| panic!("Connection that is pending must have already a socket assigned (replica: {replica}, connection_id: {connection_id})"));

            match socket.peer_addr() {
                Ok(..) => {
                    connection.status = ConnectionStatus::Connected;
                    Some(connection_id)
                }
                Err(..) => {
                    self.connect_retry.increase(replica, &mut self.rng);
                    None
                }
            }
        } else {
            match self.io.connect(connection.address, connection_id) {
                Ok(Some(socket)) => {
                    connection.socket = Some(socket);
                    connection.peer = Some(ConnectionPeer::Replica { id: replica });
                    connection.status = ConnectionStatus::Pending;

                    self.connect_retry.reset(replica);

                    None
                }
                Ok(None) => {
                    self.connect_retry.increase(replica, &mut self.rng);
                    None
                }
                Err(..) => {
                    self.connect_retry.increase(replica, &mut self.rng);
                    None
                }
            }
        }
    }

    fn accept(&mut self) -> Result<(), IOError> {
        let socket = self
            .socket
            .as_mut()
            .expect("Client socket not available while accepting new connections");

        let connection_id = self.connection_pool.next_id();
        let accepted_conns = self.io.accept(socket, connection_id)?;

        for accepted_conn in accepted_conns {
            let mut connection = Connection::new(accepted_conn.socket.peer_addr()?);
            connection.status = ConnectionStatus::Connected;
            connection.socket = Some(accepted_conn.socket);
            connection.peer = Some(ConnectionPeer::Unknown);

            self.connection_pool.add(connection);
            assert!(accepted_conn.connection_id == self.connection_pool.size());
        }

        Ok(())
    }

    fn recv(&mut self, connection_id: usize) -> Result<Option<Message>, IOError> {
        let connection = self.connection_pool.get_mut(&connection_id).unwrap_or_else(|| {
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

        let Some(message) = decode_message(&mut connection.incoming_buffer) else {
            return Ok(None);
        };

        match connection.set_message_peer(&message) {
            ConnectionPeer::Replica { id } => {
                self.replicas.insert(*id, connection_id);
            }
            ConnectionPeer::Client { .. } => unreachable!(),
            ConnectionPeer::Unknown => {}
        };

        Ok(Some(message))
    }

    fn close(&mut self, connection_id: usize) -> Result<(), IOError> {
        let connection = self.connection_pool.remove(connection_id);
        if let Some(ConnectionPeer::Replica { id }) = connection.peer {
            self.replicas.remove(&id);
        }

        Ok(())
    }

    pub fn send_to_replica(&mut self, message: Message, replica: &usize) -> Result<(), IOError> {
        let connection_id = self
            .replicas
            .get(replica)
            .unwrap_or_else(|| panic!("Replica not found (replica: {replica})"));

        let connection = self.connection_pool.get_mut(connection_id)
            .unwrap_or_else(|| panic!("Connection for replica not found (replica: {replica}, connection_id: {connection_id})"));

        assert!(connection.status == ConnectionStatus::Connected);

        connection.queue_message(&message);

        let socket = connection
            .socket
            .as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (replica: {replica}, connection_id: {connection_id})"));

        self.io.send(socket, *connection_id)?;

        Ok(())
    }

    fn write(&mut self, connection_id: usize) -> Result<bool, IOError> {
        let connection = self.connection_pool.get_mut(&connection_id).unwrap_or_else(|| {
            panic!(
                "No connection matches for a received operation (connection_id: {connection_id})"
            )
        });

        if connection.status != ConnectionStatus::Connected {
            return Ok(false);
        }

        let socket = connection.socket.as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (connection_id: {connection_id})"));

        while let Some(buffer) = connection.outgoing_buffer.send_queue.front_mut() {
            while !buffer.is_empty() {
                let Some(written) = self.io.write(socket, buffer)? else {
                    return Ok(true);
                };

                buffer.drain(..written);
            }

            connection.outgoing_buffer.send_queue.pop_front();
        }

        Ok(false)
    }

    pub fn tick(&mut self) -> Result<Vec<Message>, IOError> {
        self.connect_to_replicas()?;
        let messages = self.run_for_ns(200)?;
        self.connect_retry.tick();

        Ok(messages)
    }
}
