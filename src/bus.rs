use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::{cmp, collections::HashMap, time::Duration};

use crate::{
    ReplicaConfig,
    clock::Timeout,
    io::{IO, Operation, RecvBody},
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

    fn retry(&self) -> bool {
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

struct Connection {
    address: SocketAddr,
    status: ConnectionStatus,
    socket: Option<TcpStream>,
}

impl Connection {
    fn new(address: SocketAddr) -> Self {
        Connection {
            address,
            status: ConnectionStatus::Free,
            socket: None,
        }
    }
}

#[derive(Debug, PartialEq)]
enum ConnectionStatus {
    Connected,
    Connecting,
    Free,
}

pub struct MessageBus<I: IO> {
    address: SocketAddr,
    socket: Option<TcpListener>, // TODO: Define this as a connection?
    connections: Vec<Connection>,
    replicas: HashMap<usize, usize>,
    connect_retry: RetryInfo,
    rng: ChaCha8Rng,
    io: I,
}

// TODO: message bus for the client
impl<I: IO> MessageBus<I> {
    pub fn new(config: &ReplicaConfig, io: I, seed: u64) -> std::io::Result<Self> {
        let current_address = config.addresses.get(config.replica).unwrap();

        let mut connections = Vec::new();
        let mut replicas = HashMap::with_capacity(config.addresses.len() - 1);
        let mut connect_attempts = HashMap::with_capacity(config.addresses.len() - 1);

        for (replica, address) in config.addresses.iter().enumerate() {
            if replica != config.replica {
                let index = connections.len();

                connections.push(Connection::new(*address));
                replicas.insert(replica, index);
                connect_attempts.insert(replica, 0);
            }
        }

        Ok(MessageBus {
            address: *current_address,
            socket: None,
            connections,
            replicas,
            connect_retry: RetryInfo::start(
                CONNECT_RETRY_TIMEOUT_ID,
                CONNECT_DELAY_MIN,
                connect_attempts,
            ),
            rng: ChaCha8Rng::seed_from_u64(seed),
            io,
        })
    }

    pub fn init(&mut self) -> std::io::Result<()> {
        self.socket = Some(self.io.open_tcp(self.address)?);
        Ok(())
    }

    fn run_for_ns(&mut self, nanos: u64) -> std::io::Result<()> {
        let duration = Duration::from_nanos(nanos);

        for operation in self.io.run(duration)? {
            match operation {
                Operation::Accept => self.accept()?,
                Operation::Recv { connection } => self.recv(connection)?,
            }
        }

        Ok(())
    }

    fn connect_to_other_replicas(&mut self) -> std::io::Result<()> {
        let replicas = self.replicas.clone();
        for (replica, connection_id) in replicas.into_iter() {
            self.connect_to_replica(replica, connection_id);
        }

        Ok(())
    }

    fn connect_to_replica(&mut self, replica: usize, connection_id: usize) -> Option<usize> {
        let connection = self.connections.get_mut(connection_id).unwrap_or_else(|| panic!(
            "Connection must be initialized (replica: {replica}, connection_id: {connection_id})"
        ));

        if let ConnectionStatus::Connected = connection.status {
            return Some(connection_id);
        }

        if !self.connect_retry.retry() {
            return None;
        }

        if let ConnectionStatus::Connecting = connection.status {
            let socket = connection
                .socket
                .as_ref()
                .unwrap_or_else(|| panic!(
                    "Connection that is connecting must have already a socket assigned (replica: {replica}, connection_id: {connection_id})"
                ));

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
                Ok(socket) => {
                    connection.socket = Some(socket);
                    connection.status = ConnectionStatus::Connecting;

                    self.connect_retry.reset(replica);

                    None
                }
                Err(..) => {
                    self.connect_retry.increase(replica, &mut self.rng);
                    None
                }
            }
        }
    }

    fn accept(&mut self) -> std::io::Result<()> {
        let socket = self
            .socket
            .as_ref()
            .expect("Replica socket not available while accepting new connections");

        let connection_id = self.connections.len();
        let socket = self.io.accept(socket, connection_id)?;

        let connection = Connection {
            address: socket.peer_addr()?,
            status: ConnectionStatus::Connected,
            socket: Some(socket),
        };

        self.connections.push(connection);

        Ok(())
    }

    fn recv(&mut self, connection_id: usize) -> std::io::Result<()> {
        let connection = self.connections.get_mut(connection_id).unwrap_or_else(|| {
            panic!(
                "No connection matches for a received operation (connection_id: {connection_id})"
            )
        });

        assert!(connection.status == ConnectionStatus::Connected);

        let socket = connection.socket.as_mut().unwrap_or_else(|| panic!(
            "Connection that is connected must have already a socket assigned (connection_id: {connection_id})",
        ));

        match self.io.recv(socket, connection_id)? {
            RecvBody::Message { message } => {
                println!("Message received {message}");
            }
            RecvBody::Close => {
                self.connections.remove(connection_id);
                println!("Connection closed")
            }
            RecvBody::SocketOccupied => return Ok(()),
        }

        Ok(())
    }

    pub fn send(&mut self, replica: usize, message: &str) -> std::io::Result<()> {
        let connection_id = self
            .replicas
            .get(&replica)
            .unwrap_or_else(|| panic!("Replica not found (replica: {replica})"));

        let connection = self.connections.get_mut(*connection_id).unwrap_or_else(|| panic!(
            "Connection for replica not found (replica: {replica}, connection_id: {connection_id})"
        ));

        if connection.status != ConnectionStatus::Connected {
            return Ok(());
        }

        let socket = connection
            .socket
            .as_mut()
            .unwrap_or_else(|| panic!("Connection that is connected must have already a socket assigned (replica: {replica}, connection_id: {connection_id})"));

        self.io.send(message, socket)?;

        Ok(())
    }

    pub fn tick(&mut self) -> std::io::Result<()> {
        self.connect_to_other_replicas()?;
        self.run_for_ns(200)?;
        self.connect_retry.tick();

        Ok(())
    }
}
