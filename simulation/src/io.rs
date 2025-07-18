use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use std::{
    cell::RefCell,
    collections::{HashSet, VecDeque},
    net::SocketAddr,
    rc::Rc,
    time::Duration,
};
use vsr::io::{AcceptedConnection, Completion, IO, IOError, SocketLink};

use crate::env::Env;

pub(crate) struct FaultySocketLocal {
    env: Env,
    addr: SocketAddr,
}

pub(crate) struct FaultySocketLink {
    env: Env,
    addr: SocketAddr,
}

impl SocketLink for FaultySocketLink {
    fn peer_addr(&self) -> Result<SocketAddr, std::io::Error> {
        Ok(self.addr)
    }
}

#[derive(Debug)]
enum SimMessage {
    Message(Bytes),
    Close,
}

#[derive(Debug, Clone)]
pub(crate) struct SimQueue {
    messages: Rc<DashMap<SocketAddr, VecDeque<SimMessage>>>,
}

impl SimQueue {
    pub(crate) fn new() -> Self {
        SimQueue {
            messages: Rc::new(DashMap::new()),
        }
    }

    fn push(&self, address: SocketAddr, message: SimMessage) {
        let mut messages = self.messages.entry(address).or_default();
        messages.push_back(message);
    }

    fn pop(&self, address: &SocketAddr) -> Option<SimMessage> {
        let mut messages = self.messages.get_mut(address)?;
        messages.pop_front()
    }

    fn empty(&self, address: &SocketAddr) -> Vec<SimMessage> {
        let Some(mut messages) = self.messages.get_mut(address) else {
            return Vec::new();
        };

        messages.drain(..).collect()
    }
}

#[derive(Default, Clone, Debug)]
pub(crate) struct ConnectionLookup {
    /// Stores a key-value map of connections where the key is the source address
    /// and the value is a set of target addresses.
    connections: Rc<DashMap<SocketAddr, HashSet<SocketAddr>>>,

    /// Stores a key-value map of connection IDs where the key is the directional
    /// connection (source, target) and the value is the connection ID.
    connection_ids: Rc<DashMap<(SocketAddr, SocketAddr), usize>>,

    /// Stores the queued completions for each source address.
    completions: Rc<DashMap<SocketAddr, VecDeque<Completion>>>,
}

impl ConnectionLookup {
    pub(crate) fn new() -> Self {
        ConnectionLookup::default()
    }

    fn insert_connection(&self, source: SocketAddr, target: SocketAddr, connection_id: usize) {
        let mut connections = self.connections.entry(source).or_default();
        connections.insert(target);
        self.connection_ids.insert((source, target), connection_id);
    }

    fn get_connection_id(&self, source: SocketAddr, target: SocketAddr) -> Option<usize> {
        self.connection_ids
            .get(&(source, target))
            .map(|connection_id| *connection_id)
    }

    /// Get the addresses that are connected in one direction but not in the other one.
    fn get_partly_connected(&self, addr: SocketAddr) -> HashSet<SocketAddr> {
        let mut orphaned = HashSet::new();

        // Get outgoing connections from `addr`
        let outgoing = self
            .connections
            .get(&addr)
            .map(|targets| targets.clone())
            .unwrap_or_default();

        // Check incoming-only connections
        for entry in self.connections.iter() {
            let (source, targets) = entry.pair();

            // Skip current connections
            if source == &addr {
                continue;
            }

            // If the address is present as target address for another connection, but that
            // connection is not yet present in the outgoing connections.
            // Then it is an orphaned connection.
            if targets.contains(&addr) && !outgoing.contains(source) {
                orphaned.insert(*source);
            }
        }

        orphaned
    }

    fn remove_connection(&self, source: SocketAddr, target: SocketAddr) {
        // Remove a connection from a source to a target socket.
        // This only removes a single direction of the connection
        // e.g. if A is connected to B, and B is connected to A, and `remove_connection(A, B)`
        // i called, the B to A connection prevails.
        if let Some(mut outgoing) = self.connections.get_mut(&source) {
            outgoing.remove(&target);
        };

        self.connection_ids
            .retain(|(connection_source, connection_target), _| {
                connection_source != &source || connection_target != &target
            });
    }

    fn push_completion(&self, addr: SocketAddr, completion: Completion) {
        let mut completions = self.completions.entry(addr).or_default();
        completions.push_back(completion);
    }

    fn drain_completions(&self, addr: SocketAddr) -> Vec<Completion> {
        let mut completions = self.completions.entry(addr).or_default();
        completions.drain(..).collect()
    }
}

struct FaultyIOProbs {
    open_error: f64,
    connect_error: f64,
    accept_error: f64,
    close_error: f64,
    recv_error: f64,
    send_error: f64,
    write_error: f64,
    run_error: f64,
}

pub(crate) struct FaultyIO {
    probs: FaultyIOProbs,
    env: Env,
    address: RefCell<Option<SocketAddr>>,
    connected: ConnectionLookup,
    queue: SimQueue,
}

impl FaultyIO {
    pub(crate) fn new(queue: SimQueue, lookup: ConnectionLookup, env: Env) -> Self {
        FaultyIO {
            env,
            address: RefCell::new(None),
            // TODO: generate faulty probabilities from seed
            probs: FaultyIOProbs {
                open_error: 0.0,
                connect_error: 0.0,
                accept_error: 0.0,
                close_error: 0.0,
                recv_error: 0.0,
                send_error: 0.0,
                write_error: 0.0,
                run_error: 0.0,
            },
            connected: lookup,
            queue,
        }
    }
}

fn io_error() -> std::io::Error {
    std::io::Error::other("Simulated error")
}

impl IO for FaultyIO {
    type Local = FaultySocketLocal;
    type Link = FaultySocketLink;

    fn open_tcp(&self, addr: SocketAddr) -> Result<Self::Local, IOError> {
        if self.env.flip_coin(self.probs.open_error) {
            return Err(io_error().into());
        }

        *self.address.borrow_mut() = Some(addr);

        Ok(FaultySocketLocal {
            addr,
            env: self.env.clone(),
        })
    }

    fn connect(
        &mut self,
        addr: SocketAddr,
        connection_id: usize,
    ) -> Result<Option<Self::Link>, IOError> {
        if self.env.flip_coin(self.probs.connect_error) {
            return Err(io_error().into());
        }

        let current = self
            .address
            .borrow()
            .expect("IO needs to be initialized to receive messages");

        // If connect is successful, insert the connection and push an accept completion
        // in the target's socket.
        self.connected
            .insert_connection(current, addr, connection_id);
        self.connected.push_completion(addr, Completion::Accept);

        Ok(Some(FaultySocketLink {
            addr,
            env: self.env.clone(),
        }))
    }

    fn accept(
        &mut self,
        local: &Self::Local,
        connection_id: usize,
    ) -> Result<Vec<AcceptedConnection<Self::Link>>, IOError> {
        if local.env.flip_coin(self.probs.accept_error) {
            return Err(io_error().into());
        }

        let current = self
            .address
            .borrow()
            .expect("IO needs to be initialized to accept messages");

        assert!(current == local.addr);

        let mut accepted = Vec::new();
        let mut next_connection_id = connection_id;

        // Find the partly connected addresses due to being connected by the peer, but not accepted
        // by the current socket.
        for address in self.connected.get_partly_connected(local.addr) {
            accepted.push(AcceptedConnection {
                socket: FaultySocketLink {
                    addr: address,
                    env: self.env.clone(),
                },
                connection_id: next_connection_id,
            });
            self.connected
                .insert_connection(local.addr, address, next_connection_id);

            next_connection_id += 1;
        }

        Ok(accepted)
    }

    fn close(&self, link: &mut Self::Link) -> Result<(), IOError> {
        if link.env.flip_coin(self.probs.close_error) {
            return Err(io_error().into());
        }

        let address = self
            .address
            .borrow()
            .expect("IO needs to be initialized to accept messages");

        let target = link.peer_addr()?;

        self.queue.empty(&target);
        self.connected.remove_connection(address, target);

        // Send a close connection message to the peer connection
        let connection_id = self
            .connected
            .get_connection_id(target, address)
            .expect("Writing to a connection with an unknown connection ID");

        let message = SimMessage::Close;
        self.queue.push(target, message);
        self.connected
            .push_completion(target, Completion::Recv { connection_id });

        Ok(())
    }

    fn recv(&self, link: &mut Self::Link, buffer: &mut BytesMut) -> Result<bool, IOError> {
        if link.env.flip_coin(self.probs.recv_error) {
            return Err(io_error().into());
        }

        let address = self
            .address
            .borrow()
            .expect("IO needs to be initialized to receive messages");

        if let Some(message) = self.queue.pop(&address) {
            match message {
                SimMessage::Message(bytes) => buffer.put(bytes),
                SimMessage::Close => return Ok(true),
            };
        }

        Ok(false)
    }

    fn send(&self, link: &mut Self::Link, connection_id: usize) -> Result<(), IOError> {
        if link.env.flip_coin(self.probs.send_error) {
            return Err(io_error().into());
        }

        let address = self
            .address
            .borrow()
            .expect("IO needs to be initialized to send messages");

        self.connected
            .push_completion(address, Completion::Write { connection_id });

        Ok(())
    }

    fn write(&self, link: &mut Self::Link, bytes: &Bytes) -> Result<Option<usize>, IOError> {
        if link.env.flip_coin(self.probs.write_error) {
            return Err(io_error().into());
        }

        let address = self
            .address
            .borrow()
            .expect("IO needs to be initialized to write messages");

        let target = link.peer_addr()?;

        // Write to the peer connection
        let connection_id = self
            .connected
            .get_connection_id(target, address)
            .expect("Writing to a connection with an unknown connection ID");

        let message = SimMessage::Message(bytes.clone());
        self.queue.push(target, message);
        self.connected
            .push_completion(target, Completion::Recv { connection_id });

        Ok(Some(bytes.len()))
    }

    fn run(&mut self, _: Duration) -> Result<Vec<Completion>, IOError> {
        if self.env.flip_coin(self.probs.run_error) {
            return Err(io_error().into());
        }

        let address = self
            .address
            .borrow()
            .expect("IO needs to be initialized to run");

        let completions = self.connected.drain_completions(address);
        Ok(completions)
    }
}

#[cfg(test)]
mod tests {

    use std::{
        collections::{HashMap, HashSet},
        net::SocketAddr,
    };

    use crate::io::ConnectionLookup;

    #[test]
    fn gets_orphan_lookup() {
        // given
        let replica_a: SocketAddr = "127.0.0.1:3000".parse().unwrap();
        let replica_b: SocketAddr = "127.0.0.1:3001".parse().unwrap();
        let replica_c: SocketAddr = "127.0.0.1:3002".parse().unwrap();

        let connection_lookup = ConnectionLookup::new();

        // when
        connection_lookup.insert_connection(replica_a, replica_b, 0);

        // then
        assert_eq!(
            connection_lookup
                .get_partly_connected(replica_a)
                .into_iter()
                .collect::<Vec<_>>(),
            vec![]
        );
        assert_eq!(
            connection_lookup
                .get_partly_connected(replica_b)
                .into_iter()
                .collect::<Vec<_>>(),
            vec![replica_a]
        );
        assert_eq!(
            connection_lookup
                .get_partly_connected(replica_c)
                .into_iter()
                .collect::<Vec<_>>(),
            vec![]
        );

        // when
        connection_lookup.insert_connection(replica_b, replica_a, 0);

        // then
        assert_eq!(
            connection_lookup
                .get_partly_connected(replica_b)
                .into_iter()
                .collect::<Vec<_>>(),
            vec![]
        );

        // when
        connection_lookup.insert_connection(replica_c, replica_a, 0);

        // then
        assert_eq!(
            connection_lookup
                .get_partly_connected(replica_a)
                .into_iter()
                .collect::<Vec<_>>(),
            vec![replica_c]
        );
    }

    #[test]
    fn remove_connection() {
        // given
        let connection_lookup = ConnectionLookup::new();
        let replica_a = "127.0.0.1:3000".parse().unwrap();
        let replica_b = "127.0.0.1:3001".parse().unwrap();
        let replica_c = "127.0.0.1:3002".parse().unwrap();

        connection_lookup.insert_connection(replica_a, replica_b, 0);
        connection_lookup.insert_connection(replica_b, replica_a, 0);
        connection_lookup.insert_connection(replica_a, replica_c, 0);
        connection_lookup.insert_connection(replica_b, replica_c, 1);

        // when
        connection_lookup.remove_connection(replica_a, replica_b);

        // then
        let mut connections = HashMap::new();
        connections.insert(replica_a, HashSet::from_iter([replica_c]));
        connections.insert(replica_b, HashSet::from_iter([replica_a, replica_c]));

        let mut connection_ids = HashMap::new();
        connection_ids.insert((replica_a, replica_c), 0);
        connection_ids.insert((replica_b, replica_a), 0);
        connection_ids.insert((replica_b, replica_c), 1);

        assert_eq!(
            connection_lookup
                .connections
                .iter()
                .fold(HashMap::new(), |mut acc, connection| {
                    acc.insert(*connection.key(), connection.value().clone());
                    acc
                }),
            connections
        );
        assert_eq!(
            connection_lookup
                .connection_ids
                .iter()
                .fold(HashMap::new(), |mut acc, connection| {
                    acc.insert(*connection.key(), connection.value().clone());
                    acc
                }),
            connection_ids
        );
    }
}
