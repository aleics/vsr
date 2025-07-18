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
        cell::RefCell,
        collections::{HashMap, HashSet},
        net::SocketAddr,
        time::Duration,
    };

    use bytes::{Bytes, BytesMut};
    use vsr::io::{Completion, IO, SocketLink};

    use crate::{
        env::Env,
        io::{
            ConnectionLookup, FaultyIO, FaultyIOProbs, FaultySocketLink, FaultySocketLocal,
            SimMessage, SimQueue,
        },
    };

    fn env() -> Env {
        Env::new(42)
    }

    fn faulty_io() -> FaultyIO {
        let env = env();
        let queue = SimQueue::new();
        let lookup = ConnectionLookup::new();
        FaultyIO::new(queue, lookup, env)
    }

    #[test]
    fn test_open_tcp_success() {
        // given
        let io = faulty_io();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // when
        let result = io.open_tcp(addr);

        // then
        assert!(result.is_ok());

        let local = result.unwrap();
        assert_eq!(local.addr, addr);
        assert_eq!(*io.address.borrow(), Some(addr));
    }

    #[test]
    fn test_connect_success() {
        // given
        let mut io = faulty_io();
        let local_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let _ = io.open_tcp(local_addr).unwrap();

        // when
        let result = io.connect(remote_addr, 1);

        // then
        assert!(result.is_ok());

        let link = result.unwrap();
        assert!(link.is_some());
        assert_eq!(link.unwrap().addr, remote_addr);
    }

    #[test]
    fn test_accept_success() {
        // given
        let mut io = faulty_io();
        let local_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        // Set up the scenario: remote connects to local
        let local = io.open_tcp(local_addr).unwrap();

        // Simulate remote connection
        io.connected.insert_connection(remote_addr, local_addr, 1);

        // when
        let result = io.accept(&local, 2);
        assert!(result.is_ok());

        // then
        let accepted = result.unwrap();
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0].socket.addr, remote_addr);
        assert_eq!(accepted[0].connection_id, 2);
    }

    #[test]
    fn test_accept_multiple_connections() {
        // given
        let mut io = faulty_io();
        let local_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let remote_addr1: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let remote_addr2: SocketAddr = "127.0.0.1:8082".parse().unwrap();

        let local = io.open_tcp(local_addr).unwrap();

        // Simulate multiple remote connections
        io.connected.insert_connection(remote_addr1, local_addr, 1);
        io.connected.insert_connection(remote_addr2, local_addr, 2);

        // when
        let result = io.accept(&local, 3);
        assert!(result.is_ok());

        // then
        let accepted = result.unwrap();
        assert_eq!(accepted.len(), 2);

        // Check that connection IDs are sequential
        let mut connection_ids: Vec<_> = accepted.iter().map(|a| a.connection_id).collect();
        connection_ids.sort();
        assert_eq!(connection_ids, vec![3, 4]);
    }

    #[test]
    fn test_close_success() {
        // given
        let mut io = faulty_io();
        let local_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        // Set up a connection
        let _local = io.open_tcp(local_addr).unwrap();
        let mut link = io.connect(remote_addr, 1).unwrap().unwrap();

        // Establish bidirectional connection
        io.connected.insert_connection(remote_addr, local_addr, 2);

        // Verify connection exists before closing
        assert_eq!(
            io.connected.get_connection_id(local_addr, remote_addr),
            Some(1)
        );
        assert_eq!(
            io.connected.get_connection_id(remote_addr, local_addr),
            Some(2)
        );

        // when
        let result = io.close(&mut link);

        // then
        assert!(result.is_ok());

        // Verify the connection from local to remote has been removed
        assert_eq!(
            io.connected.get_connection_id(local_addr, remote_addr),
            None
        );

        // Verify the reverse connection still exists (only one direction is removed)
        assert_eq!(
            io.connected.get_connection_id(remote_addr, local_addr),
            Some(2)
        );

        // Verify a close message was sent to the peer
        let message = io.queue.pop(&remote_addr);
        assert!(matches!(message, Some(SimMessage::Close)));

        // Verify a recv completion was queued for the remote peer
        let completions = io.connected.drain_completions(remote_addr);
        assert!(!completions.is_empty());
        // Check that at least one completion is a Recv with connection_id 2
        let recv_completion = completions
            .iter()
            .find(|c| matches!(c, Completion::Recv { connection_id: 2 }));
        assert!(
            recv_completion.is_some(),
            "Expected Recv completion with connection_id 2"
        );
    }

    #[test]
    fn test_recv_with_message() {
        // given
        let mut io = faulty_io();
        let local_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        // Set up connection and queue message
        let _local = io.open_tcp(local_addr).unwrap();
        let mut link = io.connect(remote_addr, 1).unwrap().unwrap();

        let test_message = Bytes::from("test message");
        io.queue
            .push(local_addr, SimMessage::Message(test_message.clone()));

        let mut buffer = BytesMut::new();

        // when
        let result = io.recv(&mut link, &mut buffer);

        // then
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Should not close connection
        assert_eq!(buffer.as_ref(), test_message.as_ref());
    }

    #[test]
    fn test_recv_with_close_message() {
        // given
        let mut io = faulty_io();
        let local_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let _local = io.open_tcp(local_addr).unwrap();
        let mut link = io.connect(remote_addr, 1).unwrap().unwrap();

        io.queue.push(local_addr, SimMessage::Close);

        let mut buffer = BytesMut::new();

        // when
        let result = io.recv(&mut link, &mut buffer);

        // then
        assert!(result.is_ok());
        assert!(result.unwrap()); // Should close connection
    }

    #[test]
    fn test_recv_empty_queue() {
        // given
        let mut io = faulty_io();
        let local_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let _local = io.open_tcp(local_addr).unwrap();
        let mut link = io.connect(remote_addr, 1).unwrap().unwrap();

        let mut buffer = BytesMut::new();

        // when
        let result = io.recv(&mut link, &mut buffer);

        // then
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Should not close connection
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_send_success() {
        // given
        let mut io = faulty_io();
        let local_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let _local = io.open_tcp(local_addr).unwrap();
        let mut link = io.connect(remote_addr, 1).unwrap().unwrap();

        // when
        let result = io.send(&mut link, 1);

        // then
        assert!(result.is_ok());

        // Check that a write completion was queued
        let completions = io.connected.drain_completions(local_addr);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0], Completion::Write { connection_id: 1 });
    }

    #[test]
    fn test_write_success() {
        // given
        let mut io = faulty_io();
        let local_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let _local = io.open_tcp(local_addr).unwrap();
        let mut link = io.connect(remote_addr, 1).unwrap().unwrap();

        // Set up bidirectional connection
        io.connected.insert_connection(remote_addr, local_addr, 2);

        let test_data = Bytes::from("test data");

        // when
        let result = io.write(&mut link, &test_data);

        // then
        assert!(result.is_ok());
        let written = result.unwrap();
        assert_eq!(written, Some(test_data.len()));

        // Check that message was queued for remote
        let message = io.queue.pop(&remote_addr);
        assert!(message.is_some());

        // Check that recv completion was queued for remote
        let completions = io.connected.drain_completions(remote_addr);

        // The first completion is Accept from connect(), the second is Recv from write()
        assert_eq!(completions[0], Completion::Accept);
        assert_eq!(completions[1], Completion::Recv { connection_id: 2 });
    }

    #[test]
    fn test_run_success() {
        // given
        let mut io = faulty_io();
        let local_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        let _local = io.open_tcp(local_addr).unwrap();

        // Queue some completions
        io.connected.push_completion(local_addr, Completion::Accept);
        io.connected
            .push_completion(local_addr, Completion::Recv { connection_id: 1 });

        // when
        let result = io.run(Duration::from_millis(100));

        // then
        assert!(result.is_ok());

        let completions = result.unwrap();
        assert_eq!(completions.len(), 2);
        assert_eq!(completions[0], Completion::Accept);
        assert_eq!(completions[1], Completion::Recv { connection_id: 1 });

        // Check that completions were drained
        let remaining = io.connected.drain_completions(local_addr);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_socket_link_peer_addr() {
        // given
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let env = env();
        let link = FaultySocketLink { env, addr };

        // when
        let result = link.peer_addr();

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), addr);
    }

    #[test]
    #[should_panic(expected = "IO needs to be initialized")]
    fn test_io_without_initialization() {
        // given
        let mut io = faulty_io();
        let remote_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        // expect
        let _result = io.connect(remote_addr, 1);
    }

    #[test]
    fn test_sim_queue_operations() {
        // given
        let queue = SimQueue::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Test push and pop
        let message1 = SimMessage::Message(Bytes::from("msg1"));
        let message2 = SimMessage::Close;

        queue.push(addr, message1);
        queue.push(addr, message2);

        // expect
        let popped1 = queue.pop(&addr);
        assert!(popped1.is_some());

        let popped2 = queue.pop(&addr);
        assert!(popped2.is_some());

        let popped3 = queue.pop(&addr);
        assert!(popped3.is_none());
    }

    #[test]
    fn test_sim_queue_empty() {
        // given
        let queue = SimQueue::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        let message1 = SimMessage::Message(Bytes::from("msg1"));
        let message2 = SimMessage::Close;

        queue.push(addr, message1);
        queue.push(addr, message2);

        // when
        let all_messages = queue.empty(&addr);

        // then
        assert_eq!(all_messages.len(), 2);

        // Queue should be empty now
        let popped = queue.pop(&addr);
        assert!(popped.is_none());
    }

    #[test]
    fn test_connection_lookup_completions() {
        // given
        let lookup = ConnectionLookup::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        lookup.push_completion(addr, Completion::Accept);
        lookup.push_completion(addr, Completion::Recv { connection_id: 1 });

        // when
        let completions = lookup.drain_completions(addr);

        // then
        assert_eq!(completions.len(), 2);

        // Should be empty after drain
        let remaining = lookup.drain_completions(addr);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_connection_lookup_get_connection_id() {
        // given
        let lookup = ConnectionLookup::new();
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        lookup.insert_connection(addr1, addr2, 42);

        // expect
        let connection_id = lookup.get_connection_id(addr1, addr2);
        assert_eq!(connection_id, Some(42));

        let no_connection = lookup.get_connection_id(addr2, addr1);
        assert_eq!(no_connection, None);
    }

    #[test]
    fn test_get_orphan_lookup() {
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
    fn test_faulty_io_with_fault_injection() {
        // given
        let env = env();
        let queue = SimQueue::new();
        let lookup = ConnectionLookup::new();
        let io = FaultyIO {
            env: env.clone(),
            address: RefCell::new(None),
            probs: FaultyIOProbs {
                open_error: 1.0, // Always fail
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
        };

        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // when
        let result = io.open_tcp(addr);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn test_faulty_io_connect_error() {
        // given
        let env = env();
        let queue = SimQueue::new();
        let lookup = ConnectionLookup::new();
        let mut io = FaultyIO {
            env: env.clone(),
            address: RefCell::new(Some("127.0.0.1:8080".parse().unwrap())),
            probs: FaultyIOProbs {
                open_error: 0.0,
                connect_error: 1.0, // Always fail
                accept_error: 0.0,
                close_error: 0.0,
                recv_error: 0.0,
                send_error: 0.0,
                write_error: 0.0,
                run_error: 0.0,
            },
            connected: lookup,
            queue,
        };

        let addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        // when
        let result = io.connect(addr, 1);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn test_faulty_io_accept_error() {
        // given
        let env = env();
        let queue = SimQueue::new();
        let lookup = ConnectionLookup::new();
        let mut io = FaultyIO {
            env: env.clone(),
            address: RefCell::new(Some("127.0.0.1:8080".parse().unwrap())),
            probs: FaultyIOProbs {
                open_error: 0.0,
                connect_error: 0.0,
                accept_error: 1.0, // Always fail
                close_error: 0.0,
                recv_error: 0.0,
                send_error: 0.0,
                write_error: 0.0,
                run_error: 0.0,
            },
            connected: lookup,
            queue,
        };

        let local = FaultySocketLocal {
            env: env.clone(),
            addr: "127.0.0.1:8080".parse().unwrap(),
        };

        // when
        let result = io.accept(&local, 1);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn test_faulty_io_recv_error() {
        // given
        let env = env();
        let queue = SimQueue::new();
        let lookup = ConnectionLookup::new();
        let io = FaultyIO {
            env: env.clone(),
            address: RefCell::new(Some("127.0.0.1:8080".parse().unwrap())),
            probs: FaultyIOProbs {
                open_error: 0.0,
                connect_error: 0.0,
                accept_error: 0.0,
                close_error: 0.0,
                recv_error: 1.0, // Always fail
                send_error: 0.0,
                write_error: 0.0,
                run_error: 0.0,
            },
            connected: lookup,
            queue,
        };

        let mut link = FaultySocketLink {
            env: env.clone(),
            addr: "127.0.0.1:8081".parse().unwrap(),
        };

        let mut buffer = BytesMut::new();

        // when
        let result = io.recv(&mut link, &mut buffer);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn test_faulty_io_send_error() {
        // given
        let env = env();
        let queue = SimQueue::new();
        let lookup = ConnectionLookup::new();
        let io = FaultyIO {
            env: env.clone(),
            address: RefCell::new(Some("127.0.0.1:8080".parse().unwrap())),
            probs: FaultyIOProbs {
                open_error: 0.0,
                connect_error: 0.0,
                accept_error: 0.0,
                close_error: 0.0,
                recv_error: 0.0,
                send_error: 1.0, // Always fail
                write_error: 0.0,
                run_error: 0.0,
            },
            connected: lookup,
            queue,
        };

        let mut link = FaultySocketLink {
            env: env.clone(),
            addr: "127.0.0.1:8081".parse().unwrap(),
        };

        // when
        let result = io.send(&mut link, 1);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn test_faulty_io_write_error() {
        // given
        let env = env();
        let queue = SimQueue::new();
        let lookup = ConnectionLookup::new();
        let io = FaultyIO {
            env: env.clone(),
            address: RefCell::new(Some("127.0.0.1:8080".parse().unwrap())),
            probs: FaultyIOProbs {
                open_error: 0.0,
                connect_error: 0.0,
                accept_error: 0.0,
                close_error: 0.0,
                recv_error: 0.0,
                send_error: 0.0,
                write_error: 1.0, // Always fail
                run_error: 0.0,
            },
            connected: lookup,
            queue,
        };

        let mut link = FaultySocketLink {
            env: env.clone(),
            addr: "127.0.0.1:8081".parse().unwrap(),
        };

        let bytes = Bytes::from("test");

        // when
        let result = io.write(&mut link, &bytes);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn test_faulty_io_run_error() {
        // given
        let env = env();
        let queue = SimQueue::new();
        let lookup = ConnectionLookup::new();
        let mut io = FaultyIO {
            env: env.clone(),
            address: RefCell::new(Some("127.0.0.1:8080".parse().unwrap())),
            probs: FaultyIOProbs {
                open_error: 0.0,
                connect_error: 0.0,
                accept_error: 0.0,
                close_error: 0.0,
                recv_error: 0.0,
                send_error: 0.0,
                write_error: 0.0,
                run_error: 1.0, // Always fail
            },
            connected: lookup,
            queue,
        };

        // when
        let result = io.run(Duration::from_millis(100));

        // then
        assert!(result.is_err());
    }

    #[test]
    fn test_faulty_io_close_error() {
        // given
        let env = env();
        let queue = SimQueue::new();
        let lookup = ConnectionLookup::new();
        let io = FaultyIO {
            env: env.clone(),
            address: RefCell::new(Some("127.0.0.1:8080".parse().unwrap())),
            probs: FaultyIOProbs {
                open_error: 0.0,
                connect_error: 0.0,
                accept_error: 0.0,
                close_error: 1.0, // Always fail
                recv_error: 0.0,
                send_error: 0.0,
                write_error: 0.0,
                run_error: 0.0,
            },
            connected: lookup,
            queue,
        };

        let mut link = FaultySocketLink {
            env: env.clone(),
            addr: "127.0.0.1:8081".parse().unwrap(),
        };

        // when
        let result = io.close(&mut link);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_connection() {
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
                    acc.insert(*connection.key(), *connection.value());
                    acc
                }),
            connection_ids
        );
    }
}
