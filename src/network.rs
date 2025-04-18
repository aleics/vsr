use std::collections::HashMap;

use crossbeam::channel::{Receiver, Sender, unbounded};

use crate::replica::ReplicaError;

#[derive(Debug, Clone)]
pub(crate) enum Message<I: Clone, O: Clone> {
    Request(RequestMessage<I>),
    Prepare(PrepareMessage<I>),
    PrepareOk(PrepareOkMessage),
    Commit(CommitMessage),
    Reply(ReplyMessage<O>),
}

#[derive(Debug, Clone)]
pub(crate) struct RequestMessage<I: Clone> {
    pub(crate) view: usize,
    pub(crate) request_number: usize,
    pub(crate) client_id: usize,
    pub(crate) operation: I,
}

#[derive(Debug, Clone)]
pub(crate) struct PrepareMessage<I: Clone> {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) request: RequestMessage<I>,
}

#[derive(Debug, Clone)]
pub(crate) struct PrepareOkMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) replica_number: usize,
    pub(crate) client_id: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct CommitMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplyMessage<O> {
    pub(crate) view: usize,
    pub(crate) request_number: usize,
    pub(crate) result: O,
}

#[derive(Debug, Clone)]
pub(crate) enum ClientMessage<I: Clone> {
    Request(RequestMessage<I>),
}

pub(crate) struct AttachedChannel<I: Clone, O: Clone> {
    pub(crate) client_id: usize,
    pub(crate) channel: (Sender<ClientMessage<I>>, Receiver<Message<I, O>>),
}

#[derive(Clone)]
pub(crate) struct ClientConnection<I: Clone, O: Clone> {
    /// Incoming messages from the client to the replica. The sender is used by the client to connect to the replica.
    /// The receiver is used to receive messages from the client
    pub(crate) incoming: (Sender<ClientMessage<I>>, Receiver<ClientMessage<I>>),

    /// Outgoing messages from the replica to the client.
    pub(crate) outgoing: Vec<Sender<Message<I, O>>>,
}

impl<I: Clone, O: Clone> ClientConnection<I, O> {
    pub(crate) fn new() -> Self {
        ClientConnection {
            incoming: unbounded(),
            outgoing: Vec::new(),
        }
    }

    pub(crate) fn attach(&mut self) -> AttachedChannel<I, O> {
        let outgoing = unbounded();

        let client_id = self.outgoing.len();
        self.outgoing.push(outgoing.0);

        AttachedChannel {
            client_id,
            channel: (self.incoming.0.clone(), outgoing.1),
        }
    }

    pub(crate) fn send_out(
        &self,
        message: Message<I, O>,
        client_id: usize,
    ) -> Result<(), ReplicaError> {
        let Some(sender) = self.outgoing.get(client_id) else {
            return Err(ReplicaError::NetworkError);
        };

        sender.send(message).map_err(|_| ReplicaError::NetworkError)
    }
}

pub(crate) struct ReplicaNetwork<I: Clone, O: Clone> {
    pub(crate) client: ClientConnection<I, O>,
    pub(crate) incoming: Receiver<Message<I, O>>,
    pub(crate) other: HashMap<usize, Sender<Message<I, O>>>,
}

pub(crate) type MessageChannel<I, O> = (Sender<Message<I, O>>, Receiver<Message<I, O>>);

impl<I: Clone, O: Clone> ReplicaNetwork<I, O> {
    pub(crate) fn for_replica(
        replica: usize,
        client: ClientConnection<I, O>,
        channels: &[MessageChannel<I, O>],
    ) -> Self {
        let mut other = HashMap::with_capacity(channels.len() - 1);
        let mut incoming: Option<Receiver<_>> = None;

        for (i, (tx, rx)) in channels.iter().enumerate() {
            if i != replica {
                other.insert(i, tx.clone());
            } else {
                incoming = Some(rx.clone());
            }
        }

        ReplicaNetwork {
            client,
            incoming: incoming.expect("Replica index not found in the network channels"),
            other,
        }
    }

    pub(crate) fn send(&self, message: Message<I, O>, replica: &usize) -> Result<(), ReplicaError> {
        let sender = self
            .other
            .get(replica)
            .expect("Could not send message: replica not found in the network");

        sender.send(message).map_err(|_| ReplicaError::NetworkError)
    }

    pub(crate) fn broadcast(&self, message: &Message<I, O>) -> Result<(), ReplicaError> {
        for sender in self.other.values() {
            sender
                .send(message.clone())
                .map_err(|_| ReplicaError::NetworkError)?;
        }
        Ok(())
    }
}
