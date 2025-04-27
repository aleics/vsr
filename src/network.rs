use std::collections::HashMap;

use crossbeam::channel::{Receiver, Sender, unbounded};

use crate::replica::{Log, ReplicaError};

#[derive(Debug, Clone, PartialEq)]
pub enum Message<I> {
    Request(RequestMessage<I>),
    Prepare(PrepareMessage<I>),
    PrepareOk(PrepareOkMessage),
    Commit(CommitMessage),
    GetState(GetStateMessage),
    NewState(NewStateMessage<I>),
    StartViewChange(StartViewChangeMessage),
    DoViewChange(DoViewChangeMessage<I>),
    StartView(StartViewMessage<I>),
    Recovery(RecoveryMessage),
    RecoveryResponse(RecoveryResponseMessage<I>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RequestMessage<I> {
    pub(crate) view: usize,
    pub(crate) request_number: usize,
    pub(crate) client_id: usize,
    pub(crate) operation: I,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PrepareMessage<I> {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) request: RequestMessage<I>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PrepareOkMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) replica_number: usize,
    pub(crate) client_id: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommitMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GetStateMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) replica_number: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewStateMessage<I> {
    pub(crate) view: usize,
    pub(crate) log_after_operation: Log<I>,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StartViewChangeMessage {
    pub(crate) new_view: usize,
    pub(crate) replica_number: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DoViewChangeMessage<I> {
    pub(crate) old_view: usize,
    pub(crate) new_view: usize,
    pub(crate) log: Log<I>,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) replica_number: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StartViewMessage<I> {
    pub(crate) view: usize,
    pub(crate) log: Log<I>,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryMessage {
    pub(crate) replica_number: usize,
    pub(crate) nonce: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryResponseMessage<I> {
    pub(crate) view: usize,
    pub(crate) nonce: u64,
    pub(crate) primary: Option<RecoveryPrimaryResponse<I>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryPrimaryResponse<I> {
    pub(crate) log: Log<I>,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplyMessage<O> {
    pub(crate) view: usize,
    pub(crate) request_number: usize,
    pub(crate) result: O,
}

pub(crate) struct AttachedChannel<I, O> {
    pub(crate) client_id: usize,
    pub(crate) channel: (Sender<RequestMessage<I>>, Receiver<ReplyMessage<O>>),
}

#[derive(Clone)]
pub(crate) struct ClientConnection<I, O> {
    /// Incoming messages from the client to the replica. The sender is used by the client to connect to the replica.
    /// The receiver is used to receive messages from the client
    pub(crate) incoming: (Sender<RequestMessage<I>>, Receiver<RequestMessage<I>>),

    /// Outgoing messages from the replica to the client.
    pub(crate) outgoing: Vec<Sender<ReplyMessage<O>>>,
}

impl<I, O> ClientConnection<I, O>
where
    I: Clone + Send,
    O: Send,
{
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
        message: ReplyMessage<O>,
        client_id: usize,
    ) -> Result<(), ReplicaError> {
        let Some(sender) = self.outgoing.get(client_id) else {
            return Err(ReplicaError::Network);
        };

        sender.send(message).map_err(|_| ReplicaError::Network)
    }
}

pub trait Network {
    type Input;
    type Output;

    fn send(&self, message: Message<Self::Input>, replica: &usize) -> Result<(), ReplicaError>;

    fn broadcast(&self, message: Message<Self::Input>) -> Result<(), ReplicaError>;

    fn send_client(
        &self,
        message: ReplyMessage<Self::Output>,
        client_id: usize,
    ) -> Result<(), ReplicaError>;
}

pub struct ReplicaNetwork<I, O> {
    pub(crate) client: ClientConnection<I, O>,
    pub(crate) incoming: Receiver<Message<I>>,
    pub(crate) other: HashMap<usize, Sender<Message<I>>>,
}

pub(crate) type MessageChannel<I> = (Sender<Message<I>>, Receiver<Message<I>>);

impl<I, O> ReplicaNetwork<I, O>
where
    I: Clone + Send,
    O: Send,
{
    pub(crate) fn for_replica(
        replica: usize,
        client: ClientConnection<I, O>,
        channels: &[MessageChannel<I>],
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
}

impl<I, O> Network for ReplicaNetwork<I, O>
where
    I: Clone + Send,
    O: Send,
{
    type Input = I;
    type Output = O;

    fn send(&self, message: Message<I>, replica: &usize) -> Result<(), ReplicaError> {
        let sender = self
            .other
            .get(replica)
            .expect("Could not send message: replica not found in the network");

        sender.send(message).map_err(|_| ReplicaError::Network)
    }

    fn broadcast(&self, message: Message<I>) -> Result<(), ReplicaError> {
        for sender in self.other.values() {
            sender
                .send(message.clone())
                .map_err(|_| ReplicaError::Network)?;
        }
        Ok(())
    }

    fn send_client(
        &self,
        message: ReplyMessage<Self::Output>,
        client_id: usize,
    ) -> Result<(), ReplicaError> {
        self.client.send_out(message, client_id)
    }
}
