use std::collections::HashMap;

use bincode::{Decode, Encode};
use crossbeam::channel::{Receiver, Sender, unbounded};

use crate::{
    OPERATION_SIZE_MAX,
    replica::{Log, ReplicaError},
};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum Message {
    Request(RequestMessage),
    Prepare(PrepareMessage),
    PrepareOk(PrepareOkMessage),
    Commit(CommitMessage),
    GetState(GetStateMessage),
    NewState(NewStateMessage),
    StartViewChange(StartViewChangeMessage),
    DoViewChange(DoViewChangeMessage),
    StartView(StartViewMessage),
    Recovery(RecoveryMessage),
    RecoveryResponse(RecoveryResponseMessage),
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RequestMessage {
    pub view: usize,
    pub request_number: usize,
    pub client_id: usize,
    pub operation: Operation,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct PrepareMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) request: RequestMessage,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct PrepareOkMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) replica_number: usize,
    pub(crate) client_id: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct CommitMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct GetStateMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) replica_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct NewStateMessage {
    pub(crate) view: usize,
    pub(crate) log_after_operation: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct StartViewChangeMessage {
    pub(crate) new_view: usize,
    pub(crate) replica_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct DoViewChangeMessage {
    pub(crate) old_view: usize,
    pub(crate) new_view: usize,
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) replica_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct StartViewMessage {
    pub(crate) view: usize,
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RecoveryMessage {
    pub(crate) replica_number: usize,
    pub(crate) nonce: u64,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RecoveryResponseMessage {
    pub(crate) view: usize,
    pub(crate) nonce: u64,
    pub(crate) primary: Option<RecoveryPrimaryResponse>,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RecoveryPrimaryResponse {
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct ReplyMessage {
    pub(crate) view: usize,
    pub(crate) request_number: usize,
    pub(crate) result: Operation,
}

pub type Operation = [u8; OPERATION_SIZE_MAX];

pub(crate) struct AttachedChannel {
    pub(crate) client_id: usize,
    pub(crate) channel: (Sender<RequestMessage>, Receiver<ReplyMessage>),
}

#[derive(Clone)]
pub(crate) struct ClientConnection {
    /// Incoming messages from the client to the replica. The sender is used by the client to connect to the replica.
    /// The receiver is used to receive messages from the client
    pub(crate) incoming: (Sender<RequestMessage>, Receiver<RequestMessage>),

    /// Outgoing messages from the replica to the client.
    pub(crate) outgoing: Vec<Sender<ReplyMessage>>,
}

impl ClientConnection {
    pub(crate) fn new() -> Self {
        ClientConnection {
            incoming: unbounded(),
            outgoing: Vec::new(),
        }
    }

    pub(crate) fn attach(&mut self) -> AttachedChannel {
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
        message: ReplyMessage,
        client_id: usize,
    ) -> Result<(), ReplicaError> {
        let Some(sender) = self.outgoing.get(client_id) else {
            return Err(ReplicaError::Network);
        };

        sender.send(message).map_err(|_| ReplicaError::Network)
    }
}

pub trait Network {
    fn send(&self, message: Message, replica: &usize) -> Result<(), ReplicaError>;

    fn broadcast(&self, message: Message) -> Result<(), ReplicaError>;

    fn send_client(&self, message: ReplyMessage, client_id: usize) -> Result<(), ReplicaError>;
}

pub struct ReplicaNetwork {
    pub(crate) client: ClientConnection,
    pub(crate) incoming: Receiver<Message>,
    pub(crate) other: HashMap<usize, Sender<Message>>,
}

pub(crate) type MessageChannel = (Sender<Message>, Receiver<Message>);

impl ReplicaNetwork {
    pub(crate) fn for_replica(
        replica: usize,
        client: ClientConnection,
        channels: &[MessageChannel],
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

impl Network for ReplicaNetwork {
    fn send(&self, message: Message, replica: &usize) -> Result<(), ReplicaError> {
        let sender = self
            .other
            .get(replica)
            .expect("Could not send message: replica not found in the network");

        sender.send(message).map_err(|_| ReplicaError::Network)
    }

    fn broadcast(&self, message: Message) -> Result<(), ReplicaError> {
        for sender in self.other.values() {
            sender
                .send(message.clone())
                .map_err(|_| ReplicaError::Network)?;
        }
        Ok(())
    }

    fn send_client(&self, message: ReplyMessage, client_id: usize) -> Result<(), ReplicaError> {
        self.client.send_out(message, client_id)
    }
}
