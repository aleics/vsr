use std::collections::HashMap;

use crossbeam::channel::{Receiver, Sender, unbounded};

use crate::replica::ReplicaError;

#[derive(Debug, Clone)]
pub(crate) enum Message {
    Request(RequestMessage),
    Prepare(PrepareMessage),
    PrepareOk(PrepareOkMessage),
    Commit(CommitMessage),
    Reply(ReplyMessage),
}

#[derive(Debug, Clone)]
pub(crate) struct RequestMessage {
    pub(crate) view: usize,
    pub(crate) request_number: usize,
    pub(crate) client_id: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct PrepareMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) request: RequestMessage,
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
pub(crate) struct ReplyMessage {
    pub(crate) view: usize,
    pub(crate) request_number: usize,
    pub(crate) result: (),
}

#[derive(Debug, Clone)]
pub(crate) enum ClientMessage {
    Request(RequestMessage),
    Response(ClientResponse),
}

#[derive(Debug, Clone)]
pub(crate) struct ClientResponse {
    pub(crate) view: usize,
    pub(crate) request_number: usize,
    pub(crate) result: (),
}

#[derive(Clone)]
pub(crate) struct ClientConnection {
    /// Incoming messages from the client to the replica. The sender is used by the client to connect to the replica.
    /// The receiver is used to receive messages from the client
    pub(crate) incoming: (Sender<ClientMessage>, Receiver<ClientMessage>),

    /// Outgoing messages from the replica to the client.
    pub(crate) outgoing: Vec<Sender<Message>>,
}

impl ClientConnection {
    pub(crate) fn new() -> Self {
        ClientConnection {
            incoming: unbounded(),
            outgoing: Vec::new(),
        }
    }

    pub(crate) fn attach(&mut self) -> (usize, (Sender<ClientMessage>, Receiver<Message>)) {
        let outgoing = unbounded();

        let client_id = self.outgoing.len();
        self.outgoing.push(outgoing.0);

        (client_id, (self.incoming.0.clone(), outgoing.1))
    }

    pub(crate) fn send_out(&self, message: Message, client_id: usize) -> Result<(), ReplicaError> {
        let Some(sender) = self.outgoing.get(client_id) else {
            return Err(ReplicaError::NetworkError);
        };

        sender.send(message).map_err(|_| ReplicaError::NetworkError)
    }
}

pub(crate) struct ReplicaNetwork {
    pub(crate) client: ClientConnection,
    pub(crate) incoming: Receiver<Message>,
    pub(crate) other: HashMap<usize, Sender<Message>>,
}

impl ReplicaNetwork {
    pub(crate) fn for_replica(
        replica: usize,
        client: ClientConnection,
        channels: &[(Sender<Message>, Receiver<Message>)],
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

    pub(crate) fn send(&self, message: Message, replica: &usize) -> Result<(), ReplicaError> {
        let sender = self
            .other
            .get(replica)
            .expect("Could not send message: replica not found in the network");

        sender.send(message).map_err(|_| ReplicaError::NetworkError)
    }

    pub(crate) fn broadcast(&self, message: &Message) -> Result<(), ReplicaError> {
        for sender in self.other.values() {
            sender
                .send(message.clone())
                .map_err(|_| ReplicaError::NetworkError)?;
        }
        Ok(())
    }
}
