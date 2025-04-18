use std::cell::RefCell;

use crossbeam::channel::{Receiver, Sender};
use thiserror::Error;

use crate::{
    Message,
    network::{ClientMessage, RequestMessage},
};

#[derive(Debug)]
pub struct Client<I: Clone, O: Clone> {
    /// Client identification
    client_id: usize,

    /// Internal view number of the replica
    view: usize,

    /// Internal request number count
    next_request_number: RefCell<usize>,

    /// Communication channel between a client and the replicas
    channel: (Sender<ClientMessage<I>>, Receiver<Message<I, O>>),
}

impl<I: Clone, O: Clone> Client<I, O> {
    pub(crate) fn new(
        client_id: usize,
        view: usize,
        channel: (Sender<ClientMessage<I>>, Receiver<Message<I, O>>),
    ) -> Self {
        Client {
            client_id,
            view,
            channel,
            next_request_number: RefCell::new(0),
        }
    }

    pub fn send(&self, operation: I) -> Result<(), ClientError> {
        let mut request_number = self.next_request_number.borrow_mut();

        self.channel
            .0
            .send(ClientMessage::Request(RequestMessage {
                client_id: self.client_id,
                view: self.view,
                request_number: *request_number,
                operation,
            }))
            .map_err(|_| ClientError::NetworkError)?;

        *request_number += 1;

        Ok(())
    }

    pub fn recv(&self) -> Result<O, ClientError> {
        let message = self
            .channel
            .1
            .recv()
            .map_err(|_| ClientError::NetworkError)?;

        if let Message::Reply(reply) = message {
            return Ok(reply.result);
        }

        unreachable!("Client received another message than a reply");
    }
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("A message could not be sent or received")]
    NetworkError,
}
