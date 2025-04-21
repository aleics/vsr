use std::cell::RefCell;

use crossbeam::channel::{Receiver, Sender};
use thiserror::Error;

use crate::network::{ReplyMessage, RequestMessage};

#[derive(Debug)]
pub struct Client<I, O> {
    /// Client identification
    client_id: usize,

    /// Internal view number of the replica
    view: usize,

    /// Internal request number count
    next_request_number: RefCell<usize>,

    /// Communication channel between a client and the replicas
    channel: (Sender<RequestMessage<I>>, Receiver<ReplyMessage<O>>),
}

impl<I, O> Client<I, O>
where
    I: Clone + Send,
    O: Clone + Send,
{
    pub(crate) fn new(
        client_id: usize,
        view: usize,
        channel: (Sender<RequestMessage<I>>, Receiver<ReplyMessage<O>>),
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
            .send(RequestMessage {
                client_id: self.client_id,
                view: self.view,
                request_number: *request_number,
                operation,
            })
            .map_err(|_| ClientError::NetworkError)?;

        *request_number += 1;

        Ok(())
    }

    pub fn recv(&self) -> Result<O, ClientError> {
        let reply = self
            .channel
            .1
            .recv()
            .map_err(|_| ClientError::NetworkError)?;

        Ok(reply.result)
    }
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("A message could not be sent or received")]
    NetworkError,
}
