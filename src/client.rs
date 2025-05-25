use std::cell::RefCell;

use bincode::{Decode, Encode};
use crossbeam::channel::{Receiver, Sender};
use thiserror::Error;

use crate::{
    OPERATION_SIZE_MAX,
    network::{ReplyMessage, RequestMessage},
};

#[derive(Debug)]
pub struct Client {
    /// Client identification
    client_id: usize,

    /// Internal view number of the replica
    view: usize,

    /// Internal request number count
    next_request_number: RefCell<usize>,

    /// Communication channel between a client and the replicas
    channel: (Sender<RequestMessage>, Receiver<ReplyMessage>),
}

impl Client {
    pub(crate) fn new(
        client_id: usize,
        view: usize,
        channel: (Sender<RequestMessage>, Receiver<ReplyMessage>),
    ) -> Self {
        Client {
            client_id,
            view,
            channel,
            next_request_number: RefCell::new(0),
        }
    }

    pub fn send<I: Encode>(&self, operation: I) -> Result<(), ClientError> {
        let mut request_number = self.next_request_number.borrow_mut();

        let mut buf = [0; OPERATION_SIZE_MAX];
        bincode::encode_into_slice(operation, &mut buf, bincode::config::standard()).unwrap(); // TODO: handle this

        self.channel
            .0
            .send(RequestMessage {
                client_id: self.client_id,
                view: self.view,
                request_number: *request_number,
                operation: buf,
            })
            .map_err(|_| ClientError::NetworkError)?;

        *request_number += 1;

        Ok(())
    }

    pub fn recv<O: Decode<()>>(&self) -> Result<O, ClientError> {
        let reply = self
            .channel
            .1
            .recv()
            .map_err(|_| ClientError::NetworkError)?;

        let (result, _) =
            bincode::decode_from_slice(&reply.result, bincode::config::standard()).unwrap();

        Ok(result)
    }
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("A message could not be sent or received")]
    NetworkError,
}
