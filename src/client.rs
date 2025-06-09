use std::{cell::RefCell, net::SocketAddr};

use bincode::{Decode, Encode};
use thiserror::Error;

use crate::{
    ClientOptions, InputError,
    bus::ClientMessageBus,
    decode_operation, encode_operation,
    io::{IOError, PollIO},
    message::{Message, RequestMessage},
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClientConfig {
    pub(crate) seed: u64,
    pub(crate) client_id: usize,
    pub(crate) address: SocketAddr,
    pub(crate) replicas: Vec<SocketAddr>,
}

pub struct Client<IO: crate::io::IO> {
    /// Client identification
    config: ClientConfig,

    /// Internal view number of the replica
    view: usize,

    /// Internal request number count
    next_request_number: RefCell<usize>,

    /// Communication bus between a client and the replicas
    bus: ClientMessageBus<IO>,
}

impl Client<PollIO> {
    pub fn new(options: &ClientOptions, view: usize, io: PollIO) -> Result<Self, ClientError> {
        let config = options.parse()?;

        let bus = ClientMessageBus::new(&config, io);
        Ok(Client {
            config,
            view,
            bus,
            next_request_number: RefCell::new(0),
        })
    }

    pub fn init(&mut self) -> Result<(), ClientError> {
        self.bus.init()?;
        Ok(())
    }

    pub fn is_ready(&self) -> bool {
        self.bus.is_connected()
    }

    pub fn send<I: Encode>(&mut self, operation: I) -> Result<(), ClientError> {
        let mut request_number = self.next_request_number.borrow_mut();

        let message = RequestMessage {
            client_id: self.config.client_id,
            view: self.view,
            request_number: *request_number,
            operation: encode_operation(operation)?,
        };

        self.bus
            .send_to_replica(&Message::Request(message), self.view)?;

        *request_number += 1;

        Ok(())
    }

    pub fn tick<O: Decode<()>>(&mut self) -> Result<Vec<O>, ClientError> {
        let messages = self.bus.tick()?;

        let mut responses = Vec::with_capacity(messages.len());
        for message in messages {
            let Message::Reply(reply) = message else {
                continue;
            };

            let response = decode_operation(&reply.result)?;
            responses.push(response);
        }

        Ok(responses)
    }
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("A message could not be sent or received")]
    NetworkError,
    #[error(transparent)]
    IOError(#[from] IOError),
    #[error(transparent)]
    Input(#[from] InputError),
}
