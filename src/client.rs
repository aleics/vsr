use std::{cell::RefCell, net::SocketAddr};

use bincode::{Decode, Encode};
use thiserror::Error;

use crate::{
    ClientOptions, InputError, OPERATION_SIZE_MAX,
    bus::ClientMessageBus,
    io::{IOError, PollIO},
    message::{Message, RequestMessage},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ClientConfig {
    pub(crate) seed: u64,
    pub(crate) client_id: usize,
    pub(crate) address: SocketAddr,
    pub(crate) replicas: Vec<SocketAddr>,
}

pub struct Client<IO> {
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
    pub fn new(options: &ClientOptions, view: usize) -> Result<Self, ClientError> {
        let io = PollIO::new()?;
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

        let mut buf = [0; OPERATION_SIZE_MAX];
        bincode::encode_into_slice(operation, &mut buf, bincode::config::standard())
            .map_err(IOError::Encode)?;

        let message = RequestMessage {
            client_id: self.config.client_id,
            view: self.view,
            request_number: *request_number,
            operation: buf,
        };

        self.bus
            .send_to_replica(&Message::Request(message), &self.view)?;

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

            let (response, _) =
                bincode::decode_from_slice(&reply.result, bincode::config::standard())
                    .map_err(IOError::Decode)?;

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
