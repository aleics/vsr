use std::{cell::RefCell, net::SocketAddr};

use bincode::{Decode, Encode};
use thiserror::Error;

use crate::{
    ClientId, ClientOptions, InputError, ReplicaId,
    bus::ClientMessageBus,
    decode_operation, encode_operation,
    io::IOError,
    message::{Header, Message, RequestMessage, RequestMessageBody},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ClientConfig {
    pub seed: u64,
    pub client_id: ClientId,
    pub address: SocketAddr,
    pub replicas: Vec<SocketAddr>,
}

pub struct Client<IO: crate::io::IO> {
    /// Client identification
    pub config: ClientConfig,

    /// Internal view number of the replica
    view: ReplicaId,

    /// Internal request number count
    next_request_number: RefCell<u32>,

    /// Communication bus between a client and the replicas
    bus: ClientMessageBus<IO>,
}

impl<IO: crate::io::IO> Client<IO> {
    pub fn new(options: &ClientOptions, view: ReplicaId, io: IO) -> Result<Self, ClientError> {
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
            header: Header { view: self.view },
            body: RequestMessageBody {
                client_id: self.config.client_id,
                request_number: *request_number,
                operation: encode_operation(operation)?,
            },
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

            let response = decode_operation(&reply.body.result)?;
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
