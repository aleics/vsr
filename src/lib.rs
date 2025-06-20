use std::net::AddrParseError;

use bincode::{Decode, Encode};
use bytes::Bytes;
use client::{Client, ClientConfig, ClientError};
use io::IOError;
use message::Operation;
use replica::{Replica, ReplicaConfig, ReplicaError};
use thiserror::Error;

mod bus;
pub mod client;
mod clock;
pub mod io;
mod message;
pub mod replica;

/// `ReplicaOptions` collect the available configuration options for a replica.
pub struct ReplicaOptions {
    pub seed: u64,

    /// The current replica index matching a position in `addresses`.
    pub current: usize,

    /// Socket addresses of all the replicas (e.g. `127.0.0.1:3000`)
    pub addresses: Vec<String>,
}

impl ReplicaOptions {
    fn parse(&self) -> Result<ReplicaConfig, InputError> {
        let mut socket_addresses = Vec::with_capacity(self.addresses.len());
        for address in &self.addresses {
            socket_addresses.push(address.parse()?);
        }

        Ok(ReplicaConfig {
            seed: self.seed,
            replica: self.current,
            total: socket_addresses.len(),
            addresses: socket_addresses,
        })
    }
}

/// `ClientOptions` collect the available configuration options for a client.
pub struct ClientOptions {
    pub seed: u64,

    /// The address used by the client (e.g. `127.0.0.1:8000`)
    pub address: String,

    /// An identifier for the client
    pub client_id: usize,

    /// Socket addresses of all the replicas (e.g. `127.0.0.1:3000`)
    pub replicas: Vec<String>,
}

impl ClientOptions {
    fn parse(&self) -> Result<ClientConfig, InputError> {
        let mut socket_addresses = Vec::with_capacity(self.replicas.len());
        for address in &self.replicas {
            socket_addresses.push(address.parse()?);
        }

        Ok(ClientConfig {
            seed: self.seed,
            address: self.address.parse()?,
            client_id: self.client_id,
            replicas: socket_addresses,
        })
    }
}

/// Create a new replica given certain options and a service.
pub fn replica<S: Service<Input = I, Output = O>, I: Decode<()>, O: Encode, IO: crate::io::IO>(
    options: &ReplicaOptions,
    service: S,
    io: IO,
) -> Result<Replica<S, IO>, ReplicaError> {
    let config = options.parse().unwrap();
    Ok(Replica::new(&config, service, io))
}

/// Create a new client given certain options and known `view`.
pub fn client<IO: crate::io::IO>(
    options: &ClientOptions,
    view: usize,
    io: IO,
) -> Result<Client<IO>, ClientError> {
    Client::new(options, view, io)
}

#[derive(Error, Debug, PartialEq)]
#[non_exhaustive]
pub enum InputError {
    #[error(transparent)]
    ParseAddressError(#[from] AddrParseError),
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ServiceError {
    #[error("Unrecoverable error: {0}")]
    Unrecoverable(String),
    #[error("Recoverable error: {0}")]
    Recoverable(String),
    #[error(transparent)]
    IO(#[from] IOError),
}

/// The `Service` is the internal application layer of a `Replica`. Any service implementing this
/// trait can be executed inside a replica.
pub trait Service {
    /// The input of the service. The input matches the request messages sent by the clients.
    type Input: Decode<()>;

    /// The output of the service.
    type Output: Encode;

    /// Execute the input against the service and returns an output which will be ultimately sent
    /// to the client.
    fn execute(&self, input: Self::Input) -> Result<Self::Output, ServiceError>;

    fn execute_bytes(&self, input: &Operation) -> Result<Operation, ServiceError> {
        let input = decode_operation(input)?;
        let output = self.execute(input)?;

        Ok(encode_operation(output)?)
    }
}

fn encode_operation<T: Encode>(value: T) -> Result<Operation, IOError> {
    let buf = bincode::encode_to_vec(value, bincode::config::standard())?;
    Ok(Operation::from(Bytes::from(buf)))
}

fn decode_operation<T: Decode<()>>(value: &Operation) -> Result<T, IOError> {
    let (result, _) =
        bincode::decode_from_slice(value.content.as_ref(), bincode::config::standard())?;
    Ok(result)
}
