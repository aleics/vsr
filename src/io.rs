use std::{io::Read, net::SocketAddr};
use std::{io::Write, time::Duration};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use thiserror::Error;

use crate::MESSAGE_SIZE_MAX;

const SERVER: Token = Token(0);
const EVENTS_CAPACITY: usize = 128;

/// A completion represents an IO operation that has finished.
/// Returned by the IO system, it indicates that the operation is ready.
#[derive(Debug)]
pub enum Completion {
    Accept,
    Recv { connection: usize },
    Write { connection: usize },
}

pub struct AcceptedConnection {
    pub(crate) socket: TcpStream,
    pub(crate) connection_id: usize,
}

pub trait IO {
    /// Open a TCP connection to a given address.
    fn open_tcp(&self, addr: SocketAddr) -> Result<TcpListener, IOError>;

    /// Connect to a given address using a connection identifier.
    fn connect(
        &mut self,
        addr: SocketAddr,
        connection_id: usize,
    ) -> Result<Option<TcpStream>, IOError>;

    /// Accept an incoming connection to the provided listener.
    fn accept(
        &mut self,
        socket: &TcpListener,
        connection_id: usize,
    ) -> Result<Vec<AcceptedConnection>, IOError>;

    /// Receive a new message in the socket. The result is stored in the buffer provided as a mutable reference.
    /// A boolean is returned if the connection mus be closed or not.
    fn recv(&self, socket: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<bool, IOError>;

    /// Send a new message to the provided socket. This is used as a first step. Once the non-blocking connection
    /// is available to write the message, the `IO::write` should be used.
    fn send(&self, socket: &mut TcpStream, connection_id: usize) -> Result<(), IOError>;

    /// Write a certain amount of bytes to a socket.
    fn write(&self, socket: &mut TcpStream, bytes: &[u8]) -> Result<Option<usize>, IOError>;

    /// Run any IO operations with a certain timeout.
    fn run(&mut self, timeout: Duration) -> Result<Vec<Completion>, IOError>;
}

/// `PollIO` is an implementation of the IO trait using [`mio`](https://github.com/tokio-rs/mio).
pub struct PollIO {
    poll: Poll,
    events: Events,
}

impl PollIO {
    /// Create a new `PollIO` instance
    pub fn new() -> Result<Self, IOError> {
        Ok(Self {
            poll: Poll::new()?,
            events: Events::with_capacity(EVENTS_CAPACITY),
        })
    }
}

impl IO for PollIO {
    fn open_tcp(&self, addr: SocketAddr) -> Result<TcpListener, IOError> {
        let mut listener = TcpListener::bind(addr)?;

        self.poll
            .registry()
            .register(&mut listener, SERVER, Interest::READABLE)?;

        Ok(listener)
    }

    fn connect(
        &mut self,
        addr: SocketAddr,
        connection_id: usize,
    ) -> Result<Option<TcpStream>, IOError> {
        match TcpStream::connect(addr) {
            Ok(mut stream) => {
                self.poll.registry().register(
                    &mut stream,
                    Token(connection_id),
                    Interest::WRITABLE,
                )?;

                Ok(Some(stream))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e)?,
        }
    }

    fn accept(
        &mut self,
        socket: &TcpListener,
        connection_id: usize,
    ) -> Result<Vec<AcceptedConnection>, IOError> {
        assert!(connection_id != SERVER.0);

        let mut accepted = Vec::new();
        let mut next_connection_id = connection_id;

        loop {
            // An accept event might include multiple connections waiting to
            // be accepted. Thus, we'll accept everything from the socket.
            // A `WouldBlock` error would signal that there are no more
            // connections to accept.
            match socket.accept() {
                Ok((mut stream, _)) => {
                    self.poll.registry().register(
                        &mut stream,
                        Token(next_connection_id),
                        Interest::READABLE,
                    )?;

                    accepted.push(AcceptedConnection {
                        socket: stream,
                        connection_id: next_connection_id,
                    });
                    next_connection_id += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => Err(e)?,
            }
        }

        Ok(accepted)
    }

    fn recv(&self, socket: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<bool, IOError> {
        let mut buf = [0; MESSAGE_SIZE_MAX];

        loop {
            // The complete content of the socket is read. A `WouldBlock` error notifies that
            // there's no more content in the socket waiting to be read, and thus, it will block
            // (wait) for new incoming messages.
            let n = match socket.read(&mut buf) {
                Ok(0) => return Ok(true),
                Ok(n) => Ok(n),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => Err(e),
            }?;

            buffer.extend_from_slice(&buf[..n]);
        }

        Ok(false)
    }

    fn send(&self, socket: &mut TcpStream, connection_id: usize) -> Result<(), IOError> {
        self.poll.registry().reregister(
            socket,
            Token(connection_id),
            Interest::READABLE.add(Interest::WRITABLE),
        )?;

        Ok(())
    }

    fn write(&self, socket: &mut TcpStream, bytes: &[u8]) -> Result<Option<usize>, IOError> {
        let written = match socket.write(bytes) {
            Ok(0) => Err(std::io::ErrorKind::WriteZero.into()),
            Ok(n) => Ok(n),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) => Err(e),
        }?;

        Ok(Some(written))
    }

    fn run(&mut self, timeout: Duration) -> Result<Vec<Completion>, IOError> {
        self.events.clear();
        self.poll.poll(&mut self.events, Some(timeout))?;

        let mut completions = Vec::with_capacity(EVENTS_CAPACITY);

        for event in &self.events {
            match event.token() {
                SERVER => {
                    completions.push(Completion::Accept);
                }
                token => {
                    // The event notifies that the socket can be read on (incoming messages).
                    if event.is_readable() {
                        completions.push(Completion::Recv {
                            connection: token.0,
                        });
                    }

                    // The event notifies that the socket can be written on, thus, schedule a write of any pending
                    // events in the connection buffer.
                    if event.is_writable() {
                        completions.push(Completion::Write {
                            connection: token.0,
                        });
                    }
                }
            }
        }

        Ok(completions)
    }
}

#[derive(Error, Debug)]
pub enum IOError {
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Decode(#[from] bincode::error::DecodeError),

    #[error(transparent)]
    Encode(#[from] bincode::error::EncodeError),
}
