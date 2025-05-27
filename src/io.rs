use std::net::{TcpListener, TcpStream};
use std::num::NonZeroUsize;
use std::{io::Read, net::SocketAddr};
use std::{io::Write, time::Duration};

use bincode::config::Configuration;
use polling::{Event, Events, Poller};
use thiserror::Error;

use crate::MESSAGE_SIZE_MAX;
use crate::network::Message;

const SERVER: usize = 0;
const EVENTS_CAPACITY: NonZeroUsize = NonZeroUsize::new(128).unwrap();

pub enum Completion {
    Accept,
    Recv { connection: usize },
}

pub enum RecvBody {
    Message { message: Box<Message> },
    SocketOccupied,
    Close,
}

pub trait IO {
    fn open_tcp(&self, addr: SocketAddr) -> Result<TcpListener, IOError>;

    fn connect(&mut self, addr: SocketAddr, connection_id: usize) -> Result<TcpStream, IOError>;

    fn accept(
        &mut self,
        socket: &TcpListener,
        connection_id: usize,
    ) -> Result<Option<TcpStream>, IOError>;

    fn recv(&self, socket: &mut TcpStream, connection_id: usize) -> Result<RecvBody, IOError>;

    fn send(&self, message: Message, socket: &mut TcpStream) -> Result<(), IOError>;

    fn run(&mut self, duration: Duration) -> Result<Vec<Completion>, IOError>;
}

pub struct PollIO {
    poll: Poller,
    events: Events,
    config: Configuration,
}

impl PollIO {
    pub fn new() -> Result<Self, IOError> {
        Ok(Self {
            poll: Poller::new()?,
            events: Events::with_capacity(EVENTS_CAPACITY),
            config: bincode::config::standard(),
        })
    }
}

impl IO for PollIO {
    fn open_tcp(&self, addr: SocketAddr) -> Result<TcpListener, IOError> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;

        unsafe {
            self.poll.add(&listener, Event::readable(SERVER))?;
        }

        Ok(listener)
    }

    fn connect(&mut self, addr: SocketAddr, connection_id: usize) -> Result<TcpStream, IOError> {
        match TcpStream::connect(addr) {
            Ok(stream) => {
                unsafe {
                    self.poll.add(&stream, Event::readable(connection_id))?;
                }

                Ok(stream)
            }
            Err(e) => Err(e)?,
        }
    }

    fn accept(
        &mut self,
        socket: &TcpListener,
        connection_id: usize,
    ) -> Result<Option<TcpStream>, IOError> {
        assert!(connection_id != SERVER);

        match socket.accept() {
            Ok((stream, _)) => {
                unsafe {
                    self.poll.add(&stream, Event::all(connection_id))?;
                }

                Ok(Some(stream))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e)?,
        }
    }

    fn recv(&self, socket: &mut TcpStream, connection_id: usize) -> Result<RecvBody, IOError> {
        let mut buf = [0; MESSAGE_SIZE_MAX];

        let n = match socket.read(&mut buf) {
            Ok(n) => Ok(n),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(RecvBody::SocketOccupied);
            }
            Err(e) => Err(e),
        }?;

        // Keep interest into read events from this socket
        self.poll.modify(&socket, Event::readable(connection_id))?;

        if n == 0 {
            return Ok(RecvBody::Close);
        }

        let (message, _) = bincode::decode_from_slice(&buf, self.config)?;
        Ok(RecvBody::Message { message })
    }

    fn send(&self, message: Message, socket: &mut TcpStream) -> Result<(), IOError> {
        let mut buf = [0; MESSAGE_SIZE_MAX];
        bincode::encode_into_slice(message, &mut buf, self.config)?;

        socket.write_all(&buf)?;

        Ok(())
    }

    fn run(&mut self, duration: Duration) -> Result<Vec<Completion>, IOError> {
        self.events.clear();
        self.poll.wait(&mut self.events, Some(duration))?;

        let mut completions = Vec::with_capacity(EVENTS_CAPACITY.get());

        for event in self.events.iter() {
            if event.is_err().unwrap_or_default() {
                // TODO: handle connection failed
                continue;
            }

            let completion = match event.key {
                SERVER => Completion::Accept,
                token => Completion::Recv { connection: token },
            };

            completions.push(completion);
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
