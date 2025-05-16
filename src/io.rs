use std::net::{TcpListener, TcpStream};
use std::num::NonZeroUsize;
use std::{io::Read, net::SocketAddr};
use std::{io::Write, time::Duration};

use polling::{Event, Events, Poller};

const SERVER: usize = 0;
const EVENTS_CAPACITY: NonZeroUsize = NonZeroUsize::new(128).unwrap();

pub enum Operation {
    Accept,
    Recv { connection: usize },
}

pub enum RecvBody {
    Message { message: String }, // TODO: use replica message
    SocketOccupied,
    Close,
}

pub trait IO {
    fn open_tcp(&self, addr: SocketAddr) -> std::io::Result<TcpListener>;

    fn connect(&mut self, addr: SocketAddr, connection_id: usize) -> std::io::Result<TcpStream>;

    fn accept(&mut self, socket: &TcpListener, connection_id: usize) -> std::io::Result<TcpStream>;

    fn recv(&self, socket: &mut TcpStream, connection_id: usize) -> std::io::Result<RecvBody>;

    fn send(&self, message: &str, socket: &mut TcpStream) -> std::io::Result<()>;

    fn run(&mut self, duration: Duration) -> std::io::Result<Vec<Operation>>;
}

pub struct PollIO {
    poll: Poller,
    events: Events,
}

impl PollIO {
    pub fn new() -> std::io::Result<Self> {
        Ok(Self {
            poll: Poller::new()?,
            events: Events::with_capacity(EVENTS_CAPACITY),
        })
    }
}

impl IO for PollIO {
    fn open_tcp(&self, addr: SocketAddr) -> std::io::Result<TcpListener> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;

        unsafe {
            self.poll.add(&listener, Event::readable(SERVER))?;
        }

        Ok(listener)
    }

    fn connect(&mut self, addr: SocketAddr, connection_id: usize) -> std::io::Result<TcpStream> {
        match TcpStream::connect(addr) {
            Ok(stream) => {
                unsafe {
                    self.poll.add(&stream, Event::readable(connection_id))?;
                }

                Ok(stream)
            }
            Err(e) => Err(e),
        }
    }

    fn accept(&mut self, socket: &TcpListener, connection_id: usize) -> std::io::Result<TcpStream> {
        println!("accept");
        match socket.accept() {
            Ok((stream, _)) => {
                unsafe {
                    self.poll.add(&stream, Event::all(connection_id))?;
                }

                Ok(stream)
            }
            Err(e) => Err(e),
        }
    }

    fn recv(&self, socket: &mut TcpStream, connection_id: usize) -> std::io::Result<RecvBody> {
        let mut buf = [0; 1024];

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

        let message = String::from_utf8(buf[..n].to_vec()).unwrap();
        Ok(RecvBody::Message { message })
    }

    fn send(&self, message: &str, socket: &mut TcpStream) -> std::io::Result<()> {
        socket.write_all(message.as_bytes())?;

        Ok(())
    }

    fn run(&mut self, duration: Duration) -> std::io::Result<Vec<Operation>> {
        self.events.clear();
        self.poll.wait(&mut self.events, Some(duration))?;

        let mut operations = Vec::with_capacity(EVENTS_CAPACITY.get());

        for event in self.events.iter() {
            if event.is_err().unwrap_or_default() {
                // TODO: handle connection failed
                continue;
            }

            let operation = match event.key {
                SERVER => Operation::Accept,
                token => Operation::Recv { connection: token },
            };

            operations.push(operation);
        }

        Ok(operations)
    }
}
