use vsr::{
    ReplicaOptions,
    bus::ReplicaMessageBus,
    io::{IOError, PollIO},
    message::{Message, Operation},
};

fn main() -> Result<(), IOError> {
    let seed: u64 = 1234;

    let config = ReplicaOptions {
        seed,
        addresses: vec!["127.0.0.1:3000".to_string()],
        current: 0,
    }
    .parse()
    .unwrap();

    let io = PollIO::new().unwrap();

    let mut bus = ReplicaMessageBus::new(&config, io);
    bus.init().unwrap();

    loop {
        let messages = bus.tick()?;
        for message in messages {
            let Message::Request(request) = message else {
                panic!("unexpected message type");
            };

            let incoming = bytes_as_string(request.operation);
            println!("message received in replica 1: {}", incoming);
        }
    }
}

fn bytes_as_string(value: Operation) -> String {
    let (result, _) = bincode::decode_from_slice(&value, bincode::config::standard()).unwrap();
    result
}
