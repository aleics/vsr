use vsr::{
    ReplicaOptions,
    bus::ReplicaMessageBus,
    io::{IOError, PollIO},
    message::{Message, Operation, RequestMessage},
};

fn init_bus(replica: usize, seed: u64) -> ReplicaMessageBus<PollIO> {
    let addresses = vec!["127.0.0.1:3000".to_string(), "127.0.0.1:3001".to_string()];

    let config = ReplicaOptions {
        seed,
        addresses,
        current: replica,
    }
    .parse()
    .unwrap();

    let io = PollIO::new().unwrap();

    let mut bus = ReplicaMessageBus::new(&config, io);
    bus.init().unwrap();

    bus
}

fn main() -> Result<(), IOError> {
    let seed: u64 = 1234;

    std::thread::spawn(move || {
        let mut count = 0;
        let mut request = 0;

        let mut bus = init_bus(0, seed);

        loop {
            let messages = bus.tick().unwrap();
            for message in messages {
                let Message::Request(request) = message else {
                    panic!("unexpected message type");
                };

                let message = bytes_as_string(request.operation);
                println!("message received in replica 0: {}", message);
            }
            count += 1;

            if count % 100000 == 0 {
                let greeting = format!("hello from replica 0 ({})", request);
                let operation = string_as_bytes(greeting.clone());

                let message = Message::Request(RequestMessage {
                    view: 0,
                    request_number: request,
                    client_id: 0,
                    operation,
                });
                println!("message sent from replica 0: {:?}", greeting);
                bus.send_to_replica(message, &1).unwrap();
                count = 0;
                request += 1;
            }
        }
    });

    let mut count = 0;
    let mut request = 0;
    let mut bus = init_bus(1, seed);

    loop {
        let messages = bus.tick()?;
        for message in messages {
            let Message::Request(request) = message else {
                panic!("unexpected message type");
            };

            let incoming = bytes_as_string(request.operation);
            println!("message received in replica 1: {}", incoming);
        }
        count += 1;

        if count % 1000000 == 0 {
            let greeting = format!("hello from replica 1 ({})", request);
            let operation = string_as_bytes(greeting.clone());

            let message = Message::Request(RequestMessage {
                view: 0,
                request_number: request,
                client_id: 0,
                operation,
            });
            println!("message sent from replica 1: {:?}", greeting);
            bus.send_to_replica(message, &0).unwrap();
            count = 0;
            request += 1;
        }
    }
}

fn string_as_bytes(value: String) -> Operation {
    let mut buf = [0; 1024];
    bincode::encode_into_slice(value, &mut buf, bincode::config::standard()).unwrap();
    buf
}

fn bytes_as_string(value: Operation) -> String {
    let (result, _) = bincode::decode_from_slice(&value, bincode::config::standard()).unwrap();
    result
}
