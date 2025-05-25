use vsr::{Cluster, Config, bus::MessageBus, io::PollIO};

fn init_bus(replica: usize, seed: u64) -> MessageBus<PollIO> {
    let addresses = vec!["127.0.0.1:3000".to_string(), "127.0.0.1:3001".to_string()];

    let config = Cluster::parse_config(&Config {
        addresses,
        current: replica,
    })
    .unwrap();

    let io = PollIO::new().unwrap();

    let mut bus = MessageBus::new(&config, io, seed).unwrap();
    bus.init().unwrap();

    bus
}

fn main() -> std::io::Result<()> {
    let seed: u64 = 1234;

    std::thread::spawn(move || {
        let mut count = 0;
        let mut bus = init_bus(0, seed);

        loop {
            bus.tick().unwrap();
            count += 1;

            if count % 100000 == 0 {
                bus.send(1, "hello from replica 0").unwrap();
                println!("message sent from replica 0");
                count = 0;
            }
        }
    });

    let mut count = 0;
    let mut bus = init_bus(1, seed);
    loop {
        bus.tick()?;
        count += 1;

        if count % 1000000 == 0 {
            bus.send(0, "hello from replica 1").unwrap();
            println!("message sent from replica 1");
            count = 0;
        }
    }
}
