use std::thread;

use clap::{Parser, command};
use vsr::{ReplicaOptions, Service, io::PollIO, replica::Replica};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(short, long, value_delimiter = ',')]
    addresses: Vec<String>,

    #[arg(short, long)]
    seed: u64,
}

struct EchoService;

impl Service for EchoService {
    type Input = String;
    type Output = String;

    fn execute(&self, input: Self::Input) -> Result<Self::Output, vsr::ServiceError> {
        println!("message received {input}");
        Ok(format!("echo \"{input}\""))
    }
}

fn start_replica(options: &ReplicaOptions) -> Replica<EchoService, PollIO> {
    let io = PollIO::new().unwrap();
    let mut replica = vsr::replica(options, EchoService, io).unwrap();
    replica.init().unwrap();

    replica
}

fn main() {
    let args = Args::parse();

    let total = args.addresses.len();
    let mut handles = Vec::with_capacity(total);

    for replica_number in 0..total {
        let options = ReplicaOptions {
            seed: args.seed,
            addresses: args.addresses.clone(),
            current: replica_number as u8,
        };

        let mut replica = start_replica(&options);

        handles.push(thread::spawn(move || {
            println!("Replica {replica_number} running...");
            replica.run().unwrap();
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
