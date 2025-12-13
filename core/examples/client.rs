use std::{
    io::{self, BufRead},
    sync::mpsc,
    thread,
};

use clap::Parser;
use rand::Rng;
use vsr::{ClientOptions, client::Client, io::PollIO};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(short, long, value_delimiter = ',')]
    replicas: Vec<String>,

    #[arg(short, long)]
    address: String,

    #[arg(short, long)]
    seed: u64,
}

fn random(max: u64) -> u64 {
    let mut rng = rand::rng();
    rng.random_range(0..max)
}

fn start_client(options: &ClientOptions) -> Client<PollIO> {
    let io = PollIO::new().unwrap();
    let mut client = vsr::client(options, 0, io).unwrap();
    client.init().unwrap();

    client
}

fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let client_id = random(args.seed);
    let options = ClientOptions {
        seed: args.seed,
        address: args.address,
        client_id: client_id as u128,
        replicas: args.replicas,
    };

    let mut client = start_client(&options);
    tracing::info!("[client::main] Starting client (id: {client_id})...");

    while !client.is_ready() {
        client.tick::<i32>().unwrap();
    }

    tracing::info!(
        "[client::main] Client is ready in address {}!",
        options.address
    );

    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(msg) => {
                    if tx.send(msg).is_err() {
                        break; // Main thread is gone
                    }
                }
                Err(e) => {
                    tracing::error!("[client::main] Error reading from stdin: {e}");
                    break;
                }
            }
        }
    });

    loop {
        while let Ok(msg) = rx.try_recv() {
            client.send(msg).unwrap();
        }

        for response in client.tick::<String>().unwrap() {
            tracing::info!("[client::main] Server: \"{response}\"");
        }
    }
}
