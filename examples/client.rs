use std::{
    io::{self, BufRead},
    sync::mpsc,
    thread,
};

use clap::{Parser, command};
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

fn start_client(options: &ClientOptions) -> Client<PollIO> {
    let mut client = vsr::client(options, 0).unwrap();
    client.init().unwrap();

    client
}

fn main() {
    let args = Args::parse();
    let options = ClientOptions {
        seed: args.seed,
        address: args.address,
        client_id: 0,
        replicas: args.replicas,
    };

    let mut client = start_client(&options);

    println!("Starting client...");

    while !client.is_ready() {
        client.tick::<i32>().unwrap();
    }

    println!("Client is ready in address {}!", options.address);

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
                    eprintln!("Error reading from stdin: {}", e);
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
            println!("Server: \"{response}\"");
        }
    }
}
