use std::cell::RefCell;
use std::thread::{self, sleep};
use std::time::Duration;

use bincode::{Decode, Encode};
use vsr::io::PollIO;
use vsr::storage::InMemoryStorage;
use vsr::{ClientOptions, ReplicaOptions, Service, ServiceError};

#[derive(Clone, Debug)]
struct Counter {
    value: RefCell<i32>,
}

impl Service for Counter {
    type Input = Operation;
    type Output = i32;

    fn execute(&self, input: Self::Input) -> Result<Self::Output, ServiceError> {
        let mut current_value = self.value.borrow_mut();

        match input {
            Operation::AddOne => *current_value += 1,
            Operation::SubOne => *current_value -= 1,
        };

        Ok(*current_value)
    }
}

#[derive(Debug, Clone, Encode, Decode)]
enum Operation {
    AddOne,
    SubOne,
}

fn main() {
    let seed = 1234;
    let replica_addresses = vec![
        "127.0.0.1:3001".to_string(),
        "127.0.0.1:3002".to_string(),
        "127.0.0.1:3003".to_string(),
    ];
    let client_options = ClientOptions {
        seed,
        address: "127.0.0.1:8000".to_string(),
        client_id: 0,
        replicas: replica_addresses.clone(),
    };

    let mut handles = Vec::with_capacity(replica_addresses.len());
    let mut replicas = Vec::with_capacity(replica_addresses.len());

    for i in 0..replica_addresses.len() {
        let replica_options = ReplicaOptions {
            seed,
            current: i as u8,
            addresses: replica_addresses.clone(),
        };

        let io = PollIO::new().unwrap();
        let mut replica = vsr::replica(
            &replica_options,
            Counter {
                value: RefCell::new(1),
            },
            InMemoryStorage::new(),
            io,
        )
        .unwrap();

        replica.init().unwrap();
        replicas.push(replica);

        println!("Replica {i} created");
    }

    for mut replica in replicas.into_iter() {
        handles.push(thread::spawn(move || {
            replica.run().unwrap();
        }));
    }

    sleep(Duration::from_secs(1));

    let io = PollIO::new().unwrap();
    let mut client = vsr::client(&client_options, 0, io).unwrap();
    client.init().unwrap();

    while !client.is_ready() {
        let responses = client.tick::<i32>().unwrap();
        if !responses.is_empty() {
            println!("Received responses while getting ready: {responses:?}");
        }
    }

    println!("Client is ready!");

    client.send(Operation::AddOne).unwrap();

    loop {
        let responses = client.tick::<i32>().unwrap();
        if !responses.is_empty() {
            println!("Received responses: {responses:?}");
            break;
        }
    }

    client.send(Operation::SubOne).unwrap();

    println!("After minus one");
    loop {
        let responses = client.tick::<i32>().unwrap();
        if !responses.is_empty() {
            println!("Received responses: {responses:?}");
            break;
        }
    }

    sleep(Duration::from_secs(10));
    println!("slept for 10 seconds");

    client.send(Operation::AddOne).unwrap();

    println!("After plus one");
    loop {
        let responses = client.tick::<i32>().unwrap();
        if !responses.is_empty() {
            println!("Received responses: {responses:?}");
            break;
        }
    }

    sleep(Duration::from_secs(1));
    println!("slept for 1 seconds");

    client.send(Operation::SubOne).unwrap();

    println!("After minus one:");

    loop {
        let responses = client.tick::<i32>().unwrap();
        if !responses.is_empty() {
            println!("Received responses: {responses:?}");
            break;
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
