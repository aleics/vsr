use std::sync::Mutex;
use std::thread::{self, sleep};
use std::time::Duration;

use bincode::{Decode, Encode};
use vsr::client::Client;
use vsr::{ClientOptions, Cluster, ReplicaOptions, Service, ServiceError};

#[derive(Debug)]
struct Counter {
    value: Mutex<i32>,
}

impl Service for Counter {
    type Input = Operation;
    type Output = i32;

    fn execute(&self, input: Self::Input) -> Result<Self::Output, ServiceError> {
        let mut current_value = self.value.lock().unwrap();

        match input {
            Operation::AddOne => *current_value += 1,
            Operation::SubOne => *current_value -= 1,
        };

        Ok(*current_value)
    }
}

impl Clone for Counter {
    fn clone(&self) -> Self {
        let current_value = self.value.lock().unwrap();
        Counter {
            value: Mutex::new(*current_value),
        }
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
            current: i,
            addresses: replica_addresses.clone(),
        };

        let mut replica = Cluster::create_replica(
            &replica_options,
            Counter {
                value: Mutex::new(1),
            },
        )
        .unwrap();

        replica.init().unwrap();
        replicas.push(replica);

        println!("Replica {} created", i);
    }

    let primary = Cluster::primary(&replicas);

    for replica in replicas.into_iter() {
        handles.push(thread::spawn(move || {
            replica.run().unwrap();
        }));
    }

    sleep(Duration::from_secs(1));

    let mut client = Client::new(&client_options, primary).unwrap();

    while !client.is_ready() {
        let responses = client.tick::<i32>().unwrap();
        if !responses.is_empty() {
            println!("Received responses while getting ready: {:?}", responses);
        }
    }

    println!("Client is ready!");

    client.send(Operation::AddOne).unwrap();

    loop {
        let responses = client.tick::<i32>().unwrap();
        if !responses.is_empty() {
            println!("Received responses: {:?}", responses);
            break;
        }
    }

    client.send(Operation::SubOne).unwrap();

    println!("After minus one");
    loop {
        let responses = client.tick::<i32>().unwrap();
        if !responses.is_empty() {
            println!("Received responses: {:?}", responses);
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
            println!("Received responses: {:?}", responses);
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
            println!("Received responses: {:?}", responses);
            break;
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
