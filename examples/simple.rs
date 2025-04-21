use std::sync::Mutex;
use std::thread::{self, sleep};
use std::time::Duration;

use vsr::replica::Service;
use vsr::{Cluster, Config};

#[derive(Debug)]
struct Counter {
    value: Mutex<i32>,
}

impl Service for Counter {
    type Input = Operation;
    type Output = i32;

    fn execute(&self, input: &Self::Input) -> Self::Output {
        let mut current_value = self.value.lock().unwrap();
        match input {
            Operation::AddOne => *current_value += 1,
            Operation::SubOne => *current_value -= 1,
        };

        *current_value
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

#[derive(Debug, Clone)]
enum Operation {
    AddOne,
    SubOne,
}

fn main() {
    let config = Config {
        addresses: vec!["ip-1".to_string(), "ip-2".to_string(), "ip-3".to_string()],
    };

    let mut replicas = Cluster::create(
        &config,
        Counter {
            value: Mutex::new(1),
        },
    );
    println!("Cluster created with {} replicas.", config.addresses.len());

    let client = Cluster::handshake(&mut replicas);

    let mut handles = Vec::with_capacity(replicas.len());

    for replica in replicas {
        handles.push(thread::spawn(move || replica.run()));
    }

    client.send(Operation::AddOne).unwrap();

    let result = client.recv().unwrap();
    println!("After plus one: {}", result);

    client.send(Operation::SubOne).unwrap();

    let result = client.recv().unwrap();
    println!("After minus one: {}", result);

    sleep(Duration::from_secs(10));
    println!("slept for 10 seconds");

    client.send(Operation::AddOne).unwrap();

    let result = client.recv().unwrap();
    println!("After plus one: {}", result);

    sleep(Duration::from_secs(1));
    println!("slept for 1 seconds");

    client.send(Operation::SubOne).unwrap();

    let result = client.recv().unwrap();
    println!("After minus one: {}", result);

    for handle in handles {
        handle.join().unwrap();
    }
}
