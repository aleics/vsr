use std::sync::{Arc, Mutex};
use std::thread;

use vsr::replica::Service;
use vsr::{Cluster, Config};

#[derive(Debug, Clone)]
struct Counter {
    value: Arc<Mutex<i32>>,
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
            value: Arc::new(Mutex::new(1)),
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

    for handle in handles {
        handle.join().unwrap();
    }
}
