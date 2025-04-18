use vsr::replica::Service;
use vsr::{Cluster, Config};

#[derive(Debug, Clone)]
struct Counter {
    value: i32,
}

impl Service for Counter {
    type Input = Operation;
    type Output = i32;

    fn execute(&mut self, input: &Self::Input) -> Self::Output {
        let new_value = match input {
            Operation::AddOne => self.value + 1,
            Operation::SubOne => self.value - 1,
        };

        self.value = new_value;
        new_value
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

    let mut cluster = Cluster::new(&config, Counter { value: 1 });
    println!("Cluster created with {} replicas.", config.addresses.len());

    let client = cluster.handshake();

    client.send(Operation::AddOne).unwrap();

    cluster.tick();
    cluster.tick();

    let result = client.recv().unwrap();
    println!("After plus one: {}", result);

    client.send(Operation::SubOne).unwrap();

    cluster.tick();
    cluster.tick();

    let result = client.recv().unwrap();
    println!("After minus one: {}", result);
}
