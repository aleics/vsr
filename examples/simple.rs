use vsr::{Cluster, Config};

fn main() {
    let config = Config {
        addresses: vec!["ip-1".to_string(), "ip-2".to_string(), "ip-3".to_string()],
    };

    let mut cluster = Cluster::new(&config);

    let client = cluster.handshake();
    client.send().unwrap();

    cluster.tick();
    cluster.tick();

    let message = client.recv().unwrap();
    println!("Received response from cluster {:?}", message);
}
