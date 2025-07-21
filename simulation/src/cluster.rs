use std::cell::RefCell;

use crate::{
    env::Env,
    io::{ConnectionLookup, SimQueue},
};

use super::io::{FaultyIO, FaultyIOProbs};
use vsr::{ClientOptions, ReplicaId, ReplicaOptions, Service, client::Client, replica::Replica};

const CLUSTER_IP: &str = "127.0.0.1";
const REPLICA_BASE_PORT: u32 = 3000;
const CLIENT_BASE_PORT: u32 = 8000;

pub(crate) struct ClusterOptions {
    pub(crate) replica_count: ReplicaId,
    pub(crate) client_count: ReplicaId,
}

pub(crate) struct Cluster {
    pub(crate) replicas: Vec<Replica<SimService, FaultyIO>>,
    pub(crate) clients: Vec<Client<FaultyIO>>,
}

impl Cluster {
    pub(crate) fn new(env: Env, options: ClusterOptions) -> Self {
        let replica_addresses = generate_addresses(REPLICA_BASE_PORT, &options);
        let queue = SimQueue::new();
        let lookup = ConnectionLookup::new();

        let replicas = create_replicas(
            env.clone(),
            replica_addresses.clone(),
            queue.clone(),
            lookup.clone(),
            &options,
        );
        let clients = create_clients(
            env.clone(),
            replica_addresses,
            queue.clone(),
            lookup.clone(),
            &options,
        );

        Cluster { replicas, clients }
    }

    pub(crate) fn init(&mut self) {
        for replica in &mut self.replicas {
            replica.init().unwrap();
        }

        for client in &mut self.clients {
            client.init().unwrap();
        }
    }
}

fn create_replicas(
    env: Env,
    addresses: Vec<String>,
    queue: SimQueue,
    lookup: ConnectionLookup,
    options: &ClusterOptions,
) -> Vec<Replica<SimService, FaultyIO>> {
    let mut replicas = Vec::with_capacity(options.replica_count as usize);

    for replica in 0..options.replica_count {
        let options = ReplicaOptions {
            seed: env.seed,
            current: replica,
            addresses: addresses.clone(),
        };
        let service = SimService::default();
        let io = FaultyIO::new(
            queue.clone(),
            lookup.clone(),
            env.clone(),
            FaultyIOProbs::new_with_seed(&env),
        );

        replicas.push(vsr::replica(&options, service, io).unwrap());
    }

    replicas
}

fn create_clients(
    env: Env,
    replica_addresses: Vec<String>,
    queue: SimQueue,
    lookup: ConnectionLookup,
    options: &ClusterOptions,
) -> Vec<Client<FaultyIO>> {
    let mut clients = Vec::with_capacity(options.client_count as usize);

    for client in 0..options.client_count {
        let options = ClientOptions {
            seed: env.seed,
            address: generate_address(CLIENT_BASE_PORT + client as u32),
            client_id: client as usize,
            replicas: replica_addresses.clone(),
        };
        let io = FaultyIO::new(
            queue.clone(),
            lookup.clone(),
            env.clone(),
            FaultyIOProbs::new_with_seed(&env),
        );

        clients.push(vsr::client(&options, 0, io).unwrap());
    }

    clients
}

fn generate_addresses(port: u32, options: &ClusterOptions) -> Vec<String> {
    let mut addresses = Vec::with_capacity(options.replica_count as usize);

    for current in 0..options.replica_count {
        addresses.push(generate_address(port + current as u32));
    }

    addresses
}

fn generate_address(port: u32) -> String {
    format!("{CLUSTER_IP}:{port}")
}

#[derive(Default)]
pub(crate) struct SimService {
    counter: RefCell<u64>,
}

impl Service for SimService {
    type Input = ();
    type Output = u64;

    fn execute(&self, _: Self::Input) -> Result<Self::Output, vsr::ServiceError> {
        let mut output = self.counter.borrow_mut();
        *output += 1;

        Ok(*output)
    }
}
