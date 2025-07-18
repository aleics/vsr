use std::time::{Duration, Instant};

use clap::{Parser, command};
use vsr::{client::ClientError, replica::ReplicaError};

use crate::{
    cluster::{Cluster, ClusterOptions},
    env::Env,
};

pub(crate) mod cluster;
pub(crate) mod env;
mod io;

struct Runner {
    env: Env,
    cluster: Cluster,
    start_time: Instant,
}

impl Runner {
    fn new(env: Env, options: ClusterOptions) -> Self {
        let cluster = Cluster::new(env.clone(), options);
        Runner {
            cluster,
            env,
            start_time: Instant::now(),
        }
    }

    fn init(&mut self) {
        self.cluster.init();
    }

    fn tick_replicas(&mut self) -> Result<(), ReplicaError> {
        for replica in self.cluster.replicas.iter_mut() {
            replica.tick()?;
        }

        Ok(())
    }

    fn tick_clients(&mut self) -> Result<(), ClientError> {
        for client in self.cluster.clients.iter_mut() {
            let output = client.tick::<u64>()?;

            if client.is_ready() {
                if !output.is_empty() {
                    for new_counter in output {
                        tracing::info!(
                            "Client {} received new counter: {}",
                            client.config.client_id,
                            new_counter
                        );
                    }
                }

                if self.env.flip_coin(1.0) {
                    client.send(())?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(short, long)]
    seed: u64,
}

fn main() {
    tracing_subscriber::fmt::init();

    let seed = 1234;
    tracing::info!("Seed: {}", seed);
    let env = Env::new(seed);

    // TODO: derive options from seed
    let options = ClusterOptions {
        replica_count: 3,
        client_count: 1,
    };

    let mut runner = Runner::new(env, options);
    runner.init();

    loop {
        if let Err(err) = runner.tick_replicas() {
            tracing::error!("Error in replica: {}", err);
        }

        // Give some time to the replicas to connect to each other
        if runner.start_time.elapsed() >= Duration::from_secs(1) {
            if let Err(err) = runner.tick_clients() {
                tracing::error!("Error in client: {}", err);
            }
        }
    }
}
