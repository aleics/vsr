use std::collections::HashMap;

use client::Client;
use crossbeam::channel::unbounded;
use network::{AttachedChannel, ClientConnection, Message, ReplicaNetwork};
use replica::{Replica, Service, quorum};

/* VSR (Viewstamped Replication Revisited)

Protocols
 * Normal protocol
 * View change protocol
 * Recovery protocol

Primary is determined by the view number and the configuration.
The replicas are numbered starting with replica 1, which is the initial primary.
Replicas have an internal state to describe their availability.
State *normal* means the replica is available.

The client-proxy knows who the primary is and sends messages accordingly.
All requests sent by the client are given a number to understand ordering.
The client also sends its current known view number together with the message.
The primary replica can check that the view number matches with the one known by the client.
If the client is behind, the receiver drops the message.
If the client is ahead, the replica performs a "state transfer":
 - it requests information it is missing from the other replicas and uses this information to bring itself up to date before processing the message.

Normal protocol
 1. Client sends a message <Request, operation (with arguments), client_id, request_number>.
 2. When the primary receives the message it compares the request number to the internal client_table.
    a. If the request number is smaller, the table drops the request.
    b. If it's equal it will re-send the same response that has been executed previously.
    c. If the request number is bigger, (3)
 3. The primary advances the operation_number, adds the request to the end of the log and updates the information for this client.
    Then, it sends a <Prepare, view_number, message_client, request_operation_number, commit_number>
 4. The backup replicas process the Prepare messages in order:
    - A backup replica won't accept a prepare with an operation_number until it has entries for all earlier requests in its log.
    - Once all previous requests are available:
      a. The operation_number is increased
      b. The request is added to the end of its log
      c. Updates the client's information in the client table
      d. Sends a <PrepareOk, view_number, operation_number, replica_number> message to the primary to indicate that this operation has been processed.
 5. Primary waits for `f`  PrepareOk messages from the different backup replicas. Once that happens, the operation is considered to be successful and thus marked as committed. Then, after it has executed all earlier operations
    a. The primary executes the operation by making an up-call to the service code.
    b. Increments its commit_number
    c. Sends a <Reply, view_number, client_request_id, payload>.
    d. The primary updates the client's table to contain the payload.
 6. The primary informs the backup replicas about the commit when it sends the next Prepare message. However, if the primary does not receive a new client request in a while, it pro-actively informs the backup replicas about the latest commit with a message <Commit, view_number, commit_number>
 7. When a backup learns of a commit, it waits until it has the request in its log and until it has executed all earlier operations. Then it executes the operation by performing the up-call to the service code, increments its commit-number, updates the client's entry in the client table, but does not send the reply to the client.
 8. If a client doesn't receive a timeline response to a request, it re-sends the request to all replicas. This way if the grouped has moved to a later view, its message will reach the new primary. Backups ignore client requests; only the primary processes them.
*/

mod client;
mod network;
pub mod replica;

pub struct Config {
    pub addresses: Vec<String>,
}

pub struct Cluster;

impl Cluster {
    pub fn create<S: Clone + Service<Input = I, Output = O>, I: Clone + Send, O: Clone + Send>(
        config: &Config,
        service: S,
    ) -> Vec<Replica<S, I, O>> {
        let total = config.addresses.len();

        let mut channels = Vec::with_capacity(total);
        for _ in &config.addresses {
            channels.push(unbounded::<Message<I, O>>())
        }

        let mut replicas = Vec::with_capacity(total);
        for (i, _) in config.addresses.iter().enumerate() {
            replicas.push(Replica::new(
                i,
                total,
                service.clone(),
                ReplicaNetwork::for_replica(i, ClientConnection::new(), &channels),
            ));
        }

        replicas
    }

    pub fn handshake<
        S: Clone + Service<Input = I, Output = O>,
        I: Clone + Send,
        O: Clone + Send,
    >(
        replicas: &mut Vec<Replica<S, I, O>>,
    ) -> Client<I, O> {
        let primary = Self::primary(replicas);

        let replica = replicas.get_mut(primary).expect("Primary index not valid");

        let AttachedChannel { client_id, channel } = replica.network.client.attach();
        Client::new(client_id, replica.view, channel)
    }

    fn primary<S: Clone + Service<Input = I, Output = O>, I: Clone + Send, O: Clone + Send>(
        replicas: &Vec<Replica<S, I, O>>,
    ) -> usize {
        assert!(!replicas.is_empty());

        let total = replicas.len();
        let majority = quorum(total) + 1;

        let mut views = HashMap::<usize, usize>::with_capacity(total);
        for replica in replicas {
            let count = views.entry(replica.view).or_default();
            *count += 1;
        }

        for (view, count) in views {
            if count >= majority {
                return view;
            }
        }

        panic!("Primary could not be found")
    }
}
