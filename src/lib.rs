use std::collections::HashMap;

use client::Client;
use crossbeam::channel::unbounded;
use network::{AttachedChannel, ClientConnection, Message, ReplicaNetwork};
use replica::{Replica, quorum};
use thiserror::Error;

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

View change protocol
 1. A backup replica has not heard from the primary in a while (either from prepare messages or commit messages) and so, it carries out a view change to switch to a new primary.
 2. The replica sets its status to "view-change" and sends a <StartViewChange, new_view, replica_number> to all the other replicas.
 3. A replica notices a view change either from its own timer, a StartViewChange message or a DoViewChange message for a view number larger than its own view_number.
 4. When a replica receives a StartViewChange message for its view_number from `f` other replicas, it sends a <DoViewChange, old_view, log, new_view, operation_number, commit_number, replica_number> to the replica that will be the primary in the new view.
 5. When the new primary receives `f + 1` DoViewChange messages from different replicas (including itself), it sets its view_number to the `new_view` number and selects as the new log the one contained in the message with the largest `old_view`; if several it selects the one with the largest `operation_number`. It sets its `operation_number` to the topmost entry in the log, sets its `commit_number` to the topmost received `commit_number`, changes its statuts to `normal`, and it informs the other replicas about the completion of the view change by sending <StartView, view_number, new log, operation_number, commit_number> to the other replicas.
 6. The primary starts to accept client requests again. It also executes (in order) any committed operations that it hadn't executed previously, updates its client table, and sends the replies to the clients.
 7. When other replicas receive the StartView message, they replace their log with the one in the message, set their operation_number to that latest entry in the log, set their view_number to the view number in the message, change their status to normal, and update the information in the `client_table`. If there are non-committed operations in the log, they send a `PrepareOk` message to the primary. They execute all operations that were not committed previously, advance their `commit_number` and update their information in the `client_table`.

Recovery protocol
 1. A recovering replica sends a <Recovery, replica_number, nonce> to all the other replicas. `nonce` is a random unique number.
 2. Another replica responds the message <RecoveryResponse, view_number, nonce, log, operation_number, commit_number> only when its status is normal. Only the primary replica sends `log`, `operation_number`, `commit_number`. A backup replica won't provide those.
 3. The recovering replica waits to receiver at least `quorum + 1` messages from different replicas with the `nonce` value sent, including one from the primary of the latest view it learns of these messages. Then, it updates its state using the information from the primary, changes its status to normal and is available to receive more requests.
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
    ) -> Vec<Replica<S, ReplicaNetwork<I, O>, I, O>> {
        let total = config.addresses.len();

        let mut channels = Vec::with_capacity(total);
        for _ in &config.addresses {
            channels.push(unbounded::<Message<I>>())
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
        replicas: &mut Vec<Replica<S, ReplicaNetwork<I, O>, I, O>>,
    ) -> Client<I, O> {
        let primary = Self::primary(replicas);

        let replica = replicas.get_mut(primary).expect("Primary index not valid");

        let AttachedChannel { client_id, channel } = replica.network.client.attach();
        Client::new(client_id, replica.view, channel)
    }

    fn primary<S: Clone + Service<Input = I, Output = O>, I: Clone + Send, O: Clone + Send>(
        replicas: &Vec<Replica<S, ReplicaNetwork<I, O>, I, O>>,
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

#[derive(Error, Debug, PartialEq, Clone)]
#[non_exhaustive]
pub enum ServiceError {
    #[error("Unrecoverable error: {0}")]
    Unrecoverable(String),
    #[error("Recoverable error: {0}")]
    Recoverable(String),
}

pub trait Service {
    type Input;
    type Output;

    fn execute(&self, input: &Self::Input) -> Result<Self::Output, ServiceError>;
}
