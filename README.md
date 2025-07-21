# vsr
A library for distributed systems based on the [Viewstamped Replication Revisited](https://pmg.csail.mit.edu/pubs/liskov12vr-abstract.html) technique.

The current implementation supports all the protocols (_normal_, _view change_, _recovery_), as described in the original paper.

## Getting started

Run the example of a server:

```shell
cargo run --example server -- \
  --addresses=127.0.0.1:3001,127.0.0.1:3002,127.0.0.1:3003 \
  --seed=1234
```

Run the example of a client:

```shell
cargo run --example client -- \
  --replicas=127.0.0.1:3001,127.0.0.1:3002,127.0.0.1:3003 \
  --address=127.0.0.1:3000 \
  --seed=1234
```

## Simulation

Run the simulation:

```shell
cargo run --bin vsr_simulation -- --seed=1234
```
