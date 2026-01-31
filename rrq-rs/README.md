# Rust reference implementation

This directory is a Cargo workspace containing:

- `rrq-protocol`: shared protocol types for runners
- `rrq-producer`: minimal producer that writes jobs to Redis
- `rrq-runner`: runner reference implementation
- `rrq` (in `rrq-orchestrator/`): Rust scheduler/orchestrator (worker) implementation

## Git dependency example

```toml
[dependencies]
rrq-protocol = { git = "https://github.com/getresq/rrq", package = "rrq-protocol", rev = "<sha>" }
rrq-producer = { git = "https://github.com/getresq/rrq", package = "rrq-producer", rev = "<sha>" }
```

## Runner example

```
cd rrq-rs/runner
RRQ_RUNNER_TCP_SOCKET=127.0.0.1:9000 cargo run --example socket_runner
```

## Tests

Run the workspace tests:

```
cd rrq-rs
cargo test
```
