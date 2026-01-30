# Rust reference implementation

This directory is a Cargo workspace containing:

- `rrq-protocol`: shared protocol types for socket executors
- `rrq-producer`: minimal producer that writes jobs to Redis
- `rrq-executor`: socket executor reference implementation
- `rrq` (in `rrq-orchestrator/`): Rust scheduler/orchestrator (worker) implementation

## Git dependency example

```toml
[dependencies]
rrq-protocol = { git = "https://github.com/getresq/rrq", package = "rrq-protocol", rev = "<sha>" }
rrq-producer = { git = "https://github.com/getresq/rrq", package = "rrq-producer", rev = "<sha>" }
```

## Executor example

```
cd rrq-rs/executor
RRQ_EXECUTOR_SOCKET=/tmp/rrq-executor.sock cargo run --example socket_executor
```

Localhost TCP variant:

```
RRQ_EXECUTOR_TCP_SOCKET=127.0.0.1:9000 cargo run --example socket_executor
```

## Tests

Run the workspace tests:

```
cd rrq-rs
cargo test
```
