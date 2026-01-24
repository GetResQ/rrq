# Rust reference implementation

This directory is a Cargo workspace containing:

- `rrq-protocol`: shared protocol types for stdio executors
- `rrq-producer`: minimal producer that writes jobs to Redis
- `rrq-executor`: stdio executor reference implementation

## Git dependency example

```toml
[dependencies]
rrq-protocol = { git = "https://github.com/YOUR_ORG/rrq", package = "rrq-protocol", rev = "<sha>" }
rrq-producer = { git = "https://github.com/YOUR_ORG/rrq", package = "rrq-producer", rev = "<sha>" }
```

## Executor example

```
cd reference/rust/rrq-executor
cargo run --example stdio_executor
```

## Tests

Run the workspace tests:

```
cd reference/rust
cargo test
```
