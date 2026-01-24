# Rust examples

## Producer

```
cd examples/rust/producer
cargo run
```

This example uses the `rrq-producer` crate from `reference/rust/rrq-producer`.

Env vars:
- `RRQ_REDIS_DSN` (default: `redis://localhost:6379/3`)
- `RRQ_QUEUE` (default: `default`)
- `RRQ_FUNCTION` (default: `quick_task`)
- `RRQ_COUNT` (default: `5`)

## Executor (consumer)

The reference Rust stdio executor lives in `reference/rust/rrq-executor`.

```
cd reference/rust/rrq-executor
cargo run --example stdio_executor
```

Then point your worker config to the binary:

```toml
[rrq.executors.rust]
type = "stdio"
cmd = ["/path/to/stdio_executor"]
```
