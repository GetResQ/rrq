# Rust examples

## Producer

```
cd examples/rust/producer
cargo run
```

This example uses the `rrq-producer` crate from `rrq-rs/rrq-producer`.

Env vars:
- `RRQ_REDIS_DSN` (default: `redis://localhost:6379/3`)
- `RRQ_QUEUE` (default: `default`)
- `RRQ_FUNCTION` (default: `quick_task`)
- `RRQ_COUNT` (default: `5`)

## Executor (consumer)

The reference Rust socket executor lives in `rrq-rs/rrq-executor`.

```
cd rrq-rs/rrq-executor
RRQ_EXECUTOR_SOCKET=/tmp/rrq-executor.sock cargo run --example socket_executor
```

Then point your worker config to the binary:

```toml
[rrq.executors.rust]
type = "socket"
cmd = ["/path/to/socket_executor"]
```
