# Rust examples

## Producer

```
cd examples/rust/producer
cargo run
```

This example uses the published `rrq-producer` crate from crates.io.

Env vars:
- `RRQ_REDIS_DSN` (default: `redis://localhost:6379/3`)
- `RRQ_QUEUE` (default: `default`)
- `RRQ_FUNCTION` (default: `quick_task`)
- `RRQ_COUNT` (default: `5`)

## Runner (consumer)

The reference Rust runner is available via the `rrq-runner` crate.
You can run the example binary from crates.io:

```
cargo install rrq-runner --example socket_runner
RRQ_RUNNER_TCP_SOCKET=127.0.0.1:9000 socket_runner
```

Then point your worker config to the binary:

```toml
[rrq.runners.rust]
type = "socket"
cmd = ["/path/to/socket_runner"]
tcp_socket = "127.0.0.1:9000"
```
