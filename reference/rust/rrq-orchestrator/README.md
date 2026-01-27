# RRQ Orchestrator

The core orchestrator for RRQ, responsible for job scheduling, retries, timeouts,
dead-letter queue (DLQ) handling, and cron job management.

## Features

- Worker loop with Redis-backed job storage
- Stdio executor pool management
- Cron job scheduling
- CLI for worker management, queue inspection, and debugging

## Compatibility

- Works with the Python SDK (`rrq.client`) and Python executor runtime
- Works with the Rust producer and executor crates
- Executor protocol defined in `docs/EXECUTOR_PROTOCOL.md`

## CLI

Run a worker:

```bash
cd reference/rust
cargo run -p rrq-orchestrator -- worker run --config ../../rrq.toml
```

Worker heartbeat check:

```bash
cd reference/rust
cargo run -p rrq-orchestrator -- check --config ../../rrq.toml
```

Queue inspection:

```bash
cd reference/rust
cargo run -p rrq-orchestrator -- queue list --config ../../rrq.toml
```

Watch mode (auto-restart on file changes):

```bash
cd reference/rust
cargo run -p rrq-orchestrator -- worker watch --config ../../rrq.toml --path .
```

## Notes

- The orchestrator launches stdio executors (Python, Rust, or any language) via
  the configured `executors` section in TOML.
- Producers can enqueue jobs using either the Python SDK or Rust producer crate.
