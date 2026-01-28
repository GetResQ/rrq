# RRQ (Rust)

This crate is the Rust orchestrator for RRQ (published as `rrq`). It is intended to be fully
compatible with the Python producer (`rrq.client`) and Python socket executor
runtime (`rrq.executor_runtime`).

## Status
- Core worker loop, Redis store ops, socket executor pool, cron handling.

## Compatibility goals
- Redis schema and key prefixes match the Python implementation.
- Executor protocol matches `docs/EXECUTOR_PROTOCOL.md`.

## CLI

Run a worker using the same `rrq.toml` as the Python runtime:

```bash
cd rrq-rs
cargo run -p rrq -- worker run --config ../../rrq.toml
```

Worker heartbeat check:

```bash
cd rrq-rs
cargo run -p rrq -- check --config ../../rrq.toml
```

Queue inspection:

```bash
cd rrq-rs
cargo run -p rrq -- queue list --config ../../rrq.toml
```

Watch mode:

```bash
cd rrq-rs
cargo run -p rrq -- worker watch --config ../../rrq.toml --path .
```

## Notes
- The Rust orchestrator launches socket executors (including the Python runtime
  `rrq-executor`) via the configured `executors` section in TOML.
- Python producers can continue using `rrq.client` against the same Redis
  schema.
