# RRQ Orchestrator (Rust)

This crate is the Rust scheduler/orchestrator for RRQ. It is intended to be
fully compatible with the existing Python producer (`rrq.client`) and Python
stdio executor runtime (`rrq.executor_runtime`).

## Status
- Core worker loop, Redis store ops, stdio executor pool, cron handling.
- CLI parity for run/watch/health/queue/monitor commands.
- Engine conformance tests compare Rust outcomes to Python golden snapshots.

## Compatibility goals
- Redis schema and key prefixes match the Python implementation.
- Executor protocol matches `docs/EXECUTOR_PROTOCOL.md`.
- Job lifecycle rules mirror `rrq/worker.py`.

## CLI

Run a worker using the same `rrq.toml` as the Python orchestrator:

```bash
cd reference/rust
cargo run -p rrq-orchestrator -- worker run --config ../../rrq.toml
```

Health check:

```bash
cd reference/rust
cargo run -p rrq-orchestrator -- health --config ../../rrq.toml
```

Queue inspection:

```bash
cd reference/rust
cargo run -p rrq-orchestrator -- queue list --config ../../rrq.toml
```

Watch mode:

```bash
cd reference/rust
cargo run -p rrq-orchestrator -- worker watch --config ../../rrq.toml --path .
```

## Notes
- The Rust orchestrator is designed to launch the existing Python stdio executor
  (see `rrq.executor_runtime`) via the configured `executors` section in TOML.
- Python producers can continue using `rrq.client` against the same Redis schema.
