# RRQ Orchestrator (Rust)

This crate is the Rust orchestrator for RRQ. It is intended to be fully
compatible with the Python producer (`rrq.client`) and Python stdio executor
runtime (`rrq.executor_runtime`).

## Status
- Core worker loop, Redis store ops, stdio executor pool, cron handling.
- CLI parity for worker run/watch/check, queue/job/dlq, debug commands.
- Engine conformance tests compare Rust outcomes to Python golden snapshots.

## Compatibility goals
- Redis schema and key prefixes match the Python implementation.
- Executor protocol matches `docs/EXECUTOR_PROTOCOL.md`.
- Job lifecycle rules mirror the legacy Python worker (`legacy/rrq/worker.py`).

## CLI

Run a worker using the same `rrq.toml` as the Python runtime:

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

Watch mode:

```bash
cd reference/rust
cargo run -p rrq-orchestrator -- worker watch --config ../../rrq.toml --path .
```

## Notes
- The Rust orchestrator launches stdio executors (including the Python runtime
  `rrq-executor`) via the configured `executors` section in TOML.
- Python producers can continue using `rrq.client` against the same Redis
  schema.
