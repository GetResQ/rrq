# RRQ

Rust-first distributed Redis job queue, with Python and TypeScript bindings.

See @rrq-py/tests/CLAUDE.md for Python-specific testing guidelines.

## Commands
```bash
# Rust core (primary)
cd rrq-rs
cargo fmt && cargo clippy
cargo test

# TypeScript bindings
sh scripts/with-producer-lib.sh -- sh -c "cd rrq-ts && bun test"
cd rrq-ts && bun run lint

# Python bindings
cd rrq-py
uv run pytest
uv run pytest --maxfail=1
uv run ruff format && uv run ruff check --fix
uv run ty check
uv add <package>
```

## Repo-Wide Testing
```bash
# Rust
cd rrq-rs
cargo fmt && cargo clippy
cargo test

# TypeScript (requires Bun)
sh scripts/with-producer-lib.sh -- sh -c "cd rrq-ts && bun test"
cd rrq-ts && bun run lint

# Python (unit tests + warnings as errors)
cd rrq-py
uv run pytest
uv run pytest -W error

# Integration scenario runner (requires Redis + cargo + bun)
sh scripts/with-producer-lib.sh -- uv run --project rrq-py python examples/integration_test.py
# Smaller/isolated run (recommended if other RRQ processes are active)
sh scripts/with-producer-lib.sh -- uv run --project rrq-py python examples/integration_test.py --count 100 --redis-dsn redis://localhost:6379/15
```

Notes:
- `examples/integration_test.py` expects Redis running and an RRQ CLI binary
  available (PATH or `rrq-py/rrq/bin/rrq`).
- Use a dedicated Redis DB (`--redis-dsn redis://localhost:6379/15`) if any
  other RRQ workers/executors are running to avoid protocol/version conflicts.
- The `with-producer-lib.sh` wrapper builds and exports the producer FFI lib.

## Code Style
- Rust (`rrq-rs`): idiomatic Rust patterns, run `cargo fmt` and `clippy`.
- TypeScript (`rrq-ts`): keep strict typing and run `bun run lint`.
- Python (`rrq-py`): Python 3.11+, double quotes, 88 char lines.
- Python: type hints on all functions, Pydantic V2 for validation.
- Python: `snake_case` functions, `PascalCase` classes.
- Python: import order stdlib → third-party → local.
- Python: early returns, `match/case` for complex conditionals.
- Python: no blocking I/O in async contexts.

## Code References
Use VS Code clickable format:
- `rrq-rs/orchestrator/src/worker.rs:120`
- `rrq-py/rrq/queue.py:45`
- `rrq-ts/src/producer.ts:88`

## Rules
- Never commit broken tests
- Use `uv` for Python operations in `rrq-py`
- Follow existing patterns in codebase
- No sensitive data in logs
- Ask before large cross-domain changes
- Rust safety and reliability:
  - Avoid `unsafe` in production; if required, isolate and document invariants/assumptions.
  - Validate/sanitize external inputs (headers, query/body); use parameterized queries.
  - Prefer `Option`/`Result` and `?` for propagation; avoid `unwrap`/`expect` on external data.
    - Tests may use `unwrap`/`expect` for setup and assertions.
  - Avoid blocking calls in async contexts; use async I/O or `spawn_blocking` for CPU-bound work.
  - Enable overflow checks in release builds in `Cargo.toml`.
  - Evaluate third-party crates for maintenance/security; try to install and update to latest or reasonably new versions; run `cargo audit` after major changes.
  - Always run `cargo fmt` and `clippy` when you finish a piece of work
  - Warnings from clippy and compiler should be treated like errors.

- Adopt Idiomatic Rust Patterns: Use match expressions, enums, and traits to create expressive, functional-style logic; avoid C-like imperative constructs like long if-else chains or raw pointers, making the code more readable and maintainable.
- Structure generated code into modules and crates for reusability and separation of concerns; avoid monolithic files by advising on proper file organization to improve scalability and collaboration.
