# RRQ

Redis-based async job queue library for Python.

See @tests/CLAUDE.md for testing guidelines.

## Commands
```bash
uv run pytest                     # Run tests
uv run pytest --maxfail=1         # Debug failing tests
uv run ruff format && uv run ruff check --fix   # Format and lint (run before commits)
uv run ty check                   # Type check (must pass before commits)
uv add <package>                  # Add dependency
```

## Code Style
- Python 3.11+, double quotes, 88 char lines
- Type hints on all functions, Pydantic V2 for validation
- `snake_case` functions, `PascalCase` classes
- Import order: stdlib → third-party → local
- Early returns, `match/case` for complex conditionals
- No blocking I/O in async contexts

## Code References
Use VS Code clickable format: `rrq/queue.py:45` or `rrq/worker.py:120-135`

## Rules
- Never commit broken tests
- Use `uv` for all Python operations
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
  - Evaluate third-party crates for maintenance/security; add `cargo audit` to CI.
  - Always run `cargo fmt` and `clippy` when you finish a piece of work
  - Warnings from clippy and compiler should be treated like errors.
