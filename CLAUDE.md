# RRQ

Redis-based async job queue library for Python.

See @rrq-py/tests/CLAUDE.md for testing guidelines.

## Commands
```bash
cd rrq-py
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
Use VS Code clickable format: `rrq-py/rrq/queue.py:45` or `rrq-py/rrq/worker.py:120-135`

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
  - Evaluate third-party crates for maintenance/security; try to install and update to latest or reasonably new versions; run `cargo audit` after major changes.
  - Always run `cargo fmt` and `clippy` when you finish a piece of work
  - Warnings from clippy and compiler should be treated like errors.

- Adopt Idiomatic Rust Patterns: Use match expressions, enums, and traits to create expressive, functional-style logic; avoid C-like imperative constructs like long if-else chains or raw pointers, making the code more readable and maintainable.
- Structure generated code into modules and crates for reusability and separation of concerns; avoid monolithic files by advising on proper file organization to improve scalability and collaboration.
