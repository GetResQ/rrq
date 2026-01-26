# Engine Conformance Strategy

This repo includes a scheduler/orchestrator conformance harness in
`legacy/tests/test_engine_conformance.py`. The goal is to validate behavior
against a stable **Python baseline** today, then use the same scenarios to
validate the Rust orchestration engine.

## What exists now
- **Scenario-based tests** for success, retry, failure, timeout, and
  handler-not-found.
- A **golden snapshot** (`tests/data/engine_golden/basic.json`) that captures
  normalized job outcomes for a multi-job scenario with deterministic job IDs.
- An **extended snapshot** (`tests/data/engine_golden/extended.json`) that adds
  coverage for deferrals, unique keys, custom queues, and DLQ routing.
- A helper that runs the Python engine end-to-end and normalizes results.
- A Rust conformance test at `reference/rust/rrq-orchestrator/tests/engine_conformance.rs`
  that compares outcomes against the same golden snapshot.
- Additional scenarios cover retry backoff behavior and executor routing.

## How to update the golden snapshot
```bash
RRQ_UPDATE_GOLDENS=1 uv run pytest legacy/tests/test_engine_conformance.py::test_engine_conformance_golden_basic
```

Extended scenario:

```bash
RRQ_UPDATE_GOLDENS=1 uv run pytest legacy/tests/test_engine_conformance.py::test_engine_conformance_golden_extended
```

Retry backoff scenario:

```bash
RRQ_UPDATE_GOLDENS=1 uv run pytest legacy/tests/test_engine_conformance.py::test_engine_conformance_golden_retry_backoff
```

Executor routing scenario:

```bash
RRQ_UPDATE_GOLDENS=1 uv run pytest legacy/tests/test_engine_conformance.py::test_engine_conformance_golden_routing
```

## Strategy for Rust orchestration later
1. **Implement a Rust engine runner** that can process the same Redis schema.
2. **Reuse the same scenarios** by enqueuing the same deterministic job IDs.
3. **Normalize outputs** in the same shape as the Python engine.
4. **Compare against `basic.json`** (or additional golden files) to ensure
   parity in statuses, retry counts, DLQ decisions, and results.

## Rust conformance test
Run from the Rust workspace:

```bash
cd reference/rust
cargo test -p rrq-orchestrator --test engine_conformance
```

## Notes
- The tests deliberately **ignore transient timestamps** and focus on terminal
  state, retry counts, and DLQ membership.
- If you extend scheduling semantics (e.g., new retry policies), update the
  golden snapshots and add new scenarios.
