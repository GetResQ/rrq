# Engine Conformance Strategy

This repo includes a scheduler/orchestrator conformance harness in
`reference/python-orchestrator/tests/test_engine_conformance.py`. The goal is to validate behavior
against a stable **Python reference**, then ensure the Rust orchestrator matches
those outcomes.

## What exists now
- **Scenario-based tests** for success, retry, failure, timeout, and
  handler-not-found.
- A **golden snapshot** (`tests/data/engine_golden/basic.json`) that captures
  normalized job outcomes for a multi-job scenario with deterministic job IDs.
- An **extended snapshot** (`tests/data/engine_golden/extended.json`) that adds
  coverage for deferrals, unique keys, custom queues, and DLQ routing.
- A helper that runs the Python engine end-to-end and normalizes results.
- A Rust conformance test at
  `rrq-rs/rrq-orchestrator/tests/engine_conformance.rs` that compares
  outcomes against the same golden snapshot.
- Additional scenarios cover retry backoff behavior and executor routing.

## How to update the golden snapshot
```bash
RRQ_UPDATE_GOLDENS=1 uv run pytest reference/python-orchestrator/tests/test_engine_conformance.py::test_engine_conformance_golden_basic
```

Extended scenario:

```bash
RRQ_UPDATE_GOLDENS=1 uv run pytest reference/python-orchestrator/tests/test_engine_conformance.py::test_engine_conformance_golden_extended
```

Retry backoff scenario:

```bash
RRQ_UPDATE_GOLDENS=1 uv run pytest reference/python-orchestrator/tests/test_engine_conformance.py::test_engine_conformance_golden_retry_backoff
```

Executor routing scenario:

```bash
RRQ_UPDATE_GOLDENS=1 uv run pytest reference/python-orchestrator/tests/test_engine_conformance.py::test_engine_conformance_golden_routing
```

## Parity workflow
1. **Add or extend scenarios** in the Python test harness to define expected
   behavior.
2. **Regenerate goldens** using the Python reference when behavior changes.
3. **Run Rust conformance tests** to ensure outcomes match the goldens.
4. **Update documentation** if new semantics are introduced.

## Rust conformance test
Run from the Rust workspace:

```bash
cd rrq-rs
cargo test -p rrq --test engine_conformance
```

## Notes
- The tests deliberately **ignore transient timestamps** and focus on terminal
  state, retry counts, and DLQ membership.
- If you extend scheduling semantics (e.g., new retry policies), update the
  golden snapshots and add new scenarios.
