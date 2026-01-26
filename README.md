# RRQ: Reliable Redis Queue

RRQ is a Redis-backed job queue **system** with a Rust orchestrator and a
language-agnostic executor protocol. Producers can enqueue jobs from Python,
Rust, or any language that can write the job schema to Redis. Executors can be
written in any language that can speak the stdio protocol. Python remains a
first-class runtime for producers and executors; orchestration is Rust-first.

## At a Glance

- **Rust orchestrator**: schedules, retries, timeouts, DLQ, cron.
- **Stdio executors**: Python, Rust, or any other runtime.
- **Python SDK**: enqueue jobs and run a Python executor runtime.

## Architecture

```
┌──────────────────────────────┐
│        Producers SDKs        │
│  (Python, Rust, other langs) │
└───────────────┬──────────────┘
                │ enqueue jobs
                ▼
      ┌───────────────────────┐
      │         Redis         │
      │  - queues (ZSETs)     │
      │  - job hashes         │
      │  - locks              │
      │  - DLQ list           │
      └──────────┬────────────┘
                 │ poll/lock
                 ▼
      ┌──────────────────────────────┐
      │   Rust RRQ Orchestrator      │
      │     (rrq worker run)         │
      │ - scheduling + retries       │
      │ - timeouts + DLQ             │
      │ - queue routing              │
      │ - cron jobs                  │
      └──────────┬───────────────────┘
                 │ stdio JSONL protocol
                 ▼
   ┌─────────────────────┬─────────────────────┐
   │ Python Executor     │ Rust/Other Executor │
   │ (rrq-executor)      │ (rrq-executor)      │
   └─────────┬───────────┴─────────┬───────────┘
             │ ExecutionOutcome     │
             └──────────┬──────────┘
                        ▼
                 ┌─────────────┐
                 │    Redis    │
                 │ job updates │
                 └─────────────┘
```

## Requirements

- Python 3.11+ (producer + Python executor runtime)
- Rust `rrq` binary (bundled in wheels or provided separately)
- Redis 5.0+

If you ship the Rust binary separately, set `RRQ_RUST_BIN` to its path.

## Quickstart

### 1) Install

```
uv pip install rrq
```

### 2) Create `rrq.toml`

```toml
[rrq]
redis_dsn = "redis://localhost:6379/1"
default_executor_name = "python"

[rrq.executors.python]
type = "stdio"
cmd = ["rrq-executor", "--settings", "myapp.executor_config.python_executor_settings"]
```

### 3) Register Python handlers

```python
# executor_config.py
import os
from pathlib import Path

from rrq.config import load_toml_settings
from rrq.executor_settings import PythonExecutorSettings
from rrq.registry import JobRegistry

from . import handlers

job_registry = JobRegistry()
job_registry.register("process_message", handlers.process_message)

config_path = Path(os.getenv("RRQ_EXECUTOR_CONFIG", "rrq.toml"))
rrq_settings = load_toml_settings(str(config_path))

python_executor_settings = PythonExecutorSettings(
    rrq_settings=rrq_settings,
    job_registry=job_registry,
)
```

### 4) Run the Python executor

```
rrq-executor --settings myapp.executor_config.python_executor_settings
```

### 5) Run the Rust orchestrator

```
rrq worker run --config rrq.toml
```

### 6) Enqueue jobs (Python)

```python
import asyncio
from rrq.client import RRQClient
from rrq.config import load_toml_settings

async def main():
    settings = load_toml_settings("rrq.toml")
    client = RRQClient(settings=settings)
    await client.enqueue("process_message", "hello")
    await client.close()

asyncio.run(main())
```

## Configuration

`rrq.toml` is the source of truth for the orchestrator and executors. Key areas:

- `[rrq]` basic settings (Redis, retries, timeouts, poll delay)
- `[rrq.executors.<name>]` stdio executor commands and pool sizes
- `[rrq.routing]` queue → executor mapping
- `[[rrq.cron_jobs]]` periodic scheduling

See `docs/CLI_REFERENCE.md` for CLI details and `docs/EXECUTOR_PROTOCOL.md` for
wire format.

## Cron Jobs (rrq.toml)

Use `[[rrq.cron_jobs]]` entries to enqueue periodic jobs while a worker is
running. Schedules are evaluated in UTC.

```toml
[[rrq.cron_jobs]]
function_name = "process_message"
schedule = "* * * * *"
args = ["cron payload"]
kwargs = { source = "cron" }
queue_name = "default"
unique = true
```

Fields:
- `function_name` (required): Handler name to enqueue.
- `schedule` (required): Cron expression (standard 5-field format).
- `args` / `kwargs`: Optional arguments passed to the handler.
- `queue_name`: Optional override for the target queue.
- `unique`: Optional; uses a per-function unique lock to prevent duplicates.

## CLI Overview (Rust `rrq`)

- `rrq worker run`, `rrq worker watch`
- `rrq check`
- `rrq queue list|stats|inspect`
- `rrq job show|list|trace|replay|cancel`
- `rrq dlq list|stats|inspect|requeue`
- `rrq debug generate-jobs|generate-workers|submit|clear|stress-test`

## Worker Watch Mode

`rrq worker watch` runs a normal worker loop plus a filesystem watcher. It
watches a path recursively and normalizes change paths before matching include
globs (default `*.py`, `*.toml`) and ignore globs. A matching change triggers a
graceful worker shutdown, closes executors, and starts a fresh worker. Watch
mode is intended for local development; executor pool sizes are forced to 1 to
keep restarts lightweight.

## Testing

Runtime-only Python tests (producer + executor + store):

```
uv run pytest
```

Rust conformance tests (parity vs golden snapshots):

```
cd reference/rust
cargo test -p rrq-orchestrator --test engine_conformance
```

End-to-end integration (Python-only, Rust-only, mixed):

```
uv run python -m examples.integration_test
```

## Legacy Python Orchestrator

The retired Python orchestrator and its tests/examples are preserved under
`legacy/` for reference.

## Reference Implementations

- Rust orchestrator: `reference/rust/rrq-orchestrator`
- Rust producer: `reference/rust/rrq-producer`
- Rust executor: `reference/rust/rrq-executor`
- Protocol types: `reference/rust/rrq-protocol`
- Python stdio executor example: `reference/python/stdio_executor.py`

## Telemetry

Optional tracing integrations are available for Python producers and the Python
executor runtime. See `rrq/integrations/`.
