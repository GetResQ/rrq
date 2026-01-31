# RRQ: Reliable Redis Queue

RRQ is a Redis-backed job queue **system** with a Rust orchestrator and a
language-agnostic runner protocol. Producers can enqueue jobs from Python,
Rust, or any language that can write the job schema to Redis. Runners can be
written in any language that can speak the socket protocol. The orchestrator is
implemented in Rust, with runners available in multiple languages.

## At a Glance

- **Rust orchestrator**: schedules, retries, timeouts, DLQ, cron.
- **TCP runners**: Python, Rust, or any other runtime.
- **Python SDK**: enqueue jobs and run a Python runner runtime.

## Repo Layout

- `.github/` CI workflows
- `docs/` design docs, protocols, references
- `examples/` Python + Rust usage samples
- `rrq-py/` Python package (`rrq`), tests, and build tooling
- `rrq-rs/` Rust workspace (orchestrator, producer, runner, protocol)
- `rrq-ts/` placeholder for the future TypeScript package

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
      │  - DLQ lists          │
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
                 │ TCP socket protocol
                 │ (request <-> outcome)
                 ▼
   ┌─────────────────────┬─────────────────────┐
   │ Python Runner     │ Rust/Other Runner │
   │ (rrq-runner)      │ (rrq-runner)      │
   └───────────────────────────────────────────┘
```

Runners return outcomes to the orchestrator; the orchestrator persists job
state/results back to Redis.

## Requirements

- Python 3.11+ (producer + Python runner runtime)
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
default_runner_name = "python"

[rrq.runners.python]
type = "socket"
cmd = ["rrq-runner", "--settings", "myapp.runner_config.python_runner_settings"]
# Required: localhost TCP socket (host:port). For pool_size > 1, ports increment.
tcp_socket = "127.0.0.1:9000"
```

### 3) Register Python handlers

```python
# runner_config.py
from rrq.config import load_toml_settings
from rrq.runner_settings import PythonRunnerSettings
from rrq.registry import JobRegistry

from . import handlers

job_registry = JobRegistry()
job_registry.register("process_message", handlers.process_message)

rrq_settings = load_toml_settings("rrq.toml")

python_runner_settings = PythonRunnerSettings(
    rrq_settings=rrq_settings,
    job_registry=job_registry,
)
```

### 4) Run the Python runner

```
rrq-runner --settings myapp.runner_config.python_runner_settings
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

`rrq.toml` is the source of truth for the orchestrator and runners. Key areas:

- `[rrq]` basic settings (Redis, retries, timeouts, poll delay)
- `[rrq.runners.<name>]` runner commands, pool sizes, and
  `max_in_flight`
- `[rrq.runner_routes]` queue → runner mapping (legacy `[rrq.routing]` still
  accepted)
- `[[rrq.cron_jobs]]` periodic scheduling
- `[rrq.watch]` watch mode defaults (path/patterns)

See `docs/CONFIG_REFERENCE.md` for the full TOML schema,
`docs/CLI_REFERENCE.md` for CLI details, and `docs/RUNNER_PROTOCOL.md` for
wire format.

## Cron Jobs (rrq.toml)

Use `[[rrq.cron_jobs]]` entries to enqueue periodic jobs while a worker is
running. Schedules are evaluated in UTC.

```toml
[[rrq.cron_jobs]]
function_name = "process_message"
schedule = "0 * * * * *"
args = ["cron payload"]
kwargs = { source = "cron" }
queue_name = "default"
unique = true
```

Fields:
- `function_name` (required): Handler name to enqueue.
- `schedule` (required): Cron expression with seconds (6-field format).
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
graceful worker shutdown, closes runners, and starts a fresh worker. Watch
mode is intended for local development; runner pool sizes and
`max_in_flight` are forced to 1 to keep restarts lightweight. It also respects
`.gitignore` and `.git/info/exclude` by default; disable with `--no-gitignore`.

You can also configure watch defaults in `rrq.toml`:

```toml
[rrq.watch]
path = "."
include_patterns = ["*.py", "*.toml"]
ignore_patterns = [".venv/**", "dist/**"]
no_gitignore = false
```

## Runner Logs

Runners can emit logs to stdout/stderr. The orchestrator captures these lines
and emits them with runner prefixes. Structured logging is not part of the
wire protocol.

## Testing

Runtime-only Python tests (producer + runner + store):

```
cd rrq-py
uv run pytest
```

End-to-end integration (Python-only, Rust-only, mixed):

```
cd rrq-py
uv run python ../examples/integration_test.py
```

## Reference Implementations

- Rust orchestrator (crate `rrq`): `rrq-rs/orchestrator`
- Rust producer: `rrq-rs/producer`
- Rust runner: `rrq-rs/runner`
- Protocol types: `rrq-rs/protocol`
- Python runner examples: `examples/python/`

## Telemetry

Optional tracing integrations are available for Python producers and the Python
runner runtime. See `rrq-py/rrq/integrations/`.
