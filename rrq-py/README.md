# RRQ: Reliable Redis Queue

RRQ is a Redis-backed job queue **system** with a Rust orchestrator and a
language-agnostic runner protocol. Producers can enqueue jobs from Python,
Rust, or any language that can write the job schema to Redis. Runners can be
written in any language that can speak the socket protocol.

This Python package (`rrq`) provides the producer SDK plus the Python runner
runtime. The Rust components are published as crates and are typically used to
run the orchestrator or build native producers/runners.

## Project Components

- **Python package (this)**: producer SDK, Python runner runtime, OpenTelemetry
  runner integration.
- **Rust orchestrator (`rrq` crate)**: scheduling, retries, timeouts, DLQ, cron.
- **Rust producer (`rrq-producer` crate)**: native producer for Rust apps.
- **Rust runner (`rrq-runner` crate)**: runner runtime + example.

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
- Rust `rrq` binary (orchestrator CLI; bundled in wheels or provided separately)
- Redis 5.0+

If you ship the Rust binary separately, set `RRQ_RUST_BIN` to its path.

### FFI producer library (local dev/tests)

The Python producer uses the Rust `rrq-producer` shared library. For local
tests from the repo, use the helper script to build the library and set
`RRQ_PRODUCER_LIB_PATH` automatically:

```bash
sh scripts/with-producer-lib.sh -- sh -c "cd rrq-py && uv run pytest"
```

Published wheels include `rrq/bin/librrq_producer.*` so no extra setup is
required in production unless you want to override the library path.

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
from rrq.runner_settings import PythonRunnerSettings
from rrq.registry import Registry

from . import handlers

registry = Registry()
registry.register("process_message", handlers.process_message)

python_runner_settings = PythonRunnerSettings(
    registry=registry,
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

async def main():
    client = RRQClient(config_path="rrq.toml")
    await client.enqueue("process_message", {"args": ["hello"]})
    await client.close()

asyncio.run(main())
```

### Job status + results

```python
status = await client.get_job_status(job_id)
```

### OpenTelemetry (runner)

```python
from rrq.integrations import otel

otel.enable(service_name="my-runner")
```

See `docs/TELEMETRY.md` for end-to-end tracing from producer → runner.

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
runner runtime. See `rrq/integrations/`.
