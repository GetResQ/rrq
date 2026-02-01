# RRQ: Reliable Redis Queue

[![PyPI](https://img.shields.io/pypi/v/rrq.svg)](https://pypi.org/project/rrq/)
[![License](https://img.shields.io/pypi/l/rrq.svg)](LICENSE)

**RRQ is a distributed job queue that actually works.** It combines a Rust-powered orchestrator with language-native workers to give you the reliability of battle-tested infrastructure with the flexibility of writing job handlers in Python, TypeScript, or Rust.

## Why RRQ?

Most job queues make you choose: either fight with complex distributed systems concepts, or accept unreliable "good enough" solutions. RRQ takes a different approach:

- **Rust orchestrator, any-language workers** - The hard parts (scheduling, retries, locking, timeouts) are handled by a single-binary Rust process. Your job handlers are just normal async functions in your preferred language.

- **Redis as the source of truth** - No separate databases to manage. Jobs, queues, locks, and results all live in Redis with atomic operations and predictable semantics.

- **Production-grade features built in** - Retry policies, dead letter queues, job timeouts, cron scheduling, distributed tracing, and health checks work out of the box.

- **Fast and lightweight** - The Rust orchestrator handles thousands of jobs per second with minimal memory. Workers are isolated processes that can be scaled independently.

## How It Works

```
┌──────────────────────────────┐
│     Your Application         │
│  (Python, TypeScript, Rust)  │
│                              │
│ client.enqueue("job", {...}) │
└───────────────┬──────────────┘
                │ enqueue jobs
                ▼
      ┌───────────────────────┐
      │         Redis         │
      │  queues, jobs, locks  │
      └──────────┬────────────┘
                 │ poll/dispatch
                 ▼
      ┌──────────────────────────────┐
      │     RRQ Orchestrator         │
      │   (single Rust binary)       │
      │ • scheduling & retries       │
      │ • timeouts & deadlines       │
      │ • dead letter queue          │
      │ • cron jobs                  │
      └──────────┬───────────────────┘
                 │ socket protocol
                 ▼
   ┌─────────────────────────────────────────┐
   │              Job Runners                 │
   │  ┌─────────────┐    ┌─────────────┐     │
   │  │   Python    │    │ TypeScript  │     │
   │  │   Worker    │    │   Worker    │     │
   │  └─────────────┘    └─────────────┘     │
   └─────────────────────────────────────────┘
```

## This Package

This Python package (`rrq`) gives you everything you need to work with RRQ from Python:

- **Producer client** - Enqueue jobs from your Python application
- **Runner runtime** - Execute job handlers written in Python
- **OpenTelemetry integration** - Distributed tracing from producer to runner

The Rust orchestrator binary is bundled in the wheel, so there's nothing else to install.

## Quick Start

### 1. Install

```bash
pip install rrq
# or
uv pip install rrq
```

### 2. Create configuration (`rrq.toml`)

```toml
[rrq]
redis_dsn = "redis://localhost:6379/0"
default_runner_name = "python"

[rrq.runners.python]
type = "socket"
cmd = ["rrq-runner", "--settings", "myapp.runner:settings"]
tcp_socket = "127.0.0.1:9000"
```

### 3. Write a job handler

```python
# myapp/handlers.py
from rrq.runner import ExecutionRequest

async def send_welcome_email(request: ExecutionRequest):
    user_email = request.params.get("email")
    template = request.params.get("template", "welcome")

    # Your email sending logic here
    await send_email(to=user_email, template=template)

    return {"sent": True, "email": user_email}
```

### 4. Register handlers

```python
# myapp/runner.py
from rrq.runner_settings import PythonRunnerSettings
from rrq.registry import Registry
from myapp import handlers

registry = Registry()
registry.register("send_welcome_email", handlers.send_welcome_email)

settings = PythonRunnerSettings(registry=registry)
```

### 5. Start the system

```bash
# Terminal 1: Start the orchestrator (runs runners automatically)
rrq worker run --config rrq.toml

# That's it! The orchestrator spawns and manages your Python runners.
```

### 6. Enqueue jobs

```python
import asyncio
from rrq.client import RRQClient

async def main():
    client = RRQClient(config_path="rrq.toml")

    job_id = await client.enqueue(
        "send_welcome_email",
        {
            "params": {
                "email": "user@example.com",
                "template": "welcome",
            }
        },
    )

    print(f"Enqueued job: {job_id}")
    await client.close()

asyncio.run(main())
```

## Features

### Scheduled Jobs

Delay job execution:

```python
# Run in 5 minutes
await client.enqueue("cleanup", {"defer_by_seconds": 300})

# Run at a specific time
from datetime import datetime, timezone
await client.enqueue(
    "report",
    {"defer_until": datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc)},
)
```

### Unique Jobs (Idempotency)

Prevent duplicate jobs:

```python
# Only one job with this key will be enqueued
await client.enqueue_with_unique_key(
    "process_order",
    "order-123",
    {"params": {"order_id": "123"}},
)
```

### Rate Limiting

Limit how often a job can run:

```python
job_id = await client.enqueue_with_rate_limit(
    "sync_user",
    {
        "params": {"user_id": "456"},
        "rate_limit_key": "user-456",
        "rate_limit_seconds": 60,
    },
)
if job_id is None:
    print("Rate limited, try again later")
```

### Debouncing

Delay and deduplicate rapid job submissions:

```python
# Only the last enqueue within the window will execute
await client.enqueue_with_debounce(
    "save_document",
    {
        "params": {"doc_id": "789"},
        "debounce_key": "doc-789",
        "debounce_seconds": 5,
    },
)
```

### Cron Jobs

Schedule recurring jobs in `rrq.toml`:

```toml
[[rrq.cron_jobs]]
function_name = "daily_report"
schedule = "0 0 9 * * *"  # 9 AM daily (6-field cron with seconds)
queue_name = "scheduled"
```

### Job Status

Check job progress:

```python
status = await client.get_job_status(job_id)
print(f"Status: {status}")
```

### OpenTelemetry

Enable distributed tracing:

```python
from rrq.integrations import otel
otel.enable(service_name="my-service")
```

Traces propagate from producer → orchestrator → runner automatically.

## Configuration Reference

See [docs/CONFIG_REFERENCE.md](docs/CONFIG_REFERENCE.md) for the full TOML schema.

Key settings:

```toml
[rrq]
redis_dsn = "redis://localhost:6379/0"
default_runner_name = "python"
default_job_timeout_seconds = 300  # 5 minutes
default_max_retries = 3

[rrq.runners.python]
type = "socket"
cmd = ["rrq-runner", "--settings", "myapp.runner:settings"]
tcp_socket = "127.0.0.1:9000"
pool_size = 4           # Number of runner processes
max_in_flight = 10      # Concurrent jobs per runner
```

## Related Packages

| Package | Language | Purpose |
|---------|----------|---------|
| [rrq](https://pypi.org/project/rrq/) | Python | Producer client + runner (this package) |
| [rrq-ts](https://www.npmjs.com/package/rrq-ts) | TypeScript | Producer client + runner |
| [rrq](https://crates.io/crates/rrq) | Rust | Orchestrator binary |
| [rrq-producer](https://crates.io/crates/rrq-producer) | Rust | Native producer client |
| [rrq-runner](https://crates.io/crates/rrq-runner) | Rust | Native runner runtime |

## Requirements

- Python 3.11+
- Redis 5.0+

## License

Apache-2.0
