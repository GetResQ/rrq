# RRQ: Reliable Redis Queue

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**A distributed job queue that actually works.** RRQ combines a battle-tested Rust orchestrator with language-flexible workers, giving you the reliability of proven infrastructure with the flexibility of writing job handlers in Python, TypeScript, or Rust.

## Why RRQ?

Most job queues make you choose: either fight with complex distributed systems concepts, or accept unreliable "good enough" solutions. RRQ takes a different approach:

- **Rust orchestrator, any-language workers** - The hard parts (scheduling, retries, locking, timeouts) are handled by a single-binary Rust orchestrator. Your job handlers are just normal async functions in your preferred language.

- **Redis as the source of truth** - No separate databases to manage. Jobs, queues, locks, and results all live in Redis with atomic operations and predictable semantics.

- **Production-grade features built in** - Retry policies, dead letter queues, job timeouts, cron scheduling, distributed tracing, and health checks work out of the box.

- **Fast and lightweight** - The Rust orchestrator handles thousands of jobs per second with minimal memory. Workers are isolated processes that can be scaled independently.

## Architecture

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

## Packages

| Package | Language | Purpose | Install |
|---------|----------|---------|---------|
| [rrq](https://pypi.org/project/rrq/) | Python | Producer + runner | `pip install rrq` |
| [rrq-ts](https://www.npmjs.com/package/rrq-ts) | TypeScript | Producer + runner | `npm install rrq-ts` |
| [rrq](https://crates.io/crates/rrq) | Rust | Orchestrator CLI | `cargo install rrq` |
| [rrq-producer](https://crates.io/crates/rrq-producer) | Rust | Native producer | `rrq-producer = "0.9"` |
| [rrq-runner](https://crates.io/crates/rrq-runner) | Rust | Native runner | `rrq-runner = "0.9"` |

## Quick Start (Python)

### 1. Install

```bash
pip install rrq
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

### 3. Write a handler

```python
# myapp/handlers.py
async def send_email(request):
    email = request.params.get("email")
    await do_send_email(email)
    return {"sent": True}
```

### 4. Register and configure

```python
# myapp/runner.py
from rrq.registry import Registry
from rrq.runner_settings import PythonRunnerSettings
from myapp import handlers

registry = Registry()
registry.register("send_email", handlers.send_email)
settings = PythonRunnerSettings(registry=registry)
```

### 5. Run

```bash
rrq worker run --config rrq.toml
```

### 6. Enqueue jobs

```python
from rrq.client import RRQClient

client = RRQClient(config_path="rrq.toml")
job_id = await client.enqueue("send_email", {"params": {"email": "user@example.com"}})
```

## Quick Start (TypeScript)

```typescript
import { RRQClient, RunnerRuntime, Registry } from "rrq-ts";

// Enqueue jobs
const client = new RRQClient({ config: { redisDsn: "redis://localhost:6379/0" } });
await client.enqueue("send_email", { params: { email: "user@example.com" } });

// Run handlers
const registry = new Registry();
registry.register("send_email", async (request) => ({
  job_id: request.job_id,
  request_id: request.request_id,
  status: "success",
  result: { sent: true }
}));
const runtime = new RunnerRuntime(registry);
await runtime.runFromEnv();
```

## Features

- **Scheduled jobs** - Defer execution by seconds or until a specific time
- **Unique jobs** - Idempotency with unique keys
- **Rate limiting** - Throttle job execution per key
- **Debouncing** - Deduplicate rapid submissions
- **Cron scheduling** - Recurring jobs with cron syntax
- **Dead letter queue** - Failed jobs are preserved for debugging
- **Distributed tracing** - OpenTelemetry context propagation
- **Watch mode** - Auto-restart runners on code changes

## Repository Structure

```
rrq/
├── docs/           # Documentation
├── examples/       # Usage examples
├── rrq-py/         # Python package
├── rrq-rs/         # Rust workspace (orchestrator, producer, runner, protocol)
└── rrq-ts/         # TypeScript package
```

## Requirements

- Redis 5.0+
- Python 3.11+ (for Python SDK)
- Node.js 20+ or Bun (for TypeScript SDK)

## Documentation

- [Configuration Reference](docs/CONFIG_REFERENCE.md)
- [CLI Reference](docs/CLI_REFERENCE.md)
- [Runner Protocol](docs/RUNNER_PROTOCOL.md)
- [Telemetry](docs/TELEMETRY.md)

## License

Apache-2.0
