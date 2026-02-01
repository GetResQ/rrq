# RRQ TypeScript

[![npm](https://img.shields.io/npm/v/rrq-ts.svg)](https://www.npmjs.com/package/rrq-ts)
[![License](https://img.shields.io/npm/l/rrq-ts.svg)](LICENSE)

**TypeScript/JavaScript client for RRQ**, the distributed job queue with a Rust-powered orchestrator.

## What is RRQ?

RRQ (Reliable Redis Queue) is a distributed job queue that separates the hard parts (scheduling, retries, locking, timeouts) into a Rust orchestrator while letting you write job handlers in your preferred language. It uses Redis as the source of truth with atomic operations for reliability.

**Why RRQ?**

- **Language flexibility** - Write job handlers in TypeScript, Python, or Rust
- **Rust orchestrator** - The complex distributed systems logic is handled by battle-tested Rust code
- **Production features built in** - Retries, dead letter queues, timeouts, cron scheduling, distributed tracing
- **Redis simplicity** - No separate databases; everything lives in Redis with predictable semantics

## This Package

This package provides:

- **Producer client** - Enqueue jobs from Node.js/Bun applications
- **Runner runtime** - Execute job handlers written in TypeScript

Works with **Node.js 20+** and **Bun**.

## Quick Start

### 1. Install

```bash
npm install rrq-ts
# or
bun add rrq-ts
```

### 2. Enqueue jobs (Producer)

```typescript
import { RRQClient } from "rrq-ts";

const client = new RRQClient({
  config: { redisDsn: "redis://localhost:6379/0" }
});

const jobId = await client.enqueue("send_email", {
  params: { to: "user@example.com", template: "welcome" },
  queueName: "emails",
  maxRetries: 5
});

console.log(`Enqueued job: ${jobId}`);
await client.close();
```

### 3. Write job handlers (Runner)

```typescript
import { RunnerRuntime, Registry } from "rrq-ts";

const registry = new Registry();

registry.register("send_email", async (request, signal) => {
  const { to, template } = request.params;

  // Your email sending logic here
  await sendEmail(to, template);

  return { sent: true, to };
});

const runtime = new RunnerRuntime(registry);
await runtime.runFromEnv();
```

### 4. Configure (`rrq.toml`)

```toml
[rrq]
redis_dsn = "redis://localhost:6379/0"
default_runner_name = "node"

[rrq.runners.node]
type = "socket"
cmd = ["node", "dist/runner.js"]
tcp_socket = "127.0.0.1:9000"
pool_size = 4
max_in_flight = 10
```

### 5. Run

```bash
# Install the RRQ orchestrator
cargo install rrq
# or download from releases

# Start the orchestrator (spawns runners automatically)
rrq worker run --config rrq.toml
```

## Producer API

### Enqueue Options

```typescript
interface EnqueueOptions {
  params?: Record<string, unknown>;  // Job parameters
  queueName?: string;                 // Target queue (default: "default")
  jobId?: string;                     // Custom job ID
  maxRetries?: number;                // Max retry attempts
  jobTimeoutSeconds?: number;         // Execution timeout
  resultTtlSeconds?: number;          // How long to keep results
  deferUntil?: Date;                  // Schedule for specific time
  deferBySeconds?: number;            // Delay execution
  traceContext?: Record<string, string>;  // Distributed tracing
}
```

### Unique Jobs (Idempotency)

```typescript
const jobId = await client.enqueueWithUniqueKey(
  "process_order",
  "order-123",  // unique key
  { params: { orderId: "123" } }
);
```

### Rate Limiting

```typescript
const jobId = await client.enqueueWithRateLimit("sync_user", {
  params: { userId: "456" },
  rateLimitKey: "user-456",
  rateLimitSeconds: 60
});

if (jobId === null) {
  console.log("Rate limited");
}
```

### Debouncing

```typescript
await client.enqueueWithDebounce("save_document", {
  params: { docId: "789" },
  debounceKey: "doc-789",
  debounceSeconds: 5
});
```

### Job Status

```typescript
const status = await client.getJobStatus(jobId);
console.log(status);
```

## Runner API

### Handler Signature

```typescript
type Handler = (
  request: ExecutionRequest,
  signal: AbortSignal
) => Promise<ExecutionOutcome | unknown> | ExecutionOutcome | unknown;
```

### Execution Request

```typescript
interface ExecutionRequest {
  protocol_version: string;
  job_id: string;
  request_id: string;
  function_name: string;
  params: Record<string, unknown>;
  context: {
    job_id: string;
    attempt: number;
    queue_name: string;
    enqueue_time: string;
    deadline?: string | null;
    trace_context?: Record<string, string> | null;
    worker_id?: string | null;
  };
}
```

### Outcome Types

```typescript
// Success
const success: ExecutionOutcome = {
  job_id: jobId,
  request_id: requestId,
  status: "success",
  result: { result: "data" },
};

// Failure (may be retried)
const failure: ExecutionOutcome = {
  job_id: jobId,
  request_id: requestId,
  status: "error",
  error: { message: "Something went wrong" },
};

// Explicit retry after delay
const retry: ExecutionOutcome = {
  job_id: jobId,
  request_id: requestId,
  status: "retry",
  error: { message: "Rate limited" },
  retry_after_seconds: 60,
};
```

### OpenTelemetry

```typescript
import { RunnerRuntime, Registry, OtelTelemetry } from "rrq-ts";

const runtime = new RunnerRuntime(registry, new OtelTelemetry());
await runtime.runFromEnv();
```

## Producer FFI Setup

The producer uses a Rust FFI library for consistent behavior across languages. The library is loaded from:

1. `RRQ_PRODUCER_LIB_PATH` environment variable
2. `rrq-ts/bin/<platform>-<arch>/` (for published packages)
3. `rrq-ts/bin/` (for development)

Build from source:

```bash
cargo build -p rrq-producer --release
# Copy to bin/darwin-arm64/ or bin/linux-x64/ as appropriate
```

## Related Packages

| Package | Language | Purpose |
|---------|----------|---------|
| [rrq-ts](https://www.npmjs.com/package/rrq-ts) | TypeScript | Producer + runner (this package) |
| [rrq](https://pypi.org/project/rrq/) | Python | Producer + runner |
| [rrq](https://crates.io/crates/rrq) | Rust | Orchestrator binary |
| [rrq-producer](https://crates.io/crates/rrq-producer) | Rust | Native producer |
| [rrq-runner](https://crates.io/crates/rrq-runner) | Rust | Native runner |

## Requirements

- Node.js 20+ or Bun
- Redis 5.0+
- RRQ orchestrator binary

## License

MIT
