# RRQ TypeScript (Producer + Executor)

TypeScript/JavaScript package for RRQ (Reliable Redis Queue). Includes:

- **Producer** client for enqueuing jobs into RRQ.
- **Executor runtime** to run job handlers over the RRQ socket protocol.

Designed to work with **Node.js** and **Bun**.

## Installation

```bash
npm install rrq-ts
```

## Producer usage

```ts
import { RRQClient } from "rrq-ts";

const client = new RRQClient({
  redisDsn: "redis://localhost:6379/0",
});

const jobId = await client.enqueue("send_email", {
  args: ["user@example.com"],
  kwargs: { template: "welcome" },
  queueName: "rrq:queue:default",
  maxRetries: 5,
});

console.log("enqueued", jobId);
await client.close();
```

### Producer options

```ts
interface EnqueueOptions {
  args?: unknown[];
  kwargs?: Record<string, unknown>;
  queueName?: string;
  jobId?: string;
  uniqueKey?: string;
  maxRetries?: number;
  jobTimeoutSeconds?: number;
  resultTtlSeconds?: number;
  deferUntil?: Date;
  deferBySeconds?: number;
  traceContext?: Record<string, string> | null;
}
```

## Executor runtime usage

```ts
import { ExecutorRuntime, Registry } from "rrq-ts";

const registry = new Registry();
registry.register("send_email", async (request) => {
  // handle request.args / request.kwargs
  return {
    job_id: request.job_id,
    request_id: request.request_id,
    status: "success",
    result: { ok: true },
  };
});

const runtime = new ExecutorRuntime(registry);
await runtime.runFromEnv();
```

The orchestrator sets `RRQ_EXECUTOR_SOCKET` or `RRQ_EXECUTOR_TCP_SOCKET` when
launching executors. You can also run the runtime directly:

```bash
RRQ_EXECUTOR_SOCKET=/tmp/rrq-executor.sock node dist/executor_runtime.js
```

## Build

```bash
npm run build
```

## Notes

- Producer writes job hashes and queue entries compatible with RRQ's Python/Rust
  orchestrator.
- The executor implements the RRQ v1 socket protocol.

## License

MIT
