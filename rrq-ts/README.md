# RRQ TypeScript (Producer + Executor)

TypeScript/JavaScript package for RRQ (Reliable Redis Queue). Includes:

- **Producer** client for enqueuing jobs into RRQ.
- **Executor runtime** to run job handlers over the RRQ socket protocol.

Designed to work with **Node.js (20+)** and **Bun**. The producer FFI uses
`koffi` on Node and `bun:ffi` when running under Bun.

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

### Producer requirements

The TypeScript producer uses the Rust `rrq_producer` shared library via FFI.
Provide the library in one of these ways:

- Set `RRQ_PRODUCER_LIB_PATH` to the absolute path of the shared library.
- Place the library in `rrq-ts/bin` (e.g. `librrq_producer.dylib`, `librrq_producer.so`,
  or `rrq_producer.dll`).
- For published packages, place the library in `rrq-ts/bin/<platform>-<arch>/`
  (for example, `bin/linux-x64/librrq_producer.so` or
  `bin/darwin-arm64/librrq_producer.dylib`).

To build the library from this repo:

```bash
cargo build -p rrq-producer --release
```

### Producer options

```ts
interface EnqueueOptions {
  args?: unknown[];
  kwargs?: Record<string, unknown>;
  queueName?: string;
  jobId?: string;
  uniqueKey?: string;
  uniqueTtlSeconds?: number;
  maxRetries?: number;
  jobTimeoutSeconds?: number;
  resultTtlSeconds?: number;
  deferUntil?: Date;
  deferBySeconds?: number;
  traceContext?: Record<string, string> | null;
}
```

`uniqueKey` is idempotency: repeated enqueues with the same key return the same
job id.

### Rate limit / debounce

```ts
const jobId = await client.enqueueWithRateLimit("send_email", {
  rateLimitKey: "user-123",
  rateLimitSeconds: 5,
});
if (jobId === null) {
  // rate limited
}

const debouncedId = await client.enqueueWithDebounce("sync_user", {
  debounceKey: "user-123",
  debounceSeconds: 5,
});
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

The orchestrator sets `RRQ_EXECUTOR_TCP_SOCKET` when launching executors. You
can also run the runtime directly:

```bash
RRQ_EXECUTOR_TCP_SOCKET=127.0.0.1:9000 node dist/executor_runtime.js
```

## Build

```bash
bun run build
```

## Tests

```bash
bun test
```

If you need the Rust FFI library built automatically:

```bash
sh ../scripts/with-producer-lib.sh -- sh -c "cd rrq-ts && bun test"
```

## Notes

- Producer uses the Rust FFI library for enqueue semantics consistent with RRQ's
  Rust/Python producers.
- The executor implements the RRQ v1 socket protocol.

## License

MIT
