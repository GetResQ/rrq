# RRQ TypeScript (Producer + Runner)

TypeScript/JavaScript package for RRQ (Reliable Redis Queue). Includes:

- **Producer** client for enqueuing jobs into RRQ.
- **Runner runtime** to run job handlers over the RRQ socket protocol.

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
  config: {
    redisDsn: "redis://localhost:6379/0",
  },
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

### Producer config from rrq.toml

```ts
const client = new RRQClient({ configPath: "rrq.toml" });
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

### Job status + results

```ts
const status = await client.getJobStatus(jobId);
```

## Runner runtime usage

```ts
import { RunnerRuntime, Registry } from "rrq-ts";

const registry = new Registry();
registry.register("send_email", async (request, signal) => {
  // handle request.args / request.kwargs; use signal for cancellation
  return {
    job_id: request.job_id,
    request_id: request.request_id,
    status: "success",
    result: { ok: true },
  };
});

const runtime = new RunnerRuntime(registry);
await runtime.runFromEnv();
```

The orchestrator sets `RRQ_RUNNER_TCP_SOCKET` when launching runners. You
can also run the runtime directly:

```bash
RRQ_RUNNER_TCP_SOCKET=127.0.0.1:9000 node dist/runner_runtime.js
```

### OpenTelemetry (runner)

```ts
import { RunnerRuntime, Registry } from "rrq-ts";
import { OtelTelemetry } from "rrq-ts";

const registry = new Registry();
const runtime = new RunnerRuntime(registry, new OtelTelemetry());
await runtime.runFromEnv();
```

See `docs/TELEMETRY.md` for end-to-end producer → runner tracing.

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
- The runner implements the RRQ v1 socket protocol.

## License

MIT
