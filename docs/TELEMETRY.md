# RRQ Telemetry (OpenTelemetry)

RRQ supports end-to-end tracing by carrying OpenTelemetry propagation headers
through the job payload and emitting a runner span when the job executes.

## Trace Flow

1) **Producer** injects the active trace into `trace_context` on enqueue.
2) **Orchestrator** stores `trace_context` with the job in Redis.
3) **Runner** receives `trace_context` in `ExecutionRequest` and uses it as the
   parent context for the `rrq.runner` span.

## Producer: inject trace_context

RRQ does not auto-inject tracing headers in producers. You must explicitly pass
`trace_context` when you enqueue.

### Python

```python
from opentelemetry import propagate
from rrq import RRQClient

carrier: dict[str, str] = {}
propagate.inject(carrier)

client = RRQClient(config_path="rrq.toml")
await client.enqueue("process_message", {
    "args": ["hello"],
    "trace_context": carrier,
})
```

### TypeScript

```ts
import { context, propagation } from "@opentelemetry/api";
import { RRQClient } from "rrq-ts";

const carrier: Record<string, string> = {};
propagation.inject(context.active(), carrier);

const client = new RRQClient({ configPath: "rrq.toml" });
await client.enqueue("process_message", {
  args: ["hello"],
  traceContext: carrier,
});
```

### Rust

```rust
use std::collections::HashMap;
use opentelemetry::global;
use rrq_producer::{Producer, EnqueueOptions};

let mut carrier = HashMap::new();
global::get_text_map_propagator(|prop| prop.inject(&mut carrier));

let mut options = EnqueueOptions::default();
options.trace_context = Some(carrier);
producer.enqueue("process_message", vec![], Default::default(), options).await?;
```

## Runner: enable OpenTelemetry spans

All runner runtimes emit `rrq.runner` spans when OpenTelemetry is enabled. The
span attributes include:
- `rrq.job_id`, `rrq.function`, `rrq.queue`, `rrq.attempt`, `rrq.worker_id`
- `rrq.outcome`, `rrq.duration_ms`, `rrq.retry_delay_ms`
- `rrq.error_message`, `rrq.error_type`
- standard messaging attributes (`messaging.system`, `messaging.operation`, …)

### Python runner

```python
from rrq.integrations import otel

otel.enable(service_name="my-runner")
```

### TypeScript runner

```ts
import { RunnerRuntime, Registry } from "rrq-ts";
import { OtelTelemetry } from "rrq-ts";

const registry = new Registry();
const runtime = new RunnerRuntime(registry, new OtelTelemetry());
await runtime.runFromEnv();
```

### Rust runner

Enable the `otel` feature and initialize tracing:

```rust
rrq_runner::telemetry::otel::init_tracing("my-runner")?;
```

## Datadog / Sentry

Both Datadog and Sentry support OpenTelemetry. Configure the appropriate OTel
exporter and propagation in your application, then:
- inject `trace_context` on enqueue (producer)
- enable runner telemetry (runner)

Once wired, the `rrq.runner` span will show up as a child of the producer span,
providing end-to-end visibility from enqueue → execution.
