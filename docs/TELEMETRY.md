# RRQ Telemetry (OpenTelemetry)

RRQ supports end-to-end tracing by carrying OpenTelemetry propagation headers
through the job payload and emitting a runner span when the job executes.

Orchestrator spans include `rrq.job` plus internal lifecycle spans such as
`rrq.poll_cycle`, `rrq.fetch_queue`, and `rrq.dispatch`.

## Trace Flow

1) **Producer** injects the active trace into `trace_context` on enqueue.
2) **Orchestrator** stores `trace_context` with the job in Redis.
3) **Runner** receives `trace_context` in `ExecutionRequest` and uses it as the
   parent context for the `rrq.runner` span.

## Producer: inject trace_context

RRQ does not auto-inject tracing headers in producers. You must explicitly pass
`trace_context` when you enqueue.

Producer libraries do not configure OTLP exporters for you. They rely on the
OpenTelemetry SDK/exporter already configured in your process.

### Python

```python
from opentelemetry import propagate
from rrq import RRQClient

carrier: dict[str, str] = {}
propagate.inject(carrier)

client = RRQClient(config_path="rrq.toml")
await client.enqueue(
    "process_message",
    {"params": {"message": "hello"}, "trace_context": carrier},
)
```

### TypeScript

```ts
import { context, propagation } from "@opentelemetry/api";
import { RRQClient } from "rrq-ts";

const carrier: Record<string, string> = {};
propagation.inject(context.active(), carrier);

const client = new RRQClient({ configPath: "rrq.toml" });
await client.enqueue("process_message", {
  params: { message: "hello" },
  traceContext: carrier,
});
```

### Rust

```rust
use std::collections::HashMap;
use opentelemetry::global;
use rrq_producer::{Producer, EnqueueOptions};
use serde_json::{json, Map, Value};

let mut carrier = HashMap::new();
global::get_text_map_propagator(|prop| prop.inject(&mut carrier));

let mut options = EnqueueOptions::default();
options.trace_context = Some(carrier);
let mut params: Map<String, Value> = Map::new();
params.insert("message".to_string(), json!("hello"));
producer.enqueue("process_message", params, options).await?;
```

## Runner: enable OpenTelemetry spans

All runner runtimes emit `rrq.runner` spans when OpenTelemetry is enabled. The
span attributes include:
- `rrq.job_id`, `rrq.function`, `rrq.queue`, `rrq.attempt`, `rrq.worker_id`
- `rrq.outcome`, `rrq.duration_ms`, `rrq.retry_delay_ms`
- `rrq.error_message`, `rrq.error_type`
- optional timing attributes: `rrq.queue_wait_ms`, `rrq.deadline_remaining_ms`
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
// RRQ launches runners with: --tcp-socket host:port
await runtime.runFromArgs();
```

### Rust runner

Enable the `otel` feature and initialize tracing:

```rust
rrq_runner::telemetry::otel::init_tracing("my-runner")?;
```

### OTLP endpoint precedence (RRQ runtime exporters)

For RRQ components that initialize OTLP exporters (orchestrator and Rust
runner), endpoint resolution uses this precedence per signal
(traces/metrics/logs):
- `OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT` (signal-specific)
- `OTEL_EXPORTER_OTLP_ENDPOINT` (global fallback)
- disabled (if neither is set)

Special case:
- If `OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT` is explicitly set to an empty value, that signal is disabled and does not fall back to `OTEL_EXPORTER_OTLP_ENDPOINT`.

## Correlation Context

RRQ can automatically extract values from job parameters and attach them as
span attributes on telemetry spans. This lets you filter and group traces by
application-specific dimensions (for example, tenant ID or user ID) without
manually injecting them into `trace_context`.

### Configuration

Add a `[rrq.correlation_mappings]` table to `rrq.toml`:

```toml
[rrq.correlation_mappings]
tenant_id = "params.tenant.id"
user_id   = "user_id"
```

Each key is the span attribute name; the value is a dot-separated path into the
job's `params` object. The `params.` prefix is optional — `"tenant.id"` and
`"params.tenant.id"` are equivalent.

Correlation context is extracted at enqueue time (producer) and at dispatch time
(orchestrator), then applied as attributes on the `rrq.job` and `rrq.runner`
spans.

### Precedence

If a key already exists in the job's `trace_context`, that value takes
precedence over the params lookup. This lets you override correlation values
from the calling service when needed.

### Limits

- Max **16** correlation keys per job.
- Key names: max **64** characters.
- Values: truncated to **256** characters.
- Only scalar values (strings, numbers, booleans) are extracted; objects and
  arrays are skipped.

## Metrics (OTel)

When OTLP metrics export is enabled, RRQ emits counters/histograms for queue
and runner health, including:
- `rrq_jobs_processed_total{queue,runner,outcome}`
- `rrq_job_duration_ms` histogram
- `rrq_queue_wait_ms` histogram
- `rrq_poll_cycles_total{result}`
- `rrq_jobs_fetched_total{queue}`
- `rrq_lock_acquire_total{queue,result}`
- `rrq_runner_inflight{runner}`
- `rrq_runner_channel_pressure` histogram
- `rrq_deadline_expired_total{runner}`
- `rrq_cancellations_total{runner,scope}`

## Datadog / Sentry

Both Datadog and Sentry support OpenTelemetry. Configure the appropriate OTel
exporter and propagation in your application, then:
- inject `trace_context` on enqueue (producer)
- enable runner telemetry (runner)

Once wired, the `rrq.runner` span will show up as a child of the producer span,
providing end-to-end visibility from enqueue → execution.
