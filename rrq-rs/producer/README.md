# rrq-producer

[![Crates.io](https://img.shields.io/crates/v/rrq-producer.svg)](https://crates.io/crates/rrq-producer)
[![Documentation](https://docs.rs/rrq-producer/badge.svg)](https://docs.rs/rrq-producer)
[![License](https://img.shields.io/crates/l/rrq-producer.svg)](LICENSE)

**Rust client for enqueuing jobs into RRQ**, the distributed job queue with a Rust orchestrator.

## What is RRQ?

RRQ (Reliable Redis Queue) is a distributed job queue that separates the complex scheduling logic (retries, timeouts, locking) into a Rust orchestrator while letting you write job handlers in Python, TypeScript, or Rust. This crate lets you enqueue jobs from Rust applications.

## Installation

```toml
[dependencies]
rrq-producer = "0.9"
```

## Quick Start

```rust
use rrq_producer::Producer;
use serde_json::json;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let producer = Producer::new("redis://localhost:6379/0").await?;

    let job_id = producer.enqueue(
        "send_email",
        json!({
            "to": "user@example.com",
            "template": "welcome"
        }),
    ).await?;

    println!("Enqueued: {}", job_id);
    Ok(())
}
```

## Features

- **Auto-reconnecting** - Redis connections recover automatically
- **Atomic operations** - Jobs enqueue reliably with Redis pipelines
- **Job status polling** - Check job progress and results
- **Distributed tracing** - OpenTelemetry and Datadog context propagation
- **TLS support** - Secure Redis connections (`rediss://`)
- **Trait-based design** - Easy mocking for tests

## Enqueue Options

```rust
use rrq_producer::{Producer, EnqueueOptions};
use chrono::{Utc, Duration};

let options = EnqueueOptions {
    queue_name: Some("high-priority".to_string()),
    job_id: Some("order-123".to_string()),
    max_retries: Some(5),
    job_timeout_seconds: Some(600),
    result_ttl_seconds: Some(86400),
    scheduled_time: Some(Utc::now() + Duration::hours(1)),
    ..Default::default()
};

let job_id = producer.enqueue_with_options("process_order", json!({}), options).await?;
```

## Check Job Status

```rust
if let Some(result) = producer.get_job_status(&job_id).await? {
    match result.status {
        JobStatus::Pending => println!("Waiting in queue"),
        JobStatus::Active => println!("Running"),
        JobStatus::Completed => println!("Done: {:?}", result.result),
        JobStatus::Failed => println!("Failed: {:?}", result.last_error),
        _ => {}
    }
}
```

## Distributed Tracing

```rust
use std::collections::HashMap;

let mut trace = HashMap::new();
trace.insert("traceparent".to_string(), "00-abc-def-01".to_string());

let options = EnqueueOptions {
    trace_context: Some(trace),
    ..Default::default()
};
```

## TLS Connections

```rust
let producer = Producer::new("rediss://my-cluster.cache.amazonaws.com:6379").await?;
```

## Testing with Mocks

```rust
use rrq_producer::ProducerHandle;

struct MockProducer;

#[async_trait]
impl ProducerHandle for MockProducer {
    async fn enqueue(&self, function_name: &str, params: Value, options: EnqueueOptions) -> anyhow::Result<String> {
        Ok("mock-job-id".to_string())
    }
}
```

## Related Crates

| Crate | Purpose |
|-------|---------|
| [`rrq`](https://crates.io/crates/rrq) | Orchestrator (runs workers) |
| [`rrq-runner`](https://crates.io/crates/rrq-runner) | Build Rust job handlers |
| [`rrq-protocol`](https://crates.io/crates/rrq-protocol) | Wire protocol types |

## License

Apache-2.0
