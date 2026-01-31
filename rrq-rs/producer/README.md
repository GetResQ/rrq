# rrq-producer

[![Crates.io](https://img.shields.io/crates/v/rrq-producer.svg)](https://crates.io/crates/rrq-producer)
[![Documentation](https://docs.rs/rrq-producer/badge.svg)](https://docs.rs/rrq-producer)
[![License](https://img.shields.io/crates/l/rrq-producer.svg)](LICENSE)

A production-ready Rust client for enqueuing jobs into [RRQ](https://crates.io/crates/rrq) (Redis Reliable Queue).

## Features

- **Auto-reconnecting connections** via Redis ConnectionManager
- **Atomic job enqueue** operations with Redis pipelines
- **Job result polling** with configurable timeout
- **Trace context propagation** for distributed tracing (OpenTelemetry, Datadog)
- **Trait-based design** for easy mocking in tests
- **TLS support** for secure Redis connections

## Installation

```toml
[dependencies]
rrq-producer = "0.9"
```

## Quick Start

```rust
use rrq_producer::{Producer, EnqueueOptions};
use serde_json::{json, Map};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Connect to Redis
    let producer = Producer::new("redis://localhost:6379/0").await?;

    // Enqueue a job
    let job_id = producer.enqueue(
        "send_email",                           // handler function name
        vec![json!("user@example.com")],        // positional args
        Map::new(),                             // keyword args
        EnqueueOptions::default(),
    ).await?;

    println!("Enqueued job: {}", job_id);
    Ok(())
}
```

## Enqueue Options

Customize job behavior with `EnqueueOptions`:

```rust
use rrq_producer::EnqueueOptions;
use chrono::{Utc, Duration};
use std::collections::HashMap;

let options = EnqueueOptions {
    // Target a specific queue (default: "default")
    queue_name: Some("high-priority".to_string()),

    // Custom job ID (default: auto-generated UUID)
    job_id: Some("order-123-confirmation".to_string()),

    // Max retry attempts on failure (default: 3)
    max_retries: Some(5),

    // Job execution timeout in seconds (default: 300)
    job_timeout_seconds: Some(600),

    // How long to keep job results in Redis (default: 3600)
    result_ttl_seconds: Some(86400),

    // Schedule job for future execution
    scheduled_time: Some(Utc::now() + Duration::hours(1)),

    // Trace context for distributed tracing
    trace_context: Some(HashMap::from([
        ("traceparent".to_string(), "00-abc123-def456-01".to_string()),
    ])),

    ..Default::default()
};
```

## Waiting for Job Results

Poll for job completion with timeout:

```rust
use rrq_producer::{Producer, JobStatus};
use std::time::Duration;

let producer = Producer::new("redis://localhost:6379/0").await?;

// Enqueue and wait for result
let job_id = producer.enqueue("process_data", args, kwargs, options).await?;

let result = producer.wait_for_result(
    &job_id,
    Duration::from_secs(30),      // timeout
    Duration::from_millis(100),   // poll interval
).await?;

match result.status {
    JobStatus::Completed => {
        println!("Success: {:?}", result.result);
    }
    JobStatus::Failed => {
        println!("Failed: {:?}", result.last_error);
    }
    _ => unreachable!(),
}
```

## Check Job Status (Non-blocking)

```rust
if let Some(result) = producer.get_job_status(&job_id).await? {
    match result.status {
        JobStatus::Pending => println!("Job is waiting in queue"),
        JobStatus::Active => println!("Job is currently running"),
        JobStatus::Completed => println!("Job finished: {:?}", result.result),
        JobStatus::Failed => println!("Job failed: {:?}", result.last_error),
        JobStatus::Retrying => println!("Job is being retried"),
        JobStatus::Unknown => println!("Unknown status"),
    }
} else {
    println!("Job not found");
}
```

## Custom Configuration

```rust
use rrq_producer::{Producer, ProducerConfig};

let config = ProducerConfig {
    queue_name: "default".to_string(),
    max_retries: 3,
    job_timeout_seconds: 300,
    result_ttl_seconds: 3600,
};

let producer = Producer::with_config("redis://localhost:6379/0", config).await?;
```

## Testing with Mocks

The `ProducerHandle` trait enables easy mocking:

```rust
use rrq_producer::{ProducerHandle, EnqueueOptions, JobResult};
use async_trait::async_trait;

struct MockProducer;

#[async_trait]
impl ProducerHandle for MockProducer {
    async fn enqueue(
        &self,
        function_name: &str,
        args: Vec<serde_json::Value>,
        kwargs: serde_json::Map<String, serde_json::Value>,
        options: EnqueueOptions,
    ) -> anyhow::Result<String> {
        Ok("mock-job-id".to_string())
    }

    async fn wait_for_result(
        &self,
        job_id: &str,
        timeout: std::time::Duration,
        poll_interval: std::time::Duration,
    ) -> anyhow::Result<JobResult> {
        Ok(JobResult {
            status: rrq_producer::JobStatus::Completed,
            result: Some(serde_json::json!({"mock": true})),
            last_error: None,
        })
    }
}
```

## Distributed Tracing

Pass trace context to propagate spans across services:

```rust
use std::collections::HashMap;
use rrq_producer::EnqueueOptions;

// Build trace context from your tracing library
let mut trace_context = HashMap::new();
trace_context.insert("traceparent".to_string(), "00-abc-def-01".to_string());
trace_context.insert("tracestate".to_string(), "vendor=value".to_string());

// For Datadog
trace_context.insert("x-datadog-trace-id".to_string(), "123456".to_string());
trace_context.insert("x-datadog-parent-id".to_string(), "789".to_string());

let options = EnqueueOptions {
    trace_context: Some(trace_context),
    ..Default::default()
};
```

## TLS Connections

Connect to Redis with TLS (e.g., AWS ElastiCache):

```rust
// TLS is automatically enabled for rediss:// URLs
let producer = Producer::new("rediss://my-cluster.cache.amazonaws.com:6379").await?;
```

## Related Crates

| Crate | Description |
|-------|-------------|
| [`rrq`](https://crates.io/crates/rrq) | Job queue orchestrator (worker) |
| [`rrq-runner`](https://crates.io/crates/rrq-runner) | Runner runtime for job handlers |
| [`rrq-protocol`](https://crates.io/crates/rrq-protocol) | Protocol definitions |

## License

Apache-2.0
