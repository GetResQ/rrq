use anyhow::Result;
use rrq_producer::{EnqueueOptions, Producer};
use serde_json::{Map, Value, json};

#[tokio::main]
async fn main() -> Result<()> {
    let redis_dsn =
        std::env::var("RRQ_REDIS_DSN").unwrap_or_else(|_| "redis://localhost:6379/3".to_string());
    let queue_name = std::env::var("RRQ_QUEUE").unwrap_or_else(|_| "default".to_string());
    let function_name = std::env::var("RRQ_FUNCTION").unwrap_or_else(|_| "quick_task".to_string());
    let count: usize = std::env::var("RRQ_COUNT")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(5);

    let mut producer = Producer::new(redis_dsn).await?;

    for i in 0..count {
        let args = vec![json!(format!("from-rust-{i}"))];
        let mut kwargs: Map<String, Value> = Map::new();
        kwargs.insert("source".to_string(), json!("rust"));

        producer
            .enqueue(
                &function_name,
                args,
                kwargs,
                EnqueueOptions {
                    queue_name: Some(queue_name.clone()),
                    ..Default::default()
                },
            )
            .await?;
    }

    println!("Enqueued {count} jobs into {queue_name}");
    Ok(())
}
