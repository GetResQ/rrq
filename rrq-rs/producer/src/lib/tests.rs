use super::*;
use serde_json::json;
use std::sync::OnceLock;
use tokio::sync::Mutex;

static REDIS_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn redis_lock() -> &'static Mutex<()> {
    REDIS_LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn format_queue_key_adds_prefix() {
    assert_eq!(format_queue_key("default"), "rrq:queue:default");
}

#[test]
fn format_queue_key_preserves_prefix() {
    assert_eq!(format_queue_key("rrq:queue:custom"), "rrq:queue:custom");
}

#[test]
fn job_status_from_str() {
    assert_eq!(JobStatus::from_str("PENDING"), JobStatus::Pending);
    assert_eq!(JobStatus::from_str("ACTIVE"), JobStatus::Active);
    assert_eq!(JobStatus::from_str("COMPLETED"), JobStatus::Completed);
    assert_eq!(JobStatus::from_str("FAILED"), JobStatus::Failed);
    assert_eq!(JobStatus::from_str("RETRYING"), JobStatus::Retrying);
    assert_eq!(JobStatus::from_str("UNKNOWN"), JobStatus::Unknown);
    assert_eq!(JobStatus::from_str("garbage"), JobStatus::Unknown);
}

#[test]
fn extract_latest_stream_entry_id_returns_last_entry_id() {
    let response = redis::Value::Array(vec![redis::Value::Array(vec![
        redis::Value::SimpleString("rrq:events:job:abc".to_string()),
        redis::Value::Array(vec![
            redis::Value::Array(vec![
                redis::Value::SimpleString("1-0".to_string()),
                redis::Value::Array(vec![
                    redis::Value::SimpleString("event".to_string()),
                    redis::Value::SimpleString("started".to_string()),
                ]),
            ]),
            redis::Value::Array(vec![
                redis::Value::SimpleString("2-0".to_string()),
                redis::Value::Array(vec![
                    redis::Value::SimpleString("event".to_string()),
                    redis::Value::SimpleString("completed".to_string()),
                ]),
            ]),
        ]),
    ])]);

    assert_eq!(
        extract_latest_stream_entry_id(&response),
        Some("2-0".to_string())
    );
}

#[test]
fn extract_latest_stream_entry_id_returns_none_for_empty_stream_entries() {
    let response = redis::Value::Array(vec![redis::Value::Array(vec![
        redis::Value::SimpleString("rrq:events:job:abc".to_string()),
        redis::Value::Array(Vec::new()),
    ])]);

    assert_eq!(extract_latest_stream_entry_id(&response), None);
}

#[test]
fn merge_trace_context_preserves_existing_entries() {
    let mut existing = HashMap::new();
    existing.insert("message_id".to_string(), "m-1".to_string());

    let merged = merge_trace_context(Some(existing)).expect("expected trace context");

    assert_eq!(merged.get("message_id").map(String::as_str), Some("m-1"));
}

#[test]
fn hash_map_injector_preserves_existing_trace_headers() {
    let mut map = HashMap::from([(
        "traceparent".to_string(),
        "00-upstream-trace-upstream-span-01".to_string(),
    )]);
    {
        let mut injector = HashMapInjector(&mut map);
        injector.set("traceparent", "00-local-trace-local-span-01".to_string());
        injector.set("tracestate", "vendor=value".to_string());
    }

    assert_eq!(
        map.get("traceparent").map(String::as_str),
        Some("00-upstream-trace-upstream-span-01")
    );
    assert_eq!(
        map.get("tracestate").map(String::as_str),
        Some("vendor=value")
    );
}

#[test]
fn extract_correlation_context_maps_paths_and_scalars() {
    let params = json!({
        "session": { "id": "sess-abc" },
        "message_id": 88,
        "retry": false
    })
    .as_object()
    .expect("object params")
    .clone();
    let mappings = HashMap::from([
        ("session_id".to_string(), "session.id".to_string()),
        ("message_id".to_string(), "params.message_id".to_string()),
        ("retry".to_string(), "retry".to_string()),
    ]);

    let extracted =
        extract_correlation_context(&params, &mappings, None).expect("expected correlation");

    assert_eq!(
        extracted.get("session_id").map(String::as_str),
        Some("sess-abc")
    );
    assert_eq!(extracted.get("message_id").map(String::as_str), Some("88"));
    assert_eq!(extracted.get("retry").map(String::as_str), Some("false"));
}

#[test]
fn extract_correlation_context_strips_only_one_params_prefix() {
    let params = json!({
        "params": { "id": "nested-id" },
        "id": "top-level-id"
    })
    .as_object()
    .expect("object params")
    .clone();
    let mappings = HashMap::from([("correlation_id".to_string(), "params.params.id".to_string())]);

    let extracted =
        extract_correlation_context(&params, &mappings, None).expect("expected correlation");

    assert_eq!(
        extracted.get("correlation_id").map(String::as_str),
        Some("nested-id")
    );
}

#[test]
fn extract_correlation_context_prefers_trace_context_value() {
    let params = json!({
        "session": { "id": "sess-from-params" }
    })
    .as_object()
    .expect("object params")
    .clone();
    let mappings = HashMap::from([("session_id".to_string(), "session.id".to_string())]);
    let trace_context = HashMap::from([("session_id".to_string(), "sess-from-trace".to_string())]);

    let extracted = extract_correlation_context(&params, &mappings, Some(&trace_context))
        .expect("expected correlation");

    assert_eq!(
        extracted.get("session_id").map(String::as_str),
        Some("sess-from-trace")
    );
}

#[tokio::test]
async fn producer_enqueue_writes_job_and_queue() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let producer = Producer::new(&dsn).await.unwrap();
    let job_id = producer
        .enqueue("work", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
    let status: String = conn.hget(&job_key, "status").await.unwrap();
    assert_eq!(status, "PENDING");
    let queue_name: String = conn.hget(&job_key, "queue_name").await.unwrap();
    assert_eq!(queue_name, format_queue_key("default"));
    let queue_key = format_queue_key("default");
    let score: Option<f64> = conn.zscore(queue_key, &job_id).await.unwrap();
    assert!(score.is_some());
}

#[tokio::test]
async fn producer_with_config() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let config = ProducerConfig {
        queue_name: "custom-queue".to_string(),
        max_retries: 5,
        job_timeout_seconds: 600,
        result_ttl_seconds: 7200,
        idempotency_ttl_seconds: 1200,
        correlation_mappings: HashMap::new(),
    };
    let producer = Producer::with_config(&dsn, config).await.unwrap();
    let job_id = producer
        .enqueue("work", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
    let max_retries: i64 = conn.hget(&job_key, "max_retries").await.unwrap();
    assert_eq!(max_retries, 5);
    let queue_name: String = conn.hget(&job_key, "queue_name").await.unwrap();
    assert_eq!(queue_name, format_queue_key("custom-queue"));
}

#[tokio::test]
async fn producer_enqueue_with_custom_queue_option_normalizes_name() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let producer = Producer::new(&dsn).await.unwrap();
    let options = EnqueueOptions {
        queue_name: Some("custom-queue".to_string()),
        ..Default::default()
    };
    let job_id = producer
        .enqueue("work", serde_json::Map::new(), options)
        .await
        .unwrap();

    let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
    let queue_name: String = conn.hget(&job_key, "queue_name").await.unwrap();
    assert_eq!(queue_name, format_queue_key("custom-queue"));
    let score: Option<f64> = conn
        .zscore(format_queue_key("custom-queue"), &job_id)
        .await
        .unwrap();
    assert!(score.is_some());
}

#[tokio::test]
async fn producer_rejects_duplicate_job_id() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let producer = Producer::new(&dsn).await.unwrap();
    let options = EnqueueOptions {
        job_id: Some("fixed-id".to_string()),
        ..Default::default()
    };
    let first = producer
        .enqueue("work", serde_json::Map::new(), options.clone())
        .await
        .unwrap();
    assert_eq!(first, "fixed-id");

    let err = producer
        .enqueue("work", serde_json::Map::new(), options)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("job_id already exists"));
}

#[tokio::test]
async fn producer_idempotency_key_reuses_job() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let producer = Producer::new(&dsn).await.unwrap();
    let options = EnqueueOptions {
        idempotency_key: Some("dedupe".to_string()),
        ..Default::default()
    };
    let first = producer
        .enqueue("work", serde_json::Map::new(), options.clone())
        .await
        .unwrap();
    let second = producer
        .enqueue("work", serde_json::Map::new(), options)
        .await
        .unwrap();
    assert_eq!(first, second);
    let queue_key = format_queue_key("default");
    let count: i64 = conn.zcard(queue_key).await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn producer_extends_idempotency_ttl_for_deferrals() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let config = ProducerConfig {
        queue_name: "rrq:queue:default".to_string(),
        max_retries: 5,
        job_timeout_seconds: 300,
        result_ttl_seconds: 3600 * 24,
        idempotency_ttl_seconds: 2,
        correlation_mappings: HashMap::new(),
    };
    let producer = Producer::with_config(&dsn, config).await.unwrap();
    let enqueue_time = Utc::now();
    let scheduled_time = enqueue_time + chrono::Duration::seconds(5);
    let options = EnqueueOptions {
        idempotency_key: Some("defer-ttl".to_string()),
        enqueue_time: Some(enqueue_time),
        scheduled_time: Some(scheduled_time),
        ..Default::default()
    };
    producer
        .enqueue("work", serde_json::Map::new(), options)
        .await
        .unwrap();

    let ttl: i64 = conn.ttl(format_idempotency_key("defer-ttl")).await.unwrap();
    assert!(ttl >= 4);
}

#[tokio::test]
async fn producer_rate_limit_returns_none_when_limited() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let producer = Producer::new(&dsn).await.unwrap();
    let first = producer
        .enqueue_with_rate_limit(
            "work",
            serde_json::Map::new(),
            "rate-key",
            Duration::from_secs(5),
            EnqueueOptions::default(),
        )
        .await
        .unwrap();
    assert!(first.is_some());

    let second = producer
        .enqueue_with_rate_limit(
            "work",
            serde_json::Map::new(),
            "rate-key",
            Duration::from_secs(5),
            EnqueueOptions::default(),
        )
        .await
        .unwrap();
    assert!(second.is_none());
}

#[tokio::test]
async fn producer_debounce_reuses_pending_job() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let producer = Producer::new(&dsn).await.unwrap();
    let first = producer
        .enqueue_with_debounce(
            "work",
            serde_json::Map::new(),
            "debounce-key",
            Duration::from_secs(5),
            EnqueueOptions::default(),
        )
        .await
        .unwrap();
    let second = producer
        .enqueue_with_debounce(
            "work",
            serde_json::Map::new(),
            "debounce-key",
            Duration::from_secs(5),
            EnqueueOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(first, second);

    let queue_key = format_queue_key("default");
    let count: i64 = conn.zcard(queue_key).await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn producer_debounce_clears_stale_correlation_context_on_update() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let producer = Producer::with_config(
        &dsn,
        ProducerConfig {
            correlation_mappings: HashMap::from([(
                "session_id".to_string(),
                "session.id".to_string(),
            )]),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let first_params = json!({
        "session": { "id": "sess-1" }
    })
    .as_object()
    .expect("params object")
    .clone();

    let job_id = producer
        .enqueue_with_debounce(
            "work",
            first_params,
            "debounce-key",
            Duration::from_secs(5),
            EnqueueOptions::default(),
        )
        .await
        .unwrap();

    let second_id = producer
        .enqueue_with_debounce(
            "work",
            serde_json::Map::new(),
            "debounce-key",
            Duration::from_secs(5),
            EnqueueOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(job_id, second_id);

    let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
    let correlation_context: Option<String> =
        conn.hget(&job_key, "correlation_context").await.unwrap();
    assert!(
        correlation_context.is_none(),
        "correlation_context should be removed when debounce update has no mapped values"
    );
}

#[tokio::test]
async fn producer_idempotency_key_replaces_stale_entry() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let idem_key = format_idempotency_key("stale");
    let _: () = redis::cmd("SET")
        .arg(&idem_key)
        .arg("missing-job")
        .query_async(&mut conn)
        .await
        .unwrap();

    let producer = Producer::new(&dsn).await.unwrap();
    let options = EnqueueOptions {
        job_id: Some("fresh-id".to_string()),
        idempotency_key: Some("stale".to_string()),
        ..Default::default()
    };
    let job_id = producer
        .enqueue("work", serde_json::Map::new(), options)
        .await
        .unwrap();
    assert_eq!(job_id, "fresh-id");

    let stored: Option<String> = conn.get(&idem_key).await.unwrap();
    assert_eq!(stored.as_deref(), Some("fresh-id"));
    let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
    let exists: bool = conn.exists(job_key).await.unwrap();
    assert!(exists);
}

#[tokio::test]
async fn producer_rejects_empty_names() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let producer = Producer::new(&dsn).await.unwrap();
    let options = EnqueueOptions {
        queue_name: Some(" ".to_string()),
        ..Default::default()
    };
    let err = producer
        .enqueue("", serde_json::Map::new(), options)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("function_name cannot be empty"));
}

#[tokio::test]
async fn producer_rejects_non_positive_job_timeout() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let producer = Producer::new(&dsn).await.unwrap();
    let options = EnqueueOptions {
        job_timeout_seconds: Some(0),
        ..Default::default()
    };
    let err = producer
        .enqueue("work", serde_json::Map::new(), options)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("job_timeout_seconds must be positive")
    );
}

#[tokio::test]
async fn producer_get_job_status() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let producer = Producer::new(&dsn).await.unwrap();
    let job_id = producer
        .enqueue("work", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let result = producer.get_job_status(&job_id).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().status, JobStatus::Pending);

    // Non-existent job
    let result = producer.get_job_status("non-existent").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn producer_wait_for_completion_returns_completed_result() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let producer = Producer::new(&dsn).await.unwrap();
    let job_id = producer
        .enqueue("work", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let dsn_clone = dsn.clone();
    let job_id_clone = job_id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let client = redis::Client::open(dsn_clone.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let job_key = format!("{JOB_KEY_PREFIX}{job_id_clone}");
        let events_key = format_job_events_key(&job_id_clone);
        let result = json!({"ok": true}).to_string();
        let _: () = redis::pipe()
            .atomic()
            .hset(&job_key, "status", "COMPLETED")
            .hset(&job_key, "result", result)
            .cmd("XADD")
            .arg(&events_key)
            .arg("*")
            .arg("event")
            .arg("completed")
            .query_async(&mut conn)
            .await
            .unwrap();
    });

    let status = producer
        .wait_for_completion(&job_id, Duration::from_secs(2), Duration::from_millis(200))
        .await
        .unwrap()
        .expect("job should complete");
    assert_eq!(status.status, JobStatus::Completed);
    assert_eq!(status.result, Some(json!({"ok": true})));
}

#[tokio::test]
async fn producer_wait_for_completion_returns_failed_result() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let producer = Producer::new(&dsn).await.unwrap();
    let job_id = producer
        .enqueue("work", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let dsn_clone = dsn.clone();
    let job_id_clone = job_id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let client = redis::Client::open(dsn_clone.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let job_key = format!("{JOB_KEY_PREFIX}{job_id_clone}");
        let events_key = format_job_events_key(&job_id_clone);
        let _: () = redis::pipe()
            .atomic()
            .hset(&job_key, "status", "FAILED")
            .hset(&job_key, "last_error", "boom")
            .cmd("XADD")
            .arg(&events_key)
            .arg("*")
            .arg("event")
            .arg("failed")
            .query_async(&mut conn)
            .await
            .unwrap();
    });

    let status = producer
        .wait_for_completion(&job_id, Duration::from_secs(2), Duration::from_millis(200))
        .await
        .unwrap()
        .expect("job should fail");
    assert_eq!(status.status, JobStatus::Failed);
    assert_eq!(status.last_error.as_deref(), Some("boom"));
}

#[tokio::test]
async fn producer_wait_for_completion_times_out() {
    let _guard = redis_lock().lock().await;
    let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
        .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
    let client = redis::Client::open(dsn.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    let producer = Producer::new(&dsn).await.unwrap();
    let job_id = producer
        .enqueue("work", serde_json::Map::new(), EnqueueOptions::default())
        .await
        .unwrap();

    let status = producer
        .wait_for_completion(
            &job_id,
            Duration::from_millis(150),
            Duration::from_millis(50),
        )
        .await
        .unwrap();
    assert!(status.is_none());
}

#[tokio::test]
async fn producer_connects_with_tls_when_configured() {
    let Ok(dsn) = std::env::var("RRQ_TEST_REDIS_TLS_DSN") else {
        eprintln!("Skipping TLS test: RRQ_TEST_REDIS_TLS_DSN not set");
        return;
    };

    let result = Producer::new(&dsn).await;
    assert!(
        result.is_ok(),
        "Failed to connect to Redis with TLS: {:?}",
        result.err()
    );
}
