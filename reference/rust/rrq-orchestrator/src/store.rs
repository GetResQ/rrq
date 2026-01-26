use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use redis::Script;
use serde_json::Value;

use crate::constants::{
    ACTIVE_JOBS_PREFIX, DEFAULT_DLQ_RESULT_TTL_SECONDS, DLQ_KEY_PREFIX, HEALTH_KEY_PREFIX,
    JOB_KEY_PREFIX, LOCK_KEY_PREFIX, QUEUE_KEY_PREFIX, UNIQUE_JOB_LOCK_PREFIX,
};
use crate::job::{Job, JobStatus};
use crate::settings::RRQSettings;

#[derive(Clone)]
pub struct JobStore {
    settings: RRQSettings,
    conn: redis::aio::MultiplexedConnection,
    lock_and_remove_script: Script,
    retry_script: Script,
}

impl JobStore {
    pub async fn new(settings: RRQSettings) -> Result<Self> {
        let client = redis::Client::open(settings.redis_dsn.as_str())
            .with_context(|| "failed to create Redis client")?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .with_context(|| "failed to connect to Redis")?;
        Ok(Self::with_connection(settings, conn))
    }

    pub fn with_connection(
        settings: RRQSettings,
        conn: redis::aio::MultiplexedConnection,
    ) -> Self {
        let lock_and_remove_script = Script::new(
            "-- KEYS: [1] = lock_key, [2] = queue_key\n\
             -- ARGV: [1] = worker_id, [2] = lock_timeout_ms, [3] = job_id\n\
             local lock_result = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])\n\
             if lock_result then\n\
                 local removed_count = redis.call('ZREM', KEYS[2], ARGV[3])\n\
                 if removed_count == 0 then\n\
                     redis.call('DEL', KEYS[1])\n\
                     return {0, 0}\n\
                 end\n\
                 return {1, removed_count}\n\
             else\n\
                 return {0, 0}\n\
             end",
        );
        let retry_script = Script::new(
            "-- KEYS: [1] = job_key, [2] = queue_key\n\
             -- ARGV: [1] = job_id, [2] = retry_at_score, [3] = error_message, [4] = status\n\
             local new_retry_count = redis.call('HINCRBY', KEYS[1], 'current_retries', 1)\n\
             redis.call('HMSET', KEYS[1], 'status', ARGV[4], 'last_error', ARGV[3])\n\
             redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])\n\
             return new_retry_count",
        );

        Self {
            settings,
            conn,
            lock_and_remove_script,
            retry_script,
        }
    }

    pub fn settings(&self) -> &RRQSettings {
        &self.settings
    }

    pub fn clone_settings(&self) -> RRQSettings {
        self.settings.clone()
    }

    fn format_queue_key(&self, queue_name: &str) -> String {
        if queue_name.starts_with(QUEUE_KEY_PREFIX) {
            queue_name.to_string()
        } else {
            format!("{QUEUE_KEY_PREFIX}{queue_name}")
        }
    }

    fn format_dlq_key(&self, dlq_name: &str) -> String {
        if dlq_name.starts_with(DLQ_KEY_PREFIX) {
            dlq_name.to_string()
        } else {
            format!("{DLQ_KEY_PREFIX}{dlq_name}")
        }
    }

    fn active_jobs_key(worker_id: &str) -> String {
        format!("{ACTIVE_JOBS_PREFIX}{worker_id}")
    }

    fn parse_datetime(raw: &str) -> Option<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(raw)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    }

    fn parse_json(raw: &str) -> Option<Value> {
        if raw.is_empty() || raw.eq_ignore_ascii_case("null") {
            return None;
        }
        serde_json::from_str(raw).ok()
    }

    pub async fn save_job_definition(&mut self, job: &Job) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{}", job.id);
        let job_args_json = serde_json::to_string(&job.job_args)?;
        let job_kwargs_json = serde_json::to_string(&job.job_kwargs)?;
        let result_json = serde_json::to_string(&job.result)?;

        let mut mapping: Vec<(String, String)> = vec![
            ("id".to_string(), job.id.clone()),
            ("function_name".to_string(), job.function_name.clone()),
            ("job_args".to_string(), job_args_json),
            ("job_kwargs".to_string(), job_kwargs_json),
            (
                "enqueue_time".to_string(),
                job.enqueue_time.to_rfc3339(),
            ),
            ("status".to_string(), job.status.as_str().to_string()),
            (
                "current_retries".to_string(),
                job.current_retries.to_string(),
            ),
            ("max_retries".to_string(), job.max_retries.to_string()),
            ("result".to_string(), result_json),
        ];

        if let Some(value) = job.queue_name.as_ref() {
            mapping.push(("queue_name".to_string(), value.clone()));
        }
        if let Some(value) = job.next_scheduled_run_time {
            mapping.push(("next_scheduled_run_time".to_string(), value.to_rfc3339()));
        }
        if let Some(value) = job.start_time {
            mapping.push(("start_time".to_string(), value.to_rfc3339()));
        }
        if let Some(value) = job.job_timeout_seconds {
            mapping.push((
                "job_timeout_seconds".to_string(),
                value.to_string(),
            ));
        }
        if let Some(value) = job.result_ttl_seconds {
            mapping.push(("result_ttl_seconds".to_string(), value.to_string()));
        }
        if let Some(value) = job.job_unique_key.as_ref() {
            mapping.push(("job_unique_key".to_string(), value.clone()));
        }
        if let Some(value) = job.completion_time {
            mapping.push(("completion_time".to_string(), value.to_rfc3339()));
        }
        if let Some(value) = job.last_error.as_ref() {
            mapping.push(("last_error".to_string(), value.clone()));
        }
        if let Some(value) = job.dlq_name.as_ref() {
            mapping.push(("dlq_name".to_string(), value.clone()));
        }
        if let Some(value) = job.worker_id.as_ref() {
            mapping.push(("worker_id".to_string(), value.clone()));
        }
        if let Some(value) = job.trace_context.as_ref() {
            let trace_json = serde_json::to_string(value)?;
            mapping.push(("trace_context".to_string(), trace_json));
        }

        let mapping_ref: Vec<(&str, &str)> = mapping
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();

        self.conn
            .hset_multiple::<_, _, _, ()>(&job_key, &mapping_ref)
            .await?;

        Ok(())
    }

    pub async fn get_job_definition(&mut self, job_id: &str) -> Result<Option<Job>> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let raw: HashMap<String, String> = self.conn.hgetall(job_key).await?;
        if raw.is_empty() {
            return Ok(None);
        }

        let job_args = raw
            .get("job_args")
            .and_then(|value| serde_json::from_str(value).ok())
            .unwrap_or_default();
        let job_kwargs = raw
            .get("job_kwargs")
            .and_then(|value| serde_json::from_str(value).ok())
            .unwrap_or_default();
        let result = raw
            .get("result")
            .and_then(|value| Self::parse_json(value));
        let trace_context = raw.get("trace_context").and_then(|value| {
            if value.eq_ignore_ascii_case("null") {
                return None;
            }
            serde_json::from_str(value).ok()
        });

        let status = raw
            .get("status")
            .and_then(|value| JobStatus::parse(value))
            .ok_or_else(|| anyhow::anyhow!("invalid job status"))?;
        let enqueue_time = raw
            .get("enqueue_time")
            .and_then(|value| Self::parse_datetime(value))
            .ok_or_else(|| anyhow::anyhow!("missing enqueue_time"))?;
        let current_retries = raw
            .get("current_retries")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let max_retries = raw
            .get("max_retries")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(self.settings.default_max_retries);

        let job = Job {
            id: raw
                .get("id")
                .cloned()
                .unwrap_or_else(|| job_id.to_string()),
            function_name: raw
                .get("function_name")
                .cloned()
                .unwrap_or_default(),
            job_args,
            job_kwargs,
            enqueue_time,
            start_time: raw
                .get("start_time")
                .and_then(|value| Self::parse_datetime(value)),
            status,
            current_retries,
            next_scheduled_run_time: raw
                .get("next_scheduled_run_time")
                .and_then(|value| Self::parse_datetime(value)),
            max_retries,
            job_timeout_seconds: raw
                .get("job_timeout_seconds")
                .and_then(|value| value.parse::<i64>().ok()),
            result_ttl_seconds: raw
                .get("result_ttl_seconds")
                .and_then(|value| value.parse::<i64>().ok()),
            job_unique_key: raw.get("job_unique_key").cloned(),
            completion_time: raw
                .get("completion_time")
                .and_then(|value| Self::parse_datetime(value)),
            result,
            last_error: raw.get("last_error").cloned(),
            queue_name: raw.get("queue_name").cloned(),
            dlq_name: raw.get("dlq_name").cloned(),
            worker_id: raw.get("worker_id").cloned(),
            trace_context,
        };

        Ok(Some(job))
    }

    pub async fn add_job_to_queue(
        &mut self,
        queue_name: &str,
        job_id: &str,
        score_ms: f64,
    ) -> Result<()> {
        let queue_key = self.format_queue_key(queue_name);
        self.conn
            .zadd::<_, _, _, ()>(&queue_key, job_id, score_ms)
            .await?;
        Ok(())
    }

    pub async fn get_job_data_map(
        &mut self,
        job_id: &str,
    ) -> Result<Option<HashMap<String, String>>> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        self.get_job_data_map_by_key(&job_key).await
    }

    pub async fn get_job_data_map_by_key(
        &mut self,
        job_key: &str,
    ) -> Result<Option<HashMap<String, String>>> {
        let raw: HashMap<String, String> = self.conn.hgetall(job_key).await?;
        if raw.is_empty() {
            return Ok(None);
        }
        Ok(Some(raw))
    }

    pub async fn scan_job_keys(
        &mut self,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>)> {
        let pattern = format!("{JOB_KEY_PREFIX}*");
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut self.conn)
            .await?;
        Ok((next, keys))
    }

    pub async fn get_job_data_maps(
        &mut self,
        job_ids: &[String],
    ) -> Result<Vec<Option<HashMap<String, String>>>> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for job_id in job_ids {
            let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
            pipe.hgetall(job_key);
        }
        let results: Vec<HashMap<String, String>> = pipe.query_async(&mut self.conn).await?;
        Ok(results
            .into_iter()
            .map(|map| if map.is_empty() { None } else { Some(map) })
            .collect())
    }

    pub async fn queue_exists(&mut self, queue_name: &str) -> Result<bool> {
        let queue_key = self.format_queue_key(queue_name);
        let exists: bool = self.conn.exists(queue_key).await?;
        Ok(exists)
    }

    pub async fn delete_keys_by_pattern(&mut self, pattern: &str) -> Result<usize> {
        let mut cursor = 0u64;
        let mut deleted = 0usize;
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(200)
                .query_async(&mut self.conn)
                .await?;
            if !keys.is_empty() {
                let _: i64 = redis::cmd("DEL")
                    .arg(keys.clone())
                    .query_async(&mut self.conn)
                    .await?;
                deleted += keys.len();
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(deleted)
    }

    pub async fn scan_keys_by_pattern(&mut self, pattern: &str) -> Result<Vec<String>> {
        let mut cursor = 0u64;
        let mut keys = Vec::new();
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(200)
                .query_async(&mut self.conn)
                .await?;
            keys.extend(batch);
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(keys)
    }

    pub async fn update_job_status(&mut self, job_id: &str, status: JobStatus) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        self.conn
            .hset::<_, _, _, ()>(job_key, "status", status.as_str())
            .await?;
        Ok(())
    }

    pub async fn update_job_fields(
        &mut self,
        job_id: &str,
        fields: &HashMap<String, String>,
    ) -> Result<()> {
        if fields.is_empty() {
            return Ok(());
        }
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let mapping_ref: Vec<(&str, &str)> = fields
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        self.conn
            .hset_multiple::<_, _, _, ()>(&job_key, &mapping_ref)
            .await?;
        Ok(())
    }

    pub async fn dlq_len(&mut self, dlq_name: &str) -> Result<i64> {
        let key = self.format_dlq_key(dlq_name);
        let len: i64 = self.conn.llen(key).await?;
        Ok(len)
    }

    pub async fn dlq_remove_job(&mut self, dlq_name: &str, job_id: &str) -> Result<i64> {
        let key = self.format_dlq_key(dlq_name);
        let removed: i64 = self.conn.lrem(key, 1, job_id).await?;
        Ok(removed)
    }

    pub async fn get_ready_job_ids(
        &mut self,
        queue_name: &str,
        count: usize,
    ) -> Result<Vec<String>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let queue_key = self.format_queue_key(queue_name);
        let now_ms = Utc::now().timestamp_millis();
        let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&queue_key)
            .arg("-inf")
            .arg(now_ms)
            .arg("LIMIT")
            .arg(0)
            .arg(count)
            .query_async(&mut self.conn)
            .await?;
        Ok(ids)
    }

    pub async fn remove_job_from_queue(
        &mut self,
        queue_name: &str,
        job_id: &str,
    ) -> Result<i64> {
        let queue_key = self.format_queue_key(queue_name);
        let removed: i64 = self.conn.zrem(queue_key, job_id).await?;
        Ok(removed)
    }

    pub async fn atomic_lock_and_remove_job(
        &mut self,
        job_id: &str,
        queue_name: &str,
        worker_id: &str,
        lock_timeout_ms: i64,
    ) -> Result<(bool, i64)> {
        let lock_key = format!("{LOCK_KEY_PREFIX}{job_id}");
        let queue_key = self.format_queue_key(queue_name);
        let result: (i64, i64) = self
            .lock_and_remove_script
            .key(lock_key)
            .key(queue_key)
            .arg(worker_id)
            .arg(lock_timeout_ms)
            .arg(job_id)
            .invoke_async(&mut self.conn)
            .await?;
        Ok((result.0 != 0, result.1))
    }

    pub async fn release_job_lock(&mut self, job_id: &str) -> Result<()> {
        let lock_key = format!("{LOCK_KEY_PREFIX}{job_id}");
        let _: i64 = self.conn.del(lock_key).await?;
        Ok(())
    }

    pub async fn get_job_lock_owner(&mut self, job_id: &str) -> Result<Option<String>> {
        let lock_key = format!("{LOCK_KEY_PREFIX}{job_id}");
        let owner: Option<String> = self.conn.get(lock_key).await?;
        Ok(owner)
    }

    pub async fn mark_job_started(
        &mut self,
        job_id: &str,
        worker_id: &str,
        start_time: DateTime<Utc>,
    ) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let mut mapping: Vec<(&str, String)> = Vec::new();
        mapping.push(("status", JobStatus::Active.as_str().to_string()));
        mapping.push(("start_time", start_time.to_rfc3339()));
        mapping.push(("worker_id", worker_id.to_string()));
        let mapping_ref: Vec<(&str, &str)> = mapping
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect();
        self.conn
            .hset_multiple::<_, _, _, ()>(job_key, &mapping_ref)
            .await?;
        self.track_active_job(worker_id, job_id, start_time).await?;
        Ok(())
    }

    pub async fn mark_job_pending(
        &mut self,
        job_id: &str,
        last_error: Option<&str>,
    ) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.hset(&job_key, "status", JobStatus::Pending.as_str());
        if let Some(error) = last_error {
            pipe.hset(&job_key, "last_error", error);
        }
        pipe.hdel(&job_key, "start_time");
        pipe.hdel(&job_key, "worker_id");
        pipe.query_async::<_, ()>(&mut self.conn).await?;
        Ok(())
    }

    pub async fn update_job_next_scheduled_run_time(
        &mut self,
        job_id: &str,
        run_time: DateTime<Utc>,
    ) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        self.conn
            .hset::<_, _, _, ()>(job_key, "next_scheduled_run_time", run_time.to_rfc3339())
            .await?;
        Ok(())
    }

    pub async fn atomic_retry_job(
        &mut self,
        job_id: &str,
        queue_name: &str,
        retry_at_score: f64,
        error_message: &str,
        status: JobStatus,
    ) -> Result<i64> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let queue_key = self.format_queue_key(queue_name);
        let new_retry_count: i64 = self
            .retry_script
            .key(job_key)
            .key(queue_key)
            .arg(job_id)
            .arg(retry_at_score)
            .arg(error_message)
            .arg(status.as_str())
            .invoke_async(&mut self.conn)
            .await?;
        Ok(new_retry_count)
    }

    pub async fn increment_job_retries(&mut self, job_id: &str) -> Result<i64> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let new_retry_count: i64 = self.conn.hincr(job_key, "current_retries", 1).await?;
        Ok(new_retry_count)
    }

    pub async fn move_job_to_dlq(
        &mut self,
        job_id: &str,
        dlq_name: &str,
        error_message: &str,
        completion_time: DateTime<Utc>,
    ) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let dlq_key = self.format_dlq_key(dlq_name);
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.hset(&job_key, "status", JobStatus::Failed.as_str());
        pipe.hset(&job_key, "last_error", error_message);
        pipe.hset(&job_key, "completion_time", completion_time.to_rfc3339());
        pipe.lpush(&dlq_key, job_id);
        pipe.expire(&job_key, DEFAULT_DLQ_RESULT_TTL_SECONDS);
        pipe.query_async::<_, ()>(&mut self.conn).await?;
        Ok(())
    }

    pub async fn save_job_result(
        &mut self,
        job_id: &str,
        result: &Value,
        ttl_seconds: i64,
    ) -> Result<()> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let completion_time = Utc::now();
        let result_json = serde_json::to_string(result)?;
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.hset(&job_key, "result", result_json);
        pipe.hset(&job_key, "completion_time", completion_time.to_rfc3339());
        pipe.hset(&job_key, "status", JobStatus::Completed.as_str());
        if ttl_seconds > 0 {
            pipe.expire(&job_key, ttl_seconds);
        } else if ttl_seconds == 0 {
            pipe.persist(&job_key);
        }
        pipe.query_async::<_, ()>(&mut self.conn).await?;
        Ok(())
    }

    pub async fn track_active_job(
        &mut self,
        worker_id: &str,
        job_id: &str,
        start_time: DateTime<Utc>,
    ) -> Result<()> {
        let active_key = Self::active_jobs_key(worker_id);
        let score = start_time.timestamp() as f64;
        self.conn
            .zadd::<_, _, _, ()>(active_key, job_id, score)
            .await?;
        Ok(())
    }

    pub async fn remove_active_job(&mut self, worker_id: &str, job_id: &str) -> Result<()> {
        let active_key = Self::active_jobs_key(worker_id);
        let _: i64 = self.conn.zrem(active_key, job_id).await?;
        Ok(())
    }

    pub async fn acquire_unique_job_lock(
        &mut self,
        unique_key: &str,
        job_id: &str,
        lock_ttl_seconds: i64,
    ) -> Result<bool> {
        let lock_key = format!("{UNIQUE_JOB_LOCK_PREFIX}{unique_key}");
        let result: Option<String> = redis::cmd("SET")
            .arg(&lock_key)
            .arg(job_id)
            .arg("NX")
            .arg("EX")
            .arg(lock_ttl_seconds)
            .query_async(&mut self.conn)
            .await?;
        Ok(result.is_some())
    }

    pub async fn release_unique_job_lock(&mut self, unique_key: &str) -> Result<()> {
        let lock_key = format!("{UNIQUE_JOB_LOCK_PREFIX}{unique_key}");
        let _: i64 = self.conn.del(lock_key).await?;
        Ok(())
    }

    pub async fn get_lock_ttl(&mut self, unique_key: &str) -> Result<i64> {
        let lock_key = format!("{UNIQUE_JOB_LOCK_PREFIX}{unique_key}");
        let ttl: i64 = self.conn.ttl(lock_key).await?;
        Ok(if ttl > 0 { ttl } else { 0 })
    }

    pub async fn set_worker_health(
        &mut self,
        worker_id: &str,
        data: &serde_json::Map<String, Value>,
        ttl_seconds: i64,
    ) -> Result<()> {
        let key = format!("rrq:health:worker:{worker_id}");
        let payload = serde_json::to_string(data)?;
        redis::cmd("SET")
            .arg(key)
            .arg(payload)
            .arg("EX")
            .arg(ttl_seconds)
            .query_async::<_, ()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn get_worker_health(
        &mut self,
        worker_id: &str,
    ) -> Result<(Option<serde_json::Map<String, Value>>, Option<i64>)> {
        let key = format!("rrq:health:worker:{worker_id}");
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.get(&key);
        pipe.ttl(&key);
        let (payload, ttl): (Option<String>, i64) = pipe.query_async(&mut self.conn).await?;
        let payload = match payload {
            Some(value) => value,
            None => return Ok((None, None)),
        };
        let parsed: serde_json::Map<String, Value> =
            serde_json::from_str(&payload).unwrap_or_default();
        let ttl = if ttl >= 0 { Some(ttl) } else { None };
        Ok((Some(parsed), ttl))
    }

    pub async fn scan_active_job_keys(
        &mut self,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>)> {
        let pattern = format!("{ACTIVE_JOBS_PREFIX}*");
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut self.conn)
            .await?;
        Ok((next, keys))
    }

    pub async fn scan_queue_keys(&mut self, cursor: u64, count: usize) -> Result<(u64, Vec<String>)> {
        let pattern = format!("{QUEUE_KEY_PREFIX}*");
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut self.conn)
            .await?;
        Ok((next, keys))
    }

    pub async fn queue_size(&mut self, queue_name: &str) -> Result<i64> {
        let key = self.format_queue_key(queue_name);
        let size: i64 = self.conn.zcard(key).await?;
        Ok(size)
    }

    pub async fn queue_range_with_scores(
        &mut self,
        queue_name: &str,
        start: isize,
        stop: isize,
    ) -> Result<Vec<(String, f64)>> {
        let key = self.format_queue_key(queue_name);
        let entries: Vec<(String, f64)> = self.conn.zrange_withscores(key, start, stop).await?;
        Ok(entries)
    }

    pub async fn scan_worker_health_keys(
        &mut self,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>)> {
        let pattern = format!("{HEALTH_KEY_PREFIX}*");
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut self.conn)
            .await?;
        Ok((next, keys))
    }

    pub async fn get_active_job_ids(&mut self, worker_id: &str) -> Result<Vec<String>> {
        let key = Self::active_jobs_key(worker_id);
        let ids: Vec<String> = self.conn.zrange(key, 0, -1).await?;
        Ok(ids)
    }

    pub async fn is_job_queued(&mut self, queue_name: &str, job_id: &str) -> Result<bool> {
        let key = self.format_queue_key(queue_name);
        let score: Option<f64> = self.conn.zscore(key, job_id).await?;
        Ok(score.is_some())
    }

    pub async fn get_dlq_job_ids(&mut self, dlq_name: &str) -> Result<Vec<String>> {
        let key = self.format_dlq_key(dlq_name);
        let ids: Vec<String> = self.conn.lrange(key, 0, -1).await?;
        Ok(ids)
    }

    pub async fn flushdb(&mut self) -> Result<()> {
        redis::cmd("FLUSHDB")
            .query_async::<_, ()>(&mut self.conn)
            .await?;
        Ok(())
    }
}
