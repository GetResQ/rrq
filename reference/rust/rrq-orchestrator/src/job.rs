use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobStatus {
    Pending,
    Active,
    Completed,
    Failed,
    Retrying,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub function_name: String,
    #[serde(default)]
    pub job_args: Vec<Value>,
    #[serde(default)]
    pub job_kwargs: serde_json::Map<String, Value>,

    pub enqueue_time: DateTime<Utc>,
    #[serde(default)]
    pub start_time: Option<DateTime<Utc>>,

    pub status: JobStatus,
    pub current_retries: i64,
    #[serde(default)]
    pub next_scheduled_run_time: Option<DateTime<Utc>>,

    pub max_retries: i64,
    #[serde(default)]
    pub job_timeout_seconds: Option<i64>,
    #[serde(default)]
    pub result_ttl_seconds: Option<i64>,

    #[serde(default)]
    pub job_unique_key: Option<String>,

    #[serde(default)]
    pub completion_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub last_error: Option<String>,

    #[serde(default)]
    pub queue_name: Option<String>,
    #[serde(default)]
    pub dlq_name: Option<String>,
    #[serde(default)]
    pub worker_id: Option<String>,

    #[serde(default)]
    pub trace_context: Option<std::collections::HashMap<String, String>>,
}

impl Job {
    pub fn new_id() -> String {
        Uuid::new_v4().to_string()
    }
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "PENDING",
            JobStatus::Active => "ACTIVE",
            JobStatus::Completed => "COMPLETED",
            JobStatus::Failed => "FAILED",
            JobStatus::Retrying => "RETRYING",
            JobStatus::Cancelled => "CANCELLED",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "PENDING" => Some(JobStatus::Pending),
            "ACTIVE" => Some(JobStatus::Active),
            "COMPLETED" => Some(JobStatus::Completed),
            "FAILED" => Some(JobStatus::Failed),
            "RETRYING" => Some(JobStatus::Retrying),
            "CANCELLED" => Some(JobStatus::Cancelled),
            _ => None,
        }
    }
}
