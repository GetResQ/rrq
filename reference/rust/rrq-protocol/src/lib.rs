use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub const PROTOCOL_VERSION: &str = "1";

fn default_protocol_version() -> String {
    PROTOCOL_VERSION.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionContext {
    pub job_id: String,
    pub attempt: u32,
    pub enqueue_time: DateTime<Utc>,
    pub queue_name: String,
    pub deadline: Option<DateTime<Utc>>,
    #[serde(default)]
    pub trace_context: Option<HashMap<String, String>>,
    pub worker_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRequest {
    #[serde(default = "default_protocol_version")]
    pub protocol_version: String,
    pub job_id: String,
    pub function_name: String,
    #[serde(default)]
    pub args: Vec<Value>,
    #[serde(default)]
    pub kwargs: HashMap<String, Value>,
    pub context: ExecutionContext,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeStatus {
    Success,
    Retry,
    Timeout,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionOutcome {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    pub status: OutcomeStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_seconds: Option<f64>,
}

impl ExecutionOutcome {
    pub fn success<T: Serialize>(job_id: impl Into<String>, result: T) -> Self {
        let value = serde_json::to_value(result).unwrap_or(Value::Null);
        Self {
            job_id: Some(job_id.into()),
            status: OutcomeStatus::Success,
            result: Some(value),
            error_message: None,
            error_type: None,
            retry_after_seconds: None,
        }
    }

    pub fn retry(
        job_id: impl Into<String>,
        message: impl Into<String>,
        retry_after_seconds: Option<f64>,
    ) -> Self {
        Self {
            job_id: Some(job_id.into()),
            status: OutcomeStatus::Retry,
            result: None,
            error_message: Some(message.into()),
            error_type: None,
            retry_after_seconds,
        }
    }

    pub fn timeout(job_id: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            job_id: Some(job_id.into()),
            status: OutcomeStatus::Timeout,
            result: None,
            error_message: Some(message.into()),
            error_type: None,
            retry_after_seconds: None,
        }
    }

    pub fn error(job_id: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            job_id: Some(job_id.into()),
            status: OutcomeStatus::Error,
            result: None,
            error_message: Some(message.into()),
            error_type: None,
            retry_after_seconds: None,
        }
    }

    pub fn handler_not_found(
        job_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            job_id: Some(job_id.into()),
            status: OutcomeStatus::Error,
            result: None,
            error_message: Some(message.into()),
            error_type: Some("handler_not_found".to_string()),
            retry_after_seconds: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn execution_request_defaults_protocol_version() {
        let payload = json!({
            "job_id": "job-1",
            "function_name": "echo",
            "args": [],
            "kwargs": {},
            "context": {
                "job_id": "job-1",
                "attempt": 1,
                "enqueue_time": "2024-01-01T00:00:00Z",
                "queue_name": "default",
                "deadline": null,
                "trace_context": null,
                "worker_id": null
            }
        });
        let request: ExecutionRequest = serde_json::from_value(payload).unwrap();
        assert_eq!(request.protocol_version, PROTOCOL_VERSION);
    }

    #[test]
    fn handler_not_found_sets_error_type() {
        let outcome =
            ExecutionOutcome::handler_not_found("job-1", "missing handler");
        assert_eq!(outcome.status, OutcomeStatus::Error);
        assert_eq!(outcome.error_type.as_deref(), Some("handler_not_found"));
    }
}
