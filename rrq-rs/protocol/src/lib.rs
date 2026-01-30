use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

pub const PROTOCOL_VERSION: &str = "1";
pub const FRAME_HEADER_LEN: usize = 4;

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
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutorMessage {
    Request { payload: ExecutionRequest },
    Response { payload: ExecutionOutcome },
    Cancel { payload: CancelRequest },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelRequest {
    #[serde(default = "default_protocol_version")]
    pub protocol_version: String,
    pub job_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default)]
    pub hard_kill: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRequest {
    #[serde(default = "default_protocol_version")]
    pub protocol_version: String,
    pub request_id: String,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub status: OutcomeStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ExecutionError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_seconds: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionError {
    pub message: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl ExecutionOutcome {
    pub fn success<T: Serialize>(
        job_id: impl Into<String>,
        request_id: impl Into<String>,
        result: T,
    ) -> Self {
        let value = serde_json::to_value(result).unwrap_or(Value::Null);
        Self {
            job_id: Some(job_id.into()),
            request_id: Some(request_id.into()),
            status: OutcomeStatus::Success,
            result: Some(value),
            error: None,
            retry_after_seconds: None,
        }
    }

    pub fn retry(
        job_id: impl Into<String>,
        request_id: impl Into<String>,
        message: impl Into<String>,
        retry_after_seconds: Option<f64>,
    ) -> Self {
        Self {
            job_id: Some(job_id.into()),
            request_id: Some(request_id.into()),
            status: OutcomeStatus::Retry,
            result: None,
            error: Some(ExecutionError {
                message: message.into(),
                error_type: None,
                code: None,
                details: None,
            }),
            retry_after_seconds,
        }
    }

    pub fn timeout(
        job_id: impl Into<String>,
        request_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            job_id: Some(job_id.into()),
            request_id: Some(request_id.into()),
            status: OutcomeStatus::Timeout,
            result: None,
            error: Some(ExecutionError {
                message: message.into(),
                error_type: None,
                code: None,
                details: None,
            }),
            retry_after_seconds: None,
        }
    }

    pub fn error(
        job_id: impl Into<String>,
        request_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            job_id: Some(job_id.into()),
            request_id: Some(request_id.into()),
            status: OutcomeStatus::Error,
            result: None,
            error: Some(ExecutionError {
                message: message.into(),
                error_type: None,
                code: None,
                details: None,
            }),
            retry_after_seconds: None,
        }
    }

    pub fn handler_not_found(
        job_id: impl Into<String>,
        request_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            job_id: Some(job_id.into()),
            request_id: Some(request_id.into()),
            status: OutcomeStatus::Error,
            result: None,
            error: Some(ExecutionError {
                message: message.into(),
                error_type: Some("handler_not_found".to_string()),
                code: None,
                details: None,
            }),
            retry_after_seconds: None,
        }
    }
}

#[derive(Debug)]
pub enum FrameError {
    InvalidLength,
    Json(serde_json::Error),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength => write!(f, "invalid frame length"),
            Self::Json(err) => write!(f, "json decode error: {err}"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<serde_json::Error> for FrameError {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

pub fn encode_frame(message: &ExecutorMessage) -> Result<Vec<u8>, FrameError> {
    let payload = serde_json::to_vec(message)?;
    let length = u32::try_from(payload.len()).map_err(|_| FrameError::InvalidLength)?;
    let mut framed = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    framed.extend_from_slice(&length.to_be_bytes());
    framed.extend_from_slice(&payload);
    Ok(framed)
}

pub fn decode_frame(frame: &[u8]) -> Result<ExecutorMessage, FrameError> {
    if frame.len() < FRAME_HEADER_LEN {
        return Err(FrameError::InvalidLength);
    }
    let mut header = [0u8; FRAME_HEADER_LEN];
    header.copy_from_slice(&frame[..FRAME_HEADER_LEN]);
    let length = u32::from_be_bytes(header) as usize;
    if frame.len() - FRAME_HEADER_LEN != length {
        return Err(FrameError::InvalidLength);
    }
    Ok(serde_json::from_slice(&frame[FRAME_HEADER_LEN..])?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn execution_request_defaults_protocol_version() {
        let payload = json!({
            "job_id": "job-1",
            "request_id": "req-1",
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
        let outcome = ExecutionOutcome::handler_not_found("job-1", "req-1", "missing handler");
        assert_eq!(outcome.status, OutcomeStatus::Error);
        assert_eq!(
            outcome
                .error
                .as_ref()
                .and_then(|err| err.error_type.as_deref()),
            Some("handler_not_found")
        );
    }

    #[test]
    fn executor_message_round_trip() {
        let request = ExecutionRequest {
            protocol_version: PROTOCOL_VERSION.to_string(),
            request_id: "req-1".to_string(),
            job_id: "job-1".to_string(),
            function_name: "echo".to_string(),
            args: Vec::new(),
            kwargs: HashMap::new(),
            context: ExecutionContext {
                job_id: "job-1".to_string(),
                attempt: 1,
                enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
                queue_name: "default".to_string(),
                deadline: None,
                trace_context: None,
                worker_id: None,
            },
        };
        let msg = ExecutorMessage::Request { payload: request };
        let serialized = serde_json::to_string(&msg).unwrap();
        let decoded: ExecutorMessage = serde_json::from_str(&serialized).unwrap();
        let ExecutorMessage::Request { payload } = decoded else {
            panic!("unexpected message type")
        };
        assert_eq!(payload.protocol_version, PROTOCOL_VERSION);
        assert_eq!(payload.request_id, "req-1");
    }

    #[test]
    fn cancel_request_round_trip() {
        let cancel = CancelRequest {
            protocol_version: PROTOCOL_VERSION.to_string(),
            job_id: "job-1".to_string(),
            request_id: Some("req-1".to_string()),
            hard_kill: true,
        };
        let msg = ExecutorMessage::Cancel { payload: cancel };
        let serialized = serde_json::to_string(&msg).unwrap();
        let decoded: ExecutorMessage = serde_json::from_str(&serialized).unwrap();
        let ExecutorMessage::Cancel { payload } = decoded else {
            panic!("unexpected message type")
        };
        assert_eq!(payload.protocol_version, PROTOCOL_VERSION);
        assert_eq!(payload.request_id.as_deref(), Some("req-1"));
        assert!(payload.hard_kill);
    }

    #[test]
    fn frame_round_trip() {
        let outcome = ExecutionOutcome::success("job-1", "req-1", json!({"ok": true}));
        let message = ExecutorMessage::Response { payload: outcome };
        let framed = encode_frame(&message).expect("frame encode failed");
        let decoded = decode_frame(&framed).expect("frame decode failed");
        let ExecutorMessage::Response { payload } = decoded else {
            panic!("unexpected message variant")
        };
        assert_eq!(payload.status, OutcomeStatus::Success);
        assert_eq!(payload.job_id.as_deref(), Some("job-1"));
    }
}
