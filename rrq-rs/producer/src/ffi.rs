#![allow(unsafe_code)] // FFI requires unsafe for C interop

use crate::{EnqueueOptions, Producer, ProducerConfig};
use chrono::{DateTime, Utc};
use rrq_config::{ProducerSettings, load_producer_settings};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::any::Any;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::{self, AssertUnwindSafe};
use std::ptr;
use std::sync::mpsc;
use std::time::Duration;

const MAX_MILLIS: i64 = i64::MAX / 2;

#[derive(Debug, Serialize)]
struct ProducerConstantsPayload {
    job_key_prefix: &'static str,
    queue_key_prefix: &'static str,
    idempotency_key_prefix: &'static str,
}

pub struct ProducerHandle {
    runtime: tokio::runtime::Runtime,
    producer: Producer,
}

#[derive(Debug, Deserialize)]
struct ProducerConfigPayload {
    redis_dsn: String,
    queue_name: Option<String>,
    max_retries: Option<i64>,
    job_timeout_seconds: Option<i64>,
    result_ttl_seconds: Option<i64>,
    idempotency_ttl_seconds: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum EnqueueMode {
    Enqueue,
    Unique,
    RateLimit,
    Debounce,
    Deferred,
}

#[derive(Debug, Deserialize)]
struct EnqueueOptionsPayload {
    queue_name: Option<String>,
    job_id: Option<String>,
    unique_key: Option<String>,
    unique_ttl_seconds: Option<i64>,
    max_retries: Option<i64>,
    job_timeout_seconds: Option<i64>,
    result_ttl_seconds: Option<i64>,
    trace_context: Option<HashMap<String, String>>,
    defer_by_seconds: Option<f64>,
    defer_until: Option<String>,
    enqueue_time: Option<String>,
    rate_limit_key: Option<String>,
    rate_limit_seconds: Option<f64>,
    debounce_key: Option<String>,
    debounce_seconds: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct EnqueueRequestPayload {
    mode: Option<EnqueueMode>,
    function_name: String,
    args: Option<Vec<Value>>,
    kwargs: Option<Map<String, Value>>,
    options: Option<EnqueueOptionsPayload>,
}

#[derive(Debug, serde::Serialize)]
struct EnqueueResponsePayload {
    status: String,
    job_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JobStatusRequestPayload {
    job_id: String,
}

#[derive(Debug, Serialize)]
struct JobResultPayload {
    status: String,
    result: Option<Value>,
    last_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct JobStatusResponsePayload {
    found: bool,
    job: Option<JobResultPayload>,
}

fn infer_mode(options: &EnqueueOptionsPayload) -> Result<EnqueueMode, String> {
    let has_rate_limit = options.rate_limit_key.is_some() || options.rate_limit_seconds.is_some();
    let has_debounce = options.debounce_key.is_some() || options.debounce_seconds.is_some();
    if has_rate_limit && has_debounce {
        return Err("rate_limit and debounce options are mutually exclusive".to_string());
    }
    if has_rate_limit {
        return Ok(EnqueueMode::RateLimit);
    }
    if has_debounce {
        return Ok(EnqueueMode::Debounce);
    }
    Ok(EnqueueMode::Enqueue)
}

fn set_error(error_out: *mut *mut c_char, message: impl AsRef<str>) {
    if error_out.is_null() {
        return;
    }
    let cstr = CString::new(message.as_ref()).unwrap_or_else(|_| CString::new("error").unwrap());
    unsafe {
        *error_out = cstr.into_raw();
    }
}

fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        message.to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "panic".to_string()
    }
}

fn take_cstr(ptr: *const c_char) -> Result<String, String> {
    if ptr.is_null() {
        return Err("received null pointer".to_string());
    }
    let cstr = unsafe { CStr::from_ptr(ptr) };
    cstr.to_str()
        .map(|s| s.to_string())
        .map_err(|err| err.to_string())
}

fn parse_datetime(raw: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| err.to_string())
}

fn duration_millis_from_seconds(value: f64, label: &str, allow_zero: bool) -> Result<i64, String> {
    if !value.is_finite() {
        return Err(format!("{label} must be finite"));
    }
    if value < 0.0 || (!allow_zero && value == 0.0) {
        return Err(format!("{label} must be positive"));
    }
    let millis = (value * 1000.0).ceil();
    if millis > MAX_MILLIS as f64 {
        return Err(format!("{label} is too large"));
    }
    Ok(millis as i64)
}

fn parse_duration_seconds(value: f64, label: &str) -> Result<Duration, String> {
    let millis = duration_millis_from_seconds(value, label, false)?;
    Ok(Duration::from_millis(millis as u64))
}

fn job_status_string(status: crate::JobStatus) -> &'static str {
    match status {
        crate::JobStatus::Pending => "PENDING",
        crate::JobStatus::Active => "ACTIVE",
        crate::JobStatus::Completed => "COMPLETED",
        crate::JobStatus::Failed => "FAILED",
        crate::JobStatus::Retrying => "RETRYING",
        crate::JobStatus::Unknown => "UNKNOWN",
    }
}

fn job_result_payload(result: crate::JobResult) -> JobResultPayload {
    JobResultPayload {
        status: job_status_string(result.status).to_string(),
        result: result.result,
        last_error: result.last_error,
    }
}

fn block_on_runtime<T, F>(runtime: &tokio::runtime::Runtime, future: F) -> Result<T, String>
where
    T: Send + 'static,
    F: std::future::Future<Output = T> + Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        let handle = runtime.handle().clone();
        let (tx, rx) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let result = handle.block_on(future);
            let _ = tx.send(result);
        });
        return rx
            .recv()
            .map_err(|_| "runtime thread failed to deliver result".to_string());
    }

    Ok(runtime.block_on(future))
}

fn with_unwind<T>(
    error_out: *mut *mut c_char,
    func: impl FnOnce() -> Result<T, String>,
) -> Option<T> {
    match panic::catch_unwind(AssertUnwindSafe(func)) {
        Ok(Ok(value)) => Some(value),
        Ok(Err(err)) => {
            set_error(error_out, err);
            None
        }
        Err(panic) => {
            set_error(error_out, format!("panic: {}", panic_message(panic)));
            None
        }
    }
}

fn producer_config_from_settings(settings: &ProducerSettings) -> ProducerConfig {
    ProducerConfig {
        queue_name: settings.queue_name.clone(),
        max_retries: settings.max_retries,
        job_timeout_seconds: settings.job_timeout_seconds,
        result_ttl_seconds: settings.result_ttl_seconds,
        idempotency_ttl_seconds: settings.idempotency_ttl_seconds,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rrq_producer_new(
    config_json: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut ProducerHandle {
    with_unwind(error_out, || {
        let payload = take_cstr(config_json)?;
        let config_payload: ProducerConfigPayload = serde_json::from_str(&payload)
            .map_err(|err| format!("invalid producer config: {err}"))?;

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| err.to_string())?;

        let default_config = ProducerConfig::default();
        let config = ProducerConfig {
            queue_name: config_payload
                .queue_name
                .unwrap_or(default_config.queue_name),
            max_retries: config_payload
                .max_retries
                .unwrap_or(default_config.max_retries),
            job_timeout_seconds: config_payload
                .job_timeout_seconds
                .unwrap_or(default_config.job_timeout_seconds),
            result_ttl_seconds: config_payload
                .result_ttl_seconds
                .unwrap_or(default_config.result_ttl_seconds),
            idempotency_ttl_seconds: config_payload
                .idempotency_ttl_seconds
                .unwrap_or(default_config.idempotency_ttl_seconds),
        };

        let producer = block_on_runtime(
            &runtime,
            Producer::with_config(config_payload.redis_dsn, config),
        )?
        .map_err(|err| err.to_string())?;

        Ok(Box::into_raw(Box::new(ProducerHandle {
            runtime,
            producer,
        })))
    })
    .unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn rrq_producer_new_from_toml(
    config_path: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut ProducerHandle {
    with_unwind(error_out, || {
        let config_path = if config_path.is_null() {
            None
        } else {
            Some(take_cstr(config_path)?)
        };
        let settings =
            load_producer_settings(config_path.as_deref()).map_err(|err| err.to_string())?;
        let redis_dsn = settings.redis_dsn.clone();
        let config = producer_config_from_settings(&settings);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| err.to_string())?;

        let producer = block_on_runtime(&runtime, Producer::with_config(redis_dsn, config))?
            .map_err(|err| err.to_string())?;

        Ok(Box::into_raw(Box::new(ProducerHandle {
            runtime,
            producer,
        })))
    })
    .unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn rrq_producer_config_from_toml(
    config_path: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    with_unwind(error_out, || {
        let config_path = if config_path.is_null() {
            None
        } else {
            Some(take_cstr(config_path)?)
        };
        let settings =
            load_producer_settings(config_path.as_deref()).map_err(|err| err.to_string())?;
        let json = serde_json::to_string(&settings).map_err(|err| err.to_string())?;
        let cstr = CString::new(json).map_err(|err| err.to_string())?;
        Ok(cstr.into_raw())
    })
    .unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn rrq_producer_free(handle: *mut ProducerHandle) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() {
            return;
        }
        unsafe {
            drop(Box::from_raw(handle));
        }
    }));
}

/// Returns the RRQ producer library version as a C string.
///
/// The returned pointer must be freed with `rrq_string_free`.
#[unsafe(no_mangle)]
pub extern "C" fn rrq_producer_version() -> *mut c_char {
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    CString::new(VERSION)
        .map(|s| s.into_raw())
        .unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn rrq_producer_constants(error_out: *mut *mut c_char) -> *mut c_char {
    with_unwind(error_out, || {
        let payload = ProducerConstantsPayload {
            job_key_prefix: crate::JOB_KEY_PREFIX,
            queue_key_prefix: crate::QUEUE_KEY_PREFIX,
            idempotency_key_prefix: crate::IDEMPOTENCY_KEY_PREFIX,
        };

        let json = serde_json::to_string(&payload).map_err(|err| err.to_string())?;
        let cstr = CString::new(json).map_err(|err| err.to_string())?;
        Ok(cstr.into_raw())
    })
    .unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn rrq_producer_enqueue(
    handle: *mut ProducerHandle,
    request_json: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    with_unwind(error_out, || {
        if handle.is_null() {
            return Err("producer handle is null".to_string());
        }

        let payload = take_cstr(request_json)?;
        let request: EnqueueRequestPayload = serde_json::from_str(&payload)
            .map_err(|err| format!("invalid enqueue request: {err}"))?;

        let args = request.args.unwrap_or_default();
        let kwargs = request.kwargs.unwrap_or_default();
        let options_payload = request.options.unwrap_or(EnqueueOptionsPayload {
            queue_name: None,
            job_id: None,
            unique_key: None,
            unique_ttl_seconds: None,
            max_retries: None,
            job_timeout_seconds: None,
            result_ttl_seconds: None,
            trace_context: None,
            defer_by_seconds: None,
            defer_until: None,
            enqueue_time: None,
            rate_limit_key: None,
            rate_limit_seconds: None,
            debounce_key: None,
            debounce_seconds: None,
        });
        let mode = match request.mode {
            Some(EnqueueMode::Unique) | Some(EnqueueMode::Deferred) => EnqueueMode::Enqueue,
            Some(mode) => mode,
            None => infer_mode(&options_payload)?,
        };
        let EnqueueOptionsPayload {
            queue_name,
            job_id,
            unique_key,
            unique_ttl_seconds,
            max_retries,
            job_timeout_seconds,
            result_ttl_seconds,
            trace_context,
            defer_by_seconds,
            defer_until,
            enqueue_time: enqueue_time_raw,
            rate_limit_key,
            rate_limit_seconds,
            debounce_key,
            debounce_seconds,
        } = options_payload;

        if let Some(ttl) = unique_ttl_seconds
            && ttl <= 0
        {
            return Err("unique_ttl_seconds must be positive".to_string());
        }

        let enqueue_time = match enqueue_time_raw {
            Some(raw) => parse_datetime(&raw)?,
            None => Utc::now(),
        };

        let scheduled_time = if let Some(raw) = defer_until.as_deref() {
            Some(parse_datetime(raw)?)
        } else if let Some(seconds) = defer_by_seconds {
            let millis = duration_millis_from_seconds(seconds, "defer_by_seconds", true)?;
            if millis == 0 {
                None
            } else {
                let duration = chrono::Duration::milliseconds(millis);
                let scheduled = enqueue_time
                    .checked_add_signed(duration)
                    .ok_or_else(|| "defer_by_seconds out of range".to_string())?;
                Some(scheduled)
            }
        } else {
            None
        };

        let mut options = EnqueueOptions {
            queue_name,
            job_id,
            max_retries,
            job_timeout_seconds,
            result_ttl_seconds,
            trace_context,
            enqueue_time: Some(enqueue_time),
            scheduled_time,
            ..Default::default()
        };

        if let Some(unique_key) = unique_key {
            options.idempotency_key = Some(unique_key);
            options.idempotency_ttl_seconds = unique_ttl_seconds;
        }

        let producer_handle = unsafe { &*handle };
        let producer = producer_handle.producer.clone();
        let function_name = request.function_name;
        let response = block_on_runtime(&producer_handle.runtime, async move {
            match mode {
                EnqueueMode::Enqueue | EnqueueMode::Unique | EnqueueMode::Deferred => producer
                    .enqueue(&function_name, args, kwargs, options)
                    .await
                    .map(Some),
                EnqueueMode::RateLimit => {
                    let key = rate_limit_key
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("rate_limit_key is required"))?;
                    let seconds = rate_limit_seconds
                        .ok_or_else(|| anyhow::anyhow!("rate_limit_seconds is required"))?;
                    let window = parse_duration_seconds(seconds, "rate_limit_seconds")
                        .map_err(anyhow::Error::msg)?;
                    producer
                        .enqueue_with_rate_limit(&function_name, args, kwargs, key, window, options)
                        .await
                }
                EnqueueMode::Debounce => {
                    let key = debounce_key
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("debounce_key is required"))?;
                    let seconds = debounce_seconds
                        .ok_or_else(|| anyhow::anyhow!("debounce_seconds is required"))?;
                    let window = parse_duration_seconds(seconds, "debounce_seconds")
                        .map_err(anyhow::Error::msg)?;
                    let mut debounce_options = options;
                    let millis = duration_millis_from_seconds(
                        window.as_secs_f64(),
                        "debounce_seconds",
                        false,
                    )
                    .map_err(anyhow::Error::msg)?;
                    let duration = chrono::Duration::milliseconds(millis);
                    let scheduled = enqueue_time
                        .checked_add_signed(duration)
                        .ok_or_else(|| anyhow::anyhow!("debounce_seconds out of range"))?;
                    debounce_options.scheduled_time = Some(scheduled);
                    producer
                        .enqueue_with_debounce(
                            &function_name,
                            args,
                            kwargs,
                            key,
                            window,
                            debounce_options,
                        )
                        .await
                        .map(Some)
                }
            }
        })?;

        let response = match response {
            Ok(job_id) => EnqueueResponsePayload {
                status: if job_id.is_some() {
                    "enqueued".to_string()
                } else {
                    "rate_limited".to_string()
                },
                job_id,
            },
            Err(err) => {
                return Err(err.to_string());
            }
        };

        let json = serde_json::to_string(&response).map_err(|err| err.to_string())?;
        let cstr = CString::new(json).map_err(|err| err.to_string())?;
        Ok(cstr.into_raw())
    })
    .unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn rrq_producer_get_job_status(
    handle: *mut ProducerHandle,
    request_json: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    with_unwind(error_out, || {
        if handle.is_null() {
            return Err("producer handle is null".to_string());
        }

        let payload = take_cstr(request_json)?;
        let request: JobStatusRequestPayload = serde_json::from_str(&payload)
            .map_err(|err| format!("invalid job status request: {err}"))?;

        let producer_handle = unsafe { &*handle };
        let producer = producer_handle.producer.clone();
        let job_id = request.job_id;
        let response = block_on_runtime(&producer_handle.runtime, async move {
            producer.get_job_status(&job_id).await
        })?;

        let payload = match response {
            Ok(Some(result)) => JobStatusResponsePayload {
                found: true,
                job: Some(job_result_payload(result)),
            },
            Ok(None) => JobStatusResponsePayload {
                found: false,
                job: None,
            },
            Err(err) => {
                return Err(err.to_string());
            }
        };

        let json = serde_json::to_string(&payload).map_err(|err| err.to_string())?;
        let cstr = CString::new(json).map_err(|err| err.to_string())?;
        Ok(cstr.into_raw())
    })
    .unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "C" fn rrq_string_free(ptr: *mut c_char) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if ptr.is_null() {
            return;
        }
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::ffi::CStr;

    #[derive(Debug, Deserialize)]
    struct ConstantsPayload {
        job_key_prefix: String,
        queue_key_prefix: String,
        idempotency_key_prefix: String,
    }

    #[test]
    fn producer_constants_payload_matches_defaults() {
        let mut err: *mut c_char = ptr::null_mut();
        let ptr = rrq_producer_constants(&mut err);
        assert!(err.is_null());
        assert!(!ptr.is_null());

        let json = unsafe { CStr::from_ptr(ptr) }
            .to_str()
            .expect("constants json utf-8");
        let payload: ConstantsPayload = serde_json::from_str(json).expect("constants json parses");
        rrq_string_free(ptr);

        assert_eq!(payload.job_key_prefix, crate::JOB_KEY_PREFIX);
        assert_eq!(payload.queue_key_prefix, crate::QUEUE_KEY_PREFIX);
        assert_eq!(
            payload.idempotency_key_prefix,
            crate::IDEMPOTENCY_KEY_PREFIX
        );
    }
}
