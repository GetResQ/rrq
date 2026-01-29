use crate::registry::Registry;
use crate::telemetry::{NoopTelemetry, Telemetry};
use crate::types::{ExecutionError, ExecutionOutcome};
use chrono::{DateTime, Utc};
use rrq_protocol::{CancelRequest, ExecutorMessage, OutcomeStatus, PROTOCOL_VERSION, encode_frame};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::{Mutex, mpsc};

pub const ENV_EXECUTOR_SOCKET: &str = "RRQ_EXECUTOR_SOCKET";
pub const ENV_EXECUTOR_TCP_SOCKET: &str = "RRQ_EXECUTOR_TCP_SOCKET";
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

fn invalid_input(message: impl Into<String>) -> Box<dyn std::error::Error> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.into(),
    ))
}

pub fn parse_tcp_socket(raw: &str) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(invalid_input("executor tcp_socket cannot be empty"));
    }

    let (host, port_str) = if let Some(rest) = raw.strip_prefix('[') {
        let (host, port_str) = rest
            .split_once("]:")
            .ok_or_else(|| invalid_input("executor tcp_socket must be in [host]:port format"))?;
        (host, port_str)
    } else {
        let (host, port_str) = raw
            .rsplit_once(':')
            .ok_or_else(|| invalid_input("executor tcp_socket must be in host:port format"))?;
        if host.is_empty() {
            return Err(invalid_input("executor tcp_socket host cannot be empty"));
        }
        (host, port_str)
    };

    let port: u16 = port_str
        .parse()
        .map_err(|_| invalid_input(format!("Invalid executor tcp_socket port: {port_str}")))?;
    if port == 0 {
        return Err(invalid_input("executor tcp_socket port must be > 0"));
    }

    let ip = if host == "localhost" {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    } else {
        let parsed: IpAddr = host
            .parse()
            .map_err(|_| invalid_input(format!("Invalid executor tcp_socket host: {host}")))?;
        if !parsed.is_loopback() {
            return Err(invalid_input("executor tcp_socket host must be localhost"));
        }
        parsed
    };

    Ok(SocketAddr::new(ip, port))
}

pub struct ExecutorRuntime {
    runtime: tokio::runtime::Runtime,
}

impl ExecutorRuntime {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            runtime: tokio::runtime::Runtime::new()?,
        })
    }

    pub fn enter(&self) -> tokio::runtime::EnterGuard<'_> {
        self.runtime.enter()
    }

    pub fn run_socket(
        &self,
        registry: &Registry,
        socket_path: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let telemetry = NoopTelemetry;
        self.run_socket_with(registry, socket_path, &telemetry)
    }

    pub fn run_socket_with<T: Telemetry + ?Sized>(
        &self,
        registry: &Registry,
        socket_path: impl AsRef<Path>,
        telemetry: &T,
    ) -> Result<(), Box<dyn std::error::Error>> {
        run_socket_loop(&self.runtime, registry, socket_path.as_ref(), telemetry)
    }

    pub fn run_tcp(
        &self,
        registry: &Registry,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let telemetry = NoopTelemetry;
        self.run_tcp_with(registry, addr, &telemetry)
    }

    pub fn run_tcp_with<T: Telemetry + ?Sized>(
        &self,
        registry: &Registry,
        addr: SocketAddr,
        telemetry: &T,
    ) -> Result<(), Box<dyn std::error::Error>> {
        run_tcp_loop(&self.runtime, registry, addr, telemetry)
    }
}

pub fn run_socket(
    registry: &Registry,
    socket_path: impl AsRef<Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    ExecutorRuntime::new()?.run_socket(registry, socket_path)
}

pub fn run_socket_with<T: Telemetry + ?Sized>(
    registry: &Registry,
    socket_path: impl AsRef<Path>,
    telemetry: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    ExecutorRuntime::new()?.run_socket_with(registry, socket_path, telemetry)
}

pub fn run_tcp(registry: &Registry, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    ExecutorRuntime::new()?.run_tcp(registry, addr)
}

pub fn run_tcp_with<T: Telemetry + ?Sized>(
    registry: &Registry,
    addr: SocketAddr,
    telemetry: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    ExecutorRuntime::new()?.run_tcp_with(registry, addr, telemetry)
}

fn run_socket_loop<T: Telemetry + ?Sized>(
    runtime: &tokio::runtime::Runtime,
    registry: &Registry,
    socket_path: &Path,
    telemetry: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    let registry = registry.clone();
    let in_flight: Arc<Mutex<HashMap<String, InFlightTask>>> = Arc::new(Mutex::new(HashMap::new()));
    let job_index: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let telemetry = telemetry.clone_box();
    runtime.block_on(async move {
        if let Some(parent) = socket_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if socket_path.exists() {
            let _ = tokio::fs::remove_file(socket_path).await;
        }
        let listener = UnixListener::bind(socket_path)?;
        loop {
            let (stream, _) = listener.accept().await?;
            let registry = registry.clone();
            let telemetry = telemetry.clone();
            let in_flight = in_flight.clone();
            let job_index = job_index.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    handle_connection(stream, &registry, telemetry.as_ref(), in_flight, job_index)
                        .await
                {
                    tracing::error!("executor connection error: {err}");
                }
            });
        }
    })
}

fn run_tcp_loop<T: Telemetry + ?Sized>(
    runtime: &tokio::runtime::Runtime,
    registry: &Registry,
    addr: SocketAddr,
    telemetry: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    let registry = registry.clone();
    let in_flight: Arc<Mutex<HashMap<String, InFlightTask>>> = Arc::new(Mutex::new(HashMap::new()));
    let job_index: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let telemetry = telemetry.clone_box();
    runtime.block_on(async move {
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let registry = registry.clone();
            let telemetry = telemetry.clone();
            let in_flight = in_flight.clone();
            let job_index = job_index.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    handle_connection(stream, &registry, telemetry.as_ref(), in_flight, job_index)
                        .await
                {
                    tracing::error!("executor connection error: {err}");
                }
            });
        }
    })
}

async fn handle_connection<S, T>(
    stream: S,
    registry: &Registry,
    telemetry: &T,
    in_flight: Arc<Mutex<HashMap<String, InFlightTask>>>,
    job_index: Arc<Mutex<HashMap<String, String>>>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: Telemetry + ?Sized,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (response_tx, mut response_rx) = mpsc::channel::<ExecutionOutcome>(16);
    let writer_task = tokio::spawn(async move {
        while let Some(outcome) = response_rx.recv().await {
            let response = ExecutorMessage::Response { payload: outcome };
            if write_message(&mut writer, &response).await.is_err() {
                break;
            }
        }
    });
    let connection_requests: Arc<Mutex<std::collections::HashSet<String>>> =
        Arc::new(Mutex::new(std::collections::HashSet::new()));

    loop {
        let message = match read_message(&mut reader).await? {
            Some(message) => message,
            None => break,
        };
        match message {
            ExecutorMessage::Request { payload } => {
                if payload.protocol_version != PROTOCOL_VERSION {
                    let outcome = ExecutionOutcome::error(
                        payload.job_id.clone(),
                        payload.request_id.clone(),
                        "Unsupported protocol version",
                    );
                    let _ = response_tx.send(outcome).await;
                    continue;
                }

                let request_id = payload.request_id.clone();
                let job_id = payload.job_id.clone();
                {
                    let mut active = connection_requests.lock().await;
                    active.insert(request_id.clone());
                }
                let response_tx = response_tx.clone();
                let registry = registry.clone();
                let telemetry = telemetry.clone_box();
                let in_flight_for_task = in_flight.clone();
                let job_index_for_task = job_index.clone();
                let active_for_task = connection_requests.clone();
                let request_id_for_task = request_id.clone();
                let job_id_for_task = job_id.clone();
                let response_tx_for_task = response_tx.clone();

                let handle = tokio::spawn(async move {
                    let outcome =
                        execute_with_deadline(payload, registry, telemetry.as_ref()).await;
                    let _ = response_tx_for_task.send(outcome).await;
                    let mut in_flight = in_flight_for_task.lock().await;
                    in_flight.remove(&request_id_for_task);
                    let mut job_index = job_index_for_task.lock().await;
                    job_index.remove(&job_id_for_task);
                    let mut active = active_for_task.lock().await;
                    active.remove(&request_id_for_task);
                });

                {
                    let mut in_flight = in_flight.lock().await;
                    in_flight.insert(
                        request_id.clone(),
                        InFlightTask {
                            job_id: job_id.clone(),
                            handle,
                            response_tx: response_tx.clone(),
                        },
                    );
                }
                {
                    let mut job_index = job_index.lock().await;
                    job_index.insert(job_id, request_id);
                }
            }
            ExecutorMessage::Cancel { payload } => {
                handle_cancel(payload, &in_flight, &job_index).await;
            }
            ExecutorMessage::Response { .. } => {
                let outcome = ExecutionOutcome {
                    job_id: Some("unknown".to_string()),
                    request_id: None,
                    status: rrq_protocol::OutcomeStatus::Error,
                    result: None,
                    error: Some(ExecutionError {
                        message: "unexpected response message".to_string(),
                        error_type: None,
                        code: None,
                        details: None,
                    }),
                    retry_after_seconds: None,
                };
                let _ = response_tx.send(outcome).await;
            }
        }
    }

    let request_ids = {
        let mut active = connection_requests.lock().await;
        active.drain().collect::<Vec<_>>()
    };
    for request_id in request_ids {
        let task = {
            let mut in_flight = in_flight.lock().await;
            in_flight.remove(&request_id)
        };
        if let Some(task) = task {
            task.handle.abort();
            let mut job_index = job_index.lock().await;
            job_index.remove(&task.job_id);
        }
    }
    writer_task.abort();

    Ok(())
}

struct InFlightTask {
    job_id: String,
    handle: tokio::task::JoinHandle<()>,
    response_tx: mpsc::Sender<ExecutionOutcome>,
}

async fn handle_cancel(
    payload: CancelRequest,
    in_flight: &Arc<Mutex<HashMap<String, InFlightTask>>>,
    job_index: &Arc<Mutex<HashMap<String, String>>>,
) {
    if payload.protocol_version != PROTOCOL_VERSION {
        return;
    }
    let request_id = match payload.request_id.clone() {
        Some(request_id) => Some(request_id),
        None => {
            let job_index = job_index.lock().await;
            job_index.get(&payload.job_id).cloned()
        }
    };
    let Some(request_id) = request_id else {
        return;
    };

    let task = {
        let mut in_flight = in_flight.lock().await;
        in_flight.remove(&request_id)
    };
    if let Some(task) = task {
        task.handle.abort();
        let outcome = ExecutionOutcome {
            job_id: Some(payload.job_id.clone()),
            request_id: Some(request_id.clone()),
            status: OutcomeStatus::Error,
            result: None,
            error: Some(ExecutionError {
                message: "Job cancelled".to_string(),
                error_type: Some("cancelled".to_string()),
                code: None,
                details: None,
            }),
            retry_after_seconds: None,
        };
        let _ = task.response_tx.send(outcome).await;
        let mut job_index = job_index.lock().await;
        job_index.remove(&task.job_id);
    }
}

async fn execute_with_deadline<T: Telemetry + ?Sized>(
    request: rrq_protocol::ExecutionRequest,
    registry: Registry,
    telemetry: &T,
) -> ExecutionOutcome {
    let job_id = request.job_id.clone();
    let request_id = request.request_id.clone();
    let deadline = request.context.deadline;
    if let Some(deadline) = deadline {
        let now: DateTime<Utc> = Utc::now();
        if deadline <= now {
            return ExecutionOutcome::timeout(
                job_id.clone(),
                request_id.clone(),
                "Job deadline exceeded",
            );
        }
        if let Ok(remaining) = (deadline - now).to_std() {
            match tokio::time::timeout(remaining, registry.execute_with(request, telemetry)).await {
                Ok(outcome) => return outcome,
                Err(_) => {
                    return ExecutionOutcome::timeout(
                        job_id.clone(),
                        request_id.clone(),
                        "Job execution timed out",
                    );
                }
            }
        }
        return ExecutionOutcome::timeout(job_id, request_id, "Job deadline exceeded");
    }
    registry.execute_with(request, telemetry).await
}

async fn read_message<R: AsyncRead + Unpin>(
    stream: &mut R,
) -> Result<Option<ExecutorMessage>, Box<dyn std::error::Error>> {
    let mut header = [0u8; 4];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 {
        return Err("executor message payload cannot be empty".into());
    }
    if length > MAX_FRAME_LEN {
        return Err("executor message payload too large".into());
    }
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await?;
    let message = serde_json::from_slice(&payload)?;
    Ok(Some(message))
}

async fn write_message<W: AsyncWrite + Unpin>(
    stream: &mut W,
    message: &ExecutorMessage,
) -> Result<(), Box<dyn std::error::Error>> {
    let framed = encode_frame(message)?;
    stream.write_all(&framed).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Registry;
    use crate::telemetry::NoopTelemetry;
    use chrono::Utc;
    use rrq_protocol::{ExecutionContext, ExecutionRequest, OutcomeStatus};
    use serde_json::json;
    use tokio::net::UnixStream;
    use tokio::time::Duration;

    fn build_request(function_name: &str) -> ExecutionRequest {
        ExecutionRequest {
            protocol_version: PROTOCOL_VERSION.to_string(),
            request_id: "req-1".to_string(),
            job_id: "job-1".to_string(),
            function_name: function_name.to_string(),
            args: vec![],
            kwargs: std::collections::HashMap::new(),
            context: ExecutionContext {
                job_id: "job-1".to_string(),
                attempt: 1,
                enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
                queue_name: "default".to_string(),
                deadline: None,
                trace_context: None,
                worker_id: None,
            },
        }
    }

    #[tokio::test]
    async fn handle_connection_executes_request() {
        let mut registry = Registry::new();
        registry.register("echo", |request| async move {
            ExecutionOutcome::success(
                request.job_id.clone(),
                request.request_id.clone(),
                json!({"ok": true}),
            )
        });
        let (client, server) = UnixStream::pair().unwrap();
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let server_task = tokio::spawn(async move {
            handle_connection(server, &registry, &NoopTelemetry, in_flight, job_index)
                .await
                .unwrap();
        });
        let mut client = client;
        let request = build_request("echo");
        let message = ExecutorMessage::Request { payload: request };
        write_message(&mut client, &message).await.unwrap();
        let response = read_message(&mut client).await.unwrap().unwrap();
        match response {
            ExecutorMessage::Response { payload } => {
                assert_eq!(payload.status, OutcomeStatus::Success);
                assert_eq!(payload.result, Some(json!({"ok": true})));
            }
            _ => panic!("expected response"),
        }
        drop(client);
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn handle_connection_rejects_bad_protocol() {
        let registry = Registry::new();
        let (client, server) = UnixStream::pair().unwrap();
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let server_task = tokio::spawn(async move {
            handle_connection(server, &registry, &NoopTelemetry, in_flight, job_index)
                .await
                .unwrap();
        });
        let mut client = client;
        let mut request = build_request("echo");
        request.protocol_version = "0".to_string();
        let message = ExecutorMessage::Request { payload: request };
        write_message(&mut client, &message).await.unwrap();
        let response = read_message(&mut client).await.unwrap().unwrap();
        match response {
            ExecutorMessage::Response { payload } => {
                assert_eq!(payload.status, OutcomeStatus::Error);
            }
            _ => panic!("expected response"),
        }
        drop(client);
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn handle_connection_cancels_inflight() {
        let mut registry = Registry::new();
        registry.register("sleep", |request| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            ExecutionOutcome::success(
                request.job_id.clone(),
                request.request_id.clone(),
                json!({"ok": true}),
            )
        });
        let (client, server) = UnixStream::pair().unwrap();
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let server_task = tokio::spawn(async move {
            handle_connection(server, &registry, &NoopTelemetry, in_flight, job_index)
                .await
                .unwrap();
        });
        let mut client = client;
        let request = ExecutionRequest {
            protocol_version: PROTOCOL_VERSION.to_string(),
            request_id: "req-cancel".to_string(),
            job_id: "job-cancel".to_string(),
            function_name: "sleep".to_string(),
            args: vec![],
            kwargs: std::collections::HashMap::new(),
            context: ExecutionContext {
                job_id: "job-cancel".to_string(),
                attempt: 1,
                enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
                queue_name: "default".to_string(),
                deadline: None,
                trace_context: None,
                worker_id: None,
            },
        };
        let message = ExecutorMessage::Request {
            payload: request.clone(),
        };
        write_message(&mut client, &message).await.unwrap();
        let cancel = ExecutorMessage::Cancel {
            payload: CancelRequest {
                protocol_version: PROTOCOL_VERSION.to_string(),
                job_id: request.job_id.clone(),
                request_id: Some(request.request_id.clone()),
                hard_kill: false,
            },
        };
        write_message(&mut client, &cancel).await.unwrap();
        let response = read_message(&mut client).await.unwrap().unwrap();
        match response {
            ExecutorMessage::Response { payload } => {
                assert_eq!(payload.status, OutcomeStatus::Error);
                let error_type = payload
                    .error
                    .as_ref()
                    .and_then(|error| error.error_type.as_deref());
                assert_eq!(error_type, Some("cancelled"));
            }
            _ => panic!("expected response"),
        }
        drop(client);
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn execute_with_deadline_times_out() {
        let mut registry = Registry::new();
        registry.register("echo", |request| async move {
            ExecutionOutcome::success(
                request.job_id.clone(),
                request.request_id.clone(),
                json!({"ok": true}),
            )
        });
        let mut request = build_request("echo");
        request.context.deadline = Some(
            "2020-01-01T00:00:00Z"
                .parse::<chrono::DateTime<Utc>>()
                .unwrap(),
        );
        let outcome = execute_with_deadline(request, registry, &NoopTelemetry).await;
        assert_eq!(outcome.status, OutcomeStatus::Timeout);
    }

    #[tokio::test]
    async fn execute_with_deadline_succeeds_before_deadline() {
        let mut registry = Registry::new();
        registry.register("echo", |request| async move {
            ExecutionOutcome::success(
                request.job_id.clone(),
                request.request_id.clone(),
                json!({"ok": true}),
            )
        });
        let mut request = build_request("echo");
        request.context.deadline = Some(Utc::now() + chrono::Duration::seconds(5));
        let outcome = execute_with_deadline(request, registry, &NoopTelemetry).await;
        assert_eq!(outcome.status, OutcomeStatus::Success);
    }

    #[tokio::test]
    async fn handle_connection_handles_unexpected_response_message() {
        let registry = Registry::new();
        let (client, server) = UnixStream::pair().unwrap();
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let server_task = tokio::spawn(async move {
            handle_connection(server, &registry, &NoopTelemetry, in_flight, job_index)
                .await
                .unwrap();
        });
        let mut client = client;
        let response = ExecutorMessage::Response {
            payload: ExecutionOutcome::error("job-x", "req-x", "oops"),
        };
        write_message(&mut client, &response).await.unwrap();
        let reply = read_message(&mut client).await.unwrap().unwrap();
        match reply {
            ExecutorMessage::Response { payload } => {
                assert_eq!(payload.status, OutcomeStatus::Error);
                assert!(
                    payload
                        .error
                        .as_ref()
                        .unwrap()
                        .message
                        .contains("unexpected response")
                );
            }
            _ => panic!("expected response"),
        }
        drop(client);
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn handle_connection_cancels_by_job_id() {
        let mut registry = Registry::new();
        registry.register("sleep", |request| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            ExecutionOutcome::success(
                request.job_id.clone(),
                request.request_id.clone(),
                json!({"ok": true}),
            )
        });
        let (client, server) = UnixStream::pair().unwrap();
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let server_task = tokio::spawn(async move {
            handle_connection(server, &registry, &NoopTelemetry, in_flight, job_index)
                .await
                .unwrap();
        });
        let mut client = client;
        let request = build_request("sleep");
        let message = ExecutorMessage::Request {
            payload: request.clone(),
        };
        write_message(&mut client, &message).await.unwrap();
        let cancel = ExecutorMessage::Cancel {
            payload: CancelRequest {
                protocol_version: PROTOCOL_VERSION.to_string(),
                job_id: request.job_id.clone(),
                request_id: None,
                hard_kill: false,
            },
        };
        write_message(&mut client, &cancel).await.unwrap();
        let response = read_message(&mut client).await.unwrap().unwrap();
        match response {
            ExecutorMessage::Response { payload } => {
                assert_eq!(payload.status, OutcomeStatus::Error);
                let error_type = payload
                    .error
                    .as_ref()
                    .and_then(|error| error.error_type.as_deref());
                assert_eq!(error_type, Some("cancelled"));
            }
            _ => panic!("expected response"),
        }
        drop(client);
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn connection_teardown_clears_tracking_maps() {
        let mut registry = Registry::new();
        registry.register("sleep", |request| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            ExecutionOutcome::success(
                request.job_id.clone(),
                request.request_id.clone(),
                json!({"ok": true}),
            )
        });
        let (client, server) = UnixStream::pair().unwrap();
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let in_flight_for_server = in_flight.clone();
        let job_index_for_server = job_index.clone();
        let server_task = tokio::spawn(async move {
            handle_connection(
                server,
                &registry,
                &NoopTelemetry,
                in_flight_for_server,
                job_index_for_server,
            )
            .await
            .unwrap();
        });
        let mut client = client;
        let request = build_request("sleep");
        let message = ExecutorMessage::Request {
            payload: request.clone(),
        };
        write_message(&mut client, &message).await.unwrap();

        let mut inserted = false;
        for _ in 0..20 {
            let has_in_flight = {
                let guard = in_flight.lock().await;
                guard.contains_key(&request.request_id)
            };
            let has_job_index = {
                let guard = job_index.lock().await;
                guard.contains_key(&request.job_id)
            };
            if has_in_flight && has_job_index {
                inserted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(inserted, "request never entered tracking maps");

        drop(client);
        let _ = server_task.await;

        let in_flight = in_flight.lock().await;
        let job_index = job_index.lock().await;
        assert!(in_flight.is_empty());
        assert!(job_index.is_empty());
    }

    #[tokio::test]
    async fn handle_cancel_ignores_invalid_protocol() {
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let (tx, _rx) = mpsc::channel(1);
        let handle = tokio::spawn(async {});
        {
            let mut guard = in_flight.lock().await;
            guard.insert(
                "req-1".to_string(),
                InFlightTask {
                    job_id: "job-1".to_string(),
                    handle,
                    response_tx: tx,
                },
            );
        }
        let payload = CancelRequest {
            protocol_version: "0".to_string(),
            job_id: "job-1".to_string(),
            request_id: None,
            hard_kill: false,
        };
        handle_cancel(payload, &in_flight, &job_index).await;
        let guard = in_flight.lock().await;
        assert!(guard.contains_key("req-1"));
        guard.get("req-1").unwrap().handle.abort();
    }

    #[tokio::test]
    async fn read_message_handles_empty_and_invalid_payloads() {
        let (mut client, mut server) = tokio::io::duplex(64);
        // length = 0
        client.write_all(&0u32.to_be_bytes()).await.unwrap();
        let err = read_message(&mut server).await.unwrap_err();
        assert!(err.to_string().contains("payload cannot be empty"));

        // invalid json
        let (mut client, mut server) = tokio::io::duplex(64);
        let payload = b"not-json";
        let len = (payload.len() as u32).to_be_bytes();
        client.write_all(&len).await.unwrap();
        client.write_all(payload).await.unwrap();
        let err = read_message(&mut server).await.unwrap_err();
        assert!(err.to_string().contains("expected"));

        // oversized payload
        let (mut client, mut server) = tokio::io::duplex(64);
        let len = ((MAX_FRAME_LEN + 1) as u32).to_be_bytes();
        client.write_all(&len).await.unwrap();
        let err = read_message(&mut server).await.unwrap_err();
        assert!(err.to_string().contains("payload too large"));
    }

    #[tokio::test]
    async fn read_message_returns_none_on_eof() {
        let (client, mut server) = tokio::io::duplex(8);
        drop(client);
        let message = read_message(&mut server).await.unwrap();
        assert!(message.is_none());
    }
}
