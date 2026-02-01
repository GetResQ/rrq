use crate::registry::Registry;
use crate::telemetry::{NoopTelemetry, Telemetry};
use crate::types::{ExecutionError, ExecutionOutcome};
use chrono::{DateTime, Utc};
use rrq_protocol::{CancelRequest, OutcomeStatus, PROTOCOL_VERSION, RunnerMessage, encode_frame};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, timeout};

pub const ENV_RUNNER_TCP_SOCKET: &str = "RRQ_RUNNER_TCP_SOCKET";
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
const RESPONSE_CHANNEL_CAPACITY: usize = 64;
const RESPONSE_SEND_TIMEOUT: Duration = Duration::from_secs(1);

fn invalid_input(message: impl Into<String>) -> Box<dyn std::error::Error> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.into(),
    ))
}

pub fn parse_tcp_socket(raw: &str) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(invalid_input("runner tcp_socket cannot be empty"));
    }

    let (host, port_str) = if let Some(rest) = raw.strip_prefix('[') {
        let (host, port_str) = rest
            .split_once("]:")
            .ok_or_else(|| invalid_input("runner tcp_socket must be in [host]:port format"))?;
        (host, port_str)
    } else {
        let (host, port_str) = raw
            .rsplit_once(':')
            .ok_or_else(|| invalid_input("runner tcp_socket must be in host:port format"))?;
        if host.is_empty() {
            return Err(invalid_input("runner tcp_socket host cannot be empty"));
        }
        (host, port_str)
    };

    let port: u16 = port_str
        .parse()
        .map_err(|_| invalid_input(format!("Invalid runner tcp_socket port: {port_str}")))?;
    if port == 0 {
        return Err(invalid_input("runner tcp_socket port must be > 0"));
    }

    let ip = if host == "localhost" {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    } else {
        let parsed: IpAddr = host
            .parse()
            .map_err(|_| invalid_input(format!("Invalid runner tcp_socket host: {host}")))?;
        if !parsed.is_loopback() {
            return Err(invalid_input("runner tcp_socket host must be localhost"));
        }
        parsed
    };

    Ok(SocketAddr::new(ip, port))
}

pub struct RunnerRuntime {
    runtime: tokio::runtime::Runtime,
}

impl RunnerRuntime {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            runtime: tokio::runtime::Runtime::new()?,
        })
    }

    pub fn enter(&self) -> tokio::runtime::EnterGuard<'_> {
        self.runtime.enter()
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

pub fn run_tcp(registry: &Registry, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    RunnerRuntime::new()?.run_tcp(registry, addr)
}

pub fn run_tcp_with<T: Telemetry + ?Sized>(
    registry: &Registry,
    addr: SocketAddr,
    telemetry: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    RunnerRuntime::new()?.run_tcp_with(registry, addr, telemetry)
}

fn run_tcp_loop<T: Telemetry + ?Sized>(
    runtime: &tokio::runtime::Runtime,
    registry: &Registry,
    addr: SocketAddr,
    telemetry: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    let registry = registry.clone();
    let in_flight: Arc<Mutex<HashMap<String, InFlightTask>>> = Arc::new(Mutex::new(HashMap::new()));
    let job_index: Arc<Mutex<HashMap<String, HashSet<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let telemetry = telemetry.clone_box();
    runtime.block_on(async move {
        if !addr.ip().is_loopback() {
            return Err(invalid_input(format!(
                "runner tcp_socket must be loopback-only (got {addr})"
            )));
        }
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
                    tracing::error!("runner connection error: {err}");
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
    job_index: Arc<Mutex<HashMap<String, HashSet<String>>>>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: Telemetry + ?Sized,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (response_tx, mut response_rx) =
        mpsc::channel::<ExecutionOutcome>(RESPONSE_CHANNEL_CAPACITY);
    let writer_task = tokio::spawn(async move {
        while let Some(outcome) = response_rx.recv().await {
            let response = RunnerMessage::Response { payload: outcome };
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
            RunnerMessage::Request { payload } => {
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
                    if active.len() >= RESPONSE_CHANNEL_CAPACITY {
                        let outcome = ExecutionOutcome::error(
                            payload.job_id.clone(),
                            payload.request_id.clone(),
                            "Runner busy: too many in-flight requests",
                        );
                        drop(active);
                        let send_result =
                            timeout(RESPONSE_SEND_TIMEOUT, response_tx.send(outcome)).await;
                        match send_result {
                            Ok(Ok(())) => {}
                            Ok(Err(_)) => {
                                return Err("runner response channel closed".into());
                            }
                            Err(_) => {
                                return Err("runner response channel stalled".into());
                            }
                        }
                        continue;
                    }
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
                let completed = Arc::new(AtomicBool::new(false));
                let completed_for_task = completed.clone();

                let handle = tokio::spawn(async move {
                    let outcome =
                        execute_with_deadline(payload, registry, telemetry.as_ref()).await;
                    completed_for_task.store(true, Ordering::SeqCst);
                    let send_result =
                        timeout(RESPONSE_SEND_TIMEOUT, response_tx_for_task.send(outcome)).await;
                    match send_result {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => {
                            tracing::warn!("runner response channel closed; dropping outcome");
                        }
                        Err(_) => {
                            tracing::warn!("runner response channel stalled; dropping outcome");
                        }
                    }
                    {
                        let mut in_flight = in_flight_for_task.lock().await;
                        in_flight.remove(&request_id_for_task);
                    }
                    {
                        let mut job_index = job_index_for_task.lock().await;
                        if let Some(entries) = job_index.get_mut(&job_id_for_task) {
                            entries.remove(&request_id_for_task);
                            if entries.is_empty() {
                                job_index.remove(&job_id_for_task);
                            }
                        }
                    }
                    {
                        let mut active = active_for_task.lock().await;
                        active.remove(&request_id_for_task);
                    }
                });

                {
                    let mut in_flight = in_flight.lock().await;
                    in_flight.insert(
                        request_id.clone(),
                        InFlightTask {
                            job_id: job_id.clone(),
                            handle,
                            response_tx: response_tx.clone(),
                            connection_requests: connection_requests.clone(),
                            completed,
                        },
                    );
                }
                {
                    let mut job_index = job_index.lock().await;
                    job_index
                        .entry(job_id)
                        .or_insert_with(HashSet::new)
                        .insert(request_id);
                }
            }
            RunnerMessage::Cancel { payload } => {
                handle_cancel(payload, &in_flight, &job_index).await;
            }
            RunnerMessage::Response { .. } => {
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
            if let Some(entries) = job_index.get_mut(&task.job_id) {
                entries.remove(&request_id);
                if entries.is_empty() {
                    job_index.remove(&task.job_id);
                }
            }
        }
    }
    writer_task.abort();

    Ok(())
}

struct InFlightTask {
    job_id: String,
    handle: tokio::task::JoinHandle<()>,
    response_tx: mpsc::Sender<ExecutionOutcome>,
    connection_requests: Arc<Mutex<HashSet<String>>>,
    completed: Arc<AtomicBool>,
}

async fn handle_cancel(
    payload: CancelRequest,
    in_flight: &Arc<Mutex<HashMap<String, InFlightTask>>>,
    job_index: &Arc<Mutex<HashMap<String, HashSet<String>>>>,
) {
    if payload.protocol_version != PROTOCOL_VERSION {
        return;
    }
    let request_ids = if let Some(request_id) = payload.request_id.clone() {
        vec![request_id]
    } else {
        let job_index = job_index.lock().await;
        job_index
            .get(&payload.job_id)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_else(Vec::new)
    };
    if request_ids.is_empty() {
        return;
    }

    for request_id in request_ids {
        let task = {
            let mut in_flight = in_flight.lock().await;
            if let Some(task) = in_flight.get(&request_id)
                && task.completed.load(Ordering::SeqCst)
            {
                None
            } else {
                in_flight.remove(&request_id)
            }
        };
        if let Some(task) = task {
            task.handle.abort();
            {
                let mut active = task.connection_requests.lock().await;
                active.remove(&request_id);
            }
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
            let send_result = timeout(RESPONSE_SEND_TIMEOUT, task.response_tx.send(outcome)).await;
            match send_result {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    tracing::warn!("runner response channel closed; dropping cancel outcome");
                }
                Err(_) => {
                    tracing::warn!("runner response channel stalled; dropping cancel outcome");
                }
            }
            let mut job_index = job_index.lock().await;
            if let Some(entries) = job_index.get_mut(&task.job_id) {
                entries.remove(&request_id);
                if entries.is_empty() {
                    job_index.remove(&task.job_id);
                }
            }
        }
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
) -> Result<Option<RunnerMessage>, Box<dyn std::error::Error>> {
    let mut header = [0u8; 4];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 {
        return Err("runner message payload cannot be empty".into());
    }
    if length > MAX_FRAME_LEN {
        return Err("runner message payload too large".into());
    }
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await?;
    let message = serde_json::from_slice(&payload)?;
    Ok(Some(message))
}

async fn write_message<W: AsyncWrite + Unpin>(
    stream: &mut W,
    message: &RunnerMessage,
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
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::{Duration, timeout};

    fn build_request(function_name: &str) -> ExecutionRequest {
        ExecutionRequest {
            protocol_version: PROTOCOL_VERSION.to_string(),
            request_id: "req-1".to_string(),
            job_id: "job-1".to_string(),
            function_name: function_name.to_string(),
            params: std::collections::HashMap::new(),
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

    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
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
        let (client, server) = tcp_pair().await;
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let server_task = tokio::spawn(async move {
            handle_connection(server, &registry, &NoopTelemetry, in_flight, job_index)
                .await
                .unwrap();
        });
        let mut client = client;
        let request = build_request("echo");
        let message = RunnerMessage::Request { payload: request };
        write_message(&mut client, &message).await.unwrap();
        let response = read_message(&mut client).await.unwrap().unwrap();
        match response {
            RunnerMessage::Response { payload } => {
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
        let (client, server) = tcp_pair().await;
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
        let message = RunnerMessage::Request { payload: request };
        write_message(&mut client, &message).await.unwrap();
        let response = read_message(&mut client).await.unwrap().unwrap();
        match response {
            RunnerMessage::Response { payload } => {
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
        let (client, server) = tcp_pair().await;
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
            params: std::collections::HashMap::new(),
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
        let message = RunnerMessage::Request {
            payload: request.clone(),
        };
        write_message(&mut client, &message).await.unwrap();
        let cancel = RunnerMessage::Cancel {
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
            RunnerMessage::Response { payload } => {
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
    async fn cancel_frees_connection_capacity() {
        let mut registry = Registry::new();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let gate_for_handler = gate.clone();
        registry.register("block", move |request| {
            let gate = gate_for_handler.clone();
            async move {
                let _permit = gate.acquire().await.expect("semaphore closed");
                ExecutionOutcome::success(
                    request.job_id.clone(),
                    request.request_id.clone(),
                    json!({"ok": true}),
                )
            }
        });
        let (client, server) = tcp_pair().await;
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let server_task = tokio::spawn(async move {
            handle_connection(server, &registry, &NoopTelemetry, in_flight, job_index)
                .await
                .unwrap();
        });
        let mut client = client;
        let job_id = "job-capacity".to_string();
        for i in 0..RESPONSE_CHANNEL_CAPACITY {
            let mut request = build_request("block");
            request.request_id = format!("req-{i}");
            request.job_id = job_id.clone();
            request.context.job_id = job_id.clone();
            write_message(&mut client, &RunnerMessage::Request { payload: request })
                .await
                .unwrap();
        }

        let cancel = RunnerMessage::Cancel {
            payload: CancelRequest {
                protocol_version: PROTOCOL_VERSION.to_string(),
                job_id: job_id.clone(),
                request_id: Some("req-0".to_string()),
                hard_kill: false,
            },
        };
        write_message(&mut client, &cancel).await.unwrap();
        let response = timeout(Duration::from_secs(1), read_message(&mut client))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match response {
            RunnerMessage::Response { payload } => {
                assert_eq!(payload.status, OutcomeStatus::Error);
                let error_type = payload
                    .error
                    .as_ref()
                    .and_then(|error| error.error_type.as_deref());
                assert_eq!(error_type, Some("cancelled"));
            }
            _ => panic!("expected response"),
        }

        let mut extra_request = build_request("block");
        extra_request.request_id = "req-extra".to_string();
        extra_request.job_id = job_id.clone();
        extra_request.context.job_id = job_id.clone();
        write_message(
            &mut client,
            &RunnerMessage::Request {
                payload: extra_request,
            },
        )
        .await
        .unwrap();

        gate.add_permits(RESPONSE_CHANNEL_CAPACITY + 1);

        let mut saw_extra = false;
        for _ in 0..RESPONSE_CHANNEL_CAPACITY {
            let response = timeout(Duration::from_secs(1), read_message(&mut client))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if let RunnerMessage::Response { payload } = response
                && payload.request_id.as_deref() == Some("req-extra")
            {
                assert_eq!(payload.status, OutcomeStatus::Success);
                saw_extra = true;
            }
        }
        assert!(saw_extra, "extra request never completed");

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
        let (client, server) = tcp_pair().await;
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let server_task = tokio::spawn(async move {
            handle_connection(server, &registry, &NoopTelemetry, in_flight, job_index)
                .await
                .unwrap();
        });
        let mut client = client;
        let response = RunnerMessage::Response {
            payload: ExecutionOutcome::error("job-x", "req-x", "oops"),
        };
        write_message(&mut client, &response).await.unwrap();
        let reply = read_message(&mut client).await.unwrap().unwrap();
        match reply {
            RunnerMessage::Response { payload } => {
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
        let (client, server) = tcp_pair().await;
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let server_task = tokio::spawn(async move {
            handle_connection(server, &registry, &NoopTelemetry, in_flight, job_index)
                .await
                .unwrap();
        });
        let mut client = client;
        let request = build_request("sleep");
        let message = RunnerMessage::Request {
            payload: request.clone(),
        };
        write_message(&mut client, &message).await.unwrap();
        let cancel = RunnerMessage::Cancel {
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
            RunnerMessage::Response { payload } => {
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
    async fn handle_cancel_by_job_id_cancels_all_requests() {
        let mut registry = Registry::new();
        registry.register("sleep", |request| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            ExecutionOutcome::success(
                request.job_id.clone(),
                request.request_id.clone(),
                json!({"ok": true}),
            )
        });
        let (client, server) = tcp_pair().await;
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let server_task = tokio::spawn(async move {
            handle_connection(server, &registry, &NoopTelemetry, in_flight, job_index)
                .await
                .unwrap();
        });
        let mut client = client;
        let mut request1 = build_request("sleep");
        request1.request_id = "req-1".to_string();
        request1.job_id = "job-shared".to_string();
        let mut request2 = build_request("sleep");
        request2.request_id = "req-2".to_string();
        request2.job_id = "job-shared".to_string();
        write_message(&mut client, &RunnerMessage::Request { payload: request1 })
            .await
            .unwrap();
        write_message(&mut client, &RunnerMessage::Request { payload: request2 })
            .await
            .unwrap();
        let cancel = RunnerMessage::Cancel {
            payload: CancelRequest {
                protocol_version: PROTOCOL_VERSION.to_string(),
                job_id: "job-shared".to_string(),
                request_id: None,
                hard_kill: false,
            },
        };
        write_message(&mut client, &cancel).await.unwrap();

        let mut cancelled = 0;
        for _ in 0..2 {
            let response = timeout(Duration::from_millis(200), read_message(&mut client))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            match response {
                RunnerMessage::Response { payload } => {
                    assert_eq!(payload.status, OutcomeStatus::Error);
                    let error_type = payload
                        .error
                        .as_ref()
                        .and_then(|error| error.error_type.as_deref());
                    assert_eq!(error_type, Some("cancelled"));
                    cancelled += 1;
                }
                _ => panic!("expected response"),
            }
        }
        assert_eq!(cancelled, 2);
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
        let (client, server) = tcp_pair().await;
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
        let message = RunnerMessage::Request {
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
        let connection_requests = Arc::new(Mutex::new(HashSet::new()));
        {
            let mut guard = in_flight.lock().await;
            guard.insert(
                "req-1".to_string(),
                InFlightTask {
                    job_id: "job-1".to_string(),
                    handle,
                    response_tx: tx,
                    connection_requests,
                    completed: Arc::new(AtomicBool::new(false)),
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
    async fn handle_cancel_skips_completed_requests() {
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        let job_index = Arc::new(Mutex::new(HashMap::new()));
        let (tx, mut rx) = mpsc::channel(1);
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        let connection_requests = Arc::new(Mutex::new(HashSet::new()));
        {
            let mut guard = in_flight.lock().await;
            guard.insert(
                "req-1".to_string(),
                InFlightTask {
                    job_id: "job-1".to_string(),
                    handle,
                    response_tx: tx,
                    connection_requests,
                    completed: Arc::new(AtomicBool::new(true)),
                },
            );
        }
        {
            let mut guard = job_index.lock().await;
            guard.insert("job-1".to_string(), HashSet::from(["req-1".to_string()]));
        }
        let payload = CancelRequest {
            protocol_version: PROTOCOL_VERSION.to_string(),
            job_id: "job-1".to_string(),
            request_id: Some("req-1".to_string()),
            hard_kill: false,
        };
        handle_cancel(payload, &in_flight, &job_index).await;
        assert!(in_flight.lock().await.contains_key("req-1"));
        assert!(job_index.lock().await.contains_key("job-1"));
        assert!(rx.try_recv().is_err());
        let task = in_flight.lock().await.remove("req-1").unwrap();
        task.handle.abort();
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
