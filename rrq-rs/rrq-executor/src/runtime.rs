use crate::registry::Registry;
use crate::telemetry::{NoopTelemetry, Telemetry};
use crate::types::{ExecutionError, ExecutionOutcome};
use chrono::{DateTime, Utc};
use rrq_protocol::{encode_frame, CancelRequest, ExecutorMessage, OutcomeStatus, PROTOCOL_VERSION};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, Mutex};

pub const ENV_EXECUTOR_SOCKET: &str = "RRQ_EXECUTOR_SOCKET";
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

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

async fn handle_connection<T: Telemetry + ?Sized>(
    stream: UnixStream,
    registry: &Registry,
    telemetry: &T,
    in_flight: Arc<Mutex<HashMap<String, InFlightTask>>>,
    job_index: Arc<Mutex<HashMap<String, String>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut reader, mut writer) = stream.into_split();
    let (response_tx, mut response_rx) = mpsc::channel::<ExecutionOutcome>(16);
    let writer_task = tokio::spawn(async move {
        while let Some(outcome) = response_rx.recv().await {
            let response = ExecutorMessage::Response { payload: outcome };
            if write_message(&mut writer, &response).await.is_err() {
                break;
            }
        }
    });
    let mut connection_requests: Vec<String> = Vec::new();

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
                connection_requests.push(request_id.clone());
                let response_tx = response_tx.clone();
                let registry = registry.clone();
                let telemetry = telemetry.clone_box();
                let in_flight_for_task = in_flight.clone();
                let job_index_for_task = job_index.clone();
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

    for request_id in connection_requests {
        let mut in_flight = in_flight.lock().await;
        if let Some(task) = in_flight.remove(&request_id) {
            task.handle.abort();
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
                    )
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
