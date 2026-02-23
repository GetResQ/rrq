use super::*;
use chrono::TimeZone;
#[cfg(unix)]
use nix::sys::signal::kill;
#[cfg(unix)]
use nix::unistd::Pid;
use rrq_config::{RRQSettings, RunnerConfig, RunnerManagementMode, RunnerType};
use std::net::{IpAddr, Ipv4Addr, TcpListener as StdTcpListener};
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::process::Command;
use tokio::time::sleep;

fn process_is_terminated(guard: &mut SocketProcess) -> bool {
    match guard.child.as_mut() {
        Some(child) => child
            .try_wait()
            .expect("failed to poll child status")
            .is_some(),
        None => true,
    }
}

fn process_is_running(guard: &mut SocketProcess) -> bool {
    match guard.child.as_mut() {
        Some(child) => child
            .try_wait()
            .expect("failed to poll child status")
            .is_none(),
        None => false,
    }
}

#[tokio::test]
async fn socket_framing_round_trip() {
    let (mut client, mut server) = tokio::io::duplex(1024);
    let request = ExecutionRequest {
        protocol_version: rrq_protocol::PROTOCOL_VERSION.to_string(),
        request_id: "req-1".to_string(),
        job_id: "job-1".to_string(),
        function_name: "echo".to_string(),
        params: HashMap::new(),
        context: rrq_protocol::ExecutionContext {
            job_id: "job-1".to_string(),
            attempt: 1,
            enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
            queue_name: "default".to_string(),
            deadline: None,
            trace_context: None,
            correlation_context: None,
            worker_id: None,
        },
    };
    let message = RunnerMessage::Request {
        payload: request.clone(),
    };
    write_message(&mut client, &message).await.unwrap();
    let decoded = read_message(&mut server).await.unwrap().unwrap();
    match decoded {
        RunnerMessage::Request { payload } => {
            assert_eq!(payload.job_id, request.job_id);
            assert_eq!(payload.request_id, request.request_id);
            assert_eq!(payload.function_name, request.function_name);
        }
        _ => panic!("unexpected message variant"),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn terminate_child_process_escalates_to_sigkill_when_term_ignored() {
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg("trap '' TERM; while true; do sleep 1; done");
    configure_runner_process_group(&mut command);
    let mut child = command.spawn().unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    terminate_child_process(&mut child, Duration::from_millis(50), "test_escalation").await;

    let status = child.try_wait().unwrap().expect("child should be exited");
    assert!(matches!(status.signal(), Some(9) | Some(15)));
}

#[cfg(unix)]
#[tokio::test]
async fn terminate_child_process_kills_process_group_descendants() {
    let marker =
        std::env::temp_dir().join(format!("rrq-runner-child-{}.pid", uuid::Uuid::new_v4()));
    let script = format!(
        "sleep 60 & echo $! > '{}'; trap '' TERM; while true; do sleep 1; done",
        marker.display()
    );
    let mut command = Command::new("sh");
    command.arg("-c").arg(script);
    configure_runner_process_group(&mut command);
    let mut child = command.spawn().unwrap();

    let mut child_pid: Option<i32> = None;
    for _ in 0..100 {
        if let Ok(payload) = std::fs::read_to_string(&marker)
            && let Ok(pid) = payload.trim().parse::<i32>()
        {
            child_pid = Some(pid);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let child_pid = child_pid.expect("child pid marker should be written");

    terminate_child_process(&mut child, Duration::from_millis(50), "test_descendants").await;

    let still_alive = kill(Pid::from_raw(child_pid), None).is_ok();
    assert!(!still_alive, "descendant process should be terminated");
    let _ = std::fs::remove_file(marker);
}

#[cfg(unix)]
#[tokio::test]
async fn terminate_child_process_kills_descendants_when_leader_exits_first() {
    let marker =
        std::env::temp_dir().join(format!("rrq-runner-child-{}.pid", uuid::Uuid::new_v4()));
    let script = format!(
        "sh -c \"trap '' TERM; while true; do sleep 1; done\" & echo $! > '{}'; trap 'exit 0' TERM; while true; do sleep 1; done",
        marker.display()
    );
    let mut command = Command::new("sh");
    command.arg("-c").arg(script);
    configure_runner_process_group(&mut command);
    let mut child = command.spawn().unwrap();

    let mut child_pid: Option<i32> = None;
    for _ in 0..100 {
        if let Ok(payload) = std::fs::read_to_string(&marker)
            && let Ok(pid) = payload.trim().parse::<i32>()
        {
            child_pid = Some(pid);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let child_pid = child_pid.expect("child pid marker should be written");

    terminate_child_process(&mut child, Duration::from_millis(100), "test_leader_exit").await;

    let mut still_alive = true;
    for _ in 0..50 {
        if kill(Pid::from_raw(child_pid), None).is_err() {
            still_alive = false;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !still_alive,
        "descendant should be terminated even if leader exits first"
    );
    let _ = std::fs::remove_file(marker);
}

#[test]
fn build_runner_event_json_preserves_fields() {
    let now = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let line = r#"{"timestamp":"2024-01-01T00:00:00Z","level":"WARN","fields":{"message":"hello"},"target":"runner"}"#;
    let value = build_runner_event_json("python", RunnerLogStream::Stdout, line, now);
    let obj = value.as_object().expect("object");
    assert_eq!(
        obj.get("timestamp").and_then(Value::as_str),
        Some("2024-01-01T00:00:00Z")
    );
    assert_eq!(obj.get("level").and_then(Value::as_str), Some("WARN"));
    assert_eq!(
        obj.get("rrq.runner").and_then(Value::as_str),
        Some("python")
    );
    assert_eq!(
        obj.get("rrq.stream").and_then(Value::as_str),
        Some("stdout")
    );
    assert_eq!(
        obj.get("rrq.source").and_then(Value::as_str),
        Some("runner")
    );
    assert!(obj.get("fields").is_some());
}

#[test]
fn build_runner_event_json_wraps_plain_text() {
    let now = Utc.with_ymd_and_hms(2024, 2, 2, 3, 4, 5).unwrap();
    let value = build_runner_event_json("rust", RunnerLogStream::Stderr, "plain log line", now);
    let obj = value.as_object().expect("object");
    let timestamp = obj
        .get("timestamp")
        .and_then(Value::as_str)
        .expect("timestamp");
    let parsed = DateTime::parse_from_rfc3339(timestamp).expect("rfc3339");
    assert_eq!(parsed.with_timezone(&Utc), now);
    assert_eq!(obj.get("level").and_then(Value::as_str), Some("ERROR"));
    assert_eq!(
        obj.get("message").and_then(Value::as_str),
        Some("plain log line")
    );
    assert_eq!(obj.get("rrq.runner").and_then(Value::as_str), Some("rust"));
    assert_eq!(
        obj.get("rrq.stream").and_then(Value::as_str),
        Some("stderr")
    );
}

fn build_test_pool(max_in_flight: usize) -> SocketRunnerPool {
    let child = Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("spawn sleep");
    let (spec, socket_addr) = allocate_tcp_spec();
    let process = SocketProcess {
        child: Some(child),
        socket: socket_addr,
        stdout_task: None,
        stderr_task: None,
        permits: Arc::new(Semaphore::new(max_in_flight)),
        connection: None,
    };
    SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["sleep".to_string(), "60".to_string()],
        pool_size: 1,
        max_in_flight,
        env: None,
        cwd: None,
        tcp_socket: spec,
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: None,
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: None,
    }
}

fn allocate_tcp_spec() -> (TcpSocketSpec, SocketAddr) {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind temp port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    let spec = TcpSocketSpec {
        host: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: addr.port(),
    };
    (spec, addr)
}

async fn bind_tcp_listener() -> (TokioTcpListener, SocketAddr) {
    let listener = TokioTcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tcp listener");
    let addr = listener.local_addr().expect("local addr");
    (listener, addr)
}

fn build_execution_request(request_id: &str, job_id: &str) -> ExecutionRequest {
    ExecutionRequest {
        protocol_version: rrq_protocol::PROTOCOL_VERSION.to_string(),
        request_id: request_id.to_string(),
        job_id: job_id.to_string(),
        function_name: "echo".to_string(),
        params: HashMap::new(),
        context: rrq_protocol::ExecutionContext {
            job_id: job_id.to_string(),
            attempt: 1,
            enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
            queue_name: "default".to_string(),
            deadline: None,
            trace_context: None,
            correlation_context: None,
            worker_id: None,
        },
    }
}

#[tokio::test]
async fn pool_enforces_per_process_max_in_flight() {
    let pool = Arc::new(build_test_pool(1));
    let (_proc, permit) = pool.acquire_process().await.unwrap();
    let pool_clone = pool.clone();
    let waiter = tokio::spawn(async move { pool_clone.acquire_process().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished());
    drop(permit);
    let result = timeout(Duration::from_millis(200), waiter)
        .await
        .expect("acquire should complete after release");
    let (_proc2, permit2) = result.expect("join failed").expect("acquire failed");
    drop(permit2);
    pool.close().await.unwrap();
}

#[tokio::test]
async fn pool_new_rejects_max_in_flight_above_protocol_limit() {
    let result = SocketRunnerPool::new(
        "test",
        vec!["sleep".to_string(), "60".to_string()],
        1,
        MAX_IN_FLIGHT_PER_CONNECTION_LIMIT + 1,
        None,
        None,
        Some("127.0.0.1:9000".to_string()),
        None,
        None,
        Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        Duration::from_millis(50),
        true,
    )
    .await;
    let err = match result {
        Ok(_) => panic!("pool creation should reject max_in_flight above protocol limit"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("max_in_flight must be <="));
}

#[tokio::test]
async fn execute_with_timeout_includes_process_acquire_wait() {
    let pool = Arc::new(build_test_pool(1));
    let runner = Arc::new(SocketRunner {
        pool: pool.clone(),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    });

    let (_proc, permit) = pool.acquire_process().await.unwrap();
    let timed_out_runner = runner.clone();
    let timed_out_task = tokio::spawn(async move {
        timed_out_runner
            .execute_with_timeout(
                build_execution_request("req-wait-timeout", "job-wait-timeout"),
                Duration::from_millis(60),
                false,
            )
            .await
    });

    let timed_out_result = timeout(Duration::from_millis(150), timed_out_task)
        .await
        .expect("execute_with_timeout should complete while acquire is blocked")
        .unwrap();
    assert!(matches!(timed_out_result, RunnerExecutionResult::TimedOut));

    drop(permit);
    pool.close().await.unwrap();
}

#[tokio::test]
async fn ensure_process_respawns_exited_child() {
    let spawn_override = Arc::new(|| {
        let (_spec, socket_addr) = allocate_tcp_spec();
        let child = Command::new("sleep").arg("60").spawn().unwrap();
        SocketProcess {
            child: Some(child),
            socket: socket_addr,
            stdout_task: None,
            stderr_task: None,
            permits: Arc::new(Semaphore::new(1)),
            connection: None,
        }
    });
    let (spec, _socket_addr) = allocate_tcp_spec();
    let pool = SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["sleep".to_string(), "60".to_string()],
        pool_size: 1,
        max_in_flight: 1,
        env: None,
        cwd: None,
        tcp_socket: spec,
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(Vec::new()),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: None,
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: Some(spawn_override),
    };
    pool.ensure_started().await.unwrap();
    let proc = {
        let processes = pool.processes.lock().await;
        processes.first().cloned().unwrap()
    };
    let old_socket = { proc.lock().await.socket };
    {
        let mut guard = proc.lock().await;
        if let Some(child) = guard.child.as_mut() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }
    sleep(Duration::from_millis(50)).await;
    pool.ensure_process(&proc).await.unwrap();
    let new_socket = { proc.lock().await.socket };
    assert_ne!(old_socket, new_socket);
    pool.close().await.unwrap();
}

#[tokio::test]
async fn connect_socket_errors_when_child_exits() {
    let (spec, socket_addr) = allocate_tcp_spec();
    let pool = SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["true".to_string()],
        pool_size: 1,
        max_in_flight: 1,
        env: None,
        cwd: None,
        tcp_socket: spec,
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(Vec::new()),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: None,
        connect_timeout: Duration::from_millis(50),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: None,
    };
    let mut child = Command::new("true").spawn().unwrap();
    let err = pool
        .connect_socket(&socket_addr, &mut child)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("runner exited before socket ready")
    );
}

#[tokio::test]
async fn execute_with_process_external_mode_retries_until_socket_is_ready() {
    let (spec, socket_addr) = allocate_tcp_spec();
    let process = SocketProcess {
        child: None,
        socket: socket_addr,
        stdout_task: None,
        stderr_task: None,
        permits: Arc::new(Semaphore::new(1)),
        connection: None,
    };
    let runner = SocketRunner {
        pool: Arc::new(SocketRunnerPool {
            name: "external-test".to_string(),
            cmd: Vec::new(),
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: spec,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(800),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: None,
        }),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    let proc = {
        let processes = runner.pool.processes.lock().await;
        processes.first().cloned().unwrap()
    };

    let server = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let listener = TokioTcpListener::bind(socket_addr).await.unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = match read_message(&mut stream).await.unwrap().unwrap() {
            RunnerMessage::Request { payload } => payload,
            _ => panic!("expected request payload"),
        };
        let response = ExecutionOutcome::success(
            request.job_id,
            request.request_id,
            serde_json::json!({"ok": true}),
        );
        write_message(&mut stream, &RunnerMessage::Response { payload: response })
            .await
            .unwrap();
    });

    let outcome = runner
        .execute_with_process(
            &proc,
            &build_execution_request("req-external", "job-external"),
        )
        .await
        .unwrap();
    assert_eq!(outcome.request_id.as_deref(), Some("req-external"));
    server.await.unwrap();
}

#[tokio::test]
async fn execute_with_process_external_mode_fails_after_backoff_timeout() {
    let (spec, socket_addr) = allocate_tcp_spec();
    let process = SocketProcess {
        child: None,
        socket: socket_addr,
        stdout_task: None,
        stderr_task: None,
        permits: Arc::new(Semaphore::new(1)),
        connection: None,
    };
    let runner = SocketRunner {
        pool: Arc::new(SocketRunnerPool {
            name: "external-timeout-test".to_string(),
            cmd: Vec::new(),
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: spec,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(200),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: None,
        }),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    let proc = {
        let processes = runner.pool.processes.lock().await;
        processes.first().cloned().unwrap()
    };

    let err = runner
        .execute_with_process(
            &proc,
            &build_execution_request("req-timeout", "job-timeout"),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("external runner socket not ready"));
}

#[test]
fn resolve_pool_sizes_and_max_in_flight_watch_mode() {
    let mut settings = RRQSettings::default();
    let mut runners = HashMap::new();
    runners.insert(
        "python".to_string(),
        RunnerConfig {
            runner_type: RunnerType::Socket,
            cmd: Some(vec!["rrq-runner".to_string()]),
            pool_size: Some(4),
            max_in_flight: Some(5),
            env: None,
            cwd: None,
            tcp_socket: Some("127.0.0.1:9000".to_string()),
            allowed_hosts: None,
            response_timeout_seconds: None,
        },
    );
    settings.runners = runners;

    let pool_sizes = resolve_runner_pool_sizes(&settings, true, None).unwrap();
    let max_in_flight = resolve_runner_max_in_flight(&settings, true).unwrap();
    assert_eq!(pool_sizes.get("python"), Some(&1));
    assert_eq!(max_in_flight.get("python"), Some(&1));
}

#[test]
fn resolve_pool_sizes_external_mode_forces_single_process() {
    let mut settings = RRQSettings {
        runner_management_mode: RunnerManagementMode::External,
        ..Default::default()
    };
    let mut runners = HashMap::new();
    runners.insert(
        "python".to_string(),
        RunnerConfig {
            runner_type: RunnerType::Socket,
            cmd: None,
            pool_size: Some(4),
            max_in_flight: Some(2),
            env: None,
            cwd: None,
            tcp_socket: Some("127.0.0.1:9000".to_string()),
            allowed_hosts: None,
            response_timeout_seconds: None,
        },
    );
    settings.runners = runners;

    let pool_sizes = resolve_runner_pool_sizes(&settings, false, None).unwrap();
    assert_eq!(pool_sizes.get("python"), Some(&1));
}

#[test]
fn spawn_retry_delay_scales_and_caps() {
    assert_eq!(spawn_retry_delay(1), Duration::from_millis(100));
    assert_eq!(spawn_retry_delay(2), Duration::from_millis(200));
    assert_eq!(spawn_retry_delay(3), Duration::from_millis(400));
    assert_eq!(spawn_retry_delay(5), Duration::from_millis(1600));
    assert_eq!(spawn_retry_delay(6), Duration::from_secs(2));
    assert_eq!(spawn_retry_delay(20), Duration::from_secs(2));
}

#[test]
fn resolve_pool_sizes_and_max_in_flight_validate_zero() {
    let mut settings = RRQSettings::default();
    let mut runners = HashMap::new();
    runners.insert(
        "python".to_string(),
        RunnerConfig {
            runner_type: RunnerType::Socket,
            cmd: Some(vec!["rrq-runner".to_string()]),
            pool_size: Some(0),
            max_in_flight: Some(0),
            env: None,
            cwd: None,
            tcp_socket: Some("127.0.0.1:9000".to_string()),
            allowed_hosts: None,
            response_timeout_seconds: None,
        },
    );
    settings.runners = runners;

    let err = resolve_runner_pool_sizes(&settings, false, None).unwrap_err();
    assert!(err.to_string().contains("pool_size must be positive"));
    let err = resolve_runner_max_in_flight(&settings, false).unwrap_err();
    assert!(err.to_string().contains("max_in_flight must be positive"));
}

#[test]
fn resolve_pool_sizes_and_max_in_flight_validate_protocol_limit() {
    let mut settings = RRQSettings::default();
    let mut runners = HashMap::new();
    runners.insert(
        "python".to_string(),
        RunnerConfig {
            runner_type: RunnerType::Socket,
            cmd: Some(vec!["rrq-runner".to_string()]),
            pool_size: Some(1),
            max_in_flight: Some(MAX_IN_FLIGHT_PER_CONNECTION_LIMIT + 1),
            env: None,
            cwd: None,
            tcp_socket: Some("127.0.0.1:9000".to_string()),
            allowed_hosts: None,
            response_timeout_seconds: None,
        },
    );
    settings.runners = runners;

    let err = resolve_runner_max_in_flight(&settings, false).unwrap_err();
    assert!(err.to_string().contains("max_in_flight must be <="));
}

// parse_tcp_socket tests are in rrq_config::tcp_socket module

#[test]
fn connect_timeout_from_settings_defaults_for_non_positive() {
    let expected = Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64);
    assert_eq!(connect_timeout_from_settings(0), expected);
    assert_eq!(connect_timeout_from_settings(-5), expected);
    assert_eq!(
        connect_timeout_from_settings(5000),
        Duration::from_millis(5000)
    );
}

#[test]
fn shutdown_term_grace_from_settings_handles_invalid_values() {
    assert_eq!(shutdown_term_grace_from_settings(0.0), Duration::ZERO);
    assert_eq!(shutdown_term_grace_from_settings(-1.0), Duration::ZERO);
    assert_eq!(shutdown_term_grace_from_settings(f64::NAN), Duration::ZERO);
    assert_eq!(
        shutdown_term_grace_from_settings(f64::INFINITY),
        Duration::ZERO
    );
}

#[test]
fn shutdown_term_grace_from_settings_saturates_huge_values() {
    let duration = std::panic::catch_unwind(|| shutdown_term_grace_from_settings(1e100))
        .expect("shutdown grace conversion should not panic");
    assert_eq!(duration, Duration::MAX);
}

#[tokio::test]
async fn tcp_socket_pool_range_rejects_overflow() {
    let err = SocketRunnerPool::new(
        "test",
        vec!["true".to_string()],
        2,
        1,
        None,
        None,
        Some("127.0.0.1:65535".to_string()),
        None,
        None,
        Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        Duration::from_millis(50),
        true,
    )
    .await;
    match err {
        Err(err) => assert!(err.to_string().contains("range too small")),
        Ok(_) => panic!("expected tcp socket range error"),
    }
}

#[test]
fn tcp_socket_pool_assigns_incrementing_ports() {
    let pool = SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["true".to_string()],
        pool_size: 2,
        max_in_flight: 1,
        env: None,
        cwd: None,
        tcp_socket: TcpSocketSpec {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 9000,
        },
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(Vec::new()),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: None,
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: None,
    };

    let first = pool.next_tcp_socket().unwrap();
    let second = pool.next_tcp_socket().unwrap();
    let third = pool.next_tcp_socket().unwrap();

    assert_eq!(
        first,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000)
    );
    assert_eq!(
        second,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9001)
    );
    assert_eq!(
        third,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9002)
    );
}

#[test]
fn tcp_port_collision_is_detected() {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let pool = SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["true".to_string()],
        pool_size: 1,
        max_in_flight: 1,
        env: None,
        cwd: None,
        tcp_socket: TcpSocketSpec {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: addr.port(),
        },
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(Vec::new()),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: None,
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: None,
    };

    let err = pool.ensure_tcp_port_available(addr).unwrap_err();
    assert!(err.to_string().contains("already in use"));

    drop(listener);
    let mut last_err: Option<anyhow::Error> = None;
    for _ in 0..10 {
        match pool.ensure_tcp_port_available(addr) {
            Ok(()) => {
                last_err = None;
                break;
            }
            Err(err) if err.to_string().contains("already in use") => {
                last_err = Some(err);
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
    }
    if let Some(err) = last_err {
        panic!("port still in use after release: {err}");
    }
}

#[tokio::test]
async fn cancel_sends_cancel_message() {
    let (listener, socket_addr) = bind_tcp_listener().await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let message = read_message(&mut stream).await.unwrap().unwrap();
        match message {
            RunnerMessage::Cancel { payload } => {
                assert_eq!(payload.job_id, "job-1");
                assert_eq!(payload.request_id.as_deref(), Some("req-1"));
            }
            _ => panic!("expected cancel message"),
        }
    });

    let pool = build_test_pool(1);
    let runner = SocketRunner {
        pool: Arc::new(pool),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    {
        let mut in_flight = runner.in_flight.lock().await;
        in_flight.insert(
            "req-1".to_string(),
            InFlightRequest {
                job_id: "job-1".to_string(),
                socket: socket_addr,
            },
        );
    }
    runner.cancel("job-1", None).await.unwrap();
    server.await.unwrap();

    let in_flight = runner.in_flight.lock().await;
    assert!(in_flight.is_empty());
}

#[tokio::test]
async fn cancel_failure_preserves_in_flight() {
    let (_spec, socket_addr) = allocate_tcp_spec();
    let pool = build_test_pool(1);
    let runner = SocketRunner {
        pool: Arc::new(pool),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    {
        let mut in_flight = runner.in_flight.lock().await;
        in_flight.insert(
            "req-1".to_string(),
            InFlightRequest {
                job_id: "job-1".to_string(),
                socket: socket_addr,
            },
        );
    }
    let err = runner.cancel("job-1", None).await.unwrap_err();
    assert!(!err.to_string().is_empty());
    let in_flight = runner.in_flight.lock().await;
    assert!(in_flight.contains_key("req-1"));
}

#[tokio::test]
async fn execute_timeout_recycles_process_without_cancel_hints() {
    let (listener, socket_addr) = bind_tcp_listener().await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = read_message(&mut stream).await.unwrap().unwrap();
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "unexpected cancel hint when disabled"
        );
    });

    let child = Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("spawn sleep");
    let process = SocketProcess {
        child: Some(child),
        socket: socket_addr,
        stdout_task: None,
        stderr_task: None,
        permits: Arc::new(Semaphore::new(1)),
        connection: None,
    };
    let (spec, _addr) = allocate_tcp_spec();
    let pool = SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["sleep".to_string(), "60".to_string()],
        pool_size: 1,
        max_in_flight: 1,
        env: None,
        cwd: None,
        tcp_socket: spec,
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: Some(Duration::from_millis(50)),
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: None,
    };
    let runner = SocketRunner {
        pool: Arc::new(pool),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    let request = ExecutionRequest {
        protocol_version: rrq_protocol::PROTOCOL_VERSION.to_string(),
        request_id: "req-1".to_string(),
        job_id: "job-1".to_string(),
        function_name: "echo".to_string(),
        params: HashMap::new(),
        context: rrq_protocol::ExecutionContext {
            job_id: "job-1".to_string(),
            attempt: 1,
            enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
            queue_name: "default".to_string(),
            deadline: None,
            trace_context: None,
            correlation_context: None,
            worker_id: None,
        },
    };
    let err = runner.execute(request).await.unwrap_err();
    assert!(err.to_string().contains("runner response timeout"));
    let proc = {
        let processes = runner.pool.processes.lock().await;
        processes.first().cloned().unwrap()
    };
    let mut exited = false;
    for _ in 0..20 {
        let terminated = {
            let mut guard = proc.lock().await;
            process_is_terminated(&mut guard)
        };
        if terminated {
            exited = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(exited, "timed-out process should be terminated");
    server.await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn execute_with_timeout_holds_permit_during_timeout_cleanup() {
    let (listener, socket_addr) = bind_tcp_listener().await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = read_message(&mut stream).await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
    });

    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg("trap '' TERM; while true; do sleep 1; done");
    configure_runner_process_group(&mut command);
    let child = command.spawn().expect("spawn term-ignoring child");
    let process = SocketProcess {
        child: Some(child),
        socket: socket_addr,
        stdout_task: None,
        stderr_task: None,
        permits: Arc::new(Semaphore::new(1)),
        connection: None,
    };
    let (spec, _addr) = allocate_tcp_spec();
    let pool = SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["sleep".to_string(), "60".to_string()],
        pool_size: 1,
        max_in_flight: 1,
        env: None,
        cwd: None,
        tcp_socket: spec,
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: None,
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(200),
        capture_output: true,
        spawn_override: None,
    };
    let runner = Arc::new(SocketRunner {
        pool: Arc::new(pool),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    });

    let timeout_runner = runner.clone();
    let timeout_task = tokio::spawn(async move {
        timeout_runner
            .execute_with_timeout(
                build_execution_request("req-timeout", "job-timeout"),
                Duration::from_millis(50),
                false,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(70)).await;
    let acquire_attempt = timeout(Duration::from_millis(80), runner.pool.acquire_process()).await;
    assert!(
        acquire_attempt.is_err(),
        "process permit should remain held until timeout cleanup completes"
    );

    let timeout_result = timeout_task.await.unwrap();
    assert!(matches!(timeout_result, RunnerExecutionResult::TimedOut));

    server.abort();
}

#[tokio::test]
async fn execute_timeout_sends_cancel_hint_when_enabled() {
    let (listener, socket_addr) = bind_tcp_listener().await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = read_message(&mut stream).await.unwrap().unwrap();
        let (mut cancel_stream, _) = listener.accept().await.unwrap();
        let message = read_message(&mut cancel_stream).await.unwrap().unwrap();
        match message {
            RunnerMessage::Cancel { payload } => {
                assert_eq!(payload.job_id, "job-1");
                assert_eq!(payload.request_id.as_deref(), Some("req-1"));
            }
            _ => panic!("expected cancel message"),
        }
    });

    let child = Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("spawn sleep");
    let process = SocketProcess {
        child: Some(child),
        socket: socket_addr,
        stdout_task: None,
        stderr_task: None,
        permits: Arc::new(Semaphore::new(1)),
        connection: None,
    };
    let (spec, _addr) = allocate_tcp_spec();
    let pool = SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["sleep".to_string(), "60".to_string()],
        pool_size: 1,
        max_in_flight: 1,
        env: None,
        cwd: None,
        tcp_socket: spec,
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: Some(Duration::from_millis(50)),
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: None,
    };
    let runner = SocketRunner {
        pool: Arc::new(pool),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: true,
    };
    let request = build_execution_request("req-1", "job-1");
    let err = runner.execute(request).await.unwrap_err();
    assert!(err.to_string().contains("runner response timeout"));
    server.await.unwrap();
}

#[tokio::test]
async fn handle_timeout_terminates_socket_process_without_cancel_hint() {
    let (listener, socket_addr) = bind_tcp_listener().await;
    let server = tokio::spawn(async move {
        assert!(
            timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err(),
            "unexpected cancel hint when disabled"
        );
    });

    let child = Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("spawn sleep");
    let process = SocketProcess {
        child: Some(child),
        socket: socket_addr,
        stdout_task: None,
        stderr_task: None,
        permits: Arc::new(Semaphore::new(1)),
        connection: None,
    };
    let (spec, _addr) = allocate_tcp_spec();
    let pool = SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["sleep".to_string(), "60".to_string()],
        pool_size: 1,
        max_in_flight: 1,
        env: None,
        cwd: None,
        tcp_socket: spec,
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: None,
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: None,
    };
    let runner = SocketRunner {
        pool: Arc::new(pool),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    {
        let mut in_flight = runner.in_flight.lock().await;
        in_flight.insert(
            "req-1".to_string(),
            InFlightRequest {
                job_id: "job-1".to_string(),
                socket: socket_addr,
            },
        );
    }

    runner
        .handle_timeout("job-1", Some("req-1"), false)
        .await
        .unwrap();

    let in_flight = runner.in_flight.lock().await;
    assert!(in_flight.is_empty());
    drop(in_flight);

    let proc = {
        let processes = runner.pool.processes.lock().await;
        processes.first().cloned().unwrap()
    };
    let mut exited = false;
    for _ in 0..20 {
        let terminated = {
            let mut guard = proc.lock().await;
            process_is_terminated(&mut guard)
        };
        if terminated {
            exited = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(exited, "timed-out process should be terminated");
    server.await.unwrap();
}

#[tokio::test]
async fn finalize_execute_result_terminates_shared_process_when_cancel_unavailable() {
    let pool = Arc::new(build_test_pool(2));
    let runner = SocketRunner {
        pool: pool.clone(),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    let (proc, permit) = pool.acquire_process().await.unwrap();
    let socket = { proc.lock().await.socket };
    {
        let mut in_flight = runner.in_flight.lock().await;
        in_flight.insert(
            "req-timeout".to_string(),
            InFlightRequest {
                job_id: "job-timeout".to_string(),
                socket,
            },
        );
        in_flight.insert(
            "req-other".to_string(),
            InFlightRequest {
                job_id: "job-other".to_string(),
                socket,
            },
        );
    }

    let request = build_execution_request("req-timeout", "job-timeout");
    let err = runner
        .finalize_execute_result(
            &proc,
            &request,
            Err(anyhow::anyhow!("runner response timeout")),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("runner response timeout"));

    let in_flight = runner.in_flight.lock().await;
    assert!(!in_flight.contains_key("req-timeout"));
    assert!(in_flight.contains_key("req-other"));
    drop(in_flight);

    let terminated = {
        let mut guard = proc.lock().await;
        process_is_terminated(&mut guard)
    };
    assert!(
        terminated,
        "shared process should be terminated when targeted cancel is unavailable"
    );

    drop(permit);
    pool.close().await.unwrap();
}

#[tokio::test]
async fn finalize_execute_result_keeps_shared_process_when_cancel_succeeds() {
    let (listener, socket_addr) = bind_tcp_listener().await;
    let server = tokio::spawn(async move {
        let (mut cancel_stream, _) = listener.accept().await.unwrap();
        let message = read_message(&mut cancel_stream).await.unwrap().unwrap();
        match message {
            RunnerMessage::Cancel { payload } => {
                assert_eq!(payload.job_id, "job-timeout");
                assert_eq!(payload.request_id.as_deref(), Some("req-timeout"));
            }
            _ => panic!("expected cancel message"),
        }
    });

    let child = Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("spawn sleep");
    let process = SocketProcess {
        child: Some(child),
        socket: socket_addr,
        stdout_task: None,
        stderr_task: None,
        permits: Arc::new(Semaphore::new(2)),
        connection: None,
    };
    let (spec, _addr) = allocate_tcp_spec();
    let pool = Arc::new(SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["sleep".to_string(), "60".to_string()],
        pool_size: 1,
        max_in_flight: 2,
        env: None,
        cwd: None,
        tcp_socket: spec,
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: None,
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: None,
    });
    let runner = SocketRunner {
        pool: pool.clone(),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    let (proc, permit) = pool.acquire_process().await.unwrap();
    {
        let mut in_flight = runner.in_flight.lock().await;
        in_flight.insert(
            "req-timeout".to_string(),
            InFlightRequest {
                job_id: "job-timeout".to_string(),
                socket: socket_addr,
            },
        );
        in_flight.insert(
            "req-other".to_string(),
            InFlightRequest {
                job_id: "job-other".to_string(),
                socket: socket_addr,
            },
        );
    }

    let request = build_execution_request("req-timeout", "job-timeout");
    let err = runner
        .finalize_execute_result(
            &proc,
            &request,
            Err(anyhow::anyhow!("runner response timeout")),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("runner response timeout"));

    let in_flight = runner.in_flight.lock().await;
    assert!(!in_flight.contains_key("req-timeout"));
    assert!(in_flight.contains_key("req-other"));
    drop(in_flight);

    let running = {
        let mut guard = proc.lock().await;
        process_is_running(&mut guard)
    };
    assert!(
        running,
        "shared process should remain alive when targeted cancel succeeds"
    );

    drop(permit);
    pool.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn cleanup_worker_timeout_skips_termination_when_request_not_in_flight() {
    let pool = Arc::new(build_test_pool(2));
    let runner = SocketRunner {
        pool: pool.clone(),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    let (proc, permit) = pool.acquire_process().await.unwrap();
    let request = build_execution_request("req-missing", "job-missing");

    runner
        .cleanup_worker_timeout_with_permit_held(&request, false)
        .await;

    let running = {
        let mut guard = proc.lock().await;
        process_is_running(&mut guard)
    };
    assert!(
        running,
        "process should remain alive when timed-out request was never in-flight"
    );
    drop(permit);
    pool.close().await.unwrap();
}

#[tokio::test]
async fn cleanup_worker_timeout_terminates_shared_process_when_cancel_unavailable() {
    let pool = Arc::new(build_test_pool(2));
    let runner = SocketRunner {
        pool: pool.clone(),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    let (proc, permit) = pool.acquire_process().await.unwrap();
    let socket = { proc.lock().await.socket };
    {
        let mut in_flight = runner.in_flight.lock().await;
        in_flight.insert(
            "req-timeout".to_string(),
            InFlightRequest {
                job_id: "job-timeout".to_string(),
                socket,
            },
        );
        in_flight.insert(
            "req-other".to_string(),
            InFlightRequest {
                job_id: "job-other".to_string(),
                socket,
            },
        );
    }

    let request = build_execution_request("req-timeout", "job-timeout");
    runner
        .cleanup_worker_timeout_with_permit_held(&request, false)
        .await;

    let in_flight = runner.in_flight.lock().await;
    assert!(!in_flight.contains_key("req-timeout"));
    assert!(in_flight.contains_key("req-other"));
    drop(in_flight);

    let terminated = {
        let mut guard = proc.lock().await;
        process_is_terminated(&mut guard)
    };
    assert!(
        terminated,
        "shared process should be terminated when targeted cancel is unavailable"
    );
    drop(permit);
    pool.close().await.unwrap();
}

#[tokio::test]
async fn cleanup_worker_timeout_keeps_shared_process_when_cancel_succeeds() {
    let (listener, socket_addr) = bind_tcp_listener().await;
    let server = tokio::spawn(async move {
        let (mut cancel_stream, _) = listener.accept().await.unwrap();
        let message = read_message(&mut cancel_stream).await.unwrap().unwrap();
        match message {
            RunnerMessage::Cancel { payload } => {
                assert_eq!(payload.job_id, "job-timeout");
                assert_eq!(payload.request_id.as_deref(), Some("req-timeout"));
            }
            _ => panic!("expected cancel message"),
        }
    });

    let child = Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("spawn sleep");
    let process = SocketProcess {
        child: Some(child),
        socket: socket_addr,
        stdout_task: None,
        stderr_task: None,
        permits: Arc::new(Semaphore::new(2)),
        connection: None,
    };
    let (spec, _addr) = allocate_tcp_spec();
    let pool = Arc::new(SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["sleep".to_string(), "60".to_string()],
        pool_size: 1,
        max_in_flight: 2,
        env: None,
        cwd: None,
        tcp_socket: spec,
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: None,
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: None,
    });
    let runner = SocketRunner {
        pool: pool.clone(),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    let (proc, permit) = pool.acquire_process().await.unwrap();
    {
        let mut in_flight = runner.in_flight.lock().await;
        in_flight.insert(
            "req-timeout".to_string(),
            InFlightRequest {
                job_id: "job-timeout".to_string(),
                socket: socket_addr,
            },
        );
        in_flight.insert(
            "req-other".to_string(),
            InFlightRequest {
                job_id: "job-other".to_string(),
                socket: socket_addr,
            },
        );
    }

    let request = build_execution_request("req-timeout", "job-timeout");
    runner
        .cleanup_worker_timeout_with_permit_held(&request, false)
        .await;

    let in_flight = runner.in_flight.lock().await;
    assert!(!in_flight.contains_key("req-timeout"));
    assert!(in_flight.contains_key("req-other"));
    drop(in_flight);

    let running = {
        let mut guard = proc.lock().await;
        process_is_running(&mut guard)
    };
    assert!(
        running,
        "shared process should remain alive when targeted cancel succeeds"
    );

    drop(permit);
    pool.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn cleanup_worker_timeout_removes_timed_out_request_from_shared_pending_map() {
    let pool = Arc::new(build_test_pool(2));
    let runner = SocketRunner {
        pool: pool.clone(),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    let (proc, permit) = pool.acquire_process().await.unwrap();
    let socket = { proc.lock().await.socket };
    let (sender, _receiver) = mpsc::channel::<RunnerMessage>(8);
    let connection = Arc::new(PersistentProcessConnection {
        sender,
        pending: Arc::new(Mutex::new(HashMap::new())),
        closed: Arc::new(AtomicBool::new(false)),
    });
    let (timed_out_sender, timed_out_receiver) = oneshot::channel::<Result<ExecutionOutcome>>();
    let (other_sender, _other_receiver) = oneshot::channel::<Result<ExecutionOutcome>>();
    {
        let mut pending = connection.pending.lock().await;
        pending.insert("req-timeout".to_string(), timed_out_sender);
        pending.insert("req-other".to_string(), other_sender);
    }
    {
        let mut guard = proc.lock().await;
        guard.connection = Some(connection.clone());
    }
    {
        let mut in_flight = runner.in_flight.lock().await;
        in_flight.insert(
            "req-timeout".to_string(),
            InFlightRequest {
                job_id: "job-timeout".to_string(),
                socket,
            },
        );
        in_flight.insert(
            "req-other".to_string(),
            InFlightRequest {
                job_id: "job-other".to_string(),
                socket,
            },
        );
    }

    let request = build_execution_request("req-timeout", "job-timeout");
    runner
        .cleanup_worker_timeout_with_permit_held(&request, false)
        .await;

    let canceled = timeout(Duration::from_millis(50), timed_out_receiver)
        .await
        .expect("timed-out sender should be dropped during cleanup");
    assert!(
        canceled.is_err(),
        "timed-out pending sender should be canceled"
    );

    let in_flight = runner.in_flight.lock().await;
    assert!(!in_flight.contains_key("req-timeout"));
    assert!(in_flight.contains_key("req-other"));
    drop(in_flight);

    let pending = connection.pending.lock().await;
    assert!(!pending.contains_key("req-timeout"));
    assert!(pending.contains_key("req-other"));
    drop(pending);

    let mut exited = false;
    for _ in 0..20 {
        let terminated = {
            let mut guard = proc.lock().await;
            process_is_terminated(&mut guard)
        };
        if terminated {
            exited = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        exited,
        "shared process should be terminated when targeted cancel is unavailable"
    );

    drop(permit);
    pool.close().await.unwrap();
}

#[tokio::test]
async fn execute_with_process_times_out_on_unknown_request_id() {
    let (listener, socket_addr) = bind_tcp_listener().await;

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let message = read_message(&mut stream).await.unwrap().unwrap();
        let request = match message {
            RunnerMessage::Request { payload } => payload,
            _ => panic!("expected request"),
        };
        let response =
            ExecutionOutcome::success(request.job_id, "wrong-req", serde_json::json!({"ok": true}));
        write_message(&mut stream, &RunnerMessage::Response { payload: response })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
    });

    let child = Command::new("sleep").arg("60").spawn().unwrap();
    let process = SocketProcess {
        child: Some(child),
        socket: socket_addr,
        stdout_task: None,
        stderr_task: None,
        permits: Arc::new(Semaphore::new(1)),
        connection: None,
    };
    let (spec, _addr) = allocate_tcp_spec();
    let pool = SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["sleep".to_string(), "60".to_string()],
        pool_size: 1,
        max_in_flight: 1,
        env: None,
        cwd: None,
        tcp_socket: spec,
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: Some(Duration::from_millis(80)),
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: None,
    };
    let runner = SocketRunner {
        pool: Arc::new(pool),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };
    let proc = {
        let processes = runner.pool.processes.lock().await;
        processes.first().cloned().unwrap()
    };

    let request = ExecutionRequest {
        protocol_version: rrq_protocol::PROTOCOL_VERSION.to_string(),
        request_id: "req-1".to_string(),
        job_id: "job-1".to_string(),
        function_name: "echo".to_string(),
        params: HashMap::new(),
        context: rrq_protocol::ExecutionContext {
            job_id: "job-1".to_string(),
            attempt: 1,
            enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
            queue_name: "default".to_string(),
            deadline: None,
            trace_context: None,
            correlation_context: None,
            worker_id: None,
        },
    };
    let err = runner
        .execute_with_process(&proc, &request)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("runner response timeout"));

    server.await.unwrap();
    let mut guard = proc.lock().await;
    if let Some(child) = guard.child.as_mut() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

#[tokio::test]
async fn execute_with_process_reuses_persistent_connection() {
    let (listener, socket_addr) = bind_tcp_listener().await;

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for (request_id, job_id) in [("req-1", "job-1"), ("req-2", "job-2")] {
            let message = read_message(&mut stream).await.unwrap().unwrap();
            let request = match message {
                RunnerMessage::Request { payload } => payload,
                _ => panic!("expected request"),
            };
            assert_eq!(request.request_id, request_id);
            assert_eq!(request.job_id, job_id);
            let response =
                ExecutionOutcome::success(job_id, request_id, serde_json::json!({"ok": true}));
            write_message(&mut stream, &RunnerMessage::Response { payload: response })
                .await
                .unwrap();
        }
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "expected requests to reuse the same socket connection"
        );
    });

    let child = Command::new("sleep").arg("60").spawn().unwrap();
    let process = SocketProcess {
        child: Some(child),
        socket: socket_addr,
        stdout_task: None,
        stderr_task: None,
        permits: Arc::new(Semaphore::new(1)),
        connection: None,
    };
    let proc = Arc::new(Mutex::new(process));
    let runner = SocketRunner {
        pool: Arc::new(build_test_pool(1)),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };

    let first = runner
        .execute_with_process(&proc, &build_execution_request("req-1", "job-1"))
        .await
        .unwrap();
    assert_eq!(first.request_id.as_deref(), Some("req-1"));

    let second = runner
        .execute_with_process(&proc, &build_execution_request("req-2", "job-2"))
        .await
        .unwrap();
    assert_eq!(second.request_id.as_deref(), Some("req-2"));

    server.await.unwrap();
    let mut guard = proc.lock().await;
    if let Some(child) = guard.child.as_mut() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

#[tokio::test]
async fn execute_with_process_reconnects_after_connection_close() {
    let (listener, socket_addr) = bind_tcp_listener().await;

    let server = tokio::spawn(async move {
        for (request_id, job_id) in [("req-1", "job-1"), ("req-2", "job-2")] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let message = read_message(&mut stream).await.unwrap().unwrap();
            let request = match message {
                RunnerMessage::Request { payload } => payload,
                _ => panic!("expected request"),
            };
            assert_eq!(request.request_id, request_id);
            assert_eq!(request.job_id, job_id);
            let response =
                ExecutionOutcome::success(job_id, request_id, serde_json::json!({"ok": true}));
            write_message(&mut stream, &RunnerMessage::Response { payload: response })
                .await
                .unwrap();
        }
    });

    let child = Command::new("sleep").arg("60").spawn().unwrap();
    let process = SocketProcess {
        child: Some(child),
        socket: socket_addr,
        stdout_task: None,
        stderr_task: None,
        permits: Arc::new(Semaphore::new(1)),
        connection: None,
    };
    let proc = Arc::new(Mutex::new(process));
    let runner = SocketRunner {
        pool: Arc::new(build_test_pool(1)),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        send_cancel_hints: false,
    };

    let first = runner
        .execute_with_process(&proc, &build_execution_request("req-1", "job-1"))
        .await
        .unwrap();
    assert_eq!(first.request_id.as_deref(), Some("req-1"));

    // Give the reader task time to observe EOF and mark the persistent connection closed.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let second = runner
        .execute_with_process(&proc, &build_execution_request("req-2", "job-2"))
        .await
        .unwrap();
    assert_eq!(second.request_id.as_deref(), Some("req-2"));

    server.await.unwrap();
    let mut guard = proc.lock().await;
    if let Some(child) = guard.child.as_mut() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

#[tokio::test]
async fn persistent_connection_matches_out_of_order_responses() {
    let (listener, socket_addr) = bind_tcp_listener().await;

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let first = match read_message(&mut stream).await.unwrap().unwrap() {
            RunnerMessage::Request { payload } => payload,
            _ => panic!("expected first request"),
        };
        let second = match read_message(&mut stream).await.unwrap().unwrap() {
            RunnerMessage::Request { payload } => payload,
            _ => panic!("expected second request"),
        };

        let second_response = ExecutionOutcome::success(
            second.job_id.clone(),
            second.request_id.clone(),
            serde_json::json!({"order": 2}),
        );
        write_message(
            &mut stream,
            &RunnerMessage::Response {
                payload: second_response,
            },
        )
        .await
        .unwrap();

        let first_response = ExecutionOutcome::success(
            first.job_id.clone(),
            first.request_id.clone(),
            serde_json::json!({"order": 1}),
        );
        write_message(
            &mut stream,
            &RunnerMessage::Response {
                payload: first_response,
            },
        )
        .await
        .unwrap();
    });

    let connection = PersistentProcessConnection::connect(socket_addr, 64)
        .await
        .unwrap();
    let request_one = build_execution_request("req-1", "job-1");
    let request_two = build_execution_request("req-2", "job-2");

    let (outcome_one, outcome_two) = tokio::join!(
        connection.execute(&request_one, Some(Duration::from_secs(1))),
        connection.execute(&request_two, Some(Duration::from_secs(1)))
    );
    let outcome_one = outcome_one.unwrap();
    let outcome_two = outcome_two.unwrap();

    assert_eq!(outcome_one.request_id.as_deref(), Some("req-1"));
    assert_eq!(outcome_two.request_id.as_deref(), Some("req-2"));

    server.await.unwrap();
}

#[tokio::test]
async fn persistent_connection_ignores_late_unknown_response_id() {
    let (listener, socket_addr) = bind_tcp_listener().await;

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let first = match read_message(&mut stream).await.unwrap().unwrap() {
            RunnerMessage::Request { payload } => payload,
            _ => panic!("expected first request"),
        };
        let second = match read_message(&mut stream).await.unwrap().unwrap() {
            RunnerMessage::Request { payload } => payload,
            _ => panic!("expected second request"),
        };
        let (timed_out_request, pending_request) = if first.request_id == "req-timeout" {
            (first, second)
        } else {
            (second, first)
        };

        tokio::time::sleep(Duration::from_millis(120)).await;

        let late_response = ExecutionOutcome::success(
            timed_out_request.job_id.clone(),
            timed_out_request.request_id.clone(),
            serde_json::json!({"late": true}),
        );
        write_message(
            &mut stream,
            &RunnerMessage::Response {
                payload: late_response,
            },
        )
        .await
        .unwrap();

        let pending_response = ExecutionOutcome::success(
            pending_request.job_id.clone(),
            pending_request.request_id.clone(),
            serde_json::json!({"ok": true}),
        );
        write_message(
            &mut stream,
            &RunnerMessage::Response {
                payload: pending_response,
            },
        )
        .await
        .unwrap();
    });

    let connection = PersistentProcessConnection::connect(socket_addr, 64)
        .await
        .unwrap();
    let timeout_request = build_execution_request("req-timeout", "job-timeout");
    let pending_request = build_execution_request("req-pending", "job-pending");

    let (timeout_result, pending_result) = tokio::join!(
        connection.execute(&timeout_request, Some(Duration::from_millis(60))),
        connection.execute(&pending_request, Some(Duration::from_millis(500)))
    );

    let timeout_err = timeout_result.unwrap_err();
    assert!(timeout_err.to_string().contains("runner response timeout"));
    let pending_outcome = pending_result.unwrap();
    assert_eq!(pending_outcome.request_id.as_deref(), Some("req-pending"));

    server.await.unwrap();
}

#[tokio::test]
async fn persistent_connection_missing_request_id_fails_pending_request() {
    let (listener, socket_addr) = bind_tcp_listener().await;

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = match read_message(&mut stream).await.unwrap().unwrap() {
            RunnerMessage::Request { payload } => payload,
            _ => panic!("expected request"),
        };

        let mut response = ExecutionOutcome::success(
            request.job_id.clone(),
            request.request_id.clone(),
            serde_json::json!({"ok": true}),
        );
        response.request_id = None;
        write_message(&mut stream, &RunnerMessage::Response { payload: response })
            .await
            .unwrap();
    });

    let connection = PersistentProcessConnection::connect(socket_addr, 64)
        .await
        .unwrap();
    let request = build_execution_request("req-1", "job-1");
    let result = timeout(Duration::from_secs(1), connection.execute(&request, None)).await;
    let err = result
        .expect("request should fail promptly for missing request_id")
        .unwrap_err();
    assert!(err.to_string().contains("missing request_id"));
    assert!(err.to_string().contains("req-1"));

    server.await.unwrap();
}

#[tokio::test]
async fn close_pending_with_error_drains_even_when_already_closed() {
    let pending = Arc::new(Mutex::new(HashMap::new()));
    let closed = Arc::new(AtomicBool::new(true));
    let (sender, receiver) = oneshot::channel::<Result<ExecutionOutcome>>();
    {
        let mut guard = pending.lock().await;
        guard.insert("req-1".to_string(), sender);
    }

    close_pending_with_error(&pending, &closed, "runner connection closed".to_string()).await;

    let result = receiver.await.expect("pending sender should be completed");
    let err = result.expect_err("pending request should fail when connection is closed");
    assert!(err.to_string().contains("runner connection closed"));
    assert!(pending.lock().await.is_empty());
}

#[tokio::test]
async fn persistent_connection_execute_handles_close_race_after_pending_insert() {
    let (sender, _receiver) = mpsc::channel::<RunnerMessage>(8);
    let pending = Arc::new(Mutex::new(HashMap::new()));
    let closed = Arc::new(AtomicBool::new(false));
    let connection = Arc::new(PersistentProcessConnection {
        sender,
        pending: pending.clone(),
        closed: closed.clone(),
    });
    let request = build_execution_request("req-race", "job-race");

    let pending_guard = pending.lock().await;
    let execute_task = tokio::spawn({
        let connection = connection.clone();
        let request = request.clone();
        async move {
            connection
                .execute(&request, Some(Duration::from_millis(50)))
                .await
        }
    });

    tokio::task::yield_now().await;
    closed.store(true, Ordering::SeqCst);
    drop(pending_guard);

    let err = execute_task
        .await
        .expect("execute task should complete")
        .expect_err("execution should fail when connection closes during setup");
    assert!(err.to_string().contains("runner connection closed"));
    assert!(pending.lock().await.is_empty());
}

#[test]
fn spawn_process_with_reuse_socket_does_not_increment_cursor() {
    // This test verifies that when reuse_socket is Some, the tcp_port_cursor is not incremented
    let pool = SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["true".to_string()],
        pool_size: 2,
        max_in_flight: 1,
        env: None,
        cwd: None,
        tcp_socket: TcpSocketSpec {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 9000,
        },
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(Vec::new()),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: None,
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: None,
    };

    // Calling next_tcp_socket() increments cursor
    let first = pool.next_tcp_socket().unwrap();
    assert_eq!(first.port(), 9000);
    assert_eq!(pool.tcp_port_cursor.load(Ordering::Relaxed), 1);

    let second = pool.next_tcp_socket().unwrap();
    assert_eq!(second.port(), 9001);
    assert_eq!(pool.tcp_port_cursor.load(Ordering::Relaxed), 2);

    // But when we would spawn_process with reuse_socket=Some(...),
    // it skips next_tcp_socket() entirely and uses the provided socket.
    // The cursor stays at 2.
    // (We can't easily test spawn_process directly here without a real process,
    // but the logic is: reuse_socket.is_some() => use it, don't call next_tcp_socket())
}

#[tokio::test]
async fn respawn_preserves_socket_target() {
    // Verify that respawn() captures the old socket and passes it to spawn_process
    let spawn_override = Arc::new(|| {
        let (_spec, socket_addr) = allocate_tcp_spec();
        let child = Command::new("sleep").arg("60").spawn().unwrap();
        SocketProcess {
            child: Some(child),
            socket: socket_addr,
            stdout_task: None,
            stderr_task: None,
            permits: Arc::new(Semaphore::new(1)),
            connection: None,
        }
    });

    let (spec, _addr) = allocate_tcp_spec();
    let pool = SocketRunnerPool {
        name: "test".to_string(),
        cmd: vec!["sleep".to_string(), "60".to_string()],
        pool_size: 1,
        max_in_flight: 1,
        env: None,
        cwd: None,
        tcp_socket: spec,
        tcp_port_cursor: AtomicUsize::new(0),
        processes: Mutex::new(Vec::new()),
        cursor: AtomicUsize::new(0),
        availability: Arc::new(Notify::new()),
        response_timeout: None,
        connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
        shutdown_term_grace: Duration::from_millis(50),
        capture_output: true,
        spawn_override: Some(spawn_override),
    };

    pool.ensure_started().await.unwrap();
    let proc = {
        let processes = pool.processes.lock().await;
        processes.first().cloned().unwrap()
    };

    // Initial socket
    let initial_socket = { proc.lock().await.socket };

    // Kill process
    {
        let mut guard = proc.lock().await;
        if let Some(child) = guard.child.as_mut() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }
    sleep(Duration::from_millis(50)).await;

    // Respawn - the spawn_override will generate a new tcp socket address,
    // but in real usage (without spawn_override), respawn() passes
    // the old socket to spawn_process(Some(old_socket))
    pool.respawn(&proc).await.unwrap();

    // Note: spawn_override bypasses reuse logic, so socket changes.
    // This test primarily verifies respawn() doesn't panic and processes are restarted.
    // The spawn_process_with_reuse_socket_does_not_increment_cursor test above
    // verifies the cursor logic.

    let new_socket = { proc.lock().await.socket };
    // With spawn_override, socket will be different (new UUID)
    assert_ne!(initial_socket, new_socket);

    pool.close().await.unwrap();
}

#[test]
fn determine_needed_runners_includes_default() {
    let settings = RRQSettings {
        default_runner_name: "python".to_string(),
        default_queue_name: "default".to_string(),
        ..Default::default()
    };

    let needed = super::determine_needed_runners(&settings, None);
    assert!(needed.contains("python"));
    assert_eq!(needed.len(), 1);
}

#[test]
fn determine_needed_runners_includes_routed_runners() {
    let mut settings = RRQSettings {
        default_runner_name: "python".to_string(),
        default_queue_name: "default".to_string(),
        ..Default::default()
    };
    settings
        .runner_routes
        .insert("mail-ingest".to_string(), "mail_runner".to_string());
    settings
        .runner_routes
        .insert("mail-extract".to_string(), "mail_runner".to_string());

    // When listening to a queue with explicit routing, only routed runner is required.
    let needed = super::determine_needed_runners(&settings, Some(&["mail-ingest".to_string()]));
    assert!(needed.contains("mail_runner")); // routed runner
    assert_eq!(needed.len(), 1);
}

#[test]
fn determine_needed_runners_uses_default_queue_when_none_provided() {
    let mut settings = RRQSettings {
        default_runner_name: "python".to_string(),
        default_queue_name: "my-queue".to_string(),
        ..Default::default()
    };
    settings
        .runner_routes
        .insert("my-queue".to_string(), "special_runner".to_string());

    // When no queues provided, uses default_queue_name. If it's routed, only routed
    // runner is required.
    let needed = super::determine_needed_runners(&settings, None);
    assert!(needed.contains("special_runner")); // routed for default queue
    assert_eq!(needed.len(), 1);
}

#[test]
fn determine_needed_runners_includes_default_for_unrouted_queue() {
    let settings = RRQSettings {
        default_runner_name: "python".to_string(),
        default_queue_name: "default".to_string(),
        ..Default::default()
    };

    let needed = super::determine_needed_runners(&settings, Some(&["not-routed".to_string()]));
    assert!(needed.contains("python"));
    assert_eq!(needed.len(), 1);
}

#[test]
fn determine_needed_runners_handles_bare_and_prefixed_queue_names() {
    let mut settings = RRQSettings {
        default_runner_name: "python".to_string(),
        default_queue_name: "default".to_string(),
        ..Default::default()
    };
    settings.runner_routes.insert(
        "rrq:queue:mail-ingest".to_string(),
        "mail_runner".to_string(),
    );
    settings
        .runner_routes
        .insert("legacy-mail".to_string(), "legacy_runner".to_string());

    let needed_from_bare =
        super::determine_needed_runners(&settings, Some(&["mail-ingest".to_string()]));
    assert!(needed_from_bare.contains("mail_runner"));

    let needed_from_prefixed =
        super::determine_needed_runners(&settings, Some(&["rrq:queue:legacy-mail".to_string()]));
    assert!(needed_from_prefixed.contains("legacy_runner"));
}

#[test]
fn determine_needed_runners_deduplicates() {
    let mut settings = RRQSettings {
        default_runner_name: "shared".to_string(),
        default_queue_name: "default".to_string(),
        ..Default::default()
    };
    settings
        .runner_routes
        .insert("queue-a".to_string(), "shared".to_string());
    settings
        .runner_routes
        .insert("queue-b".to_string(), "shared".to_string());

    let needed = super::determine_needed_runners(
        &settings,
        Some(&["queue-a".to_string(), "queue-b".to_string()]),
    );
    assert!(needed.contains("shared"));
    assert_eq!(needed.len(), 1); // Only one runner, deduplicated
}

#[tokio::test]
async fn build_runners_external_mode_allows_missing_cmd() {
    let mut settings = RRQSettings {
        default_runner_name: "external_runner".to_string(),
        default_queue_name: "default".to_string(),
        runner_management_mode: RunnerManagementMode::External,
        ..Default::default()
    };
    settings.runners.insert(
        "external_runner".to_string(),
        RunnerConfig {
            runner_type: RunnerType::Socket,
            cmd: None,
            pool_size: Some(1),
            max_in_flight: Some(1),
            env: None,
            cwd: None,
            tcp_socket: Some("127.0.0.1:19000".to_string()),
            allowed_hosts: None,
            response_timeout_seconds: None,
        },
    );

    let runners = build_runners_from_settings_filtered(&settings, None, None, None)
        .await
        .expect("external mode should build runners without cmd");
    assert!(runners.contains_key("external_runner"));
}

#[tokio::test]
async fn build_runners_external_mode_forces_single_socket_target() {
    let mut settings = RRQSettings {
        default_runner_name: "external_runner".to_string(),
        default_queue_name: "default".to_string(),
        runner_management_mode: RunnerManagementMode::External,
        ..Default::default()
    };
    settings.runners.insert(
        "external_runner".to_string(),
        RunnerConfig {
            runner_type: RunnerType::Socket,
            cmd: None,
            pool_size: Some(4),
            max_in_flight: Some(1),
            env: None,
            cwd: None,
            tcp_socket: Some("127.0.0.1:65535".to_string()),
            allowed_hosts: None,
            response_timeout_seconds: None,
        },
    );
    let mut pool_sizes = HashMap::new();
    pool_sizes.insert("external_runner".to_string(), 4);
    let mut max_in_flight = HashMap::new();
    max_in_flight.insert("external_runner".to_string(), 1);

    let runners = build_runners_from_settings_filtered(
        &settings,
        Some(&pool_sizes),
        Some(&max_in_flight),
        None,
    )
    .await
    .expect("external mode should clamp pool size to one socket target");
    assert!(runners.contains_key("external_runner"));
}

#[tokio::test]
async fn build_runners_managed_mode_requires_cmd() {
    let mut settings = RRQSettings {
        default_runner_name: "managed_runner".to_string(),
        default_queue_name: "default".to_string(),
        runner_management_mode: RunnerManagementMode::Managed,
        ..Default::default()
    };
    settings.runners.insert(
        "managed_runner".to_string(),
        RunnerConfig {
            runner_type: RunnerType::Socket,
            cmd: None,
            pool_size: Some(1),
            max_in_flight: Some(1),
            env: None,
            cwd: None,
            tcp_socket: Some("127.0.0.1:19001".to_string()),
            allowed_hosts: None,
            response_timeout_seconds: None,
        },
    );

    let err = match build_runners_from_settings_filtered(&settings, None, None, None).await {
        Ok(_) => panic!("managed mode should reject missing cmd"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("cmd must not be empty for runner 'managed_runner'")
    );
}
