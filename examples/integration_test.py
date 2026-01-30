"""Integration test script for RRQ example apps.

Runs Python and Rust producers against Python/Rust executors, verifying that
queued work drains once the Rust orchestrator is started.
"""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import redis
from rrq.producer_ffi import get_producer_constants

_CONSTANTS = get_producer_constants()
JOB_KEY_PREFIX = _CONSTANTS.job_key_prefix
QUEUE_KEY_PREFIX = _CONSTANTS.queue_key_prefix


@dataclass
class ProducerSpec:
    cmd: list[str]
    cwd: Path | None = None
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class Scenario:
    name: str
    config: Path
    queues: list[str]
    producers: list[ProducerSpec]


def _toml_list(values: Iterable[str]) -> str:
    return "[" + ", ".join(f'"{value}"' for value in values) + "]"


def _format_queue_key(queue_name: str) -> str:
    if queue_name.startswith(QUEUE_KEY_PREFIX):
        return queue_name
    return f"{QUEUE_KEY_PREFIX}{queue_name}"


def _write_config(
    path: Path,
    *,
    redis_dsn: str,
    default_executor: str,
    python_cmd: list[str] | None = None,
    rust_cmd: list[str] | None = None,
    max_in_flight: int = 4,
    executor_routes: dict[str, str] | None = None,
    default_max_retries: int | None = None,
    base_retry_delay_seconds: float | None = None,
    max_retry_delay_seconds: float | None = None,
) -> None:
    lines: list[str] = [
        "[rrq]",
        f'redis_dsn = "{redis_dsn}"',
        f'default_executor_name = "{default_executor}"',
    ]
    if default_max_retries is not None:
        lines.append(f"default_max_retries = {default_max_retries}")
    if base_retry_delay_seconds is not None:
        lines.append(f"base_retry_delay_seconds = {base_retry_delay_seconds}")
    if max_retry_delay_seconds is not None:
        lines.append(f"max_retry_delay_seconds = {max_retry_delay_seconds}")
    lines.append("")

    if python_cmd is not None:
        lines.extend(
            [
                "[rrq.executors.python]",
                'type = "socket"',
                f"cmd = {_toml_list(python_cmd)}",
                "pool_size = 1",
                f"max_in_flight = {max_in_flight}",
                "",
            ]
        )

    if rust_cmd is not None:
        lines.extend(
            [
                "[rrq.executors.rust]",
                'type = "socket"',
                f"cmd = {_toml_list(rust_cmd)}",
                "pool_size = 1",
                f"max_in_flight = {max_in_flight}",
                "",
            ]
        )

    if executor_routes:
        lines.append("[rrq.executor_routes]")
        for queue_name, executor_name in executor_routes.items():
            lines.append(f'{queue_name} = "{executor_name}"')
        lines.append("")

    path.write_text("\n".join(lines))


def _run(
    cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None
) -> None:
    print(f"→ {' '.join(shlex.quote(part) for part in cmd)}")
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def _resolve_rrq_cmd() -> list[str]:
    rrq_path = shutil.which("rrq")
    if rrq_path:
        return [rrq_path]

    raise SystemExit(
        "rrq CLI not found on PATH. Install it via `pip install rrq` or "
        "`cargo install rrq`."
    )


def _start_worker(
    *,
    rrq_cmd: list[str],
    config_path: Path,
    queues: list[str] | None,
    env: dict[str, str],
    cwd: Path,
    burst: bool,
) -> subprocess.Popen:
    cmd = [*rrq_cmd, "worker", "run", "--config", str(config_path)]
    if burst:
        cmd.append("--burst")
    if queues:
        for queue in queues:
            cmd.extend(["--queue", queue])
    print(f"→ {' '.join(shlex.quote(part) for part in cmd)}")
    return subprocess.Popen(cmd, env=env, cwd=cwd)


def _stop_worker(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


def _queue_sizes(client: redis.Redis, queue_names: list[str]) -> dict[str, int]:
    sizes: dict[str, int] = {}
    for name in queue_names:
        sizes[name] = int(client.zcard(_format_queue_key(name)))
    return sizes


def _job_status_counts(client: redis.Redis) -> Counter[str]:
    counts: Counter[str] = Counter()
    for key in client.scan_iter(match=f"{JOB_KEY_PREFIX}*"):
        status = client.hget(key, "status")
        if status is None:
            continue
        if isinstance(status, bytes):
            status = status.decode("utf-8")
        counts[str(status)] += 1
    return counts


def _wait_for_completion(
    client: redis.Redis,
    *,
    queue_names: list[str],
    timeout_seconds: float,
    log_interval_seconds: float,
) -> bool:
    deadline = time.monotonic() + timeout_seconds
    last_log_time = 0.0
    last_signature: (
        tuple[tuple[tuple[str, int], ...], int, tuple[tuple[str, int], ...]] | None
    ) = None
    while time.monotonic() < deadline:
        sizes = _queue_sizes(client, queue_names)
        counts = _job_status_counts(client)
        pending = int(
            sum(counts.get(status, 0) for status in ["PENDING", "ACTIVE", "RETRYING"])
        )
        if sum(sizes.values()) == 0 and pending == 0:
            print("✓ queues drained")
            return True
        signature = (
            tuple(sorted(sizes.items())),
            pending,
            tuple(sorted(counts.items())),
        )
        now = time.monotonic()
        should_log = signature != last_signature or (
            now - last_log_time >= log_interval_seconds
        )
        if should_log:
            print(
                f"waiting... queues={sizes} pending={pending} statuses={dict(counts)}"
            )
            last_log_time = now
            last_signature = signature
        time.sleep(0.5)
    return False


def _resolve_rust_executor_cmd(override: str | None) -> list[str]:
    if override:
        return shlex.split(override)
    if "RRQ_RUST_EXECUTOR_CMD" in os.environ:
        return shlex.split(os.environ["RRQ_RUST_EXECUTOR_CMD"])

    executor_path = shutil.which("socket_executor")
    if executor_path:
        return [executor_path]

    raise SystemExit(
        "socket_executor not found on PATH. Install it via "
        "`cargo install rrq-executor --example socket_executor` or provide "
        "--rust-executor-cmd."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="RRQ integration test script")
    parser.add_argument(
        "--redis-dsn",
        default=os.getenv("RRQ_REDIS_DSN", "redis://localhost:6379/3"),
        help="Redis DSN (default: redis://localhost:6379/3)",
    )
    parser.add_argument("--count", type=int, default=1000, help="Job count per run")
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Seconds to wait for queues to drain",
    )
    parser.add_argument(
        "--log-interval",
        type=float,
        default=2.0,
        help="Seconds between progress logs if status unchanged",
    )
    parser.add_argument(
        "--max-in-flight",
        type=int,
        default=4,
        help="Max in-flight requests per executor process",
    )
    parser.add_argument(
        "--flush",
        action="store_true",
        help="Flush Redis DB before each scenario (default)",
    )
    parser.add_argument(
        "--no-flush",
        action="store_true",
        help="Do not flush Redis DB before each scenario",
    )
    parser.add_argument(
        "--rust-executor-cmd",
        type=str,
        default=None,
        help="Command to launch the Rust socket executor (overrides PATH lookup)",
    )
    parser.add_argument(
        "--burst-worker",
        action="store_true",
        help="Run worker in burst mode (not recommended for retries)",
    )
    args = parser.parse_args()

    if args.no_flush and args.flush:
        raise SystemExit("Use either --flush or --no-flush, not both.")

    if args.no_flush:
        args.flush = False
    else:
        args.flush = True

    root = Path(__file__).resolve().parents[1]
    client = redis.Redis.from_url(args.redis_dsn, decode_responses=False)

    rrq_cmd = _resolve_rrq_cmd()
    if shutil.which("cargo") is None:
        raise SystemExit("cargo not found on PATH. Install Rust to run Rust scenarios.")

    python_executor_cmd = [
        sys.executable,
        "-m",
        "rrq.executor_runtime",
        "--settings",
        "examples.python.executor_config.python_executor_settings",
    ]
    rust_executor_cmd = _resolve_rust_executor_cmd(args.rust_executor_cmd)

    env_base = os.environ.copy()

    with tempfile.TemporaryDirectory(prefix="rrq-integration-") as temp_dir:
        temp_path = Path(temp_dir)
        python_config = temp_path / "rrq_python.toml"
        rust_config = temp_path / "rrq_rust.toml"
        mixed_config = temp_path / "rrq_mixed.toml"

        _write_config(
            python_config,
            redis_dsn=args.redis_dsn,
            default_executor="python",
            python_cmd=python_executor_cmd,
            max_in_flight=args.max_in_flight,
            default_max_retries=3,
            base_retry_delay_seconds=1.0,
            max_retry_delay_seconds=5.0,
        )
        _write_config(
            rust_config,
            redis_dsn=args.redis_dsn,
            default_executor="rust",
            rust_cmd=rust_executor_cmd,
            max_in_flight=args.max_in_flight,
            default_max_retries=3,
            base_retry_delay_seconds=1.0,
            max_retry_delay_seconds=5.0,
        )
        _write_config(
            mixed_config,
            redis_dsn=args.redis_dsn,
            default_executor="python",
            python_cmd=python_executor_cmd,
            rust_cmd=rust_executor_cmd,
            max_in_flight=args.max_in_flight,
            executor_routes={"rust_queue": "rust"},
            default_max_retries=3,
            base_retry_delay_seconds=1.0,
            max_retry_delay_seconds=5.0,
        )

        rust_cwd = root / "examples" / "rust" / "producer"
        scenarios = [
            Scenario(
                name="python-only",
                config=python_config,
                queues=["default"],
                producers=[
                    ProducerSpec(
                        cmd=[
                            sys.executable,
                            "-m",
                            "examples.python.producer",
                            "--config",
                            str(python_config),
                            "--count",
                            str(args.count),
                        ],
                        cwd=root,
                    )
                ],
            ),
            Scenario(
                name="rust-only",
                config=rust_config,
                queues=["default"],
                producers=[
                    ProducerSpec(
                        cmd=["cargo", "run", "--quiet"],
                        cwd=rust_cwd,
                        env={
                            "RRQ_REDIS_DSN": args.redis_dsn,
                            "RRQ_QUEUE": "default",
                            "RRQ_FUNCTION": "echo",
                            "RRQ_COUNT": str(args.count),
                        },
                    )
                ],
            ),
            Scenario(
                name="mixed",
                config=mixed_config,
                queues=["default", "rust_queue"],
                producers=[
                    ProducerSpec(
                        cmd=[
                            sys.executable,
                            "-m",
                            "examples.python.producer",
                            "--config",
                            str(mixed_config),
                            "--count",
                            str(args.count),
                            "--rust-count",
                            str(args.count),
                        ],
                        cwd=root,
                    ),
                    ProducerSpec(
                        cmd=["cargo", "run", "--quiet"],
                        cwd=rust_cwd,
                        env={
                            "RRQ_REDIS_DSN": args.redis_dsn,
                            "RRQ_QUEUE": "rust_queue",
                            "RRQ_FUNCTION": "echo",
                            "RRQ_COUNT": str(args.count),
                        },
                    ),
                    ProducerSpec(
                        cmd=["cargo", "run", "--quiet"],
                        cwd=rust_cwd,
                        env={
                            "RRQ_REDIS_DSN": args.redis_dsn,
                            "RRQ_QUEUE": "default",
                            "RRQ_FUNCTION": "quick_task",
                            "RRQ_COUNT": str(args.count),
                        },
                    ),
                ],
            ),
        ]

        for scenario in scenarios:
            print(f"\n=== Scenario: {scenario.name} ===")
            if args.flush:
                print("Flushing Redis DB...")
                client.flushdb()
            else:
                existing = next(client.scan_iter(match=f"{JOB_KEY_PREFIX}*"), None)
                if existing is not None:
                    print(
                        "Warning: existing RRQ job keys found. "
                        "Use --flush for a clean run."
                    )

            producer_env_base = env_base.copy()
            producer_env_base["RRQ_EXECUTOR_CONFIG"] = str(scenario.config)

            for producer in scenario.producers:
                env = producer_env_base.copy()
                env.update(producer.env)
                _run(
                    producer.cmd,
                    cwd=producer.cwd,
                    env=env,
                )

            sizes = _queue_sizes(client, scenario.queues)
            print(f"Queue sizes after enqueue: {sizes}")

            worker_env = env_base.copy()
            worker_env["RRQ_EXECUTOR_CONFIG"] = str(scenario.config)
            worker = _start_worker(
                rrq_cmd=rrq_cmd,
                config_path=scenario.config,
                queues=scenario.queues,
                env=worker_env,
                cwd=root,
                burst=args.burst_worker,
            )

            try:
                completed = _wait_for_completion(
                    client,
                    queue_names=scenario.queues,
                    timeout_seconds=args.timeout,
                    log_interval_seconds=args.log_interval,
                )
                if not completed:
                    print("✗ timeout waiting for queues to drain")
                    return 1
            finally:
                _stop_worker(worker)

    print("\nAll scenarios completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
