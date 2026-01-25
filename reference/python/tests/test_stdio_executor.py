from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from rrq.executor import ExecutionContext, ExecutionOutcome, ExecutionRequest


def _run_executor(lines: Iterable[str]) -> list[ExecutionOutcome]:
    script_path = Path(__file__).resolve().parents[1] / "stdio_executor.py"
    proc = subprocess.Popen(
        [sys.executable, str(script_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdin is not None
    assert proc.stdout is not None
    proc.stdin.write("\n".join(lines) + "\n")
    proc.stdin.flush()
    proc.stdin.close()
    stdout = proc.stdout.read()
    proc.wait(timeout=5)
    outcomes: list[ExecutionOutcome] = []
    for raw in stdout.splitlines():
        raw = raw.strip()
        if not raw:
            continue
        outcomes.append(ExecutionOutcome.model_validate_json(raw))
    return outcomes


def _request(function_name: str) -> ExecutionRequest:
    context = ExecutionContext(
        job_id="job-1",
        attempt=1,
        enqueue_time=datetime.now(timezone.utc),
        queue_name="default",
    )
    return ExecutionRequest(
        job_id="job-1",
        function_name=function_name,
        args=[],
        kwargs={},
        context=context,
    )


def test_stdio_executor_echo_handler() -> None:
    request = _request("echo")
    outcomes = _run_executor([request.model_dump_json()])
    assert len(outcomes) == 1
    assert outcomes[0].job_id == "job-1"
    assert outcomes[0].status == "success"
    assert outcomes[0].result == {"job_id": "job-1"}


def test_stdio_executor_unknown_handler() -> None:
    request = _request("missing")
    outcomes = _run_executor([request.model_dump_json()])
    assert len(outcomes) == 1
    assert outcomes[0].job_id == "job-1"
    assert outcomes[0].status == "error"
    assert outcomes[0].error_type == "handler_not_found"


def test_stdio_executor_invalid_request() -> None:
    outcomes = _run_executor(["not-json"])
    assert len(outcomes) == 1
    assert outcomes[0].status == "error"
    assert outcomes[0].error_message is not None
