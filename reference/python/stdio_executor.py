"""Reference stdio executor implementation (Python).

Reads ExecutionRequest JSON from stdin and writes ExecutionOutcome JSON to stdout.
"""

from __future__ import annotations

import sys
from typing import Any

from rrq.executor import ExecutionOutcome, ExecutionRequest


def handle_echo(request: ExecutionRequest) -> ExecutionOutcome:
    return ExecutionOutcome(status="success", result={"job_id": request.job_id})


def main() -> int:
    handlers: dict[str, Any] = {
        "echo": handle_echo,
    }

    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            request = ExecutionRequest.model_validate_json(raw)
        except Exception as exc:
            outcome = ExecutionOutcome(status="error", error_message=str(exc))
            sys.stdout.write(outcome.model_dump_json() + "\n")
            sys.stdout.flush()
            continue

        handler = handlers.get(request.function_name)
        if handler is None:
            outcome = ExecutionOutcome(
                status="error",
                error_message=f"No handler for '{request.function_name}'",
                error_type="handler_not_found",
            )
        else:
            outcome = handler(request)

        sys.stdout.write(outcome.model_dump_json() + "\n")
        sys.stdout.flush()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
