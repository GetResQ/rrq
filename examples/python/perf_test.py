"""Simple performance test for RRQ (Python)."""

from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path

from rrq.client import RRQClient
from rrq.config import load_toml_settings
from rrq.store import JobStore


async def main() -> None:
    parser = argparse.ArgumentParser(description="RRQ perf test (Python)")
    parser.add_argument(
        "--config",
        type=str,
        default=str(Path(__file__).with_name("rrq.toml")),
        help="Path to rrq.toml",
    )
    parser.add_argument("--count", type=int, default=200, help="Job count")
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Seconds to wait for completion",
    )
    args = parser.parse_args()

    settings = load_toml_settings(args.config)
    client = RRQClient(settings=settings)
    store = JobStore(settings=settings)

    job_ids: list[str] = []
    start = time.monotonic()
    try:
        for i in range(args.count):
            job = await client.enqueue("quick_task", f"perf-{i}")
            if job is not None:
                job_ids.append(job.id)

        deadline = time.monotonic() + args.timeout
        completed: set[str] = set()
        while time.monotonic() < deadline and len(completed) < len(job_ids):
            for job_id in job_ids:
                if job_id in completed:
                    continue
                job_def = await store.get_job_definition(job_id)
                if job_def is not None and job_def.status in {"COMPLETED", "FAILED"}:
                    completed.add(job_id)
            await asyncio.sleep(0.1)
    finally:
        await client.close()
        await store.aclose()

    elapsed = time.monotonic() - start
    rate = len(job_ids) / elapsed if elapsed > 0 else 0
    print(f"Completed {len(job_ids)} jobs in {elapsed:.2f}s ({rate:.1f} jobs/s)")


if __name__ == "__main__":
    asyncio.run(main())
