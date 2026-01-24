"""Toy consumer/orchestrator for RRQ (Python)."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from rrq.config import load_toml_settings
from rrq.executor import build_executors_from_settings
from rrq.worker import RRQWorker


async def main() -> None:
    parser = argparse.ArgumentParser(description="RRQ Python consumer example")
    parser.add_argument(
        "--config",
        type=str,
        default=str(Path(__file__).with_name("rrq.toml")),
        help="Path to rrq.toml",
    )
    parser.add_argument("--queue", action="append", default=None)
    parser.add_argument("--burst", action="store_true")
    args = parser.parse_args()

    settings = load_toml_settings(args.config)
    executors = build_executors_from_settings(settings)
    worker = RRQWorker(
        settings=settings,
        executors=executors,
        queues=args.queue or None,
        burst=args.burst,
    )

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
