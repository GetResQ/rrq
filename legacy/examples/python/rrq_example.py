"""rrq_example.py: End-to-end example using the stdio Python executor."""

from __future__ import annotations

import asyncio
from pathlib import Path

from rrq.client import RRQClient
from rrq.config import load_toml_settings
from rrq.executor import build_executors_from_settings
from rrq.worker import RRQWorker


async def main() -> None:
    config_path = Path(__file__).with_name("rrq.toml")
    settings = load_toml_settings(str(config_path))

    client = RRQClient(settings=settings)
    await client.enqueue("quick_task", "hello")
    await client.enqueue("slow_task", 0.5)
    await client.enqueue("retry_task", 2)
    await client.enqueue("error_task")
    await client.close()

    executors = build_executors_from_settings(settings)
    worker = RRQWorker(settings=settings, executors=executors, burst=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
