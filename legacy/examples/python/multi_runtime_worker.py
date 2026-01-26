"""Example: Route queues to different executors (Python + Rust via stdio)."""

import asyncio
import logging
from pathlib import Path

from rrq.config import load_toml_settings
from rrq.executor import build_executors_from_settings
from rrq.worker import RRQWorker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RRQMultiRuntimeExample")


async def main() -> None:
    config_path = Path(__file__).with_name("rrq_multi.toml")
    settings = load_toml_settings(str(config_path))
    executors = build_executors_from_settings(settings)
    worker = RRQWorker(
        settings=settings,
        executors=executors,
        queues=[settings.default_queue_name, "rust_queue"],
    )

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
