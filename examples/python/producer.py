"""Toy producer for RRQ (Python)."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from rrq.client import RRQClient


async def main() -> None:
    parser = argparse.ArgumentParser(description="RRQ Python producer example")
    parser.add_argument(
        "--config",
        type=str,
        default=str(Path(__file__).with_name("rrq.toml")),
        help="Path to rrq.toml",
    )
    parser.add_argument("--count", type=int, default=5, help="Quick task count")
    parser.add_argument(
        "--include-rust",
        action="store_true",
        help="Enqueue a single rust# task (requires rust runner)",
    )
    parser.add_argument(
        "--rust-count",
        type=int,
        default=0,
        help="Number of rust#echo tasks to enqueue",
    )
    args = parser.parse_args()

    client = RRQClient(config_path=args.config)

    try:
        for i in range(args.count):
            await client.enqueue("quick_task", {"args": [f"msg-{i}"]})
        await client.enqueue("slow_task", {"args": [1.0]})
        await client.enqueue("error_task", {})
        await client.enqueue("retry_task", {"args": [2]})

        rust_count = args.rust_count or (1 if args.include_rust else 0)
        for i in range(rust_count):
            await client.enqueue("rust#echo", {"kwargs": {"hello": f"from python {i}"}})
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
