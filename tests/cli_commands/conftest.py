"""Test fixtures for CLI commands"""

import json
import time
from datetime import datetime, timedelta
from typing import AsyncGenerator, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from click.testing import CliRunner

from rrq.constants import JOB_KEY_PREFIX, QUEUE_KEY_PREFIX
from rrq.registry import JobRegistry
from rrq.settings import RRQSettings
from rrq.store import JobStore


@pytest.fixture
def cli_runner():
    """CLI test runner"""
    return CliRunner()


@pytest.fixture(scope="session")
def redis_url_for_cli() -> str:
    """Redis URL for CLI tests using a dedicated database"""
    return "redis://localhost:6379/10"  # DB 10 for CLI tests


@pytest.fixture
def mock_settings(redis_url_for_cli):
    """Mock RRQ settings for testing"""
    registry = JobRegistry()

    # Register some test functions
    def test_function(arg1: str, arg2: int = 10):
        return f"Processed {arg1} with {arg2}"

    def slow_function(duration: int = 1):
        time.sleep(duration)
        return f"Completed after {duration}s"

    registry.register("test_function", test_function)
    registry.register("slow_function", slow_function)

    return RRQSettings(
        redis_dsn=redis_url_for_cli,
        job_registry=registry,
        default_queue_name="test_queue",
        default_dlq_name="test_dlq",
    )


@pytest_asyncio.fixture
async def job_store(mock_settings) -> AsyncGenerator[JobStore, None]:
    """JobStore instance with real Redis for testing"""
    store = JobStore(settings=mock_settings)
    # Clear the database before and after tests
    if hasattr(store, "redis") and hasattr(store.redis, "flushdb"):
        await store.redis.flushdb()
    yield store
    if hasattr(store, "redis") and hasattr(store.redis, "flushdb"):
        await store.redis.flushdb()
    await store.aclose()


@pytest.fixture
async def populated_job_store(job_store):
    """JobStore with sample data for testing"""
    await _populate_test_data(job_store)
    return job_store


async def _populate_test_data(job_store: JobStore):
    """Populate JobStore with test data"""
    now = datetime.now()

    # Create test jobs with various statuses
    test_jobs = [
        {
            "id": "job_001",
            "function_name": "test_function",
            "queue_name": "test_queue",
            "status": "pending",
            "args": json.dumps(["arg1", "arg2"]),
            "kwargs": json.dumps({"key": "value"}),
            "created_at": (now - timedelta(minutes=5)).timestamp(),
            "retries": 0,
            "max_retries": 3,
        },
        {
            "id": "job_002",
            "function_name": "slow_function",
            "queue_name": "urgent",
            "status": "completed",
            "args": json.dumps([2]),
            "kwargs": json.dumps({}),
            "created_at": (now - timedelta(minutes=10)).timestamp(),
            "started_at": (now - timedelta(minutes=8)).timestamp(),
            "completed_at": (now - timedelta(minutes=6)).timestamp(),
            "result": json.dumps({"success": True, "result": "Completed after 2s"}),
            "worker_id": "worker_001",
            "retries": 0,
            "max_retries": 3,
        },
        {
            "id": "job_003",
            "function_name": "test_function",
            "queue_name": "test_queue",
            "status": "failed",
            "args": json.dumps(["invalid"]),
            "kwargs": json.dumps({}),
            "created_at": (now - timedelta(minutes=15)).timestamp(),
            "started_at": (now - timedelta(minutes=13)).timestamp(),
            "completed_at": (now - timedelta(minutes=12)).timestamp(),
            "error": "Invalid argument provided",
            "traceback": "Traceback (most recent call last):\n  File ...\nValueError: Invalid argument",
            "worker_id": "worker_002",
            "retries": 2,
            "max_retries": 3,
        },
        {
            "id": "job_004",
            "function_name": "test_function",
            "queue_name": "low_priority",
            "status": "retrying",
            "args": json.dumps(["retry_me"]),
            "kwargs": json.dumps({}),
            "created_at": (now - timedelta(minutes=20)).timestamp(),
            "started_at": (now - timedelta(minutes=18)).timestamp(),
            "error": "Temporary failure",
            "retries": 1,
            "max_retries": 3,
        },
        {
            "id": "job_005",
            "function_name": "slow_function",
            "queue_name": "urgent",
            "status": "active",
            "args": json.dumps([10]),
            "kwargs": json.dumps({}),
            "created_at": (now - timedelta(minutes=2)).timestamp(),
            "started_at": (now - timedelta(minutes=1)).timestamp(),
            "worker_id": "worker_001",
            "retries": 0,
            "max_retries": 3,
        },
    ]

    # Insert jobs into Redis
    for job_data in test_jobs:
        job_key = f"{JOB_KEY_PREFIX}{job_data['id']}"
        await job_store.redis.hset(job_key, mapping=job_data)

        # Add pending jobs to queues
        if job_data["status"] == "pending":
            queue_key = f"{QUEUE_KEY_PREFIX}{job_data['queue_name']}"
            priority = job_data["created_at"]
            await job_store.redis.zadd(queue_key, {job_data["id"]: priority})

    # Create test workers
    worker_data = [
        {
            "worker_id": "worker_001",
            "status": "running",
            "active_jobs": 1,
            "concurrency_limit": 5,
            "queues": ["test_queue", "urgent"],
            "timestamp": now.timestamp(),
        },
        {
            "worker_id": "worker_002",
            "status": "idle",
            "active_jobs": 0,
            "concurrency_limit": 3,
            "queues": ["test_queue", "low_priority"],
            "timestamp": (now - timedelta(seconds=30)).timestamp(),
        },
        {
            "worker_id": "worker_003",
            "status": "polling",
            "active_jobs": 0,
            "concurrency_limit": 10,
            "queues": ["urgent"],
            "timestamp": (now - timedelta(seconds=5)).timestamp(),
        },
    ]

    # Insert worker health data
    for worker in worker_data:
        await job_store.set_worker_health(
            worker["worker_id"],
            worker,
            60,  # TTL
        )


@pytest.fixture
def mock_job_store():
    """Mock JobStore for testing without Redis"""
    mock_store = MagicMock()
    mock_store.aclose = AsyncMock()
    mock_store.redis = MagicMock()
    return mock_store


@pytest.fixture
def sample_jobs():
    """Sample job data for testing"""
    now = datetime.now()
    return [
        {
            "id": "test_job_001",
            "function_name": "process_data",
            "queue_name": "default",
            "status": "completed",
            "created_at": (now - timedelta(hours=1)).timestamp(),
            "completed_at": now.timestamp(),
        },
        {
            "id": "test_job_002",
            "function_name": "send_email",
            "queue_name": "urgent",
            "status": "failed",
            "created_at": (now - timedelta(hours=2)).timestamp(),
            "error": "SMTP connection failed",
        },
        {
            "id": "test_job_003",
            "function_name": "generate_report",
            "queue_name": "default",
            "status": "pending",
            "created_at": (now - timedelta(minutes=30)).timestamp(),
        },
    ]


@pytest.fixture
def sample_workers():
    """Sample worker data for testing"""
    now = datetime.now()
    return [
        {
            "id": "worker_001",
            "status": "running",
            "active_jobs": 2,
            "last_heartbeat": now.timestamp(),
            "ttl": 60,
        },
        {
            "id": "worker_002",
            "status": "idle",
            "active_jobs": 0,
            "last_heartbeat": (now - timedelta(seconds=30)).timestamp(),
            "ttl": 30,
        },
    ]


def create_test_job_data(
    job_id: str = "test_job",
    status: str = "pending",
    function_name: str = "test_function",
    queue_name: str = "test_queue",
    **kwargs,
) -> Dict:
    """Helper to create test job data"""
    now = datetime.now()

    job_data = {
        "id": job_id,
        "function_name": function_name,
        "queue_name": queue_name,
        "status": status,
        "args": json.dumps(kwargs.get("args", [])),
        "kwargs": json.dumps(kwargs.get("kwargs", {})),
        "created_at": kwargs.get("created_at", now.timestamp()),
        "retries": kwargs.get("retries", 0),
        "max_retries": kwargs.get("max_retries", 3),
    }

    # Add status-specific fields
    if status in ["completed", "failed"]:
        job_data.update(
            {
                "started_at": kwargs.get(
                    "started_at", (now - timedelta(minutes=2)).timestamp()
                ),
                "completed_at": kwargs.get("completed_at", now.timestamp()),
                "worker_id": kwargs.get("worker_id", "test_worker"),
            }
        )

        if status == "completed":
            job_data["result"] = kwargs.get("result", json.dumps({"success": True}))
        else:
            job_data["error"] = kwargs.get("error", "Test error")
            job_data["traceback"] = kwargs.get("traceback", "Test traceback")

    elif status == "active":
        job_data.update(
            {
                "started_at": kwargs.get(
                    "started_at", (now - timedelta(minutes=1)).timestamp()
                ),
                "worker_id": kwargs.get("worker_id", "test_worker"),
            }
        )

    return job_data


def create_test_worker_data(
    worker_id: str = "test_worker", status: str = "running", **kwargs
) -> Dict:
    """Helper to create test worker data"""
    now = datetime.now()

    return {
        "worker_id": worker_id,
        "status": status,
        "active_jobs": kwargs.get("active_jobs", 1),
        "concurrency_limit": kwargs.get("concurrency_limit", 5),
        "queues": kwargs.get("queues", ["test_queue"]),
        "timestamp": kwargs.get("timestamp", now.timestamp()),
    }


# Mock the load_app_settings function for tests
@pytest.fixture(autouse=True)
def mock_load_settings(mock_settings):
    """Auto-mock load_app_settings for all CLI tests"""
    import rrq.cli_commands.base

    original_load = rrq.cli_commands.base.load_app_settings

    def mock_load(_settings_path=None):
        return mock_settings

    rrq.cli_commands.base.load_app_settings = mock_load
    yield mock_load
    rrq.cli_commands.base.load_app_settings = original_load
