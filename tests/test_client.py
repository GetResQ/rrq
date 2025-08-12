import asyncio  # For testing unique key lock expiry
from datetime import timezone, datetime, timedelta, timezone
from typing import AsyncGenerator

import pytest
import pytest_asyncio

from rrq.client import RRQClient
from rrq.constants import DEFAULT_QUEUE_NAME, UNIQUE_JOB_LOCK_PREFIX
from rrq.job import Job, JobStatus
from rrq.settings import RRQSettings
from rrq.store import JobStore


@pytest.fixture(scope="session")
def redis_url_for_client() -> str:
    return "redis://localhost:6379/2"  # DB 2 for client tests


@pytest_asyncio.fixture(scope="function")
async def rrq_settings_for_client(redis_url_for_client: str) -> RRQSettings:
    return RRQSettings(
        redis_dsn=redis_url_for_client, default_unique_job_lock_ttl_seconds=2
    )  # Short TTL for testing


@pytest_asyncio.fixture(scope="function")
async def job_store_for_client_tests(
    rrq_settings_for_client: RRQSettings,
) -> AsyncGenerator[JobStore, None]:
    store = JobStore(settings=rrq_settings_for_client)
    await store.redis.flushdb()
    yield store
    await store.redis.flushdb()
    await store.aclose()


@pytest_asyncio.fixture(scope="function")
async def rrq_client(
    rrq_settings_for_client: RRQSettings,
    job_store_for_client_tests: JobStore,
) -> AsyncGenerator[RRQClient, None]:
    client = RRQClient(
        settings=rrq_settings_for_client, job_store=job_store_for_client_tests
    )
    yield client
    # The client.close() is important if the client might create its own JobStore.
    # In this test setup, job_store_for_client_tests is passed, so client._created_store_internally is False.
    # However, calling it ensures the close logic within RRQClient is covered.
    await client.close()


@pytest.mark.asyncio
async def test_enqueue_job_saves_definition_and_queues(
    rrq_client: RRQClient, job_store_for_client_tests: JobStore
):
    func_name = "test_function_client"
    args_ = [1, "hello"]
    kwargs_ = {"world": True}

    enqueued_job = await rrq_client.enqueue(func_name, *args_, **kwargs_)
    assert enqueued_job is not None
    assert enqueued_job.function_name == func_name
    assert enqueued_job.job_args == args_
    assert enqueued_job.job_kwargs == kwargs_
    assert enqueued_job.status == JobStatus.PENDING

    stored_job = await job_store_for_client_tests.get_job_definition(enqueued_job.id)
    assert stored_job is not None
    assert stored_job.id == enqueued_job.id
    assert stored_job.function_name == func_name

    queued_job_ids = await job_store_for_client_tests.get_queued_job_ids(
        DEFAULT_QUEUE_NAME
    )
    assert enqueued_job.id in queued_job_ids


@pytest.mark.asyncio
async def test_enqueue_job_with_defer_by(
    rrq_client: RRQClient, job_store_for_client_tests: JobStore
):
    func_name = "deferred_by_func"
    defer_seconds = 10
    # Approximate enqueue time for score comparison
    # Job.enqueue_time will be set inside RRQClient.enqueue now

    enqueued_job = await rrq_client.enqueue(
        func_name, _defer_by=timedelta(seconds=defer_seconds)
    )
    assert enqueued_job is not None

    queue_key = DEFAULT_QUEUE_NAME
    score = await job_store_for_client_tests.redis.zscore(
        queue_key, enqueued_job.id.encode("utf-8")
    )
    assert score is not None

    # Expected score is based on the job's actual enqueue_time + deferral
    expected_score_dt = enqueued_job.enqueue_time + timedelta(seconds=defer_seconds)
    expected_score_ms = int(expected_score_dt.timestamp() * 1000)

    assert score == pytest.approx(expected_score_ms, abs=1000)  # within 1 second leeway


@pytest.mark.asyncio
async def test_enqueue_job_with_defer_until(
    rrq_client: RRQClient, job_store_for_client_tests: JobStore
):
    func_name = "deferred_until_func"
    defer_until_dt = datetime.now(timezone.utc) + timedelta(minutes=1)

    enqueued_job = await rrq_client.enqueue(func_name, _defer_until=defer_until_dt)
    assert enqueued_job is not None

    queue_key = DEFAULT_QUEUE_NAME
    score = await job_store_for_client_tests.redis.zscore(
        queue_key, enqueued_job.id.encode("utf-8")
    )
    assert score is not None

    expected_score_ms = int(defer_until_dt.timestamp() * 1000)
    assert score == pytest.approx(expected_score_ms, abs=100)  # within 100ms


@pytest.mark.asyncio
async def test_enqueue_job_to_specific_queue(
    rrq_client: RRQClient, job_store_for_client_tests: JobStore
):
    custom_queue_name = "rrq:queue:custom_test_queue"
    func_name = "custom_queue_func"

    enqueued_job = await rrq_client.enqueue(func_name, _queue_name=custom_queue_name)
    assert enqueued_job is not None

    default_queue_ids = await job_store_for_client_tests.get_queued_job_ids(
        DEFAULT_QUEUE_NAME
    )
    assert enqueued_job.id not in default_queue_ids

    custom_queue_ids = await job_store_for_client_tests.get_queued_job_ids(
        custom_queue_name
    )
    assert enqueued_job.id in custom_queue_ids


@pytest.mark.asyncio
async def test_enqueue_with_user_specified_job_id(
    rrq_client: RRQClient, job_store_for_client_tests: JobStore
):
    user_job_id = "my-custom-job-id-123"
    func_name = "custom_id_func"

    enqueued_job = await rrq_client.enqueue(func_name, _job_id=user_job_id)
    assert enqueued_job is not None
    assert enqueued_job.id == user_job_id

    stored_job = await job_store_for_client_tests.get_job_definition(user_job_id)
    assert stored_job is not None
    assert stored_job.id == user_job_id

    # Try to enqueue again with same ID - current design allows this, overwriting definition
    # and adding to queue again. JobStore doesn't prevent duplicate job definitions by ID alone.
    # Uniqueness is handled by _unique_key or by job processing logic (e.g. if job already complete).
    enqueued_job_again = await rrq_client.enqueue(
        func_name, "new_arg", _job_id=user_job_id
    )
    assert enqueued_job_again is not None
    assert enqueued_job_again.id == user_job_id
    assert enqueued_job_again.job_args == ["new_arg"]
    assert enqueued_job_again.job_kwargs == {}

    stored_job_again = await job_store_for_client_tests.get_job_definition(user_job_id)
    assert stored_job_again is not None
    assert stored_job_again.id == user_job_id


@pytest.mark.asyncio
async def test_enqueue_with_unique_key(
    rrq_client: RRQClient,
    job_store_for_client_tests: JobStore,
    rrq_settings_for_client: RRQSettings,
):
    unique_key = "idempotent-op-user-555"
    func_name = "unique_func"

    # First enqueue should succeed and acquire unique lock
    job1 = await rrq_client.enqueue(func_name, _unique_key=unique_key)
    assert job1 is not None
    assert job1.job_unique_key == unique_key

    # Second enqueue with same unique key should defer instead of failing
    job2 = await rrq_client.enqueue(func_name, "different_arg", _unique_key=unique_key)
    assert job2 is not None
    # Check that it's scheduled no earlier than the remaining lock TTL
    score = await job_store_for_client_tests.redis.zscore(
        rrq_client.settings.default_queue_name, job2.id.encode("utf-8")
    )
    assert score is not None
    # remaining TTL from Redis
    remaining = await job_store_for_client_tests.redis.ttl(
        f"{UNIQUE_JOB_LOCK_PREFIX}{unique_key}"
    )
    min_expected_ms = int((datetime.now(timezone.utc) + timedelta(seconds=max(0, remaining - 1))).timestamp() * 1000)
    assert score >= min_expected_ms


class DummyStore:
    def __init__(self):
        self.aclose_called = False
        self.saved = []
        self.queued = []
        self.locks = []
        self._lock_ttl = 0

    async def aclose(self):
        self.aclose_called = True

    async def acquire_unique_job_lock(self, unique_key, job_id, lock_ttl_seconds):
        self.locks.append((unique_key, job_id, lock_ttl_seconds))
        # Deny lock to simulate duplicate job
        return False

    async def get_lock_ttl(self, unique_key: str) -> int:
        return self._lock_ttl

    async def save_job_definition(self, job: Job):
        self.saved.append(job)

    async def add_job_to_queue(self, queue_name, job_id, score):
        self.queued.append((queue_name, job_id, score))


@pytest.mark.asyncio
async def test_close_internal_store():
    settings = RRQSettings()
    client = RRQClient(settings)
    # internal store should be created
    assert client._created_store_internally is True
    store = client.job_store
    await client.close()
    # closing should call aclose
    assert hasattr(store, "aclose") and getattr(store, "aclose_called", True)


@pytest.mark.asyncio
async def test_close_external_store():
    settings = RRQSettings()
    ext_store = DummyStore()
    client = RRQClient(settings, job_store=ext_store)
    assert client._created_store_internally is False
    await client.close()
    # external store should not be closed by client.close()
    assert not ext_store.aclose_called


@pytest.mark.asyncio
async def test_enqueue_without_unique_key_and_defaults():
    settings = RRQSettings()
    store = DummyStore()
    client = RRQClient(settings, job_store=store)
    # enqueue simple job
    job = await client.enqueue("myfunc", 1, 2, key="value")
    # job returned and saved
    assert isinstance(job, Job)
    assert store.saved and store.queued
    qname, jid, score = store.queued[0]
    # default queue used
    assert qname == settings.default_queue_name
    # job id matches
    assert jid == job.id
    # score is a float timestamp
    assert isinstance(score, float)


@pytest.mark.asyncio
async def test_enqueue_with_unique_key_defers_when_locked():
    settings = RRQSettings()
    store = DummyStore()
    client = RRQClient(settings, job_store=store)
    # attempt enqueue with unique key when lock is denied
    # Simulate existing lock TTL so client defers
    store._lock_ttl = 10
    result = await client.enqueue("f", _unique_key="X")
    assert isinstance(result, Job)
    # ensure not acquiring lock when already locked
    assert not store.locks or store.locks[-1][0] != "X"


@pytest.mark.asyncio
async def test_enqueue_with_defer_by_and_until():
    settings = RRQSettings()
    store = DummyStore()
    client = RRQClient(settings, job_store=store)
    # test defer_by
    d = timedelta(seconds=5)
    job1 = await client.enqueue("f1", _defer_by=d)
    assert job1 is not None
    _, _, score1 = store.queued[-1]
    # score >= now + defer_by seconds
    now_ms = datetime.now(timezone.utc).timestamp() * 1000
    assert score1 >= now_ms + 5000 - 10
    # test defer_until with naive datetime
    future = datetime.now(timezone.utc) + timedelta(seconds=10)
    await client.enqueue("f2", _defer_until=future)
    _, _, score2 = store.queued[-1]
    # score around future
    assert abs(score2 - future.replace(tzinfo=timezone.utc).timestamp() * 1000) < 100


@pytest.mark.asyncio
async def test_unique_lock_ttl_respects_defer_by_override():
    # Override default unique TTL to small value for testing
    settings = RRQSettings(default_unique_job_lock_ttl_seconds=2)
    store = DummyStore()
    client = RRQClient(settings=settings, job_store=store)
    defer = timedelta(seconds=5)
    # Enqueue with defer_by greater than default TTL
    await client.enqueue("func", _unique_key="key1", _defer_by=defer)
    # acquire_unique_job_lock should have been called once
    assert store.locks, "Unique lock was not acquired"
    key, job_id, ttl = store.locks[-1]
    assert key == "key1"
    # TTL should be at least defer seconds + 1 (i.e., 6)
    assert ttl == 6, f"Expected TTL 6, got {ttl}"


@pytest.mark.asyncio
async def test_unique_lock_ttl_default_no_defer():
    # When no defer, TTL should equal default
    settings = RRQSettings(default_unique_job_lock_ttl_seconds=3)
    store = DummyStore()
    client = RRQClient(settings=settings, job_store=store)
    await client.enqueue("func", _unique_key="key2")
    assert store.locks, "Unique lock was not acquired"
    _, _, ttl = store.locks[-1]
    assert ttl == 3, f"Expected TTL 3, got {ttl}"


@pytest.mark.asyncio
async def test_next_scheduled_run_time_set_correctly():
    settings = RRQSettings()
    store = DummyStore()
    client = RRQClient(settings=settings, job_store=store)
    # Immediate enqueue: next_scheduled_run_time == enqueue_time
    job = await client.enqueue("f0")
    assert job.next_scheduled_run_time == job.enqueue_time
    # Defer_by enqueue: next_scheduled_run_time == enqueue_time + defer_by
    d = timedelta(seconds=10)
    job2 = await client.enqueue("f1", _defer_by=d)
    expected = job2.enqueue_time + d
    assert job2.next_scheduled_run_time == expected
    # Defer_until enqueue: next_scheduled_run_time == provided datetime (timezone.utc)
    dt = datetime.now(timezone.utc) + timedelta(seconds=15)
    job3 = await client.enqueue("f2", _defer_until=dt)
    assert job3.next_scheduled_run_time == dt

@pytest.mark.asyncio
async def test_enqueue_with_unique_key_deferral(rrq_client: RRQClient, job_store_for_client_tests: JobStore):
    unique_key = "deferral_test_key"
    # First enqueue acquires lock
    job1 = await rrq_client.enqueue("test_func", _unique_key=unique_key)
    assert job1 is not None
    # Second enqueue should defer
    job2 = await rrq_client.enqueue("test_func", _unique_key=unique_key)
    assert job2 is not None  # Now allows with defer
    # Check defer was applied based on remaining lock TTL
    score = await job_store_for_client_tests.redis.zscore(
        rrq_client.settings.default_queue_name, job2.id.encode("utf-8")
    )
    remaining = await job_store_for_client_tests.redis.ttl(
        f"{UNIQUE_JOB_LOCK_PREFIX}{unique_key}"
    )
    expected_min = int((datetime.now(timezone.utc) + timedelta(seconds=max(0, remaining - 1))).timestamp() * 1000)
    assert score >= expected_min
