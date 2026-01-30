from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, cast

import pytest
import pytest_asyncio

from rrq.client import RRQClient
from rrq.constants import DEFAULT_QUEUE_NAME, IDEMPOTENCY_KEY_PREFIX
from rrq.job import Job, JobStatus
from rrq.settings import RRQSettings
from rrq.store import JobStore
from rrq.telemetry import EnqueueSpan, Telemetry, disable, configure


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

    enqueued_job = await rrq_client.enqueue(func_name, *args_, world=True)
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

    # Try to enqueue again with same ID - should be rejected to avoid overwriting.
    with pytest.raises(ValueError):
        await rrq_client.enqueue(func_name, "new_arg", _job_id=user_job_id)


@pytest.mark.asyncio
async def test_enqueue_rejects_non_positive_timeout():
    settings = RRQSettings()
    store = DummyStore()
    client = RRQClient(settings, job_store=cast(JobStore, store))
    with pytest.raises(ValueError):
        await client.enqueue("func", _job_timeout_seconds=0)
    with pytest.raises(ValueError):
        await client.enqueue("func", _job_timeout_seconds=-5)


@pytest.mark.asyncio
async def test_enqueue_invalid_timeout_does_not_set_idempotency_key(
    rrq_settings_for_client: RRQSettings, job_store_for_client_tests: JobStore
):
    client = RRQClient(
        settings=rrq_settings_for_client, job_store=job_store_for_client_tests
    )
    unique_key = "bad-timeout-idem"
    with pytest.raises(ValueError):
        await client.enqueue("func", _unique_key=unique_key, _job_timeout_seconds=0)
    stored = await job_store_for_client_tests.redis.get(
        f"{IDEMPOTENCY_KEY_PREFIX}{unique_key}"
    )
    assert stored is None


@pytest.mark.asyncio
async def test_enqueue_with_unique_key(
    rrq_client: RRQClient,
    job_store_for_client_tests: JobStore,
    rrq_settings_for_client: RRQSettings,
):
    unique_key = "idempotent-op-user-555"
    func_name = "unique_func"

    # First enqueue should succeed and set idempotency key
    job1 = await rrq_client.enqueue(func_name, _unique_key=unique_key)
    assert job1 is not None

    idempotency_value = await job_store_for_client_tests.redis.get(
        f"{IDEMPOTENCY_KEY_PREFIX}{unique_key}"
    )
    assert idempotency_value is not None

    # Second enqueue with same unique key should return the same job id
    job2 = await rrq_client.enqueue(func_name, "different_arg", _unique_key=unique_key)
    assert job2 is not None
    assert job2.id == job1.id


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

    async def atomic_enqueue_job(
        self, job: Job, queue_name: str, score_ms: float
    ) -> bool:
        if any(saved.id == job.id for saved in self.saved):
            return False
        self.saved.append(job)
        self.queued.append((queue_name, job.id, score_ms))
        return True


class _TestEnqueueSpan(EnqueueSpan):
    def __enter__(self):
        return {"traceparent": "00-abc-123-01"}


class _TestTelemetry(Telemetry):
    def enqueue_span(self, *, job_id: str, function_name: str, queue_name: str):
        return _TestEnqueueSpan()


@pytest.mark.asyncio
async def test_enqueue_stores_trace_context(
    rrq_client: RRQClient, job_store_for_client_tests: JobStore
):
    configure(_TestTelemetry())
    try:
        job = await rrq_client.enqueue("test_function_trace_context")
        assert job is not None
        stored_job = await job_store_for_client_tests.get_job_definition(job.id)
        assert stored_job is not None
        assert stored_job.trace_context == {"traceparent": "00-abc-123-01"}
    finally:
        disable()


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
    client = RRQClient(settings, job_store=cast(JobStore, ext_store))
    assert client._created_store_internally is False
    await client.close()
    # external store should not be closed by client.close()
    assert not ext_store.aclose_called


@pytest.mark.asyncio
async def test_enqueue_with_unique_key_idempotent(
    rrq_client: RRQClient, job_store_for_client_tests: JobStore
):
    unique_key = "deferral_test_key"
    job1 = await rrq_client.enqueue("test_func", _unique_key=unique_key)
    assert job1 is not None
    job2 = await rrq_client.enqueue("test_func", _unique_key=unique_key)
    assert job2 is not None
    assert job2.id == job1.id
