import pytest

from rrq.registry import JobRegistry


# --- Dummy Handler Functions for Testing ---
async def dummy_handler_1(ctx, *args, **kwargs):
    return "handler1", args, kwargs


async def dummy_handler_2(ctx, param1):
    return "handler2", param1


def sync_handler(ctx):
    return "sync"


@pytest.fixture
def registry():
    """Provides a clean JobRegistry instance for each test."""
    reg = JobRegistry()
    yield reg
    reg.clear()  # Ensure cleanup after test


def test_register_and_get_handler(registry: JobRegistry):
    registry.register("task1", dummy_handler_1)
    handler = registry.get_handler("task1")
    assert handler is dummy_handler_1


def test_get_non_existent_handler(registry: JobRegistry):
    handler = registry.get_handler("non_existent_task")
    assert handler is None


def test_register_duplicate_handler_raises_error(registry: JobRegistry):
    registry.register("task1", dummy_handler_1)
    with pytest.raises(ValueError, match="already registered"):
        registry.register("task1", dummy_handler_2)  # Registering same name again


def test_register_non_callable_raises_error(registry: JobRegistry):
    with pytest.raises(ValueError, match="Handler for 'task_bad' must be a callable."):
        registry.register("task_bad", 123)  # Not a callable


# Optional: Test if we enforce async handlers (currently commented out in JobRegistry)
# def test_register_sync_handler_raises_error(registry: JobRegistry):
#     # This test depends on uncommenting the asyncio.iscoroutinefunction check
#     with pytest.raises(TypeError, match="must be an async function"):
#         registry.register("sync_task", sync_handler)


def test_unregister_handler(registry: JobRegistry):
    registry.register("task1", dummy_handler_1)
    assert registry.get_handler("task1") is dummy_handler_1
    registry.unregister("task1")
    assert registry.get_handler("task1") is None
    # Unregistering non-existent should not raise error
    registry.unregister("non_existent_task")


def test_get_registered_functions(registry: JobRegistry):
    assert registry.get_registered_functions() == []
    registry.register("task1", dummy_handler_1)
    registry.register("task2", dummy_handler_2)
    registered = registry.get_registered_functions()
    assert len(registered) == 2
    assert "task1" in registered
    assert "task2" in registered


def test_clear_registry(registry: JobRegistry):
    registry.register("task1", dummy_handler_1)
    registry.register("task2", dummy_handler_2)
    assert len(registry.get_registered_functions()) == 2
    registry.clear()
    assert len(registry.get_registered_functions()) == 0
    assert registry.get_handler("task1") is None
