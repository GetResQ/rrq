---
description: RRQ Testing Guidelines
globs:
alwaysApply: true
---
# RRQ Testing Guidelines

## Testing Philosophy
- **100% test pass rate is mandatory** - No exceptions
- Tests should be deterministic and reliable
- Mock external dependencies whenever possible 
- Test behavior, not implementation
- Write tests that serve as documentation

## Testing Commands
```bash
# Backend Testing
uv run pytest                          # Full test suite with coverage
uv run pytest --quick                  # Quick run for development
uv run pytest --warnings-as-errors     # Catch async issues early (recommended)
uv run pytest --maxfail=1              # Stop on first failure (debugging)
uv run pytest tests/unit/test_file.py::TestClass::test_function

```

## Backend Testing Structure

### Backend Testing Best Practices

#### Async Testing
```python
import pytest
from unittest.mock import AsyncMock, MagicMock

@pytest.mark.asyncio
async def test_async_function(admin_user):
    # Always use AsyncMock for async methods
    mock_service = AsyncMock(return_value=expected)

    # For chained async calls
    mock_query = MagicMock()
    mock_query.first = AsyncMock(return_value=result)
    Model.filter.return_value = mock_query
```

``

#### Cleanup Pattern
```python
@pytest.fixture
async def test_setup(app):
    # Setup
    app.dependency_overrides[get_current_user] = lambda: test_user

    yield

    # ALWAYS clean up
    app.dependency_overrides = {}
```

## Self-Review Before Test Submission

### Backend Tests
- [ ] All tests pass with `--warnings-as-errors`
- [ ] No hardcoded values or flaky time dependencies
- [ ] Proper async/await usage
- [ ] Database changes are rolled back
- [ ] External services are mocked
- [ ] Test names clearly describe behavior
- [ ] Edge cases are covered
- [ ] Error scenarios are tested


## Common Testing Pitfalls

### Async Issues
```python
# WRONG - Will cause RuntimeWarning
mock.method = MagicMock(return_value=value)

# CORRECT
mock.method = AsyncMock(return_value=value)
```

### Time-based Tests
```python
# WRONG - Flaky
await asyncio.sleep(1)
assert job.status == "completed"

# CORRECT - Poll with timeout
async with timeout(5):
    while job.status != "completed":
        await asyncio.sleep(0.1)
        await job.refresh_from_db()
```

## Test Data Management
- Use fixtures for reusable test data
- Keep test data minimal but realistic
- Use factories for dynamic data generation
- Store large test files in `tests/data/`

## Performance Testing
- Mock heavy operations (AI calls, media processing)
- Use `pytest-benchmark` for performance-critical code
- Set reasonable timeouts for async operations
- Profile tests that take >1 second

## Important Reminders
- NEVER skip tests to make the suite pass
- Fix the root cause, not the symptom
- Tests are documentation - keep them readable
- If a test is hard to write, the code might need refactoring
- Always run the full test suite before marking work complete
