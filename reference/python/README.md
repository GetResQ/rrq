# Python reference implementation

`reference/python/socket_executor.py` shows the socket protocol in the simplest form.

```
RRQ_EXECUTOR_SOCKET=/tmp/rrq-executor.sock python reference/python/socket_executor.py
```

## Tests

These tests are not part of the main RRQ test suite. Run them explicitly:

```
uv run pytest reference/python/tests
```
