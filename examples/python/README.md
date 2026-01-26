# Python examples

## Configure

`examples/python/rrq.toml` configures the Rust orchestrator and points at the Python
stdio executor.

## Run the Rust orchestrator + Python executor

```
rrq worker run --config examples/python/rrq.toml
```

In another terminal, run the Python executor runtime:

```
rrq-executor --settings examples.python.executor_config.python_executor_settings
```

## Run a producer

```
python examples/python/producer.py --config examples/python/rrq.toml
```

## Perf test

Start the orchestrator/executor first, then run:

```
python examples/python/perf_test.py --config examples/python/rrq.toml --count 500
```
