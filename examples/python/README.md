# Python examples

## Configure

`examples/python/rrq.toml` configures the Rust orchestrator and points at the Python
runner.

Install the published packages first:

```
pip install rrq
```

## Run the Rust orchestrator + Python runner

```
rrq worker run --config examples/python/rrq.toml
```

In another terminal, run the Python runner runtime:

```
rrq-runner --settings examples.python.runner_config.python_runner_settings
```

## Run a producer

```
python examples/python/producer.py --config examples/python/rrq.toml
```

## Perf test

Start the orchestrator/runner first, then run:

```
python examples/python/perf_test.py --config examples/python/rrq.toml --count 500
```
