# Python examples

## Configure

`examples/python/rrq.toml` configures the worker and points at the Python stdio executor.

## Run a consumer (orchestrator)

```
python examples/python/consumer.py --config examples/python/rrq.toml
```

## Run a producer

```
python examples/python/producer.py --config examples/python/rrq.toml
```

## Perf test

Start a worker first, then run:

```
python examples/python/perf_test.py --config examples/python/rrq.toml --count 500
```
