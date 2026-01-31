# RRQ FFI Packaging (Python + TypeScript)

RRQ producers in Python and TypeScript use the Rust `rrq-producer` shared
library. This document explains how the shared library is built and shipped.

## Rust shared library

The shared library is built from the `rrq-producer` crate:

```
cargo build -p rrq-producer --release
```

Output files:
- Linux: `librrq_producer.so`
- macOS: `librrq_producer.dylib`
- Windows: `rrq_producer.dll`

## Python wheels

Python wheels bundle both the `rrq` CLI binary and `librrq_producer.*` in
`rrq/bin`.

- Build path: `rrq-py/scripts/build_rust_binary.py`
- Packaging: `rrq-py/pyproject.toml` (`rrq/bin/*` is included in wheels)
- Runtime lookup: `RRQ_PRODUCER_LIB_PATH` or `rrq/bin/*` fallback

Local dev/tests can use:

```
sh scripts/with-producer-lib.sh -- sh -c "cd rrq-py && uv run pytest"
```

## TypeScript npm package

The npm package bundles per-platform libraries in `rrq-ts/bin/<platform>-<arch>`.

- CI job: `.github/workflows/build-wheels.yml` builds and downloads the shared
  libs, then packages them into the npm artifact.
- Runtime lookup: `RRQ_PRODUCER_LIB_PATH`, `rrq-ts/bin`, or
  `rrq-ts/bin/<platform>-<arch>`.

Local dev/tests can use:

```
sh scripts/with-producer-lib.sh -- sh -c "cd rrq-ts && bun test"
```
