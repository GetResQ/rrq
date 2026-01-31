This directory is expected to contain the `rrq_producer` shared library for the
TypeScript producer FFI.

Supported filenames:
- `librrq_producer.dylib` (macOS)
- `librrq_producer.so` (Linux)

Preferred layout for cross-platform packages:
- `bin/darwin-arm64/librrq_producer.dylib`
- `bin/linux-x64/librrq_producer.so`

Alternatively, set `RRQ_PRODUCER_LIB_PATH` to the absolute path of the shared
library.
