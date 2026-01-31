This directory is reserved for the Rust `rrq` binary and shared libraries.

Packaging note: build pipelines should place the compiled `rrq` executable and
the `rrq_producer` shared library here before producing a wheel so the Python
wrapper can locate them.
