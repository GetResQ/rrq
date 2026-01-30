#!/bin/sh
set -eu

if [ "$#" -eq 0 ]; then
  echo "usage: $0 -- <command> [args...]" >&2
  exit 2
fi

if [ "$1" = "--" ]; then
  shift
fi

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

cd "$ROOT_DIR/rrq-rs"
cargo build -p rrq-producer --release

LIB=""
for ext in dylib so dll; do
  candidate="$ROOT_DIR/rrq-rs/target/release/librrq_producer.$ext"
  if [ -f "$candidate" ]; then
    LIB="$candidate"
    break
  fi
done

if [ -z "$LIB" ]; then
  echo "RRQ producer library not found in rrq-rs/target/release" >&2
  exit 1
fi

cd "$ROOT_DIR"
RRQ_PRODUCER_LIB_PATH="$LIB" exec "$@"
