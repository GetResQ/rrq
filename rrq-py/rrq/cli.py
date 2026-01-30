"""Thin wrapper that delegates the rrq CLI to the Rust binary."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from typing import Iterable

try:
    from importlib import resources
except ImportError:  # pragma: no cover - Python 3.11+ always available
    resources = None  # type: ignore[assignment]


ENV_RRQ_RUST_BIN = "RRQ_RUST_BIN"


def _binary_names() -> Iterable[str]:
    if os.name == "nt":
        return ("rrq.exe", "rrq")
    return ("rrq",)


def _find_packaged_binary() -> Path | None:
    if resources is None:
        return None
    try:
        package_root = resources.files("rrq")
    except (ModuleNotFoundError, AttributeError):
        return None
    for name in _binary_names():
        candidate = package_root / "bin" / name
        if candidate.is_file():
            return Path(candidate)
    return None


def _find_path_binary(wrapper_path: Path) -> Path | None:
    for name in _binary_names():
        candidate = shutil.which(name)
        if not candidate:
            continue
        resolved = Path(candidate).resolve()
        if resolved == wrapper_path:
            continue
        return resolved
    return None


def _resolve_binary() -> Path | None:
    env_path = os.getenv(ENV_RRQ_RUST_BIN)
    if env_path:
        candidate = Path(env_path).expanduser()
        if candidate.is_file():
            return candidate.resolve()

    packaged = _find_packaged_binary()
    if packaged is not None:
        return packaged

    wrapper_path = Path(sys.argv[0]).resolve()
    return _find_path_binary(wrapper_path)


def main() -> None:
    binary = _resolve_binary()
    if binary is None:
        print(
            "RRQ Rust binary not found. Set RRQ_RUST_BIN or install the rrq binary.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    os.execv(str(binary), [str(binary), *sys.argv[1:]])


__all__ = ["main"]


if __name__ == "__main__":
    main()
