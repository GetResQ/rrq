"""Build the Rust rrq binary and copy it into rrq/bin for wheel builds."""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

PY_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PY_ROOT.parent
RUST_WORKSPACE = REPO_ROOT / "rrq-rs"
TARGET_DIR = RUST_WORKSPACE / "target"
PACKAGE_BIN_DIR = PY_ROOT / "rrq" / "bin"
PRODUCER_LIB_NAME = "rrq_producer"


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    print(f"+ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, env=env)


def _ensure_path(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _cargo_bin_dir() -> Path:
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
    return cargo_home / "bin"


def _ensure_rust() -> None:
    if shutil.which("cargo"):
        return

    print("cargo not found; installing Rust toolchain via rustup")
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        if sys.platform == "win32":
            url = "https://win.rustup.rs/x86_64"
            installer = tmp_path / "rustup-init.exe"
            urllib.request.urlretrieve(url, installer)
            _run([str(installer), "-y", "--profile", "minimal", "--no-modify-path"])
        else:
            url = "https://sh.rustup.rs"
            script = tmp_path / "rustup-init.sh"
            urllib.request.urlretrieve(url, script)
            script.chmod(0o755)
            _run(["sh", str(script), "-y", "--profile", "minimal", "--no-modify-path"])

    cargo_bin = _cargo_bin_dir()
    os.environ["PATH"] = f"{cargo_bin}{os.pathsep}{os.environ.get('PATH', '')}"


def _detect_target() -> str:
    override = os.environ.get("RRQ_CARGO_TARGET")
    if override:
        return override

    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "linux":
        if machine in {"x86_64", "amd64"}:
            return "x86_64-unknown-linux-gnu"
        if machine in {"aarch64", "arm64"}:
            return "aarch64-unknown-linux-gnu"
    elif system == "darwin":
        if machine == "x86_64":
            return "x86_64-apple-darwin"
        if machine in {"arm64", "aarch64"}:
            return "aarch64-apple-darwin"
    elif system == "windows":
        if machine in {"x86_64", "amd64"}:
            return "x86_64-pc-windows-msvc"
        if machine in {"arm64", "aarch64"}:
            return "aarch64-pc-windows-msvc"

    raise RuntimeError(f"Unsupported build platform: {system} / {machine}")


def _build_binary(target: str) -> Path:
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(TARGET_DIR)

    _run(["rustup", "target", "add", target], env=env)
    _run(
        [
            "cargo",
            "build",
            "--release",
            "-p",
            "rrq",
            "--bin",
            "rrq",
            "--target",
            target,
            "--manifest-path",
            str(RUST_WORKSPACE / "Cargo.toml"),
        ],
        env=env,
    )

    bin_name = "rrq.exe" if sys.platform == "win32" else "rrq"
    built = TARGET_DIR / target / "release" / bin_name
    if not built.exists():
        raise FileNotFoundError(f"Built binary not found at {built}")
    return built


def _producer_lib_filename() -> str:
    if sys.platform == "win32":
        return f"{PRODUCER_LIB_NAME}.dll"
    if sys.platform == "darwin":
        return f"lib{PRODUCER_LIB_NAME}.dylib"
    return f"lib{PRODUCER_LIB_NAME}.so"


def _build_producer_lib(target: str) -> Path:
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(TARGET_DIR)

    _run(["rustup", "target", "add", target], env=env)
    _run(
        [
            "cargo",
            "build",
            "--release",
            "-p",
            "rrq-producer",
            "--lib",
            "--target",
            target,
            "--manifest-path",
            str(RUST_WORKSPACE / "Cargo.toml"),
        ],
        env=env,
    )

    lib_name = _producer_lib_filename()
    built = TARGET_DIR / target / "release" / lib_name
    if not built.exists():
        raise FileNotFoundError(f"Built producer library not found at {built}")
    return built


def _install_binary(built: Path) -> None:
    _ensure_path(PACKAGE_BIN_DIR)
    dest = PACKAGE_BIN_DIR / built.name
    shutil.copy2(built, dest)
    if sys.platform != "win32":
        dest.chmod(0o755)
    print(f"Installed {dest}")


def _install_producer_lib(built: Path) -> None:
    _ensure_path(PACKAGE_BIN_DIR)
    dest = PACKAGE_BIN_DIR / built.name
    shutil.copy2(built, dest)
    if sys.platform != "win32":
        dest.chmod(0o755)
    print(f"Installed {dest}")


def main() -> None:
    _ensure_rust()
    target = _detect_target()
    built = _build_binary(target)
    _install_binary(built)
    producer_lib = _build_producer_lib(target)
    _install_producer_lib(producer_lib)


if __name__ == "__main__":
    main()
