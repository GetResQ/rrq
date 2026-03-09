from __future__ import annotations

from pathlib import Path

import pytest

from rrq import cli


class _ExecCalled(Exception):
    pass


def test_find_packaged_binary_returns_existing_binary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    package_root = tmp_path / "rrq_pkg"
    bin_dir = package_root / "bin"
    bin_dir.mkdir(parents=True)
    binary = bin_dir / "rrq"
    binary.write_text("", encoding="utf-8")

    class _FakeResources:
        @staticmethod
        def files(_package: str) -> Path:
            return package_root

    monkeypatch.setattr(cli, "resources", _FakeResources)

    assert cli._find_packaged_binary() == binary


def test_find_path_binary_skips_wrapper_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    wrapper = tmp_path / "rrq"
    wrapper.write_text("", encoding="utf-8")
    system_binary = tmp_path / "rrq-system"
    system_binary.write_text("", encoding="utf-8")

    monkeypatch.setattr(cli, "_binary_names", lambda: ("rrq", "rrq-system"))

    def _fake_which(name: str) -> str | None:
        if name == "rrq":
            return str(wrapper)
        if name == "rrq-system":
            return str(system_binary)
        return None

    monkeypatch.setattr(cli.shutil, "which", _fake_which)

    assert cli._find_path_binary(wrapper.resolve()) == system_binary.resolve()


def test_resolve_binary_prefers_env_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env_binary = tmp_path / "rrq-env"
    env_binary.write_text("", encoding="utf-8")
    monkeypatch.setenv(cli.ENV_RRQ_RUST_BIN, str(env_binary))
    monkeypatch.setattr(cli, "_find_packaged_binary", lambda: None)
    monkeypatch.setattr(cli, "_find_path_binary", lambda _wrapper: None)

    assert cli._resolve_binary() == env_binary.resolve()


def test_main_execs_resolved_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    binary = Path("/tmp/rrq-rust-bin")
    recorded: dict[str, object] = {}

    def _fake_execv(path: str, argv: list[str]) -> None:
        recorded["path"] = path
        recorded["argv"] = argv
        raise _ExecCalled()

    monkeypatch.setattr(cli, "_resolve_binary", lambda: binary)
    monkeypatch.setattr(cli.sys, "argv", ["rrq", "job", "list"])
    monkeypatch.setattr(cli.os, "execv", _fake_execv)

    with pytest.raises(_ExecCalled):
        cli.main()

    assert recorded == {
        "path": str(binary),
        "argv": [str(binary), "job", "list"],
    }


def test_main_exits_when_binary_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli, "_resolve_binary", lambda: None)

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    assert isinstance(exc_info.value, SystemExit)
    assert exc_info.value.code == 1
    assert "RRQ Rust binary not found" in capsys.readouterr().err
