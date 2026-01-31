"""Hatchling build hook to tag wheels as py3-none-<platform>."""

from __future__ import annotations

from pathlib import Path

from packaging.tags import sys_tags

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    """Customize wheel tags for ABI-independent, platform-specific wheels."""

    def initialize(self, version: str, build_data: dict) -> None:
        platform_tag = next(sys_tags()).platform
        build_data["tag"] = f"py3-none-{platform_tag}"
        build_data["pure_python"] = False
        force_include = build_data.setdefault("force_include", {})
        bin_dir = Path("rrq") / "bin"
        for name in ("rrq", "rrq.exe", "README.md"):
            src = bin_dir / name
            if src.exists():
                force_include[str(src)] = f"rrq/bin/{name}"
