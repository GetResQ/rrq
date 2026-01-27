from __future__ import annotations

import importlib
import sys
from pathlib import Path

import rrq as rrq_pkg

reference_pkg = Path(__file__).resolve().parents[1] / "python" / "rrq"
paths = list(rrq_pkg.__path__)
ref_path = str(reference_pkg)
if ref_path not in paths:
    rrq_pkg.__path__ = [ref_path, *paths]

# Ensure rrq.settings resolves to the reference module for tests.
sys.modules.pop("rrq.settings", None)
importlib.invalidate_caches()
importlib.import_module("rrq.settings")
