"""
Pytest configuration and shared fixtures.

All external I/O (postgres, redis, network) is mocked here.
The paper_trading_engine/ package lives in a sub-directory; we add it and
src/ to sys.path so tests can import directly by module name, mirroring the
Docker WORKDIR that the engine itself uses.
"""
from __future__ import annotations

import sys
import os
from pathlib import Path

# ── Path setup ────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).parent.parent
ENGINE_DIR = REPO_ROOT / "paper_trading_engine"

# Engine modules must shadow same-named modules in src/ (e.g. models.py, risk.py).
# So engine dir is inserted FIRST (index 0); src/ is appended only as a fallback.
if str(ENGINE_DIR) not in sys.path:
    sys.path.insert(0, str(ENGINE_DIR))
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.append(str(REPO_ROOT / "src"))

# Stub the one module that is genuinely optional at test time.
# Everything else the engine imports (asyncpg, httpx, fastapi, structlog) is a
# real dependency of the suite — install BOTH requirement files before running:
#     pip install -r requirements.txt -r requirements-dev.txt
# The stub is a minimal shim — just enough to satisfy `from X import Y`.
import types

def _make_stub(name: str) -> types.ModuleType:
    """Return a simple namespace module registered under `name`."""
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    return mod


# prometheus_client — only Counter is used in truth_router
if "prometheus_client" not in sys.modules:
    _pc = _make_stub("prometheus_client")
    class _Counter:
        def __init__(self, *a, **kw): pass
        def labels(self, **kw): return self
        def inc(self, amount=1): pass
    _pc.Counter = _Counter

import pytest
