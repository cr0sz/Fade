"""
Unit tests for the engine-intel proxy endpoints in src/api/routes/dashboard.py.

These endpoints fan out to an internal validation service via _proxy_validation.
The contract under test: the dashboard NEVER 500s when the upstream service is
down, slow, or returns an unexpected shape — it degrades to empty/None.

All tests are pure-unit: no real network, DB, or Redis. We patch either
_proxy_validation directly (to drive the merge/shape logic) or
httpx.AsyncClient (to exercise the helper's own graceful-degradation path).

The project's test suite is synchronous (no pytest-asyncio), so each coroutine
is driven with asyncio.run().
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from src.api.routes import dashboard


# A throwaway Request stand-in: the endpoints accept `request` but never touch
# it for the proxied paths, so a bare object is enough.
_DUMMY_REQUEST = object()


# ── helpers ──────────────────────────────────────────────────────────────────

def _fake_async_client(*, get_side_effect=None, get_return=None):
    """
    Return a MagicMock usable as `httpx.AsyncClient(...)` whose `async with`
    yields a client with an async `.get`. Either raises `get_side_effect` or
    returns a response whose `.json()` yields `get_return` (raise_for_status is
    a no-op).
    """
    client = MagicMock()
    if get_side_effect is not None:
        client.get = AsyncMock(side_effect=get_side_effect)
    else:
        resp = MagicMock()
        resp.raise_for_status = MagicMock(return_value=None)
        resp.json = MagicMock(return_value=get_return)
        client.get = AsyncMock(return_value=resp)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=None)
    return MagicMock(return_value=cm)


# ── 1. validation unavailable → engine-trust returns {"trust": []} ───────────

def test_engine_trust_service_unavailable():
    """When the validation service connection fails, no 500 — trust is []."""
    factory = _fake_async_client(
        get_side_effect=httpx.ConnectError("connection refused")
    )
    with patch.object(dashboard.httpx, "AsyncClient", factory):
        result = asyncio.run(dashboard.get_engine_trust(_DUMMY_REQUEST))
    assert result == {"trust": []}


# ── 2. trust list returned → passed through unchanged ────────────────────────

def test_engine_trust_list_passthrough():
    """A list from upstream is forwarded verbatim under the `trust` key."""
    trust_rows = [
        {"engine": "TDE", "trust": 0.81, "status": "healthy"},
        {"engine": "ICE", "trust": 0.44, "status": "watch"},
    ]
    with patch.object(dashboard, "_proxy_validation", AsyncMock(return_value=trust_rows)):
        result = asyncio.run(dashboard.get_engine_trust(_DUMMY_REQUEST))
    assert result == {"trust": trust_rows}


# ── 3. timeout → engine-accuracy degrades to empty lists ─────────────────────

def test_engine_accuracy_timeout_graceful():
    """A timeout on the upstream calls leaves both result lists empty (no 500)."""
    factory = _fake_async_client(
        get_side_effect=httpx.TimeoutException("timed out")
    )
    with patch.object(dashboard.httpx, "AsyncClient", factory):
        result = asyncio.run(dashboard.get_engine_accuracy(_DUMMY_REQUEST))
    assert result == {"accuracy": [], "win_rates": []}


# ── 4. accuracy + win_rates merged correctly when both succeed ───────────────

def test_engine_accuracy_merges_both_sources():
    """
    accuracy is pulled from the `engines` key of a dict; win_rates is a bare
    list. The merged response keeps them in their respective keys.
    """
    accuracy_payload = {"engines": [{"engine": "TDE", "accuracy": 0.62}], "days": 7}
    win_rate_payload = [{"engine": "TDE", "win_rate": 0.55}]

    async def fake_proxy(path: str):
        if path.startswith("/validation/accuracy"):
            return accuracy_payload
        if path.startswith("/feedback/engines/ranking"):
            return win_rate_payload
        raise AssertionError(f"unexpected path {path}")

    with patch.object(dashboard, "_proxy_validation", side_effect=fake_proxy):
        result = asyncio.run(dashboard.get_engine_accuracy(_DUMMY_REQUEST))

    assert result == {
        "accuracy": [{"engine": "TDE", "accuracy": 0.62}],
        "win_rates": [{"engine": "TDE", "win_rate": 0.55}],
    }


# ── 5. meta-risk missing keys → still a valid dict, no KeyError ──────────────

def test_meta_risk_handles_missing_keys():
    """
    Upstream returns shapes lacking expected keys (regime without 'regime',
    confluence as the unavailable-error dict). The endpoint must not raise and
    must return well-formed sub-keys.
    """
    async def fake_proxy(path: str):
        if path == "/meta/risk":
            return {"composite": 0.3}            # valid dict, kept as-is
        if path == "/intelligence/regime":
            return {"note": "no regime field"}   # missing 'regime' key
        if path == "/intelligence/confluence":
            return {"error": "validation service unavailable"}
        raise AssertionError(f"unexpected path {path}")

    with patch.object(dashboard, "_proxy_validation", side_effect=fake_proxy):
        result = asyncio.run(dashboard.get_meta_risk(_DUMMY_REQUEST))

    assert result["meta_risk"] == {"composite": 0.3}
    assert result["regime"] is None
    # confluence is a dict (even if it carries an error marker) — never crashes
    assert isinstance(result["confluence"], dict)


# ── bonus: helper returns the unavailable marker on raise_for_status error ───

def test_proxy_validation_returns_error_marker_on_http_error():
    """A 4xx/5xx from upstream is swallowed into the standard error dict."""
    def _raise():
        raise httpx.HTTPStatusError("500", request=None, response=None)

    resp = MagicMock()
    resp.raise_for_status = MagicMock(side_effect=_raise)
    resp.json = MagicMock(return_value={"should": "not be reached"})
    client = MagicMock()
    client.get = AsyncMock(return_value=resp)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=None)

    with patch.object(dashboard.httpx, "AsyncClient", MagicMock(return_value=cm)):
        out = asyncio.run(dashboard._proxy_validation("/governance/trust"))
    assert out == {"error": "validation service unavailable"}
