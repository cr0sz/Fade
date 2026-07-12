"""
Unit tests for the trade-autopsy dashboard endpoint
(GET /dashboard/autopsy/{trade_id} in src/api/routes/dashboard.py).

The endpoint runs up to four SQL queries against a single asyncpg connection
acquired from ``request.app.state.db.acquire()``:

  Q1 (fetchrow)  base trade  + LEFT JOIN llm_trade_audits
  Q2 (fetch)     signals_registry + LEFT JOIN signal_outcomes  (skipped if no signal_id)
  Q3 (fetch)     trade_reflections
  Q4 (fetch)     signal_trace                                   (skipped if no signal_id)

No real DB is used. We stub the connection with a fake whose ``fetchrow`` /
``fetch`` coroutines dispatch off the SQL text so each logical query can be
controlled independently. asyncpg ``Record`` objects are emulated with plain
dicts (the endpoint only ever does ``row["col"]`` access).
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

import pytest


def run(coro):
    """Drive a coroutine to completion (no pytest-asyncio dependency)."""
    return asyncio.run(coro)

# asyncpg is stubbed in conftest only lazily; the dashboard module imports
# httpx + fastapi which are real. Import the route handler directly.
from src.api.routes import dashboard as dash


# ── Fakes ─────────────────────────────────────────────────────────────────────

class FakeConn:
    """
    Fake asyncpg connection. ``fetchrow``/``fetch`` look at the SQL string to
    decide which canned result to return, so each test only has to set the
    handful of results it cares about.
    """

    def __init__(
        self,
        *,
        trade_row=None,
        signal_rows=None,
        reflection_rows=None,
        trace_rows=None,
    ):
        self.trade_row = trade_row
        self.signal_rows = signal_rows if signal_rows is not None else []
        self.reflection_rows = reflection_rows if reflection_rows is not None else []
        self.trace_rows = trace_rows if trace_rows is not None else []
        self.calls: list[str] = []

    async def fetchrow(self, query, *args):
        self.calls.append("fetchrow")
        # Only Q1 uses fetchrow in this endpoint.
        return self.trade_row

    async def fetch(self, query, *args):
        self.calls.append("fetch")
        q = " ".join(query.split())
        if "signals_registry" in q:
            return self.signal_rows
        if "trade_reflections" in q:
            return self.reflection_rows
        if "signal_trace" in q:
            return self.trace_rows
        raise AssertionError(f"unexpected fetch query: {q[:80]!r}")


class FakeDB:
    def __init__(self, conn: FakeConn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        @asynccontextmanager
        async def _cm():
            yield conn

        return _cm()


class FakeState:
    def __init__(self, db):
        self.db = db


class FakeApp:
    def __init__(self, state):
        self.state = state


class FakeRequest:
    def __init__(self, conn: FakeConn):
        self.app = FakeApp(FakeState(FakeDB(conn)))


def _request(conn: FakeConn) -> FakeRequest:
    return FakeRequest(conn)


# ── Row builders ───────────────────────────────────────────────────────────────

T_OPEN = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)
T_CLOSE = T_OPEN + timedelta(hours=3)


def make_trade_row(**overrides) -> dict:
    base = {
        "id": "trade-1",
        "asset": "BTC",
        "exchange": "CRYPTO",
        "direction": "long",
        "mode": "paper",
        "entry_price": 100.0,
        "exit_price": 110.0,
        "size": 1.0,
        "notional": 100.0,
        "fee": 0.1,
        "pnl": 10.0,
        "close_reason": "take_profit",
        "router_action": "invert",
        "router_edge": 0.012,
        "signal_id": "",          # default: pre-signal-era trade (NOT NULL '' in schema)
        "opened_at": T_OPEN,
        "closed_at": T_CLOSE,
        # LLM audit columns (NULL when no audit row joined)
        "verdict": None,
        "audit_confidence": None,
        "bull_case": None,
        "bear_case": None,
        "audit_model": None,
        "audit_latency_ms": None,
    }
    base.update(overrides)
    return base


def make_signal_row(**overrides) -> dict:
    base = {
        "engine_name": "TrendDetectionEngine",
        "signal_type": "panic_short",
        "signal_direction": "short",
        "composite_score": 0.42,
        "confidence_level": "high",
        "emitted_at": T_OPEN,
        "signal_data": {"drop_pct": -7.5},
        "evaluation_window_h": None,
        "direction_correct": None,
        "predicted_direction": None,
        "actual_direction": None,
        "outcome_score_delta": None,
        "outcome_evaluated_at": None,
    }
    base.update(overrides)
    return base


def make_reflection_row(**overrides) -> dict:
    base = {
        "lesson": "Held too long past peak.",
        "hold_hours": 3.0,
        "model": "qwen2.5:7b",
        "created_at": T_CLOSE,
    }
    base.update(overrides)
    return base


def make_trace_row(**overrides) -> dict:
    base = {
        "engine_name": "TrendDetectionEngine",
        "signal_type": "panic_short",
        "signal_strength": 0.8,
        "signal_direction": "short",
        "latency_ms": 12.0,
        "created_at": T_OPEN,
    }
    base.update(overrides)
    return base


# ── Tests ──────────────────────────────────────────────────────────────────────

def test_trade_not_found_returns_404():
    conn = FakeConn(trade_row=None)
    with pytest.raises(dash.HTTPException) as ei:
        run(dash.get_trade_autopsy(_request(conn), "missing"))
    assert ei.value.status_code == 404
    assert ei.value.detail == "Trade not found"


def test_no_signal_id_skips_signal_and_trace_queries():
    """A trade with empty signal_id must NOT touch signals_registry or signal_trace."""
    conn = FakeConn(
        trade_row=make_trade_row(signal_id=""),
        # Provide signal/trace data that should be IGNORED because queries are skipped.
        signal_rows=[make_signal_row()],
        trace_rows=[make_trace_row()],
        reflection_rows=[],
    )
    result = run(dash.get_trade_autopsy(_request(conn), "trade-1"))

    assert result["signal"] is None
    assert result["outcomes"] == []
    assert result["trace"] == []
    # Q2 (signals_registry) and Q4 (signal_trace) must have been skipped:
    # only one fetchrow (Q1) and one fetch (Q3 reflections) ran.
    assert conn.calls == ["fetchrow", "fetch"]


def test_with_signal_id_populates_signal_block():
    sig = make_signal_row(
        engine_name="IceColdEngine",
        signal_type="confluence",
        signal_direction="long",
        composite_score=0.55,
        evaluation_window_h=24,
        direction_correct=True,
        predicted_direction="long",
        actual_direction="long",
        outcome_score_delta=0.031,
        outcome_evaluated_at=T_CLOSE,
    )
    conn = FakeConn(
        trade_row=make_trade_row(signal_id="12345"),
        signal_rows=[sig],
        trace_rows=[make_trace_row()],
        reflection_rows=[],
    )
    result = run(dash.get_trade_autopsy(_request(conn), "trade-1"))

    assert result["signal"] is not None
    assert result["signal"]["engine_name"] == "IceColdEngine"
    assert result["signal"]["signal_type"] == "confluence"
    assert result["signal"]["direction"] == "long"
    assert result["signal"]["composite_score"] == 0.55
    assert result["signal"]["signal_data"] == {"drop_pct": -7.5}
    # Outcome row present and typed.
    assert len(result["outcomes"]) == 1
    assert result["outcomes"][0]["window_h"] == 24
    assert result["outcomes"][0]["direction_correct"] is True
    assert result["outcomes"][0]["outcome_score_delta"] == 0.031
    # Trace populated (Q4 ran because signal_id present).
    assert len(result["trace"]) == 1
    assert result["trace"][0]["engine_name"] == "TrendDetectionEngine"
    # All four queries ran: Q1 fetchrow, then Q2/Q3/Q4 fetch.
    assert conn.calls == ["fetchrow", "fetch", "fetch", "fetch"]


def test_llm_audit_block_populated():
    conn = FakeConn(
        trade_row=make_trade_row(
            verdict="approve",
            audit_confidence=0.77,
            bull_case="Strong oversold bounce setup.",
            bear_case="Macro still bearish.",
            audit_model="qwen2.5:7b",
            audit_latency_ms=850,
        ),
        reflection_rows=[],
    )
    result = run(dash.get_trade_autopsy(_request(conn), "trade-1"))

    audit = result["audit"]
    assert audit is not None
    assert audit["verdict"] == "approve"
    assert audit["confidence"] == 0.77
    assert audit["bull_case"] == "Strong oversold bounce setup."
    assert audit["bear_case"] == "Macro still bearish."
    assert audit["model"] == "qwen2.5:7b"
    assert audit["latency_ms"] == 850.0


def test_reflections_array_populated():
    conn = FakeConn(
        trade_row=make_trade_row(),
        reflection_rows=[
            make_reflection_row(lesson="Lesson A", hold_hours=1.5),
            make_reflection_row(lesson="Lesson B", hold_hours=None, model=None),
        ],
    )
    result = run(dash.get_trade_autopsy(_request(conn), "trade-1"))

    reflections = result["reflections"]
    assert len(reflections) == 2
    assert reflections[0]["lesson"] == "Lesson A"
    assert reflections[0]["hold_hours"] == 1.5
    assert reflections[1]["lesson"] == "Lesson B"
    # None-safe coercions.
    assert reflections[1]["hold_hours"] is None
    assert reflections[1]["model"] is None


def test_pnl_pct_calculated_correctly():
    conn = FakeConn(
        trade_row=make_trade_row(pnl=10.0, notional=100.0),
        reflection_rows=[],
    )
    result = run(dash.get_trade_autopsy(_request(conn), "trade-1"))
    # 10 / 100 * 100 = 10.0 (%)
    assert result["trade"]["pnl_pct"] == 10.0
    # Sanity on hold_hours derivation (3h span).
    assert result["trade"]["hold_hours"] == 3.0


def test_pnl_pct_none_when_notional_zero_or_none():
    # notional == 0 -> no ZeroDivisionError, pnl_pct is None.
    conn0 = FakeConn(trade_row=make_trade_row(pnl=10.0, notional=0.0), reflection_rows=[])
    r0 = run(dash.get_trade_autopsy(_request(conn0), "trade-1"))
    assert r0["trade"]["pnl_pct"] is None

    # notional is None -> also None, no crash.
    connN = FakeConn(trade_row=make_trade_row(pnl=10.0, notional=None), reflection_rows=[])
    rN = run(dash.get_trade_autopsy(_request(connN), "trade-1"))
    assert rN["trade"]["pnl_pct"] is None
    assert rN["trade"]["notional"] is None
