"""
Unit tests for the /dashboard/graveyard endpoint (src/api/routes/dashboard.py).

The endpoint reads from the `retired_strategies` table (migration 028) and shapes
the rows into a JSON payload with reactivation status and a still-retired count.

All DB I/O is stubbed — conn.fetch() is mocked, no real Postgres connection.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

# prometheus_client / asyncpg / structlog stubs + sys.path are set up in conftest.py
from src.api.routes.dashboard import get_graveyard


def _run(coro):
    """Run an async coroutine to completion (no pytest-asyncio dependency)."""
    return asyncio.run(coro)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _row(
    *,
    id=1,
    strategy_name="Some Engine",
    engine_key="SomeEngine",
    retired_at=None,
    sprint="Sprint 1",
    retirement_reason="reason",
    retirement_category="performance",
    win_rate_at_retirement=None,
    notes="notes",
    reactivated_at=None,
    reactivation_notes=None,
) -> dict:
    """Build a dict shaped like an asyncpg Record (supports r["key"] access)."""
    if retired_at is None:
        retired_at = datetime(2026, 3, 1, tzinfo=timezone.utc)
    return {
        "id": id,
        "strategy_name": strategy_name,
        "engine_key": engine_key,
        "retired_at": retired_at,
        "sprint": sprint,
        "retirement_reason": retirement_reason,
        "retirement_category": retirement_category,
        "win_rate_at_retirement": win_rate_at_retirement,
        "notes": notes,
        "reactivated_at": reactivated_at,
        "reactivation_notes": reactivation_notes,
    }


def _request_returning(rows: list) -> SimpleNamespace:
    """Build a fake FastAPI Request whose app.state.db.acquire() yields a conn
    whose fetch() returns `rows`."""
    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=rows)

    # acquire() returns an async context manager yielding conn.
    acquire_cm = MagicMock()
    acquire_cm.__aenter__ = AsyncMock(return_value=conn)
    acquire_cm.__aexit__ = AsyncMock(return_value=False)

    db = MagicMock()
    db.acquire = MagicMock(return_value=acquire_cm)

    app = SimpleNamespace(state=SimpleNamespace(db=db))
    return SimpleNamespace(app=app)


# ── Tests ──────────────────────────────────────────────────────────────────

def test_empty_table():
    """No rows → count=0, still_retired=0, entries=[]."""
    req = _request_returning([])
    resp = _run(get_graveyard(req))
    assert resp == {"count": 0, "still_retired": 0, "entries": []}


def test_all_retired_still_retired_equals_count():
    """No entry has reactivated_at → still_retired == count."""
    rows = [
        _row(id=1, reactivated_at=None),
        _row(id=2, reactivated_at=None),
        _row(id=3, reactivated_at=None),
    ]
    req = _request_returning(rows)
    resp = _run(get_graveyard(req))
    assert resp["count"] == 3
    assert resp["still_retired"] == 3
    assert all(e["is_reactivated"] is False for e in resp["entries"])
    assert all(e["reactivated_at"] is None for e in resp["entries"])


def test_reactivated_entry():
    """An entry with reactivated_at → is_reactivated True and ISO string set."""
    react = datetime(2026, 5, 22, tzinfo=timezone.utc)
    rows = [_row(id=1, reactivated_at=react, reactivation_notes="recovered")]
    req = _request_returning(rows)
    resp = _run(get_graveyard(req))

    entry = resp["entries"][0]
    assert entry["is_reactivated"] is True
    assert isinstance(entry["reactivated_at"], str)
    assert entry["reactivated_at"] == react.isoformat()
    assert entry["reactivation_notes"] == "recovered"
    assert resp["still_retired"] == 0


def test_null_win_rate_serializes_as_none():
    """win_rate_at_retirement NULL → field is None, no 500/error raised."""
    rows = [_row(id=1, win_rate_at_retirement=None)]
    req = _request_returning(rows)
    resp = _run(get_graveyard(req))
    assert resp["entries"][0]["win_rate_at_retirement"] is None


def test_non_null_win_rate_is_float():
    """A NUMERIC win rate (Decimal) is coerced to a float in the payload."""
    rows = [_row(id=1, win_rate_at_retirement=Decimal("0.0000"))]
    req = _request_returning(rows)
    resp = _run(get_graveyard(req))
    wr = resp["entries"][0]["win_rate_at_retirement"]
    assert isinstance(wr, float)
    assert wr == 0.0


def test_ordering_still_retired_before_reactivated():
    """Endpoint preserves DB order (reactivated_at NULLS FIRST): still-retired
    entries appear before reactivated ones. We feed rows already in that order
    and confirm the endpoint does not reshuffle them."""
    react = datetime(2026, 5, 22, tzinfo=timezone.utc)
    rows = [
        _row(id=10, reactivated_at=None),
        _row(id=11, reactivated_at=None),
        _row(id=12, reactivated_at=react),
    ]
    req = _request_returning(rows)
    resp = _run(get_graveyard(req))

    flags = [e["is_reactivated"] for e in resp["entries"]]
    # All False (still retired) must come before any True (reactivated).
    first_reactivated = flags.index(True)
    assert all(f is False for f in flags[:first_reactivated])
    assert flags == [False, False, True]
    assert resp["still_retired"] == 2
