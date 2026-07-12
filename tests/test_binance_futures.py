"""Integration tests for BinanceFuturesBroker.

aiohttp is not installed on the test host, so it is stubbed at module import
time before binance_futures is imported. The suite is synchronous (no
pytest-asyncio) — each coroutine is driven with asyncio.run(). conftest.py
already puts paper_trading_engine/ on sys.path.

Module-level caches (_FILTER_CACHE / _LEVERAGE_SET / _DUAL_SIDE_CHECKED) persist
across tests, so _fresh() clears them per test.
"""
from __future__ import annotations

import os
import sys
import types
import asyncio

# ── aiohttp stub (must run before importing the broker) ───────────────────────
if "aiohttp" not in sys.modules:
    _ah = types.ModuleType("aiohttp")

    class _CT:  # ClientTimeout
        def __init__(self, *a, **k):
            pass

    class _CS:  # ClientSession
        def __init__(self, *a, **k):
            pass

    _ah.ClientTimeout = _CT
    _ah.ClientSession = _CS
    sys.modules["aiohttp"] = _ah

from brokers import binance_futures as _bf  # noqa: E402


# ── helpers ───────────────────────────────────────────────────────────────────

def _fresh():
    """Fresh broker with all module-level caches cleared and paper mode."""
    _bf._FILTER_CACHE.clear()
    _bf._LEVERAGE_SET.clear()
    _bf._DUAL_SIDE_CHECKED.clear()
    os.environ.update({
        "BINANCE_API_KEY": "k",
        "BINANCE_API_SECRET": "s",
        "BINANCE_TESTNET": "true",
    })
    os.environ.pop("TRADING_MODE", None)  # default paper — no stop placement
    return _bf.BinanceFuturesBroker()


def _av(v):
    """Build a coroutine function that ignores args and returns v."""
    async def _f(*a, **k):
        return v
    return _f


def _happy_post(posts):
    """A _signed_post recorder for place_order's leverage + order flow."""
    async def _post(path, params=None):
        posts.append((path, params or {}))
        if path == "/fapi/v1/leverage":
            return {"leverage": 2}
        if path == "/fapi/v1/marginType":
            return {}
        return {"orderId": 1, "executedQty": "0.5", "cumQuote": "50", "avgPrice": "100"}
    return _post


# ── 1. pure unit: _floor_to_step ──────────────────────────────────────────────

def test_floor_to_step_pure():
    assert _bf._floor_to_step(0.507, 0.01) == 0.5
    assert _bf._floor_to_step(0.001, 0.001) == 0.001


# ── 2. min-notional bump ──────────────────────────────────────────────────────

def test_min_notional_bump():
    b = _fresh()
    posts = []
    b._signed_get = _av({"dualSidePosition": False})
    b._mark_price = _av(100.0)
    b._filters = _av((0.001, 20.0, 0.0))   # min_notional 20
    b._signed_post = _happy_post(posts)

    # notional 5 / price 100 = 0.05 → below min 20 → bumped to 0.2
    result = asyncio.run(b.place_order("BTC", "short", 5.0))
    assert result.success
    order_posts = [p for p in posts if p[0] == "/fapi/v1/order"]
    assert len(order_posts) == 1
    assert order_posts[0][1]["quantity"] == "0.2"


# ── 3. qty step rounding ──────────────────────────────────────────────────────

def test_qty_step_rounding():
    b = _fresh()
    posts = []
    b._signed_get = _av({"dualSidePosition": False})
    b._mark_price = _av(100.0)
    b._filters = _av((0.01, 5.0, 0.0))     # step 0.01
    b._signed_post = _happy_post(posts)

    # notional 50.7 / price 100 = 0.507 → floored to 0.5 at step 0.01
    result = asyncio.run(b.place_order("BTC", "short", 50.7))
    assert result.success
    order_posts = [p for p in posts if p[0] == "/fapi/v1/order"]
    assert order_posts[0][1]["quantity"] == "0.5"


# ── 4. hedge mode rejected ────────────────────────────────────────────────────

def test_hedge_mode_rejected():
    b = _fresh()
    posts = []
    b._signed_get = _av({"dualSidePosition": True})  # hedge mode
    b._mark_price = _av(100.0)
    b._filters = _av((0.001, 5.0, 0.0))
    b._signed_post = _happy_post(posts)

    result = asyncio.run(b.place_order("BTC", "short", 50.0))
    assert not result.success
    assert "hedge" in result.error
    # no order should have been placed
    assert not [p for p in posts if p[0] == "/fapi/v1/order"]


# ── 5. leverage failure blocks the order ──────────────────────────────────────

def test_leverage_failure_blocks():
    b = _fresh()
    posts = []
    b._signed_get = _av({"dualSidePosition": False})
    b._mark_price = _av(100.0)
    b._filters = _av((0.001, 5.0, 0.0))

    async def _post(path, params=None):
        posts.append((path, params or {}))
        if path == "/fapi/v1/leverage":
            raise Exception("leverage rejected")
        if path == "/fapi/v1/marginType":
            return {}
        return {"orderId": 1, "executedQty": "0.5", "cumQuote": "50", "avgPrice": "100"}
    b._signed_post = _post

    result = asyncio.run(b.place_order("BTC", "short", 50.0))
    assert not result.success
    # order POST must never have been reached
    assert not [p for p in posts if p[0] == "/fapi/v1/order"]


# ── 6. isolated margin set before the order ───────────────────────────────────

def test_isolated_margin_before_order():
    b = _fresh()
    posts = []
    b._signed_get = _av({"dualSidePosition": False})
    b._mark_price = _av(100.0)
    b._filters = _av((0.001, 5.0, 0.0))
    b._signed_post = _happy_post(posts)

    result = asyncio.run(b.place_order("BTC", "short", 50.0))
    assert result.success
    paths = [p[0] for p in posts]
    assert "/fapi/v1/marginType" in paths
    assert "/fapi/v1/order" in paths
    assert paths.index("/fapi/v1/marginType") < paths.index("/fapi/v1/order")


# ── 7. close with zero fill is a failure ──────────────────────────────────────

def test_close_zero_qty_is_failure():
    b = _fresh()
    b._position_qty = _av(1.0)
    b._filters = _av((0.001, 5.0, 0.0))
    b._signed_post = _av(
        {"orderId": 9, "executedQty": "0", "cumQuote": "0", "avgPrice": "0"}
    )

    result = asyncio.run(b.close_position("BTC", 1.0, "short"))
    assert not result.success
    assert "filled nothing" in result.error


# ── 8. maker close: GTX LIMIT fully fills → maker fee, no market order ─────────

def test_maker_close_pays_maker_fee():
    b = _fresh()
    b._position_qty = _av(1.0)
    b._filters = _av((0.001, 5.0, 0.01))      # step, min_notional, tick

    async def _get(path, params=None):
        if "bookTicker" in path:
            return {"bidPrice": "100.0", "askPrice": "100.5"}
        return {}
    b._signed_get = _get

    posts = []
    async def _post(path, params=None):
        posts.append((path, params or {}))
        # GTX limit fills completely on placement
        return {"orderId": 5, "executedQty": "1.0", "cumQuote": "100", "avgPrice": "100"}
    b._signed_post = _post

    res = asyncio.run(b.close_position("BTC", 1.0, "short", maker=True))
    assert res.success
    # closing a short = BUY, maker rests at best_bid (100.0)
    order_posts = [p for p in posts if p[0] == "/fapi/v1/order"]
    assert len(order_posts) == 1                       # no taker remainder
    assert order_posts[0][1]["type"] == "LIMIT"
    assert order_posts[0][1]["timeInForce"] == "GTX"
    assert order_posts[0][1]["reduceOnly"] == "true"
    # maker fee 0.02% of 100 = 0.02, not taker 0.05
    assert abs(res.fee - 0.02) < 1e-9, res.fee


# ── 8. cancel stop order graceful handling ────────────────────────────────────

def test_cancel_stop_graceful():
    # sub-test 1: network exception → False
    b = _fresh()

    async def _del_fail(*a, **k):
        raise Exception("net")
    b._signed_delete = _del_fail
    assert asyncio.run(b.cancel_stop_order("BTC", "123")) is False

    # sub-test 2: -2011 (already gone) → True
    b2 = _fresh()
    b2._signed_delete = _av({"code": -2011})
    assert asyncio.run(b2.cancel_stop_order("BTC", "123")) is True


# ── 9. full_exit=True uses exchange qty, not the passed qty ───────────────────

def test_close_full_exit_uses_exchange_qty():
    """When full_exit=True the broker must send the exchange's actual position
    qty (from _position_qty) rather than the engine-tracked qty passed in."""
    b = _fresh()
    # Exchange holds 2.0 coins; engine thinks it has 1.5 (partial-exit drift).
    b._position_qty = _av(2.0)
    b._filters = _av((0.001, 5.0, 0.1))  # (step, min_notional, tick)
    b._mark_price = _av(100.0)

    posted_params: list[dict] = []

    async def _capture_post(path, params=None):
        posted_params.append(params or {})
        return {"orderId": 42, "executedQty": "2.0", "cumQuote": "200", "avgPrice": "100"}

    b._signed_post = _capture_post

    result = asyncio.run(b.close_position("BTC", 1.5, "short", full_exit=True))
    assert result.success
    order = next(p for p in posted_params if p.get("symbol") == "BTCUSDT")
    # Must have sent 2.0 (exchange qty), not 1.5 (engine qty).
    assert float(order["quantity"]) == 2.0


# ── 10. MAKER_ENTRY=false falls straight through to a plain MARKET order ──────

def test_maker_disabled_market_fallback():
    """With MAKER_ENTRY=false, place_order must POST exactly one MARKET order
    (no bookTicker fetch, no LIMIT order) and return success — proving that the
    market-only fallback path is intact regardless of maker logic."""
    os.environ["MAKER_ENTRY"] = "false"
    try:
        b = _fresh()
        posts: list[tuple] = []
        b._signed_get = _av({"dualSidePosition": False})
        b._mark_price = _av(100.0)
        b._filters = _av((0.001, 5.0, 0.1))  # 3-tuple: (step, min_notional, tick)
        b._signed_post = _happy_post(posts)

        result = asyncio.run(b.place_order("BTC", "long", 50.0))
        assert result.success, result.error
        order_posts = [p for p in posts if p[0] == "/fapi/v1/order"]
        # Exactly one order and it must be MARKET (no LIMIT maker attempt)
        assert len(order_posts) == 1, f"expected 1 order POST, got {len(order_posts)}"
        assert order_posts[0][1]["type"] == "MARKET"
    finally:
        os.environ.pop("MAKER_ENTRY", None)
