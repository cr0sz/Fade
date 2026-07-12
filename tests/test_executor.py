"""
Unit tests for executor.py — position sizing, PnL arithmetic, max_hold routing.

Covers bug classes:
  3. max_hold per exchange (CRYPTO short 24h, STOCK 48h, BIST 6h) must not silently default.
  6. Kelly sizing / vol_scale caps, MIN_NOTIONAL guard.
"""
from __future__ import annotations

import sys
import os
import types

import executor
from executor import (
    open_trade,
    close_trade,
    _max_hold_hours,
    MIN_NOTIONAL,
    SLIPPAGE_PCT,
    FEE_RATE,
    _HOLD_CRYPTO_LONG,
    _HOLD_CRYPTO_SHORT,
    _HOLD_STOCK,
    _HOLD_BIST,
)
from models import PaperTrade, Signal
from portfolio import Portfolio


# ── Helpers ──────────────────────────────────────────────────────────────────

def _signal(
    direction="long",
    exchange="CRYPTO",
    price=100.0,
    confidence=0.60,
    asset="BTC",
) -> Signal:
    return Signal(
        signal_id="1",
        asset=asset,
        direction=direction,
        confidence=confidence,
        timestamp="",
        source="test",
        composite_score=0.30,
        exchange=exchange,
        real_price=price,
        volume_cap_usd=0.0,
        news_score=0.0,
        social_score=0.0,
        volume_score=0.0,
    )


def _portfolio(balance: float = 10_000.0) -> Portfolio:
    p = Portfolio()
    p.balance = balance
    return p


# ── Bug class 3: max_hold routing ─────────────────────────────────────────────

class TestMaxHoldHours:
    def test_crypto_short_returns_correct_hold(self):
        """CRYPTO short must use _HOLD_CRYPTO_SHORT (24h), not the default 8h."""
        assert _max_hold_hours(0.6, "CRYPTO", "short") == _HOLD_CRYPTO_SHORT

    def test_crypto_long_returns_correct_hold(self):
        assert _max_hold_hours(0.6, "CRYPTO", "long") == _HOLD_CRYPTO_LONG

    def test_stock_hold(self):
        """NASDAQ/NYSE/CBOE must return _HOLD_STOCK (48h)."""
        for exch in ("NASDAQ", "NYSE", "CBOE"):
            assert _max_hold_hours(0.6, exch, "long") == _HOLD_STOCK
            assert _max_hold_hours(0.6, exch, "short") == _HOLD_STOCK

    def test_bist_hold(self):
        assert _max_hold_hours(0.6, "BIST", "long") == _HOLD_BIST

    def test_empty_exchange_defaults_to_crypto(self):
        """Empty exchange string falls through to CRYPTO hold times (unknown = conservative short hold)."""
        assert _max_hold_hours(0.6, "", "long") == _HOLD_CRYPTO_LONG
        assert _max_hold_hours(0.6, "", "short") == _HOLD_CRYPTO_SHORT

    def test_crypto_short_not_same_as_long(self):
        """Regression: crypto short and long used to both be 2h before Sprint 38."""
        # Both are 24h now — they should be equal (sprint 38 unified them)
        # but they must NOT be 2 (the old default that triggered the bug).
        assert _max_hold_hours(0.6, "CRYPTO", "short") != 2
        assert _max_hold_hours(0.6, "CRYPTO", "long") != 2

    def test_open_trade_carries_correct_max_hold(self):
        """The trade object produced by open_trade must inherit the exchange hold."""
        sig = _signal(exchange="BIST", price=50.0, confidence=0.65)
        p = _portfolio()
        trade = open_trade(sig, p)
        assert trade is not None
        assert trade.max_hold_hours == _HOLD_BIST


# ── Bug class 6: MIN_NOTIONAL guard, vol_scale caps ──────────────────────────

class TestSizingGuards:
    def test_below_min_notional_returns_none(self):
        """Tiny balance must not produce a trade below MIN_NOTIONAL."""
        # With balance=50 and fraction=0.035 → notional=1.75 < MIN_NOTIONAL=20
        sig = _signal(price=100.0)
        p = _portfolio(balance=50.0)
        trade = open_trade(sig, p)
        assert trade is None

    def test_zero_price_returns_none(self):
        sig = _signal(price=0.0)
        p = _portfolio()
        trade = open_trade(sig, p)
        assert trade is None

    def test_negative_price_returns_none(self):
        sig = _signal(price=-1.0)
        p = _portfolio()
        trade = open_trade(sig, p)
        assert trade is None

    def test_volume_cap_limits_notional(self):
        """volume_cap_usd must cap position size."""
        sig = _signal(price=100.0)
        sig.volume_cap_usd = 25.0  # much less than 10_000 * 0.035 = 350
        p = _portfolio()
        trade = open_trade(sig, p)
        assert trade is not None
        assert trade.notional <= 25.0 + 0.01  # tiny float wiggle room

    def test_vol_scale_applied(self):
        """vol_scale of 0.5 must halve the notional vs vol_scale=1.0."""
        sig1 = _signal(price=100.0)
        sig1.vol_scale = 1.0
        p1 = _portfolio()
        t1 = open_trade(sig1, p1)

        sig2 = _signal(price=100.0)
        sig2.vol_scale = 0.5
        p2 = _portfolio()
        t2 = open_trade(sig2, p2)

        assert t1 is not None and t2 is not None
        assert abs(t2.notional - t1.notional * 0.5) < 0.01

    def test_vol_scale_capped_at_1_5(self):
        """vol_scale > 1.5 must be clamped to 1.5 (not allow unbounded sizing)."""
        sig = _signal(price=100.0)
        sig.vol_scale = 3.0  # well above cap
        p = _portfolio()
        trade = open_trade(sig, p)

        sig_baseline = _signal(price=100.0)
        sig_baseline.vol_scale = 1.5
        p2 = _portfolio()
        trade_cap = open_trade(sig_baseline, p2)

        assert trade is not None and trade_cap is not None
        assert abs(trade.notional - trade_cap.notional) < 0.01

    def test_vol_scale_tiny_skipped_by_min_notional(self):
        """vol_scale 0.01 produces notional below MIN_NOTIONAL — trade rejected (None).
        Removed the 0.3 floor so derisk multipliers (drawdown_scale=0.4 etc.) are not
        defeated by an artificial minimum — MIN_NOTIONAL guards against micro-trades instead."""
        sig = _signal(price=100.0)
        sig.vol_scale = 0.01
        p = _portfolio()
        trade = open_trade(sig, p)
        assert trade is None  # too small after vol scaling

    def test_slippage_direction_long(self):
        """Long entries get adverse (higher) slippage."""
        sig = _signal(direction="long", price=100.0)
        p = _portfolio()
        trade = open_trade(sig, p)
        assert trade is not None
        assert trade.entry_price == 100.0 * (1 + SLIPPAGE_PCT)

    def test_slippage_direction_short(self):
        """Short entries get adverse (lower) slippage."""
        sig = _signal(direction="short", price=100.0)
        p = _portfolio()
        trade = open_trade(sig, p)
        assert trade is not None
        assert trade.entry_price == 100.0 * (1 - SLIPPAGE_PCT)


# ── PnL arithmetic ────────────────────────────────────────────────────────────

class TestCloseTrade:
    def _make_trade(self, direction="long", entry=100.0, size=10.0) -> PaperTrade:
        return PaperTrade(
            trade_id="t1",
            signal_id="s1",
            asset="BTC",
            direction=direction,
            size=size,
            entry_price=entry,
            notional=entry * size,
            fee=entry * size * FEE_RATE,
        )

    def test_long_profit(self):
        trade = self._make_trade("long", entry=100.0, size=1.0)
        pnl = close_trade(trade, exit_price=110.0)
        # gross=(110-100)*1=10; entry_fee=100*1*0.0005=0.05; exit_fee=1*110*0.0005=0.055
        assert abs(pnl - (10.0 - 0.05 - 0.055)) < 0.001

    def test_long_loss(self):
        trade = self._make_trade("long", entry=100.0, size=1.0)
        pnl = close_trade(trade, exit_price=90.0)
        # gross=-10; entry_fee=0.05; exit_fee=1*90*0.0005=0.045
        assert abs(pnl - (-10.0 - 0.05 - 0.045)) < 0.001

    def test_short_profit(self):
        """Short positions profit when price falls."""
        trade = self._make_trade("short", entry=100.0, size=1.0)
        pnl = close_trade(trade, exit_price=90.0)
        # gross=(100-90)*1=10; entry_fee=0.05; exit_fee=1*90*0.0005=0.045
        assert abs(pnl - (10.0 - 0.05 - 0.045)) < 0.001

    def test_short_loss(self):
        """Short positions lose when price rises."""
        trade = self._make_trade("short", entry=100.0, size=1.0)
        pnl = close_trade(trade, exit_price=110.0)
        # gross=(100-110)*1=-10; entry_fee=0.05; exit_fee=1*110*0.0005=0.055
        assert abs(pnl - (-10.0 - 0.05 - 0.055)) < 0.001

    def test_close_trade_sets_exit_price(self):
        trade = self._make_trade()
        close_trade(trade, 105.0)
        assert trade.exit_price == 105.0

    def test_close_trade_sets_pnl(self):
        trade = self._make_trade()
        pnl = close_trade(trade, 105.0)
        assert trade.pnl == pnl
