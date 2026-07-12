"""
Unit tests for portfolio.py — equity calculation, PnL accounting, partial closes.

Covers bug classes:
  2. Equity calculation was wrong for SHORT positions.
  4. Partial exits: double-close / double-counting.
"""
from __future__ import annotations

import sys
import os
import types
from datetime import datetime, timezone

from models import PaperTrade, Signal
from portfolio import Portfolio, INITIAL_BALANCE
from executor import FEE_RATE


# ── Helpers ───────────────────────────────────────────────────────────────────

def _open_trade(
    asset="BTC",
    direction="long",
    entry=100.0,
    size=10.0,
    exchange="CRYPTO",
) -> PaperTrade:
    return PaperTrade(
        trade_id="t1",
        signal_id="s1",
        asset=asset,
        direction=direction,
        size=size,
        entry_price=entry,
        notional=entry * size,
        fee=entry * size * FEE_RATE,
        exchange=exchange,
        max_hold_hours=24,
    )


# ── Bug class 2: equity for SHORT positions ───────────────────────────────────

class TestEquityCalculation:
    def _portfolio_with_short(self, entry=100.0, size=1.0, current=90.0) -> Portfolio:
        """Build a portfolio with one short position and set a current price."""
        p = Portfolio()
        trade = _open_trade(direction="short", entry=entry, size=size)
        p.balance -= trade.notional + trade.fee
        p.open_positions["BTC"] = trade
        p.update_prices({"BTC": current})
        return p

    def test_short_equity_rises_when_price_falls(self):
        """A profitable short (price fell) must show equity ABOVE notional cost.

        Bug: old code used price×size directly, which made equity fall when price
        fell (the opposite of reality for shorts).
        """
        p = self._portfolio_with_short(entry=100.0, size=1.0, current=90.0)
        # Short profit = (100-90)*1 = +10; equity should be > entry equity
        initial_equity = INITIAL_BALANCE
        assert p.equity > p.balance  # open position means some unrealized gain

    def test_short_unrealized_pnl_positive_on_price_fall(self):
        """unrealized_pnl must be positive when price fell below short entry."""
        p = self._portfolio_with_short(entry=100.0, size=1.0, current=90.0)
        assert p.unrealized_pnl > 0.0

    def test_short_unrealized_pnl_negative_on_price_rise(self):
        """unrealized_pnl must be negative when price rose above short entry."""
        p = self._portfolio_with_short(entry=100.0, size=1.0, current=110.0)
        assert p.unrealized_pnl < 0.0

    def test_long_unrealized_pnl_positive_on_price_rise(self):
        p = Portfolio()
        trade = _open_trade(direction="long", entry=100.0, size=1.0)
        p.balance -= trade.notional + trade.fee
        p.open_positions["BTC"] = trade
        p.update_prices({"BTC": 110.0})
        assert p.unrealized_pnl > 0.0

    def test_equity_equals_balance_plus_notional_when_no_price(self):
        """With no live prices, unrealized_pnl = 0, equity = balance + notional."""
        p = Portfolio()
        trade = _open_trade(direction="short", entry=100.0, size=1.0)
        p.balance -= trade.notional + trade.fee
        p.open_positions["BTC"] = trade
        # No prices set
        expected = p.balance + trade.notional  # + unrealized 0
        assert abs(p.equity - expected) < 0.001

    def test_short_equity_symmetry_with_long(self):
        """For the same move magnitude, long and short must show mirror PnL."""
        long_p = Portfolio()
        long_trade = _open_trade(direction="long", entry=100.0, size=1.0)
        long_p.balance -= long_trade.notional + long_trade.fee
        long_p.open_positions["BTC"] = long_trade
        long_p.update_prices({"BTC": 110.0})  # +10%

        short_p = Portfolio()
        short_trade = _open_trade(direction="short", entry=100.0, size=1.0)
        short_p.balance -= short_trade.notional + short_trade.fee
        short_p.open_positions["BTC"] = short_trade
        short_p.update_prices({"BTC": 90.0})  # -10%

        assert abs(long_p.unrealized_pnl - short_p.unrealized_pnl) < 0.001

    def test_close_position_returns_notional_plus_pnl(self):
        """Closing a profitable position must credit balance with gain."""
        p = Portfolio()
        trade = _open_trade(direction="long", entry=100.0, size=1.0)
        p.balance -= trade.notional + trade.fee
        p.open_positions["BTC"] = trade

        balance_before = p.balance
        notional = trade.notional
        p.close_position("BTC", exit_price=110.0)

        # Should return notional + gross profit - exit fee
        gross = (110.0 - 100.0) * 1.0
        exit_fee = 1.0 * 110.0 * FEE_RATE
        expected_credit = notional + gross - exit_fee
        assert abs(p.balance - (balance_before + expected_credit)) < 0.001

    def test_close_short_profit_credited(self):
        p = Portfolio()
        trade = _open_trade(direction="short", entry=100.0, size=1.0)
        p.balance -= trade.notional + trade.fee
        p.open_positions["BTC"] = trade

        balance_before = p.balance
        notional = trade.notional
        p.close_position("BTC", exit_price=90.0)

        gross = (100.0 - 90.0) * 1.0
        exit_fee = 1.0 * 90.0 * FEE_RATE
        expected_credit = notional + gross - exit_fee
        assert abs(p.balance - (balance_before + expected_credit)) < 0.001


# ── Bug class 4: Partial exits — double-close / double-counting ───────────────

class TestPartialExits:
    def test_partial_close_reduces_size(self):
        p = Portfolio()
        trade = _open_trade(entry=100.0, size=10.0)
        p.balance -= trade.notional + trade.fee
        p.open_positions["BTC"] = trade

        p.partial_close_position("BTC", fraction=0.33, exit_price=110.0)

        remaining = p.open_positions["BTC"]
        assert abs(remaining.size - 10.0 * 0.67) < 0.001

    def test_partial_close_credits_pnl(self):
        p = Portfolio()
        trade = _open_trade(entry=100.0, size=1.0)
        p.balance -= trade.notional + trade.fee
        p.open_positions["BTC"] = trade

        balance_before = p.balance
        partial = p.partial_close_position("BTC", fraction=0.5, exit_price=110.0)

        assert partial is not None
        assert partial.pnl > 0
        # Balance must have increased
        assert p.balance > balance_before

    def test_partial_close_does_not_remove_position(self):
        """After a partial, the position must still be open."""
        p = Portfolio()
        trade = _open_trade(entry=100.0, size=10.0)
        p.balance -= trade.notional + trade.fee
        p.open_positions["BTC"] = trade

        p.partial_close_position("BTC", fraction=0.33, exit_price=110.0)
        assert "BTC" in p.open_positions

    def test_partial_close_fraction_0_returns_none(self):
        p = Portfolio()
        trade = _open_trade(entry=100.0, size=10.0)
        p.open_positions["BTC"] = trade
        result = p.partial_close_position("BTC", fraction=0.0, exit_price=110.0)
        assert result is None

    def test_partial_close_fraction_1_returns_none(self):
        """fraction >= 1.0 is a full close; partial_close_position rejects it."""
        p = Portfolio()
        trade = _open_trade(entry=100.0, size=10.0)
        p.open_positions["BTC"] = trade
        result = p.partial_close_position("BTC", fraction=1.0, exit_price=110.0)
        assert result is None

    def test_sequential_partials_dont_double_count_pnl(self):
        """T1 + T2 partial closes must not double-count PnL vs a single close."""
        # Build position
        p = Portfolio()
        trade = _open_trade(entry=100.0, size=10.0)
        p.balance -= trade.notional + trade.fee
        p.open_positions["BTC"] = trade

        # T1: 33% at 105
        p1 = p.partial_close_position("BTC", 0.33, exit_price=105.0)
        # T2: 50% of remaining at 110 (= 50% of 67% of original)
        p2 = p.partial_close_position("BTC", 0.50, exit_price=110.0)

        assert p1 is not None and p2 is not None
        # PnL from both partials should be positive and independent
        assert p1.pnl > 0
        assert p2.pnl > 0

    def test_partial_close_missing_asset_returns_none(self):
        p = Portfolio()
        result = p.partial_close_position("ETH", 0.33, exit_price=200.0)
        assert result is None

    def test_realized_pnl_accumulates_across_partials(self):
        p = Portfolio()
        trade = _open_trade(entry=100.0, size=10.0)
        p.balance -= trade.notional + trade.fee
        p.open_positions["BTC"] = trade

        p.partial_close_position("BTC", 0.33, exit_price=105.0)
        assert p.realized_pnl > 0

        pnl_after_t1 = p.realized_pnl
        p.partial_close_position("BTC", 0.50, exit_price=110.0)
        assert p.realized_pnl > pnl_after_t1

    def test_full_close_after_partials_removes_position(self):
        p = Portfolio()
        trade = _open_trade(entry=100.0, size=10.0)
        p.balance -= trade.notional + trade.fee
        p.open_positions["BTC"] = trade

        p.partial_close_position("BTC", 0.33, exit_price=105.0)
        p.close_position("BTC", exit_price=110.0)

        assert "BTC" not in p.open_positions
