"""In-memory portfolio state."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from executor import FEE_RATE, close_trade, open_trade
from models import PaperTrade, Signal

INITIAL_BALANCE = 10_000.0
# Exchange-specific hold limits (hours). signal_scores proves TDE edge decays after 1h;
# 6h accuracy = 42.5% (below random). Crypto max=2h. Stocks need multi-day for P&L after fees.
MAX_HOLD_HOURS = 2       # default (CRYPTO): signal half-life is 1h, 2h captures the edge
MAX_HOLD_HOURS_STOCK = 48  # NASDAQ/NYSE/CBOE: daily candles need 2-day hold for fee-beating moves
MAX_HOLD_HOURS_BIST  = 6   # BIST: ~1 trading session


class Portfolio:
    def __init__(self) -> None:
        self.balance: float = INITIAL_BALANCE
        self.open_positions: dict[str, PaperTrade] = {}   # asset → trade
        self.closed_trades: list[PaperTrade] = []
        self.realized_pnl: float = 0.0
        self._live_prices: dict[str, float] = {}  # last known market prices

    # ── Core operations ───────────────────────────────────────────────────────

    def open_position(self, signal: Signal) -> "PaperTrade | None":
        trade = open_trade(signal, self)
        if trade is None:
            return None
        self.balance -= trade.notional + trade.fee
        self.open_positions[trade.asset] = trade
        return trade

    def register_live_fill(self, signal: Signal, order) -> "PaperTrade | None":
        """Record a REAL broker fill 1:1 — size, price, notional, and fee come
        from the exchange, not from paper sizing (Sprint 42).

        Paper sizing must never apply to live fills: it sizes off
        portfolio.balance, which can diverge wildly from the real wallet
        (paper $9.6k vs a $100 pilot), and its MIN_NOTIONAL floor would
        reject small real fills entirely — orphaning a position the
        exchange already holds.
        """
        from executor import _max_hold_hours
        if order.qty <= 0 or order.fill_price <= 0:
            return None
        trade = PaperTrade(
            trade_id=str(uuid.uuid4()),
            signal_id=signal.signal_id,
            asset=signal.asset,
            direction=signal.direction,
            size=order.qty,
            entry_price=order.fill_price,
            notional=order.notional,
            fee=order.fee,
            exchange=signal.exchange,
            max_hold_hours=_max_hold_hours(signal.confidence, signal.exchange, signal.direction, regime=getattr(signal, "macro_regime", "")),
            router_action=getattr(signal, "router_action", ""),
            router_edge=getattr(signal, "router_edge", 0.0),
            is_live=True,
        )
        self.balance -= trade.notional + trade.fee
        self.open_positions[trade.asset] = trade
        return trade

    def close_position(self, asset: str, exit_price: float) -> Optional[PaperTrade]:
        trade = self.open_positions.pop(asset, None)
        if trade is None:
            return None
        self._live_prices.pop(asset, None)
        entry_fee = trade.fee  # entry leg, already debited from balance at open
        net_pnl = close_trade(trade, exit_price)
        # net_pnl = gross - entry_fee - exit_fee (true realized PnL). Balance only needs the
        # exit-side adjustment — the entry fee was already debited at open, so add it back here.
        self.balance += trade.notional + net_pnl + entry_fee
        self.realized_pnl += net_pnl
        self.closed_trades.append(trade)
        return trade

    def partial_close_position(self, asset: str, fraction: float, exit_price: float) -> Optional[PaperTrade]:
        """Close `fraction` (0–1) of an open position in-place. Returns a synthetic closed trade."""
        trade = self.open_positions.get(asset)
        if trade is None:
            return None
        if not (0.0 < fraction < 1.0):
            return None

        close_size = trade.size * fraction
        close_notional = close_size * trade.entry_price

        if trade.direction == "long":
            gross_pnl = (exit_price - trade.entry_price) * close_size
        else:
            gross_pnl = (trade.entry_price - exit_price) * close_size

        # Exit fee on the exit transaction value (not entry — that would over/undercharge by
        # the move × size × rate). FEE_RATE matches the full-close path + broker taker rate.
        # entry_fee_portion was already debited at open; it's subtracted in net_pnl (true PnL)
        # but added back to the balance credit below, so each leg hits the ledger exactly once.
        entry_fee_portion = trade.fee * fraction
        exit_fee = close_size * exit_price * FEE_RATE
        net_pnl = round(gross_pnl - entry_fee_portion - exit_fee, 4)

        closed = PaperTrade(
            trade_id=str(uuid.uuid4()),
            signal_id=trade.signal_id,
            asset=trade.asset,
            exchange=trade.exchange,
            direction=trade.direction,
            size=close_size,
            entry_price=trade.entry_price,
            notional=close_notional,
            fee=round(entry_fee_portion + exit_fee, 6),
            max_hold_hours=trade.max_hold_hours,
            opened_at=trade.opened_at,
            closed_at=datetime.now(timezone.utc),
            exit_price=exit_price,
            pnl=net_pnl,
            is_live=getattr(trade, "is_live", False),
        )

        # Reduce remaining open position
        trade.size = round(trade.size * (1 - fraction), 8)
        trade.notional = round(trade.notional * (1 - fraction), 4)
        trade.fee = round(trade.fee * (1 - fraction), 6)

        self.balance += close_notional + net_pnl + entry_fee_portion
        self.realized_pnl += net_pnl
        self.closed_trades.append(closed)
        return closed

    # ── Live price management ─────────────────────────────────────────────────

    def update_prices(self, prices: dict[str, float]) -> None:
        """Update known market prices for open positions."""
        for asset, price in prices.items():
            if price > 0:
                self._live_prices[asset] = price

    # ── Derived state ─────────────────────────────────────────────────────────

    @property
    def unrealized_pnl(self) -> float:
        total = 0.0
        for asset, pos in self.open_positions.items():
            price = self._live_prices.get(asset, 0.0)
            if price <= 0 or pos.entry_price <= 0:
                continue
            if pos.direction == "long":
                total += (price - pos.entry_price) * pos.size
            else:
                total += (pos.entry_price - price) * pos.size
        return round(total, 4)

    @property
    def equity(self) -> float:
        # balance has notional deducted; add notional back + unrealized P&L.
        # Using price×size directly would show shorts losing equity when they profit
        # (price falls → price×size falls → equity appears to shrink when we're winning).
        total_notional = sum(t.notional for t in self.open_positions.values())
        return round(self.balance + total_notional + self.unrealized_pnl, 4)

    def update_equity(self) -> float:
        return self.equity

    # ── Age-out check ─────────────────────────────────────────────────────────

    def aged_positions(self) -> list[str]:
        """Return asset names for positions held past their per-trade max_hold_hours."""
        now = datetime.now(timezone.utc)
        aged = []
        for asset, trade in self.open_positions.items():
            opened = trade.opened_at
            if opened.tzinfo is None:
                opened = opened.replace(tzinfo=timezone.utc)
            hold_limit = getattr(trade, "max_hold_hours", MAX_HOLD_HOURS)
            if (now - opened).total_seconds() > hold_limit * 3600:
                aged.append(asset)
        return aged

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict:
        return {
            "balance": round(self.balance, 4),
            "equity": round(self.equity, 4),
            "open_positions": len(self.open_positions),
            "open_assets": list(self.open_positions.keys()),
            "realized_pnl": round(self.realized_pnl, 4),
            "unrealized_pnl": self.unrealized_pnl,
            "total_trades_closed": len(self.closed_trades),
        }
