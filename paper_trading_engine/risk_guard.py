"""Risk guard — last line of defense before any live order.

Checks (in order):
1. Kill switch: Redis key  trading:kill_switch  set to "1" → halt everything
2. Daily loss limit: sum of closed PnL today vs MAX_DAILY_LOSS env var
3. Position already open for this asset → skip (deduplication)

Usage:
    guard = RiskGuard(redis_client, db_pool)
    ok, reason = await guard.check(signal, portfolio)
    if not ok:
        ...skip...
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone

import structlog

logger = structlog.get_logger(__name__)

_KILL_SWITCH_KEY = "trading:kill_switch"
_DEFAULT_MAX_DAILY_LOSS = 50.0   # USD — conservative default


class RiskGuard:
    def __init__(self, redis, db_pool, notifier=None) -> None:
        self._redis   = redis
        self._db      = db_pool
        self._max_daily_loss = float(os.getenv("MAX_DAILY_LOSS", str(_DEFAULT_MAX_DAILY_LOSS)))
        self._live_daily_loss_limit = float(os.getenv("LIVE_DAILY_LOSS_LIMIT", "50.0"))
        self._notifier = notifier
        self._daily_halt_notified_date: date | None = None  # fire the alert only once per calendar day

    async def check(self, asset: str, direction: str, portfolio) -> tuple[bool, str]:
        """Return (allowed, reason). Call before every broker order."""

        # 1. Kill switch
        if await self._kill_switch_active():
            return False, "kill switch active"

        # 2. Daily loss limit
        daily_loss = await self._daily_loss_usd()
        from broker_factory import trading_mode as _trading_mode
        effective_limit = (
            min(self._max_daily_loss, self._live_daily_loss_limit)
            if _trading_mode() == "live"
            else self._max_daily_loss
        )
        if daily_loss >= effective_limit:
            logger.warning(
                "risk_guard_daily_loss_halt",
                daily_loss=round(daily_loss, 2),
                limit=effective_limit,
            )
            if self._notifier and self._daily_halt_notified_date != date.today():
                self._daily_halt_notified_date = date.today()
                try:
                    await self._notifier.daily_loss_halt(daily_loss, effective_limit)
                except Exception:
                    pass
            return False, f"daily loss ${daily_loss:.2f} >= limit ${effective_limit:.2f}"

        # 3. Position already open for this asset
        if asset in portfolio.open_positions:
            return False, f"position already open for {asset}"

        return True, "ok"

    async def set_kill_switch(self, on: bool) -> None:
        """Enable or disable the kill switch. Survives restarts (persisted in Redis)."""
        if on:
            await self._redis.set(_KILL_SWITCH_KEY, "1")
            logger.warning("risk_guard_kill_switch_ON")
        else:
            await self._redis.delete(_KILL_SWITCH_KEY)
            logger.info("risk_guard_kill_switch_OFF")

    async def kill_switch_status(self) -> bool:
        return await self._kill_switch_active()

    # ── internals ────────────────────────────────────────────────────────────

    async def _kill_switch_active(self) -> bool:
        try:
            val = await self._redis.get(_KILL_SWITCH_KEY)
            self._redis_consecutive_failures = 0
            return val in (b"1", "1")
        except Exception as exc:
            failures = getattr(self, "_redis_consecutive_failures", 0) + 1
            self._redis_consecutive_failures = failures
            logger.error("risk_guard_redis_error", error=str(exc), consecutive=failures)
            if failures >= 3:
                is_live = os.getenv("TRADING_MODE", "paper").strip().lower() == "live"
                if self._notifier:
                    try:
                        if is_live:
                            await self._notifier.send_message(
                                f"🚨 RiskGuard: Redis unreachable ({failures} consecutive failures) "
                                f"in LIVE mode — kill switch FAILING CLOSED. Trading halted until Redis recovers."
                            )
                        else:
                            await self._notifier.send_message(
                                f"⚠️ RiskGuard: Redis unreachable ({failures} consecutive failures) "
                                f"— kill switch bypassed (paper mode). Check Redis connectivity immediately."
                            )
                    except Exception:
                        pass
                # Sprint 57: fail-closed in live mode — halt trading when Redis is unavailable.
                # Paper mode retains fail-open behaviour to avoid blocking simulation.
                if is_live:
                    logger.error("risk_guard_kill_switch_fail_closed",
                                 reason="Redis unavailable in live mode — halting trading")
                    return True  # fail-closed: halt
            return False  # paper mode or < 3 failures: fail-open

    async def _daily_loss_usd(self) -> float:
        """Daily loss from two sources: DB closed trades + Binance wallet delta.

        Takes the larger of the two so that DB corruption or unrealized losses
        can't hide a real wallet drawdown from the daily-loss gate.
        """
        live_only = os.getenv("TRADING_MODE", "paper").strip().lower() == "live"
        try:
            # closed_at is stored UTC — derive the day boundary in UTC too, so the
            # gate window can't skew if the container clock is ever not UTC.
            today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            async with self._db.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT COALESCE(SUM(pnl), 0) AS total_loss
                      FROM paper_trades
                     WHERE closed_at >= $1
                       AND pnl < 0
                       AND ($2 = FALSE OR mode = 'live')
                    """,
                    today_start,
                    live_only,
                )
            db_loss = abs(float(row["total_loss"])) if row else 0.0
        except Exception as exc:
            logger.error("risk_guard_db_error_fail_closed", error=str(exc))
            return self._max_daily_loss

        # Wallet-delta check: compare Binance balance now vs start-of-day.
        # Only applies in live mode; Redis keys written by engine._sync_live_balance.
        wallet_loss = 0.0
        if live_only:
            try:
                from datetime import date as _date
                day_key = f"trading:wallet_start:{_date.today().isoformat()}"
                start_b = await self._redis.get(day_key)
                current_b = await self._redis.get("trading:wallet_current")
                if start_b and current_b:
                    wallet_loss = max(0.0, float(start_b) - float(current_b))
            except Exception:
                pass  # Redis unavailable: fall back to DB-only

        loss = max(db_loss, wallet_loss)
        if wallet_loss > db_loss and wallet_loss > 0:
            logger.info("risk_guard_wallet_delta_loss", wallet_loss=round(wallet_loss, 2),
                        db_loss=round(db_loss, 2))
        return loss
