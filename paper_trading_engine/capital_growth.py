"""Capital Growth Framework — equity-phased position limits and sizing.

Phases by live account equity:
  P1 $0-200:   1 position, 2x leverage, ×0.8 size (capital preservation)
  P2 $200-500: 2 positions, 2x leverage, ×1.0 size (growth mode)
  P3 $500-1k:  3 positions, 3x leverage, ×1.0 size (scaling)
  P4 $1000+:   current max settings, ×1.0 size

Hysteresis of ±5% on boundaries prevents phase flapping.
Monthly trajectory check: <2% growth vs 30d ago → size multiplier tightened ×0.9.
Runway alert: equity < $120 → Discord alert + force P1.

Enable: always active (reads DB for snapshots, never blocks if DB unavailable).
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import structlog

logger = structlog.get_logger(__name__)

# Phase boundaries (equity in USD)
_PHASES = [
    # (min_equity, phase, max_positions, leverage_cap, size_multiplier)
    (0.0,    1, 1, 2, 0.8),
    (200.0,  2, 2, 2, 1.0),
    (500.0,  3, 3, 3, 1.0),
    (1000.0, 4, 4, 3, 1.0),  # P4 uses engine defaults for max_positions
]
_HYSTERESIS_PCT = 0.05   # 5% band on phase-up, 5% band on phase-down
_RUNWAY_ALERT_USD = 120.0
_MONTHLY_TARGET_PCT = 0.02  # 2% monthly growth target

# How often to write a snapshot (seconds) and check monthly trajectory (seconds)
_SNAPSHOT_INTERVAL_S = 86400.0   # daily
_MONTHLY_CHECK_INTERVAL_S = 86400.0


@dataclass
class GrowthDecision:
    phase: int
    max_positions: int
    leverage_cap: int
    size_multiplier: float
    runway_warning: bool = False


class CapitalGrowthFramework:
    def __init__(self) -> None:
        self._current_phase: int = 1
        self._last_snapshot_at: float = 0.0
        self._last_monthly_check_at: float = 0.0
        self._monthly_size_penalty: float = 1.0  # tightened if growth below target
        self._runway_alerted: bool = False

    def evaluate(self, equity: float) -> GrowthDecision:
        """Determine current growth phase from equity. Pure (no DB/Redis)."""
        # Natural phase: highest phase whose min_eq is satisfied
        natural_ph = 1
        for min_eq, ph, *_ in _PHASES:
            if equity >= min_eq:
                natural_ph = ph

        # Hysteresis: prevent flapping near boundaries
        target_ph = natural_ph
        if natural_ph > self._current_phase:
            # Phase-up only when equity clears boundary + band
            boundary = next(r[0] for r in _PHASES if r[1] == natural_ph)
            if equity < boundary * (1 + _HYSTERESIS_PCT):
                target_ph = self._current_phase
        elif natural_ph < self._current_phase:
            # Phase-down only when equity drops below boundary - band
            boundary = next(r[0] for r in _PHASES if r[1] == self._current_phase)
            if equity >= boundary * (1 - _HYSTERESIS_PCT):
                target_ph = self._current_phase

        _, phase, max_pos, lev_cap, size_mult = next(r for r in _PHASES if r[1] == target_ph)

        if self._current_phase != phase:
            logger.info("capital_growth_phase_change",
                        old=self._current_phase, new=phase,
                        equity=round(equity, 2))
            self._current_phase = phase

        runway = equity < _RUNWAY_ALERT_USD
        if runway and phase > 1:
            phase, max_pos, lev_cap, size_mult = 1, 1, 2, 0.8
            self._current_phase = 1

        effective_size = size_mult * self._monthly_size_penalty
        return GrowthDecision(
            phase=phase,
            max_positions=max_pos,
            leverage_cap=lev_cap,
            size_multiplier=round(effective_size, 3),
            runway_warning=runway,
        )

    async def tick(self, pool, equity: float, notifier=None,
                   decision: "GrowthDecision | None" = None) -> None:
        """Called every engine tick (after equity sync). Handles snapshots + monthly check.

        Pass the already-computed GrowthDecision from evaluate() to avoid a second
        evaluate() call (engine.py calls evaluate() before tick()).
        """
        now = time.monotonic()

        if now - self._last_snapshot_at > _SNAPSHOT_INTERVAL_S:
            await self._log_snapshot(pool, equity, decision)
            self._last_snapshot_at = now

        if now - self._last_monthly_check_at > _MONTHLY_CHECK_INTERVAL_S:
            await self._check_monthly_trajectory(pool, equity)
            self._last_monthly_check_at = now

        # Runway alert (one-shot per session)
        if equity < _RUNWAY_ALERT_USD and not self._runway_alerted and notifier:
            self._runway_alerted = True
            try:
                await notifier.send_message(
                    f"⚠️ **CAPITAL RUNWAY WARNING** — equity ${equity:.2f} below ${_RUNWAY_ALERT_USD:.0f}. "
                    f"Forced to Phase 1 (1 position, ×0.8 size). Top up or reduce risk."
                )
            except Exception:
                pass
        elif equity >= _RUNWAY_ALERT_USD:
            self._runway_alerted = False

    async def _log_snapshot(self, pool, equity: float,
                             decision: "GrowthDecision | None" = None) -> None:
        """Write daily growth log entry. Accepts pre-computed decision to avoid double evaluate()."""
        if pool is None:
            return
        if decision is None:
            decision = self.evaluate(equity)
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO capital_growth_log
                           (phase, equity, max_positions, leverage_cap, size_multiplier)
                       VALUES ($1,$2,$3,$4,$5)""",
                    decision.phase, equity, decision.max_positions,
                    decision.leverage_cap, decision.size_multiplier,
                )
        except Exception as exc:
            logger.warning("capital_growth_log_failed", error=str(exc))

    async def _check_monthly_trajectory(self, pool, equity: float) -> None:
        """Compare current equity to 30d-ago snapshot. Tighten sizing if <2% growth."""
        if pool is None:
            return
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """SELECT equity FROM capital_growth_log
                        WHERE captured_at < NOW() - INTERVAL '29 days'
                        ORDER BY captured_at DESC LIMIT 1""",
                )
            if not row:
                return
            equity_30d_ago = float(row["equity"])
            if equity_30d_ago <= 0:
                return
            growth_pct = (equity - equity_30d_ago) / equity_30d_ago
            if growth_pct < _MONTHLY_TARGET_PCT:
                self._monthly_size_penalty = 0.9
                logger.info("capital_growth_below_target",
                            growth_pct=round(growth_pct * 100, 2),
                            penalty=self._monthly_size_penalty)
            else:
                self._monthly_size_penalty = 1.0
        except Exception as exc:
            logger.warning("capital_growth_monthly_check_failed", error=str(exc))
