"""Adaptive Position Sizing — per-asset performance + portfolio heat + streak.

Three components (regime component intentionally excluded — owned by regime_filter):
  1. per_asset_mult:   last 10 closed trades for this asset
                       WR>60% → ×1.2, WR<30% → ×0.6, else ×1.0
  2. heat_mult:        unrealized PnL of open positions; negative → ×0.8
  3. streak_mult:      last 3 closed trades all losses → ×0.7 until next win

Redis cache: per-asset and streak queries cached 300s to avoid per-signal DB load.
Heat is computed live from in-memory open_positions (no cache, no DB).

Enable: gate behind ADAPTIVE_SIZING_ENABLED=true (default false).
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

_ENABLED = os.getenv("ADAPTIVE_SIZING_ENABLED", "false").lower() in ("1", "true", "yes")
_CACHE_TTL_S = 300.0
_ASSET_KEY = "paper_trading:adaptive:{asset}"
_STREAK_KEY = "paper_trading:adaptive:streak"


class AdaptiveSizer:
    def __init__(self, pool, redis) -> None:
        self._pool = pool
        self._redis = redis

    async def multiplier(
        self, asset: str, open_positions: dict, live_prices: dict
    ) -> tuple[float, dict]:
        """Return (combined_multiplier, breakdown).

        combined = per_asset * heat * streak
        Regime multiplier excluded — owned by risk.regime_filter.
        Returns (1.0, {}) instantly if ADAPTIVE_SIZING_ENABLED=false.
        """
        if not _ENABLED:
            return 1.0, {}

        per_asset = await self._per_asset_mult(asset)
        heat = self._heat_mult(open_positions, live_prices)
        streak = await self._streak_mult()

        combined = per_asset * heat * streak
        breakdown = {
            "per_asset": round(per_asset, 3),
            "heat": round(heat, 3),
            "streak": round(streak, 3),
            "combined": round(combined, 3),
        }
        if combined != 1.0:
            logger.debug("adaptive_sizer", asset=asset, **breakdown)
        return combined, breakdown

    async def _per_asset_mult(self, asset: str) -> float:
        cache_key = _ASSET_KEY.format(asset=asset)
        cached = await self._get_cache(cache_key)
        if cached is not None:
            return cached

        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    """SELECT pnl FROM paper_trades
                        WHERE asset = $1 AND closed_at IS NOT NULL
                          AND close_reason <> 'dedup_cleanup'
                        ORDER BY closed_at DESC LIMIT 10""",
                    asset,
                )
        except Exception as exc:
            logger.warning("adaptive_sizer_per_asset_query_failed", asset=asset, error=str(exc))
            return 1.0

        if len(rows) < 3:
            await self._set_cache(cache_key, 1.0)
            return 1.0

        wins = sum(1 for r in rows if (r["pnl"] or 0.0) > 0)
        wr = wins / len(rows)
        mult = 1.2 if wr > 0.6 else (0.6 if wr < 0.3 else 1.0)
        await self._set_cache(cache_key, mult)
        return mult

    def _heat_mult(self, open_positions: dict, live_prices: dict) -> float:
        """Portfolio heat: if total unrealized PnL of open positions is negative → ×0.8."""
        total_unrealized = 0.0
        for asset, pos in open_positions.items():
            current = live_prices.get(asset, pos.entry_price)
            if current and pos.entry_price:
                if pos.direction == "long":
                    total_unrealized += (current - pos.entry_price) * pos.size
                else:
                    total_unrealized += (pos.entry_price - current) * pos.size
        return 0.8 if total_unrealized < 0 else 1.0

    async def _streak_mult(self) -> float:
        cached = await self._get_cache(_STREAK_KEY)
        if cached is not None:
            return cached

        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    """SELECT pnl FROM paper_trades
                        WHERE closed_at IS NOT NULL
                          AND close_reason <> 'dedup_cleanup'
                        ORDER BY closed_at DESC LIMIT 3""",
                )
        except Exception as exc:
            logger.warning("adaptive_sizer_streak_query_failed", error=str(exc))
            return 1.0

        if len(rows) < 3:
            await self._set_cache(_STREAK_KEY, 1.0)
            return 1.0

        all_losses = all((r["pnl"] or 0.0) <= 0 for r in rows)
        mult = 0.7 if all_losses else 1.0
        await self._set_cache(_STREAK_KEY, mult)
        return mult

    async def _get_cache(self, key: str) -> float | None:
        if self._redis is None:
            return None
        try:
            val = await self._redis.get(key)
            if val is not None:
                data = json.loads(val)
                if time.time() - data["ts"] < _CACHE_TTL_S:
                    return float(data["v"])
        except Exception:
            pass
        return None

    async def _set_cache(self, key: str, value: float) -> None:
        if self._redis is None:
            return
        try:
            await self._redis.set(key, json.dumps({"v": value, "ts": time.time()}), ex=600)
        except Exception:
            pass
