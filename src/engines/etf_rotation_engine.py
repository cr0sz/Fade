"""
ETF Rotation Intelligence Engine — Level 3.

Detects sector rotation velocity, capital migration between sectors, and
identifies leading vs lagging sectors using composite scores of sector ETFs
already tracked in market_trends.

Does NOT modify any Level 1/2 engine.
Reads from market_trends (existing); writes to etf_rotations (new).

Architecture:
  - Fixed ETF→sector mapping (16 major US sector/theme ETFs)
  - Rotation velocity: delta of composite_score vs previous cycle (Redis-backed)
  - Sector ranks updated each cycle: top 3 = leading, bottom 3 = lagging
  - Cross-asset coupling: equities↔bonds, tech↔energy detected via correlation proxy
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import asyncpg

from src.core.logging import get_logger
from src.models.market import ETFRotation

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

# ── ETF → Sector Mapping ───────────────────────────────────────────────────────
# Covers major SPDR sector ETFs + key thematic vehicles
ETF_SECTOR_MAP: dict[str, str] = {
    # SPDR Sector ETFs
    "XLK":  "technology",
    "XLF":  "financials",
    "XLE":  "energy",
    "XLV":  "healthcare",
    "XLI":  "industrials",
    "XLP":  "consumer_staples",
    "XLY":  "consumer_discretionary",
    "XLU":  "utilities",
    "XLC":  "communication_services",
    "XLRE": "real_estate",
    "XLB":  "materials",
    # Macro / cross-asset benchmarks
    "SPY":  "broad_market",
    "QQQ":  "tech_growth",
    "IWM":  "small_cap",
    "GLD":  "gold",
    "TLT":  "bonds",
}

_LEADING_COUNT   = 3   # top N sectors = leading
_LAGGING_COUNT   = 3   # bottom N sectors = lagging
_VELOCITY_TTL    = 3600 * 2   # 2h Redis TTL for prev scores
_PREV_SCORE_KEY  = "etf:prev_score:"   # + etf_ticker → float

# Velocity threshold: min delta to classify as directional rotation
_VELOCITY_INFLOW_THRESHOLD  =  0.05
_VELOCITY_OUTFLOW_THRESHOLD = -0.05


def _rotation_direction(velocity: float) -> str:
    if velocity >= _VELOCITY_INFLOW_THRESHOLD:
        return "inflow"
    if velocity <= _VELOCITY_OUTFLOW_THRESHOLD:
        return "outflow"
    return "neutral"


@dataclass
class ETFRotationCycleResult:
    sectors_analyzed: int = 0
    leading_sectors: list[str] = field(default_factory=list)
    lagging_sectors: list[str] = field(default_factory=list)
    fastest_rotation: str | None = None
    fastest_velocity: float = 0.0
    equities_bonds_signal: str = "neutral"  # risk_on | risk_off | neutral
    tech_energy_signal: str = "neutral"     # tech_outperform | energy_outperform | neutral
    errors: int = 0


class ETFRotationEngine:
    """
    Computes sector rotation velocity from existing market_trends data.
    Redis stores the previous-cycle ETF scores for delta computation.
    """

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
    ) -> None:
        self._pool = db_pool
        self._redis = redis

    async def run_cycle(self) -> ETFRotationCycleResult:
        result = ETFRotationCycleResult()

        # Fetch composite scores for all tracked ETF tickers
        trend_scores = await self._fetch_etf_scores()

        rotations: list[ETFRotation] = []
        for etf, sector in ETF_SECTOR_MAP.items():
            try:
                score    = trend_scores.get(etf, 0.0)
                prev     = await self._get_prev_score(etf)
                velocity = round(score - prev, 5)
                await self._store_prev_score(etf, score)

                rot = ETFRotation(
                    sector=sector,
                    etf_ticker=etf,
                    composite_score=round(score, 4),
                    rotation_velocity=velocity,
                    rotation_direction=_rotation_direction(velocity),
                    computed_at=datetime.now(timezone.utc),
                )
                rotations.append(rot)
            except Exception as exc:
                logger.error("etf_rotation_error", etf=etf, error=str(exc))
                result.errors += 1

        if not rotations:
            return result

        # Rank sectors by composite score (highest = most capital)
        rotations.sort(key=lambda r: r.composite_score, reverse=True)
        for i, rot in enumerate(rotations):
            rot.rank_position = i + 1
            rot.is_leading = i < _LEADING_COUNT
            rot.is_lagging = i >= len(rotations) - _LAGGING_COUNT

        result.sectors_analyzed = len(rotations)
        result.leading_sectors = [r.sector for r in rotations[:_LEADING_COUNT]]
        result.lagging_sectors = [r.sector for r in rotations[-_LAGGING_COUNT:]]

        # Fastest rotation (highest abs velocity)
        fastest = max(rotations, key=lambda r: abs(r.rotation_velocity))
        result.fastest_rotation = fastest.sector
        result.fastest_velocity = fastest.rotation_velocity

        # Cross-asset signals
        result.equities_bonds_signal = self._equities_bonds_signal(trend_scores)
        result.tech_energy_signal    = self._tech_energy_signal(trend_scores)

        # Persist
        for rot in rotations:
            try:
                await self._persist_rotation(rot)
            except Exception as exc:
                logger.error("etf_persist_error", sector=rot.sector, error=str(exc))
                result.errors += 1

        logger.info(
            "etf_rotation_cycle_complete",
            sectors=result.sectors_analyzed,
            leading=result.leading_sectors,
            lagging=result.lagging_sectors,
            fastest=result.fastest_rotation,
            equities_bonds=result.equities_bonds_signal,
        )
        return result

    def _equities_bonds_signal(self, scores: dict[str, float]) -> str:
        """
        Equities (SPY) up while bonds (TLT) down → risk_on capital rotation.
        Bonds up while equities down → risk_off flight.
        """
        spy = scores.get("SPY", 0.0)
        tlt = scores.get("TLT", 0.0)
        if spy > 0.1 and tlt < -0.05:
            return "risk_on"
        if tlt > 0.1 and spy < -0.05:
            return "risk_off"
        return "neutral"

    def _tech_energy_signal(self, scores: dict[str, float]) -> str:
        """
        Tech (XLK/QQQ) vs Energy (XLE) rotation proxy.
        """
        tech   = (scores.get("XLK", 0.0) + scores.get("QQQ", 0.0)) / 2
        energy = scores.get("XLE", 0.0)
        delta  = tech - energy
        if delta > 0.15:
            return "tech_outperform"
        if delta < -0.15:
            return "energy_outperform"
        return "neutral"

    async def _get_prev_score(self, etf: str) -> float:
        if not self._redis:
            return 0.0
        try:
            raw = await self._redis.get(f"{_PREV_SCORE_KEY}{etf}")
            return float(raw) if raw else 0.0
        except Exception:
            return 0.0

    async def _store_prev_score(self, etf: str, score: float) -> None:
        if not self._redis:
            return
        try:
            await self._redis.setex(f"{_PREV_SCORE_KEY}{etf}", _VELOCITY_TTL, str(score))
        except Exception:
            pass

    async def _fetch_etf_scores(self) -> dict[str, float]:
        etf_list = list(ETF_SECTOR_MAP.keys())
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ticker, composite_score
                FROM market_trends
                WHERE ticker = ANY($1::text[])
                """,
                etf_list,
            )
        return {r["ticker"]: float(r["composite_score"]) for r in rows}

    async def _persist_rotation(self, rot: ETFRotation) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO etf_rotations (
                    sector, etf_ticker, composite_score, rotation_velocity,
                    rotation_direction, rank_position, is_leading, is_lagging,
                    computed_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                """,
                rot.sector, rot.etf_ticker, rot.composite_score, rot.rotation_velocity,
                rot.rotation_direction, rot.rank_position,
                rot.is_leading, rot.is_lagging, rot.computed_at,
            )

    async def get_latest_rotations(self) -> list[dict]:
        """Returns the most recent rotation snapshot per sector."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (sector)
                    sector, etf_ticker, composite_score, rotation_velocity,
                    rotation_direction, rank_position, is_leading, is_lagging, computed_at
                FROM etf_rotations
                WHERE computed_at > now() - interval '4 hours'
                ORDER BY sector, computed_at DESC
                """
            )
            return sorted([dict(r) for r in rows], key=lambda r: r["rank_position"] or 99)

    async def get_cross_asset_summary(self) -> dict:
        """Summary of latest cross-asset rotation signals for Telegram/API."""
        rotations = await self.get_latest_rotations()
        leading   = [r for r in rotations if r.get("is_leading")]
        lagging   = [r for r in rotations if r.get("is_lagging")]
        return {
            "leading_sectors":  [r["sector"] for r in leading],
            "lagging_sectors":  [r["sector"] for r in lagging],
            "fastest_rotation": max(rotations, key=lambda r: abs(r.get("rotation_velocity", 0)), default={}).get("sector"),
        }
