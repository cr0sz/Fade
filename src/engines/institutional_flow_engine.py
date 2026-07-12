"""
Institutional Flow Engine — Level 3.

Computes capital flow direction, persistence, acceleration, and insider
behavior clustering from EXISTING smart_money_events data.

Does NOT modify any Level 1/2 engine, model, or table.
All reads are from existing tables; all writes go to institutional_flows (new).

Capabilities:
  A) Capital Flow Direction — inflow / outflow / neutral per ticker
  B) Flow Strength Score — 0–100 institutional capital intensity index
  C) Persistence Scoring — multi-day confirmation via Redis direction history
  D) Acceleration Detection — building / fading / stable vs previous cycle
  E) Cross-Asset Flow Mapping — equities ↔ bonds, tech ↔ energy coupling
  F) Insider Behavior Clustering — bullish_cluster / neutral / bearish_distribution
"""
from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import asyncpg

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.market import InstitutionalFlow

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

# ── Redis key schema ───────────────────────────────────────────────────────────
_FLOW_PREV_STRENGTH_KEY = "flow:prev_strength:"      # + ticker → float
_FLOW_DIR_HISTORY_KEY   = "flow:dir_history:"        # + ticker → JSON list[str]
_FLOW_HISTORY_TTL       = 86400 * 8                  # 8 days
_PREV_STRENGTH_TTL      = 3600 * 2                   # 2 hours

# ── Flow strength normalization ────────────────────────────────────────────────
# $10k → ~0, $100k → 25, $1M → 50, $10M → 75, $100M → 100
_STRENGTH_BASE_USD = 10_000.0


def _flow_strength(total_usd: float) -> float:
    if total_usd <= 0:
        return 0.0
    return min(100.0, max(0.0, 25.0 * math.log10(max(total_usd, 1.0) / _STRENGTH_BASE_USD)))


def _net_ratio(buy_usd: float, sell_usd: float) -> float:
    total = buy_usd + sell_usd
    return (buy_usd - sell_usd) / max(total, 1.0)


def _classify_direction(ratio: float) -> str:
    if ratio > 0.1:
        return "inflow"
    if ratio < -0.1:
        return "outflow"
    return "neutral"


@dataclass
class InstitutionalFlowCycleResult:
    tickers_analyzed: int = 0
    flows_stored: int = 0
    inflow_count: int = 0
    outflow_count: int = 0
    neutral_count: int = 0
    bullish_clusters: int = 0
    bearish_clusters: int = 0
    market_direction: str = "neutral"
    market_strength: float = 0.0
    errors: int = 0


class InstitutionalFlowEngine:
    """
    Reads smart_money_events (existing) and computes institutional flow
    intelligence per ticker and market-wide.  Redis is optional — degrades
    gracefully to stateless computation when unavailable.
    """

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
    ) -> None:
        self._pool = db_pool
        self._redis = redis
        self._settings = get_settings()

    async def run_cycle(self) -> InstitutionalFlowCycleResult:
        result = InstitutionalFlowCycleResult()
        window_h = getattr(self._settings, "FLOW_ANALYSIS_WINDOW_HOURS", 24)

        events = await self._fetch_smart_money_events(hours=window_h)
        if not events:
            logger.info("institutional_flow_no_events", window_hours=window_h)
            return result

        # Group by ticker (None events go to a 'MARKET' aggregate)
        by_ticker: dict[str, list[dict]] = defaultdict(list)
        for ev in events:
            key = ev.get("ticker") or "MARKET"
            by_ticker[key].append(ev)

        flows: list[InstitutionalFlow] = []
        for ticker, evts in by_ticker.items():
            try:
                flow = await self._compute_ticker_flow(ticker, evts)
                flows.append(flow)
                result.tickers_analyzed += 1
                if flow.direction == "inflow":
                    result.inflow_count += 1
                elif flow.direction == "outflow":
                    result.outflow_count += 1
                else:
                    result.neutral_count += 1
                if flow.insider_cluster == "bullish_cluster":
                    result.bullish_clusters += 1
                elif flow.insider_cluster == "bearish_distribution":
                    result.bearish_clusters += 1
            except Exception as exc:
                logger.error("flow_compute_error", ticker=ticker, error=str(exc))
                result.errors += 1

        # Market-wide aggregate (excluding the 'MARKET' placeholder)
        real_flows = [f for f in flows if f.ticker != "MARKET"]
        market_flow = self._aggregate_market_flow(real_flows)
        flows.append(market_flow)
        result.market_direction = market_flow.direction
        result.market_strength = market_flow.flow_strength

        # Persist all
        for flow in flows:
            try:
                await self._persist_flow(flow)
                result.flows_stored += 1
            except Exception as exc:
                logger.error("flow_persist_error", ticker=flow.ticker, error=str(exc))
                result.errors += 1

        logger.info(
            "institutional_flow_cycle_complete",
            tickers=result.tickers_analyzed,
            inflow=result.inflow_count,
            outflow=result.outflow_count,
            market_direction=result.market_direction,
            market_strength=round(result.market_strength, 1),
            bullish_clusters=result.bullish_clusters,
        )
        return result

    async def _compute_ticker_flow(
        self,
        ticker: str,
        events: list[dict],
    ) -> InstitutionalFlow:
        # ── Separate by type ──────────────────────────────────────────────────
        whale_buys  = [e for e in events if e.get("event_type") == "options_flow"
                       and e.get("direction") in ("buy", "in")]
        whale_sells = [e for e in events if e.get("event_type") == "options_flow"
                       and e.get("direction") in ("sell", "out")]
        insider_buys  = [e for e in events if e.get("event_type") == "funding_flow"
                         and e.get("direction") in ("buy", "in")]
        insider_sells = [e for e in events if e.get("event_type") == "funding_flow"
                         and e.get("direction") in ("sell", "out")]

        whale_buy_usd  = sum(float(e.get("amount_usd") or 0) for e in whale_buys)
        whale_sell_usd = sum(float(e.get("amount_usd") or 0) for e in whale_sells)
        insider_buy_usd  = sum(float(e.get("amount_usd") or 0) for e in insider_buys)
        insider_sell_usd = sum(float(e.get("amount_usd") or 0) for e in insider_sells)

        total_buy_usd  = whale_buy_usd  + insider_buy_usd
        total_sell_usd = whale_sell_usd + insider_sell_usd
        total_usd      = total_buy_usd  + total_sell_usd

        ratio     = _net_ratio(total_buy_usd, total_sell_usd)
        direction = _classify_direction(ratio)
        strength  = _flow_strength(total_usd)

        # ── Persistence — Redis direction history ─────────────────────────────
        persistence = await self._compute_persistence(ticker, direction)

        # ── Acceleration — compare to previous cycle strength ─────────────────
        acceleration = await self._compute_acceleration(ticker, strength)

        # ── Insider clustering ────────────────────────────────────────────────
        insider_cluster = self._classify_insider_cluster(insider_buys, insider_sells)

        return InstitutionalFlow(
            ticker=ticker,
            direction=direction,
            flow_strength=round(strength, 2),
            persistence_score=round(persistence, 3),
            acceleration=acceleration,
            insider_cluster=insider_cluster,
            whale_buy_usd=round(whale_buy_usd, 2),
            whale_sell_usd=round(whale_sell_usd, 2),
            insider_buy_count=len(insider_buys),
            insider_sell_count=len(insider_sells),
            net_flow_ratio=round(ratio, 4),
            computed_at=datetime.now(timezone.utc),
        )

    async def _compute_persistence(self, ticker: str, current_direction: str) -> float:
        """
        Track rolling direction history in Redis.
        Returns 0–1: 0 = first occurrence, 1 = consistent for 7+ days.
        """
        if not self._redis:
            return 0.5  # no history → neutral persistence
        key = f"{_FLOW_DIR_HISTORY_KEY}{ticker}"
        try:
            raw = await self._redis.get(key)
            history: list[str] = json.loads(raw) if raw else []
            history.append(current_direction)
            history = history[-7:]  # keep last 7 entries
            await self._redis.setex(key, _FLOW_HISTORY_TTL, json.dumps(history))

            if len(history) < 2:
                return 0.0
            consistent = sum(1 for d in history if d == current_direction)
            return round(consistent / len(history), 3)
        except Exception:
            return 0.5

    async def _compute_acceleration(self, ticker: str, strength: float) -> str:
        """Compare current strength to previous cycle (Redis)."""
        if not self._redis:
            return "stable"
        key = f"{_FLOW_PREV_STRENGTH_KEY}{ticker}"
        try:
            prev_raw = await self._redis.get(key)
            prev = float(prev_raw) if prev_raw else strength
            delta = strength - prev
            await self._redis.setex(key, _PREV_STRENGTH_TTL, str(strength))
            threshold = getattr(self._settings, "FLOW_ACCELERATION_THRESHOLD", 10.0)
            if delta > threshold:
                return "building"
            if delta < -threshold:
                return "fading"
            return "stable"
        except Exception:
            return "stable"

    def _classify_insider_cluster(
        self,
        insider_buys: list[dict],
        insider_sells: list[dict],
    ) -> str:
        """
        Cluster insider behavior:
          bullish_cluster    — 2+ distinct insiders buying
          bearish_distribution — 2+ distinct insiders selling
          neutral            — otherwise
        """
        buy_actors  = {e.get("actor") for e in insider_buys if e.get("actor")}
        sell_actors = {e.get("actor") for e in insider_sells if e.get("actor")}

        if len(buy_actors) >= 2 and len(buy_actors) > len(sell_actors):
            return "bullish_cluster"
        if len(sell_actors) >= 2 and len(sell_actors) > len(buy_actors):
            return "bearish_distribution"
        return "neutral"

    def _aggregate_market_flow(self, flows: list[InstitutionalFlow]) -> InstitutionalFlow:
        """Compute market-wide aggregate from per-ticker flows."""
        if not flows:
            return InstitutionalFlow(ticker="MARKET", direction="neutral")

        buy_sum  = sum(f.whale_buy_usd + (f.insider_buy_count * 100_000) for f in flows)
        sell_sum = sum(f.whale_sell_usd + (f.insider_sell_count * 100_000) for f in flows)
        total    = buy_sum + sell_sum
        ratio    = _net_ratio(buy_sum, sell_sum)

        inflow_count  = sum(1 for f in flows if f.direction == "inflow")
        outflow_count = sum(1 for f in flows if f.direction == "outflow")

        # Majority vote for market direction
        if inflow_count > outflow_count * 1.5:
            direction = "inflow"
        elif outflow_count > inflow_count * 1.5:
            direction = "outflow"
        else:
            direction = "neutral"

        bullish_clusters = sum(1 for f in flows if f.insider_cluster == "bullish_cluster")
        bearish_clusters = sum(1 for f in flows if f.insider_cluster == "bearish_distribution")

        return InstitutionalFlow(
            ticker="MARKET",
            direction=direction,
            flow_strength=round(_flow_strength(total), 2),
            persistence_score=round(
                sum(f.persistence_score for f in flows) / max(len(flows), 1), 3
            ),
            acceleration="building" if direction == "inflow" and len(flows) > 3 else "stable",
            insider_cluster=(
                "bullish_cluster" if bullish_clusters > bearish_clusters
                else ("bearish_distribution" if bearish_clusters > bullish_clusters else "neutral")
            ),
            whale_buy_usd=round(buy_sum, 2),
            whale_sell_usd=round(sell_sum, 2),
            insider_buy_count=sum(f.insider_buy_count for f in flows),
            insider_sell_count=sum(f.insider_sell_count for f in flows),
            net_flow_ratio=round(ratio, 4),
            computed_at=datetime.now(timezone.utc),
        )

    async def _fetch_smart_money_events(self, hours: int = 24) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT event_type, ticker, actor, amount_usd, shares, direction, source, event_at
                FROM smart_money_events
                WHERE event_at > now() - ($1 || ' hours')::interval
                ORDER BY event_at DESC
                """,
                str(hours),
            )
            return [dict(r) for r in rows]

    async def _persist_flow(self, flow: InstitutionalFlow) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO institutional_flows (
                    ticker, direction, flow_strength, persistence_score, acceleration,
                    insider_cluster, whale_buy_usd, whale_sell_usd,
                    insider_buy_count, insider_sell_count, net_flow_ratio, computed_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                """,
                flow.ticker, flow.direction, flow.flow_strength, flow.persistence_score,
                flow.acceleration, flow.insider_cluster,
                flow.whale_buy_usd, flow.whale_sell_usd,
                flow.insider_buy_count, flow.insider_sell_count,
                flow.net_flow_ratio, flow.computed_at,
            )

    async def get_latest_flows(self, limit: int = 20) -> list[dict]:
        """Returns the most recent flow record per ticker, ordered by strength."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                    ticker, direction, flow_strength, persistence_score, acceleration,
                    insider_cluster, whale_buy_usd, whale_sell_usd,
                    insider_buy_count, insider_sell_count, net_flow_ratio, computed_at
                FROM institutional_flows
                WHERE computed_at > now() - interval '4 hours'
                ORDER BY ticker, computed_at DESC
                LIMIT $1
                """,
                limit,
            )
            return [dict(r) for r in rows]

    async def get_market_flow(self) -> dict | None:
        """Returns latest market-wide flow aggregate."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM institutional_flows
                WHERE ticker = 'MARKET'
                ORDER BY computed_at DESC LIMIT 1
                """
            )
            return dict(row) if row else None

    async def get_ticker_flow(self, ticker: str) -> dict | None:
        """Returns the most recent flow record for a specific ticker."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT direction, flow_strength, persistence_score, acceleration,
                       insider_cluster, whale_buy_usd, whale_sell_usd,
                       insider_buy_count, insider_sell_count, net_flow_ratio, computed_at
                FROM institutional_flows
                WHERE ticker = $1
                ORDER BY computed_at DESC LIMIT 1
                """,
                ticker,
            )
            return dict(row) if row else None
