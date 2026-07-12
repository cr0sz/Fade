"""
Institutional Confluence Engine — Level 3.

Synthesizes Level-2 and Level-3 intelligence into a single
institutional_confluence_signal per ticker and market-wide.

Inputs (read-only from existing and new tables):
  - narrative_clusters     (Level 2 NarrativeEngine output)
  - market_trends          (Level 2 ConfidenceEngine overlay fields)
  - institutional_flows    (Level 3 InstitutionalFlowEngine output)
  - macro_regimes          (Level 3 MacroRegimeEngine output)

Output:
  - confluence_signals table (Level 3, append-only)

Confluence strength formula (0–100):
  strength = 100 × (
      0.30 × narrative_alignment  +   # top narrative in same direction
      0.25 × confidence_weighted  +   # average confidence_score across tickers
      0.25 × flow_alignment       +   # institutional flow supports narrative
      0.20 × regime_coherence         # macro regime matches direction
  )

Direction:
  risk_on   — majority of signals point to risk appetite / growth
  risk_off  — majority point to safety / defensive
  mixed     — signals disagree or insufficient data
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import asyncpg

from src.core.logging import get_logger
from src.core.signal_tracer import SignalTracer
from src.models.market import ConfluenceSignal

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

# ── Regime → direction mapping ─────────────────────────────────────────────────
_REGIME_DIRECTION: dict[str, str] = {
    "risk_on":            "risk_on",
    "liquidity_expansion": "risk_on",
    "risk_off":           "risk_off",
    "tightening":         "risk_off",
    "volatility_regime":  "risk_off",
}

# ── Narrative → direction mapping ─────────────────────────────────────────────
_NARRATIVE_DIRECTION: dict[str, str] = {
    "ai_semiconductors":  "risk_on",
    "energy_transition":  "risk_on",
    "crypto_speculation": "risk_on",
    "macro_liquidity":    "mixed",    # resolved by momentum
    "risk_on_off":        "mixed",    # resolved by momentum
    "bist_turkish_macro": "mixed",
}

# ── Confluence component weights ───────────────────────────────────────────────
_WEIGHTS = {
    "narrative_alignment": 0.30,
    "confidence_weighted": 0.25,
    "flow_alignment":      0.25,
    "regime_coherence":    0.20,
}


@dataclass
class ConfluenceCycleResult:
    market_signal: ConfluenceSignal | None = None
    ticker_signals: list[ConfluenceSignal] = field(default_factory=list)
    signals_stored: int = 0
    errors: int = 0


class InstitutionalConfluenceEngine:
    """
    Reads from all Level-2 and Level-3 engine outputs and synthesizes a
    unified confluence signal.  All operations are additive — no existing
    table or engine output is modified.
    """

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
    ) -> None:
        self._pool = db_pool
        self._redis = redis

    async def run_cycle(self, cycle_id: str | None = None) -> ConfluenceCycleResult:
        import time as _time
        _t0 = _time.monotonic()
        result = ConfluenceCycleResult()

        # ── Load all inputs ────────────────────────────────────────────────────
        narratives    = await self._fetch_top_narratives()
        trend_data    = await self._fetch_trend_confidence()
        latest_flows  = await self._fetch_latest_flows()
        current_regime = await self._fetch_current_regime()

        signals: list[ConfluenceSignal] = []

        # ── Market-wide signal ─────────────────────────────────────────────────
        market_signal = self._compute_market_confluence(
            narratives, trend_data, latest_flows, current_regime
        )
        signals.append(market_signal)
        result.market_signal = market_signal

        # ── Per-ticker signals (tickers with flow data) ────────────────────────
        flow_tickers = {f["ticker"] for f in latest_flows if f["ticker"] != "MARKET"}
        for ticker in list(flow_tickers)[:50]:   # cap to avoid runaway
            try:
                ticker_trend  = trend_data.get(ticker)
                ticker_flow   = next((f for f in latest_flows if f["ticker"] == ticker), None)
                sig = self._compute_ticker_confluence(
                    ticker, narratives, ticker_trend, ticker_flow, current_regime
                )
                signals.append(sig)
                result.ticker_signals.append(sig)
            except Exception as exc:
                logger.error("confluence_ticker_error", ticker=ticker, error=str(exc))
                result.errors += 1

        # ── Persist ───────────────────────────────────────────────────────────
        for sig in signals:
            try:
                await self._persist_signal(sig)
                result.signals_stored += 1
            except Exception as exc:
                logger.error("confluence_persist_error", error=str(exc))
                result.errors += 1

        logger.info(
            "confluence_cycle_complete",
            market_strength=round(market_signal.strength, 1),
            market_direction=market_signal.direction,
            tickers=len(result.ticker_signals),
            stored=result.signals_stored,
        )

        if cycle_id:
            await SignalTracer(self._pool).record(
                cycle_id=cycle_id,
                engine_name="institutional_confluence",
                signal_type="confluence",
                signal_strength=round(market_signal.strength, 2),
                signal_direction=market_signal.direction,
                output_data={
                    "market_direction": market_signal.direction,
                    "market_strength": market_signal.strength,
                    "ticker_signals": len(result.ticker_signals),
                    "signals_stored": result.signals_stored,
                },
                latency_ms=round((_time.monotonic() - _t0) * 1000, 1),
            )

        return result

    def _compute_market_confluence(
        self,
        narratives: list[dict],
        trend_data: dict[str, dict],
        flows: list[dict],
        regime: dict | None,
    ) -> ConfluenceSignal:
        # ── Component 1: Narrative alignment ──────────────────────────────────
        top_narrative    = narratives[0] if narratives else {}
        narrative_name   = top_narrative.get("name", "")
        narrative_strength = float(top_narrative.get("strength_score") or 0)
        narrative_momentum = top_narrative.get("momentum", "neutral")

        narrative_alignment = narrative_strength
        narrative_dir = _resolve_narrative_direction(narrative_name, narrative_momentum)

        # ── Component 2: Average confidence from trend data ────────────────────
        conf_scores = [
            float(t.get("confidence_score") or 0)
            for t in trend_data.values()
            if t.get("confidence_score") is not None
        ]
        confidence_weighted = (sum(conf_scores) / len(conf_scores)) if conf_scores else 0.5

        # ── Component 3: Flow alignment ────────────────────────────────────────
        market_flow = next((f for f in flows if f.get("ticker") == "MARKET"), None)
        if market_flow:
            raw_strength = float(market_flow.get("flow_strength") or 0)
            flow_alignment = raw_strength / 100.0
            flow_dir = market_flow.get("direction", "neutral")
        else:
            flow_alignment = 0.5
            flow_dir = "neutral"

        # ── Component 4: Regime coherence ──────────────────────────────────────
        regime_dir = "mixed"
        regime_coherence = 0.5
        if regime:
            regime_name = regime.get("regime", "risk_on")
            regime_conf = float(regime.get("confidence") or 0)
            regime_dir  = _REGIME_DIRECTION.get(regime_name, "mixed")
            regime_coherence = regime_conf

        # ── Confluence strength ────────────────────────────────────────────────
        raw = (
            _WEIGHTS["narrative_alignment"] * narrative_alignment
            + _WEIGHTS["confidence_weighted"] * confidence_weighted
            + _WEIGHTS["flow_alignment"] * flow_alignment
            + _WEIGHTS["regime_coherence"] * regime_coherence
        )
        strength = round(raw * 100, 2)

        # ── Direction vote ─────────────────────────────────────────────────────
        directions = [narrative_dir, regime_dir]
        if flow_dir == "inflow":
            directions.append("risk_on")
        elif flow_dir == "outflow":
            directions.append("risk_off")

        risk_on_votes  = directions.count("risk_on")
        risk_off_votes = directions.count("risk_off")

        if risk_on_votes > risk_off_votes:
            direction = "risk_on"
        elif risk_off_votes > risk_on_votes:
            direction = "risk_off"
        else:
            direction = "mixed"

        return ConfluenceSignal(
            ticker=None,
            strength=strength,
            direction=direction,
            narrative_alignment=round(narrative_alignment, 4),
            confidence_weighted=round(confidence_weighted, 4),
            flow_alignment=round(flow_alignment, 4),
            regime_coherence=round(regime_coherence, 4),
            contributing_sources={
                "top_narrative": narrative_name,
                "narrative_momentum": narrative_momentum,
                "market_flow": flow_dir,
                "macro_regime": regime.get("regime") if regime else None,
            },
            computed_at=datetime.now(timezone.utc),
        )

    def _compute_ticker_confluence(
        self,
        ticker: str,
        narratives: list[dict],
        trend: dict | None,
        flow: dict | None,
        regime: dict | None,
    ) -> ConfluenceSignal:
        # Narrative: does the ticker's cluster match the top narrative?
        ticker_narrative = (trend or {}).get("narrative_cluster", "") if trend else ""
        top_narrative_name = narratives[0].get("name", "") if narratives else ""
        narrative_alignment = float((narratives[0] or {}).get("strength_score") or 0) \
            if ticker_narrative == top_narrative_name else 0.3

        # Confidence from trend
        confidence_weighted = float((trend or {}).get("confidence_score") or 0.5)

        # Flow: ticker-specific
        if flow:
            raw_strength = float(flow.get("flow_strength") or 0)
            flow_alignment = raw_strength / 100.0
            flow_dir = flow.get("direction", "neutral")
        else:
            flow_alignment = 0.3   # no flow data = uncertain
            flow_dir = "neutral"

        # Regime coherence (same as market-wide)
        if regime:
            regime_coherence = float(regime.get("confidence") or 0.5)
            regime_name = regime.get("regime", "risk_on")
            regime_dir  = _REGIME_DIRECTION.get(regime_name, "mixed")
        else:
            regime_coherence = 0.5
            regime_dir = "mixed"

        raw = (
            _WEIGHTS["narrative_alignment"] * narrative_alignment
            + _WEIGHTS["confidence_weighted"] * confidence_weighted
            + _WEIGHTS["flow_alignment"] * flow_alignment
            + _WEIGHTS["regime_coherence"] * regime_coherence
        )
        strength = round(raw * 100, 2)

        direction_votes = [regime_dir]
        if flow_dir == "inflow":
            direction_votes.append("risk_on")
        elif flow_dir == "outflow":
            direction_votes.append("risk_off")

        risk_on  = direction_votes.count("risk_on")
        risk_off = direction_votes.count("risk_off")
        direction = "risk_on" if risk_on > risk_off else ("risk_off" if risk_off > risk_on else "mixed")

        return ConfluenceSignal(
            ticker=ticker,
            strength=strength,
            direction=direction,
            narrative_alignment=round(narrative_alignment, 4),
            confidence_weighted=round(confidence_weighted, 4),
            flow_alignment=round(flow_alignment, 4),
            regime_coherence=round(regime_coherence, 4),
            contributing_sources={
                "narrative_cluster": ticker_narrative,
                "flow_direction": flow_dir,
                "flow_acceleration": (flow or {}).get("acceleration", "stable"),
                "insider_cluster": (flow or {}).get("insider_cluster", "neutral"),
                "macro_regime": regime.get("regime") if regime else None,
            },
            computed_at=datetime.now(timezone.utc),
        )

    # ── Data fetchers ─────────────────────────────────────────────────────────

    async def _fetch_top_narratives(self) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT name, label, strength_score, momentum FROM narrative_clusters ORDER BY strength_score DESC LIMIT 6"
            )
            return [dict(r) for r in rows]

    async def _fetch_trend_confidence(self) -> dict[str, dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ticker, confidence_score, confidence_level,
                       narrative_cluster, composite_score, smoothed_composite
                FROM market_trends
                WHERE computed_at > now() - interval '4 hours'
                """
            )
            return {r["ticker"]: dict(r) for r in rows}

    async def _fetch_latest_flows(self) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                    ticker, direction, flow_strength, acceleration,
                    insider_cluster, net_flow_ratio, computed_at
                FROM institutional_flows
                WHERE computed_at > now() - interval '4 hours'
                ORDER BY ticker, computed_at DESC
                LIMIT 100
                """
            )
            return [dict(r) for r in rows]

    async def _fetch_current_regime(self) -> dict | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT regime, confidence, regime_shift FROM macro_regimes ORDER BY computed_at DESC LIMIT 1"
            )
            return dict(row) if row else None

    async def _persist_signal(self, sig: ConfluenceSignal) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO confluence_signals (
                    ticker, strength, direction,
                    narrative_alignment, confidence_weighted,
                    flow_alignment, regime_coherence,
                    contributing_sources, computed_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
                """,
                sig.ticker, sig.strength, sig.direction,
                sig.narrative_alignment, sig.confidence_weighted,
                sig.flow_alignment, sig.regime_coherence,
                json.dumps(sig.contributing_sources),
                sig.computed_at,
            )

    async def get_market_confluence(self) -> dict | None:
        """Returns the latest market-wide confluence signal."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT strength, direction, narrative_alignment, confidence_weighted,
                       flow_alignment, regime_coherence, contributing_sources, computed_at
                FROM confluence_signals
                WHERE ticker IS NULL
                ORDER BY computed_at DESC LIMIT 1
                """
            )
            if not row:
                return None
            r = dict(row)
            r["contributing_sources"] = (
                json.loads(r["contributing_sources"])
                if r.get("contributing_sources") else {}
            )
            return r

    async def get_ticker_confluence(self, ticker: str) -> dict | None:
        """Returns the latest confluence signal for a specific ticker."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT strength, direction, narrative_alignment, confidence_weighted,
                       flow_alignment, regime_coherence, contributing_sources, computed_at
                FROM confluence_signals
                WHERE ticker = $1
                ORDER BY computed_at DESC LIMIT 1
                """,
                ticker,
            )
            if not row:
                return None
            r = dict(row)
            r["contributing_sources"] = (
                json.loads(r["contributing_sources"])
                if r.get("contributing_sources") else {}
            )
            return r

    async def get_strongest_confluences(self, limit: int = 10) -> list[dict]:
        """Tickers with the highest recent confluence strength."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                    ticker, strength, direction, regime_coherence, flow_alignment,
                    contributing_sources, computed_at
                FROM confluence_signals
                WHERE ticker IS NOT NULL
                  AND computed_at > now() - interval '4 hours'
                ORDER BY ticker, computed_at DESC
                LIMIT $1
                """,
                limit * 3,   # fetch more, then sort by strength
            )
            result = sorted([dict(r) for r in rows], key=lambda r: r["strength"], reverse=True)
            for r in result:
                r["contributing_sources"] = (
                    json.loads(r["contributing_sources"])
                    if r.get("contributing_sources") else {}
                )
            return result[:limit]


def _resolve_narrative_direction(narrative_name: str, momentum: str) -> str:
    base = _NARRATIVE_DIRECTION.get(narrative_name, "mixed")
    if base != "mixed":
        return base
    # Resolve ambiguous narratives by momentum
    if narrative_name in ("macro_liquidity",):
        return "risk_off" if momentum == "decelerating" else "risk_on"
    if narrative_name in ("risk_on_off",):
        return "risk_off" if momentum == "decelerating" else "risk_on"
    return "mixed"
