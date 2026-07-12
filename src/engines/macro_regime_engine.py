"""
Macro Regime Engine — Level 3.

Detects the dominant macro environment from three independent signal sources:

  1. Narrative signals   — strength of macro-linked NarrativeEngine clusters
  2. Proxy market signals — composite scores of VIX, GLD, TLT, SPY, DXY
  3. News keyword signals — frequency of regime-specific terms in recent news

Detects 5 regimes:
  risk_on             — growth, equity bullishness, risk appetite
  risk_off            — flight to safety, VIX spike, bond rally
  tightening          — rate hike cycle, hawkish central banks, inflation
  liquidity_expansion — rate cuts, QE, dovish pivot, easing
  volatility_regime   — cross-asset spike, panic, extreme uncertainty

Does NOT modify any Level 1/2 engine or table.
Reads from narrative_clusters, market_trends, news (existing).
Writes to macro_regimes (new).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import asyncpg

from src.core.logging import get_logger
from src.core.signal_tracer import SignalTracer
from src.models.market import MacroRegime

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

# ── Redis keys ─────────────────────────────────────────────────────────────────
_PREV_REGIME_KEY  = "regime:prev"
_REGIME_SCORE_KEY = "regime:scores"
_REGIME_TTL       = 3600 * 4   # 4h

# ── Regime → Narrative mapping ─────────────────────────────────────────────────
# Which Level-2 narrative ID contributes to which regime, and in which direction
_NARRATIVE_REGIME_MAP: dict[str, dict[str, float]] = {
    "macro_liquidity": {
        "tightening":          0.5,   # strong macro_liquidity can mean tightening OR expansion
        "liquidity_expansion": 0.5,   # resolved by momentum/direction
    },
    "risk_on_off": {
        "risk_on":  0.5,
        "risk_off": 0.5,
    },
    "ai_semiconductors": {
        "risk_on": 0.8,               # AI boom = risk appetite
    },
    "energy_transition": {
        "risk_on": 0.4,
        "tightening": 0.3,
    },
    "crypto_speculation": {
        "risk_on": 0.7,
        "volatility_regime": 0.3,
    },
    "bist_turkish_macro": {
        "tightening": 0.4,
        "volatility_regime": 0.3,
    },
}

# ── Proxy ticker → regime signals ─────────────────────────────────────────────
# A positive composite_score for each ticker implies the listed regime
_PROXY_SIGNALS: dict[str, dict[str, float]] = {
    "VIX":  {"risk_off": 0.9, "volatility_regime": 0.7},
    "GLD":  {"risk_off": 0.7, "tightening": 0.3},
    "TLT":  {"risk_off": 0.8, "liquidity_expansion": 0.4},
    "SPY":  {"risk_on": 0.9},
    "QQQ":  {"risk_on": 0.8},
    "DXY":  {"risk_off": 0.6, "tightening": 0.4},
}

# ── News keyword → regime scores ───────────────────────────────────────────────
_KEYWORD_REGIME: dict[str, list[str]] = {
    "risk_on": [
        "risk appetite", "bullish", "rally", "growth", "optimism",
        "record high", "equity rally", "risk assets", "positive sentiment",
    ],
    "risk_off": [
        "risk aversion", "safe haven", "flight to quality", "sell-off",
        "fear", "correction", "bear market", "panic", "recession fears",
    ],
    "tightening": [
        "rate hike", "tightening", "hawkish", "inflation", "CPI", "PCE",
        "FOMC", "quantitative tightening", "QT", "Jerome Powell", "rate increase",
    ],
    "liquidity_expansion": [
        "rate cut", "easing", "stimulus", "QE", "quantitative easing",
        "dovish", "liquidity injection", "pivot", "rate reduction",
    ],
    "volatility_regime": [
        "VIX spike", "volatility", "uncertainty", "crash", "circuit breaker",
        "extreme fear", "cross-asset sell-off", "systemic risk", "tail risk",
    ],
}

ALL_REGIMES = list(_KEYWORD_REGIME.keys())


@dataclass
class MacroRegimeCycleResult:
    regime: str = "risk_on"
    confidence: float = 0.0
    regime_shift: bool = False
    previous_regime: str | None = None
    regime_scores: dict[str, float] = field(default_factory=dict)
    dominant_narrative: str | None = None
    error: str | None = None


class MacroRegimeEngine:
    """
    Scores each macro regime against three independent signal sources
    and determines the dominant environment for the current cycle.
    """

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
    ) -> None:
        self._pool = db_pool
        self._redis = redis

    async def run_cycle(self, cycle_id: str | None = None) -> MacroRegimeCycleResult:
        import time as _time
        _t0 = _time.monotonic()
        result = MacroRegimeCycleResult()
        try:
            # Signal source 1: narrative strengths
            narrative_scores, dominant_narrative = await self._score_from_narratives()

            # Signal source 2: proxy market signals
            proxy_scores = await self._score_from_proxies()

            # Signal source 3: news keyword frequency
            keyword_scores = await self._score_from_news_keywords()

            # Signal source 4: crypto Fear & Greed Index (free, no key)
            fg_scores = await self._score_from_fear_greed()

            # Weighted combination (30% / 30% / 25% / 15%)
            final_scores: dict[str, float] = {}
            for regime in ALL_REGIMES:
                final_scores[regime] = round(
                    0.30 * narrative_scores.get(regime, 0.0)
                    + 0.30 * proxy_scores.get(regime, 0.0)
                    + 0.25 * keyword_scores.get(regime, 0.0)
                    + 0.15 * fg_scores.get(regime, 0.0),
                    4,
                )

            dominant = max(final_scores, key=final_scores.get)
            confidence = round(final_scores[dominant], 4)

            # Shift detection
            prev_regime = await self._get_prev_regime()
            regime_shift = dominant != prev_regime and prev_regime is not None

            # Persist regime
            regime_obj = MacroRegime(
                regime=dominant,
                confidence=confidence,
                previous_regime=prev_regime,
                regime_shift=regime_shift,
                dominant_narrative=dominant_narrative,
                narrative_score=round(narrative_scores.get(dominant, 0.0), 4),
                proxy_score=round(proxy_scores.get(dominant, 0.0), 4),
                keyword_score=round(keyword_scores.get(dominant, 0.0), 4),
                regime_scores=final_scores,
                computed_at=datetime.now(timezone.utc),
            )
            await self._persist_regime(regime_obj)
            await self._store_prev_regime(dominant)

            result.regime = dominant
            result.confidence = confidence
            result.regime_shift = regime_shift
            result.previous_regime = prev_regime
            result.regime_scores = final_scores
            result.dominant_narrative = dominant_narrative

            logger.info(
                "macro_regime_detected",
                regime=dominant,
                confidence=round(confidence, 3),
                shift=regime_shift,
                previous=prev_regime,
                narrative=dominant_narrative,
            )

            if cycle_id:
                await SignalTracer(self._pool).record(
                    cycle_id=cycle_id,
                    engine_name="macro_regime",
                    signal_type="macro",
                    signal_strength=round(confidence * 100, 2),
                    signal_direction=dominant,
                    output_data={
                        "regime": dominant,
                        "confidence": confidence,
                        "regime_shift": regime_shift,
                        "dominant_narrative": dominant_narrative,
                        "regime_scores": final_scores,
                    },
                    latency_ms=round((_time.monotonic() - _t0) * 1000, 1),
                )

        except Exception as exc:
            logger.error("macro_regime_error", error=str(exc))
            result.error = str(exc)

        return result

    async def _score_from_narratives(self) -> tuple[dict[str, float], str | None]:
        """
        Read narrative_clusters and map each narrative strength to regime scores.
        Returns (regime_scores, dominant_narrative_id).
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT name, strength_score, momentum
                FROM narrative_clusters
                ORDER BY strength_score DESC
                """
            )

        scores: dict[str, float] = {r: 0.0 for r in ALL_REGIMES}
        dominant_narrative: str | None = None
        top_strength = 0.0

        for row in rows:
            name     = row["name"]
            strength = float(row["strength_score"])
            momentum = row.get("momentum", "neutral")

            if name not in _NARRATIVE_REGIME_MAP:
                continue
            if strength > top_strength:
                top_strength = strength
                dominant_narrative = name

            regime_map = _NARRATIVE_REGIME_MAP[name]

            # For ambiguous narratives (e.g. macro_liquidity), resolve by momentum
            if name == "macro_liquidity":
                if momentum == "accelerating":
                    scores["tightening"]          += strength * 0.6
                    scores["liquidity_expansion"]  += strength * 0.2
                else:
                    scores["liquidity_expansion"]  += strength * 0.5
                    scores["tightening"]           += strength * 0.2
            elif name == "risk_on_off":
                # narrative momentum resolves direction
                if momentum == "accelerating":
                    scores["risk_on"]  += strength * 0.6
                    scores["risk_off"] += strength * 0.2
                elif momentum == "decelerating":
                    scores["risk_off"] += strength * 0.6
                    scores["risk_on"]  += strength * 0.2
                else:
                    scores["risk_on"]  += strength * 0.4
                    scores["risk_off"] += strength * 0.4
            else:
                for regime, weight in regime_map.items():
                    scores[regime] += strength * weight

        # Normalize to 0–1
        max_score = max(scores.values()) or 1.0
        normalized = {k: round(v / max_score, 4) for k, v in scores.items()}
        return normalized, dominant_narrative

    async def _score_from_proxies(self) -> dict[str, float]:
        """
        Read composite scores for proxy tickers (VIX, GLD, TLT, SPY, DXY).
        Positive composite = signal is active.
        """
        proxy_tickers = list(_PROXY_SIGNALS.keys())
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT ticker, composite_score FROM market_trends WHERE ticker = ANY($1::text[])",
                proxy_tickers,
            )

        trend_scores = {r["ticker"]: float(r["composite_score"]) for r in rows}
        regime_scores: dict[str, float] = {r: 0.0 for r in ALL_REGIMES}

        for ticker, regime_map in _PROXY_SIGNALS.items():
            score = trend_scores.get(ticker, 0.0)
            # Only positive score contributes (negative = proxy NOT signaling this regime)
            if score > 0:
                for regime, weight in regime_map.items():
                    regime_scores[regime] += score * weight

        # Normalize
        max_score = max(regime_scores.values()) or 1.0
        return {k: round(v / max_score, 4) for k, v in regime_scores.items()}

    async def _score_from_fear_greed(self) -> dict[str, float]:
        """Crypto Fear & Greed Index (alternative.me free API).

        Score 0–100:  extreme fear (<25) → risk_off/volatility,
                      extreme greed (>75) → risk_on (with contrarian flip near 90)
        Cached 4h in Redis to avoid hammering the free API.
        """
        import asyncio
        cache_key = "macro:fear_greed"
        fg_value: float | None = None

        if self._redis:
            try:
                cached = await self._redis.get(cache_key)
                if cached:
                    fg_value = float(cached)
            except Exception:
                pass

        if fg_value is None:
            try:
                fg_value = await asyncio.get_event_loop().run_in_executor(
                    None, _fetch_fear_greed_sync
                )
                if self._redis and fg_value is not None:
                    await self._redis.setex(cache_key, 14400, str(fg_value))
            except Exception:
                pass

        scores: dict[str, float] = {r: 0.0 for r in ALL_REGIMES}
        if fg_value is None:
            return scores

        logger.debug("fear_greed_loaded", value=fg_value)

        if fg_value <= 25:   # extreme fear
            scores["risk_off"]          = min(1.0, (25 - fg_value) / 25)
            scores["volatility_regime"] = min(0.5, (25 - fg_value) / 50)
        elif fg_value >= 75: # extreme greed → risk_on, but near 90+ flip to volatility
            scores["risk_on"] = min(1.0, (fg_value - 75) / 25)
            if fg_value >= 90:
                scores["volatility_regime"] = (fg_value - 90) / 10 * 0.3
        else:
            # neutral zone 25–75: mild lean
            mid = (fg_value - 50) / 50   # -0.5 → +0.5
            if mid > 0:
                scores["risk_on"]  = mid * 0.4
            else:
                scores["risk_off"] = abs(mid) * 0.4

        return scores

    async def _score_from_news_keywords(self) -> dict[str, float]:
        """
        Count regime-specific keyword occurrences in recent news titles + summaries.
        Returns normalized scores per regime.
        """
        since = datetime.now(timezone.utc) - timedelta(hours=24)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT title, ai_summary
                FROM news
                WHERE published_at > $1
                ORDER BY published_at DESC
                LIMIT 200
                """,
                since,
            )

        combined_text = " ".join(
            f"{r.get('title') or ''} {r.get('ai_summary') or ''}"
            for r in rows
        ).lower()

        regime_scores: dict[str, float] = {}
        for regime, keywords in _KEYWORD_REGIME.items():
            hit_count = sum(combined_text.count(kw.lower()) for kw in keywords)
            regime_scores[regime] = float(hit_count)

        max_score = max(regime_scores.values()) or 1.0
        return {k: round(v / max_score, 4) for k, v in regime_scores.items()}

    async def _get_prev_regime(self) -> str | None:
        if not self._redis:
            return None
        try:
            raw = await self._redis.get(_PREV_REGIME_KEY)
            return raw.decode() if raw else None
        except Exception:
            return None

    async def _store_prev_regime(self, regime: str) -> None:
        if not self._redis:
            return
        try:
            await self._redis.setex(_PREV_REGIME_KEY, _REGIME_TTL, regime)
        except Exception:
            pass

    async def _persist_regime(self, regime: MacroRegime) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO macro_regimes (
                    regime, confidence, previous_regime, regime_shift,
                    dominant_narrative, narrative_score, proxy_score, keyword_score,
                    regime_scores, computed_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)
                """,
                regime.regime, regime.confidence, regime.previous_regime,
                regime.regime_shift, regime.dominant_narrative,
                regime.narrative_score, regime.proxy_score, regime.keyword_score,
                json.dumps(regime.regime_scores), regime.computed_at,
            )

    async def get_current_regime(self) -> dict | None:
        """Returns the most recently detected macro regime."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT regime, confidence, previous_regime, regime_shift,
                       dominant_narrative, regime_scores, computed_at
                FROM macro_regimes
                ORDER BY computed_at DESC LIMIT 1
                """
            )
            if not row:
                return None
            r = dict(row)
            r["regime_scores"] = json.loads(r["regime_scores"]) if r.get("regime_scores") else {}
            return r


def _fetch_fear_greed_sync() -> float | None:
    """Fetch the latest Crypto Fear & Greed Index value (0–100).  Free, no key."""
    try:
        import urllib.request
        import json as _json
        url = "https://api.alternative.me/fng/?limit=1&format=json"
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = _json.loads(resp.read())
            val = int(data["data"][0]["value"])
            return float(val)
    except Exception:
        return None
