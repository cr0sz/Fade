"""
Narrative Intelligence Engine — Level 2.

Groups market signals into named thematic narratives.
Narratives explain WHY multiple tickers are moving together,
providing context beyond individual ticker analysis.

Architecture:
  - Fixed narrative definitions (keyword + ticker anchors)
  - Scoring: how strongly do current trends match each narrative?
  - Momentum: accelerating vs decelerating vs neutral
  - Redis-backed: stores previous cycle scores for delta computation
  - DB-backed: upserts to narrative_clusters table for persistence
  - Semantic fallback: OpenAI embedding similarity for tickers not matched
    by keyword/anchor methods (prototype embeddings cached in Redis 1h)

Does NOT modify any existing engine. Called from TrendDetectionEngine.run_cycle()
at the end, using already-computed MarketTrend objects.
"""
from __future__ import annotations

import json
import struct
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import asyncpg

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.market import MarketTrend, NarrativeCluster

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

_PROTO_KEY_PREFIX = "narrative:proto:"
_PROTO_TTL = 3600  # refresh prototype embeddings every hour

# ── Narrative Definitions ──────────────────────────────────────────────────────
# Each narrative has:
#   - label: human-readable name
#   - keyword_signals: words that appear in headlines/summaries → score weight 1.0
#   - ticker_anchors: tickers whose trend signals contribute directly
#   - source_boost: sources that particularly matter for this narrative

NARRATIVES: dict[str, dict] = {
    "ai_semiconductors": {
        "label": "AI & Semiconductors",
        "keyword_signals": [
            "artificial intelligence", "AI", "semiconductor", "chip", "GPU", "data center",
            "machine learning", "large language model", "LLM", "NVIDIA", "AMD",
            "TSMC", "quantum", "inference", "training", "neural", "cloud AI",
            "Azure", "Copilot", "OpenAI", "Gemini", "Llama", "generative",
        ],
        "ticker_anchors": [
            "NVDA", "AMD", "INTC", "QCOM", "ASML", "AVGO", "MU", "TSM",
            "MSFT", "GOOGL", "META", "AMZN", "AAPL",   # hyperscalers with deep AI
            "CRM", "ORCL",                              # enterprise AI/cloud
        ],
        "source_boost": ["ft", "bloomberg"],
    },
    "macro_liquidity": {
        "label": "Macro Liquidity Cycle",
        "keyword_signals": [
            "Federal Reserve", "Fed", "interest rate", "rate cut", "rate hike",
            "liquidity", "quantitative easing", "QE", "treasury yield", "yield curve",
            "inflation", "CPI", "PCE", "FOMC", "Jerome Powell", "monetary policy",
            "TCMB", "ECB", "central bank",
        ],
        "ticker_anchors": ["TLT", "SHY", "SPY", "DXY", "GLD"],
        "source_boost": ["ft", "reuters", "bloomberg"],
    },
    "risk_on_off": {
        "label": "Risk-On / Risk-Off Regime",
        "keyword_signals": [
            "risk appetite", "risk aversion", "safe haven", "flight to quality",
            "VIX", "volatility", "fear", "greed", "recession", "bear market",
            "bull market", "correction", "sell-off", "rally",
        ],
        "ticker_anchors": [
            "VIX", "GLD", "TLT", "SPY", "IWM", "QQQ",
            "NFLX", "PYPL",                             # consumer discretionary / growth beta
        ],
        "source_boost": ["cnbc", "bloomberg"],
    },
    "energy_transition": {
        "label": "Energy Transition",
        "keyword_signals": [
            "renewable energy", "solar", "wind power", "EV", "electric vehicle",
            "carbon", "emissions", "green energy", "climate", "OPEC", "oil price",
            "natural gas", "LNG", "nuclear", "hydrogen", "battery",
        ],
        "ticker_anchors": ["XOM", "CVX", "ENPH", "TSLA", "NEE", "FSLR"],
        "source_boost": ["ft", "reuters"],
    },
    "crypto_speculation": {
        "label": "Crypto Speculation Cycle",
        "keyword_signals": [
            "bitcoin", "BTC", "ethereum", "ETH", "crypto", "blockchain",
            "DeFi", "NFT", "halving", "altcoin", "stablecoin", "SEC crypto",
            "spot ETF", "coinbase", "binance", "web3", "solana", "avalanche",
            "layer 2", "defi protocol", "yield farming", "liquidity pool",
        ],
        "ticker_anchors": [
            "COIN", "MSTR", "RIOT", "MARA", "HOOD",
            # All tracked crypto assets
            "BTC", "ETH", "BNB", "SOL", "ADA", "AVAX", "DOT", "MATIC",
            "LINK", "UNI", "AAVE", "DOGE", "TON", "SUI", "ARB", "OP",
        ],
        "source_boost": ["cnbc", "bloomberg"],
    },
    "bist_turkish_macro": {
        "label": "BIST / Turkish Macro",
        "keyword_signals": [
            "BIST", "Borsa Istanbul", "Turkish lira", "TRY", "TCMB",
            "Turkey inflation", "Erdogan", "Şimşek", "Naci Ağbal",
            "Türkiye", "Istanbul stock", "Turkish economy",
        ],
        "ticker_anchors": [
            "THYAO.IS", "GARAN.IS", "AKBNK.IS", "EREGL.IS", "KCHOL.IS",
            "ASELS.IS", "BIMAS.IS", "TCELL.IS",
            # Additional BIST tickers tracked in the system
            "FROTO.IS", "PGSUS.IS", "SAHOL.IS", "SISE.IS",
            "TOASO.IS", "TUPRS.IS", "YKBNK.IS",
        ],
        "source_boost": ["newsapi"],
    },
}

NARRATIVE_PREV_KEY = "narrative:prev_scores"
NARRATIVE_TTL = 7200  # 2h Redis TTL

# Prototype text for each narrative (label + top keywords) — used for embedding similarity
_NARRATIVE_PROTOTYPE: dict[str, str] = {
    "ai_semiconductors":  "AI semiconductors artificial intelligence GPU chip data center machine learning NVDA AMD TSMC Azure OpenAI Copilot",
    "macro_liquidity":    "Federal Reserve interest rate monetary policy inflation CPI treasury yield QE central bank FOMC liquidity",
    "risk_on_off":        "risk appetite volatility VIX safe haven recession bull market bear market sell-off flight to quality",
    "energy_transition":  "renewable energy solar electric vehicle EV carbon emissions oil price OPEC natural gas hydrogen battery climate",
    "crypto_speculation": "bitcoin ethereum crypto blockchain DeFi NFT halving altcoin stablecoin solana layer2 defi protocol binance coinbase",
    "bist_turkish_macro": "BIST Borsa Istanbul Turkish lira TRY Turkey inflation Erdogan Şimşek Turkish economy central bank TCMB",
}


def _pack_vec(vec: list[float]) -> bytes:
    return struct.pack(f"{len(vec)}f", *vec)


def _unpack_vec(data: bytes) -> list[float]:
    n = len(data) // 4
    return list(struct.unpack(f"{n}f", data))


def _cosine_sim(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    return dot / (na * nb) if na and nb else 0.0


class NarrativeEngine:
    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
    ) -> None:
        self._pool = db_pool
        self._redis = redis

    def _score_narrative(
        self,
        narrative_id: str,
        defn: dict,
        trends: list[MarketTrend],
        news_items: list[dict],
    ) -> tuple[float, list[str], list[str], list[str]]:
        """
        Score one narrative against current market signals.
        Returns (strength_score, top_tickers, contributing_sources, sample_headlines).
        """
        keyword_signals = [kw.lower() for kw in defn["keyword_signals"]]
        ticker_anchors: set[str] = set(defn.get("ticker_anchors", []))
        source_boost: set[str] = set(defn.get("source_boost", []))

        score_components: list[float] = []
        contributing_tickers: list[str] = []
        contributing_sources: set[str] = set()
        matched_headlines: list[str] = []

        # Component 1: Ticker anchor scores
        for trend in trends:
            if trend.ticker in ticker_anchors:
                # Positive trend in anchor ticker contributes positively
                # Negative trend in anchor ticker contributes negatively
                abs_score = abs(trend.composite_score)
                if abs_score > 0.05:
                    score_components.append(abs_score)
                    contributing_tickers.append(trend.ticker)
                    contributing_sources.add(trend.exchange)

        # Component 2: News headline keyword matches
        news_matches = 0
        for item in news_items:
            title = (item.get("title") or "").lower()
            ai_summary = (item.get("ai_summary") or "").lower()
            combined = f"{title} {ai_summary}"

            matched_kws = sum(1 for kw in keyword_signals if kw in combined)
            if matched_kws >= 1:
                news_matches += 1
                # Boost if it's from a preferred source
                source = item.get("source", "").lower()
                if any(bs in source for bs in source_boost):
                    score_components.append(0.15)
                    contributing_sources.add(item.get("source", ""))
                else:
                    score_components.append(0.08)
                    contributing_sources.add(item.get("source", ""))

                if len(matched_headlines) < 3:
                    matched_headlines.append(item.get("title", "")[:100])

        # Aggregate score: average of components, capped at 1.0
        # Apply a coverage multiplier — more signals = higher confidence
        if not score_components:
            return 0.0, [], [], []

        raw_avg = sum(score_components) / max(len(score_components), 1)
        coverage_multiplier = min(1.0, len(score_components) / 10.0) * 0.3 + 0.7
        strength = min(1.0, raw_avg * coverage_multiplier)

        # Sort contributing tickers by their composite scores (descending abs value)
        ticker_trend_map = {t.ticker: t for t in trends}
        contributing_tickers.sort(
            key=lambda t: abs(ticker_trend_map.get(t, MarketTrend(ticker=t, exchange="")).composite_score),
            reverse=True,
        )

        return (
            round(strength, 4),
            contributing_tickers[:5],
            sorted(contributing_sources)[:4],
            matched_headlines,
        )

    async def _get_previous_score(self, narrative_id: str) -> float | None:
        if not self._redis:
            return None
        try:
            raw = await self._redis.hget(NARRATIVE_PREV_KEY, narrative_id)
            return float(raw) if raw else None
        except Exception:
            return None

    async def _store_scores(self, scores: dict[str, float]) -> None:
        if not self._redis:
            return
        try:
            await self._redis.hset(NARRATIVE_PREV_KEY, mapping={k: str(v) for k, v in scores.items()})
            await self._redis.expire(NARRATIVE_PREV_KEY, NARRATIVE_TTL)
        except Exception:
            pass

    async def compute_narratives(
        self,
        trends: list[MarketTrend],
        news_items: list[dict],
    ) -> list[NarrativeCluster]:
        clusters: list[NarrativeCluster] = []
        current_scores: dict[str, float] = {}

        for narrative_id, defn in NARRATIVES.items():
            strength, top_tickers, sources, headlines = self._score_narrative(
                narrative_id, defn, trends, news_items
            )
            current_scores[narrative_id] = strength

            prev_score = await self._get_previous_score(narrative_id)
            if prev_score is not None:
                delta = strength - prev_score
                if delta > 0.05:
                    momentum = "accelerating"
                elif delta < -0.05:
                    momentum = "decelerating"
                else:
                    momentum = "neutral"
            else:
                delta = 0.0
                momentum = "neutral"

            cluster = NarrativeCluster(
                name=narrative_id,
                label=defn["label"],
                strength_score=strength,
                momentum=momentum,
                momentum_delta=round(delta, 4),
                top_tickers=top_tickers,
                contributing_sources=sources,
                sample_headlines=headlines,
                computed_at=datetime.now(timezone.utc),
            )
            clusters.append(cluster)

        # Persist previous scores for next cycle momentum calculation
        await self._store_scores(current_scores)

        # Sort by strength descending
        clusters.sort(key=lambda c: c.strength_score, reverse=True)

        # Persist to DB
        await self._upsert_clusters(clusters)

        logger.info(
            "narratives_computed",
            count=len(clusters),
            top=clusters[0].name if clusters else "none",
            top_strength=clusters[0].strength_score if clusters else 0,
        )
        return clusters

    async def _upsert_clusters(self, clusters: list[NarrativeCluster]) -> None:
        async with self._pool.acquire() as conn:
            for c in clusters:
                await conn.execute(
                    """
                    INSERT INTO narrative_clusters (
                        name, label, strength_score, momentum, momentum_delta,
                        top_tickers, contrib_sources, sample_headlines, computed_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT (name) DO UPDATE SET
                        label = EXCLUDED.label,
                        strength_score = EXCLUDED.strength_score,
                        momentum = EXCLUDED.momentum,
                        momentum_delta = EXCLUDED.momentum_delta,
                        top_tickers = EXCLUDED.top_tickers,
                        contrib_sources = EXCLUDED.contrib_sources,
                        sample_headlines = EXCLUDED.sample_headlines,
                        computed_at = EXCLUDED.computed_at
                    """,
                    c.name, c.label, c.strength_score, c.momentum, c.momentum_delta,
                    c.top_tickers, c.contributing_sources, c.sample_headlines, c.computed_at,
                )

    async def classify_ticker(self, ticker: str, headlines: list[str]) -> str | None:
        """Return the best-matching narrative ID for a single ticker, or None.

        Strategy (in priority order):
          1. Direct anchor match — O(1), zero cost.
          2. Keyword score ≥ 0.10 — fast substring scan.
          3. Semantic embedding fallback — only when #1 and #2 both fail,
             and only when Redis + OpenAI are available.
        """
        combined_text = f"{ticker} " + " ".join(headlines).lower()
        best_id: str | None = None
        best_score = 0.0

        for narrative_id, defn in NARRATIVES.items():
            if ticker in defn.get("ticker_anchors", []):
                return narrative_id  # direct anchor match — stop here

            kw_matches = sum(
                1 for kw in defn["keyword_signals"] if kw.lower() in combined_text
            )
            kw_score = kw_matches / max(len(defn["keyword_signals"]), 1)
            if kw_score > best_score:
                best_score = kw_score
                best_id = narrative_id

        if best_score >= 0.10:
            return best_id

        # Semantic fallback: embed the ticker + headlines and compare to narrative prototypes
        if self._redis and headlines:
            result = await self._semantic_classify(combined_text)
            if result:
                return result

        return best_id if best_score > 0.05 else None

    async def _get_embedding(self, text: str) -> list[float] | None:
        """Embed text via OpenAI text-embedding-3-small (512 dims)."""
        try:
            import aiohttp
            key = get_settings().OPENAI_API_KEY
            if not key or key.startswith("YOUR_"):
                return None
            payload = {
                "input": text[:2000],
                "model": "text-embedding-3-small",
                "dimensions": 512,
            }
            headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                    "https://api.openai.com/v1/embeddings",
                    json=payload, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    return data["data"][0]["embedding"]
        except Exception as exc:
            logger.debug("narrative_embed_error", error=str(exc))
            return None

    async def _get_prototype_embedding(self, narrative_id: str) -> list[float] | None:
        """Return cached prototype embedding, computing it on first call or cache miss."""
        if not self._redis:
            return None
        key = f"{_PROTO_KEY_PREFIX}{narrative_id}"
        try:
            raw = await self._redis.get(key)
            if raw:
                return _unpack_vec(raw)
            proto_text = _NARRATIVE_PROTOTYPE.get(narrative_id, "")
            if not proto_text:
                return None
            vec = await self._get_embedding(proto_text)
            if vec:
                await self._redis.setex(key, _PROTO_TTL, _pack_vec(vec))
            return vec
        except Exception:
            return None

    async def _semantic_classify(self, text: str) -> str | None:
        """Embed text and return the narrative_id with highest cosine similarity (≥0.40)."""
        ticker_vec = await self._get_embedding(text[:500])
        if not ticker_vec:
            return None
        best_id: str | None = None
        best_sim = 0.40  # minimum similarity threshold
        for narrative_id in NARRATIVES:
            proto_vec = await self._get_prototype_embedding(narrative_id)
            if not proto_vec:
                continue
            sim = _cosine_sim(ticker_vec, proto_vec)
            if sim > best_sim:
                best_sim = sim
                best_id = narrative_id
        if best_id:
            logger.debug("narrative_semantic_match", narrative=best_id, similarity=round(best_sim, 4))
        return best_id

    async def get_top_narratives(self, limit: int = 3) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT name, label, strength_score, momentum, momentum_delta,
                       top_tickers, contrib_sources, sample_headlines, computed_at
                FROM narrative_clusters
                ORDER BY strength_score DESC
                LIMIT $1
                """,
                limit,
            )
            return [dict(r) for r in rows]
