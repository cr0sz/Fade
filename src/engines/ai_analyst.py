from __future__ import annotations

import hashlib
from datetime import datetime

import asyncpg
import redis.asyncio as aioredis

from src.core.config import get_settings
from src.core.cost_control import CostController
from src.core.logging import get_logger
from src.db.repositories import AIOutputRepository, NewsRepository, ProviderUsageRepository
from src.providers.ai import AI_PROVIDERS
from src.providers.ai.openai_provider import AIResponse

logger = get_logger(__name__)

SYSTEM_PROMPT = """You are a senior market intelligence analyst providing data-driven analysis
of financial markets. You synthesize news, sentiment, and market data into actionable intelligence.

CRITICAL RULES:
1. NEVER provide specific buy/sell recommendations
2. ALWAYS append: "⚠️ For informational purposes only. Not financial advice."
3. Be objective and data-driven
4. Quantify when possible (percentages, timeframes)
5. Highlight key risks and uncertainties
6. Keep responses concise and structured
"""


class AIAnalystEngine:
    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: aioredis.Redis,
        cost_controller: CostController,
    ) -> None:
        self._pool = db_pool
        self._redis = redis
        self._cost = cost_controller
        self._settings = get_settings()
        self._ai_repo = AIOutputRepository(db_pool)
        self._news_repo = NewsRepository(db_pool)
        self._usage_repo = ProviderUsageRepository(db_pool)

    async def _get_provider(self):
        provider_name = await self._cost.get_preferred_ai_provider()
        if provider_name == "none":
            raise RuntimeError("AI budget exhausted for today")
        provider_cls = AI_PROVIDERS.get(provider_name)
        if not provider_cls:
            provider_cls = AI_PROVIDERS["openai"]
        return provider_cls(), provider_name

    def _make_prompt_hash(self, prompt: str) -> str:
        return hashlib.sha256(prompt.encode()).hexdigest()[:16]

    async def _cached_complete(
        self,
        prompt: str,
        output_type: str,
        ticker: str | None = None,
        ttl: int | None = None,
        max_tokens: int = 1000,
    ) -> str:
        if not await self._cost.is_ai_allowed():
            return "⚠️ AI analysis temporarily unavailable (daily budget reached). Try again tomorrow."

        prompt_hash = self._make_prompt_hash(prompt)
        ttl = ttl or self._settings.AI_CACHE_TTL_DIGEST

        # Check Redis cache first
        cache_key = f"ai:response:{prompt_hash}"
        cached = await self._redis.get(cache_key)
        if cached:
            logger.debug("ai_cache_hit", hash=prompt_hash)
            return cached

        # Check DB cache
        db_cached = await self._ai_repo.get_by_hash(prompt_hash)
        if db_cached:
            await self._redis.setex(cache_key, ttl, db_cached["content"])
            return db_cached["content"]

        # Call AI provider
        provider, provider_name = await self._get_provider()
        try:
            resp: AIResponse = await provider.complete(
                prompt=prompt,
                system=SYSTEM_PROMPT,
                max_tokens=max_tokens,
            )
            content = resp.content

            # Record cost
            await self._cost.record_cost(provider_name, resp.cost_usd)
            await self._usage_repo.log(
                provider=provider_name,
                operation=output_type,
                input_tokens=resp.input_tokens,
                output_tokens=resp.output_tokens,
                cost_usd=resp.cost_usd,
            )

            # Cache response
            await self._redis.setex(cache_key, ttl, content)
            await self._ai_repo.store(
                output_type=output_type,
                ticker=ticker,
                prompt_hash=prompt_hash,
                content=content,
                model_used=resp.model,
                cost_usd=resp.cost_usd,
            )

            logger.info(
                "ai_completion",
                provider=provider_name,
                type=output_type,
                cost=resp.cost_usd,
            )
            return content

        except Exception as exc:
            await self._usage_repo.log(
                provider=provider_name,
                operation=output_type,
                success=False,
            )
            logger.error("ai_completion_error", provider=provider_name, error=str(exc))
            raise

    async def analyze_ticker(self, ticker: str, context: str) -> str:
        prompt = (
            f"Analyze {ticker} based on the following recent market context:\n\n"
            f"{context}\n\n"
            f"Provide:\n"
            f"1. Key factors driving the current narrative (2-3 points)\n"
            f"2. Risk factors to monitor (2 points)\n"
            f"3. Macro context relevance\n"
            f"4. Sentiment assessment (bullish/neutral/bearish with reasoning)\n\n"
            f"Be concise. Max 200 words."
        )
        return await self._cached_complete(
            prompt, output_type="ticker_analysis", ticker=ticker,
            ttl=self._settings.AI_CACHE_TTL_QUERY, max_tokens=400,
        )

    async def generate_market_digest(self, news_headlines: list[str], trends: list[dict]) -> str:
        headlines_text = "\n".join(f"- {h}" for h in news_headlines[:15])
        trending = ", ".join(t.get("ticker", "") for t in trends[:10])
        prompt = (
            f"Generate a brief market intelligence digest based on today's data.\n\n"
            f"TOP NEWS HEADLINES:\n{headlines_text}\n\n"
            f"TRENDING TICKERS: {trending}\n\n"
            f"Provide:\n"
            f"1. Market narrative (3-4 sentences)\n"
            f"2. Sectors to watch (2-3 points)\n"
            f"3. Key risk factors (2 points)\n"
            f"4. Notable macro signals\n\n"
            f"Format for Telegram. Use emojis sparingly. Max 250 words."
        )
        return await self._cached_complete(
            prompt, output_type="market_digest",
            ttl=self._settings.AI_CACHE_TTL_DIGEST, max_tokens=500,
        )

    async def generate_bist_digest(self, bist_context: str) -> str:
        prompt = (
            f"Generate a Borsa Istanbul (BIST) market intelligence brief.\n\n"
            f"Context:\n{bist_context}\n\n"
            f"Include:\n"
            f"1. BIST 100 momentum assessment\n"
            f"2. Key BIST sectors in focus\n"
            f"3. TRY/USD macro context\n"
            f"4. TCMB policy relevance\n"
            f"5. Notable Turkish corporate actions\n\n"
            f"Format for Telegram. Max 200 words."
        )
        return await self._cached_complete(
            prompt, output_type="bist_digest",
            ttl=self._settings.AI_CACHE_TTL_DIGEST, max_tokens=400,
        )

    async def answer_market_question(self, question: str) -> str:
        prompt = (
            f"A user asks: '{question}'\n\n"
            f"Answer using your market intelligence knowledge. "
            f"Be factual, objective, and concise (max 150 words). "
            f"If you need real-time data you don't have, say so clearly."
        )
        return await self._cached_complete(
            prompt, output_type="user_query",
            ttl=self._settings.AI_CACHE_TTL_QUERY, max_tokens=300,
        )

    async def analyze_ticker_with_confidence(
        self,
        ticker: str,
        context: str,
        confidence_level: str = "medium",
        narrative_cluster: str | None = None,
        signal_quality: str = "medium",
    ) -> str:
        narrative_note = (
            f"\nThematic narrative: {narrative_cluster.replace('_', ' ').title()}"
            if narrative_cluster else ""
        )
        uncertainty_note = {
            "low": "\n⚠️ Signal confidence is LOW — data is limited or contradictory. High uncertainty.",
            "medium": "\n📊 Signal confidence is MEDIUM — treat with moderate caution.",
            "high": "",
        }.get(confidence_level, "")

        prompt = (
            f"Analyze {ticker} based on the following market intelligence context:\n\n"
            f"{context}{narrative_note}\n\n"
            f"Confidence level: {confidence_level.upper()} | Signal quality: {signal_quality.upper()}"
            f"{uncertainty_note}\n\n"
            f"Provide:\n"
            f"1. Key narrative drivers (2-3 points) — explain WHY, not just WHAT\n"
            f"2. Cross-source signal agreement (do news, social, and smart money align?)\n"
            f"3. Risk factors and uncertainty (2 points)\n"
            f"4. Macro / thematic context relevance\n"
            f"5. Sentiment assessment (bullish/neutral/bearish with explicit reasoning)\n\n"
            f"IMPORTANT: Do NOT predict price targets. Do NOT recommend buying or selling.\n"
            f"Be explicit about uncertainty when confidence is low.\n"
            f"Max 220 words."
        )
        return await self._cached_complete(
            prompt, output_type="ticker_analysis_v2", ticker=ticker,
            ttl=self._settings.AI_CACHE_TTL_QUERY, max_tokens=450,
        )

    async def generate_institutional_brief(
        self,
        regime: dict | None,
        confluence: dict | None,
        market_flow: dict | None,
        leading_sectors: list[str] | None = None,
    ) -> str:
        """
        Generate an institutional capital flow intelligence brief.
        Explains WHY capital is moving and WHERE it is rotating.

        STRICT RULES: No price prediction. No buy/sell advice. No financial instructions.
        """
        if not regime and not confluence and not market_flow:
            return "⚠️ Insufficient institutional intelligence data for briefing."

        regime_line = ""
        if regime:
            shift_note = " ⚠️ REGIME SHIFT DETECTED" if regime.get("regime_shift") else ""
            regime_line = (
                f"Macro Regime: {regime.get('regime', 'unknown').replace('_', ' ').upper()}"
                f" (confidence: {float(regime.get('confidence') or 0):.0%}){shift_note}"
            )

        confluence_line = ""
        if confluence:
            confluence_line = (
                f"Institutional Confluence: strength={float(confluence.get('strength') or 0):.0f}/100, "
                f"direction={confluence.get('direction', 'mixed')}"
            )

        flow_line = ""
        if market_flow:
            flow_line = (
                f"Market Flow: {market_flow.get('direction', 'neutral').upper()} "
                f"(strength={float(market_flow.get('flow_strength') or 0):.0f}/100, "
                f"acceleration={market_flow.get('acceleration', 'stable')})"
            )

        sector_line = ""
        if leading_sectors:
            sector_line = f"Capital Rotation: Leading sectors — {', '.join(leading_sectors[:3])}"

        context_block = "\n".join(filter(None, [regime_line, confluence_line, flow_line, sector_line]))

        prompt = (
            f"Generate an institutional market intelligence brief based on the following signals:\n\n"
            f"{context_block}\n\n"
            f"Explain:\n"
            f"1. What the macro regime implies for capital allocation direction (2 sentences)\n"
            f"2. WHY institutional capital is moving in this direction (fundamental reasons)\n"
            f"3. Which sectors/assets are attracting vs losing institutional attention\n"
            f"4. Key risks that could invalidate the current institutional positioning thesis\n"
            f"5. Uncertainty assessment: how reliable are these signals?\n\n"
            f"STRICT RULES:\n"
            f"- Do NOT predict prices or price targets\n"
            f"- Do NOT recommend buying, selling, or any position\n"
            f"- This is INTELLIGENCE ONLY — describe what institutions appear to be doing, not what to do\n"
            f"- Format for Telegram. Max 250 words."
        )
        return await self._cached_complete(
            prompt, output_type="institutional_brief",
            ttl=self._settings.AI_CACHE_TTL_DIGEST, max_tokens=500,
        )

    async def analyze_ticker_flow(
        self,
        ticker: str,
        flow: dict | None,
        confluence: dict | None,
        regime: dict | None,
        existing_context: str = "",
    ) -> str:
        """
        Extend ticker analysis with institutional flow + confluence context.
        Called alongside analyze_ticker_with_confidence — additive only.

        STRICT: No price prediction. No buy/sell signals.
        """
        if not flow and not confluence:
            return ""

        flow_section = ""
        if flow:
            flow_section = (
                f"Institutional Flow: {flow.get('direction', 'neutral').upper()} "
                f"(strength={float(flow.get('flow_strength') or 0):.0f}/100, "
                f"{flow.get('acceleration', 'stable')}, "
                f"insider_cluster={flow.get('insider_cluster', 'neutral')})"
            )

        confluence_section = ""
        if confluence:
            sources = confluence.get("contributing_sources") or {}
            confluence_section = (
                f"Confluence: strength={float(confluence.get('strength') or 0):.0f}/100, "
                f"direction={confluence.get('direction', 'mixed')}, "
                f"narrative={sources.get('narrative_cluster', 'unknown')}"
            )

        regime_section = ""
        if regime:
            regime_section = f"Macro regime: {regime.get('regime', 'unknown').replace('_', ' ')}"

        combined = "\n".join(filter(None, [flow_section, confluence_section, regime_section]))
        if existing_context:
            combined = f"{existing_context}\n\n--- Institutional Signals ---\n{combined}"

        prompt = (
            f"Provide additional institutional perspective on {ticker}:\n\n"
            f"{combined}\n\n"
            f"Explain in 3 bullet points:\n"
            f"• What the institutional flow signals suggest about smart money positioning on {ticker}\n"
            f"• How the macro regime context amplifies or contradicts the ticker's narrative\n"
            f"• What contradictions or uncertainties exist in the institutional signal picture\n\n"
            f"Max 120 words. No price targets. No recommendations."
        )
        return await self._cached_complete(
            prompt, output_type="ticker_flow_analysis", ticker=ticker,
            ttl=self._settings.AI_CACHE_TTL_QUERY, max_tokens=250,
        )

    async def generate_narrative_digest(
        self,
        narratives: list[dict],
        headlines: list[str],
    ) -> str:
        """
        Generate a thematic market narrative digest from NarrativeEngine clusters.
        Does NOT predict prices. Explains WHY themes are active.
        """
        if not narratives:
            return "No active market narratives detected."

        narrative_lines = []
        for n in narratives[:4]:
            momentum_arrow = {"accelerating": "↑", "decelerating": "↓"}.get(
                n.get("momentum", ""), "→"
            )
            narrative_lines.append(
                f"- {n.get('label', n['name'])} "
                f"(strength: {n.get('strength_score', 0):.2f} {momentum_arrow}): "
                + (n.get("sample_headlines") or [""])[0][:80]
            )

        headlines_text = "\n".join(f"• {h}" for h in headlines[:10])
        prompt = (
            f"Synthesize the following active market narratives into a brief thematic digest.\n\n"
            f"ACTIVE NARRATIVES:\n" + "\n".join(narrative_lines) + "\n\n"
            f"SUPPORTING HEADLINES:\n{headlines_text}\n\n"
            f"Provide:\n"
            f"1. The dominant market theme today (1-2 sentences)\n"
            f"2. Why these narratives are converging or diverging\n"
            f"3. Which sectors / asset classes are most affected\n"
            f"4. One key uncertainty or risk to the dominant narrative\n\n"
            f"Format for Telegram. Max 200 words. No price targets. No buy/sell signals."
        )
        return await self._cached_complete(
            prompt, output_type="narrative_digest",
            ttl=self._settings.AI_CACHE_TTL_DIGEST, max_tokens=400,
        )

    async def get_cost_summary(self) -> dict:
        return await self._cost.get_cost_summary()
