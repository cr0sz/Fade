from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import asyncpg

from src.core.config import get_settings
from src.core.logging import get_logger
from src.db.repositories import NewsRepository
from src.engines.deduplication import HybridDeduplicator
from src.models.news import NewsItem
from src.providers.news import NEWS_PROVIDERS
from src.providers.resilience import CircuitOpenError, get_all_circuit_statuses

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

TICKER_RE = re.compile(r'\b([A-Z]{1,5}(?:\.IS)?)\b')

# Common name → ticker mappings for financial entities not mentioned by symbol
_NAME_TO_TICKER: dict[str, str] = {
    "bitcoin": "BTC", "btc": "BTC",
    "ethereum": "ETH", "ether": "ETH",
    "solana": "SOL",
    "binance": "BNB",
    "ripple": "XRP",
    "avalanche": "AVAX",
    "cardano": "ADA",
    "dogecoin": "DOGE",
    "polkadot": "DOT",
    "chainlink": "LINK",
    "polygon": "MATIC",
    "arbitrum": "ARB",
    "optimism": "OP",
    "uniswap": "UNI",
    "aave": "AAVE",
    "sui": "SUI",
    "toncoin": "TON", "telegram": "TON",
    "nvidia": "NVDA",
    "amd": "AMD",
    "apple": "AAPL",
    "microsoft": "MSFT",
    "google": "GOOGL", "alphabet": "GOOGL",
    "meta": "META", "facebook": "META",
    "tesla": "TSLA",
    "amazon": "AMZN",
    "netflix": "NFLX",
    "intel": "INTC",
    "qualcomm": "QCOM",
    "broadcom": "AVGO",
    "salesforce": "CRM",
    "oracle": "ORCL",
    "paypal": "PYPL",
    "coinbase": "COIN",
    "microstrategy": "MSTR",
    "fed ": "TLT", "federal reserve": "TLT",
    "gold": "GLD",
    "s&p": "SPY", "s&p 500": "SPY",
    # BIST Turkish companies
    "türk hava": "THYAO.IS", "türk havayolları": "THYAO.IS", "thy": "THYAO.IS",
    "garanti": "GARAN.IS", "garanti bbva": "GARAN.IS",
    "aselsan": "ASELS.IS",
    "koç holding": "KCHOL.IS",
    "ereğli": "EREGL.IS", "erdemir": "EREGL.IS",
    "bim": "BIMAS.IS",
    "şişecam": "SISE.IS",
    "turkcell": "TCELL.IS",
    "akbank": "AKBNK.IS",
    "yapı kredi": "YKBNK.IS",
    "tüpraş": "TUPRS.IS",
    "sabancı": "SAHOL.IS",
    "ford otosan": "FROTO.IS",
    "tofaş": "TOASO.IS",
    "pegasus": "PGSUS.IS",
    # English transliterations for BIST (matched in Yahoo Finance / non-Turkish sources)
    "turkish airlines": "THYAO.IS", "turk hava yollari": "THYAO.IS",
    "garanti bank": "GARAN.IS",
    "koc holding": "KCHOL.IS",
    "sisecam": "SISE.IS",
    "yapi kredi": "YKBNK.IS", "yapi kredi bank": "YKBNK.IS",
    "tupras": "TUPRS.IS",
    "sabanci holding": "SAHOL.IS",
    "pegasus airlines": "PGSUS.IS",
    "bim stores": "BIMAS.IS",
}

TICKER_BLACKLIST = {
    "A", "I", "AT", "BE", "BY", "DO", "GO", "IF", "IN", "IS", "IT",
    "ME", "MY", "NO", "OF", "OK", "ON", "OR", "SO", "TO", "UP", "US",
    "WE", "NYSE", "NASDAQ", "ETF", "IPO", "CEO", "CFO", "CTO", "SEC",
    "FED", "GDP", "CPI", "PMI", "ECB", "IMF", "GDP", "M1", "M2",
    "PE", "EPS", "PEG", "ROE", "ROA", "EBIT", "EBITDA",
}


@dataclass
class CycleResult:
    fetched: int = 0
    new_items: int = 0
    duplicates: int = 0
    fuzzy_dupes: int = 0
    semantic_dupes: int = 0
    errors: int = 0
    providers_ok: list[str] = field(default_factory=list)
    providers_failed: list[str] = field(default_factory=list)
    providers_circuit_open: list[str] = field(default_factory=list)
    total_latency_ms: float = 0.0


class NewsIntelligenceEngine:
    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: "aioredis.Redis | None" = None,
    ) -> None:
        self._pool = db_pool
        self._redis = redis
        self._repo = NewsRepository(db_pool)
        self._settings = get_settings()
        self._known_symbols: set[str] = set()
        self._dedup: HybridDeduplicator | None = None

    def _get_dedup(self) -> HybridDeduplicator | None:
        if self._redis is None:
            return None
        if self._dedup is None:
            self._dedup = HybridDeduplicator(self._redis)
        return self._dedup

    async def _load_known_symbols(self) -> None:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT symbol FROM tickers WHERE is_active = true")
            self._known_symbols = {r["symbol"] for r in rows}

    def _extract_tickers(self, text: str) -> list[str]:
        found: set[str] = set()
        upper = text.upper()
        lower = text.lower()

        # Regex symbol extraction (e.g. "BTC", "NVDA", "THYAO.IS")
        for t in TICKER_RE.findall(upper):
            if t not in TICKER_BLACKLIST and (t in self._known_symbols or t.endswith(".IS")):
                found.add(t)

        # Name → ticker mapping (e.g. "bitcoin" → "BTC", "nvidia" → "NVDA")
        for name, ticker in _NAME_TO_TICKER.items():
            if name in lower and ticker in self._known_symbols:
                found.add(ticker)

        return list(found)

    def _classify_sector(self, title: str, content: str) -> str | None:
        combined = f"{title} {content}".lower()
        for sector, keywords in self._settings.SECTOR_KEYWORDS.items():
            if any(kw.lower() in combined for kw in keywords):
                return sector
        return None

    def _vader_score(self, text: str) -> float:
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            return SentimentIntensityAnalyzer().polarity_scores(text)["compound"]
        except Exception:
            return 0.0

    async def run_cycle(
        self,
        cycle_id: str | None = None,
        job: str = "news_cycle",
    ) -> CycleResult:
        result = CycleResult()
        cycle_start = time.monotonic()
        await self._load_known_symbols()

        dedup = self._get_dedup()
        use_embeddings = self._settings.DEDUP_USE_EMBEDDINGS and bool(self._settings.OPENAI_API_KEY)

        for name, provider_cls in NEWS_PROVIDERS.items():
            provider = provider_cls()
            provider_start = time.monotonic()
            p_status = "ok"

            # Snapshot result counters before this provider so we can compute deltas
            snap_new      = result.new_items
            snap_exact    = result.duplicates
            snap_fuzzy    = result.fuzzy_dupes
            snap_semantic = result.semantic_dupes
            p_fetched = 0
            p_errors  = 0

            logger.info("provider_fetch_start", provider=name)

            try:
                healthy = await provider.health_check()
                if not healthy:
                    logger.warning(
                        "news_provider_unhealthy",
                        provider=name,
                        circuit_statuses=get_all_circuit_statuses(),
                    )
                    result.providers_failed.append(name)
                    p_status = "failed"
                    continue

                items = await provider.fetch(limit=self._settings.NEWS_FETCH_LIMIT)
                p_fetched = len(items)
                result.fetched += p_fetched

                for item in items:
                    await self._process_item(item, dedup, use_embeddings, result)

                provider_latency_ms = (time.monotonic() - provider_start) * 1000

                p_new      = result.new_items   - snap_new
                p_exact    = result.duplicates  - snap_exact
                p_fuzzy    = result.fuzzy_dupes - snap_fuzzy
                p_semantic = result.semantic_dupes - snap_semantic

                result.providers_ok.append(name)
                logger.info(
                    "provider_fetch_done",
                    provider=name,
                    fetched=p_fetched,
                    new=p_new,
                    dup_exact=p_exact,
                    dup_fuzzy=p_fuzzy,
                    dup_semantic=p_semantic,
                    latency_ms=round(provider_latency_ms, 1),
                )

            except CircuitOpenError:
                provider_latency_ms = (time.monotonic() - provider_start) * 1000
                logger.warning("news_provider_circuit_open", provider=name)
                result.providers_circuit_open.append(name)
                p_status = "circuit_open"
                p_new = p_exact = p_fuzzy = p_semantic = 0

            except Exception as exc:
                provider_latency_ms = (time.monotonic() - provider_start) * 1000
                logger.error(
                    "news_provider_error",
                    provider=name,
                    error=str(exc),
                    latency_ms=round(provider_latency_ms, 1),
                )
                result.errors += 1
                result.providers_failed.append(name)
                p_status = "failed"
                p_errors = 1
                p_new = p_exact = p_fuzzy = p_semantic = 0

            finally:
                latency_ms = round((time.monotonic() - provider_start) * 1000, 1)
                if cycle_id:
                    await self._write_ingestion_log(
                        cycle_id=cycle_id,
                        job=job,
                        provider=name,
                        fetched=p_fetched,
                        new_count=result.new_items - snap_new,
                        dup_exact=result.duplicates - snap_exact,
                        dup_fuzzy=result.fuzzy_dupes - snap_fuzzy,
                        dup_semantic=result.semantic_dupes - snap_semantic,
                        errors=p_errors,
                        latency_ms=latency_ms,
                        status=p_status,
                    )

        result.total_latency_ms = (time.monotonic() - cycle_start) * 1000
        logger.info(
            "news_cycle_complete",
            fetched=result.fetched,
            new=result.new_items,
            exact_dupes=result.duplicates,
            fuzzy_dupes=result.fuzzy_dupes,
            semantic_dupes=result.semantic_dupes,
            providers_ok=result.providers_ok,
            providers_failed=result.providers_failed,
            circuit_open=result.providers_circuit_open,
            total_latency_ms=round(result.total_latency_ms, 1),
        )
        return result

    async def _write_ingestion_log(
        self,
        *,
        cycle_id: str,
        job: str,
        provider: str,
        fetched: int,
        new_count: int,
        dup_exact: int,
        dup_fuzzy: int,
        dup_semantic: int,
        errors: int,
        latency_ms: float,
        status: str,
    ) -> None:
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO provider_ingestion_log
                        (cycle_id, job, provider, fetched, new_count,
                         dup_exact, dup_fuzzy, dup_semantic, errors, latency_ms, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """,
                    cycle_id, job, provider, fetched, new_count,
                    dup_exact, dup_fuzzy, dup_semantic, errors, latency_ms, status,
                )
        except Exception as exc:
            logger.warning("ingestion_log_write_error", provider=provider, error=str(exc))

    async def _process_item(
        self,
        item: NewsItem,
        dedup: HybridDeduplicator | None,
        use_embeddings: bool,
        result: CycleResult,
    ) -> bool:
        if not item.content_hash:
            return False

        # Level 1: exact DB hash check (always runs)
        db_hash_exists = await self._repo.exists_by_hash(item.content_hash)

        if dedup is not None:
            is_dup, reason = await dedup.check(
                item=item,
                db_hash_exists=db_hash_exists,
                use_embeddings=use_embeddings,
                openai_api_key=self._settings.OPENAI_API_KEY,
            )
            if is_dup:
                if reason == "fuzzy":
                    result.fuzzy_dupes += 1
                elif reason == "semantic":
                    result.semantic_dupes += 1
                else:
                    result.duplicates += 1
                return False
        else:
            # Fallback: SHA256 only (no Redis available)
            if db_hash_exists:
                result.duplicates += 1
                return False

        text = f"{item.title} {item.content or ''}"
        extracted = self._extract_tickers(text)
        # Merge provider-pre-tagged tickers (e.g. BTC+ETH from crypto RSS) with
        # text-extracted ones (e.g. AVAX from "Avalanche" mention), preserving both.
        pre_tagged = list(item.ticker_symbols or [])
        item.ticker_symbols = list(dict.fromkeys(pre_tagged + extracted))
        item.sector = self._classify_sector(item.title, item.content or "")
        item.sentiment_score = self._vader_score(text)

        item_id = await self._repo.insert(item)
        if item_id:
            result.new_items += 1
            return True

        result.duplicates += 1
        return False

    # ── Source authority weights ──────────────────────────────────────────────
    # Higher = more market-moving. Applied as multiplier to impact_score in TDE.
    _SOURCE_AUTHORITY: dict[str, float] = {
        "reuters.com": 2.0,
        "bloomberg.com": 2.0,
        "ft.com": 1.8,
        "wsj.com": 1.8,
        "apnews.com": 1.8,
        "coindesk.com": 1.6,
        "theblock.co": 1.5,
        "cnbc.com": 1.5,
        "cointelegraph.com": 1.4,
        "decrypt.co": 1.3,
        "marketwatch.com": 1.2,
        "yahoo.com": 1.0,
        "investing.com": 1.0,
        "seekingalpha.com": 0.9,
    }
    _SOURCE_AUTHORITY_DEFAULT = 0.8

    def _source_authority(self, article: dict) -> float:
        domain = (article.get("source_domain") or "").lower().lstrip("www.")
        for pattern, weight in self._SOURCE_AUTHORITY.items():
            if pattern in domain:
                return weight
        return self._SOURCE_AUTHORITY_DEFAULT

    async def batch_ai_summarize(self, ai_provider) -> int:
        """Legacy entrypoint — delegates to batch_analyze."""
        return await self.batch_analyze(ai_provider)

    async def batch_analyze(self, ai_provider) -> int:
        """Analyze unscored articles: Ollama produces summary + impact_score + urgency."""
        articles = await self._repo.get_without_impact_score(limit=50)
        if not articles:
            return 0

        analyzed = 0
        for article in articles:
            try:
                analyzed += await self._analyze_one(article, ai_provider)
            except Exception as exc:
                logger.error("news_analyze_error", news_id=article.get("id"), error=str(exc))

        return analyzed

    async def _analyze_one(self, article: dict, ai_provider) -> int:
        content_preview = (article.get("content") or article.get("summary") or "")[:1200]
        title = article.get("title", "")
        source = article.get("source") or article.get("source_domain") or "unknown"

        prompt = f"""Analyze this financial news article and respond with valid JSON only. No explanation, no markdown.

Title: {title}
Source: {source}
Content: {content_preview}

Respond with this exact JSON:
{{
  "summary": "<2 sentence market-focused summary>",
  "impact_score": <float from -1.0 (very bearish) to 1.0 (very bullish) for crypto/stocks>,
  "urgency": <0=noise, 1=low, 2=medium, 3=breaking/market-moving>,
  "reason": "<why this score>"
}}

Rules:
- urgency=3 only for: central bank decisions, major regulatory actions, large exchange hacks, major political statements about crypto/markets
- urgency=2 for: earnings surprises, ETF approvals, partnership announcements, executive comments
- urgency=1 for: routine updates, analyst opinions, minor news
- urgency=0 for: fluff, editorials, irrelevant content"""

        start = time.monotonic()
        resp = await ai_provider.complete(prompt, max_tokens=300)
        latency_ms = (time.monotonic() - start) * 1000

        # Parse JSON from response — strip any markdown fences Ollama adds
        raw = resp.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip().rstrip("```").strip()

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            # Fallback: store summary only with neutral scores
            parsed = {"summary": raw[:500], "impact_score": 0.0, "urgency": 0}

        summary = str(parsed.get("summary", ""))[:1000]
        impact_score = max(-1.0, min(1.0, float(parsed.get("impact_score", 0.0))))
        urgency = max(0, min(3, int(parsed.get("urgency", 0))))
        source_authority = self._source_authority(article)

        await self._repo.update_impact_analysis(
            str(article["id"]), summary, impact_score, urgency, source_authority
        )

        logger.info(
            "news_analyzed",
            news_id=article["id"],
            impact=round(impact_score, 3),
            urgency=urgency,
            authority=source_authority,
            model=resp.model,
            latency_ms=round(latency_ms, 1),
        )

        # Breaking news (urgency=3 + strong impact) → emit instant signals + Discord alert
        if urgency == 3 and abs(impact_score) >= 0.4:
            tickers = article.get("ticker_symbols") or []
            await self._emit_breaking_news_signals(article, tickers, impact_score, urgency, source_authority, summary)

        return 1

    # Tickers we trade live — only emit breaking signals for these
    _LIVE_TICKERS = frozenset({"BTC", "ETH", "SOL", "BNB", "XRP", "AVAX", "AAVE", "OP", "DOGE"})

    async def _emit_breaking_news_signals(
        self,
        article: dict,
        tickers: list[str],
        impact_score: float,
        urgency: int,
        authority: float,
        summary: str,
    ) -> None:
        from datetime import datetime, timezone
        from src.engines.signal_registry_engine import SignalRegistryEngine
        from src.core.discord import send_embed, WEBHOOK_ALERTS, COLOR_RED, COLOR_GREEN, COLOR_WARN

        direction = "long" if impact_score > 0 else "short"
        weighted_score = round(impact_score * authority * 1.5, 4)  # urgency=3 boost
        title = article.get("title", "")
        source = article.get("source") or article.get("source_domain") or "unknown"
        emitted_at = datetime.now(timezone.utc)

        signal_data = {
            "title": title[:300],
            "summary": summary[:500],
            "source": source,
            "impact_score": impact_score,
            "urgency": urgency,
            "source_authority": authority,
            "breaking_news": True,
            "news_id": article.get("id"),
        }

        registry = SignalRegistryEngine(self._pool)
        affected: list[str] = [t for t in tickers if t in self._LIVE_TICKERS]

        # If no specific live tickers, use BTC as market proxy for strong macro news
        if not affected and abs(impact_score) >= 0.6:
            affected = ["BTC"]

        for ticker in affected:
            try:
                sig_id = await registry.register(
                    signal_type="breaking_news",
                    engine_name="NewsBreakingEngine",
                    ticker=ticker,
                    direction=direction,
                    composite_score=weighted_score,
                    confidence_level="high",
                    signal_data={**signal_data, "ticker": ticker},
                    source_table="news",
                    source_id=article.get("id"),
                    emitted_at=emitted_at,
                )
                logger.info(
                    "breaking_news_signal_emitted",
                    signal_id=sig_id,
                    ticker=ticker,
                    direction=direction,
                    impact=impact_score,
                )
            except Exception as exc:
                logger.error("breaking_news_signal_error", ticker=ticker, error=str(exc))

        # Discord #alerts notification
        try:
            color = COLOR_RED if impact_score < 0 else COLOR_GREEN
            impact_pct = f"{impact_score:+.2f}"
            tickers_str = ", ".join(affected) if affected else "market"
            await send_embed(
                WEBHOOK_ALERTS,
                f"🚨 Breaking News — {tickers_str}",
                description=f"**{title[:200]}**\n\n{summary[:400]}",
                color=color,
                fields=[
                    {"name": "Source", "value": source, "inline": True},
                    {"name": "Impact", "value": impact_pct, "inline": True},
                    {"name": "Authority", "value": f"{authority:.1f}×", "inline": True},
                    {"name": "Direction", "value": direction.upper(), "inline": True},
                ],
            )
        except Exception as exc:
            logger.warning("breaking_news_discord_error", error=str(exc))

    def get_circuit_statuses(self) -> list[dict]:
        return get_all_circuit_statuses()
