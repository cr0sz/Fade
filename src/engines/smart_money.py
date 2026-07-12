from __future__ import annotations

import math
from dataclasses import dataclass, field

import asyncpg

from src.core.logging import get_logger
from src.db.repositories import SmartMoneyRepository
from src.providers.smart_money import SMART_MONEY_PROVIDERS

logger = get_logger(__name__)


@dataclass
class SmartMoneyCycleResult:
    fetched: int = 0
    stored: int = 0
    errors: int = 0
    whale_events: int = 0
    insider_events: int = 0
    tickers_scored: int = 0


class SmartMoneyEngine:
    def __init__(self, db_pool: asyncpg.Pool, redis=None) -> None:
        self._pool = pool = db_pool
        self._repo = SmartMoneyRepository(pool)
        self._redis = redis

    async def run_cycle(self) -> SmartMoneyCycleResult:
        result = SmartMoneyCycleResult()
        all_events = []

        for name, provider_cls in SMART_MONEY_PROVIDERS.items():
            try:
                import inspect
                sig = inspect.signature(provider_cls.__init__)
                kwargs: dict = {}
                if "redis" in sig.parameters:
                    kwargs["redis"] = self._redis
                if "db" in sig.parameters:
                    kwargs["db"] = self._pool
                provider = provider_cls(**kwargs) if kwargs else provider_cls()
            except Exception:
                provider = provider_cls()

            try:
                events = await provider.fetch()
                result.fetched += len(events)
                all_events.extend(events)

                for event in events:
                    try:
                        await self._repo.insert(event)
                        result.stored += 1
                        if event.event_type == "whale_tx":
                            result.whale_events += 1
                        elif event.event_type in ("insider_trade", "options_flow"):
                            result.insider_events += 1
                    except Exception as exc:
                        logger.error("smart_money_insert_error", error=str(exc))

            except Exception as exc:
                logger.error("smart_money_provider_error", provider=name, error=str(exc))
                result.errors += 1

        # Aggregate per-ticker smart_money_score and write back to market_trends
        scored = await self._update_market_trend_scores(all_events)
        result.tickers_scored = scored

        logger.info(
            "smart_money_cycle_complete",
            fetched=result.fetched,
            whales=result.whale_events,
            insiders=result.insider_events,
            tickers_scored=result.tickers_scored,
        )
        return result

    async def _update_market_trend_scores(self, events) -> int:
        """Aggregate events into per-ticker smart_money_score and persist to market_trends.

        Scoring logic:
          buy  events → positive contribution (strength based on amount_usd or count)
          sell events → negative contribution (weaker weight — routine selling is noise)

        Final score is tanh-normalised to [-1, +1].
        Only tickers with existing market_trends rows are updated.
        """
        if not events:
            return 0

        # Accumulate raw net flow per ticker
        ticker_flow: dict[str, float] = {}
        for event in events:
            ticker = event.ticker
            if not ticker:
                continue
            # Whale alerts / options use dollar amount; insider trades may not
            amount = float(event.amount_usd or 0)
            weight = 1.0 if amount == 0 else math.log1p(amount / 1000)
            if event.direction == "buy":
                ticker_flow[ticker] = ticker_flow.get(ticker, 0.0) + weight
            elif event.direction == "sell":
                ticker_flow[ticker] = ticker_flow.get(ticker, 0.0) - weight * 0.5

        if not ticker_flow:
            return 0

        # Normalise and write to market_trends
        updated = 0
        async with self._pool.acquire() as conn:
            for ticker, raw_flow in ticker_flow.items():
                score = round(math.tanh(raw_flow / 5.0), 4)   # /5 → reasonable scale
                try:
                    result = await conn.execute(
                        "UPDATE market_trends SET smart_money_score = $1 WHERE ticker = $2",
                        score, ticker,
                    )
                    if result != "UPDATE 0":
                        updated += 1
                except Exception as exc:
                    logger.debug("smart_money_score_update_error", ticker=ticker, error=str(exc))

        return updated

    async def get_recent_signals(self, hours: int = 24) -> dict:
        events = await self._repo.get_recent(hours=hours)
        whale_events   = [e for e in events if e.get("event_type") == "whale_tx"]
        insider_events = [e for e in events if e.get("event_type") in ("insider_trade", "options_flow")]

        return {
            "total_events":       len(events),
            "whale_transactions": len(whale_events),
            "insider_trades":     len(insider_events),
            "recent_whales":      whale_events[:5],
            "recent_insiders":    insider_events[:5],
        }
