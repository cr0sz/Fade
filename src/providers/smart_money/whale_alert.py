from __future__ import annotations

import asyncio
from datetime import datetime

import aiohttp

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.market import SmartMoneyEvent
from src.providers.base import BaseProvider

logger = get_logger(__name__)


class WhaleAlertProvider(BaseProvider):
    provider_name = "whale_alert"

    def __init__(self) -> None:
        self._settings = get_settings()
        self._api_key = self._settings.WHALE_ALERT_API_KEY
        self._base = self._settings.WHALE_ALERT_BASE_URL

    async def fetch(self, min_value_usd: int = 500_000, limit: int = 100) -> list[SmartMoneyEvent]:
        if not self._api_key or self._api_key == "your_whale_alert_key":
            logger.warning("whale_alert_key_missing")
            return []

        events: list[SmartMoneyEvent] = []
        try:
            async with aiohttp.ClientSession() as session:
                await self.rate_limit_handler()
                raw_txs = await self._fetch_transactions(session, min_value_usd, limit)
                for tx in raw_txs:
                    event = await self.normalize(tx)
                    if event:
                        events.append(event)
        except Exception as exc:
            logger.error("whale_alert_error", error=str(exc))

        logger.info("whale_alert_fetched", count=len(events))
        return events

    async def _fetch_transactions(
        self,
        session: aiohttp.ClientSession,
        min_value: int,
        limit: int,
    ) -> list[dict]:
        import time
        params = {
            "api_key": self._api_key,
            "min_value": min_value,
            "limit": min(limit, 100),
            "start": int(time.time()) - 3600,  # last hour
        }
        async with session.get(f"{self._base}/transactions", params=params) as resp:
            if resp.status == 429:
                await asyncio.sleep(60)
                return []
            resp.raise_for_status()
            data = await resp.json()
            return data.get("transactions", [])

    async def normalize(self, raw: dict) -> SmartMoneyEvent | None:
        amount = raw.get("amount_usd", 0)
        if not amount:
            return None

        from_ = raw.get("from", {})
        to_ = raw.get("to", {})
        actor = from_.get("owner") or from_.get("owner_type") or "unknown"
        direction = "out" if from_.get("owner") else "in"

        return SmartMoneyEvent(
            id=str(raw.get("id", "")),
            event_type="whale_tx",
            ticker=None,  # crypto transactions; map to tickers if needed
            actor=actor,
            amount_usd=float(amount),
            direction=direction,
            source="whale_alert",
            event_at=datetime.utcfromtimestamp(raw.get("timestamp", 0)),
            raw={"blockchain": raw.get("blockchain"), "symbol": raw.get("symbol")},
        )

    async def health_check(self) -> bool:
        if not self._api_key:
            return False
        try:
            async with aiohttp.ClientSession() as session:
                params = {"api_key": self._api_key, "limit": 1}
                async with session.get(f"{self._base}/status", params=params) as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0

    async def rate_limit_handler(self) -> None:
        await asyncio.sleep(6.0)  # 10 calls/min
