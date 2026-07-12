"""Funding rate proxy for smart-money positioning.

Extreme Binance futures funding rates reveal crowded positioning:
  Extreme negative (shorts paying longs) → crowded short → contrarian BUY
  Extreme positive (longs paying shorts) → crowded long  → contrarian SELL

Standard Binance rate = ±0.01% (0.0001) per 8h.
Threshold: |avg_24h_rate| > 0.0003 (3× standard) before emitting an event.
"""
from __future__ import annotations

from datetime import datetime, timezone

import asyncpg

from src.core.logging import get_logger
from src.models.market import SmartMoneyEvent
from src.providers.base import BaseProvider

logger = get_logger(__name__)

_THRESHOLD = 0.0003   # 3× standard rate → meaningful crowding
_STRONG    = 0.0007   # 7× → strong signal (higher amount_usd proxy)
_MIN_READINGS = 4     # require ≥4 data points in the 24h window

# Only generate signals for tickers we actually trade
_SYMBOL_MAP: dict[str, str] = {
    "BTCUSDT":  "BTC",
    "ETHUSDT":  "ETH",
    "SOLUSDT":  "SOL",
    "BNBUSDT":  "BNB",
    "AVAXUSDT": "AVAX",
    "AAVEUSDT": "AAVE",
    "XRPUSDT":  "XRP",
    "DOGEUSDT": "DOGE",
    "OPUSDT":   "OP",
    "ADAUSDT":  "ADA",
    "INJUSDT":  "INJ",
    "APTUSDT":  "APT",
    "SUIUSDT":  "SUI",
    "NEARUSDT": "NEAR",
    "FILUSDT":  "FIL",
    "ATOMUSDT": "ATOM",
    "UNIUSDT":  "UNI",
    "TONUSDT":  "TON",
    "LTCUSDT":  "LTC",
    "TRXUSDT":  "TRX",
}


class FundingFlowProvider(BaseProvider):
    """Derives smart-money flow signals from Binance perpetual funding rates."""

    provider_name = "funding_flow"

    def __init__(self, db: asyncpg.Pool | None = None, **kwargs) -> None:
        self._db = db

    async def fetch(self) -> list[SmartMoneyEvent]:
        if not self._db:
            logger.warning("funding_flow_no_db")
            return []

        async with self._db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol,
                       AVG(funding_rate) AS avg_fr,
                       AVG(mark_price)   AS avg_price,
                       COUNT(*)          AS n
                  FROM funding_rates
                 WHERE ts > NOW() - INTERVAL '24 hours'
                   AND funding_rate IS NOT NULL
                 GROUP BY symbol
                HAVING COUNT(*) >= $1
                """,
                _MIN_READINGS,
            )

        events: list[SmartMoneyEvent] = []
        now = datetime.now(timezone.utc)

        for row in rows:
            symbol = row["symbol"]
            ticker = _SYMBOL_MAP.get(symbol)
            if not ticker:
                if symbol.endswith("USDT"):
                    ticker = symbol[:-4]
                else:
                    continue

            avg_fr = float(row["avg_fr"])
            if abs(avg_fr) < _THRESHOLD:
                continue

            # Contrarian interpretation: crowded short → buy, crowded long → sell
            direction = "buy" if avg_fr < 0 else "sell"
            strength_mult = abs(avg_fr) / 0.0001
            amount_proxy = strength_mult * 50_000

            events.append(SmartMoneyEvent(
                id=f"funding_{symbol}_{now.strftime('%Y%m%d%H')}",
                event_type="funding_flow",
                ticker=ticker,
                actor=f"binance_funding_{symbol}",
                amount_usd=amount_proxy,
                direction=direction,
                source="binance_funding",
                event_at=now,
                raw={
                    "symbol": symbol,
                    "avg_fr_24h": round(avg_fr, 8),
                    "mark_price": float(row["avg_price"] or 0),
                    "strength_mult": round(strength_mult, 1),
                },
            ))

        logger.info("funding_flow_fetched", count=len(events))
        return events

    async def normalize(self, raw: dict) -> SmartMoneyEvent | None:
        return None  # fetch() builds events directly; normalize not used

    async def health_check(self) -> bool:
        return self._db is not None

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0

    async def rate_limit_handler(self) -> None:
        pass
