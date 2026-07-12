"""yfinance market data provider — NASDAQ, NYSE, and BIST prices.

Uses the yfinance library (no API key required, wraps Yahoo Finance).
Fetches real-time quotes for all non-CRYPTO tickers in the DB.

Exchange mapping:
  NASDAQ / NYSE  — standard symbols (INTC, NVDA, TSLA, ...)
  BIST           — Yahoo Finance format (THYAO.IS, GARAN.IS, ...)
  CBOE           — VIX (^VIX on Yahoo Finance)

Rate limit: yfinance uses Yahoo Finance internally; batch via download()
to minimise requests. Stays well within free limits.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from src.core.config import get_settings
from src.core.logging import get_logger
from src.models.market import TickerQuote
from src.providers.base import BaseProvider

logger = get_logger(__name__)

# Yahoo Finance symbol overrides (our internal symbol → YF symbol)
_SYMBOL_MAP: dict[str, str] = {
    "VIX": "^VIX",
    "SPY": "SPY",
    "QQQ": "QQQ",
    "GLD": "GLD",
    "IWM": "IWM",
    "TLT": "TLT",
}

# Exchange labels returned by yfinance → our internal exchange label
_EXCHANGE_MAP: dict[str, str] = {
    "NMS": "NASDAQ", "NGM": "NASDAQ", "NCM": "NASDAQ",
    "NYQ": "NYSE", "PCX": "NYSE",
    "IST": "BIST",
    "CBF": "CBOE", "CBOE": "CBOE",
}


class YFinanceProvider(BaseProvider):
    provider_name = "yfinance"

    def __init__(self) -> None:
        self._settings = get_settings()

    async def fetch(self, symbols: list[str] | None = None, **kwargs) -> list[TickerQuote]:
        """Fetch quotes for all non-CRYPTO tickers (or explicit symbols list)."""
        target_symbols = symbols or await self._load_symbols()
        if not target_symbols:
            return []

        yf_symbols = [_SYMBOL_MAP.get(s, s) for s in target_symbols]
        symbol_reverse = {_SYMBOL_MAP.get(s, s): s for s in target_symbols}

        quotes: dict = {}
        for attempt in range(3):
            try:
                quotes = await asyncio.get_event_loop().run_in_executor(
                    None, self._fetch_sync, yf_symbols
                )
                if quotes:
                    break
                # Empty result on rate-limit: wait and retry
                if attempt < 2:
                    await asyncio.sleep(15 * (attempt + 1))
            except Exception as exc:
                logger.error("yfinance_fetch_error", attempt=attempt, error=str(exc))
                if attempt < 2:
                    await asyncio.sleep(15 * (attempt + 1))
        if not quotes:
            logger.warning("yfinance_fetch_exhausted", symbols_count=len(yf_symbols))
            return []

        results: list[TickerQuote] = []
        for yf_sym, data in quotes.items():
            our_sym = symbol_reverse.get(yf_sym, yf_sym)
            quote = self._to_ticker_quote(our_sym, yf_sym, data)
            if quote:
                results.append(quote)

        logger.info("yfinance_fetched", count=len(results))
        return results

    @staticmethod
    def _us_market_open() -> bool:
        """True if UTC clock is inside regular US market hours Mon–Fri."""
        now = datetime.now(timezone.utc)
        if now.weekday() >= 5:
            return False
        t = (now.hour, now.minute)
        return (13, 30) <= t < (20, 0)

    def _fetch_sync(self, symbols: list[str]) -> dict[str, dict]:
        """Blocking yfinance call — run in thread executor.

        During US market hours uses 5m intraday to get live prices.
        Outside market hours falls back to daily candles (previous close).
        Returns {symbol: {close, prev_close, volume}} dict.
        """
        import time
        import yfinance as yf
        import pandas as pd

        use_intraday = self._us_market_open()
        if use_intraday:
            for _attempt in range(3):
                try:
                    data = yf.download(
                        tickers=symbols,
                        period="1d",
                        interval="5m",
                        group_by="ticker",
                        auto_adjust=True,
                        progress=False,
                        threads=True,
                    )
                    break
                except Exception:
                    if _attempt < 2:
                        time.sleep(10)
                    else:
                        data = pd.DataFrame()
            # Fall back to daily if intraday returns nothing (early pre-market edge case)
            if data.empty:
                use_intraday = False

        if not use_intraday:
            for _attempt in range(3):
                try:
                    data = yf.download(
                        tickers=symbols,
                        period="5d",
                        interval="1d",
                        group_by="ticker",
                        auto_adjust=True,
                        progress=False,
                        threads=True,
                    )
                    break
                except Exception:
                    if _attempt < 2:
                        time.sleep(10)
                    else:
                        return {}

        result: dict[str, dict] = {}
        if data.empty:
            return result

        for sym in symbols:
            try:
                if len(symbols) == 1:
                    df = data
                else:
                    df = data[sym] if sym in data.columns.get_level_values(0) else None
                if df is None or df.empty:
                    continue

                closes = df["Close"].dropna()
                volumes = df["Volume"].dropna()
                if len(closes) == 0:
                    continue

                price = float(closes.iloc[-1])
                prev_close = float(closes.iloc[-2]) if len(closes) >= 2 else price
                # Use last non-zero volume — intraday candles may show 0 mid-bar
                nonzero_vols = volumes[volumes > 0]
                volume = float(nonzero_vols.iloc[-1]) if len(nonzero_vols) > 0 else 0.0
                result[sym] = {"price": price, "prev_close": prev_close, "volume": volume}
            except Exception as exc:
                logger.debug("yfinance_parse_error", symbol=sym, error=str(exc))

        return result

    def _to_ticker_quote(self, our_sym: str, yf_sym: str, data: dict) -> TickerQuote | None:
        try:
            price = float(data.get("price") or 0.0)
            if not price:
                return None

            prev_close = float(data.get("prev_close") or price)
            change = price - prev_close
            change_pct = (change / prev_close * 100) if prev_close else 0.0
            volume = float(data.get("volume") or 0.0)

            if our_sym.endswith(".IS"):
                exchange = "BIST"
                currency = "TRY"
            elif our_sym == "VIX":
                exchange = "CBOE"
                currency = "USD"
            else:
                exchange = "NASDAQ"
                currency = "USD"

            return TickerQuote(
                symbol=our_sym,
                price=round(price, 6),
                change=round(change, 6),
                change_pct=round(change_pct, 4),
                volume=round(volume, 2),
                exchange=exchange,
                currency=currency,
                timestamp=datetime.now(timezone.utc),
            )
        except Exception as exc:
            logger.debug("yfinance_normalize_error", symbol=our_sym, error=str(exc))
            return None

    async def _load_symbols(self) -> list[str]:
        """Return all non-CRYPTO active tickers from DB."""
        from src.db.connection import get_db_pool
        try:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT symbol FROM tickers
                     WHERE is_active = true
                       AND exchange != 'CRYPTO'
                    """
                )
            return [r["symbol"] for r in rows]
        except Exception as exc:
            logger.error("yfinance_symbol_load_failed", error=str(exc))
            return []

    async def health_check(self) -> bool:
        try:
            import yfinance as yf  # noqa: F401
            return True
        except ImportError:
            return False

    async def cost_estimate(self, request_count: int) -> float:
        return 0.0

    async def normalize(self, raw: dict) -> TickerQuote | None:
        return None

    async def rate_limit_handler(self) -> None:
        pass  # yfinance handles its own rate limiting internally
