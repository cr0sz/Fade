"""
Technical Indicators Engine — price-action signals from ticker_quotes + yfinance.

Components per ticker:
  1. RSI(14)          — oversold (<30) → long signal, overbought (>70) → short signal
  2. Price momentum   — multi-period return consistency (1h/4h/12h for crypto, 1d/5d/20d for stocks)
  3. Bollinger Band   — contrarian: extended ±2σ from 20-period mean
  4. Volume anomaly   — current vs rolling-average volume (trend confirmation)
  5. Funding rate     — Binance perpetual funding rate for CRYPTO only
                        (positive funding = longs overextended = mild short signal)

Output: technical_score in [-1.0, +1.0]
  > +0.3  → price action bullish (supports long)
  < -0.3  → price action bearish (supports short)
  ~  0.0  → no clear signal

Data sources:
  CRYPTO : ticker_quotes (CoinGecko 5-min polls → rich intraday data)
  Stocks : yfinance download("3mo", "1d") cached in Redis for 6h
  BIST   : same as stocks via yfinance (.IS suffix)

Binance funding rates are fetched per crypto ticker and cached 1h in Redis.
"""
from __future__ import annotations

import asyncio
import math
from typing import TYPE_CHECKING

import asyncpg

from src.core.logging import get_logger

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

_SCORE_CACHE_KEY  = "tech:score:"
_FUND_CACHE_KEY   = "tech:fund:"
_SCORE_CACHE_TTL  = 82800   # 23 h — stock daily data changes once/day; long TTL avoids thundering herd
_FUND_CACHE_TTL   = 3600    # 1 h — funding rates update every 8 h


# ── Pure-function indicator library ──────────────────────────────────────────

def _rsi(prices: list[float], period: int = 14) -> float:
    """Wilder RSI in [0, 100]. Requires ≥ period+1 points."""
    if len(prices) <= period:
        return 50.0  # insufficient data — return neutral rather than computing on sub-period window
    n = period
    if n < 2:
        return 50.0
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains  = [max(0.0, d) for d in deltas[-n:]]
    losses = [abs(min(0.0, d)) for d in deltas[-n:]]
    avg_gain = sum(gains)  / n
    avg_loss = sum(losses) / n
    if avg_loss == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


def _rsi_signal(rsi_val: float) -> float:
    """Map RSI [0,100] → [-1, +1]. Oversold → positive, overbought → negative."""
    if rsi_val <= 30:
        return min(1.0, (30 - rsi_val) / 30)
    if rsi_val >= 70:
        return max(-1.0, -(rsi_val - 70) / 30)
    return (50 - rsi_val) / 50 * 0.25  # mild lean in mid-range


def _momentum(prices: list[float], lookbacks: list[int]) -> float:
    """Average tanh-normalised return over multiple lookback windows.

    tanh(ret/0.01) → 1% move ≈ 0.76, 2% ≈ 0.96.  Prevents single outliers dominating.
    """
    if len(prices) < 2:
        return 0.0
    current = prices[-1]
    sigs = []
    for lb in lookbacks:
        if len(prices) > lb:
            past = prices[-(lb + 1)]
            if past > 0:
                sigs.append(math.tanh((current - past) / past / 0.01))
    return round(sum(sigs) / len(sigs), 4) if sigs else 0.0


def _bollinger(prices: list[float], period: int = 20) -> float:
    """Contrarian BB signal: below lower band → +1, above upper → -1."""
    if len(prices) < period:
        return 0.0
    window = prices[-period:]
    mean = sum(window) / period
    std  = (sum((p - mean) ** 2 for p in window) / period) ** 0.5
    if std == 0:
        return 0.0
    z = (prices[-1] - mean) / std   # >0 = above mean, <0 = below
    return round(-max(-1.0, min(1.0, z / 2.0)), 4)  # contrarian flip, capped ±1


def _vol_signal(volumes: list[float]) -> float:
    """Volume anomaly: tanh(log(current / rolling_avg))."""
    if len(volumes) < 3:
        return 0.0
    recent = volumes[-1]
    avg    = sum(volumes[:-1]) / (len(volumes) - 1)
    if avg <= 0:
        return 0.0
    return round(math.tanh(math.log(max(recent / avg, 0.01))), 4)


def _combine(rsi_s: float, mom_s: float, bb_s: float, vol_s: float,
             fund_s: float = 0.0, is_crypto: bool = False) -> float:
    if is_crypto:
        raw = 0.30 * rsi_s + 0.30 * mom_s + 0.15 * bb_s + 0.10 * vol_s + 0.15 * fund_s
    else:
        raw = 0.35 * rsi_s + 0.40 * mom_s + 0.15 * bb_s + 0.10 * vol_s
    return round(max(-1.0, min(1.0, raw)), 4)


# ── Engine ────────────────────────────────────────────────────────────────────

class TechnicalEngine:
    """Computes technical_score for a ticker from available price history."""

    def __init__(self, pool: asyncpg.Pool, redis: "aioredis.Redis | None" = None) -> None:
        self._pool  = pool
        self._redis = redis

    async def compute(self, ticker: str, exchange: str) -> float:
        """Return technical_score in [-1.0, +1.0]. Returns 0.0 on error or insufficient data."""
        try:
            is_crypto = exchange == "CRYPTO"

            if is_crypto:
                return await self._from_quotes(ticker, is_crypto=True)

            # Stocks: try ticker_quotes first (populated by yfinance market_data_cycle)
            score = await self._from_quotes(ticker, is_crypto=False)
            if score != 0.0:
                return score
            # Fall back to direct yfinance fetch (cached 6 h)
            return await self._from_yfinance_cached(ticker)

        except Exception as exc:
            logger.debug("technical_compute_error", ticker=ticker, error=str(exc))
            return 0.0

    async def compute_batch(self, tickers: list[str],
                            exchange_map: dict[str, str]) -> dict[str, float]:
        """Compute scores for all tickers concurrently."""
        coros = [self.compute(t, exchange_map.get(t, "")) for t in tickers]
        scores = await asyncio.gather(*coros, return_exceptions=True)
        return {
            t: (s if isinstance(s, float) else 0.0)
            for t, s in zip(tickers, scores)
        }

    # ── Data paths ────────────────────────────────────────────────────────────

    async def _from_quotes(self, ticker: str, is_crypto: bool) -> float:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT price, volume FROM ticker_quotes "
                "WHERE symbol = $1 AND price > 0 ORDER BY ts DESC LIMIT 300",
                ticker,
            )
        if len(rows) < 5:
            return 0.0

        prices_desc  = [float(r["price"])  for r in rows]
        volumes_desc = [float(r["volume"]) for r in rows if float(r["volume"]) > 0]

        prices  = list(reversed(prices_desc))
        volumes = list(reversed(volumes_desc)) if volumes_desc else []

        rsi_s = _rsi_signal(_rsi(prices))
        bb_s  = _bollinger(prices)
        vol_s = _vol_signal(volumes) if len(volumes) >= 3 else 0.0

        if is_crypto:
            # 5-min CoinGecko ticks: 12=1h, 48=4h, 144=12h
            mom_s    = _momentum(prices, [12, 48, 144])
            fund_s   = await self._funding_rate(ticker)
            return _combine(rsi_s, mom_s, bb_s, vol_s, fund_s, is_crypto=True)
        else:
            # Daily stock ticks: 1=1d, 3=3d, 5=5d
            mom_s = _momentum(prices, [1, 3, 5])
            return _combine(rsi_s, mom_s, bb_s, vol_s)

    async def _from_yfinance_cached(self, ticker: str) -> float:
        key = f"{_SCORE_CACHE_KEY}{ticker}"
        if self._redis:
            try:
                cached = await self._redis.get(key)
                if cached:
                    return float(cached)
            except Exception:
                pass

        # Small stagger on cache miss to avoid thundering-herd rate limits on cold start
        import random
        await asyncio.sleep(random.uniform(0.2, 1.5))
        score = await asyncio.get_running_loop().run_in_executor(
            None, _yfinance_score_sync, ticker
        )

        if self._redis:
            try:
                await self._redis.setex(key, _SCORE_CACHE_TTL, str(score))
            except Exception:
                pass
        return score

    async def _funding_rate(self, ticker: str) -> float:
        """Binance perpetual funding rate → contrarian signal.

        Positive funding (longs pay shorts) = market overextended long → mild short bias.
        Returns funding_signal in [-1, +1].
        """
        key = f"{_FUND_CACHE_KEY}{ticker}"
        if self._redis:
            try:
                cached = await self._redis.get(key)
                if cached:
                    rate = float(cached)
                    return -math.tanh(rate * 1000)  # 0.1% funding → ≈ -0.10 signal
            except Exception:
                pass

        rate = await asyncio.get_running_loop().run_in_executor(
            None, _fetch_funding_sync, ticker
        )

        if self._redis and rate != 0.0:
            try:
                await self._redis.setex(key, _FUND_CACHE_TTL, str(rate))
            except Exception:
                pass

        return -math.tanh(rate * 1000) if rate != 0.0 else 0.0


# ── Thread-executor helpers (blocking I/O) ────────────────────────────────────

def _yfinance_score_sync(ticker: str) -> float:
    """Blocking yfinance call for non-crypto daily technical score."""
    try:
        import yfinance as yf
        yf_sym = "^VIX" if ticker == "VIX" else ticker
        df = yf.download(
            tickers=yf_sym, period="3mo", interval="1d",
            auto_adjust=True, progress=False, threads=False,
        )
        if df is None or df.empty:
            return 0.0

        if hasattr(df.columns, "get_level_values"):
            # Multi-ticker download returns MultiIndex — flatten
            try:
                df = df[yf_sym] if yf_sym in df.columns.get_level_values(0) else df
            except Exception:
                pass

        closes  = df["Close"].dropna().tolist()
        volumes = df["Volume"].dropna().tolist()
        if len(closes) < 10:
            return 0.0

        rsi_s = _rsi_signal(_rsi(closes))
        mom_s = _momentum(closes, [1, 5, 20])
        bb_s  = _bollinger(closes)
        vol_s = _vol_signal(volumes) if len(volumes) >= 3 else 0.0
        return _combine(rsi_s, mom_s, bb_s, vol_s)
    except Exception as exc:
        logger.debug("yfinance_tech_sync_error", ticker=ticker, error=str(exc))
        return 0.0


def _fetch_funding_sync(ticker: str) -> float:
    """Blocking Binance API call for latest perpetual funding rate."""
    try:
        import urllib.request
        import json as _json
        sym = f"{ticker}USDT"
        url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={sym}&limit=1"
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = _json.loads(resp.read())
            if data:
                return float(data[0]["fundingRate"])
    except Exception:
        pass
    return 0.0
