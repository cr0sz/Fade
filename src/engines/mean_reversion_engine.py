"""Mean Reversion Engine — cross-sectional 24h reversal signals on crypto.

Sprint 39c. The system's only OOS-validated edge is fading extreme 24h moves
(Truth Router fades: +1.2%/trade, ADA bounce fades 82% WR). That flow currently
depends entirely on TrendDetectionEngine emitting risk_off bursts — a single
point of failure. This engine produces the same trade candidates directly from
price data: no news, no social, no LLM, no API keys.

Logic, deliberately primitive:
  1. Universe: CRYPTO tickers with a fresh quote (≤30 min).
  2. 24h return per ticker from ticker_quotes.
  3. Cross-sectional z-score of returns (vs the universe's mean/std today —
     so a broad market dump doesn't flag every ticker, only the outliers).
  4. |z| ≥ Z_MIN and |ret| ≥ ABS_MIN  →  signal:
       extreme DOWN move → risk_on  (bounce long)
       extreme UP move   → risk_off (fade the pump short)
  5. 4h cooldown per (ticker, direction) to avoid burst spam.

Signals go straight into signals_registry, so the outcome tracker measures
them against real price truth from day one, and the Truth Router will route
them on measured lift once n ≥ 30. Until then they are 'passthrough' and face
the full legacy gate stack — deliberately conservative for a new engine.

RESOLVED (Sprint 39d): the question of whether CONFIRM_LONG_EDGE=0.15 should
apply to this engine's dump-buying longs was answered by measurement instead
of waiting 2 weeks. scripts/meanrev_replay.py replayed this exact rule over
7 months of quotes: bounce longs lose -0.67%/trade (43% WR), and fade shorts'
apparent profit is market beta, not cross-sectional alpha (p_up ≈ CRYPTO base
rate). scripts/meanrev_seed_router.py seeded the router's 30-day window with
backdated signals (source_table='meanrev_replay', purgeable), so the router
rejects both buckets from day one instead of letting them trade passthrough.
The fade edge lives in TrendDetectionEngine's panic-burst selection (inverted
emit_short, lift +0.10), not in raw price outliers — this engine stays as a
cheap, honest, continuously-measured candidate the router will start trading
only if its lift ever materializes.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone

import asyncpg

from src.core.logging import get_logger

logger = get_logger(__name__)

Z_MIN = 1.25         # cross-sectional outlier threshold
ABS_MIN = 0.04       # 4% absolute 24h move floor — z alone can flag tiny moves in calm tape
COOLDOWN_H = 4       # per (ticker, direction) re-emission cooldown
MIN_UNIVERSE = 6     # need enough tickers for a meaningful cross-section
ENGINE_NAME = "MeanReversionEngine"

_UNIVERSE_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (q.symbol) q.symbol, q.price, q.ts
      FROM ticker_quotes q
      JOIN tickers t ON t.symbol = q.symbol
     WHERE UPPER(t.exchange) = 'CRYPTO' AND q.price > 0
       AND q.ts > NOW() - INTERVAL '30 minutes'
     ORDER BY q.symbol, q.ts DESC
), past AS (
    SELECT DISTINCT ON (q.symbol) q.symbol, q.price AS past_price
      FROM ticker_quotes q
     WHERE q.price > 0
       AND q.ts BETWEEN NOW() - INTERVAL '26 hours' AND NOW() - INTERVAL '22 hours'
     ORDER BY q.symbol,
              ABS(EXTRACT(EPOCH FROM (q.ts - (NOW() - INTERVAL '24 hours')))) ASC
)
SELECT l.symbol, l.price, p.past_price
  FROM latest l JOIN past p USING (symbol)
 WHERE p.past_price > 0
"""

_COOLDOWN_SQL = """
SELECT ticker, direction FROM signals_registry
 WHERE engine_name = $1 AND emitted_at > NOW() - ($2 || ' hours')::interval
"""

_INSERT_SQL = """
INSERT INTO signals_registry (
    signal_type, engine_name, ticker, direction,
    composite_score, confidence_level, signal_data,
    source_table, source_id, emitted_at
) VALUES ('mean_reversion', $1, $2, $3, $4, $5, $6, 'mean_reversion_engine', NULL, $7)
ON CONFLICT DO NOTHING
"""


class MeanReversionEngine:
    def __init__(self, db: asyncpg.Pool) -> None:
        self._db = db

    async def run_cycle(self) -> int:
        """Compute cross-sectional signals and persist them. Returns emit count."""
        async with self._db.acquire() as conn:
            rows = await conn.fetch(_UNIVERSE_SQL)
            if len(rows) < MIN_UNIVERSE:
                logger.debug("meanrev_universe_too_small", n=len(rows))
                return 0

            rets = {r["symbol"]: (float(r["price"]) - float(r["past_price"]))
                                 / float(r["past_price"]) for r in rows}
            mean = sum(rets.values()) / len(rets)
            var = sum((x - mean) ** 2 for x in rets.values()) / len(rets)
            std = math.sqrt(var)
            if std <= 1e-6:
                return 0

            recent = await conn.fetch(_COOLDOWN_SQL, ENGINE_NAME, str(COOLDOWN_H))
            on_cooldown = {(r["ticker"], r["direction"]) for r in recent}

            now = datetime.now(timezone.utc)
            emitted = 0
            for sym, ret in rets.items():
                z = (ret - mean) / std
                if abs(z) < Z_MIN or abs(ret) < ABS_MIN:
                    continue
                # Extreme dump → expect bounce (risk_on); extreme pump → fade (risk_off)
                direction = "risk_on" if ret < 0 else "risk_off"
                if (sym, direction) in on_cooldown:
                    continue
                # Score sign matches direction; magnitude scales with the outlier size.
                score = max(-0.9, min(0.9, math.copysign(abs(z) * 0.15, 1 if direction == "risk_on" else -1)))
                confidence = min(0.85, 0.50 + abs(z) * 0.10)
                signal_data = json.dumps({
                    "confidence_score": round(confidence, 4),
                    "ret_24h": round(ret, 5),
                    "zscore": round(z, 3),
                    "universe_n": len(rets),
                    "universe_mean_ret": round(mean, 5),
                })
                await conn.execute(
                    _INSERT_SQL, ENGINE_NAME, sym, direction,
                    round(score, 4), "medium", signal_data, now,
                )
                emitted += 1
                logger.info("meanrev_signal", ticker=sym, direction=direction,
                            ret_24h=round(ret * 100, 2), z=round(z, 2),
                            score=round(score, 3))

            if emitted == 0:
                logger.debug("meanrev_no_outliers", universe=len(rets),
                             mean=round(mean * 100, 2), std=round(std * 100, 2))
            return emitted
