"""
Signal Tracer — shared write helper for the signal_trace table.

Usage in any engine:
    from src.core.signal_tracer import SignalTracer
    tracer = SignalTracer(db_pool)
    await tracer.record(
        cycle_id="...",
        engine_name="trend_detection",
        signal_type="trend",
        signal_strength=72.5,
        signal_direction="risk_on",
        signal_id="1234",
        latency_ms=42.1,
    )

Design notes:
- Fire-and-forget: never raises, logs warnings only.
- Idempotent: ON CONFLICT DO NOTHING on (cycle_id, engine_name, signal_id).
  A retry of the same cycle will not produce duplicate rows.
- All fields except cycle_id/engine_name/signal_type are optional.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import asyncpg

from src.core.logging import get_logger

logger = get_logger(__name__)

_INSERT = """
INSERT INTO signal_trace (
    cycle_id, news_id, provider, engine_name,
    signal_id, signal_type, signal_strength, signal_direction,
    ticker, input_data_hash, output_data_hash, latency_ms, created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
ON CONFLICT DO NOTHING
"""


def _sha256(obj: Any) -> str | None:
    if obj is None:
        return None
    try:
        return hashlib.sha256(
            json.dumps(obj, sort_keys=True, default=str).encode()
        ).hexdigest()
    except Exception:
        return None


class SignalTracer:
    def __init__(self, db_pool: asyncpg.Pool) -> None:
        self._pool = db_pool

    async def record(
        self,
        *,
        cycle_id: str,
        engine_name: str,
	ticker: str | None = None,
        signal_type: str,
        signal_id: str | None = None,
        signal_strength: float | None = None,
        signal_direction: str | None = None,
        news_id: str | None = None,
        provider: str | None = None,
        input_data: Any = None,
        output_data: Any = None,
        latency_ms: float | None = None,
    ) -> None:
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    _INSERT,
                    cycle_id,
                    news_id,
                    provider,
                    engine_name,
                    str(signal_id) if signal_id is not None else None,
                    signal_type,
                    signal_strength,
                    signal_direction,
		    ticker,
                    _sha256(input_data),
                    _sha256(output_data),
                    latency_ms,
                    datetime.now(timezone.utc),
                )
        except Exception as exc:
            logger.warning(
                "signal_trace_write_error",
                engine=engine_name,
                cycle_id=cycle_id,
                error=str(exc),
            )
