"""30-minute equity snapshot writer for equity_snapshots table."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

async def write_snapshot(
    db_pool,
    balance: float,
    equity: float,
    peak_balance: float,
    governor_state: str,
    regime: str,
    cluster_edges: dict,
    mode: str = "paper",
) -> None:
    """Write one equity snapshot row. Best-effort: errors are logged and swallowed."""
    drawdown_pct = 0.0
    # Use equity (balance + notional + unrealized) vs peak equity for accurate drawdown.
    # balance alone drops when positions open, creating phantom drawdown while profitable.
    if peak_balance > 0 and equity < peak_balance:
        drawdown_pct = round((peak_balance - equity) / peak_balance, 5)
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO equity_snapshots "
                "(mode, balance, equity, drawdown_pct, governor_state, regime, cluster_edges) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)",
                mode,
                round(balance, 4),
                round(equity, 4),
                drawdown_pct,
                governor_state,
                regime,
                json.dumps(cluster_edges),
            )
    except Exception as exc:
        logger.warning("equity_tracker_write_failed: %s", exc)
