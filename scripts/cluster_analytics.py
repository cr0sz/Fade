"""Per-cluster (TREND/FLOW/STRESS) P&L analytics from paper_trades history.

Run: docker exec market-intelligence-paper-trading-engine-1 python -m scripts.cluster_analytics
Or:  python scripts/cluster_analytics.py (from repo root with PYTHONPATH=.)
"""
from __future__ import annotations

import asyncio
import os
import sys

# Allow running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg

try:
    from src.core.discord import send_embed, WEBHOOK_ALERTS, COLOR_INFO, COLOR_WARN
except ImportError:
    send_embed = None  # type: ignore[assignment]
    WEBHOOK_ALERTS = os.getenv("DISCORD_WEBHOOK_ALERTS", "")
    COLOR_INFO = 3447003
    COLOR_WARN = 15105570

from paper_trading_engine.risk_governor import cluster_of, CLUSTERS


async def compute_cluster_pnl(pool: asyncpg.Pool) -> dict[str, dict]:
    """Join paper_trades with signals_registry, map to clusters, aggregate."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT sr.engine_name,
                   pt.pnl,
                   pt.direction
            FROM paper_trades pt
            JOIN signals_registry sr ON sr.id = pt.signal_id
            WHERE pt.closed_at IS NOT NULL
              AND pt.pnl IS NOT NULL
              AND sr.engine_name IS NOT NULL
        """)

    clusters: dict[str, dict] = {
        c: {"trades": 0, "wins": 0, "total_pnl": 0.0, "pnls": []}
        for c in CLUSTERS
    }
    clusters["UNKNOWN"] = {"trades": 0, "wins": 0, "total_pnl": 0.0, "pnls": []}

    for row in rows:
        cname = cluster_of(row["engine_name"]) or "UNKNOWN"
        d = clusters[cname]
        d["trades"] += 1
        d["total_pnl"] += float(row["pnl"])
        d["pnls"].append(float(row["pnl"]))
        if float(row["pnl"]) > 0:
            d["wins"] += 1

    result = {}
    for cname, d in clusters.items():
        n = d["trades"]
        wr = d["wins"] / n if n else 0.0
        avg = d["total_pnl"] / n if n else 0.0
        result[cname] = {
            "trades": n,
            "win_rate": round(wr, 3),
            "total_pnl": round(d["total_pnl"], 2),
            "avg_pnl": round(avg, 2),
        }
    return result


async def main() -> None:
    db_url = (
        f"postgresql://{os.getenv('POSTGRES_USER', 'mktintel')}"
        f":{os.getenv('POSTGRES_PASSWORD', 'bekir1905')}"
        f"@{os.getenv('POSTGRES_HOST', 'postgres')}"
        f":{os.getenv('POSTGRES_PORT', '5432')}"
        f"/{os.getenv('POSTGRES_DB', 'mktintel')}"
    )
    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=3)
    try:
        stats = await compute_cluster_pnl(pool)
    finally:
        await pool.close()

    # Print to stdout
    print("\n=== CLUSTER ANALYTICS ===")
    for cname, s in stats.items():
        print(f"{cname:8s}: {s['trades']:4d} trades | WR {s['win_rate']:.1%} | Avg ${s['avg_pnl']:+.2f} | Total ${s['total_pnl']:+.2f}")
    print()

    # Push to Discord
    if send_embed and WEBHOOK_ALERTS:
        fields = []
        for cname, s in stats.items():
            if s["trades"] == 0:
                continue
            color_indicator = "🟢" if s["total_pnl"] >= 0 else "🔴"
            fields.append({
                "name": f"{color_indicator} {cname}",
                "value": f"Trades: {s['trades']} | WR: {s['win_rate']:.1%}\nAvg: ${s['avg_pnl']:+.2f} | Total: ${s['total_pnl']:+.2f}",
                "inline": True,
            })
        if fields:
            await send_embed(
                WEBHOOK_ALERTS,
                "📊 Cluster P&L Analytics",
                color=COLOR_INFO if sum(s["total_pnl"] for s in stats.values()) >= 0 else COLOR_WARN,
                fields=fields,
            )
            print("Pushed to Discord.")


if __name__ == "__main__":
    asyncio.run(main())
