"""Aggregation Worker — Lessons Learned Engine pipeline step 1.

Queries PostgreSQL and builds aggregated_stats.json containing:
- Overall performance summary
- Confidence bucket analysis
- Symbol / engine / regime breakdowns
- Position sizing data
- Holding time analysis
- Sentiment score analysis
- Individual trade history with full signal context

Usage:
    python scripts/aggregate_stats.py
    python scripts/aggregate_stats.py --days 90 --out /tmp/stats.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import statistics
import sys
from datetime import datetime, timezone
from typing import Any

import asyncpg

DB_DSN = os.getenv(
    "DATABASE_URL",
    f"postgresql://{os.getenv('POSTGRES_USER','mktintel')}:{os.getenv('POSTGRES_PASSWORD','bekir1905')}"
    f"@{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}"
    f"/{os.getenv('POSTGRES_DB','mktintel')}",
)


def _safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _profit_factor(pnls: list[float]) -> float:
    gross_win  = sum(p for p in pnls if p > 0)
    gross_loss = abs(sum(p for p in pnls if p < 0))
    return round(gross_win / gross_loss, 4) if gross_loss > 0 else float("inf")


def _metrics(pnls: list[float]) -> dict:
    if not pnls:
        return {"count": 0, "win_rate": 0, "avg_pnl": 0, "median_pnl": 0,
                "total_pnl": 0, "profit_factor": 0, "std_pnl": 0}
    wins = [p for p in pnls if p > 0]
    return {
        "count":         len(pnls),
        "win_rate":      round(len(wins) / len(pnls), 4),
        "avg_pnl":       round(statistics.mean(pnls), 4),
        "median_pnl":    round(statistics.median(pnls), 4),
        "total_pnl":     round(sum(pnls), 4),
        "profit_factor": _profit_factor(pnls),
        "std_pnl":       round(statistics.stdev(pnls) if len(pnls) > 1 else 0, 4),
    }


async def aggregate(days: int = 60) -> dict[str, Any]:
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=5)
    try:
        async with pool.acquire() as conn:
            return await _run_queries(conn, days)
    finally:
        await pool.close()


async def _run_queries(conn: asyncpg.Connection, days: int) -> dict[str, Any]:
    # ── 1. Raw trade history with full signal context ─────────────────────────
    raw = await conn.fetch(
        """
        SELECT
            pt.id, pt.asset, pt.exchange, pt.direction,
            pt.entry_price, pt.exit_price, pt.size, pt.notional,
            pt.fee, pt.pnl, pt.close_reason, pt.opened_at, pt.closed_at,
            EXTRACT(EPOCH FROM (pt.closed_at - pt.opened_at))/3600.0 AS hold_hours,
            sr.engine_name,
            sr.composite_score,
            sr.confidence_level   AS confidence_tier,
            (sr.signal_data->>'confidence_score')::float  AS confidence_score,
            (sr.signal_data->>'news_score')::float        AS news_score,
            (sr.signal_data->>'social_score')::float      AS social_score,
            (sr.signal_data->>'volume_score')::float      AS volume_score,
            (sr.signal_data->>'smart_money_score')::float AS smart_money_score,
            sr.signal_data->>'signal_quality'             AS signal_quality,
            sr.signal_data->>'narrative_cluster'          AS narrative_cluster
        FROM paper_trades pt
        JOIN signals_registry sr ON sr.id = pt.signal_id::bigint
        WHERE pt.closed_at IS NOT NULL
          AND pt.close_reason != 'dedup_cleanup'
          AND pt.pnl IS NOT NULL
          AND pt.closed_at > NOW() - make_interval(days => $1)
        ORDER BY pt.closed_at DESC
        """,
        days,
    )

    trades = []
    for r in raw:
        trades.append({
            "id":               r["id"],
            "asset":            r["asset"],
            "exchange":         r["exchange"],
            "direction":        r["direction"],
            "entry_price":      _safe_float(r["entry_price"]),
            "exit_price":       _safe_float(r["exit_price"]),
            "notional":         _safe_float(r["notional"]),
            "fee":              _safe_float(r["fee"]),
            "pnl":              _safe_float(r["pnl"]),
            "close_reason":     r["close_reason"],
            "opened_at":        r["opened_at"].isoformat() if r["opened_at"] else None,
            "closed_at":        r["closed_at"].isoformat() if r["closed_at"] else None,
            "hold_hours":       round(_safe_float(r["hold_hours"]), 2),
            "engine":           r["engine_name"],
            "composite_score":  _safe_float(r["composite_score"]),
            "confidence_tier":  r["confidence_tier"],
            "confidence_score": _safe_float(r["confidence_score"]),
            "news_score":       _safe_float(r["news_score"]),
            "social_score":     _safe_float(r["social_score"]),
            "volume_score":     _safe_float(r["volume_score"]),
            "smart_money_score":_safe_float(r["smart_money_score"]),
            "signal_quality":   r["signal_quality"],
            "narrative_cluster":r["narrative_cluster"],
        })

    if not trades:
        return {"generated_at": datetime.now(timezone.utc).isoformat(),
                "data_window_days": days, "trade_count": 0, "trades": []}

    pnls = [t["pnl"] for t in trades]

    # ── 2. Overall summary ────────────────────────────────────────────────────
    hold_hours = [t["hold_hours"] for t in trades if t["hold_hours"] > 0]
    summary = {
        **_metrics(pnls),
        "avg_hold_hours":    round(statistics.mean(hold_hours), 2) if hold_hours else 0,
        "max_hold_hours":    round(max(hold_hours), 2) if hold_hours else 0,
        "min_hold_hours":    round(min(hold_hours), 2) if hold_hours else 0,
        "long_count":        sum(1 for t in trades if t["direction"] == "long"),
        "short_count":       sum(1 for t in trades if t["direction"] == "short"),
        "total_fees":        round(sum(t["fee"] for t in trades), 4),
        "total_notional":    round(sum(t["notional"] for t in trades), 2),
    }

    # Max drawdown measured from $10,000 initial balance watermark
    _initial = float(os.getenv("INITIAL_BALANCE", "10000"))
    running, peak, max_dd = _initial, _initial, 0.0
    for t in sorted(trades, key=lambda x: x["closed_at"] or ""):
        running += t["pnl"]
        if running > peak:
            peak = running
        dd = (peak - running) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
    summary["max_drawdown_pct"] = round(max_dd * 100, 2)

    # ── 3. Confidence bucket analysis ────────────────────────────────────────
    buckets = {"0.00-0.20": [], "0.20-0.40": [], "0.40-0.60": [], "0.60-0.80": [], "0.80-1.00": []}
    for t in trades:
        c = t["confidence_score"]
        if c < 0.20:    buckets["0.00-0.20"].append(t["pnl"])
        elif c < 0.40:  buckets["0.20-0.40"].append(t["pnl"])
        elif c < 0.60:  buckets["0.40-0.60"].append(t["pnl"])
        elif c < 0.80:  buckets["0.60-0.80"].append(t["pnl"])
        else:           buckets["0.80-1.00"].append(t["pnl"])
    confidence_analysis = {
        "buckets": [{"range": k, **_metrics(v)} for k, v in buckets.items()],
        "optimal_threshold": None,
        "danger_zones": [],
    }
    # Find the bucket with best win_rate and positive avg_pnl
    best = max((b for b in confidence_analysis["buckets"] if b["count"] >= 3),
               key=lambda b: b["win_rate"] * (1 if b["avg_pnl"] > 0 else 0.1),
               default=None)
    if best:
        confidence_analysis["optimal_threshold"] = best["range"]
    confidence_analysis["danger_zones"] = [
        b["range"] for b in confidence_analysis["buckets"]
        if b["count"] >= 3 and b["win_rate"] < 0.30
    ]

    # ── 4. Symbol analysis ───────────────────────────────────────────────────
    sym_map: dict[str, list[float]] = {}
    for t in trades:
        sym_map.setdefault(t["asset"], []).append(t["pnl"])
    symbol_analysis = sorted(
        [{"symbol": s, **_metrics(p)} for s, p in sym_map.items()],
        key=lambda x: x["total_pnl"], reverse=True
    )

    # ── 5. Engine analysis ───────────────────────────────────────────────────
    eng_map: dict[str, list[float]] = {}
    for t in trades:
        eng_map.setdefault(t["engine"] or "unknown", []).append(t["pnl"])
    engine_analysis = sorted(
        [{"engine": e, **_metrics(p)} for e, p in eng_map.items()],
        key=lambda x: x["total_pnl"], reverse=True
    )

    # Attach signal_edge_metrics
    metrics_rows = await conn.fetch(
        """
        SELECT DISTINCT ON (engine_name) engine_name, win_rate, quality_score,
               sharpe_score, expectancy, sample_size, computed_at
          FROM signal_edge_metrics
         WHERE sample_size >= 10
         ORDER BY engine_name, computed_at DESC
        """
    )
    sig_metrics = [
        {"engine": r["engine_name"], "signal_win_rate": _safe_float(r["win_rate"]),
         "quality_score": _safe_float(r["quality_score"]),
         "sharpe_score": _safe_float(r["sharpe_score"]),
         "expectancy": _safe_float(r["expectancy"]),
         "sample_size": r["sample_size"]}
        for r in metrics_rows
    ]

    # ── 6. Close reason analysis ─────────────────────────────────────────────
    reason_map: dict[str, list[float]] = {}
    for t in trades:
        reason_map.setdefault(t["close_reason"] or "unknown", []).append(t["pnl"])
    close_reason_analysis = sorted(
        [{"close_reason": r, **_metrics(p)} for r, p in reason_map.items()],
        key=lambda x: x["total_pnl"], reverse=True
    )

    # ── 7. Holding time analysis ──────────────────────────────────────────────
    hold_buckets = {"<1h": [], "1-4h": [], "4-8h": [], "8-24h": [], ">24h": []}
    for t in trades:
        h = t["hold_hours"]
        if h < 1:    hold_buckets["<1h"].append(t["pnl"])
        elif h < 4:  hold_buckets["1-4h"].append(t["pnl"])
        elif h < 8:  hold_buckets["4-8h"].append(t["pnl"])
        elif h < 24: hold_buckets["8-24h"].append(t["pnl"])
        else:        hold_buckets[">24h"].append(t["pnl"])
    holding_time_analysis = {
        "buckets": [{"range": k, **_metrics(v)} for k, v in hold_buckets.items()],
        "optimal_range": None,
    }
    best_hold = max(
        (b for b in holding_time_analysis["buckets"] if b["count"] >= 3),
        key=lambda b: b["avg_pnl"], default=None
    )
    if best_hold:
        holding_time_analysis["optimal_range"] = best_hold["range"]

    # ── 8. Position sizing analysis ──────────────────────────────────────────
    sizing_buckets = {"<$50": [], "$50-$100": [], "$100-$200": [], ">$200": []}
    for t in trades:
        n = t["notional"]
        if n < 50:    sizing_buckets["<$50"].append(t["pnl"])
        elif n < 100: sizing_buckets["$50-$100"].append(t["pnl"])
        elif n < 200: sizing_buckets["$100-$200"].append(t["pnl"])
        else:         sizing_buckets[">$200"].append(t["pnl"])
    position_sizing_analysis = {
        "buckets": [{"range": k, **_metrics(v)} for k, v in sizing_buckets.items()],
        "avg_notional": round(statistics.mean(t["notional"] for t in trades), 2),
        "median_notional": round(statistics.median(t["notional"] for t in trades), 2),
    }

    # ── 9. Sentiment analysis (news / social / volume scores) ────────────────
    def _sentiment_buckets(field: str):
        bkts = {"negative(<-0.3)": [], "weak(-0.3-0)": [], "neutral(0-0.3)": [], "positive(>0.3)": []}
        for t in trades:
            v = t[field]
            if v < -0.3:   bkts["negative(<-0.3)"].append(t["pnl"])
            elif v < 0:    bkts["weak(-0.3-0)"].append(t["pnl"])
            elif v < 0.3:  bkts["neutral(0-0.3)"].append(t["pnl"])
            else:          bkts["positive(>0.3)"].append(t["pnl"])
        return [{"range": k, **_metrics(v)} for k, v in bkts.items()]

    sentiment_analysis = {
        "news_score_buckets":         _sentiment_buckets("news_score"),
        "social_score_buckets":       _sentiment_buckets("social_score"),
        "volume_score_buckets":       _sentiment_buckets("volume_score"),
        "smart_money_score_buckets":  _sentiment_buckets("smart_money_score"),
    }

    # ── 10. Direction analysis ───────────────────────────────────────────────
    direction_analysis = {
        "long":  _metrics([t["pnl"] for t in trades if t["direction"] == "long"]),
        "short": _metrics([t["pnl"] for t in trades if t["direction"] == "short"]),
    }

    # ── 11. Composite score vs pnl ───────────────────────────────────────────
    score_buckets = {
        "strong_short(<-0.25)": [], "moderate_short(-0.25--0.13)": [],
        "weak_short(-0.13--0.10)": [], "neutral(-0.10-0.10)": [],
        "weak_long(0.10-0.13)": [], "moderate_long(0.13-0.25)": [],
        "strong_long(>0.25)": [],
    }
    for t in trades:
        s = t["composite_score"]
        if s < -0.25:   score_buckets["strong_short(<-0.25)"].append(t["pnl"])
        elif s < -0.13: score_buckets["moderate_short(-0.25--0.13)"].append(t["pnl"])
        elif s < -0.10: score_buckets["weak_short(-0.13--0.10)"].append(t["pnl"])
        elif s < 0.10:  score_buckets["neutral(-0.10-0.10)"].append(t["pnl"])
        elif s < 0.13:  score_buckets["weak_long(0.10-0.13)"].append(t["pnl"])
        elif s < 0.25:  score_buckets["moderate_long(0.13-0.25)"].append(t["pnl"])
        else:           score_buckets["strong_long(>0.25)"].append(t["pnl"])
    composite_score_analysis = [{"range": k, **_metrics(v)} for k, v in score_buckets.items()]

    # ── 12. Winner / loser patterns ──────────────────────────────────────────
    winners = [t for t in trades if t["pnl"] > 0]
    losers  = [t for t in trades if t["pnl"] < 0]

    def _common_attrs(subset: list[dict]) -> dict:
        if not subset:
            return {}
        assets   = [t["asset"] for t in subset]
        engines  = [t["engine"] for t in subset]
        reasons  = [t["close_reason"] for t in subset]
        conf_avg = statistics.mean(t["confidence_score"] for t in subset)
        soc_avg  = statistics.mean(t["social_score"] for t in subset)
        news_avg = statistics.mean(t["news_score"] for t in subset)
        top_asset  = max(set(assets),  key=assets.count)
        top_engine = max(set(engines), key=engines.count)
        top_reason = max(set(reasons), key=reasons.count)
        return {
            "count":              len(subset),
            "avg_pnl":            round(statistics.mean(t["pnl"] for t in subset), 4),
            "top_asset":          top_asset,
            "top_engine":         top_engine,
            "top_close_reason":   top_reason,
            "avg_confidence":     round(conf_avg, 4),
            "avg_social_score":   round(soc_avg, 4),
            "avg_news_score":     round(news_avg, 4),
            "avg_hold_hours":     round(statistics.mean(t["hold_hours"] for t in subset), 2),
        }

    winner_characteristics = _common_attrs(winners)
    loser_characteristics  = _common_attrs(losers)

    # ── 13. Funding rate analysis (limited — not stored per-trade) ────────────
    funding_analysis = {
        "note": "Historical funding rates not stored per-trade. Gate added Sprint 33.",
        "current_gate_threshold": -0.001,
        "current_btc_funding":    "see logs",
        "data_available":         False,
    }

    # ── 14. Volatility proxy (price change over hold period) ─────────────────
    vol_trades = [t for t in trades if t["hold_hours"] > 0 and t["entry_price"] > 0]
    if vol_trades:
        price_changes = [
            abs(t["exit_price"] - t["entry_price"]) / t["entry_price"]
            for t in vol_trades if t["exit_price"]
        ]
        vol_buckets_data: dict[str, list[float]] = {
            "low(<1%)": [], "medium(1-3%)": [], "high(3-7%)": [], "extreme(>7%)": []
        }
        for t, pc in zip(vol_trades, price_changes):
            if pc < 0.01:    vol_buckets_data["low(<1%)"].append(t["pnl"])
            elif pc < 0.03:  vol_buckets_data["medium(1-3%)"].append(t["pnl"])
            elif pc < 0.07:  vol_buckets_data["high(3-7%)"].append(t["pnl"])
            else:            vol_buckets_data["extreme(>7%)"].append(t["pnl"])
        volatility_analysis: dict[str, Any] = {
            "price_move_buckets": [{"range": k, **_metrics(v)} for k, v in vol_buckets_data.items()],
            "avg_price_move_pct": round(statistics.mean(price_changes) * 100, 3),
        }
    else:
        volatility_analysis = {"note": "insufficient data"}

    # ── 15. Regime analysis (proxy: BTC direction at trade open) ─────────────
    # Simplified: use composite_score direction as regime proxy
    regime_map: dict[str, list[float]] = {"bearish_signal": [], "bullish_signal": [], "neutral_signal": []}
    for t in trades:
        s = t["composite_score"]
        if s < -0.10:   regime_map["bearish_signal"].append(t["pnl"])
        elif s > 0.10:  regime_map["bullish_signal"].append(t["pnl"])
        else:           regime_map["neutral_signal"].append(t["pnl"])
    regime_analysis = [{"regime": k, **_metrics(v)} for k, v in regime_map.items()]

    # ── Assemble output ───────────────────────────────────────────────────────
    return {
        "generated_at":         datetime.now(timezone.utc).isoformat(),
        "data_window_days":     days,
        "summary":              summary,
        "trades":               trades,
        "confidence_analysis":  confidence_analysis,
        "symbol_analysis":      symbol_analysis,
        "engine_analysis":      engine_analysis,
        "signal_edge_metrics":  sig_metrics,
        "close_reason_analysis": close_reason_analysis,
        "holding_time_analysis": holding_time_analysis,
        "position_sizing_analysis": position_sizing_analysis,
        "sentiment_analysis":   sentiment_analysis,
        "direction_analysis":   direction_analysis,
        "composite_score_analysis": composite_score_analysis,
        "regime_analysis":      regime_analysis,
        "volatility_analysis":  volatility_analysis,
        "funding_analysis":     funding_analysis,
        "winner_characteristics": winner_characteristics,
        "loser_characteristics":  loser_characteristics,
    }


async def main(days: int, out_path: str) -> None:
    print(f"Aggregating last {days} days of trade data...")
    stats = await aggregate(days)
    with open(out_path, "w") as f:
        json.dump(stats, f, indent=2, default=str)
    count = stats.get("summary", {}).get("count", 0)
    print(f"Written {count} trades → {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--out",  default="aggregated_stats.json")
    args = parser.parse_args()
    asyncio.run(main(args.days, args.out))
