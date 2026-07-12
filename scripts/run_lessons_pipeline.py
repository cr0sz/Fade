"""Lessons Learned Pipeline — aggregate → LLM governance → apply.

Architecture:
    aggregate_stats (PostgreSQL)
        → qwen2.5:3b fast pre-signal (optional)
        → qwen2.5:7b main governance decision
        → JSON guard + safety filter + rule engine
        → governance_engine (Redis + .env + DB + Telegram)

Runs weekly via EdgeScheduler, or manually.

Usage:
    python scripts/run_lessons_pipeline.py
    python scripts/run_lessons_pipeline.py --days 30 --dry-run
    python scripts/run_lessons_pipeline.py --model qwen2.5:7b --fast-model qwen2.5:3b
    python scripts/run_lessons_pipeline.py --no-fast-model
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aggregate_stats   import aggregate
from lessons_learned   import run as run_lessons
from governance_engine import run as run_governance

OLLAMA_BASE   = os.getenv("OLLAMA_BASE_URL",   "http://localhost:11434")
DEFAULT_MODEL = os.getenv("OLLAMA_MODEL",      "qwen2.5:7b")
DEFAULT_FAST  = os.getenv("OLLAMA_FAST_MODEL", "qwen2.5:3b")


async def pipeline(days: int, model: str, fast_model: str | None,
                   ollama: str, min_trades: int, dry_run: bool) -> None:
    print(f"\n{'='*60}")
    print(f" Lessons Learned Pipeline  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f" Main: {model}  Fast: {fast_model or 'disabled'}  @ {ollama}")
    print(f"{'='*60}\n")

    # ── Step 1: Aggregate ────────────────────────────────────────────────────
    print("STEP 1/3 — Aggregate stats")
    stats = await aggregate(days)
    count = stats.get("summary", {}).get("count", 0)
    wr    = stats.get("summary", {}).get("win_rate", 0)
    pnl   = stats.get("summary", {}).get("total_pnl", 0)
    dd    = stats.get("summary", {}).get("max_drawdown_pct", 0)
    print(f"  Trades: {count} | WR: {wr:.1%} | PnL: ${pnl:+.2f} | Max DD: {dd:.2f}%")

    if count < min_trades:
        print(f"\n  Insufficient trades ({count} < {min_trades}). Pipeline aborted.")
        return

    stats_path = lessons_path = ""
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as sf:
            stats_path = sf.name
            json.dump(stats, sf, default=str)

        # ── Step 2: LLM analysis ─────────────────────────────────────────────
        print(f"\nSTEP 2/3 — LLM analysis")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as lf:
            lessons_path = lf.name

        try:
            run_lessons(
                stats_path, lessons_path, model, ollama,
                fast_model=fast_model, use_fast=(fast_model is not None),
            )
        except Exception as e:
            print(f"  LLM step failed: {e}")
            return

        with open(lessons_path) as f:
            lessons = json.load(f)

        risk    = lessons.get("risk_level", "?")
        regime  = lessons.get("regime", "?")
        summary = lessons.get("summary", "")
        actions = lessons.get("actions", [])
        recs    = lessons.get("strategy_recommendations", [])
        meta    = lessons.get("_meta", {})
        print(f"\n  Risk level : {risk}  |  Regime: {regime}")
        print(f"  Summary    : {summary}")
        print(f"  Latency    : V4={meta.get('latency_ms','?')}ms  "
              f"V5={meta.get('v5_latency_ms','?')}ms")
        if actions:
            print(f"\n  Governance actions ({len(actions)}):")
            for a in actions:
                print(f"    [{a.get('type'):25s}] {a.get('target'):15s}"
                      f" conf={a.get('confidence',0):.2f}  {a.get('reason','')[:60]}")
        if recs:
            print(f"\n  V5 strategy recommendations ({len(recs)}):")
            for r in recs:
                print(f"    [{r.get('action'):20s}] {r.get('strategy'):15s}"
                      f" conf={r.get('confidence',0):.2f}  {r.get('reason','')[:60]}")
        p_health  = lessons.get("portfolio_health", "")
        p_summary = lessons.get("portfolio_summary", "")
        p_actions = lessons.get("portfolio_actions", [])
        if p_health:
            print(f"\n  Portfolio health : {p_health}")
            print(f"  Portfolio summary: {p_summary}")
        if p_actions:
            print(f"\n  V6 portfolio actions ({len(p_actions)}):")
            for a in p_actions:
                print(f"    [{a.get('type'):22s}] {a.get('strategy'):20s}"
                      f" conf={a.get('confidence',0):.2f}  {a.get('reason','')[:55]}")
        sys_health = lessons.get("system_health", "")
        meta_sum   = lessons.get("meta_summary", "")
        issues     = lessons.get("detected_issues", [])
        imp_acts   = lessons.get("improvement_actions", [])
        v7_lat     = meta.get("v7_latency_ms", "?")
        if sys_health:
            print(f"\n  System health    : {sys_health}  (V7 latency: {v7_lat}ms)")
            print(f"  Meta summary     : {meta_sum}")
        if issues:
            print(f"\n  V7 detected issues ({len(issues)}):")
            for i in issues:
                print(f"    [{i.get('severity').upper():6s}] [{i.get('layer'):6s}] "
                      f"{i.get('issue','')[:70]}")
        if imp_acts:
            print(f"\n  V7 improvement actions ({len(imp_acts)}):")
            for a in imp_acts:
                print(f"    [{a.get('type'):25s}] layer={a.get('target_layer'):6s} "
                      f"conf={a.get('confidence',0):.2f}  {a.get('reason','')[:55]}")
        arch_health  = lessons.get("architecture_health", "")
        arch_changes = lessons.get("architecture_changes", [])
        layer_anal   = lessons.get("layer_analysis", [])
        sys_rec      = lessons.get("system_recommendation", "")
        v8_lat       = meta.get("v8_latency_ms", "?")
        if arch_health:
            print(f"\n  Architecture health : {arch_health}  (V8 latency: {v8_lat}ms)")
            print(f"  System recommendation: {sys_rec}")
        if layer_anal:
            print(f"\n  V8 layer analysis ({len(layer_anal)}):")
            for la in layer_anal:
                print(f"    [{la.get('impact').upper():6s}] [{la.get('layer'):3s}] "
                      f"{la.get('issue','')[:70]}")
        if arch_changes:
            print(f"\n  V8 architecture changes ({len(arch_changes)}):")
            for a in arch_changes:
                print(f"    [{a.get('type'):18s}] layer={a.get('target_layer'):6s} "
                      f"conf={a.get('confidence',0):.2f}  {a.get('reason','')[:55]}")

        # ── Step 3: Governance ───────────────────────────────────────────────
        label = "  [DRY-RUN]" if dry_run else ""
        print(f"\nSTEP 3/3 — Apply governance{label}")
        try:
            await run_governance(lessons_path, dry_run)
        except Exception as e:
            print(f"  Governance step failed: {e}")

    finally:
        for p in (stats_path, lessons_path):
            if p:
                try:
                    os.unlink(p)
                except Exception:
                    pass

    print(f"\n{'='*60}")
    print(" Pipeline complete.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days",          type=int, default=60)
    parser.add_argument("--model",         default=DEFAULT_MODEL)
    parser.add_argument("--fast-model",    default=DEFAULT_FAST)
    parser.add_argument("--no-fast-model", action="store_true")
    parser.add_argument("--ollama",        default=OLLAMA_BASE)
    parser.add_argument("--min-trades",    type=int, default=15)
    parser.add_argument("--dry-run",       action="store_true")
    args = parser.parse_args()
    fm = None if args.no_fast_model else args.fast_model
    asyncio.run(pipeline(args.days, args.model, fm, args.ollama, args.min_trades, args.dry_run))
