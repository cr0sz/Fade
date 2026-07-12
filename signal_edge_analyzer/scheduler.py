"""Job scheduler for the Signal Edge Analyzer.

Uses plain asyncio coroutines — no APScheduler dependency.
Each job runs in an infinite loop with configurable sleep intervals.
All jobs are idempotent: running twice in the same window is safe.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import pathlib
import time
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import redis.asyncio as aioredis
import structlog

from aggregator import run_aggregation_cycle, run_metrics_cycle
from collector import collect_recent_signals, get_max_signal_id
from models import SystemEvent
from discord_client import DiscordClient as TelegramClient
from walkforward import run_walkforward

logger = structlog.get_logger(__name__)

# ── Intervals (seconds) ────────────────────────────────────────────────────────
OUTCOME_INTERVAL     = int(os.getenv("EDGE_ANALYZER_OUTCOME_INTERVAL",     "300"))    # 5 min
METRICS_INTERVAL     = int(os.getenv("EDGE_ANALYZER_METRICS_INTERVAL",     "3600"))   # 1 hour
AGGREGATION_INTERVAL = int(os.getenv("EDGE_ANALYZER_AGGREGATION_INTERVAL", "86400"))  # 24 hours
CLAUDE_MD_INTERVAL   = int(os.getenv("EDGE_ANALYZER_CLAUDE_MD_INTERVAL",   "1800"))   # 30 min
HEALTH_INTERVAL      = int(os.getenv("EDGE_ANALYZER_HEALTH_INTERVAL",      "30"))     # 30 sec
WEEKLY_REPORT_INTERVAL   = int(os.getenv("EDGE_ANALYZER_WEEKLY_INTERVAL",    "604800")) # 7 days
LESSONS_PIPELINE_INTERVAL= int(os.getenv("EDGE_ANALYZER_LESSONS_INTERVAL",  "604800")) # 7 days
WALKFORWARD_INTERVAL     = int(os.getenv("EDGE_ANALYZER_WALKFORWARD_INTERVAL", "86400")) # daily
META_STRATEGY_INTERVAL   = int(os.getenv("META_STRATEGY_INTERVAL", "1800"))               # 30 min

CLAUDE_MD_PATH = os.getenv("CLAUDE_MD_PATH", "/workspace/architecture.md")

# Prometheus counters — imported lazily to avoid circular import
_outcomes_processed    = None
_metrics_computed      = None
_aggregations_run      = None
_claude_md_reads       = None
_claude_md_changes     = None


def _init_prom() -> None:
    global _outcomes_processed, _metrics_computed, _aggregations_run
    global _claude_md_reads, _claude_md_changes
    if _outcomes_processed is not None:
        return
    from prometheus_client import Counter, Gauge
    _outcomes_processed = Counter("edge_outcomes_processed_total", "Signal outcomes processed")
    _metrics_computed   = Counter("edge_metrics_computed_total", "Metric computation cycles")
    _aggregations_run   = Counter("edge_aggregations_total", "Aggregation cycles completed")
    _claude_md_reads    = Counter("edge_claude_md_reads_total", "Architecture doc reads")
    _claude_md_changes  = Counter("edge_claude_md_changes_total", "Architecture doc changes detected")


class EdgeScheduler:
    """Coordinates all recurring jobs for the edge analyzer."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        redis: aioredis.Redis,
        telegram: TelegramClient,
    ) -> None:
        self._pool = pool
        self._redis = redis
        self._tg = telegram
        self._watermark = 0
        self._claude_md_hash: Optional[str] = None
        self._running = False
        self._last_weekly_report: float = 0.0
        _init_prom()

    async def start(self) -> None:
        """Initialise state and launch all job coroutines."""
        self._watermark = await get_max_signal_id(self._pool)
        self._running = True
        logger.info("scheduler_started", watermark=self._watermark)

        await asyncio.gather(
            self._loop("outcome_collection", OUTCOME_INTERVAL,         self._outcome_job),
            self._loop("metrics_update",     METRICS_INTERVAL,         self._metrics_job),
            self._loop("aggregation",        AGGREGATION_INTERVAL,     self._aggregation_job),
            self._loop("claude_md_check",    CLAUDE_MD_INTERVAL,       self._claude_md_job),
            self._loop("weekly_diagnosis",   WEEKLY_REPORT_INTERVAL,   self._weekly_diagnosis_job),
            self._loop("lessons_pipeline",   LESSONS_PIPELINE_INTERVAL,self._lessons_pipeline_job),
            self._loop("walkforward_daily",  WALKFORWARD_INTERVAL,     self._walkforward_job),
            self._loop("meta_strategy",      META_STRATEGY_INTERVAL,   self._meta_strategy_job),
        )

    async def stop(self) -> None:
        self._running = False

    # ── Job loop wrapper ───────────────────────────────────────────────────────

    async def _loop(self, name: str, interval: int, job) -> None:
        """Run `job` every `interval` seconds with exponential backoff on failure.

        Long-interval jobs (>= 1 day) persist their last-run timestamp in Redis:
        without that, every container restart immediately re-fired the weekly
        jobs — including the full LLM lessons pipeline and governance writes.
        """
        await asyncio.sleep({"outcome_collection": 0, "metrics_update": 30,
                             "aggregation": 60, "claude_md_check": 10}.get(name, 0))
        persist_key = f"edge_analyzer:last_run:{name}"
        persist = interval >= 86400
        if persist and self._redis:
            try:
                last = await self._redis.get(persist_key)
                if last:
                    remaining = float(last) + interval - time.time()
                    if remaining > 0:
                        logger.info("job_resume_wait", job=name,
                                    wait_s=round(remaining))
                        await asyncio.sleep(remaining)
            except Exception as exc:
                logger.warning("job_last_run_load_failed", job=name, error=str(exc))
        consecutive_errors = 0
        while self._running:
            t0 = time.monotonic()
            try:
                await job()
                consecutive_errors = 0
                if persist and self._redis:
                    try:
                        await self._redis.set(persist_key, str(time.time()))
                    except Exception:
                        pass
            except asyncio.CancelledError:
                break
            except Exception as exc:
                consecutive_errors += 1
                logger.exception("job_failed", job=name, error=str(exc),
                                 consecutive=consecutive_errors)
                # Only alert on first failure and every 5th after to avoid Telegram floods
                if consecutive_errors == 1 or consecutive_errors % 5 == 0:
                    await self._tg.send_error(name, str(exc))
                # Exponential backoff capped at 1 hour; don't busy-loop on persistent errors
                backoff = min(3600, interval * (2 ** min(consecutive_errors - 1, 6)))
                await asyncio.sleep(backoff)
                continue
            elapsed = time.monotonic() - t0
            sleep_for = max(0.0, interval - elapsed)
            await asyncio.sleep(sleep_for)

    # ── Individual jobs ────────────────────────────────────────────────────────

    async def _outcome_job(self) -> None:
        """Poll for new signals and report them to Telegram."""
        new_signals = await collect_recent_signals(self._pool, since_id=self._watermark)
        if not new_signals:
            return

        count = 0
        for sig in new_signals:
            signal_id = int(sig["id"])
            if signal_id > self._watermark:
                self._watermark = signal_id
            count += 1

        if _outcomes_processed:
            _outcomes_processed.inc(count)

        logger.info("outcome_job", new_signals=count, watermark=self._watermark)

        # Telegram: report the first new signal that isn't a duplicate of a recent notification.
        # Dedup key = "asset:engine"; TTL = 30 minutes to suppress e.g. IFE spamming COIN every 5 min.
        _DEDUP_TTL = 1800
        notified = False
        for s in new_signals:
            dedup_key = f"edge_notif:{s.get('ticker', '')}:{s.get('engine_name', '')}"
            try:
                if self._redis and await self._redis.set(dedup_key, "1", nx=True, ex=_DEDUP_TTL):
                    event = SystemEvent(
                        event_type="signal_processed",
                        signal_id=int(s["id"]),
                        asset=s.get("ticker"),
                        engine=s.get("engine_name"),
                    )
                    await self._tg.send_json(event.to_dict())
                    notified = True
                    break
            except Exception:
                pass
        if not notified:
            logger.debug("outcome_job_all_deduped", count=count)

    async def _walkforward_job(self) -> None:
        """Daily out-of-sample replay of the Truth Router policy → Telegram.

        The edge-decay heartbeat: if the routed policy's OOS average goes
        non-positive, the summary carries an explicit warning so threshold
        review happens within a day, not after a drawdown.
        """
        summary = await run_walkforward(self._pool, days=7)
        logger.info("walkforward_job", summary=summary.replace("\n", " | "))
        await self._tg.send_message(summary)

    async def _metrics_job(self) -> None:
        """Compute rolling quality metrics for all strategy groups."""
        metrics = await run_metrics_cycle(self._pool, period_days=30)
        if _metrics_computed:
            _metrics_computed.inc()

        logger.info("metrics_job", groups_computed=len(metrics))

        if metrics:
            best = max(metrics, key=lambda m: m.quality_score)
            await self._tg.send_metrics_updated(
                engine=best.engine_name,
                sample_size=best.sample_size,
                quality=best.quality_score,
            )

    async def _aggregation_job(self) -> None:
        """Run full aggregation and send daily top-5 strategies to Telegram."""
        rankings = await run_aggregation_cycle(self._pool, top_n=20, period_days=30)
        if _aggregations_run:
            _aggregations_run.inc()

        logger.info("aggregation_job", strategies_ranked=len(rankings))

        top5 = [
            {
                "engine_name":   r.engine_name,
                "ticker":        r.ticker,
                "direction":     r.direction,
                "window_hours":  r.window_hours,
                "quality_score": r.quality_score,
                "win_rate":      r.win_rate,
                "expectancy":    r.expectancy,
            }
            for r in rankings[:5]
        ]
        await self._tg.send_top_strategies(top5)

    async def _weekly_diagnosis_job(self) -> None:
        """Push weekly self-diagnosis report to Telegram every Monday 08:00 UTC (approx).

        Reports: 7-day win rate, avg PnL, Sharpe estimate, top/bottom assets,
        active vs excluded engines, threshold drift, and a plain-language recommendation.
        """
        import math
        import json as _json

        async with self._pool.acquire() as conn:
            # 7-day performance summary
            perf = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) FILTER (WHERE close_reason != 'dedup_cleanup') AS total,
                    COUNT(*) FILTER (WHERE pnl > 0 AND close_reason != 'dedup_cleanup') AS wins,
                    SUM(pnl) FILTER (WHERE close_reason != 'dedup_cleanup') AS total_pnl,
                    AVG(pnl) FILTER (WHERE close_reason != 'dedup_cleanup') AS avg_pnl,
                    STDDEV(pnl) FILTER (WHERE close_reason != 'dedup_cleanup') AS stddev_pnl
                  FROM paper_trades
                 WHERE closed_at > NOW() - INTERVAL '7 days'
                   AND closed_at IS NOT NULL
                   AND pnl IS NOT NULL
                """
            )
            # Per-asset 7-day PnL
            asset_rows = await conn.fetch(
                """
                SELECT asset, SUM(pnl) AS total_pnl, COUNT(*) AS n,
                       COUNT(*) FILTER (WHERE pnl > 0) AS wins
                  FROM paper_trades
                 WHERE closed_at > NOW() - INTERVAL '7 days'
                   AND closed_at IS NOT NULL AND pnl IS NOT NULL
                   AND close_reason != 'dedup_cleanup'
                 GROUP BY asset
                 ORDER BY SUM(pnl) DESC
                """
            )
            # Engine accuracy: sample-weighted across per-ticker/window slices.
            # (signal_edge_metrics rows are per (ticker, direction, window_hours);
            # a DISTINCT ON pick would report one arbitrary slice as "the engine".)
            engine_rows = await conn.fetch(
                """
                SELECT engine_name,
                       SUM(win_rate * sample_size) / NULLIF(SUM(sample_size), 0)      AS win_rate,
                       SUM(quality_score * sample_size) / NULLIF(SUM(sample_size), 0) AS quality_score,
                       SUM(sample_size) AS sample_size
                  FROM signal_edge_metrics
                 WHERE computed_at > NOW() - INTERVAL '48 hours'
                   AND window_hours <= 6
                   AND sample_size >= 5
                 GROUP BY engine_name
                HAVING SUM(sample_size) >= 10
                """
            )

        total   = int(perf["total"] or 0)
        wins    = int(perf["wins"] or 0)
        wr      = wins / total if total else 0.0
        avg_pnl = float(perf["avg_pnl"] or 0.0)
        std_pnl = float(perf["stddev_pnl"] or 0.0)
        total_pnl = float(perf["total_pnl"] or 0.0)
        sharpe  = (avg_pnl / std_pnl) if std_pnl > 0.001 else 0.0

        # Top 3 / Bottom 3 assets
        assets_sorted = [(r["asset"], float(r["total_pnl"] or 0), int(r["n"]), int(r["wins"])) for r in asset_rows]
        top3    = assets_sorted[:3]
        bottom3 = [a for a in reversed(assets_sorted) if a[1] < 0][:3]

        # Active vs excluded engines
        # Must match _EXCLUDED_ENGINES in paper_trading_engine/engine.py
        _HARDCODED_EXCLUDED = {"ETFRotationEngine", "InstitutionalConfluenceEngine", "InstitutionalFlowEngine"}
        dynamic_excluded: set[str] = set()
        dynamic_banned: dict[str, str] = {}
        regime_profile = "neutral"
        if self._redis:
            try:
                _ex = await self._redis.smembers("trading:excluded_engines")
                if _ex:
                    dynamic_excluded = set(_ex)
                _bans = await self._redis.hgetall("trading:dynamic_bans")
                if _bans:
                    for asset_key, val in _bans.items():
                        try:
                            info = _json.loads(val)
                            dynamic_banned[asset_key] = info.get("reason", val)
                        except Exception:
                            dynamic_banned[asset_key] = str(val)
                _rp = await self._redis.get("trading:regime_profile")
                if _rp:
                    regime_profile = _rp
            except Exception:
                pass

        all_excluded = _HARDCODED_EXCLUDED | dynamic_excluded
        active_engines = [r["engine_name"] for r in engine_rows if r["engine_name"] not in all_excluded]
        excluded_with_acc = [
            f"{r['engine_name']} ({(r['win_rate'] or 0)*100:.0f}% acc)"
            for r in engine_rows if r["engine_name"] in all_excluded
        ]

        # Plain-language recommendation
        if total < 10:
            recommendation = "Insufficient data (< 10 trades). Continue collecting."
        elif wr >= 0.50:
            recommendation = f"WR {wr:.0%} — strong. Consider re-enabling longs if disabled."
        elif wr >= 0.45:
            recommendation = f"WR {wr:.0%} — on target. Hold current settings."
        elif wr >= 0.35:
            recommendation = f"WR {wr:.0%} — below 45% target. Monitor for 7 more days before tightening."
        else:
            recommendation = f"WR {wr:.0%} — poor. Consider tightening short_threshold or reviewing banned assets."

        # Format report
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        lines = [f"📊 *Weekly Self-Diagnosis — {now_str}*\n"]

        lines.append(f"*Performance (7 days):*")
        wr_arrow = "↑" if wr >= 0.45 else "↓"
        lines.append(f"  Win rate: {wr:.1%} {wr_arrow}  |  Trades: {total}")
        lines.append(f"  Avg PnL: ${avg_pnl:+.2f}/trade  |  Total: ${total_pnl:+.2f}")
        lines.append(f"  Sharpe est: {sharpe:.2f}  |  Regime: {regime_profile}")

        if top3:
            lines.append(f"\n*Top assets:*")
            for asset, pnl, n, w in top3:
                lines.append(f"  {asset}: ${pnl:+.2f} ({w}/{n} wins)")

        if bottom3:
            lines.append(f"\n*Bottom assets:*")
            for asset, pnl, n, w in bottom3:
                lines.append(f"  {asset}: ${pnl:+.2f} ({w}/{n} wins)")

        if active_engines:
            lines.append(f"\n*Active engines:* {', '.join(active_engines)}")

        if excluded_with_acc or all_excluded:
            excl_list = excluded_with_acc if excluded_with_acc else list(all_excluded)
            lines.append(f"*Excluded:* {', '.join(excl_list)}")

        if dynamic_banned:
            lines.append(f"*Auto-banned assets:* {', '.join(dynamic_banned.keys())}")
        else:
            lines.append(f"*Auto-banned:* none")

        lines.append(f"\n*Recommendation:* {recommendation}")

        await self._tg.send_message("\n".join(lines))
        logger.info("weekly_diagnosis_sent", total_trades=total, win_rate=round(wr, 3))

    async def _lessons_pipeline_job(self) -> None:
        """Run the full Lessons Learned pipeline: aggregate → LLM → governance.

        Runs weekly. Scripts are mounted at /app/scripts via docker-compose volume.
        Skips silently if fewer than 15 trades are available.
        """
        import sys as _sys
        import json as _json
        import tempfile as _tmp

        # Scripts are volume-mounted at /app/scripts; add to path if not already there.
        _scripts = "/app/scripts"
        if _scripts not in _sys.path:
            _sys.path.insert(0, _scripts)

        ollama_base = os.getenv("OLLAMA_BASE_URL",   "http://host.docker.internal:11434")
        model       = os.getenv("OLLAMA_MODEL",       "qwen2.5:7b")
        fast_model  = os.getenv("OLLAMA_FAST_MODEL",  "qwen2.5:3b")

        # Initialize before try so finally block can always reference them.
        stats_path = ""
        lessons_path = ""

        try:
            from aggregate_stats import aggregate
            from lessons_learned import run as run_lessons
            from governance_engine import run as run_governance

            logger.info("lessons_pipeline_start")
            stats = await aggregate(60)
            count = stats.get("summary", {}).get("count", 0)
            if count < 15:
                logger.info("lessons_pipeline_skipped", reason=f"only {count} trades < 15 minimum")
                return

            with _tmp.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as sf:
                stats_path = sf.name
                _json.dump(stats, sf, default=str)

            with _tmp.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as lf:
                lessons_path = lf.name

            # run_lessons makes blocking urllib calls to Ollama (CPU inference can
            # take 10+ min per pass) — run in a worker thread so the event loop
            # keeps serving outcome collection, metrics, and Telegram jobs.
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: run_lessons(stats_path, lessons_path, model, ollama_base,
                                    fast_model=fast_model, use_fast=bool(fast_model)),
            )

            await run_governance(lessons_path, dry_run=False)
            logger.info("lessons_pipeline_complete", trades=count)

        except Exception as exc:
            logger.exception("lessons_pipeline_failed", error=str(exc))
            raise  # re-raise so _loop handles backoff
        finally:
            for p in (stats_path, lessons_path):
                try:
                    os.unlink(p)
                except Exception:
                    pass

    async def _claude_md_job(self) -> None:
        """Re-read architecture.md and alert if it changed."""
        path = pathlib.Path(CLAUDE_MD_PATH)
        if not path.exists():
            logger.debug("claude_md_not_found: %s", path)
            return

        if _claude_md_reads:
            _claude_md_reads.inc()

        content = path.read_bytes()
        new_hash = hashlib.sha256(content).hexdigest()

        if self._claude_md_hash is None:
            self._claude_md_hash = new_hash
            logger.info("claude_md_loaded", hash=new_hash[:12], path=str(path))
            return

        if new_hash != self._claude_md_hash:
            old = self._claude_md_hash
            self._claude_md_hash = new_hash
            if _claude_md_changes:
                _claude_md_changes.inc()

            logger.warning(
                "claude_md_changed",
                old_hash=old[:12],
                new_hash=new_hash[:12],
                path=str(path),
            )
            await self._tg.send_architecture_changed(old, new_hash)

            # Attempt to detect service count change
            text = content.decode("utf-8", errors="replace")
            port_count = text.count("port 80")
            logger.info("claude_md_port_count_estimate", count=port_count)
        else:
            logger.debug("claude_md_unchanged", hash=new_hash[:12])

    async def _meta_strategy_job(self) -> None:
        """Compute regime-aware engine weight multipliers and publish to Redis."""
        if os.getenv("META_STRATEGY_ENABLED", "false").lower() not in ("1", "true", "yes"):
            return
        from meta_strategy import MetaStrategySelector
        weights = await MetaStrategySelector(self._pool, self._redis).run_once()
        logger.info("meta_strategy_job_done", engines_weighted=len(weights))
