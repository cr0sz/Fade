from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.cache.redis_client import get_redis
from src.core.config import get_settings
from src.core.cost_control import init_cost_controller
from src.core.logging import (
    bind_cycle_context,
    bind_service_context,
    clear_cycle_context,
    configure_logging,
    get_logger,
)
from src.core.metrics import (
    ingestion_duplicates_total,
    ingestion_fetched_total,
    ingestion_new_total,
    job_duration_seconds,
    job_errors_total,
    job_runs_total,
)
from src.db.connection import get_db_pool
from src.engines.feedback_aggregator import FeedbackAggregator
from src.engines.institutional_confluence_engine import InstitutionalConfluenceEngine
from src.engines.institutional_flow_engine import InstitutionalFlowEngine
from src.engines.macro_regime_engine import MacroRegimeEngine
from src.engines.news_intelligence import NewsIntelligenceEngine
from src.engines.sentiment_engine import SentimentEngine
from src.engines.signal_evaluation_engine import SignalEvaluationEngine
from src.engines.smart_money import SmartMoneyEngine
from src.engines.trend_detection import TrendDetectionEngine
from src.engines.mean_reversion_engine import MeanReversionEngine
from src.providers.market.coingecko import CoinGeckoProvider
from src.providers.market.finnhub import FinnhubProvider
from src.scheduler.jobs.daily_brief import run_daily_brief
from src.scheduler.jobs.trading_journal import run_trading_journal
from src.scheduler.jobs.executive_report import run_executive_report

logger = get_logger(__name__)

_METRICS_PORT = 8009
_metrics_server_started = False

# Redis key that carries the latest news-ingestion cycle_id forward to
# signal-tracing jobs (trend_cycle, intelligence_cycle).  This ensures
# signal_trace.cycle_id == provider_ingestion_log.cycle_id for the same
# data batch, which is the invariant the pipeline integrity check relies on.
_LATEST_INGESTION_CYCLE_KEY = "news:latest_cycle_id"
_LATEST_INGESTION_CYCLE_TTL = 1800  # 30 min — survives missed cycles


# ── Embedded metrics HTTP server (same pattern as engines) ────────────────────
class _MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            data = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        elif self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):  # suppress default access log
        pass


def _start_metrics_server(port: int = _METRICS_PORT) -> None:
    global _metrics_server_started
    if _metrics_server_started:
        return
    _metrics_server_started = True
    server = HTTPServer(("0.0.0.0", port), _MetricsHandler)
    Thread(target=server.serve_forever, daemon=True).start()
    logger.info("worker_metrics_server_started", port=port)


class MarketIntelligenceWorker:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._scheduler = AsyncIOScheduler()
        self._db = None
        self._redis = None
        self._cost = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            logger.warning("worker_already_started")
            return
        self._started = True
        configure_logging()
        bind_service_context("worker")
        _start_metrics_server()
        self._db = await get_db_pool()
        self._redis = await get_redis()
        self._cost = await init_cost_controller(self._redis)

        logger.info("worker_starting")
        self._register_jobs()
        self._scheduler.start()

        # Run initial cycles immediately (L3 runs after L2 trend cycle)
        await self._run_news_cycle()
        await self._run_trend_cycle()
        await self._run_intelligence_cycle()

        logger.info("worker_running", jobs=len(self._scheduler.get_jobs()))

    def _register_jobs(self) -> None:
        s = self._settings

        self._scheduler.add_job(
            self._run_news_cycle,
            "interval",
            seconds=s.SCHEDULER_NEWS_INTERVAL,
            id="news_cycle",
            name="News Intelligence Cycle",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_sentiment_cycle,
            "interval",
            seconds=s.SCHEDULER_SOCIAL_INTERVAL,
            id="sentiment_cycle",
            name="Social Sentiment Cycle",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_smart_money_cycle,
            "interval",
            seconds=s.SCHEDULER_SMART_MONEY_INTERVAL,
            id="smart_money_cycle",
            name="Smart Money Cycle",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_trend_cycle,
            "interval",
            seconds=s.SCHEDULER_TREND_INTERVAL,
            id="trend_cycle",
            name="Trend Detection Cycle",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_intelligence_cycle,
            "interval",
            seconds=getattr(s, "SCHEDULER_INTELLIGENCE_INTERVAL", 900),
            id="intelligence_cycle",
            name="Level-3 Institutional Intelligence Cycle",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_mean_reversion_cycle,
            "interval",
            seconds=getattr(s, "SCHEDULER_MEANREV_INTERVAL", 1800),
            id="mean_reversion_cycle",
            name="Cross-Sectional Mean Reversion Cycle",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_ai_digest,
            "interval",
            seconds=s.SCHEDULER_AI_DIGEST_INTERVAL,
            id="ai_digest",
            name="AI Digest Generation",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_cleanup,
            "cron",
            hour=2,
            minute=0,
            id="cleanup",
            name="Daily Cleanup",
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_signal_evaluation_cycle,
            "interval",
            seconds=getattr(s, "SCHEDULER_EVAL_INTERVAL", 900),
            id="signal_evaluation_cycle",
            name="Signal Evaluation Cycle",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_feedback_aggregation_cycle,
            "interval",
            seconds=getattr(s, "SCHEDULER_FEEDBACK_INTERVAL", 3600),
            id="feedback_aggregation_cycle",
            name="Feedback Aggregation Cycle",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_market_data_cycle,
            "interval",
            seconds=getattr(s, "SCHEDULER_MARKET_DATA_INTERVAL", 300),
            id="market_data_cycle",
            name="Market Data Cycle (Crypto + BIST)",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_daily_brief,
            "cron",
            hour=7,
            minute=0,
            id="daily_brief",
            name="Daily Brief",
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_trading_journal,
            "cron",
            day_of_week="mon",
            hour=8,
            minute=0,
            id="trading_journal",
            name="Trading Journal",
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_executive_report,
            "cron",
            day_of_week="sun",
            hour=20,
            minute=0,
            id="executive_report",
            name="Weekly Executive Report",
            replace_existing=True,
        )

    async def _latest_ingestion_cycle_id(self) -> str:
        """Return the cycle_id from the most recent news ingestion, or a fresh UUID."""
        if self._redis:
            try:
                raw = await self._redis.get(_LATEST_INGESTION_CYCLE_KEY)
                if raw:
                    return raw.decode() if isinstance(raw, bytes) else str(raw)
            except Exception:
                pass
        return str(uuid.uuid4())

    async def _run_news_cycle(self) -> None:
        job = "news_cycle"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            engine = NewsIntelligenceEngine(self._db, redis=self._redis)
            result = await engine.run_cycle(cycle_id=cycle_id, job=job)

            # Score new articles immediately via Ollama (free/local, no budget gate)
            if result.new_items > 0:
                from src.providers.ai.ollama_provider import OllamaProvider
                try:
                    scored = await engine.batch_analyze(OllamaProvider())
                    if scored:
                        logger.info("news_realtime_scored", count=scored)
                except Exception as _score_exc:
                    logger.warning("news_realtime_score_failed", error=str(_score_exc))

            # Publish cycle_id so downstream signal-tracing jobs link their
            # signal_trace rows back to this provider_ingestion_log batch.
            if self._redis:
                try:
                    await self._redis.setex(
                        _LATEST_INGESTION_CYCLE_KEY,
                        _LATEST_INGESTION_CYCLE_TTL,
                        cycle_id,
                    )
                except Exception:
                    pass
            elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
            job_duration_seconds.labels(job=job).observe(elapsed_ms / 1000)
            ingestion_fetched_total.labels(job=job).inc(result.fetched)
            ingestion_new_total.labels(job=job).inc(result.new_items)
            ingestion_duplicates_total.labels(job=job, dedup_type="exact").inc(result.duplicates)
            ingestion_duplicates_total.labels(job=job, dedup_type="fuzzy").inc(result.fuzzy_dupes)
            ingestion_duplicates_total.labels(job=job, dedup_type="semantic").inc(result.semantic_dupes)
            logger.info(
                "job_complete",
                job=job,
                fetched=result.fetched,
                new=result.new_items,
                exact_dupes=result.duplicates,
                fuzzy_dupes=result.fuzzy_dupes,
                semantic_dupes=result.semantic_dupes,
                providers_ok=result.providers_ok,
                circuit_open=result.providers_circuit_open,
                total_latency_ms=round(result.total_latency_ms, 1),
                cycle_latency_ms=elapsed_ms,
            )
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_sentiment_cycle(self) -> None:
        job = "sentiment_cycle"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            engine = SentimentEngine(self._db)
            result = await engine.run_cycle()
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            ingestion_fetched_total.labels(job=job).inc(result.fetched)
            logger.info(
                "job_complete",
                job=job,
                fetched=result.fetched,
                hype=len(result.hype_signals),
                panic=len(result.panic_signals),
            )
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_smart_money_cycle(self) -> None:
        job = "smart_money_cycle"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            engine = SmartMoneyEngine(self._db, redis=self._redis)
            result = await engine.run_cycle()
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            logger.info(
                "job_complete",
                job=job,
                fetched=result.fetched,
                stored=result.stored,
            )
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_trend_cycle(self) -> None:
        job = "trend_cycle"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            signal_cycle_id = await self._latest_ingestion_cycle_id()
            engine = TrendDetectionEngine(self._db, redis=self._redis)
            result = await engine.run_cycle(cycle_id=signal_cycle_id)
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            logger.info("job_complete", job=job, **result)
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_mean_reversion_cycle(self) -> None:
        job = "mean_reversion_cycle"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            emitted = await MeanReversionEngine(self._db).run_cycle()
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            logger.info("job_complete", job=job, emitted=emitted)
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_intelligence_cycle(self) -> None:
        """Level-3 sequential pipeline: flow → macro → confluence."""
        job = "intelligence_cycle"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            signal_cycle_id = await self._latest_ingestion_cycle_id()
            flow_result = await InstitutionalFlowEngine(self._db, self._redis).run_cycle()
            regime_result = await MacroRegimeEngine(self._db, self._redis).run_cycle(cycle_id=signal_cycle_id)
            conf_result = await InstitutionalConfluenceEngine(self._db, self._redis).run_cycle(cycle_id=signal_cycle_id)
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            logger.info(
                "job_complete",
                job=job,
                flows_stored=flow_result.flows_stored,
                market_direction=flow_result.market_direction,
                market_strength=round(flow_result.market_strength, 1),
                macro_regime=regime_result.regime,
                regime_shift=regime_result.regime_shift,
                confluence_strength=round(
                    conf_result.market_signal.strength if conf_result.market_signal else 0, 1
                ),
                confluence_direction=(
                    conf_result.market_signal.direction if conf_result.market_signal else "mixed"
                ),
            )
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_ai_digest(self) -> None:
        job = "ai_digest"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            if not await self._cost.is_ai_allowed():
                logger.warning("ai_digest_skipped_budget")
                job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
                return

            news_engine = NewsIntelligenceEngine(self._db, redis=self._redis)
            from src.providers.ai import AI_PROVIDERS
            ai_provider_name = await self._cost.get_preferred_ai_provider()
            if ai_provider_name == "none":
                job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
                return

            ai_provider = AI_PROVIDERS[ai_provider_name]()
            count = await news_engine.batch_ai_summarize(ai_provider)
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            logger.info("job_complete", job=job, summaries=count)
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_cleanup(self) -> None:
        job = "cleanup"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            s = self._settings
            async with self._db.acquire() as conn:
                deleted_news = await conn.execute(
                    "DELETE FROM news WHERE published_at < now() - $1::interval",
                    f"{s.NEWS_RETENTION_DAYS} days",
                )
                deleted_social = await conn.execute(
                    "DELETE FROM social_posts WHERE created_at < now() - $1::interval",
                    f"{s.SOCIAL_RETENTION_DAYS} days",
                )
                quotes_retention = int(s.__dict__.get("QUOTES_RETENTION_DAYS", 30))
                deleted_quotes = await conn.execute(
                    f"DELETE FROM ticker_quotes WHERE ts < now() - interval '{quotes_retention} days'"
                )
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            logger.info(
                "job_complete",
                job=job,
                deleted_news=deleted_news,
                deleted_social=deleted_social,
                deleted_quotes=deleted_quotes,
            )
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_signal_evaluation_cycle(self) -> None:
        job = "signal_evaluation_cycle"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            engine = SignalEvaluationEngine(self._db, redis=self._redis)
            result = await engine.run_cycle(cycle_id=cycle_id)
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            logger.info(
                "job_complete",
                job=job,
                windows=result.windows_processed,
                written=result.outcomes_written,
                skipped=result.outcomes_skipped,
                errors=result.errors,
            )
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_feedback_aggregation_cycle(self) -> None:
        job = "feedback_aggregation_cycle"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            aggregator = FeedbackAggregator(self._db, redis=self._redis)
            result = await aggregator.run_cycle(cycle_id=cycle_id)
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            logger.info(
                "job_complete",
                job=job,
                engines_updated=result.engines_updated,
                providers_updated=result.providers_updated,
                errors=result.errors,
            )
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_market_data_cycle(self) -> None:
        """Fetch live prices: CoinGecko (crypto), yfinance (NASDAQ/NYSE/BIST), Finnhub (BIST fallback).

        Stores results in ticker_quotes via TickerRepository so TrendDetectionEngine
        picks up real volume/price data on its next cycle.
        """
        job = "market_data_cycle"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            from src.db.repositories import TickerRepository
            from src.providers.market.yfinance_provider import YFinanceProvider

            repo = TickerRepository(self._db)
            total = 0

            # ── Crypto via CoinGecko (free, no key) ───────────────────────────
            try:
                cg = CoinGeckoProvider()
                crypto_quotes = await cg.fetch()
                for q in crypto_quotes:
                    await repo.upsert_quote(q)
                total += len(crypto_quotes)
                logger.info("market_data_crypto", count=len(crypto_quotes))
            except Exception as exc:
                logger.error("market_data_crypto_error", error=str(exc))

            # ── NASDAQ / NYSE / BIST via yfinance (free, no key) ──────────────
            # Outside US market hours, daily candles don't update — skip yfinance
            # every other cycle (effective interval = 10 min) to avoid rate-limiting.
            # During market hours (13:30-20:00 UTC), always fetch for live prices.
            _yf_market_open = YFinanceProvider._us_market_open()
            _yf_skip = not _yf_market_open and (int(time.time()) // 300) % 2 == 0
            if not _yf_skip:
                try:
                    yf_prov = YFinanceProvider()
                    yf_quotes = await yf_prov.fetch()
                    for q in yf_quotes:
                        await repo.upsert_quote(q)
                    total += len(yf_quotes)
                    logger.info("market_data_yfinance", count=len(yf_quotes))
                except Exception as exc:
                    logger.error("market_data_yfinance_error", error=str(exc))
            else:
                logger.debug("market_data_yfinance_skipped", reason="off_hours_rate_limit_protection")

            # ── BIST via Finnhub (only if API key set — more reliable than yfinance for .IS) ──
            try:
                s = self._settings
                if s.FINNHUB_API_KEY:
                    fh = FinnhubProvider()
                    bist_quotes = await fh.fetch(symbols=s.DEFAULT_BIST_WATCHLIST)
                    for q in bist_quotes:
                        await repo.upsert_quote(q)
                    total += len(bist_quotes)
                    logger.info("market_data_bist_finnhub", count=len(bist_quotes))
            except Exception as exc:
                logger.error("market_data_bist_error", error=str(exc))

            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            logger.info("job_complete", job=job, quotes_stored=total)
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_daily_brief(self) -> None:
        job = "daily_brief"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            await run_daily_brief(self._db, self._settings)
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            logger.info("job_complete", job=job)
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_trading_journal(self) -> None:
        job = "trading_journal"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            await run_trading_journal(self._db, self._settings)
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            logger.info("job_complete", job=job)
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def _run_executive_report(self) -> None:
        job = "executive_report"
        cycle_id = str(uuid.uuid4())
        bind_cycle_context(cycle_id, job)
        job_runs_total.labels(job=job).inc()
        t0 = time.monotonic()
        try:
            logger.info("job_start", job=job)
            await run_executive_report(self._db, self._settings, self._redis)
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            logger.info("job_complete", job=job)
        except Exception as exc:
            job_duration_seconds.labels(job=job).observe(time.monotonic() - t0)
            job_errors_total.labels(job=job).inc()
            logger.error("job_error", job=job, error=str(exc))
        finally:
            clear_cycle_context()

    async def stop(self) -> None:
        self._scheduler.shutdown(wait=False)
        logger.info("worker_stopped")


async def main() -> None:
    worker = MarketIntelligenceWorker()
    await worker.start()

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, asyncio.CancelledError):
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
