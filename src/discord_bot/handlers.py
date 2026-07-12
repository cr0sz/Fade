"""Discord slash-command handlers — ported from src/telegram/handlers.py.

All business logic (DB queries, engine calls) is identical to the Telegram
version.  Only the receive/send I/O layer changes.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import asyncpg
import discord
from discord import app_commands

import redis.asyncio as aioredis

from src.core.config import get_settings
from src.core.cost_control import CostController
from src.core.logging import get_logger
from src.db.repositories import NewsRepository
from src.engines.ai_analyst import AIAnalystEngine
from src.engines.institutional_confluence_engine import InstitutionalConfluenceEngine
from src.engines.institutional_flow_engine import InstitutionalFlowEngine
from src.engines.macro_regime_engine import MacroRegimeEngine
from src.engines.narrative_engine import NarrativeEngine
from src.engines.sentiment_engine import SentimentEngine
from src.engines.trend_detection import TrendDetectionEngine
from src.providers.market.finnhub import FinnhubProvider

logger = get_logger(__name__)

DISCLAIMER = "\n*Not financial advice — system health only*"


def _admin_id() -> int:
    return int(os.getenv("DISCORD_ADMIN_USER_ID", "0"))


def _send(text: str) -> dict:
    """Return kwargs for interaction.followup.send based on text length."""
    if len(text) <= 2000:
        return {"content": text}
    return {"embed": discord.Embed(description=text[:4096], color=0x3498DB)}


def register_commands(
    tree: app_commands.CommandTree,
    db: asyncpg.Pool,
    redis: aioredis.Redis,
    cost: CostController,
) -> None:
    settings = get_settings()

    # ── /help ─────────────────────────────────────────────────────────────────

    @tree.command(name="help", description="Show all available commands")
    async def cmd_help(interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        text = (
            "🤖 **Market Intelligence Bot**\n\n"
            "📊 Market:\n"
            "/market — Full market digest + regime\n"
            "/us — US equities overview\n"
            "/bist — Borsa Istanbul overview\n"
            "/hot — Trending tickers + confidence\n"
            "/news — Latest market news\n"
            "/macro — Macro indicators\n"
            "/watch `AAPL` — Deep ticker analysis\n"
            "/flows — Institutional flow intelligence\n\n"
            "💼 Trading:\n"
            "/positions — Open paper positions\n"
            "/pnl — P&L summary + win rate\n"
            "/stats — Win rate by exchange, direction + best/worst assets\n"
            "/router — Truth Router edge table + routed trade results\n"
            "/cide — Capital injection readiness\n"
            "/engine_health — Crypto engine accuracy\n"
            "/regime — Current regime + governor\n"
            "/cluster — Per-cluster edge + WR\n"
            "/kill — Toggle trading kill switch (admin)\n\n"
            "⚙️ System:\n"
            "/pipeline — Pipeline health status\n"
            "/cost — API cost status\n\n"
            "⚠️ _Not financial advice._"
        )
        await interaction.followup.send(**_send(text))

    # ── /market ───────────────────────────────────────────────────────────────

    @tree.command(name="market", description="Full market digest + macro regime")
    async def cmd_market(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        try:
            trend_engine      = TrendDetectionEngine(db, redis)
            ai_engine         = AIAnalystEngine(db, redis, cost)
            news_repo         = NewsRepository(db)
            narrative_engine  = NarrativeEngine(db, redis)
            regime_engine     = MacroRegimeEngine(db, redis)
            confluence_engine = InstitutionalConfluenceEngine(db, redis)
            flow_engine       = InstitutionalFlowEngine(db, redis)

            trends         = await trend_engine.get_hot_tickers(limit=10)
            headlines      = [n["title"] for n in await news_repo.get_recent(hours=24, limit=15)]
            top_narratives = await narrative_engine.get_top_narratives(limit=3)
            current_regime = await regime_engine.get_current_regime()
            market_conf    = await confluence_engine.get_market_confluence()
            market_flow    = await flow_engine.get_market_flow()

            if top_narratives:
                digest = await ai_engine.generate_narrative_digest(top_narratives, headlines)
            else:
                digest = await ai_engine.generate_market_digest(headlines, trends)

            hot_tickers = " ".join(f"`{t['ticker']}`" for t in trends[:5])

            narrative_line = ""
            if top_narratives:
                top = top_narratives[0]
                momentum_arrow = {"accelerating": "↑", "decelerating": "↓"}.get(
                    top.get("momentum", ""), "→"
                )
                narrative_line = (
                    f"\n🏷 Theme: _{top.get('label', '')}_  "
                    f"strength: {top.get('strength_score', 0):.2f} {momentum_arrow}\n"
                )

            regime_line = ""
            if current_regime:
                regime_name = current_regime.get("regime", "unknown").replace("_", " ").title()
                regime_conf = float(current_regime.get("confidence") or 0)
                shift_tag   = " 🚨 SHIFT" if current_regime.get("regime_shift") else ""
                regime_line = f"🌍 Regime: **{regime_name}** {regime_conf:.0%}{shift_tag}\n"

            intel_line = ""
            if market_conf and market_flow:
                csig_str = float(market_conf.get("strength") or 0)
                csig_dir = market_conf.get("direction", "mixed")
                flow_dir = market_flow.get("direction", "neutral")
                flow_str = float(market_flow.get("flow_strength") or 0)
                _flow_arrow = {"inflow": "🔵", "outflow": "🔴", "neutral": "⚪"}
                intel_line = (
                    f"🔄 Confluence: {csig_str:.0f}/100 [{csig_dir}] "
                    f"{_flow_arrow.get(flow_dir, '⚪')} Flow: {flow_dir} {flow_str:.0f}/100\n"
                )

            text = (
                f"📊 **MARKET DIGEST**\n\n"
                f"{digest}"
                f"{narrative_line}"
                f"{regime_line}"
                f"{intel_line}\n"
                f"🔥 Hot: {hot_tickers}"
                f"{DISCLAIMER}"
            )
        except Exception as exc:
            logger.error("discord_market_error", error=str(exc))
            text = "❌ Error generating digest. Try again later."

        await interaction.followup.send(**_send(text))

    # ── /us ───────────────────────────────────────────────────────────────────

    @tree.command(name="us", description="US equities overview")
    async def cmd_us(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        provider = FinnhubProvider()
        quotes = await provider.fetch(symbols=settings.DEFAULT_US_WATCHLIST[:8])

        lines = ["🇺🇸 **US MARKETS**\n"]
        for q in quotes:
            arrow = "🟢" if q.change_pct >= 0 else "🔴"
            sign = "+" if q.change_pct >= 0 else ""
            lines.append(
                f"{arrow} `{q.symbol}` ${q.price:.2f} "
                f"({sign}{q.change_pct:.2f}%)"
            )

        lines.append(DISCLAIMER)
        await interaction.followup.send(**_send("\n".join(lines)))

    # ── /bist ─────────────────────────────────────────────────────────────────

    @tree.command(name="bist", description="Borsa Istanbul overview")
    async def cmd_bist(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        provider = FinnhubProvider()
        quotes = await provider.fetch(symbols=settings.DEFAULT_BIST_WATCHLIST[:8])

        lines = ["🇹🇷 **BORSA İSTANBUL**\n"]
        for q in quotes:
            arrow = "🟢" if q.change_pct >= 0 else "🔴"
            sign = "+" if q.change_pct >= 0 else ""
            symbol = q.symbol.replace(".IS", "")
            lines.append(
                f"{arrow} `{symbol}` ₺{q.price:.2f} "
                f"({sign}{q.change_pct:.2f}%)"
            )

        lines.append(DISCLAIMER)
        await interaction.followup.send(**_send("\n".join(lines)))

    # ── /hot ──────────────────────────────────────────────────────────────────

    @tree.command(name="hot", description="Trending tickers + confidence")
    async def cmd_hot(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        trend_engine = TrendDetectionEngine(db, redis)
        trends = await trend_engine.get_hot_tickers(limit=15)
        signals = await trend_engine.get_signals()

        _conf_badge = {"high": "🟢", "medium": "🟡", "low": "🔴"}

        lines = ["🔥 **TRENDING TICKERS**\n"]
        for i, t in enumerate(trends[:10], 1):
            score = t.get("composite_score", 0)
            conf = t.get("confidence_level") or "low"
            narrative = t.get("narrative_cluster") or ""
            narrative_tag = f" _{narrative.replace('_', ' ')}_" if narrative else ""
            hype = "⚡" if t.get("hype_signal") else ""
            panic = "💥" if t.get("panic_signal") else ""
            lines.append(
                f"{i}. `{t['ticker']}` {score:.2f} "
                f"{_conf_badge.get(conf, '⚪')}{hype}{panic}{narrative_tag}"
            )

        if signals.get("hype_signals"):
            hype_tickers = " ".join(f"`{t}`" for t in signals["hype_signals"][:5])
            lines.append(f"\n⚡ Hype: {hype_tickers}")

        if signals.get("panic_signals"):
            panic_tickers = " ".join(f"`{t}`" for t in signals["panic_signals"][:5])
            lines.append(f"💥 Panic: {panic_tickers}")

        lines.append(DISCLAIMER)
        await interaction.followup.send(**_send("\n".join(lines)))

    # ── /news ─────────────────────────────────────────────────────────────────

    @tree.command(name="news", description="Latest market news (last 12 hours)")
    async def cmd_news(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        news_repo = NewsRepository(db)
        items = await news_repo.get_recent(hours=12, limit=8)

        lines = ["📰 **LATEST MARKET NEWS**\n"]
        for n in items:
            title = n["title"][:80]
            source = n.get("source", "")
            lines.append(f"• {title}\n  _{source}_\n")

        if not items:
            lines.append("No recent news found.")

        lines.append(DISCLAIMER)
        await interaction.followup.send(**_send("\n".join(lines)))

    # ── /macro ────────────────────────────────────────────────────────────────

    @tree.command(name="macro", description="Macro indicators")
    async def cmd_macro(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        provider = FinnhubProvider()
        indicators = await provider.fetch_macro_indicators()

        lines = ["🌍 **MACRO INDICATORS**\n"]
        if indicators:
            for ind in indicators[:8]:
                name = ind.name[:40]
                val = f"{ind.value:.2f}"
                lines.append(f"• {name}: `{val}`")
        else:
            lines.append("Macro data unavailable — check FINNHUB_API_KEY.")

        lines.append(DISCLAIMER)
        await interaction.followup.send(**_send("\n".join(lines)))

    # ── /watch ────────────────────────────────────────────────────────────────

    @tree.command(name="watch", description="Deep ticker analysis")
    @app_commands.describe(ticker="Ticker symbol e.g. BTC, AAPL")
    async def cmd_watch(interaction: discord.Interaction, ticker: str) -> None:
        await interaction.response.defer()

        ticker = ticker.upper()

        _conf_badge = {"high": "🟢", "medium": "🟡", "low": "🔴"}

        try:
            provider = FinnhubProvider()
            quotes = await provider.fetch(symbols=[ticker])

            sentiment_engine = SentimentEngine(db)
            sentiment = await sentiment_engine.get_sentiment_summary(ticker, hours=24)

            news_repo = NewsRepository(db)
            news = await news_repo.get_recent(hours=24, ticker=ticker, limit=5)

            async with db.acquire() as conn:
                trend_row = await conn.fetchrow(
                    """SELECT confidence_score, confidence_level, narrative_cluster,
                              signal_quality, smoothed_composite
                       FROM market_trends WHERE ticker = $1""",
                    ticker,
                )
            conf_level = (trend_row["confidence_level"] if trend_row else None) or "low"
            narrative = (trend_row["narrative_cluster"] if trend_row else None)
            signal_quality = (trend_row["signal_quality"] if trend_row else None) or "low"
            smoothed = trend_row["smoothed_composite"] if trend_row else None

            context_parts = []
            if quotes:
                q = quotes[0]
                context_parts.append(
                    f"Price: ${q.price:.2f} ({'+' if q.change_pct >= 0 else ''}{q.change_pct:.2f}%)"
                )
            context_parts.append(
                f"Social sentiment: {sentiment.avg_sentiment:.2f} "
                f"({sentiment.mention_count} mentions in 24h, velocity: {sentiment.velocity_ratio:.1f}x)"
            )
            if smoothed is not None:
                context_parts.append(f"Smoothed composite trend score: {smoothed:.3f}")
            if news:
                context_parts.append(
                    "Recent headlines: " + " | ".join(n["title"][:60] for n in news[:3])
                )

            ai_engine = AIAnalystEngine(db, redis, cost)
            analysis = await ai_engine.analyze_ticker_with_confidence(
                ticker,
                "\n".join(context_parts),
                confidence_level=conf_level,
                narrative_cluster=narrative,
                signal_quality=signal_quality,
            )

            quote_line = ""
            if quotes:
                q = quotes[0]
                arrow = "🟢" if q.change_pct >= 0 else "🔴"
                sign = "+" if q.change_pct >= 0 else ""
                quote_line = f"{arrow} ${q.price:.2f} ({sign}{q.change_pct:.2f}%)\n"

            sent_emoji = (
                "📈" if sentiment.avg_sentiment > 0.2
                else ("📉" if sentiment.avg_sentiment < -0.2 else "➡️")
            )
            hype_tag = " ⚡HYPE" if sentiment.hype_signal else ""
            panic_tag = " 💥PANIC" if sentiment.panic_signal else ""

            conf_line = (
                f"{_conf_badge.get(conf_level, '⚪')} Confidence: {conf_level.upper()} "
                f"| Quality: {signal_quality.upper()}\n"
            )
            narrative_line = (
                f"🏷 Narrative: _{narrative.replace('_', ' ').title()}_\n"
                if narrative else ""
            )

            flow_engine       = InstitutionalFlowEngine(db, redis)
            confluence_engine = InstitutionalConfluenceEngine(db, redis)
            regime_engine     = MacroRegimeEngine(db, redis)

            ticker_flow       = await flow_engine.get_ticker_flow(ticker)
            ticker_confluence = await confluence_engine.get_ticker_confluence(ticker)
            current_regime    = await regime_engine.get_current_regime()

            flow_line = ""
            if ticker_flow:
                flow_dir   = ticker_flow.get("direction", "neutral")
                flow_str   = float(ticker_flow.get("flow_strength") or 0)
                flow_accel = ticker_flow.get("acceleration", "stable")
                insider_cl = ticker_flow.get("insider_cluster", "neutral")
                _flow_arrow = {"inflow": "🔵", "outflow": "🔴", "neutral": "⚪"}
                flow_line = (
                    f"{_flow_arrow.get(flow_dir, '⚪')} Flow: "
                    f"{flow_dir.upper()} {flow_str:.0f}/100 "
                    f"({flow_accel}) insider: {insider_cl}\n"
                )

            conf_signal_line = ""
            if ticker_confluence:
                csig_str = float(ticker_confluence.get("strength") or 0)
                csig_dir = ticker_confluence.get("direction", "mixed")
                conf_signal_line = f"🔄 Confluence: {csig_str:.0f}/100 [{csig_dir}]\n"

            regime_line = ""
            if current_regime:
                regime_name = current_regime.get("regime", "unknown").replace("_", " ").title()
                regime_conf = float(current_regime.get("confidence") or 0)
                shift_tag   = " 🚨" if current_regime.get("regime_shift") else ""
                regime_line = f"🌍 Regime: _{regime_name}_ {regime_conf:.0%}{shift_tag}\n"

            text = (
                f"📊 **{ticker}**\n"
                f"{quote_line}\n"
                f"{sent_emoji} Sentiment: {sentiment.avg_sentiment:.2f}"
                f"{hype_tag}{panic_tag}\n"
                f"💬 Mentions: {sentiment.mention_count} "
                f"(velocity: {sentiment.velocity_ratio:.1f}x)\n"
                f"{conf_line}"
                f"{narrative_line}"
                f"{flow_line}"
                f"{conf_signal_line}"
                f"{regime_line}\n"
                f"🤖 **AI Analysis:**\n{analysis}"
            )

        except Exception as exc:
            logger.error("discord_watch_error", ticker=ticker, error=str(exc))
            text = f"❌ Error analyzing `{ticker}`. Try again later."

        await interaction.followup.send(**_send(text))

    # ── /flows ────────────────────────────────────────────────────────────────

    @tree.command(name="flows", description="Institutional flow intelligence dashboard")
    async def cmd_flows(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        try:
            regime_engine     = MacroRegimeEngine(db, redis)
            flow_engine       = InstitutionalFlowEngine(db, redis)
            confluence_engine = InstitutionalConfluenceEngine(db, redis)
            ai_engine         = AIAnalystEngine(db, redis, cost)

            current_regime = await regime_engine.get_current_regime()
            market_flow    = await flow_engine.get_market_flow()
            latest_flows   = await flow_engine.get_latest_flows(limit=5)
            market_conf    = await confluence_engine.get_market_confluence()

            _regime_emoji = {
                "risk_on":            "🟢",
                "liquidity_expansion": "🟢",
                "risk_off":           "🔴",
                "tightening":         "🟠",
                "volatility_regime":  "🟣",
            }
            regime_block = "No regime data yet."
            if current_regime:
                rname = current_regime.get("regime", "unknown")
                rconf = float(current_regime.get("confidence") or 0)
                shift = " 🚨 **SHIFT DETECTED**" if current_regime.get("regime_shift") else ""
                emoji = _regime_emoji.get(rname, "⚪")
                regime_block = (
                    f"{emoji} **{rname.replace('_', ' ').upper()}**"
                    f" {rconf:.0%}{shift}"
                )

            _flow_arrow = {"inflow": "🔵", "outflow": "🔴", "neutral": "⚪"}
            flow_block = "No flow data yet."
            if market_flow:
                fdir   = market_flow.get("direction", "neutral")
                fstr   = float(market_flow.get("flow_strength") or 0)
                faccel = market_flow.get("acceleration", "stable")
                flow_block = (
                    f"{_flow_arrow.get(fdir, '⚪')} Market: {fdir.upper()} "
                    f"strength {fstr:.0f}/100 ({faccel})"
                )

            conf_block = ""
            if market_conf:
                csig_str = float(market_conf.get("strength") or 0)
                csig_dir = market_conf.get("direction", "mixed")
                sources  = market_conf.get("contributing_sources") or {}
                top_narr = str(sources.get("top_narrative", "—")).replace("_", " ")
                conf_block = (
                    f"🔄 Confluence: **{csig_str:.0f}/100** [{csig_dir}]\n"
                    f"   Narrative driver: _{top_narr}_"
                )

            ticker_lines = []
            for f in [x for x in latest_flows if x.get("ticker") != "MARKET"][:5]:
                t    = f.get("ticker", "")
                dir_ = f.get("direction", "neutral")
                str_ = float(f.get("flow_strength") or 0)
                acc  = f.get("acceleration", "stable")
                ins  = f.get("insider_cluster", "neutral")
                ticker_lines.append(
                    f"  {_flow_arrow.get(dir_, '⚪')} `{t}` {str_:.0f} "
                    f"({acc}) {ins}"
                )

            brief = await ai_engine.generate_institutional_brief(
                regime=current_regime,
                confluence=market_conf,
                market_flow=market_flow,
            )

            text = (
                f"🏦 **INSTITUTIONAL FLOW INTELLIGENCE**\n\n"
                f"**Macro Regime:**\n{regime_block}\n\n"
                f"**Capital Flow:**\n{flow_block}\n\n"
                + (f"{conf_block}\n\n" if conf_block else "")
                + (
                    "**Top Ticker Flows:**\n" + "\n".join(ticker_lines) + "\n\n"
                    if ticker_lines else ""
                )
                + f"🤖 **Institutional Brief:**\n{brief}"
                + DISCLAIMER
            )

        except Exception as exc:
            logger.error("discord_flows_error", error=str(exc))
            text = "❌ Error fetching institutional intelligence. Try again later."

        await interaction.followup.send(**_send(text))

    # ── /pipeline ─────────────────────────────────────────────────────────────

    @tree.command(name="pipeline", description="End-to-end pipeline health status")
    async def cmd_pipeline(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        try:
            async with db.acquire() as conn:
                prov_row = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(DISTINCT provider)               AS active_providers,
                        SUM(errors)                            AS total_errors,
                        MAX(ts)                                AS last_ingestion
                    FROM provider_ingestion_log
                    WHERE ts > now() - interval '2 hours'
                    """
                )
                sig_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM signal_trace WHERE created_at > now() - interval '24 hours'"
                )
                eval_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM signal_feedback WHERE outcome_type != 'pending'"
                )
                fb_count = await conn.fetchval(
                    """
                    SELECT COUNT(*) FROM signal_feedback
                    WHERE outcome_type != 'pending'
                      AND evaluated_at > now() - interval '7 days'
                    """
                )
                cycle_row = await conn.fetchrow(
                    "SELECT cycle_id FROM provider_ingestion_log ORDER BY ts DESC LIMIT 1"
                )
                complete_count = await conn.fetchval(
                    """
                    WITH i AS (SELECT DISTINCT cycle_id FROM provider_ingestion_log WHERE ts > now() - interval '48 hours'),
                         s AS (SELECT DISTINCT cycle_id FROM signal_trace WHERE created_at > now() - interval '48 hours'),
                         f AS (SELECT DISTINCT cycle_id FROM signal_feedback WHERE evaluated_at > now() - interval '72 hours' AND outcome_type != 'pending')
                    SELECT COUNT(DISTINCT i.cycle_id)
                    FROM i
                    JOIN s USING (cycle_id)
                    JOIN f USING (cycle_id)
                    """
                )

            active_prov = int(prov_row["active_providers"] or 0)
            errors      = int(prov_row["total_errors"]     or 0)
            signals     = int(sig_count  or 0)
            evals       = int(eval_count or 0)
            feedbacks   = int(fb_count   or 0)
            complete    = int(complete_count or 0)
            cycle_id    = str(cycle_row["cycle_id"]) if cycle_row else "none"

            def _status(ok: bool) -> str:
                return "OK" if ok else "WARN"

            prov_ok = active_prov > 0
            sig_ok  = signals > 0
            eval_ok = evals > 0

            overall      = "PASS" if (prov_ok and sig_ok) else "WARN"
            health_emoji = "✅" if overall == "PASS" else "⚠️"

            prov_detail = f"{active_prov} active" + (f", {errors} errors" if errors else "")
            sig_detail  = f"{signals} in 24h"
            eval_detail = f"{evals} completed"
            fb_detail   = f"{feedbacks} in 7d"
            cycle_short = cycle_id[:8] + "..." if len(cycle_id) > 8 else cycle_id

            text = (
                f"📊 **PIPELINE STATUS**\n\n"
                f"Providers: **{_status(prov_ok)}** ({prov_detail})\n"
                f"Signals: **{_status(sig_ok)}** ({sig_detail})\n"
                f"Evaluations: **{_status(eval_ok)}** ({eval_detail})\n"
                f"Feedback: **OK** ({fb_detail})\n"
                f"Complete Cycles (48h): **{complete}**\n\n"
                f"Latest Cycle:\n`{cycle_short}`\n\n"
                f"Pipeline Health:\n"
                f"{health_emoji} **{overall}**"
            )
        except Exception as exc:
            logger.error("discord_pipeline_error", error=str(exc))
            text = "❌ Error querying pipeline status. Check DB connection."

        await interaction.followup.send(**_send(text))

    # ── /cost (admin) ─────────────────────────────────────────────────────────

    @tree.command(name="cost", description="[ADMIN] API cost status")
    async def cmd_cost(interaction: discord.Interaction) -> None:
        admin_id = _admin_id()
        if admin_id and interaction.user.id != admin_id:
            await interaction.response.send_message("❌ Admin only.", ephemeral=True)
            return
        await interaction.response.defer()

        ai_engine = AIAnalystEngine(db, redis, cost)
        summary = await ai_engine.get_cost_summary()

        text = (
            f"💰 **COST STATUS**\n\n"
            f"Today: `${summary['daily_total_usd']:.4f}`\n"
            f"Budget: `${summary['daily_budget_usd']:.2f}`\n"
            f"Used: `{summary['used_pct']:.1f}%`\n"
            f"Remaining: `${summary['remaining_usd']:.4f}`\n"
            f"Status: `{summary['status']}`"
        )
        await interaction.followup.send(**_send(text))

    # ── /positions ────────────────────────────────────────────────────────────

    @tree.command(name="positions", description="Open paper/live positions")
    async def cmd_positions(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        try:
            async with db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT asset, exchange, direction, entry_price,
                           notional, size, opened_at, mode
                      FROM paper_trades
                     WHERE closed_at IS NULL
                     ORDER BY opened_at DESC
                     LIMIT 20
                    """
                )
                balance_row = await conn.fetchrow(
                    """
                    SELECT COALESCE(SUM(pnl), 0) AS realized
                      FROM paper_trades
                     WHERE closed_at IS NOT NULL
                    """
                )

            if not rows:
                await interaction.followup.send(content="📭 **No open positions.**")
                return

            lines = [f"📂 **OPEN POSITIONS** ({len(rows)})\n"]
            for r in rows:
                arrow = "🟢" if r["direction"] == "long" else "🔴"
                opened = r["opened_at"]
                if opened.tzinfo is None:
                    opened = opened.replace(tzinfo=timezone.utc)
                age_h = round((datetime.now(timezone.utc) - opened).total_seconds() / 3600, 1)
                lines.append(
                    f"{arrow} `{r['asset']}` {r['direction'].upper()}\n"
                    f"   Entry: `{r['entry_price']:.4f}` · Size: `{r['size']:.4f}`\n"
                    f"   Notional: `${r['notional']:.2f}` · Age: `{age_h}h`"
                )

            realized = float(balance_row["realized"] or 0)
            sign = "+" if realized >= 0 else ""
            lines.append(f"\n💼 Realized PnL: `{sign}{realized:.2f} USD`")

            await interaction.followup.send(**_send("\n".join(lines)))

        except Exception as exc:
            logger.error("discord_positions_error", error=str(exc))
            await interaction.followup.send(content="❌ Error fetching positions.")

    # ── /pnl ──────────────────────────────────────────────────────────────────

    @tree.command(name="pnl", description="P&L summary + win rate for closed trades")
    async def cmd_pnl(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        try:
            async with db.acquire() as conn:
                stats = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE close_reason != 'dedup_cleanup')                                        AS total_trades,
                        COUNT(*) FILTER (WHERE pnl > 0  AND close_reason != 'dedup_cleanup')                          AS wins,
                        COUNT(*) FILTER (WHERE pnl < 0  AND close_reason != 'dedup_cleanup')                          AS losses,
                        COALESCE(SUM(pnl)  FILTER (WHERE close_reason != 'dedup_cleanup'), 0)                         AS total_pnl,
                        COALESCE(AVG(pnl)  FILTER (WHERE pnl > 0 AND close_reason != 'dedup_cleanup'), 0)             AS avg_win,
                        COALESCE(AVG(pnl)  FILTER (WHERE pnl < 0 AND close_reason != 'dedup_cleanup'), 0)             AS avg_loss,
                        COALESCE(SUM(pnl)  FILTER (
                            WHERE closed_at > now() - interval '24 hours'
                              AND close_reason != 'dedup_cleanup'), 0)                                                 AS pnl_24h
                      FROM paper_trades
                     WHERE closed_at IS NOT NULL
                    """
                )
                recent = await conn.fetch(
                    """
                    SELECT asset, direction, pnl, entry_price, exit_price, close_reason
                      FROM paper_trades
                     WHERE closed_at IS NOT NULL
                     ORDER BY closed_at DESC
                     LIMIT 5
                    """
                )
                live_stats = await conn.fetchrow(
                    """
                    SELECT COUNT(*) AS n,
                           COUNT(*) FILTER (WHERE pnl > 0) AS wins,
                           COALESCE(SUM(pnl), 0) AS total_pnl
                      FROM paper_trades
                     WHERE closed_at IS NOT NULL AND mode = 'live'
                       AND close_reason != 'dedup_cleanup'
                    """
                )

            total    = int(stats["total_trades"] or 0)
            wins     = int(stats["wins"] or 0)
            losses   = int(stats["losses"] or 0)
            breakeven = total - wins - losses
            win_rate  = (wins / total * 100) if total else 0.0
            total_pnl = float(stats["total_pnl"] or 0)
            avg_win   = float(stats["avg_win"] or 0)
            avg_loss  = float(stats["avg_loss"] or 0)
            pnl_24h   = float(stats["pnl_24h"] or 0)

            pnl_emoji = "📈" if total_pnl >= 0 else "📉"
            sign      = "+" if total_pnl >= 0 else ""
            sign_24h  = "+" if pnl_24h >= 0 else ""

            wl_str = f"{wins}W / {losses}L"
            if breakeven > 0:
                wl_str += f" / {breakeven}B"

            lines = [
                f"{pnl_emoji} **PAPER TRADING P&L**\n",
                f"Total trades: `{total}` ({wl_str})",
                f"Win rate: `{win_rate:.1f}%`",
                f"Total PnL: `{sign}{total_pnl:.2f} USD`",
                f"24h PnL: `{sign_24h}{pnl_24h:.2f} USD`",
                f"Avg win: `+{avg_win:.2f}` · Avg loss: `{avg_loss:.2f}`",
            ]

            live_n = int(live_stats["n"] or 0)
            if live_n:
                live_pnl  = float(live_stats["total_pnl"] or 0)
                live_wins = int(live_stats["wins"] or 0)
                live_sign = "+" if live_pnl >= 0 else ""
                lines.append(
                    f"\n💵 **LIVE (real money)**: `{live_n}` trades, "
                    f"`{live_wins}W`, PnL `{live_sign}{live_pnl:.2f} USD`"
                )

            if recent:
                lines.append("\n📋 **Recent closes:**")
                for r in recent:
                    p = float(r["pnl"] or 0)
                    emoji = "✅" if p >= 0 else "❌"
                    psign = "+" if p >= 0 else ""
                    lines.append(
                        f"{emoji} `{r['asset']}` {r['direction']} "
                        f"`{psign}{p:.2f}` _{r['close_reason'] or ''}_"
                    )

            await interaction.followup.send(**_send("\n".join(lines)))

        except Exception as exc:
            logger.error("discord_pnl_error", error=str(exc))
            await interaction.followup.send(content="❌ Error fetching P&L.")

    # ── /stats ────────────────────────────────────────────────────────────────

    @tree.command(name="stats", description="Trading stats by exchange, direction, best/worst assets")
    async def cmd_stats(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        try:
            async with db.acquire() as conn:
                by_exchange = await conn.fetch(
                    """
                    SELECT exchange,
                           COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE pnl > 0) AS wins,
                           COALESCE(SUM(pnl), 0) AS total_pnl
                      FROM paper_trades
                     WHERE closed_at IS NOT NULL
                       AND close_reason != 'dedup_cleanup'
                     GROUP BY exchange
                     ORDER BY total DESC
                    """
                )
                by_direction = await conn.fetch(
                    """
                    SELECT direction,
                           COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE pnl > 0) AS wins,
                           COALESCE(SUM(pnl), 0) AS total_pnl
                      FROM paper_trades
                     WHERE closed_at IS NOT NULL
                       AND close_reason != 'dedup_cleanup'
                     GROUP BY direction
                     ORDER BY total DESC
                    """
                )
                by_asset = await conn.fetch(
                    """
                    SELECT asset,
                           COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE pnl > 0) AS wins,
                           COALESCE(SUM(pnl), 0) AS total_pnl
                      FROM paper_trades
                     WHERE closed_at IS NOT NULL
                       AND close_reason != 'dedup_cleanup'
                     GROUP BY asset
                     ORDER BY total_pnl DESC
                    """
                )

            lines = ["📊 **TRADING STATS**\n"]

            if by_exchange:
                lines.append("**By Exchange:**")
                for r in by_exchange:
                    total = int(r["total"])
                    wins  = int(r["wins"])
                    wr    = wins / total * 100 if total else 0
                    pnl   = float(r["total_pnl"])
                    sign  = "+" if pnl >= 0 else ""
                    exch  = r["exchange"] or "UNKNOWN"
                    lines.append(
                        f"  `{exch}` {total}T {wr:.0f}% WR  PnL: `{sign}{pnl:.2f}`"
                    )
                lines.append("")

            if by_direction:
                lines.append("**By Direction:**")
                for r in by_direction:
                    total = int(r["total"])
                    wins  = int(r["wins"])
                    wr    = wins / total * 100 if total else 0
                    pnl   = float(r["total_pnl"])
                    sign  = "+" if pnl >= 0 else ""
                    dir_  = r["direction"] or "?"
                    emoji = "🟢" if r["direction"] == "long" else "🔴"
                    lines.append(
                        f"  {emoji} `{dir_}` {total}T {wr:.0f}% WR  PnL: `{sign}{pnl:.2f}`"
                    )
                lines.append("")

            if by_asset:
                best_3  = [r for r in by_asset if int(r["total"]) >= 1][:3]
                worst_3 = [r for r in reversed(by_asset) if int(r["total"]) >= 1][:3]

                if best_3:
                    lines.append("**Best Assets:**")
                    for r in best_3:
                        total = int(r["total"])
                        wins  = int(r["wins"])
                        wr    = wins / total * 100 if total else 0
                        pnl   = float(r["total_pnl"])
                        sign  = "+" if pnl >= 0 else ""
                        lines.append(
                            f"  ✅ `{r['asset']}` {total}T {wr:.0f}% WR `{sign}{pnl:.2f}`"
                        )
                    lines.append("")

                if worst_3 and worst_3 != best_3:
                    lines.append("**Worst Assets:**")
                    for r in worst_3:
                        total = int(r["total"])
                        wins  = int(r["wins"])
                        wr    = wins / total * 100 if total else 0
                        pnl   = float(r["total_pnl"])
                        sign  = "+" if pnl >= 0 else ""
                        lines.append(
                            f"  ❌ `{r['asset']}` {total}T {wr:.0f}% WR `{sign}{pnl:.2f}`"
                        )

            if not by_exchange and not by_direction:
                lines.append("No closed trades yet.")

            await interaction.followup.send(**_send("\n".join(lines)))

        except Exception as exc:
            logger.error("discord_stats_error", error=str(exc))
            await interaction.followup.send(content="❌ Error fetching stats.")

    # ── /router ───────────────────────────────────────────────────────────────

    @tree.command(name="router", description="Truth Router edge table + routed trade results")
    async def cmd_router(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        try:
            async with db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    WITH src AS (
                      SELECT sr.engine_name,
                             CASE WHEN sr.direction IN ('risk_on','inflow','liquidity_expansion') THEN 'emit_long'
                                  WHEN sr.direction IN ('risk_off','outflow','tightening','volatility_regime') THEN 'emit_short'
                                  WHEN sr.composite_score >= 0.10 THEN 'score_long'
                                  WHEN sr.composite_score <= -0.10 THEN 'score_short'
                                  ELSE NULL END AS bucket,
                             CASE WHEN UPPER(COALESCE(t.exchange,'')) = 'CRYPTO' THEN 'CRYPTO' ELSE 'OTHER' END AS exch,
                             sr.ticker, date_trunc('hour', sr.emitted_at) AS hr,
                             so.outcome_score_delta AS ret
                        FROM signal_outcomes so
                        JOIN signals_registry sr ON sr.id = so.signal_id
                        LEFT JOIN tickers t ON t.symbol = sr.ticker
                       WHERE so.evaluation_window_h = 24
                         AND so.outcome_score_delta IS NOT NULL
                         AND sr.emitted_at > NOW() - INTERVAL '30 days'
                         AND sr.engine_name <> 'technical_replay'
                    ), dedup AS (
                      SELECT DISTINCT ON (engine_name, bucket, exch, ticker, hr)
                             engine_name, bucket, exch, ret
                        FROM src WHERE bucket IS NOT NULL
                    )
                    SELECT engine_name, bucket, exch,
                           COUNT(*) FILTER (WHERE ret > 0.002)  AS ups,
                           COUNT(*) FILTER (WHERE ret < -0.002) AS downs
                      FROM dedup GROUP BY 1, 2, 3
                    """
                )
                trade_rows = await conn.fetch(
                    """
                    SELECT router_action,
                           COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE closed_at IS NOT NULL) AS closed,
                           COUNT(*) FILTER (WHERE pnl > 0) AS wins,
                           COALESCE(SUM(pnl), 0) AS total_pnl
                      FROM paper_trades
                     WHERE router_action IS NOT NULL
                       AND close_reason IS DISTINCT FROM 'dedup_cleanup'
                     GROUP BY 1 ORDER BY 1
                    """
                )

            base: dict[str, tuple[int, int]] = {}
            for r in rows:
                bu, bd = base.get(r["exch"], (0, 0))
                base[r["exch"]] = (bu + int(r["ups"]), bd + int(r["downs"]))

            entries = []
            for r in rows:
                n = int(r["ups"]) + int(r["downs"])
                base_ups, base_downs = base[r["exch"]]
                base_n = base_ups + base_downs
                if n < 30 or base_n <= 0:
                    continue
                lift = (int(r["ups"]) / n - base_ups / base_n) * (n / (n + 50.0))
                entries.append((r["engine_name"], r["bucket"], r["exch"], lift, n))
            entries.sort(key=lambda e: -abs(e[3]))

            lines = ["🧭 **TRUTH ROUTER** (24h edge, 30d window)\n"]
            lines.append("**Measured lift** (+ => long evidence):")
            for eng, bucket, exch, lift, n in entries[:10]:
                short_eng = eng.replace("Engine", "")
                verdict = "→long" if lift >= 0.05 else ("→short" if lift <= -0.05 else "no edge")
                lines.append(
                    f"  `{short_eng}` {bucket}/{exch}: "
                    f"`{lift:+.3f}` n={n} _{verdict}_"
                )
            if not entries:
                lines.append("  (no buckets with n >= 30 yet)")

            if trade_rows:
                lines.append("\n**Routed trades:**")
                for r in trade_rows:
                    closed = int(r["closed"])
                    wins   = int(r["wins"])
                    pnl    = float(r["total_pnl"])
                    sign   = "+" if pnl >= 0 else ""
                    wr     = f"{wins / closed * 100:.0f}%" if closed else "—"
                    lines.append(
                        f"  `{r['router_action']}`: {int(r['total'])} opened, "
                        f"{closed} closed, WR {wr}, PnL `{sign}{pnl:.2f}`"
                    )
            else:
                lines.append("\n**Routed trades:** none yet")

            await interaction.followup.send(**_send("\n".join(lines)))

        except Exception as exc:
            logger.error("discord_router_error", error=str(exc))
            await interaction.followup.send(content="❌ Error fetching router state.")

    # ── /kill (admin) ─────────────────────────────────────────────────────────

    @tree.command(name="kill", description="[ADMIN] Toggle trading kill switch")
    async def cmd_kill(interaction: discord.Interaction) -> None:
        admin_id = _admin_id()
        if admin_id and interaction.user.id != admin_id:
            await interaction.response.send_message("❌ Admin only.", ephemeral=True)
            return
        await interaction.response.defer()

        try:
            current = await redis.get("trading:kill_switch")
            is_active = current in ("1", b"1")

            if is_active:
                await redis.delete("trading:kill_switch")
                msg = "✅ **Kill switch DEACTIVATED.** Trading resumed."
            else:
                await redis.set("trading:kill_switch", "1")
                msg = "🛑 **Kill switch ACTIVATED.** All new orders halted."

            await interaction.followup.send(content=msg)

        except Exception as exc:
            logger.error("discord_kill_error", error=str(exc))
            await interaction.followup.send(content="❌ Error toggling kill switch.")

    # ── /cide ─────────────────────────────────────────────────────────────────

    @tree.command(name="cide", description="Capital injection readiness (CIDe)")
    async def cmd_cide(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        try:
            from paper_trading_engine.capital_injection_engine import CIDe
            engine = CIDe(db)
            result = await engine.evaluate(requested_capital=0.0)
            decision = result["decision"]
            score    = result["edge_stability_score"]
            reasons  = result.get("reasoning", [])
            top3     = reasons[:6]
            lines = [
                "**💉 CIDe Capital Readiness**",
                f"Decision: `{decision}`",
                f"Score: `{score:.4f}`",
                "",
                *[f"• {r}" for r in top3],
            ]
            await interaction.followup.send(**_send("\n".join(lines)))
        except Exception as exc:
            logger.error("discord_cide_error", error=str(exc))
            await interaction.followup.send(content="❌ CIDe error.")

    # ── /engine_health ────────────────────────────────────────────────────────

    @tree.command(name="engine_health", description="Crypto engine accuracy (7d)")
    async def cmd_engine_health(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        try:
            async with db.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT sr.engine_name,
                           COUNT(*) AS n,
                           AVG(CASE WHEN so.direction_correct THEN 1.0 ELSE 0.0 END) AS accuracy
                    FROM signal_outcomes so
                    JOIN signals_registry sr ON sr.id = so.signal_id
                    WHERE so.evaluated_at > NOW() - INTERVAL '7 days'
                      AND so.direction_correct IS NOT NULL
                      AND sr.exchange IN ('BINANCE', 'CRYPTO', 'BINANCE_FUTURES')
                    GROUP BY sr.engine_name
                    ORDER BY accuracy ASC
                """)
            if not rows:
                await interaction.followup.send(content="No crypto engine data yet.")
                return
            lines = ["**⚙️ Crypto Engine Health (7d)**\n"]
            for r in rows:
                acc   = float(r["accuracy"])
                n     = int(r["n"])
                flag  = "⚠️" if acc < 0.45 else ("✅" if acc >= 0.55 else "🔶")
                short = str(r["engine_name"]).replace("Engine", "")
                lines.append(f"{flag} `{short}`: {acc:.1%} (n={n})")
            await interaction.followup.send(**_send("\n".join(lines)))
        except Exception as exc:
            logger.error("discord_engine_health_error", error=str(exc))
            await interaction.followup.send(content="❌ Error.")

    # ── /regime ───────────────────────────────────────────────────────────────

    @tree.command(name="regime", description="Current macro regime + risk governor state")
    async def cmd_regime(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        try:
            async with db.acquire() as conn:
                regime_row = await conn.fetchrow(
                    "SELECT regime, computed_at FROM macro_regimes ORDER BY computed_at DESC LIMIT 1"
                )
                vix_row = await conn.fetchrow(
                    "SELECT price FROM ticker_quotes WHERE symbol = 'VIX' ORDER BY ts DESC LIMIT 1"
                )
                snap = await conn.fetchrow(
                    "SELECT governor_state, drawdown_pct FROM equity_snapshots ORDER BY captured_at DESC LIMIT 1"
                )
            regime = regime_row["regime"] if regime_row else "unknown"
            vix    = float(vix_row["price"]) if vix_row and vix_row["price"] else 0.0
            gov    = snap["governor_state"] if snap else "NORMAL"
            dd     = float(snap["drawdown_pct"]) * 100 if snap else 0.0
            lines = [
                "**📊 Regime + Governor**\n",
                f"Macro: `{regime}`",
                f"VIX: `{vix:.1f}`",
                f"Governor: `{gov}`",
                f"Drawdown: `{dd:.1f}%`",
            ]
            await interaction.followup.send(**_send("\n".join(lines)))
        except Exception as exc:
            logger.error("discord_regime_error", error=str(exc))
            await interaction.followup.send(content="❌ Error.")

    # ── /cluster ──────────────────────────────────────────────────────────────

    @tree.command(name="cluster", description="Per-cluster edge + win rate (14d)")
    async def cmd_cluster(interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        try:
            from paper_trading_engine.risk_governor import cluster_of, CLUSTERS
            async with db.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT sr.engine_name, pt.pnl
                    FROM paper_trades pt
                    JOIN signals_registry sr ON sr.id = pt.signal_id
                    WHERE pt.closed_at IS NOT NULL
                      AND pt.pnl IS NOT NULL
                      AND sr.engine_name IS NOT NULL
                      AND pt.closed_at > NOW() - INTERVAL '14 days'
                """)
            stats: dict[str, dict] = {c: {"n": 0, "wins": 0, "pnl": 0.0} for c in list(CLUSTERS) + ["UNKNOWN"]}
            for r in rows:
                cname = cluster_of(r["engine_name"]) or "UNKNOWN"
                stats[cname]["n"] += 1
                stats[cname]["pnl"] += float(r["pnl"])
                if float(r["pnl"]) > 0:
                    stats[cname]["wins"] += 1
            lines = ["**🔬 Cluster Edge (14d)**\n"]
            for cname, s in stats.items():
                if s["n"] == 0:
                    continue
                wr  = s["wins"] / s["n"]
                avg = s["pnl"] / s["n"]
                flag = "🟢" if s["pnl"] >= 0 else "🔴"
                lines.append(
                    f"{flag} `{cname}`: {wr:.0%} WR, "
                    f"avg `{avg:+.2f}`, n={s['n']}"
                )
            if len(lines) == 1:
                lines.append("  No closed trades in 14d")
            await interaction.followup.send(**_send("\n".join(lines)))
        except Exception as exc:
            logger.error("discord_cluster_error", error=str(exc))
            await interaction.followup.send(content="❌ Error.")
