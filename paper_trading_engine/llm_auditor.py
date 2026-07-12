"""LLM trade auditor — TradingAgents-style bull/bear/judge review, shadow mode.

Sprint 38. For every opened trade, a dossier of the evidence (engine, scores,
router verdict, RSI, regime, funding) is sent to the in-network Ollama model,
which must argue the bull case, the bear case, and deliver a verdict. The
verdict is LOGGED to llm_trade_audits and nothing else: it cannot veto or size
trades. After a few weeks, `SELECT verdict, AVG(pnl)` against closed trades
tells us whether the model discriminates winners from losers — promotion to a
real gate happens only on that evidence.

Inspired by TauricResearch/TradingAgents (analyst → researcher debate → risk
team). Compressed to a single structured call because a 7B local model gains
nothing from talking to itself across multiple requests.
"""
from __future__ import annotations

import json
import os
import re
import time

import aiohttp
import asyncpg
import structlog

logger = structlog.get_logger(__name__)

_PROMPT_TEMPLATE = """You are the risk desk of a crypto/equities trading firm.
A trade was just opened. Audit it.

TRADE:
- Asset: {asset} ({exchange})
- Direction: {direction}
- Entry price: {entry_price}
- Source engine: {engine}
- Composite signal score: {composite_score:+.3f} (positive = bullish evidence)
- Router action: {router_action} (measured 24h edge {router_edge:+.3f}, "invert" means we are FADING the engine signal because that engine is reliably wrong)
- RSI(14): {rsi:.1f}
- Technical score: {tech_score:+.3f}
- Macro regime: {regime}
- BTC perp funding rate: {funding:+.4%}
{lessons}
Respond with ONLY a JSON object, no other text:
{{"bull_case": "<strongest 1-2 sentence case the price rises>",
"bear_case": "<strongest 1-2 sentence case the price falls>",
"verdict": "approve" or "reject",
"confidence": <0.0-1.0>}}

"approve" means the {direction} position is justified by the evidence above."""

_POSTMORTEM_TEMPLATE = """You are the risk post-mortem desk of a crypto trading firm.
A live trade just closed. Deliver a structured verdict.

CLOSED TRADE:
- Asset: {asset} ({exchange})
- Direction: {direction}
- Entry {entry_price} → Exit {exit_price}
- PnL: {pnl:+.2f} USD ({pnl_pct:+.2f}%)
- Held: {hold_hours:.1f}h, closed by: {close_reason}
- Router action at entry: {router_action} (edge {router_edge:+.3f})
- Macro regime at entry: {regime}
- Engine scores at entry: {engine_scores}

Verdicts:
  won_correctly      = signal was right AND regime fit
  won_despite_signal = signal was weak/wrong but closed profitable anyway
  lost_correctly     = signal was wrong, loss was predictable
  lost_despite_signal = signal was strong/correct but lost anyway (bad luck / execution)

Respond with ONLY a JSON object, no other text:
{{"verdict": "<one of the four above>",
"key_mistake": "<1 sentence: what went wrong, or null if none>",
"lesson": "<1-2 sentences: transferable lesson for future {asset} trades>",
"regime_fit": "good" or "neutral" or "bad",
"should_have_traded": true or false}}"""

_REFLECT_TEMPLATE = """You are the post-trade review desk of a trading firm.
A trade just closed. Extract ONE transferable lesson.

CLOSED TRADE:
- Asset: {asset} ({exchange})
- Direction: {direction}
- Entry {entry_price} -> Exit {exit_price}
- PnL: {pnl:+.2f} USD ({outcome})
- Held: {hold_hours:.1f}h, closed by: {close_reason}
- Router action at entry: {router_action} (edge {router_edge:+.3f})

Respond with ONLY a JSON object, no other text:
{{"lesson": "<one concise, transferable lesson for future {asset} trades, max 2 sentences, grounded in the facts above>"}}"""


class LLMTradeAuditor:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.enabled = os.getenv("LLM_AUDIT_ENABLED", "true").lower() == "true"
        self.base_url = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434").rstrip("/")
        self.model = os.getenv("LLM_AUDIT_MODEL", "qwen2.5:7b")
        self.pm_model = os.getenv("LLM_POSTMORTEM_MODEL", "qwen2.5:3b")
        self.timeout_s = int(os.getenv("LLM_AUDIT_TIMEOUT_S", "180"))
        self._pool = pool
        self._webhook = os.getenv("DISCORD_WEBHOOK_TRADES", "")

    async def audit(self, trade, signal, context: dict) -> None:
        """Fire-and-forget audit of an opened trade. Never raises."""
        if not self.enabled:
            return
        dossier = {
            "asset": trade.asset,
            "exchange": trade.exchange or "",
            "direction": trade.direction,
            "entry_price": trade.entry_price,
            "engine": signal.source,
            "composite_score": signal.composite_score,
            "router_action": signal.router_action or "passthrough",
            "router_edge": signal.router_edge,
            "rsi": context.get("rsi", 50.0),
            "tech_score": signal.tech_score,
            "regime": context.get("regime", "unknown"),
            "funding": context.get("funding", 0.0),
        }
        prompt = _PROMPT_TEMPLATE.format(lessons=await self._lessons_block(trade.asset), **dossier)

        started = time.monotonic()
        verdict, confidence, bull, bear, raw = "error", None, None, None, ""
        try:
            raw = await self._generate(prompt)
            parsed = _parse_json(raw)
            if parsed:
                v = str(parsed.get("verdict", "")).lower()
                verdict = v if v in ("approve", "reject") else "error"
                try:
                    confidence = max(0.0, min(1.0, float(parsed.get("confidence"))))
                except (TypeError, ValueError):
                    confidence = None
                bull = str(parsed.get("bull_case") or "")[:1000] or None
                bear = str(parsed.get("bear_case") or "")[:1000] or None
        except Exception as exc:
            logger.warning("llm_audit_failed", asset=trade.asset, error=str(exc))

        latency_ms = int((time.monotonic() - started) * 1000)
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO llm_trade_audits
                        (trade_id, signal_id, asset, direction, model, verdict,
                         confidence, bull_case, bear_case, raw_response,
                         latency_ms, dossier)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                    """,
                    trade.trade_id, trade.signal_id, trade.asset, trade.direction,
                    self.model, verdict, confidence, bull, bear,
                    raw[:4000], latency_ms, json.dumps(dossier),
                )
        except Exception as exc:
            logger.warning("llm_audit_persist_failed", asset=trade.asset, error=str(exc))
            return

        logger.info(
            "llm_audit_done",
            asset=trade.asset,
            direction=trade.direction,
            verdict=verdict,
            confidence=confidence,
            latency_ms=latency_ms,
        )
        if verdict != "error" and self._webhook:
            icon = "✅" if verdict == "approve" else "❌"
            conf_pct = f"{confidence:.0%}" if confidence is not None else "?"
            embed = {
                "title": f"🔍 LLM Audit — {trade.asset} {trade.direction} | {icon} {verdict.upper()} ({conf_pct})",
                "color": 0x2ecc71 if verdict == "approve" else 0xe74c3c,
                "fields": [],
            }
            if bull:
                embed["fields"].append({"name": "🐂 Bull case", "value": bull[:512], "inline": False})
            if bear:
                embed["fields"].append({"name": "🐻 Bear case", "value": bear[:512], "inline": False})
            embed["fields"].append({"name": "Model", "value": f"`{self.model}` • {latency_ms}ms", "inline": True})
            await self._post_discord({"embeds": [embed]})

    async def post_mortem(self, trade, context: dict) -> None:
        """Structured post-mortem for a closed live trade. Fire-and-forget, never raises."""
        if not self.enabled:
            return
        if (trade.close_reason or "") == "dedup_cleanup":
            return
        if not getattr(trade, "is_live", False):
            return  # only post-mortem live trades

        hold_hours = 0.0
        if trade.opened_at and trade.closed_at:
            hold_hours = (trade.closed_at - trade.opened_at).total_seconds() / 3600
        pnl = trade.pnl or 0.0
        entry = trade.entry_price or 0.0
        exit_p = trade.exit_price or entry
        pnl_pct = ((exit_p - entry) / entry * 100.0 * (-1 if trade.direction == "short" else 1)) if entry else 0.0

        # Best-effort: fetch engine scores from signal_trace.
        # signal_trace.signal_id is always NULL (signal_archiver assigns the ID after
        # engines write their trace rows), so a signal_id lookup never matches.
        # Use ticker + time-proximity instead: fetch all trace rows within a ±30/+5 min
        # window of trade open, ordered by proximity, to find the best match.
        engine_scores = "unavailable"
        opened_at = getattr(trade, "opened_at", None)
        if opened_at is not None:
            try:
                async with self._pool.acquire() as conn:
                    rows = await conn.fetch(
                        """SELECT engine_name, signal_strength, signal_direction
                             FROM signal_trace
                            WHERE ticker = $1
                              AND created_at BETWEEN $2 - INTERVAL '30 minutes'
                                                 AND $2 + INTERVAL '5 minutes'
                            ORDER BY ABS(EXTRACT(EPOCH FROM (created_at - $2))) ASC
                            LIMIT 5""",
                        trade.asset,
                        opened_at,
                    )
                    if rows:
                        engine_scores = ", ".join(
                            f"{r['engine_name']}:{r['signal_direction']}({r['signal_strength'] or 0:.2f})"
                            for r in rows
                        )
            except Exception:
                pass

        prompt = _POSTMORTEM_TEMPLATE.format(
            asset=trade.asset,
            exchange=trade.exchange or "",
            direction=trade.direction,
            entry_price=entry,
            exit_price=exit_p,
            pnl=pnl,
            pnl_pct=pnl_pct,
            hold_hours=hold_hours,
            close_reason=trade.close_reason or "unknown",
            router_action=getattr(trade, "router_action", "") or "passthrough",
            router_edge=getattr(trade, "router_edge", 0.0),
            regime=context.get("regime", "unknown"),
            engine_scores=engine_scores,
        )

        started = time.monotonic()
        verdict = key_mistake = lesson = regime_fit = None
        should_have_traded = None
        try:
            raw = await self._generate_pm(prompt)
            parsed = _parse_json(raw)
            if parsed:
                _VERDICTS = {"won_correctly", "won_despite_signal", "lost_correctly", "lost_despite_signal"}
                v = str(parsed.get("verdict", "")).strip()
                verdict = v if v in _VERDICTS else None
                key_mistake = str(parsed.get("key_mistake") or "")[:500] or None
                lesson = str(parsed.get("lesson") or "")[:1000] or None
                rf = str(parsed.get("regime_fit", "")).lower()
                regime_fit = rf if rf in ("good", "neutral", "bad") else None
                stv = parsed.get("should_have_traded")
                if isinstance(stv, bool):
                    should_have_traded = stv
                elif isinstance(stv, str):
                    should_have_traded = stv.lower() == "true"
        except Exception as exc:
            logger.warning("llm_postmortem_failed", asset=trade.asset, error=str(exc))

        latency_ms = int((time.monotonic() - started) * 1000)
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO trade_post_mortems
                           (trade_id, asset, direction, pnl, verdict, key_mistake,
                            lesson, regime_fit, should_have_traded, regime, model, latency_ms)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)""",
                    trade.trade_id, trade.asset, trade.direction, pnl,
                    verdict, key_mistake, lesson, regime_fit, should_have_traded,
                    context.get("regime"), self.pm_model, latency_ms,
                )
        except Exception as exc:
            logger.warning("llm_postmortem_persist_failed", asset=trade.asset, error=str(exc))
            return

        logger.info("llm_postmortem_done", asset=trade.asset, verdict=verdict,
                    regime_fit=regime_fit, latency_ms=latency_ms)

        if verdict and self._webhook:
            icon = "✅" if "correctly" in (verdict or "") else "⚠️"
            pnl_str = f"{'+' if pnl >= 0 else ''}{pnl:.2f}"
            embed = {
                "title": f"🔬 Post-Mortem — {trade.asset} {trade.direction} | {pnl_str} USD",
                "color": 0x2ecc71 if pnl > 0 else 0xe74c3c,
                "fields": [
                    {"name": "Verdict", "value": f"{icon} {verdict}", "inline": True},
                    {"name": "Regime Fit", "value": regime_fit or "?", "inline": True},
                    {"name": "Should Trade?", "value": "Yes" if should_have_traded else "No", "inline": True},
                ],
            }
            if key_mistake:
                embed["fields"].append({"name": "⚡ Key Mistake", "value": key_mistake[:300], "inline": False})
            if lesson:
                embed["fields"].append({"name": "💡 Lesson", "value": lesson[:400], "inline": False})
            embed["footer"] = {"text": f"{self.pm_model} • {latency_ms}ms"}
            await self._post_discord({"embeds": [embed]})

    async def _generate(self, prompt: str) -> str:
        """Single Ollama completion; returns raw response text."""
        timeout = aiohttp.ClientTimeout(total=self.timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "format": "json",
                    "options": {"temperature": 0.2, "num_predict": 400},
                },
            ) as resp:
                body = await resp.json()
        return (body.get("response") or "").strip()

    async def _generate_pm(self, prompt: str) -> str:
        """Like _generate but uses pm_model (qwen2.5:3b for speed)."""
        timeout = aiohttp.ClientTimeout(total=self.timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.pm_model,
                    "prompt": prompt,
                    "stream": False,
                    "format": "json",
                    "options": {"temperature": 0.2, "num_predict": 500},
                },
            ) as resp:
                body = await resp.json()
        return (body.get("response") or "").strip()

    async def _lessons_block(self, asset: str) -> str:
        """Recent house lessons (same asset first, then global) for the audit prompt.

        TradingAgents-style reflection injection: realized-outcome lessons feed
        future decisions for the same ticker plus recent cross-ticker context.
        """
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    (SELECT asset, lesson FROM trade_reflections
                      WHERE asset = $1 AND lesson IS NOT NULL
                      ORDER BY created_at DESC LIMIT 3)
                    UNION ALL
                    (SELECT asset, lesson FROM trade_reflections
                      WHERE asset <> $1 AND lesson IS NOT NULL
                      ORDER BY created_at DESC LIMIT 3)
                    """,
                    asset,
                )
        except Exception:
            return ""
        if not rows:
            return ""
        lines = [f"- [{r['asset']}] {str(r['lesson'])[:220]}" for r in rows]
        return "\nLessons from this desk's recent closed trades:\n" + "\n".join(lines) + "\n"

    async def _post_discord(self, payload: dict) -> None:
        """Fire-and-forget Discord webhook. Never raises."""
        if not self._webhook:
            return
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(self._webhook, json=payload, timeout=aiohttp.ClientTimeout(total=5))
        except Exception:
            pass

    async def reflect(self, trade) -> None:
        """Generate and store a one-paragraph lesson for a closed trade. Never raises."""
        if not self.enabled:
            return
        if (trade.close_reason or "") == "dedup_cleanup":
            return
        hold_hours = 0.0
        if trade.opened_at and trade.closed_at:
            hold_hours = (trade.closed_at - trade.opened_at).total_seconds() / 3600
        pnl = trade.pnl or 0.0
        prompt = _REFLECT_TEMPLATE.format(
            asset=trade.asset,
            exchange=trade.exchange or "",
            direction=trade.direction,
            entry_price=trade.entry_price,
            exit_price=trade.exit_price or 0.0,
            pnl=pnl,
            outcome="WIN" if pnl > 0 else "LOSS",
            hold_hours=hold_hours,
            close_reason=trade.close_reason or "unknown",
            router_action=getattr(trade, "router_action", "") or "passthrough",
            router_edge=getattr(trade, "router_edge", 0.0),
        )
        lesson = None
        try:
            parsed = _parse_json(await self._generate(prompt))
            if parsed:
                lesson = str(parsed.get("lesson") or "")[:1000] or None
        except Exception as exc:
            logger.warning("llm_reflect_failed", asset=trade.asset, error=str(exc))
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO trade_reflections
                        (trade_id, asset, direction, router_action, close_reason,
                         pnl, hold_hours, lesson, model)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    """,
                    trade.trade_id, trade.asset, trade.direction,
                    getattr(trade, "router_action", "") or None,
                    trade.close_reason, pnl, round(hold_hours, 2), lesson, self.model,
                )
            logger.info("llm_reflection_saved", asset=trade.asset,
                        pnl=round(pnl, 2), has_lesson=bool(lesson))
            if lesson and self._webhook:
                outcome_icon = "✅" if pnl > 0 else "❌"
                embed = {
                    "title": f"💡 Lesson — {trade.asset} {trade.direction} | {outcome_icon} {'+' if pnl >= 0 else ''}{pnl:.2f} USD",
                    "description": lesson,
                    "color": 0x2ecc71 if pnl > 0 else 0xe74c3c,
                    "footer": {"text": f"closed by {trade.close_reason} • held {hold_hours:.1f}h"},
                }
                await self._post_discord({"embeds": [embed]})
        except Exception as exc:
            logger.warning("llm_reflect_persist_failed", asset=trade.asset, error=str(exc))


def _parse_json(text: str) -> dict | None:
    """Parse the model's JSON, tolerating stray prose around it."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                return None
    return None
