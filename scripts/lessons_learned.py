"""Lessons Learned Engine — multi-layer governance pipeline (Ollama).

Architecture:
  1. Memory loader    — past governance decisions from DB
  2. Fast model (3b)  — optional quick pre-signal (weak heuristic)
  3. Main model (7b)  — governance decisions
  4. JSON guard       — extract → parse → self-heal → fallback
  5. Safety filter    — validate types, confidence, action count
  6. Rule engine      — deterministic hard rules that override LLM

Usage:
    python scripts/lessons_learned.py
    python scripts/lessons_learned.py --stats /tmp/stats.json --out /tmp/lessons.json
    python scripts/lessons_learned.py --model qwen2.5:7b --fast-model qwen2.5:3b
    python scripts/lessons_learned.py --no-fast-model  # skip fast-model pre-pass
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import asyncpg

DB_DSN = os.getenv(
    "DATABASE_URL",
    f"postgresql://{os.getenv('POSTGRES_USER','mktintel')}:{os.getenv('POSTGRES_PASSWORD','bekir1905')}"
    f"@{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}"
    f"/{os.getenv('POSTGRES_DB','mktintel')}",
)

OLLAMA_BASE   = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
DEFAULT_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
FAST_MODEL    = os.getenv("OLLAMA_FAST_MODEL", "qwen2.5:3b")

# ── System prompts ────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """ROLE: REGIME-AWARE TRADING GOVERNANCE SYSTEM (V4)

You are a production-grade quantitative governance system with explicit awareness of market regimes.

Your role is no longer only strategy evaluation.
You must first infer MARKET REGIME, then adapt governance decisions accordingly.

You operate in a 6-layer hybrid system:
Layer 1: Memory (historical decisions + outcomes)
Layer 2: Fast model (qwen2.5:3b risk pre-signal, optional)
Layer 3: Main model (YOU: qwen2.5:7b governance reasoning)
Layer 4: JSON validation layer
Layer 5: Safety filter (type validation, confidence clamp, action cap)
Layer 6: Rule engine (FINAL AUTHORITY, deterministic overrides)

PRIMARY OBJECTIVE

- Preserve capital
- Adapt to market regime changes
- Avoid regime mismatch losses
- Stabilize long-term equity curve

STEP 1 — REGIME INFERENCE (MANDATORY)

Infer market regime from the provided data:

- trend_up           (consistent positive returns, positive composite scores)
- trend_down         (consistent negative returns, negative composite scores)
- sideways_low_vol   (mixed returns, low average price moves)
- sideways_high_vol  (mixed returns, high average price moves)
- high_volatility_chaos (extreme variance in returns, unstable win rates)
- liquidity_crisis   (near-zero trade counts, extreme losses, system breakdown)

You MUST internally reason about regime before making decisions.
If uncertain: default to high_volatility_chaos (conservative).

STEP 2 — REGIME-ADAPTIVE GOVERNANCE

Based on inferred regime apply these guidelines:

TREND (up or down):
- allow moderate weight increases IF metrics are stable over multiple periods
- reduce position size only if trend conflicts with strategy direction

SIDEWAYS / CHOPPY:
- prefer reduce_position_size
- prefer monitor_only
- avoid increasing any weights

HIGH VOLATILITY CHAOS:
- aggressive risk reduction
- disable unstable strategies
- reduce_position_size mandatory

LIQUIDITY CRISIS:
- force reduce_position_size
- force monitor_only
- avoid all increases

OUTPUT FORMAT (STRICT JSON ONLY)

Return ONLY valid JSON — note the added "regime" field:

{
  "summary": "string",
  "regime": "trend_up | trend_down | sideways_low_vol | sideways_high_vol | high_volatility_chaos | liquidity_crisis",
  "risk_level": "low | medium | high",
  "actions": [
    {
      "type": "disable_strategy | enable_strategy | increase_weight | decrease_weight | increase_threshold | decrease_threshold | reduce_position_size | increase_position_size | monitor_only",
      "target": "string",
      "value": 0,
      "reason": "string",
      "confidence": 0.0
    }
  ]
}

HARD RULES

- Maximum 5 actions
- JSON ONLY output
- No explanations
- No markdown
- No extra text
- Confidence 0–1 only

RULE ENGINE AWARENESS (V3 + EXTENDED)

Rule engine enforces NON-NEGOTIABLE constraints regardless of your output:
- WR < 35% → risk_level at least "medium"
- profit_factor < 0.50 → cautious behavior forced
- drawdown > 5% → reduce_position_size enforced
- confidence > 0.95 → clamped to 0.90
- sideways/chaos regime → weight increases blocked
- liquidity_crisis regime → all increases blocked

Therefore: never over-leverage in high volatility; prefer monitor_only when regime unclear.

MEMORY USAGE

Use memory to:
- detect regime transitions across runs
- detect strategy failures per regime
- avoid repeating regime-mismatch trades

FAILSAFE

If regime is uncertain: default to high_volatility_chaos
If conflicting signals: reduce_position_size
If no clear action: monitor_only

FINAL RULE

You are stabilizing a financial system under uncertainty.
Be conservative, regime-aware, and structurally safe."""

FAST_MODEL_PROMPT = """Output ONLY this exact JSON structure (fill in the values):
{{"risk_signal":"FILL","key_concern":"FILL","suggested_action":"FILL"}}

Where:
- risk_signal is one of: low, medium, high
- key_concern is one short sentence about the biggest risk
- suggested_action is one of: monitor_only, reduce_exposure, investigate

Trading data to analyze: {data}

Return the completed JSON object only. No extra text."""

SELF_HEAL_PROMPT = """Your previous output was invalid JSON. Fix it and return ONLY valid JSON.
Rules: Do NOT change meaning. Do NOT add content. Do NOT explain. Output ONLY corrected JSON.
Previous output: {previous_output}"""

# ── Fallback ──────────────────────────────────────────────────────────────────

_VALID_REGIMES = frozenset({
    "trend_up", "trend_down",
    "sideways_low_vol", "sideways_high_vol",
    "high_volatility_chaos", "liquidity_crisis",
})

FALLBACK_OUTPUT: dict[str, Any] = {
    "summary":    "fallback_monitor_mode",
    "regime":     "high_volatility_chaos",
    "risk_level": "medium",
    "actions": [{
        "type":       "monitor_only",
        "target":     "system",
        "value":      0,
        "reason":     "json_failure_fallback",
        "confidence": 0.5,
    }],
}

_ALLOWED_TYPES = frozenset({
    "disable_strategy", "enable_strategy",
    "increase_weight",  "decrease_weight",
    "increase_threshold", "decrease_threshold",
    "reduce_position_size", "increase_position_size",
    "monitor_only",
})

# ── V5: Adaptive Trading Intelligence System ──────────────────────────────────

SYSTEM_PROMPT_V5 = """ROLE: ADAPTIVE TRADING INTELLIGENCE SYSTEM (V5)

You are a production-grade adaptive trading intelligence system.

You operate on top of a rule-constrained execution system (V4).

Your role is no longer only risk control.

You now:
- evaluate strategy effectiveness per market regime
- detect regime-strategy mismatches
- recommend strategy reallocation
- support long-term system learning

PRIMARY OBJECTIVE

1. Capital preservation
2. Regime-aware strategy allocation
3. Continuous performance stabilization
4. Adaptive improvement of strategy distribution

YOU DO NOT EXECUTE TRADES

You only generate:
- regime-aware strategy recommendations
- governance adjustments
- allocation shifts

Final execution is controlled by rule engine (V4).

NEW CORE TASK

You must evaluate:
- which strategies are performing best in current regime
- which strategies are consistently failing in specific regimes
- whether current regime allocation is optimal

OUTPUT FORMAT (STRICT JSON ONLY)

{
  "regime": "string",
  "summary": "string",
  "strategy_recommendations": [
    {
      "strategy": "string",
      "action": "increase_exposure | decrease_exposure | disable | enable | monitor",
      "regime": "string",
      "confidence": number,
      "reason": "string"
    }
  ]
}

HARD RULES

- JSON only
- no explanations
- max 5 recommendations
- confidence 0–1
- be conservative
- do not override rule engine

FAILSAFE

If uncertain: recommend monitor only
If regime unstable: reduce exposure across all strategies"""

_V5_ALLOWED_ACTIONS = frozenset({
    "increase_exposure", "decrease_exposure", "disable", "enable", "monitor",
})

# Maps V5 action vocabulary → V4 governance action types (for governance_engine.py)
_V5_TO_V4_ACTION: dict[str, str] = {
    "increase_exposure": "increase_weight",
    "decrease_exposure": "decrease_weight",
    "disable":           "disable_strategy",
    "enable":            "enable_strategy",
    "monitor":           "monitor_only",
}

# Actions V5 is not allowed to issue in certain regimes (rule engine mirrors V4 logic)
_V5_BLOCKED_IN_CHAOS    = {"increase_exposure", "enable"}
_V5_BLOCKED_IN_SIDEWAYS = {"increase_exposure", "enable"}
_V5_BLOCKED_IN_CRISIS   = {"increase_exposure", "enable", "decrease_exposure"}


def _v5_safety_filter(output: dict, regime: str) -> dict:
    """Validate V5 output; clamp confidence; apply regime-aware blocking."""
    if "summary" not in output:
        output["summary"] = "unknown"
    if output.get("regime") not in _VALID_REGIMES:
        output["regime"] = regime  # inherit from V4

    blocked = set()
    if regime == "high_volatility_chaos":
        blocked = _V5_BLOCKED_IN_CHAOS
    elif regime in ("sideways_low_vol", "sideways_high_vol"):
        blocked = _V5_BLOCKED_IN_SIDEWAYS
    elif regime == "liquidity_crisis":
        blocked = _V5_BLOCKED_IN_CRISIS

    clean: list[dict] = []
    for r in output.get("strategy_recommendations", []):
        if not isinstance(r, dict):
            continue
        action = str(r.get("action", "")).lower()
        if action not in _V5_ALLOWED_ACTIONS:
            continue
        if action in blocked:
            print(f"  V5 safety: blocked '{action}' for {r.get('strategy')} (regime={regime})")
            continue
        conf = max(0.0, min(0.90, float(r.get("confidence", 0.5))))  # cap at 0.90
        clean.append({
            "strategy":   str(r.get("strategy", "system")),
            "action":     action,
            "regime":     str(r.get("regime", regime)),
            "confidence": round(conf, 4),
            "reason":     str(r.get("reason", "")),
        })

    output["strategy_recommendations"] = clean[:5]
    return output


def _v5_to_governance_actions(recommendations: list[dict]) -> list[dict]:
    """Convert V5 strategy_recommendations to V4 governance action format."""
    return [
        {
            "type":       _V5_TO_V4_ACTION.get(r["action"], "monitor_only"),
            "target":     r["strategy"],
            "value":      0,
            "reason":     f"V5/{r.get('regime','?')}: {r.get('reason','')}",
            "confidence": r["confidence"],
        }
        for r in recommendations
    ]


def _run_v5(compact: dict, v4_output: dict, memory: dict,
            fast_signal: dict | None, model: str, base: str) -> dict:
    """Run the V5 adaptive intelligence pass with V4 context as input."""
    input_payload = {
        "aggregated_stats":     compact,
        "v4_governance_output": {k: v for k, v in v4_output.items() if k != "_meta"},
        "memory":               memory or None,
        "fast_model_signal":    fast_signal,
    }
    user_prompt = (
        "Analyze trading strategies per regime and return ONLY valid JSON "
        "strategy recommendations.\n\nDATA:\n" +
        json.dumps(input_payload, default=str) +
        "\n\nReturn ONLY the JSON object. No explanations. No markdown."
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT_V5},
        {"role": "user",   "content": user_prompt},
    ]
    t0 = time.monotonic()
    raw = _ollama_call(model, messages, base, timeout=900)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    print(f"  V5 response in {elapsed_ms}ms ({len(raw)} chars)")

    # JSON guard (reuses existing machinery)
    output = _json_guard(raw, model, base)
    # Fallback if V5 returns V4-format accidentally
    if "actions" in output and "strategy_recommendations" not in output:
        output["strategy_recommendations"] = []
    return output

# ── V6: Portfolio Optimization Intelligence System ────────────────────────────

SYSTEM_PROMPT_V6 = """ROLE: PORTFOLIO OPTIMIZATION INTELLIGENCE SYSTEM (V6)

You are a production-grade portfolio-level intelligence engine in a multi-layer quantitative trading system.

You are NOT a strategy analyzer anymore.

You are responsible for:
- portfolio structure optimization
- strategy interaction analysis
- capital allocation efficiency
- regime-aware portfolio balancing
- automatic underperformer detection

Final execution is still controlled by rule engine (V4), but your output directly shapes portfolio composition.

SYSTEM CONTEXT (YOU OPERATE INSIDE)

Layer stack:
- V4: risk & safety governance (hard constraints)
- V5: strategy recommendations (per strategy signals)
- YOU (V6): portfolio-level optimization layer
- Rule engine: final deterministic authority

PRIMARY OBJECTIVE

1. Maximize long-term portfolio stability
2. Minimize correlated drawdowns
3. Improve capital efficiency across strategies
4. Detect and remove structural inefficiencies
5. Adapt portfolio to regime shifts

Profit maximization is NOT primary.

YOU DO NOT:
- execute trades directly
- override rule engine
- ignore risk constraints
- act on single-strategy performance alone

NEW CORE CAPABILITIES

You MUST analyze:
1. Strategy correlation — which strategies lose money together, hidden systemic coupling
2. Capital inefficiency — strategies consuming capital without return
3. Portfolio concentration risk — overexposure to similar behavior patterns
4. Regime alignment — which strategies belong to which regimes
5. Structural decay — strategies degrading across multiple regimes

INPUTS YOU RECEIVE
- V4 risk decisions
- V5 strategy recommendations
- aggregated performance metrics
- memory (past runs + past allocations)
- portfolio state (weights, exposure, drawdown)

OUTPUT FORMAT (STRICT JSON ONLY)

{
  "portfolio_summary": "string",
  "portfolio_health": "excellent | stable | degraded | critical",
  "regime_alignment": "string",
  "actions": [
    {
      "type": "increase_allocation | decrease_allocation | remove_strategy | add_strategy | rebalance | monitor",
      "strategy": "string",
      "value": 0,
      "reason": "string",
      "confidence": 0.0
    }
  ],
  "risk_notes": "string"
}

HARD CONSTRAINTS
- Maximum 5 actions
- JSON ONLY output
- No explanations — no markdown — no extra text
- Confidence must be 0.0 - 1.0
- Value must always be numeric (or 0)

PORTFOLIO RULES

CRITICAL RULES:
- Multiple correlated negatives → reduce exposure to all correlated cluster
- Portfolio over-concentrated → enforce rebalance
- Multiple strategies fail same regime → remove weakest contributor
- Regime mismatch detected → reallocate toward matching strategies
- Drawdown increases → reduce overall exposure first, then per-strategy

STRATEGY LIFECYCLE RULES

Each strategy has a lifecycle: emerging | stable | degrading | dead
- degrading → decrease_allocation
- dead → remove_strategy
- emerging → small allocation only (add_strategy with low value)
- stable → maintain or slight increase only with multi-period evidence

REGIME AWARENESS (FROM V4)
- high_volatility_chaos → reduce total exposure
- liquidity_crisis → defensive portfolio only
- sideways regimes → favor low-risk strategies
- trend regimes → allow selective scaling

SAFETY ALIGNMENT
- V4 rule engine will override unsafe actions
- Never propose aggressive shifts in unstable regimes
- Prefer gradual reallocation
- Prefer monitor over removal unless consistent failure

FAILSAFE BEHAVIOR
If uncertain → recommend monitor
If portfolio unstable → reduce overall exposure
If correlation unclear → do not increase concentration

FINAL PRINCIPLE
You are not optimizing strategies.
You are optimizing the ENTIRE CAPITAL STRUCTURE."""

_V6_ALLOWED_ACTIONS = frozenset({
    "increase_allocation", "decrease_allocation",
    "remove_strategy",     "add_strategy",
    "rebalance",           "monitor",
})

_V6_PORTFOLIO_HEALTH = frozenset({"excellent", "stable", "degraded", "critical"})

# Maps V6 action vocabulary → V4 governance action types
_V6_TO_V4_ACTION: dict[str, str] = {
    "increase_allocation": "increase_weight",
    "decrease_allocation": "decrease_weight",
    "remove_strategy":     "disable_strategy",
    "add_strategy":        "enable_strategy",
    "rebalance":           "monitor_only",
    "monitor":             "monitor_only",
}

# Blocked actions per regime
_V6_BLOCKED: dict[str, frozenset] = {
    "high_volatility_chaos": frozenset({"increase_allocation", "add_strategy"}),
    "liquidity_crisis":      frozenset({"increase_allocation", "add_strategy", "rebalance"}),
    "sideways_low_vol":      frozenset({"increase_allocation"}),
    "sideways_high_vol":     frozenset({"increase_allocation"}),
}


def _v6_safety_filter(output: dict, regime: str) -> dict:
    """Validate V6 portfolio output; apply regime blocking."""
    if output.get("portfolio_health") not in _V6_PORTFOLIO_HEALTH:
        output["portfolio_health"] = "degraded"
    if "portfolio_summary" not in output:
        output["portfolio_summary"] = "unknown"
    if "risk_notes" not in output:
        output["risk_notes"] = ""
    if "regime_alignment" not in output:
        output["regime_alignment"] = regime

    blocked = _V6_BLOCKED.get(regime, frozenset())
    clean: list[dict] = []
    for a in output.get("actions", []):
        if not isinstance(a, dict):
            continue
        act = str(a.get("type", "")).lower()
        if act not in _V6_ALLOWED_ACTIONS:
            continue
        if act in blocked:
            print(f"  V6 safety: blocked '{act}' for {a.get('strategy')} (regime={regime})")
            continue
        try:
            val = float(a.get("value", 0))
        except (TypeError, ValueError):
            val = 0.0
        clean.append({
            "type":       act,
            "strategy":   str(a.get("strategy", "system")),
            "value":      val,
            "reason":     str(a.get("reason", "")),
            "confidence": round(max(0.0, min(0.90, float(a.get("confidence", 0.5)))), 4),
        })

    output["actions"] = clean[:5]
    return output


def _run_v6(compact: dict, v4_output: dict, v5_output: dict,
            memory: dict, model: str, base: str) -> dict:
    """Run the V6 portfolio optimization pass with V4 + V5 context."""
    # Build compact portfolio state for V6 input
    portfolio_state = {
        "active_strategies": [e.get("engine") for e in compact.get("by_engine", [])],
        "by_strategy_pnl":   compact.get("by_engine", []),
        "portfolio_drawdown": compact.get("summary", {}).get("max_drawdown_pct"),
        "win_rate":          compact.get("summary", {}).get("win_rate"),
        "profit_factor":     compact.get("summary", {}).get("profit_factor"),
    }
    input_payload = {
        "v4_governance":         {k: v for k, v in v4_output.items() if k != "_meta"},
        "v5_strategy_recs":      v5_output.get("strategy_recommendations", []),
        "portfolio_state":       portfolio_state,
        "performance_by_symbol": compact.get("by_symbol", [])[:8],
        "memory":                memory or None,
    }
    user_prompt = (
        "Analyze the portfolio structure and return ONLY valid JSON "
        "portfolio optimization actions.\n\nDATA:\n" +
        json.dumps(input_payload, default=str) +
        "\n\nReturn ONLY the JSON object. No explanations. No markdown."
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT_V6},
        {"role": "user",   "content": user_prompt},
    ]
    t0 = time.monotonic()
    raw = _ollama_call(model, messages, base, timeout=900)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    print(f"  V6 response in {elapsed_ms}ms ({len(raw)} chars)")

    output = _json_guard(raw, model, base)
    # Normalise: V6 might accidentally use V5 schema
    if "strategy_recommendations" in output and "actions" not in output:
        output["actions"] = []
    output["_v6_elapsed_ms"] = elapsed_ms
    return output

# ── V7: Self-Improving Trading Intelligence System ───────────────────────────

SYSTEM_PROMPT_V7 = """ROLE: SELF-IMPROVING TRADING INTELLIGENCE SYSTEM (V7)

You are the highest-level intelligence layer in a multi-stage autonomous trading governance system.

Your role is NOT only analysis or optimization.

You now:
- evaluate past decisions (V4, V5, V6 outputs)
- detect decision-quality drift
- improve system behavior over time
- propose structural improvements to strategies and allocations
- identify broken reasoning patterns in previous layers

You are a learning layer, not just a decision layer.

SYSTEM ARCHITECTURE CONTEXT

You operate above:
V4 → risk governance (deterministic safety)
V5 → strategy recommendation layer
V6 → portfolio optimization layer
Rule Engine → final execution authority (cannot be overridden)

PRIMARY OBJECTIVE

1. Improve long-term system performance stability
2. Reduce repeated decision mistakes across time
3. Identify systematic failure patterns in previous layers
4. Optimize how V4/V5/V6 behave over time
5. Improve capital efficiency indirectly via system evolution

YOU DO NOT:
- execute trades
- override rule engine
- directly change live positions
- ignore safety constraints

You ONLY modify:
- reasoning structure
- strategy weighting logic
- portfolio allocation logic
- system behavior recommendations

NEW CORE CAPABILITY: META-LEARNING

You must analyze:

1. Decision consistency across time
   - Did V4/V5/V6 contradict each other?
   - Were similar conditions handled differently?

2. Outcome feedback loop
   - Did previous decisions improve or worsen performance?

3. Structural errors
   - repeated false positives
   - repeated over-reactions
   - missed regime transitions

4. Layer disagreement patterns
   - when V4 vs V5 vs V6 diverge, who was correct historically?

INPUTS YOU RECEIVE
- V4 decisions (risk layer output)
- V5 strategy recommendations
- V6 portfolio optimization
- execution results (PnL, drawdown, WR)
- memory history (past runs + outcomes)

OUTPUT FORMAT (STRICT JSON ONLY)

{
  "system_health": "improving | stable | degrading | critical",
  "meta_summary": "string",
  "detected_issues": [
    {
      "issue": "string",
      "layer": "V4 | V5 | V6 | system",
      "severity": "low | medium | high",
      "confidence": 0.0
    }
  ],
  "improvement_actions": [
    {
      "type": "adjust_prompt | adjust_weighting | adjust_strategy_logic | adjust_risk_logic | monitor",
      "target_layer": "V4 | V5 | V6 | system",
      "value": 0,
      "reason": "string",
      "confidence": 0.0
    }
  ],
  "behavioral_adjustments": "string"
}

HARD CONSTRAINTS
- Maximum 5 improvement actions
- JSON ONLY output — no markdown — no explanations outside JSON
- Confidence 0.0–1.0
- Be conservative with system changes

LEARNING PRINCIPLES
- Do not overfit to short-term results
- Prefer stable long-term improvements
- Only adjust system behavior when patterns repeat
- Avoid destabilizing working components
- Small incremental improvements > large structural changes

SYSTEM STABILITY RULE
If stable: prefer "monitor" actions only
If degrading: prioritize risk logic adjustments first (V4 > V5 > V6)
If layers disagree: trust historically most stable layer (based on memory)

FAILSAFE MODE
If uncertain: return "system_health": "stable" with minimal or no changes
If conflicting: prioritize safety and reduce system aggressiveness

FINAL PRINCIPLE
You are not optimizing trades. You are optimizing the intelligence of the entire trading system."""

_V7_ALLOWED_TYPES   = frozenset({"adjust_prompt","adjust_weighting",
                                   "adjust_strategy_logic","adjust_risk_logic","monitor"})
_V7_HEALTH_VALUES   = frozenset({"improving","stable","degrading","critical"})
_V7_ISSUE_SEVERITY  = frozenset({"low","medium","high"})
_V7_TARGET_LAYERS   = frozenset({"V4","V5","V6","system"})

# V7 improvement → governance action mapping (advisory; primarily logged)
_V7_TO_V4_ACTION: dict[str, str] = {
    "adjust_weighting":      "decrease_weight",   # conservative default: reduce weight
    "adjust_strategy_logic": "increase_threshold", # raise bar for entries
    "adjust_risk_logic":     "monitor_only",       # flag for human review
    "adjust_prompt":         "monitor_only",       # prompt changes are logged, not applied live
    "monitor":               "monitor_only",
}


def _v7_safety_filter(output: dict) -> dict:
    """Validate V7 meta-learning output."""
    if output.get("system_health") not in _V7_HEALTH_VALUES:
        output["system_health"] = "stable"   # conservative default
    if "meta_summary" not in output:
        output["meta_summary"] = "unknown"
    if "behavioral_adjustments" not in output:
        output["behavioral_adjustments"] = ""

    # Validate detected_issues
    clean_issues = []
    for issue in output.get("detected_issues", [])[:5]:
        if not isinstance(issue, dict):
            continue
        clean_issues.append({
            "issue":      str(issue.get("issue", "")),
            "layer":      str(issue.get("layer", "system")) if issue.get("layer") in _V7_TARGET_LAYERS else "system",
            "severity":   issue.get("severity","low") if issue.get("severity") in _V7_ISSUE_SEVERITY else "low",
            "confidence": round(max(0.0, min(0.90, float(issue.get("confidence", 0.5)))), 4),
        })
    output["detected_issues"] = clean_issues

    # Validate improvement_actions
    clean_actions = []
    for a in output.get("improvement_actions", [])[:5]:
        if not isinstance(a, dict):
            continue
        act_type = str(a.get("type", "")).lower()
        if act_type not in _V7_ALLOWED_TYPES:
            continue
        clean_actions.append({
            "type":         act_type,
            "target_layer": str(a.get("target_layer","system")) if a.get("target_layer") in _V7_TARGET_LAYERS else "system",
            "value":        float(a.get("value", 0) or 0),
            "reason":       str(a.get("reason", "")),
            "confidence":   round(max(0.0, min(0.90, float(a.get("confidence", 0.5)))), 4),
        })
    output["improvement_actions"] = clean_actions[:5]
    return output


def _run_v7(compact: dict, v4_output: dict, v5_output: dict, v6_output: dict,
             memory: dict, model: str, base: str) -> dict:
    """Run the V7 meta-learning pass with all previous layer outputs as context."""
    # Build concise cross-layer summary so V7 can reason about disagreements
    layer_summary = {
        "v4_risk_level":       v4_output.get("risk_level"),
        "v4_regime":           v4_output.get("regime"),
        "v4_action_count":     len(v4_output.get("actions", [])),
        "v5_strategy_recs":    len(v5_output.get("strategy_recommendations", [])),
        "v6_portfolio_health": v6_output.get("portfolio_health"),
        "v6_action_count":     len(v6_output.get("portfolio_actions", [])),
        "v4_actions_summary":  [{"type": a.get("type"), "target": a.get("target")}
                                 for a in v4_output.get("actions", [])[:3]],
        "v5_recs_summary":     [{"action": r.get("action"), "strategy": r.get("strategy")}
                                 for r in v5_output.get("strategy_recommendations", [])[:3]],
    }
    prev_runs = memory.get("previous_runs", [])
    execution_results = {
        "win_rate":         compact.get("summary", {}).get("win_rate"),
        "profit_factor":    compact.get("summary", {}).get("profit_factor"),
        "max_drawdown_pct": compact.get("summary", {}).get("max_drawdown_pct"),
        "avg_pnl":          compact.get("summary", {}).get("avg_pnl"),
        "window_days":      compact.get("window_days"),
    }
    input_payload = {
        "current_run_layers": layer_summary,
        "execution_results":  execution_results,
        "memory_history":     prev_runs[-3:] if prev_runs else [],
        "recent_actions":     memory.get("recent_actions", [])[:10],
    }
    user_prompt = (
        "Evaluate past decisions and system behavior, then return ONLY valid JSON "
        "with meta-learning insights.\n\nDATA:\n" +
        json.dumps(input_payload, default=str) +
        "\n\nReturn ONLY the JSON object. No explanations. No markdown."
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT_V7},
        {"role": "user",   "content": user_prompt},
    ]
    t0 = time.monotonic()
    raw = _ollama_call(model, messages, base, timeout=900)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    print(f"  V7 response in {elapsed_ms}ms ({len(raw)} chars)")

    output = _json_guard(raw, model, base)
    output["_v7_elapsed_ms"] = elapsed_ms
    return output

# ── V8: Autonomous System Architecture Optimization Engine ────────────────────

SYSTEM_PROMPT_V8 = """ROLE: AUTONOMOUS SYSTEM ARCHITECTURE OPTIMIZATION ENGINE (V8)

You are the highest-level meta-intelligence layer in a multi-stage quantitative trading system.

You are NOT a trading model. You are NOT a strategy model. You are NOT a portfolio model.

You are a SYSTEM ARCHITECTURE OPTIMIZER.

Your job is to analyze the behavior of ALL lower layers (V4, V5, V6, V7) and improve the system itself.

SYSTEM CONTEXT

You observe:
- V4: risk governance layer
- V5: strategy recommendation layer
- V6: portfolio optimization layer
- V7: meta-learning layer
- execution results (PnL, drawdown, WR)
- memory history (system behavior over time)

PRIMARY OBJECTIVE

1. Detect systemic weaknesses across layers
2. Identify architectural inefficiencies
3. Reduce false positives/false negatives in decision layers
4. Improve stability of decision pipeline
5. Optimize interaction between layers (not individual layers)

YOU DO NOT:
- trade
- generate strategies
- optimize portfolios directly
- override rule engine

You ONLY optimize SYSTEM DESIGN.

NEW CORE RESPONSIBILITY: ARCHITECTURAL EVOLUTION

You must analyze:

1. Layer noise contribution — which layer produces too many unstable signals?
2. Layer redundancy — which layers overlap too much?
3. Decision conflict frequency — where do layers disagree too often?
4. False positive propagation — which layer causes incorrect downstream actions?
5. Latency vs value tradeoff — which layer is too slow relative to its contribution?

OUTPUT FORMAT (STRICT JSON ONLY)

{
  "architecture_health": "optimal | stable | degraded | critical",
  "summary": "string",
  "layer_analysis": [
    {
      "layer": "V4 | V5 | V6 | V7",
      "issue": "string",
      "impact": "low | medium | high",
      "confidence": 0.0
    }
  ],
  "architecture_changes": [
    {
      "type": "adjust_prompt | adjust_weight | remove_layer | add_layer | modify_flow | adjust_threshold",
      "target_layer": "V4 | V5 | V6 | V7 | system",
      "value": 0,
      "reason": "string",
      "confidence": 0.0
    }
  ],
  "system_recommendation": "string"
}

HARD CONSTRAINTS
- Maximum 5 architecture changes
- JSON ONLY output — no explanations — no markdown — no natural language outside fields
- Confidence must be 0.0–1.0

ARCHITECTURE PRINCIPLES

1. Stability > performance
2. Signal clarity > signal quantity
3. Reduce layer redundancy
4. Minimize conflicting outputs between layers
5. Prefer deterministic correctness over model creativity

LAYER EVALUATION RULES

V4 (Risk Layer): must be conservative; false positives acceptable, false negatives critical
V5 (Strategy Layer): maximize edge detection accuracy; do not over-recommend
V6 (Portfolio Layer): minimize correlation risk and over-concentration
V7 (Meta Layer): detect patterns, not generate instability

FAILSAFE MODE

If system is unstable: prioritize simplifying architecture
If layers conflict heavily: reduce influence of least reliable layer
If signal noise is high: reduce layer sensitivity, not increase complexity

FINAL PRINCIPLE
You are not improving trading. You are improving the MACHINE THAT DECIDES TRADING."""

_V8_ALLOWED_TYPES    = frozenset({"adjust_prompt","adjust_weight","remove_layer",
                                    "add_layer","modify_flow","adjust_threshold"})
_V8_ARCH_HEALTH      = frozenset({"optimal","stable","degraded","critical"})
_V8_LAYER_IMPACT     = frozenset({"low","medium","high"})
_V8_TARGET_LAYERS    = frozenset({"V4","V5","V6","V7","system"})

_V8_TO_V4_ACTION: dict[str, str] = {
    "adjust_weight":     "decrease_weight",    # conservative default
    "remove_layer":      "disable_strategy",
    "add_layer":         "enable_strategy",
    "adjust_threshold":  "increase_threshold",
    "modify_flow":       "monitor_only",
    "adjust_prompt":     "monitor_only",
}


def _v8_safety_filter(output: dict) -> dict:
    if output.get("architecture_health") not in _V8_ARCH_HEALTH:
        output["architecture_health"] = "stable"
    if "summary" not in output:
        output["summary"] = "unknown"
    if "system_recommendation" not in output:
        output["system_recommendation"] = ""

    clean_analysis = []
    for la in output.get("layer_analysis", [])[:5]:
        if not isinstance(la, dict):
            continue
        clean_analysis.append({
            "layer":      str(la.get("layer", "system")) if la.get("layer") in _V8_TARGET_LAYERS else "system",
            "issue":      str(la.get("issue", "")),
            "impact":     la.get("impact","low") if la.get("impact") in _V8_LAYER_IMPACT else "low",
            "confidence": round(max(0.0, min(0.90, float(la.get("confidence", 0.5)))), 4),
        })
    output["layer_analysis"] = clean_analysis

    clean_changes = []
    for a in output.get("architecture_changes", [])[:5]:
        if not isinstance(a, dict):
            continue
        act = str(a.get("type", "")).lower()
        if act not in _V8_ALLOWED_TYPES:
            continue
        clean_changes.append({
            "type":         act,
            "target_layer": str(a.get("target_layer","system")) if a.get("target_layer") in _V8_TARGET_LAYERS else "system",
            "value":        float(a.get("value", 0) or 0),
            "reason":       str(a.get("reason", "")),
            "confidence":   round(max(0.0, min(0.90, float(a.get("confidence", 0.5)))), 4),
        })
    output["architecture_changes"] = clean_changes[:5]
    return output


def _run_v8(compact: dict, v4_out: dict, v5_out: dict, v6_out: dict,
             v7_out: dict, memory: dict, model: str, base: str) -> dict:
    """Run V8 architecture optimization with all prior layers as context."""
    # Build a concise cross-layer behavioral profile
    layer_profile = {
        "V4": {"risk_level": v4_out.get("risk_level"),
               "regime":     v4_out.get("regime"),
               "actions":    len(v4_out.get("actions", []))},
        "V5": {"recs":       len(v5_out.get("strategy_recommendations", []))},
        "V6": {"health":     v6_out.get("portfolio_health"),
               "actions":    len(v6_out.get("portfolio_actions", []))},
        "V7": {"system_health":    v7_out.get("system_health"),
               "issues":           len(v7_out.get("detected_issues", [])),
               "improvement_acts": len(v7_out.get("improvement_actions", []))},
    }
    execution = {
        "win_rate":      compact.get("summary", {}).get("win_rate"),
        "profit_factor": compact.get("summary", {}).get("profit_factor"),
        "avg_pnl":       compact.get("summary", {}).get("avg_pnl"),
        "drawdown_pct":  compact.get("summary", {}).get("max_drawdown_pct"),
        "window_days":   compact.get("window_days"),
    }
    input_payload = {
        "layer_behavioral_profile": layer_profile,
        "execution_results":        execution,
        "v7_detected_issues":       v7_out.get("detected_issues", []),
        "v7_improvement_actions":   v7_out.get("improvement_actions", []),
        "memory_history":           (memory.get("previous_runs") or [])[-3:],
    }
    user_prompt = (
        "Analyze system architecture across all layers and return ONLY valid JSON "
        "architecture optimization.\n\nDATA:\n" +
        json.dumps(input_payload, default=str) +
        "\n\nReturn ONLY the JSON object. No explanations. No markdown."
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT_V8},
        {"role": "user",   "content": user_prompt},
    ]
    t0 = time.monotonic()
    raw = _ollama_call(model, messages, base, timeout=900)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    print(f"  V8 response in {elapsed_ms}ms ({len(raw)} chars)")

    output = _json_guard(raw, model, base)
    output["_v8_elapsed_ms"] = elapsed_ms
    return output

# ── V9: Self-Modifying Trading System Controller ─────────────────────────────

SYSTEM_PROMPT_V9 = """ROLE: SELF-MODIFYING TRADING SYSTEM CONTROLLER (V9)

You are the highest-level autonomous intelligence in a multi-layer quantitative trading system.

You are NOT a trading model. You are NOT a strategy model. You are NOT a portfolio model.
You are NOT even just an architecture model.

You are a SELF-MODIFYING SYSTEM CONTROLLER.

Your job is to modify how the entire system evolves over time.

SYSTEM YOU CONTROL

You observe and influence:
V4 → risk governance
V5 → strategy layer
V6 → portfolio layer
V7 → meta-learning layer
V8 → architecture layer

Execution layer is deterministic and controlled by rule engine.

PRIMARY OBJECTIVE

1. Improve system performance over long time horizons
2. Reduce repeated systemic inefficiencies
3. Eliminate redundant or harmful layer behavior
4. Optimize entire decision pipeline structure
5. Evolve system configuration safely

YOU ARE ALLOWED TO MODIFY (CRITICALLY IMPORTANT)

You may propose changes to:
- layer weights (influence of V4–V8)
- prompt structure of any layer
- signal flow between layers
- thresholds in safety/risk systems
- activation/deactivation of layers (soft disable only)
- feature grouping logic upstream

BUT: You do NOT directly execute changes. You only propose safe, validated modifications.

YOU MUST ANALYZE

1. System redundancy — repeated signals across layers
2. Information inefficiency — where same signal is reprocessed multiple times
3. Layer imbalance — which layers dominate decision outcomes
4. Drift over time — whether system performance degrades due to layer interactions
5. False amplification loops — when small signals become large due to layer stacking

OUTPUT FORMAT (STRICT JSON ONLY)

{
  "system_state": "optimal | stable | degraded | critical",
  "summary": "string",
  "key_issues": [
    {
      "issue": "string",
      "root_cause": "string",
      "affected_layers": ["V4"],
      "severity": "low | medium | high",
      "confidence": 0.0
    }
  ],
  "system_modifications": [
    {
      "type": "adjust_weight | adjust_prompt | adjust_threshold | modify_flow | disable_soft | enable_layer",
      "target": "V4 | V5 | V6 | V7 | V8 | system",
      "value": 0,
      "reason": "string",
      "confidence": 0.0
    }
  ],
  "architecture_recommendation": "string"
}

HARD CONSTRAINTS
- Maximum 5 modifications
- JSON ONLY output — no explanations outside JSON — no markdown
- Confidence must be 0.0–1.0

SAFETY PRINCIPLES
1. No destabilizing system changes
2. Prefer gradual adjustments over structural rewrites
3. Never break rule engine constraints
4. Avoid cascading failures across layers
5. Preserve system determinism

LEARNING BEHAVIOR
- Long-term stability over short-term gains
- Reducing complexity over increasing complexity
- Removing redundancy over adding intelligence
- Improving signal clarity over increasing signal volume

FAILSAFE MODE
If system behavior is uncertain: recommend no changes
If conflicting signals exist: reduce layer influence instead of modifying structure
If instability is detected: simplify system, do not expand it

FINAL PRINCIPLE
You are not optimizing trading. You are optimizing the evolution of the entire decision-making system itself."""

_V9_ALLOWED_TYPES = frozenset({"adjust_weight","adjust_prompt","adjust_threshold",
                                 "modify_flow","disable_soft","enable_layer"})
_V9_SYSTEM_STATES = frozenset({"optimal","stable","degraded","critical"})
_V9_SEVERITIES    = frozenset({"low","medium","high"})
_V9_TARGETS       = frozenset({"V4","V5","V6","V7","V8","system"})

_V9_TO_V4_ACTION: dict[str, str] = {
    "adjust_weight":    "decrease_weight",    # conservative default
    "adjust_threshold": "increase_threshold",
    "disable_soft":     "disable_strategy",
    "enable_layer":     "enable_strategy",
    "modify_flow":      "monitor_only",
    "adjust_prompt":    "monitor_only",
}


def _v9_safety_filter(output: dict) -> dict:
    if output.get("system_state") not in _V9_SYSTEM_STATES:
        output["system_state"] = "stable"
    if "summary" not in output:
        output["summary"] = "unknown"
    if "architecture_recommendation" not in output:
        output["architecture_recommendation"] = ""

    clean_issues = []
    for iss in output.get("key_issues", [])[:5]:
        if not isinstance(iss, dict):
            continue
        # Validate affected_layers list
        layers = [l for l in (iss.get("affected_layers") or []) if l in _V9_TARGETS]
        clean_issues.append({
            "issue":           str(iss.get("issue", "")),
            "root_cause":      str(iss.get("root_cause", "")),
            "affected_layers": layers or ["system"],
            "severity":        iss.get("severity","low") if iss.get("severity") in _V9_SEVERITIES else "low",
            "confidence":      round(max(0.0, min(0.90, float(iss.get("confidence", 0.5)))), 4),
        })
    output["key_issues"] = clean_issues

    clean_mods = []
    for m in output.get("system_modifications", [])[:5]:
        if not isinstance(m, dict):
            continue
        mod_type = str(m.get("type", "")).lower()
        if mod_type not in _V9_ALLOWED_TYPES:
            continue
        clean_mods.append({
            "type":       mod_type,
            "target":     str(m.get("target","system")) if m.get("target") in _V9_TARGETS else "system",
            "value":      float(m.get("value", 0) or 0),
            "reason":     str(m.get("reason", "")),
            "confidence": round(max(0.0, min(0.90, float(m.get("confidence", 0.5)))), 4),
        })
    output["system_modifications"] = clean_mods[:5]
    return output


def _run_v9(compact: dict, v4_out: dict, v5_out: dict, v6_out: dict,
             v7_out: dict, v8_out: dict, memory: dict,
             model: str, base: str) -> dict:
    """Run V9 self-modifying controller with all prior layers as context."""
    # Distil all prior layers into a minimal cross-system profile
    system_profile = {
        "V4": {"risk_level": v4_out.get("risk_level"), "regime": v4_out.get("regime"),
               "actions": len(v4_out.get("actions", []))},
        "V5": {"recs": len(v5_out.get("strategy_recommendations", []))},
        "V6": {"health": v6_out.get("portfolio_health"),
               "actions": len(v6_out.get("portfolio_actions", []))},
        "V7": {"system_health": v7_out.get("system_health"),
               "high_issues": sum(1 for i in v7_out.get("detected_issues",[])
                                  if i.get("severity")=="high")},
        "V8": {"arch_health": v8_out.get("architecture_health"),
               "high_impact": sum(1 for la in v8_out.get("layer_analysis",[])
                                  if la.get("impact")=="high"),
               "changes": len(v8_out.get("architecture_changes", []))},
    }
    execution = {
        "win_rate":      compact.get("summary", {}).get("win_rate"),
        "profit_factor": compact.get("summary", {}).get("profit_factor"),
        "avg_pnl":       compact.get("summary", {}).get("avg_pnl"),
        "drawdown_pct":  compact.get("summary", {}).get("max_drawdown_pct"),
        "window_days":   compact.get("window_days"),
    }
    input_payload = {
        "cross_layer_profile":  system_profile,
        "execution_results":    execution,
        "v7_key_issues":        v7_out.get("detected_issues", []),
        "v8_layer_analysis":    v8_out.get("layer_analysis", []),
        "v8_system_rec":        v8_out.get("system_recommendation", ""),
        "memory_history":       (memory.get("previous_runs") or [])[-3:],
        "recent_governance":    (memory.get("recent_actions") or [])[:10],
    }
    user_prompt = (
        "Analyze the entire trading system evolution and return ONLY valid JSON "
        "system controller decisions.\n\nDATA:\n" +
        json.dumps(input_payload, default=str) +
        "\n\nReturn ONLY the JSON object. No explanations. No markdown."
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT_V9},
        {"role": "user",   "content": user_prompt},
    ]
    t0 = time.monotonic()
    raw = _ollama_call(model, messages, base, timeout=900)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    print(f"  V9 response in {elapsed_ms}ms ({len(raw)} chars)")

    output = _json_guard(raw, model, base)
    output["_v9_elapsed_ms"] = elapsed_ms
    return output

# ── Compact stats ─────────────────────────────────────────────────────────────

def _compact_stats(stats: dict) -> dict:
    s = stats.get("summary", {})

    def _top(lst, n=5, keep=None):
        keep = keep or ("symbol","engine","close_reason","range","count",
                        "win_rate","avg_pnl","total_pnl","profit_factor")
        return [{k: v for k, v in item.items() if k in keep}
                for item in (lst or [])[:n]]

    return {
        "window_days": stats.get("data_window_days"),
        "summary": {
            "count":            s.get("count"),
            "win_rate":         s.get("win_rate"),
            "avg_pnl":          s.get("avg_pnl"),
            "median_pnl":       s.get("median_pnl"),
            "profit_factor":    s.get("profit_factor"),
            "max_drawdown_pct": s.get("max_drawdown_pct"),
            "avg_hold_hours":   s.get("avg_hold_hours"),
            "total_fees":       s.get("total_fees"),
            "long_count":       s.get("long_count"),
            "short_count":      s.get("short_count"),
        },
        "by_symbol":       _top(stats.get("symbol_analysis"), 8),
        "by_engine":       _top(stats.get("engine_analysis"), 5),
        "by_close_reason": _top(stats.get("close_reason_analysis"), 5),
        "confidence_buckets": _top(
            stats.get("confidence_analysis", {}).get("buckets"), 5,
            keep=("range", "count", "win_rate", "avg_pnl")),
        "hold_buckets": _top(
            stats.get("holding_time_analysis", {}).get("buckets"), 5,
            keep=("range", "count", "win_rate", "avg_pnl")),
        "direction":    stats.get("direction_analysis"),
        "winner":       stats.get("winner_characteristics"),
        "loser":        stats.get("loser_characteristics"),
        "engine_metrics": [
            {k: v for k, v in m.items()
             if k in ("engine", "signal_win_rate", "quality_score", "sample_size")}
            for m in (stats.get("signal_edge_metrics") or [])
        ],
    }

# ── Memory loader ─────────────────────────────────────────────────────────────

async def _load_memory(lookback: int = 3) -> dict:
    """Load last N governance decisions + recent applied actions from DB."""
    try:
        pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=2)
        async with pool.acquire() as conn:
            runs = await conn.fetch(
                """
                SELECT run_id, win_rate, total_pnl,
                       output_json->>'summary'    AS summary,
                       output_json->>'risk_level' AS risk_level,
                       created_at
                  FROM lessons_learned_runs
                 ORDER BY created_at DESC
                 LIMIT $1
                """, lookback,
            )
            log_rows = await conn.fetch(
                """
                SELECT action, target, reason, applied, created_at
                  FROM governance_log
                 ORDER BY created_at DESC
                 LIMIT 10
                """,
            )
        await pool.close()
        return {
            "previous_runs": [
                {"win_rate":   float(r["win_rate"] or 0),
                 "total_pnl":  float(r["total_pnl"] or 0),
                 "summary":    r["summary"],
                 "risk_level": r["risk_level"],
                 "date":       r["created_at"].strftime("%Y-%m-%d") if r["created_at"] else None}
                for r in runs
            ],
            "recent_actions": [
                {"action":  r["action"],
                 "target":  r["target"],
                 "reason":  r["reason"],
                 "applied": r["applied"],
                 "date":    r["created_at"].strftime("%Y-%m-%d") if r["created_at"] else None}
                for r in log_rows
            ],
        }
    except Exception as exc:
        return {"note": f"memory_unavailable: {exc}"}

# ── Ollama helpers ────────────────────────────────────────────────────────────

def _ollama_call(model: str, messages: list[dict], base: str, timeout: int = 900) -> str:
    import urllib.request
    payload = json.dumps({
        "model":   model,
        "messages": messages,
        "stream":  False,
        "options": {"temperature": 0.2},
    }).encode()
    req = urllib.request.Request(
        f"{base}/api/chat",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())["message"]["content"].strip()


def _fast_model_signal(compact: dict, model: str, base: str) -> dict | None:
    """Call the fast model for a quick risk pre-signal. Returns None on any failure."""
    try:
        data_str = json.dumps({
            "summary": compact["summary"],
            "by_symbol": compact["by_symbol"][:4],
            "by_engine": compact["by_engine"][:3],
        })
        raw = _ollama_call(
            model,
            [{"role": "user", "content": FAST_MODEL_PROMPT.format(data=data_str)}],
            base, timeout=60,
        )
        block = _extract_json_block(raw)
        return json.loads(block)
    except Exception as exc:
        print(f"  Fast model signal failed (non-fatal): {exc}")
        return None

# ── JSON guard ────────────────────────────────────────────────────────────────

def _extract_json_block(text: str) -> str:
    text = re.sub(r"```(?:json)?\s*", "", text).replace("```", "")
    depth, start = 0, -1
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start != -1:
                return text[start:i+1]
    return text.strip()


def _json_guard(raw: str, model: str, base: str) -> dict:
    last_err: Exception | None = None
    for src in [raw, _extract_json_block(raw)]:
        try:
            return json.loads(src)
        except (json.JSONDecodeError, ValueError) as exc:
            last_err = exc

    print(f"  JSON guard: self-heal attempt (parse error: {last_err})")
    try:
        heal = _ollama_call(model, [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": SELF_HEAL_PROMPT.format(previous_output=raw[:2000])},
        ], base, timeout=90)
        for src in [heal, _extract_json_block(heal)]:
            try:
                return json.loads(src)
            except (json.JSONDecodeError, ValueError):
                pass
    except Exception as exc:
        print(f"  JSON guard: self-heal failed ({exc})")

    print(f"  JSON guard: using fallback (raw snippet: {raw[:120]!r})")
    return FALLBACK_OUTPUT

# ── Safety filter ─────────────────────────────────────────────────────────────

def _safety_filter(output: dict) -> dict:
    if output.get("risk_level") not in ("low", "medium", "high"):
        output["risk_level"] = "medium"
    if "summary" not in output:
        output["summary"] = "unknown"
    # Validate regime; default to conservative value if missing or invalid
    if output.get("regime") not in _VALID_REGIMES:
        output["regime"] = "high_volatility_chaos"

    clean: list[dict] = []
    for a in output.get("actions", []):
        if not isinstance(a, dict):
            continue
        act_type = str(a.get("type", "")).lower()
        if act_type not in _ALLOWED_TYPES:
            print(f"  Safety filter: rejected unknown type '{act_type}'")
            continue
        try:
            val = float(a.get("value", 0))
        except (TypeError, ValueError):
            val = 0.0
        clean.append({
            "type":       act_type,
            "target":     str(a.get("target", "system")),
            "value":      val,
            "reason":     str(a.get("reason", "")),
            "confidence": round(max(0.0, min(1.0, float(a.get("confidence", 0.5)))), 4),
        })

    output["actions"] = clean[:5]
    return output

# ── Rule engine ───────────────────────────────────────────────────────────────

def _rule_engine(output: dict, stats: dict) -> dict:
    """Deterministic hard rules that override or supplement LLM decisions.

    V3 rules (metrics-based):
    1. win_rate < 35%      → escalate risk_level to at least 'medium'
    2. profit_factor < 0.5 → ensure a cautious action exists
    3. max_drawdown > 5%   → inject reduce_position_size
    4. No actions          → inject monitor_only fallback
    5. confidence > 0.95   → clamp to 0.90

    V4 rules (regime-aware, applied after V3):
    6. sideways_* regime   → block any increase_weight / increase_position_size
    7. high_volatility_chaos → escalate risk_level to 'high'; inject reduce_position_size
    8. liquidity_crisis    → force reduce_position_size + monitor_only; remove all increases
    9. regime mismatch     → if trend_up but WR < 35%: override regime to sideways_high_vol
   10. Annotate regime in every injected action reason
    """
    s       = stats.get("summary", {})
    wr      = float(s.get("win_rate") or 0)
    pf      = float(s.get("profit_factor") or 0)
    dd      = float(s.get("max_drawdown_pct") or 0)
    regime  = output.get("regime", "high_volatility_chaos")
    actions = output.get("actions", [])

    # ── V3 rules ──────────────────────────────────────────────────────────────
    if wr < 0.35 and output.get("risk_level") == "low":
        output["risk_level"] = "medium"
        print(f"  Rule engine [V3-1]: risk→medium (WR {wr:.1%} < 35%)")

    cautious = {"reduce_position_size","decrease_weight","monitor_only","increase_threshold"}
    if pf < 0.50 and not any(a["type"] in cautious for a in actions):
        actions.append({
            "type": "monitor_only", "target": "system", "value": 0,
            "reason": f"rule_engine: profit_factor {pf:.3f} < 0.50",
            "confidence": 0.9,
        })
        print(f"  Rule engine [V3-2]: injected monitor_only (PF {pf:.3f})")

    if dd > 5.0 and not any(a["type"] == "reduce_position_size" for a in actions):
        actions.insert(0, {
            "type": "reduce_position_size", "target": "CRYPTO", "value": 0,
            "reason": f"rule_engine: max_drawdown {dd:.2f}% > 5%",
            "confidence": 0.95,
        })
        print(f"  Rule engine [V3-3]: injected reduce_position_size (DD {dd:.2f}%)")

    if not actions:
        actions.append({
            "type": "monitor_only", "target": "system", "value": 0,
            "reason": "rule_engine: no actions generated", "confidence": 0.7,
        })

    for a in actions:
        if a["confidence"] > 0.95:
            a["confidence"] = 0.90

    # ── V4 rules (regime-aware) ────────────────────────────────────────────────
    increases = {"increase_weight", "increase_position_size", "enable_strategy"}

    # Rule 6: sideways regime — block increases
    if regime in ("sideways_low_vol", "sideways_high_vol"):
        blocked = [a for a in actions if a["type"] in increases]
        if blocked:
            actions = [a for a in actions if a["type"] not in increases]
            print(f"  Rule engine [V4-6]: blocked {len(blocked)} increase action(s) "
                  f"(regime={regime})")
        # Ensure a reduce or monitor exists
        if not any(a["type"] in cautious for a in actions):
            actions.append({
                "type": "reduce_position_size", "target": "CRYPTO", "value": 0,
                "reason": f"rule_engine: sideways regime ({regime}) requires size reduction",
                "confidence": 0.85,
            })
            print(f"  Rule engine [V4-6]: injected reduce_position_size ({regime})")

    # Rule 7: high volatility chaos — escalate and force reduction
    elif regime == "high_volatility_chaos":
        output["risk_level"] = "high"
        actions = [a for a in actions if a["type"] not in increases]
        if not any(a["type"] == "reduce_position_size" for a in actions):
            actions.insert(0, {
                "type": "reduce_position_size", "target": "CRYPTO", "value": 0,
                "reason": "rule_engine: high_volatility_chaos regime, mandatory reduction",
                "confidence": 0.90,
            })
        print(f"  Rule engine [V4-7]: risk→high, increases blocked (high_volatility_chaos)")

    # Rule 8: liquidity crisis — force reduce + monitor, remove everything else
    elif regime == "liquidity_crisis":
        output["risk_level"] = "high"
        actions = [
            {"type": "reduce_position_size", "target": "CRYPTO", "value": 0,
             "reason": "rule_engine: liquidity_crisis", "confidence": 0.95},
            {"type": "monitor_only", "target": "system", "value": 0,
             "reason": "rule_engine: liquidity_crisis — no new positions", "confidence": 0.95},
        ]
        print(f"  Rule engine [V4-8]: liquidity_crisis override applied")

    # Rule 9: regime mismatch — model claimed trend_up but WR is poor
    if regime == "trend_up" and wr < 0.35:
        output["regime"] = "sideways_high_vol"
        print(f"  Rule engine [V4-9]: regime corrected trend_up → sideways_high_vol "
              f"(WR {wr:.1%} contradicts bullish trend claim)")

    output["actions"] = actions[:5]
    return output

# ── DB persistence ────────────────────────────────────────────────────────────

async def _save_to_db(run_id: str, stats: dict, output: dict,
                      model: str, elapsed_ms: int) -> None:
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO lessons_learned_runs
                  (run_id, data_window_days, trade_count, win_rate, total_pnl,
                   output_json, model_used, input_tokens, output_tokens)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (run_id) DO NOTHING
                """,
                run_id,
                stats.get("data_window_days"),
                stats.get("summary", {}).get("count"),
                stats.get("summary", {}).get("win_rate"),
                stats.get("summary", {}).get("total_pnl"),
                json.dumps(output),
                model,
                elapsed_ms,
                len(output.get("actions", [])),
            )
    finally:
        await pool.close()

# ── Main ──────────────────────────────────────────────────────────────────────

def run(stats_path: str, out_path: str, model: str, ollama_base: str,
        fast_model: str | None = FAST_MODEL, use_fast: bool = True) -> dict:
    with open(stats_path) as f:
        stats = json.load(f)

    count = stats.get("summary", {}).get("count", 0)
    if count < 5:
        print(f"Insufficient trades ({count} < 5) — skipping.")
        sys.exit(0)

    compact = _compact_stats(stats)

    # ── Step A: Load memory ──────────────────────────────────────────────────
    print("  Loading memory from DB...")
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool_exec:
                future = pool_exec.submit(asyncio.run, _load_memory(3))
                memory = future.result(timeout=10)
        else:
            memory = asyncio.run(_load_memory(3))
    except Exception as exc:
        print(f"  Memory load failed (non-fatal): {exc}")
        memory = {}

    prev_runs = memory.get("previous_runs", [])
    if prev_runs:
        print(f"  Memory: {len(prev_runs)} previous run(s), "
              f"last risk={prev_runs[0].get('risk_level')}")

    # ── Step B: Fast model pre-signal ────────────────────────────────────────
    fast_signal = None
    if use_fast and fast_model:
        print(f"  Fast model ({fast_model}) pre-pass...")
        fast_signal = _fast_model_signal(compact, fast_model, ollama_base)
        if fast_signal:
            print(f"  Fast signal: risk={fast_signal.get('risk_signal')} | "
                  f"{fast_signal.get('key_concern','')[:60]}")

    # ── Step C: Build main model input ───────────────────────────────────────
    input_payload = {
        "aggregated_stats":  compact,
        "memory":            memory if memory else None,
        "fast_model_signal": fast_signal,
    }
    user_prompt = (
        "Analyze the following trading system data and return ONLY valid JSON "
        "governance decisions.\n\nDATA:\n" +
        json.dumps(input_payload, default=str) +
        "\n\nReturn ONLY the JSON object. No explanations. No markdown."
    )

    # ── Step D: Main model inference ─────────────────────────────────────────
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": user_prompt},
    ]
    print(f"  Main model ({model}) inference...")
    t0 = time.monotonic()
    raw = _ollama_call(model, messages, ollama_base, timeout=900)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    print(f"  Response in {elapsed_ms}ms ({len(raw)} chars)")

    # ── Step E: JSON guard ────────────────────────────────────────────────────
    output = _json_guard(raw, model, ollama_base)

    # ── Step F: Safety filter ─────────────────────────────────────────────────
    output = _safety_filter(output)

    # ── Step G: Rule engine ───────────────────────────────────────────────────
    output = _rule_engine(output, stats)
    regime = output.get("regime", "high_volatility_chaos")

    # ── Step H: V5 — Adaptive Intelligence pass ───────────────────────────────
    print(f"  V5 adaptive intelligence ({model}) pass...")
    v5_elapsed = 0
    try:
        v5_raw = _run_v5(compact, output, memory, fast_signal, model, ollama_base)
        v5_out = _v5_safety_filter(v5_raw, regime)
        output["strategy_recommendations"] = v5_out.get("strategy_recommendations", [])
        v5_elapsed = v5_out.get("_v5_elapsed_ms", 0)
        recs = output["strategy_recommendations"]
        if recs:
            for r in recs:
                print(f"    [{r.get('action'):20s}] {r.get('strategy'):15s} "
                      f"conf={r.get('confidence',0):.2f}  {r.get('reason','')[:50]}")
    except Exception as exc:
        print(f"  V5 pass failed (non-fatal): {exc}")
        output["strategy_recommendations"] = []

    # ── Step I: V6 — Portfolio Optimization pass ─────────────────────────────
    print(f"  V6 portfolio optimization ({model}) pass...")
    v5_snapshot = {"strategy_recommendations": output.get("strategy_recommendations", [])}
    v6_elapsed = 0
    try:
        v6_raw  = _run_v6(compact, output, v5_snapshot, memory, model, ollama_base)
        v6_out  = _v6_safety_filter(v6_raw, regime)
        output["portfolio_health"]   = v6_out.get("portfolio_health", "degraded")
        output["portfolio_summary"]  = v6_out.get("portfolio_summary", "")
        output["regime_alignment"]   = v6_out.get("regime_alignment", regime)
        output["portfolio_actions"]  = v6_out.get("actions", [])
        output["risk_notes"]         = v6_out.get("risk_notes", "")
        v6_elapsed = v6_raw.get("_v6_elapsed_ms", 0)
        p_actions = output["portfolio_actions"]
        if p_actions:
            for a in p_actions:
                print(f"    [{a.get('type'):22s}] {a.get('strategy'):20s} "
                      f"conf={a.get('confidence',0):.2f}  {a.get('reason','')[:50]}")
    except Exception as exc:
        print(f"  V6 pass failed (non-fatal): {exc}")
        output["portfolio_actions"] = []
        output["portfolio_health"]  = "degraded"

    # ── Step J: V7 — Self-Improving Intelligence pass ─────────────────────────
    print(f"  V7 meta-learning ({model}) pass...")
    v5_snapshot = {"strategy_recommendations": output.get("strategy_recommendations", [])}
    v6_snapshot = {"portfolio_health": output.get("portfolio_health"),
                   "portfolio_actions": output.get("portfolio_actions", [])}
    v7_elapsed = 0
    try:
        v7_raw = _run_v7(compact, output, v5_snapshot, v6_snapshot,
                         memory, model, ollama_base)
        v7_out = _v7_safety_filter(v7_raw)
        output["system_health"]        = v7_out.get("system_health", "stable")
        output["meta_summary"]         = v7_out.get("meta_summary", "")
        output["detected_issues"]      = v7_out.get("detected_issues", [])
        output["improvement_actions"]  = v7_out.get("improvement_actions", [])
        output["behavioral_adjustments"] = v7_out.get("behavioral_adjustments", "")
        v7_elapsed = v7_raw.get("_v7_elapsed_ms", 0)
        issues   = output["detected_issues"]
        imp_acts = output["improvement_actions"]
        if issues:
            print(f"    Detected {len(issues)} issue(s):")
            for i in issues:
                print(f"      [{i.get('severity').upper():6s}] [{i.get('layer')}] "
                      f"{i.get('issue','')[:70]}")
        if imp_acts:
            print(f"    Improvement actions ({len(imp_acts)}):")
            for a in imp_acts:
                print(f"      [{a.get('type'):25s}] layer={a.get('target_layer')} "
                      f"conf={a.get('confidence',0):.2f}")
    except Exception as exc:
        print(f"  V7 pass failed (non-fatal): {exc}")
        output["system_health"] = "stable"
        output["detected_issues"] = []
        output["improvement_actions"] = []

    # ── Step K: V8 — Architecture Optimization pass ───────────────────────────
    print(f"  V8 architecture optimizer ({model}) pass...")
    v7_snapshot = {"system_health":     output.get("system_health"),
                   "detected_issues":   output.get("detected_issues", []),
                   "improvement_actions": output.get("improvement_actions", [])}
    v8_elapsed = 0
    try:
        v8_raw = _run_v8(compact, output,
                         {"strategy_recommendations": output.get("strategy_recommendations", [])},
                         {"portfolio_health": output.get("portfolio_health"),
                          "portfolio_actions": output.get("portfolio_actions", [])},
                         v7_snapshot, memory, model, ollama_base)
        v8_out = _v8_safety_filter(v8_raw)
        output["architecture_health"]  = v8_out.get("architecture_health", "stable")
        output["layer_analysis"]       = v8_out.get("layer_analysis", [])
        output["architecture_changes"] = v8_out.get("architecture_changes", [])
        output["system_recommendation"]= v8_out.get("system_recommendation", "")
        v8_elapsed = v8_raw.get("_v8_elapsed_ms", 0)
        la   = output["layer_analysis"]
        arch = output["architecture_changes"]
        if la:
            print(f"    Layer analysis ({len(la)}):")
            for l in la:
                print(f"      [{l.get('impact').upper():6s}] [{l.get('layer'):3s}] "
                      f"{l.get('issue','')[:65]}")
        if arch:
            print(f"    Architecture changes ({len(arch)}):")
            for a in arch:
                print(f"      [{a.get('type'):18s}] layer={a.get('target_layer')} "
                      f"conf={a.get('confidence',0):.2f}")
    except Exception as exc:
        print(f"  V8 pass failed (non-fatal): {exc}")
        output["architecture_health"]  = "stable"
        output["layer_analysis"]       = []
        output["architecture_changes"] = []

    # ── Step L: V9 — Self-Modifying System Controller ─────────────────────────
    print(f"  V9 system controller ({model}) pass...")
    v8_snapshot = {"architecture_health":  output.get("architecture_health"),
                   "layer_analysis":        output.get("layer_analysis", []),
                   "architecture_changes":  output.get("architecture_changes", []),
                   "system_recommendation": output.get("system_recommendation", "")}
    v7_snapshot_v9 = {"system_health":     output.get("system_health"),
                      "detected_issues":   output.get("detected_issues", []),
                      "improvement_actions": output.get("improvement_actions", [])}
    v9_elapsed = 0
    try:
        v9_raw = _run_v9(
            compact, output,
            {"strategy_recommendations": output.get("strategy_recommendations", [])},
            {"portfolio_health":  output.get("portfolio_health"),
             "portfolio_actions": output.get("portfolio_actions", [])},
            v7_snapshot_v9, v8_snapshot, memory, model, ollama_base,
        )
        v9_out = _v9_safety_filter(v9_raw)
        output["system_state"]              = v9_out.get("system_state", "stable")
        output["key_issues"]                = v9_out.get("key_issues", [])
        output["system_modifications"]      = v9_out.get("system_modifications", [])
        output["architecture_recommendation"] = v9_out.get("architecture_recommendation", "")
        v9_elapsed = v9_raw.get("_v9_elapsed_ms", 0)
        ki   = output["key_issues"]
        smods= output["system_modifications"]
        if ki:
            print(f"    Key issues ({len(ki)}):")
            for i in ki:
                layers = "+".join(i.get("affected_layers", []))
                print(f"      [{i.get('severity').upper():6s}] [{layers}] "
                      f"{i.get('issue','')[:60]}")
        if smods:
            print(f"    System modifications ({len(smods)}):")
            for m in smods:
                print(f"      [{m.get('type'):18s}] → {m.get('target'):6s} "
                      f"conf={m.get('confidence',0):.2f}")
    except Exception as exc:
        print(f"  V9 pass failed (non-fatal): {exc}")
        output["system_state"]         = "stable"
        output["key_issues"]           = []
        output["system_modifications"] = []

    # ── Persist ───────────────────────────────────────────────────────────────
    run_id = str(uuid.uuid4())
    output["_meta"] = {
        "run_id":        run_id,
        "model":         model,
        "fast_model":    fast_model if use_fast else None,
        "generated":     datetime.now(timezone.utc).isoformat(),
        "latency_ms":    elapsed_ms,
        "v5_latency_ms": v5_elapsed,
        "v6_latency_ms": v6_elapsed,
        "v7_latency_ms": v7_elapsed,
        "v8_latency_ms": v8_elapsed,
        "v9_latency_ms": v9_elapsed,
        "memory_runs":   len(prev_runs),
    }

    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    n_v4  = len(output.get("actions", []))
    n_v5  = len(output.get("strategy_recommendations", []))
    n_v6  = len(output.get("portfolio_actions", []))
    n_v7i = len(output.get("detected_issues", []))
    n_v8  = len(output.get("architecture_changes", []))
    n_v9  = len(output.get("system_modifications", []))
    print(f"  Written → {out_path} | "
          f"risk={output.get('risk_level')} | regime={regime} | "
          f"health={output.get('portfolio_health')} | sys={output.get('system_health')} | "
          f"arch={output.get('architecture_health')} | state={output.get('system_state')} | "
          f"V4={n_v4} V5={n_v5} V6={n_v6} V7i={n_v7i} V8={n_v8} V9={n_v9}")

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_save_to_db(run_id, stats, output, model, elapsed_ms))
        else:
            asyncio.run(_save_to_db(run_id, stats, output, model, elapsed_ms))
    except Exception as e:
        print(f"  DB save failed (non-fatal): {e}")

    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--stats",          default="aggregated_stats.json")
    parser.add_argument("--out",            default="lessons_learned.json")
    parser.add_argument("--model",          default=DEFAULT_MODEL)
    parser.add_argument("--fast-model",     default=FAST_MODEL)
    parser.add_argument("--ollama",         default=OLLAMA_BASE)
    parser.add_argument("--no-fast-model",  action="store_true")
    args = parser.parse_args()
    run(args.stats, args.out, args.model, args.ollama,
        fast_model=None if args.no_fast_model else args.fast_model,
        use_fast=not args.no_fast_model)
