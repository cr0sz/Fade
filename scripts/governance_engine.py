"""Governance Engine — Lessons Learned pipeline step 3.

Reads lessons_learned.json, applies governance actions to the trading system:
  - Redis: engine exclusions, asset bans, threshold overrides
  - .env: threshold and sizing updates
  - DB: audit log in governance_log table
  - Telegram: notification of applied changes

Usage:
    python scripts/governance_engine.py
    python scripts/governance_engine.py --lessons /tmp/lessons.json --dry-run
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
import redis.asyncio as aioredis

DB_DSN = os.getenv(
    "DATABASE_URL",
    f"postgresql://{os.getenv('POSTGRES_USER','mktintel')}:{os.getenv('POSTGRES_PASSWORD','bekir1905')}"
    f"@{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}"
    f"/{os.getenv('POSTGRES_DB','mktintel')}",
)
REDIS_URL  = f"redis://:{os.getenv('REDIS_PASSWORD','bekir1905')}@{os.getenv('REDIS_HOST','localhost')}:{os.getenv('REDIS_PORT','6379')}/0"
ENV_FILE   = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_ALERTS", "")

# Redis keys (matches paper_trading_engine/engine.py)
_DYNAMIC_EXCLUSIONS_KEY = "trading:excluded_engines"
_DYNAMIC_BANS_KEY       = "trading:dynamic_bans"

# Engines that may never be excluded — mirrors _PROTECTED_ENGINES in
# paper_trading_engine/engine.py. TrendDetectionEngine produces ~57% of all
# signals; excluding it silently halts the bot (Sprint 34 incident). Governance
# may still penalise these via engine weight multipliers.
_PROTECTED_ENGINES = frozenset({"TrendDetectionEngine"})

# Whitelisted .env keys that governance engine may update
_ENV_WRITABLE = {
    "SCORE_SHORT_THRESHOLD",
    "SCORE_LONG_THRESHOLD",
    "TRAIL_STOP_PCT",
    "TRAIL_ACTIVATE_PCT",
    "MAX_DAILY_LOSS",
}

# Redis key prefix for governance hot-reload (must match engine.py _GOV_OVERRIDE_PREFIX)
_GOV_OVERRIDE_PREFIX = "trading:gov_override:"
_GOV_RELOAD_TTL      = 86400  # 24h expiry

# Safety bounds — governance engine cannot push params outside these
_PARAM_BOUNDS: dict[str, tuple[float, float]] = {
    "SCORE_SHORT_THRESHOLD": (-0.40, -0.08),
    "SCORE_LONG_THRESHOLD":  (0.08,  0.40),
    "TRAIL_STOP_PCT":        (0.8,   4.0),
    "TRAIL_ACTIVATE_PCT":    (0.2,   2.0),
    "MAX_DAILY_LOSS":        (50.0,  500.0),
}

# Max adjustment per governance run (prevent runaway changes)
_MAX_DELTA = 0.05   # 5% or 0.05 units max per action


class GovernanceEngine:
    def __init__(self, pool: asyncpg.Pool, redis, run_id: str, dry_run: bool):
        self._pool    = pool
        self._redis   = redis
        self._run_id  = run_id
        self._dry_run = dry_run
        self._applied: list[dict] = []
        self._skipped: list[dict] = []

    async def apply_all(self, actions: list[dict]) -> None:
        for action in actions:
            await self._apply_action(action)

    async def _apply_action(self, action: dict) -> None:
        # Support both old schema ("action" key) and new schema ("type" key)
        act_type = (action.get("type") or action.get("action") or "").lower()
        target   = action.get("target", "")
        reason   = action.get("reason", "")
        evidence = action.get("evidence", "")
        effect   = action.get("expected_effect", "")

        handler = {
            "disable_strategy":      self._disable_strategy,
            "enable_strategy":       self._enable_strategy,
            "monitor_only":          self._monitor_only,
            "increase_weight":       self._adjust_weight,
            "decrease_weight":       self._adjust_weight,
            "increase_threshold":    self._adjust_threshold,
            "decrease_threshold":    self._adjust_threshold,
            "reduce_position_size":  self._adjust_size,
            "increase_position_size":self._adjust_size,
        }.get(act_type)

        if handler is None:
            await self._log(action, applied=False, skip_reason=f"unknown action type: {act_type}")
            return

        try:
            skip_reason = await handler(action)
            applied = skip_reason is None
            await self._log(action, applied=applied, skip_reason=skip_reason)
        except Exception as exc:
            await self._log(action, applied=False, skip_reason=f"exception: {exc}")

    async def _disable_strategy(self, action: dict) -> str | None:
        target = action["target"]
        reason = action.get("reason", "governance_engine")
        if target in _PROTECTED_ENGINES:
            return (f"{target} is protected (sole signal source — excluding it "
                    f"halts all trading); use decrease_weight instead")
        # Treat as a dynamic exclusion (Redis) not a hardcoded change
        if self._dry_run:
            print(f"[DRY-RUN] disable_strategy: {target}")
            return "dry_run"
        ban_payload = json.dumps({"reason": f"governance: {reason}", "ts": time.time()})
        # If target looks like a symbol (short name), add to dynamic bans
        if len(target) <= 10 and target.upper() == target:
            await self._redis.hset(_DYNAMIC_BANS_KEY, target.upper(), ban_payload)
            self._applied.append({"action": "disable_strategy", "target": target,
                                   "mechanism": "dynamic_asset_ban"})
        else:
            # Engine name — add to dynamic engine exclusions
            await self._redis.sadd(_DYNAMIC_EXCLUSIONS_KEY, target)
            self._applied.append({"action": "disable_strategy", "target": target,
                                   "mechanism": "dynamic_engine_exclusion"})
        return None

    async def _enable_strategy(self, action: dict) -> str | None:
        target = action["target"]
        if self._dry_run:
            print(f"[DRY-RUN] enable_strategy: {target}")
            return "dry_run"
        # Remove from dynamic exclusions / bans
        await self._redis.srem(_DYNAMIC_EXCLUSIONS_KEY, target)
        await self._redis.hdel(_DYNAMIC_BANS_KEY, target.upper())
        self._applied.append({"type": "enable_strategy", "target": target})
        return None

    async def _monitor_only(self, action: dict) -> str | None:
        # monitor_only is a no-op — logged as applied=false with skip_reason to
        # prevent false confidence from a filled applied list.
        return "no_op"

    async def _adjust_weight(self, action: dict) -> str | None:
        target  = action["target"]
        act     = (action.get("type") or action.get("action", "")).lower()
        if self._dry_run:
            print(f"[DRY-RUN] {act}: {target}")
            return "dry_run"
        key  = "trading:engine_weights:governance"
        # Recompute from neutral (1.0) each run — do NOT read the existing Redis value.
        # Reading and accumulating the stored weight causes multiplicative drift: three
        # consecutive "increase_weight" calls would push the multiplier to 1.3 instead
        # of the intended 1.1.  Each governance run expresses a one-shot directional
        # opinion relative to neutral; meta_strategy.py delete-then-sets FINAL_KEY each
        # cycle using GOV × META, so the composed weight resets cleanly every cycle.
        delta = 0.10 if "increase" in act else -0.10
        w = max(0.0, min(2.0, 1.0 + delta))
        await self._redis.hset(key, target, str(round(w, 3)))
        self._applied.append({"action": act, "target": target, "new_weight": w})
        return None

    async def _adjust_threshold(self, action: dict) -> str | None:
        target = action["target"]
        act    = (action.get("type") or action.get("action", "")).lower()
        # Map target names to .env keys
        key_map = {
            "short_threshold":   "SCORE_SHORT_THRESHOLD",
            "long_threshold":    "SCORE_LONG_THRESHOLD",
            "confidence":        "SCORE_SHORT_THRESHOLD",
            "trail_stop":        "TRAIL_STOP_PCT",
            "trail_activate":    "TRAIL_ACTIVATE_PCT",
        }
        env_key = key_map.get(target.lower()) or (target if target in _ENV_WRITABLE else None)
        if not env_key:
            return f"unknown threshold target: {target}"

        env_lines = _read_env(ENV_FILE)
        curr_val = _get_env_value(env_lines, env_key)
        if curr_val is None:
            return f"{env_key} not found in .env"

        delta = _MAX_DELTA if "increase" in act else -_MAX_DELTA
        new_val = round(float(curr_val) + delta, 4)

        bounds = _PARAM_BOUNDS.get(env_key)
        if bounds:
            lo, hi = bounds
            if not (lo <= new_val <= hi):
                return f"{env_key} new value {new_val} outside bounds [{lo},{hi}]"

        if self._dry_run:
            print(f"[DRY-RUN] {act}: {env_key} {curr_val} → {new_val}")
            return "dry_run"

        env_lines = _set_env_value(env_lines, env_key, str(new_val))
        with open(ENV_FILE, "w") as f:
            f.writelines(env_lines)

        # Hot-reload: write to Redis so the engine picks up the change without restart.
        # Store the raw numeric value; engine applies its own /100 scale for pct params.
        try:
            await self._redis.set(
                f"{_GOV_OVERRIDE_PREFIX}{env_key}", str(new_val), ex=_GOV_RELOAD_TTL
            )
        except Exception as exc:
            print(f"governance_redis_override_failed: {exc}")

        # Effectiveness verification: read back from both .env and Redis.
        verified_env   = _get_env_value(_read_env(ENV_FILE), env_key)
        verified_redis = None
        try:
            verified_redis = await self._redis.get(f"{_GOV_OVERRIDE_PREFIX}{env_key}")
        except Exception:
            pass
        env_ok   = str(verified_env) == str(new_val)
        redis_ok = str(verified_redis) == str(new_val)
        print(f"  governance_verify {env_key}: env={verified_env}({'✅' if env_ok else '❌'}) "
              f"redis={verified_redis}({'✅' if redis_ok else '❌'})")

        self._applied.append({"action": act, "env_key": env_key,
                               "old_value": curr_val, "new_value": new_val,
                               "env_verified": env_ok, "redis_verified": redis_ok})
        return None

    async def _adjust_size(self, action: dict) -> str | None:
        target  = action["target"]
        act     = (action.get("type") or action.get("action", "")).lower()
        if self._dry_run:
            print(f"[DRY-RUN] {act}: {target}")
            return "dry_run"
        key  = f"trading:size_override:{target.upper()}"
        curr_bytes = await self._redis.get(key)
        curr = float(curr_bytes) if curr_bytes else 1.0
        delta = 0.10 if "increase" in act else -0.10
        new  = max(0.2, min(2.0, curr + delta))
        await self._redis.set(key, str(round(new, 3)))
        # Effectiveness verification
        verified = await self._redis.get(key)
        verified_ok = str(verified) == str(round(new, 3))
        print(f"  governance_verify {key}: {verified}({'✅' if verified_ok else '❌'})")
        self._applied.append({"type": act, "target": target, "new_multiplier": new,
                               "redis_verified": verified_ok})
        return None

    async def _log(self, action: dict, applied: bool, skip_reason: str | None) -> None:
        act_type = action.get("type") or action.get("action") or "unknown"
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO governance_log
                      (run_id, action, target, target_type, reason, evidence,
                       expected_effect, applied, applied_at, skipped_reason)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    """,
                    self._run_id,
                    act_type,
                    action.get("target"),
                    action.get("target_type"),
                    action.get("reason"),
                    str(action.get("evidence", action.get("value", ""))),
                    action.get("expected_effect"),
                    applied,
                    datetime.now(timezone.utc) if applied else None,
                    skip_reason,
                )
        except Exception as exc:
            print(f"governance_log write failed: {exc}")

    def summary(self) -> dict:
        return {"run_id": self._run_id, "applied": self._applied, "skipped": self._skipped}


# ── .env helpers ──────────────────────────────────────────────────────────────

def _read_env(path: str) -> list[str]:
    try:
        with open(path) as f:
            return f.readlines()
    except FileNotFoundError:
        return []


def _get_env_value(lines: list[str], key: str) -> str | None:
    pattern = re.compile(rf"^{re.escape(key)}\s*=\s*(.*)$")
    for line in lines:
        m = pattern.match(line.strip())
        if m:
            return m.group(1).strip()
    return None


def _set_env_value(lines: list[str], key: str, value: str) -> list[str]:
    pattern = re.compile(rf"^{re.escape(key)}\s*=")
    for i, line in enumerate(lines):
        if pattern.match(line):
            lines[i] = f"{key}={value}\n"
            return lines
    lines.append(f"{key}={value}\n")
    return lines


# ── Discord ───────────────────────────────────────────────────────────────────

async def _send_discord(message: str) -> None:
    if not DISCORD_WEBHOOK:
        return
    import aiohttp
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(DISCORD_WEBHOOK, json={"content": message[:2000]}, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        print(f"discord_send_error: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def run(lessons_path: str, dry_run: bool) -> None:
    with open(lessons_path) as f:
        lessons = json.load(f)

    # Support both new schema ("actions") and old schema ("governance_actions")
    actions = list(lessons.get("actions") or lessons.get("governance_actions") or [])

    # Merge V5 strategy_recommendations (converted to V4 action format)
    _v5_map = {
        "increase_exposure": "increase_weight",
        "decrease_exposure": "decrease_weight",
        "disable":           "disable_strategy",
        "enable":            "enable_strategy",
        "monitor":           "monitor_only",
    }
    for r in lessons.get("strategy_recommendations", []):
        act = _v5_map.get(str(r.get("action", "")).lower(), "monitor_only")
        actions.append({
            "type":       act,
            "target":     r.get("strategy", "system"),
            "value":      0,
            "reason":     f"V5/{r.get('regime','?')}: {r.get('reason','')}",
            "confidence": float(r.get("confidence", 0.5)),
        })

    # Merge V6 portfolio_actions (converted to V4 action format)
    _v6_map = {
        "increase_allocation": "increase_weight",
        "decrease_allocation": "decrease_weight",
        "remove_strategy":     "disable_strategy",
        "add_strategy":        "enable_strategy",
        "rebalance":           "monitor_only",
        "monitor":             "monitor_only",
    }
    for a in lessons.get("portfolio_actions", []):
        act = _v6_map.get(str(a.get("type", "")).lower(), "monitor_only")
        actions.append({
            "type":       act,
            "target":     a.get("strategy", "system"),
            "value":      float(a.get("value", 0)),
            "reason":     f"V6: {a.get('reason','')}",
            "confidence": float(a.get("confidence", 0.5)),
        })

    # Merge V7 improvement_actions (meta-learning layer — advisory only)
    # Primarily logged; only "monitor_only" and conservative weight adjustments applied.
    _v7_map = {
        "adjust_weighting":      "decrease_weight",   # conservative: reduce weight of target layer
        "adjust_strategy_logic": "increase_threshold",
        "adjust_risk_logic":     "monitor_only",
        "adjust_prompt":         "monitor_only",      # prompt changes are logged, not live-applied
        "monitor":               "monitor_only",
    }
    for a in lessons.get("improvement_actions", []):
        act = _v7_map.get(str(a.get("type", "")).lower(), "monitor_only")
        actions.append({
            "type":       act,
            "target":     a.get("target_layer", "system"),
            "value":      float(a.get("value", 0)),
            "reason":     f"V7: {a.get('reason','')}",
            "confidence": float(a.get("confidence", 0.5)),
        })

    # Log V7 detected_issues directly to governance_log (informational, not applied)
    sys_health = lessons.get("system_health", "stable")
    for issue in lessons.get("detected_issues", []):
        print(f"  V7 issue [{issue.get('severity','?').upper()}] "
              f"[{issue.get('layer','?')}]: {issue.get('issue','')[:80]}")

    # If V7 signals critical system health and no risk reduction exists, inject one
    if sys_health == "critical" and not any(
        a.get("type") in {"reduce_position_size","decrease_weight"} for a in actions
    ):
        actions.insert(0, {
            "type":       "reduce_position_size",
            "target":     "CRYPTO",
            "value":      0,
            "reason":     "V7: system_health=critical, injecting mandatory risk reduction",
            "confidence": 0.90,
        })
        print(f"  Governance: V7 critical health → injected reduce_position_size")

    # Merge V8 architecture_changes (system architecture advisory layer)
    _v8_map = {
        "adjust_weight":    "decrease_weight",    # conservative: reduce weight
        "remove_layer":     "disable_strategy",
        "add_layer":        "enable_strategy",
        "adjust_threshold": "increase_threshold",
        "modify_flow":      "monitor_only",
        "adjust_prompt":    "monitor_only",
    }
    for a in lessons.get("architecture_changes", []):
        act = _v8_map.get(str(a.get("type", "")).lower(), "monitor_only")
        actions.append({
            "type":       act,
            "target":     a.get("target_layer", "system"),
            "value":      float(a.get("value", 0)),
            "reason":     f"V8: {a.get('reason','')}",
            "confidence": float(a.get("confidence", 0.5)),
        })

    # Log V8 layer_analysis as informational
    arch_health = lessons.get("architecture_health", "stable")
    for la in lessons.get("layer_analysis", []):
        print(f"  V8 [{la.get('impact','?').upper():6s}] [{la.get('layer','?')}]: "
              f"{la.get('issue','')[:80]}")

    # If V8 signals critical architecture, inject a system-wide monitor
    if arch_health == "critical" and not any(
        a.get("type") == "monitor_only" and a.get("target") == "system" for a in actions
    ):
        actions.insert(0, {
            "type": "monitor_only", "target": "system", "value": 0,
            "reason": "V8: architecture_health=critical, system-wide monitor injected",
            "confidence": 0.85,
        })

    if not actions:
        print("No governance actions found.")
        return

    run_id = lessons.get("_meta", {}).get("run_id") or str(uuid.uuid4())
    print(f"Governance run {run_id} — {len(actions)} actions")

    pool  = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=2)
    redis = await aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)

    try:
        eng = GovernanceEngine(pool, redis, run_id, dry_run)
        await eng.apply_all(actions)
        summary = eng.summary()

        if not dry_run:
            applied = summary["applied"]
            risk_level = lessons.get("risk_level", "unknown")
            summary_text = lessons.get("summary", "")
            if applied:
                regime = lessons.get("regime", "")
                v5_recs = lessons.get("strategy_recommendations", [])
                lines = [
                    f"🏛️ *Governance Engine — {len(applied)} action(s) applied*",
                    f"Risk: `{risk_level}` | Regime: `{regime}`",
                    f"_{summary_text[:80]}_",
                    "",
                ]
                for a in applied[:5]:
                    t   = a.get("type") or a.get("action", "?")
                    tgt = a.get("target") or a.get("env_key", "?")
                    lines.append(f"• `{t}` → `{tgt}`")
                v5_recs = lessons.get("strategy_recommendations", [])
                if v5_recs:
                    lines.append("")
                    lines.append("*V5 strategy recs:*")
                    for r in v5_recs[:3]:
                        lines.append(f"• `{r.get('action')}` {r.get('strategy')} "
                                     f"({r.get('regime','')})")
                p_health  = lessons.get("portfolio_health", "")
                p_actions = lessons.get("portfolio_actions", [])
                if p_health:
                    lines.append(f"\n*V6 portfolio health:* `{p_health}`")
                if p_actions:
                    lines.append("*V6 portfolio actions:*")
                    for a in p_actions[:3]:
                        lines.append(f"• `{a.get('type')}` {a.get('strategy')}")
                sys_health = lessons.get("system_health", "")
                meta_sum   = lessons.get("meta_summary", "")
                issues     = lessons.get("detected_issues", [])
                if sys_health:
                    lines.append(f"\n*V7 system health:* `{sys_health}`")
                    if meta_sum:
                        lines.append(f"_{meta_sum[:80]}_")
                high_issues = [i for i in issues if i.get("severity") == "high"]
                if high_issues:
                    lines.append("*V7 high-severity issues:*")
                    for i in high_issues[:2]:
                        lines.append(f"• [{i.get('layer')}] {i.get('issue','')[:60]}")
                arch_health  = lessons.get("architecture_health", "")
                sys_rec      = lessons.get("system_recommendation", "")
                arch_changes = lessons.get("architecture_changes", [])
                if arch_health:
                    lines.append(f"\n*V8 arch health:* `{arch_health}`")
                    if sys_rec:
                        lines.append(f"_{sys_rec[:80]}_")
                if arch_changes:
                    lines.append("*V8 architecture changes:*")
                    for a in arch_changes[:2]:
                        lines.append(f"• `{a.get('type')}` [{a.get('target_layer')}]")
                await _send_discord("\n".join(lines))

        print(f"Applied: {len(summary['applied'])} | Skipped: {len(summary['skipped'])}")
        for a in summary["applied"]:
            print(f"  ✅ {json.dumps(a)}")

    finally:
        await pool.close()
        await redis.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lessons",  default="lessons_learned.json")
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args.lessons, args.dry_run))
