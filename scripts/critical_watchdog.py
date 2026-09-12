#!/usr/bin/env python3
"""Critical-only watchdog — DMs the admin via the bot when something drastic happens.

Separate from the noisy Discord channels (which the user muted). Fires ONLY on:
  ENGINE_DOWN      container not running
  ENGINE_FROZEN    running but no idle_tick in 10 min (or error flood)
  BINANCE_DOWN     Binance futures API unreachable
  MONEY_CRASH      daily-loss halt fired, or live realized PnL today <= -$8

Dedup: per-condition state file; re-alert the SAME condition only after COOLDOWN.
Recovery: when a previously-firing condition clears, send one "recovered" DM.

Run from cron (every 10 min). Reads .env for DISCORD_BOT_TOKEN + DISCORD_ADMIN_USER_ID.
"""
import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

# Repo root, resolved from this file, so the script works in any clone.
REPO_ROOT = Path(__file__).resolve().parent.parent

# Docker Compose names containers "<project>-<service>-<n>" and defaults the
# project to the directory name. Override with COMPOSE_PROJECT_NAME if you
# started the stack under a different project name.
PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", REPO_ROOT.name)
ENGINE = f"{PROJECT}-paper-trading-engine-1"
PG = f"{PROJECT}-postgres-1"
STATE_FILE = str(REPO_ROOT / "reports" / "watchdog_state.json")
COOLDOWN_S = 3600          # don't re-DM the same active condition within 1h
UA = "DiscordBot (https://github.com/cr0sz/market-ai, 1.0)"
MONEY_FLOOR = -8.0         # live realized PnL today at/below this $ = crash (cap is -$10)


def sh(cmd: str, timeout: int = 20) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout or "") + (p.stderr or "")
    except Exception as e:
        return 1, str(e)


# ── checks: each returns (is_bad: bool, detail: str) ─────────────────────────

def check_engine_down():
    rc, out = sh(f"docker inspect -f '{{{{.State.Running}}}}' {ENGINE}")
    running = out.strip() == "true"
    return (not running), f"container running={out.strip() or 'missing'}"


def check_engine_frozen():
    # running but no idle_tick in last 10 min => event loop wedged
    rc, out = sh(f"docker logs --since 10m {ENGINE} 2>&1 | grep -c idle_tick")
    ticks = int(out.strip() or "0") if out.strip().isdigit() else 0
    rc2, errs = sh(f"docker logs --since 10m {ENGINE} 2>&1 | grep -c '\"level\": \"error\"'")
    nerr = int(errs.strip() or "0") if errs.strip().isdigit() else 0
    bad = ticks == 0 or nerr >= 20
    return bad, f"idle_ticks_10m={ticks} errors_10m={nerr}"


def check_binance_down():
    try:
        req = urllib.request.Request("https://fapi.binance.com/fapi/v1/ping",
                                     headers={"User-Agent": UA})
        urllib.request.urlopen(req, timeout=10)
        return False, "ping ok"
    except Exception as e:
        return True, f"ping failed: {e}"


def check_money_crash():
    # (a) daily-loss halt event in last 30 min
    rc, out = sh(f"docker logs --since 30m {ENGINE} 2>&1 | grep -c risk_guard_daily_loss_halt")
    halted = (int(out.strip()) if out.strip().isdigit() else 0) > 0
    # (b) live realized PnL today
    sql = ("SELECT COALESCE(ROUND(SUM(pnl)::numeric,2),0) FROM paper_trades "
           "WHERE mode='live' AND closed_at::date = CURRENT_DATE AND pnl IS NOT NULL;")
    rc2, out2 = sh(f"docker exec {PG} psql -U mktintel -d mktintel -tA -c \"{sql}\"")
    try:
        pnl = float(out2.strip().splitlines()[-1]) if out2.strip() else 0.0
    except Exception:
        pnl = 0.0
    bad = halted or pnl <= MONEY_FLOOR
    return bad, f"daily_loss_halt={halted} live_pnl_today=${pnl}"


CHECKS = {
    "ENGINE_DOWN": check_engine_down,
    "ENGINE_FROZEN": check_engine_frozen,
    "BINANCE_DOWN": check_binance_down,
    "MONEY_CRASH": check_money_crash,
}
# If the engine is down, frozen/money checks are noise — suppress them.
SUPPRESS_IF_DOWN = {"ENGINE_FROZEN", "MONEY_CRASH"}


def dm(token: str, user_id: str, content: str) -> None:
    def post(path, payload):
        req = urllib.request.Request(
            f"https://discord.com/api/v10{path}",
            data=json.dumps(payload).encode(),
            headers={"Authorization": f"Bot {token}", "Content-Type": "application/json",
                     "User-Agent": UA}, method="POST")
        return json.load(urllib.request.urlopen(req, timeout=15))
    chan = post("/users/@me/channels", {"recipient_id": user_id})
    post(f"/channels/{chan['id']}/messages", {"content": content})


def main():
    env = {}
    try:
        for line in open(REPO_ROOT / ".env", encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    except Exception:
        pass
    token = env.get("DISCORD_BOT_TOKEN", "").strip()
    admin = env.get("DISCORD_ADMIN_USER_ID", "").strip()
    if not token or not admin:
        print("missing DISCORD_BOT_TOKEN / DISCORD_ADMIN_USER_ID")
        return

    try:
        state = json.load(open(STATE_FILE))
    except Exception:
        state = {}

    now = time.time()
    engine_down = check_engine_down()[0]

    for name, fn in CHECKS.items():
        if engine_down and name in SUPPRESS_IF_DOWN:
            continue
        try:
            bad, detail = fn()
        except Exception as e:
            bad, detail = False, f"check error: {e}"
        prev = state.get(name, {"active": False, "last_dm": 0})

        if bad:
            if (not prev["active"]) or (now - prev.get("last_dm", 0) > COOLDOWN_S):
                dm(token, admin, f"🚨 <@{admin}> **{name}**\n{detail}\n_server: market-intelligence_")
                prev["last_dm"] = now
            prev["active"] = True
        else:
            if prev.get("active"):
                dm(token, admin, f"✅ **{name} recovered**\n{detail}")
                prev["last_dm"] = 0
            prev["active"] = False
        state[name] = prev

    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    json.dump(state, open(STATE_FILE, "w"))


if __name__ == "__main__":
    main()
