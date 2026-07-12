#!/usr/bin/env bash
# Nightly autonomous system diagnosis via headless Claude Code (Sonnet — cheap).
# Runs the /diagnose skill, saves the full report, posts summary to Discord #watchdog.
# Installed in crontab: 30 3 * * * (UTC server time).
set -u
export PATH="/home/bekir/.local/bin:/usr/local/bin:/usr/bin:/bin"
cd /opt/market-intelligence || exit 1

mkdir -p reports/nightly
day=$(date +%F)
out="reports/nightly/diagnose-${day}.md"

claude -p "Run the /diagnose skill for the market-intelligence system and output the complete report in markdown. Start the report with a single verdict line: VERDICT: TRADING | QUIET-BY-DESIGN | DEGRADED | BROKEN. Then add a 'DEPOSIT-READINESS' section reporting, for live mode: (1) 7d net PnL = SUM(pnl)-SUM(fee); (2) count of close_reason='no_momentum' exits in 7d and their total pnl (proves the stalled-short cut is working); (3) count of 'live_orphan_auto_closed' and 'live_reconcile_drift' log events in 24h (should trend to 0 — proves the execution leak stays plugged); (4) count of 'binance_futures_maker_filled' vs market fallbacks in 24h (maker fill rate). Flag NOT-READY if 7d net PnL is negative or orphan/drift events are still recurring." \
  --model claude-sonnet-4-6 \
  --allowedTools "Read,Grep,Glob,Skill,Bash(docker logs *),Bash(docker compose ps*),Bash(docker exec market-intelligence-postgres-1 psql *),Bash(docker exec * psql *),Bash(make check-sql),Bash(python3 scripts/show_effective_config.py*)" \
  > "$out" 2>"reports/nightly/diagnose-${day}.err"
rc=$?

# Discord summary: verdict line + first 1900 chars of report (embed description limit 4096, content limit 2000)
set -a; . ./.env 2>/dev/null; set +a
if [ -n "${DISCORD_WEBHOOK_WATCHDOG:-}" ]; then
  if [ $rc -eq 0 ] && [ -s "$out" ]; then
    discord_src="$out"
    discord_color=3066993
    discord_prefix="🤖 Nightly Claude diagnose ${day}"
  else
    discord_src="reports/nightly/diagnose-${day}.err"
    discord_color=15548997
    discord_prefix="💥 Nightly Claude diagnose ${day} FAILED (rc=${rc})"
  fi
  python3 - <<PYEOF
import json, urllib.request, sys
title = "${discord_prefix}"
try:
    with open("${discord_src}") as f:
        body = f.read(1900)
except Exception as e:
    body = str(e)
payload = json.dumps({"embeds": [{"title": title, "description": body, "color": ${discord_color}}]})
req = urllib.request.Request(
    "${DISCORD_WEBHOOK_WATCHDOG}",
    data=payload.encode(),
    # User-Agent required — Discord's Cloudflare 403s urllib's default UA.
    headers={"Content-Type": "application/json",
             "User-Agent": "DiscordBot (https://github.com/cr0sz/market-ai, 1.0)"},
    method="POST",
)
try:
    urllib.request.urlopen(req, timeout=30)
except Exception as e:
    print(f"discord_send_failed: {e}", file=sys.stderr)
PYEOF
fi

# Keep 30 days of reports
find reports/nightly -name 'diagnose-*' -mtime +30 -delete 2>/dev/null
exit $rc
