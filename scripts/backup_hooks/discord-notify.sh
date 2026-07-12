#!/bin/sh
# Called by run-parts with $1 = action (pre-backup / post-backup / error)
# Sends a Discord embed to DISCORD_WEBHOOK_ALERTS on post-backup and error.
[ -z "$DISCORD_WEBHOOK_ALERTS" ] && exit 0
ACTION="$1"

case "$ACTION" in
  post-backup)
    LATEST=$(ls -t /backups/last/*.gz 2>/dev/null | head -1)
    SIZE="unknown"
    [ -f "$LATEST" ] && SIZE=$(du -h "$LATEST" | cut -f1)
    TITLE="✅ Postgres backup OK"
    COLOR=3066993
    DESC="File: \`$(basename "$LATEST")\`\nSize: $SIZE"
    ;;
  error)
    TITLE="🔴 Postgres backup FAILED"
    COLOR=15158332
    DESC="Check container logs: docker compose logs postgres-backup"
    ;;
  *)
    exit 0
    ;;
esac

curl -fsS -H "Content-Type: application/json" \
  -d "{\"embeds\":[{\"title\":\"$TITLE\",\"color\":$COLOR,\"description\":\"$DESC\"}]}" \
  "$DISCORD_WEBHOOK_ALERTS" >/dev/null 2>&1 || true
