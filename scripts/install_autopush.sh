#!/usr/bin/env bash
# Sprint 49 — git auto-push installer.
#
# .git/hooks is NOT tracked by git, so this script (which IS tracked) reinstalls:
#   1. a post-commit hook that pushes origin/main in the background, never
#      blocking or failing the commit, logging to .git/auto-push.log;
#   2. a fallback user-crontab entry that pushes every 30 min (preserving any
#      existing crontab lines).
#
# Idempotent: safe to run repeatedly.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOOK="$REPO/.git/hooks/post-commit"
LOG="$REPO/.git/auto-push.log"

echo "Installing post-commit hook at $HOOK"
mkdir -p "$REPO/.git/hooks"
cat > "$HOOK" <<EOF
#!/usr/bin/env bash
# Auto-push origin/main in the background. Never blocks or fails the commit.
LOG="$REPO/.git/auto-push.log"
{
  echo "[\$(date -u '+%Y-%m-%dT%H:%M:%SZ')] post-commit push starting"
  cd "$REPO" && git push origin main --quiet 2>&1
  echo "[\$(date -u '+%Y-%m-%dT%H:%M:%SZ')] post-commit push done (exit \$?)"
} >> "\$LOG" 2>&1 &
exit 0
EOF
chmod +x "$HOOK"

echo "Installing fallback crontab entry (every 30 min)"
CRON_LINE="*/30 * * * * cd $REPO && git push origin main --quiet >> $REPO/.git/auto-push.log 2>&1"
MARKER="# market-intelligence auto-push"
# Preserve existing crontab; drop any prior auto-push block, then append fresh.
( crontab -l 2>/dev/null | grep -vF "$MARKER" | grep -vF "$REPO && git push origin main" ; \
  echo "$CRON_LINE $MARKER" ) | crontab -

echo "Done. Hook: $HOOK"
echo "Cron:"
crontab -l 2>/dev/null | grep -F "$MARKER" || true
