#!/usr/bin/env bash
# PostToolUse(Write|Edit): after any .py edit, run the SQL schema checker.
# On failure, feed the checker output back into Claude's context.
#
# Paths are derived from $CLAUDE_PROJECT_DIR (falls back to the hook's own
# location) so this works in any clone, not just one specific deploy path.
ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

f=$(python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("tool_input",{}).get("file_path",""))' 2>/dev/null)
case "$f" in
  *.py)
    # Prefer the make target; fall back to the checker directly when make is
    # not installed (Windows hosts, minimal containers).
    if command -v make >/dev/null 2>&1; then
      out=$(make -C "$ROOT" check-sql 2>&1); rc=$?
    else
      out=$(PYTHONUTF8=1 python3 "$ROOT/scripts/check_sql_schema.py" --strict 2>&1); rc=$?
    fi
    if [ "$rc" -ne 0 ]; then
      OUT="$out" python3 -c 'import json,os;print(json.dumps({"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":"check-sql FAILED after this edit:\n"+os.environ["OUT"]}}))'
    fi
    ;;
esac
exit 0
