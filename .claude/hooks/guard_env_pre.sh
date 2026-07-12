#!/usr/bin/env bash
# PreToolUse(Write|Edit): editing .env (or any .env.*) always asks the user first.
# Enforces the paper-first rule: TRADING_MODE=live must never land without explicit approval.
f=$(python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("tool_input",{}).get("file_path",""))' 2>/dev/null)
case "$f" in
  *.env|*/.env|*.env.*)
    python3 -c 'import json;print(json.dumps({"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"ask","permissionDecisionReason":".env / trading config edit - explicit user approval required (paper-first rule)"}}))'
    ;;
esac
exit 0
