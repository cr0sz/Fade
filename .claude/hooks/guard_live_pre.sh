#!/usr/bin/env bash
# PreToolUse(Bash): any shell command that sets TRADING_MODE=live asks the user first.
c=$(python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("tool_input",{}).get("command",""))' 2>/dev/null)
if printf '%s' "$c" | grep -q 'TRADING_MODE=live'; then
  python3 -c 'import json;print(json.dumps({"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"ask","permissionDecisionReason":"Command sets TRADING_MODE=live - explicit user approval required (paper-first rule)"}}))'
fi
exit 0
