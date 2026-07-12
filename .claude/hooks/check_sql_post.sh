#!/usr/bin/env bash
# PostToolUse(Write|Edit): after any .py edit, run the SQL schema checker.
# On failure, feed the checker output back into Claude's context.
f=$(python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("tool_input",{}).get("file_path",""))' 2>/dev/null)
case "$f" in
  *.py)
    if ! out=$(make -C /opt/market-intelligence check-sql 2>&1); then
      OUT="$out" python3 -c 'import json,os;print(json.dumps({"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":"make check-sql FAILED after this edit:\n"+os.environ["OUT"]}}))'
    fi
    ;;
esac
exit 0
