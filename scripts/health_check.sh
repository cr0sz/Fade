#!/usr/bin/env bash
# scripts/health_check.sh — hit every live health endpoint and report status.
# Called by "make health". Safe to run against a running stack at any time.

set -uo pipefail

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[0;33m'
BOLD='\033[1m'; RESET='\033[0m'

PASS=0; FAIL=0; WARN=0

http_check() {
    local name=$1
    local url=$2
    local result
    if result=$(curl -sf --max-time 5 "$url" 2>/dev/null); then
        echo -e "  ${GREEN}✓${RESET}  ${BOLD}$name${RESET}  →  $url"
        # If response has a "status" field, print it inline
        if echo "$result" | grep -q '"status"'; then
            status=$(echo "$result" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
            [ -n "$status" ] && echo -e "             status: $status"
        fi
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}✗${RESET}  ${BOLD}$name${RESET}  →  $url  (no response)"
        FAIL=$((FAIL + 1))
    fi
}

container_check() {
    local name=$1
    local service=$2
    local state
    state=$(docker compose ps --format json "$service" 2>/dev/null \
            | grep -o '"Health":"[^"]*"' | head -1 | cut -d'"' -f4 || true)
    case "$state" in
        healthy)
            echo -e "  ${GREEN}✓${RESET}  ${BOLD}$name${RESET}  (container: healthy)"
            PASS=$((PASS + 1))
            ;;
        starting)
            echo -e "  ${YELLOW}⚠${RESET}  ${BOLD}$name${RESET}  (container: starting — wait and retry)"
            WARN=$((WARN + 1))
            ;;
        unhealthy)
            echo -e "  ${RED}✗${RESET}  ${BOLD}$name${RESET}  (container: unhealthy)"
            FAIL=$((FAIL + 1))
            ;;
        *)
            echo -e "  ${YELLOW}⚠${RESET}  ${BOLD}$name${RESET}  (container: $state — may not be running)"
            WARN=$((WARN + 1))
            ;;
    esac
}

# ── Infrastructure ─────────────────────────────────────────────────────────────
echo -e "${BOLD}Infrastructure${RESET}"
container_check "postgres"     "postgres"
container_check "redis"        "redis"

# ── Core APIs ─────────────────────────────────────────────────────────────────
echo -e "\n${BOLD}Core APIs${RESET}"
http_check "api"              "http://localhost:8000/health"
http_check "validation-api"  "http://localhost:8010/health"

# ── Scheduler worker ──────────────────────────────────────────────────────────
echo -e "\n${BOLD}Scheduler${RESET}"
container_check "worker" "worker"

# ── Discord bot ───────────────────────────────────────────────────────────────
echo -e "\n${BOLD}Bot${RESET}"
container_check "discord-bot" "discord-bot"

# ── Validation engines (internal ports, Docker-network only) ───────────────────
echo -e "\n${BOLD}Validation Engines (container health)${RESET}"
container_check "backtest-worker"  "backtest-worker"
container_check "signal-archiver"  "signal-archiver"
container_check "outcome-tracker"  "outcome-tracker"
container_check "signal-scorer"    "signal-scorer"
container_check "replay-scheduler" "replay-scheduler"
container_check "governance-engine" "governance-engine"
container_check "meta-intelligence" "meta-intelligence"
container_check "validation-alert"  "validation-alert"

# ── Observability ──────────────────────────────────────────────────────────────
echo -e "\n${BOLD}Observability${RESET}"
http_check "prometheus" "http://localhost:9091/-/healthy"
http_check "grafana"    "http://localhost:3001/api/health"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "  ${GREEN}Passed${RESET}: $PASS  |  ${YELLOW}Warnings${RESET}: $WARN  |  ${RED}Failed${RESET}: $FAIL"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"

[ "$FAIL" -eq 0 ] && exit 0 || exit 1
