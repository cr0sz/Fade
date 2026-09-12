#!/usr/bin/env bash
# scripts/preflight.sh — run before every "make up"
# Validates environment, tools, ports, and compose config.
# Exits non-zero with a clear error message on any hard failure.

set -euo pipefail

# ── Colours ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[0;33m'; GREEN='\033[0;32m'
BOLD='\033[1m'; RESET='\033[0m'

pass()  { echo -e "  ${GREEN}✓${RESET}  $*"; }
warn()  { echo -e "  ${YELLOW}⚠${RESET}  $*"; }
fail()  { echo -e "  ${RED}✗${RESET}  $*"; ERRORS=$((ERRORS + 1)); }
section(){ echo -e "\n${BOLD}$*${RESET}"; }

ERRORS=0
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo -e "${BOLD}══════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Fade — Preflight Check${RESET}"
echo -e "${BOLD}══════════════════════════════════════════════════${RESET}"

# ── 1. Tool availability ──────────────────────────────────────────────────────
section "1/6  Tools"

if command -v docker &>/dev/null; then
    pass "docker found: $(docker --version | head -1)"
else
    fail "docker is not installed or not in PATH"
fi

if docker compose version &>/dev/null 2>&1; then
    pass "docker compose plugin: $(docker compose version)"
elif command -v docker-compose &>/dev/null; then
    pass "docker-compose (standalone) found"
else
    fail "neither 'docker compose' plugin nor 'docker-compose' binary found"
fi

if ! docker info &>/dev/null 2>&1; then
    fail "Docker daemon is not running. Start it and retry."
else
    pass "Docker daemon is running"
fi

# ── 2. .env file ──────────────────────────────────────────────────────────────
section "2/6  Environment file"

ENV_FILE="$PROJECT_DIR/.env"
EXAMPLE_FILE="$PROJECT_DIR/.env.example"

if [ ! -f "$ENV_FILE" ]; then
    fail ".env not found at $ENV_FILE"
    if [ -f "$EXAMPLE_FILE" ]; then
        echo -e "       ${YELLOW}Run: cp .env.example .env  and fill in the required values${RESET}"
    fi
    # Cannot continue without .env — exit now
    echo ""
    echo -e "${RED}${BOLD}Preflight FAILED — $ERRORS error(s) above.${RESET}"
    exit 1
fi

pass ".env found"

# Safe .env parser: handles unquoted values with spaces, skips comments.
# "source .env" would try to execute "Intelligence" from APP_NAME=Market Intelligence Platform.
_SQ="'"   # a bare ' inside [[ =~ ]] would be stripped as a shell quote
while IFS= read -r line || [ -n "$line" ]; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue   # skip comments
    [[ -z "${line//[[:space:]]/}" ]] && continue   # skip blank lines
    [[ "$line" != *=* ]] && continue               # skip lines without =
    key="${line%%=*}"
    key="${key#"${key%%[![:space:]]*}"}"           # trim leading space on key
    value="${line#*=}"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue  # skip invalid keys

    # Inline comments MUST be stripped the same way Docker Compose strips them
    # (whitespace then #). Without this, a line copied verbatim from
    # .env.example --
    #     POSTGRES_PASSWORD=CHANGE_ME_STRONG_PASSWORD   # [REQUIRED] ...
    # -- parses as "CHANGE_ME_STRONG_PASSWORD   # [REQUIRED] ...", which never
    # equals the placeholder sentinel below, so the "you left the placeholder
    # in" check silently passes on a completely unedited .env.
    # A quoted value is taken verbatim: a # inside quotes belongs to the value.
    if [[ "$value" =~ ^[[:space:]]*\"(.*)\"[[:space:]]*(#.*)?$ ]]; then
        value="${BASH_REMATCH[1]}"
    elif [[ "$value" =~ ^[[:space:]]*${_SQ}(.*)${_SQ}[[:space:]]*(#.*)?$ ]]; then
        value="${BASH_REMATCH[1]}"
    else
        value="${value%%[[:space:]]#*}"             # drop inline comment
        value="${value#"${value%%[![:space:]]*}"}"  # trim leading whitespace
        value="${value%"${value##*[![:space:]]}"}"  # trim trailing whitespace
    fi
    export "$key=$value"
done < "$ENV_FILE"

# ── 3. Required variables ─────────────────────────────────────────────────────
section "3/6  Required variables"

# Format: "VAR_NAME|placeholder_sentinel|description"
REQUIRED_VARS=(
    "POSTGRES_PASSWORD|CHANGE_ME_STRONG_PASSWORD|Database password (REQUIRED — set a strong unique value)"
    "GRAFANA_PASSWORD|CHANGE_ME_STRONG_GRAFANA_PASSWORD|Grafana admin password (REQUIRED)"
    "DASHBOARD_TOKEN|CHANGE_ME_DASHBOARD_TOKEN|Dashboard API token (REQUIRED — generate with: openssl rand -hex 32)"
)

for entry in "${REQUIRED_VARS[@]}"; do
    IFS='|' read -r var_name placeholder description <<< "$entry"
    val="${!var_name:-}"

    if [ -z "$val" ]; then
        fail "$var_name is not set — $description"
    elif [ "$val" = "$placeholder" ]; then
        fail "$var_name is still the placeholder value — $description"
    else
        pass "$var_name is set"
    fi
done

# Optional but loudly warned
OPTIONAL_WARN=(
    "OPENAI_API_KEY|AI digest generation will be disabled"
    "DISCORD_BOT_TOKEN|Discord bot will fail to start"
)

for entry in "${OPTIONAL_WARN[@]}"; do
    IFS='|' read -r var_name notice <<< "$entry"
    val="${!var_name:-}"
    # .env.example uses the YOUR_<NAME>_HERE convention. The old check only
    # looked for lowercase "your_", so every untouched placeholder reported as
    # "is set".
    if [ -z "$val" ] || [[ "$val" == YOUR_*_HERE ]] || [[ "$val" == *[Yy][Oo][Uu][Rr]_* ]]        || [[ "$val" == CHANGE_ME* ]] || [[ "$val" == "sk-..."* ]]; then
        warn "$var_name not configured — $notice"
    else
        pass "$var_name is set"
    fi
done

# ── 4. Compose file validation ────────────────────────────────────────────────
section "4/6  Compose file"

cd "$PROJECT_DIR"

if docker compose config --quiet 2>/dev/null; then
    pass "docker-compose.yml is valid"
else
    fail "docker-compose.yml has errors (run 'docker compose config' for details)"
fi

# ── 5. Port availability ──────────────────────────────────────────────────────
section "5/6  Host ports"

# Every host port published in docker-compose.yml, as "port:service".
# A plain list, not an associative array: "declare -A" needs bash 4 and macOS
# still ships bash 3.2, where it aborts the whole script under "set -e".
PORTS="
3001:homepage
4000:miniflux
5432:postgres
6333:qdrant
6334:qdrant-grpc
6379:redis
8000:api
8010:validation-api
8888:jupyter
9091:prometheus
"

# Is anything listening on $1? ss on Linux, netstat/lsof elsewhere (macOS has
# no ss). Returns non-zero when no tool exists, so a missing binary is never
# reported as a port conflict.
port_in_use() {
    local port="$1"
    if command -v ss &>/dev/null; then
        ss -tln 2>/dev/null | grep -qE "[:.]${port}[[:space:]]"
    elif command -v netstat &>/dev/null; then
        netstat -an 2>/dev/null | grep -qE "[:.]${port}[[:space:]].*LISTEN"
    elif command -v lsof &>/dev/null; then
        lsof -nP -iTCP:"${port}" -sTCP:LISTEN &>/dev/null
    else
        return 1
    fi
}

if ! command -v ss &>/dev/null && ! command -v netstat &>/dev/null && ! command -v lsof &>/dev/null; then
    warn "no ss/netstat/lsof found - port availability not checked"
fi

# Detect if our compose stack is already running
COMPOSE_RUNNING=false
if docker compose ps --services --filter status=running 2>/dev/null | grep -q .; then
    COMPOSE_RUNNING=true
fi

for entry in $PORTS; do
    port="${entry%%:*}"
    service="${entry#*:}"
    if port_in_use "$port"; then
        if [ "$COMPOSE_RUNNING" = "true" ]; then
            pass "Port $port ($service) in use by running stack"
        else
            warn "Port $port ($service) is already in use by another process"
        fi
    else
        pass "Port $port ($service) is free"
    fi
done

# ── 6. SQL schema check (warn-only) ───────────────────────────────────────────
section "6/6  SQL schema check"

if command -v python3 &>/dev/null; then
    # Static-check embedded SQL strings against the migration-derived schema.
    # Warn-only: a finding here never blocks 'make up'. Run 'make check-sql' to
    # see details, or for a strict (exit-1) check.
    sql_out="$(python3 "$SCRIPT_DIR/check_sql_schema.py" 2>/dev/null)"
    sql_findings="$(echo "$sql_out" | grep -cE 'unknown (table|column)' || true)"
    if [ "${sql_findings:-0}" -gt 0 ]; then
        warn "$sql_findings SQL/schema mismatch(es) found — run 'make check-sql' for details"
    else
        pass "No SQL/schema mismatches in Python sources"
    fi
else
    warn "python3 not found — skipping SQL schema check"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}══════════════════════════════════════════════════${RESET}"

if [ "$ERRORS" -gt 0 ]; then
    echo -e "${RED}${BOLD}Preflight FAILED — $ERRORS error(s). Fix the issues above before running 'make up'.${RESET}"
    echo -e "${BOLD}══════════════════════════════════════════════════${RESET}"
    exit 1
else
    echo -e "${GREEN}${BOLD}Preflight PASSED — environment looks good.${RESET}"
    echo -e "${BOLD}══════════════════════════════════════════════════${RESET}"
fi
