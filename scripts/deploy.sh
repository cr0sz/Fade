#!/usr/bin/env bash
# scripts/deploy.sh — Production deployment for AI Market Intelligence Platform
#
# Usage:
#   ./scripts/deploy.sh              # incremental deploy (default — recommended)
#   ./scripts/deploy.sh --safe       # restart services only; skip explicit rebuild
#   ./scripts/deploy.sh --reset-db   # wipe DB and redeploy  ⚠  DESTRUCTIVE
#   ./scripts/deploy.sh --no-cache   # force full image rebuild from scratch (slow)
#
# On any failure the script auto-rolls back to the last successfully deployed
# commit (stored in .last_good_commit) and restarts the stack from that state.

set -euo pipefail

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCKFILE="$PROJECT_DIR/.deploy.lock"
LAST_GOOD_COMMIT_FILE="$PROJECT_DIR/.last_good_commit"

# ── Config ─────────────────────────────────────────────────────────────────────
COMPOSE="docker compose"
POSTGRES_READY_TIMEOUT=60   # seconds before giving up on postgres startup

# ── Colour helpers ─────────────────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[0;33m'; GREEN='\033[0;32m'
BOLD='\033[1m'; RESET='\033[0m'

log()  { echo -e "${BOLD}[deploy]${RESET} $*"; }
ok()   { echo -e "${GREEN}[deploy] ✓${RESET} $*"; }
warn() { echo -e "${YELLOW}[deploy] ⚠${RESET} $*" >&2; }
die()  { echo -e "${RED}[deploy] ✗ FATAL:${RESET} $*" >&2; exit 1; }

# ── Flag parsing ───────────────────────────────────────────────────────────────
RESET_DB=false
NO_CACHE=false
SAFE_MODE=false

for arg in "$@"; do
    case "$arg" in
        --reset-db)  RESET_DB=true ;;
        --no-cache)  NO_CACHE=true ;;
        --safe)      SAFE_MODE=true ;;
        -h|--help)
            grep '^#' "$0" | head -12 | sed 's/^# \?//'
            exit 0
            ;;
        *)
            die "Unknown argument: '$arg'  (valid: --reset-db | --no-cache | --safe | --help)"
            ;;
    esac
done

$RESET_DB && $SAFE_MODE && die "--reset-db and --safe are mutually exclusive"
$NO_CACHE && $SAFE_MODE && die "--no-cache and --safe are mutually exclusive"

# ── Exclusive deploy lock (prevents concurrent runs) ───────────────────────────
exec 9>"$LOCKFILE"
flock -n 9 || die "Another deployment is already running. Remove $LOCKFILE if it is stale."

# ── Rollback support ───────────────────────────────────────────────────────────
CURRENT_COMMIT="$(git -C "$PROJECT_DIR" rev-parse HEAD)"
PREVIOUS_COMMIT=""
[ -f "$LAST_GOOD_COMMIT_FILE" ] && PREVIOUS_COMMIT="$(cat "$LAST_GOOD_COMMIT_FILE")"

_rollback() {
    local exit_code=$?
    trap - ERR   # prevent recursive trap
    echo ""
    warn "Deployment failed (exit code: $exit_code) at step: ${DEPLOY_STEP:-unknown}"

    if [ -n "$PREVIOUS_COMMIT" ] && [ "$PREVIOUS_COMMIT" != "$CURRENT_COMMIT" ]; then
        warn "Rolling back to last good commit: $PREVIOUS_COMMIT"

        if git -C "$PROJECT_DIR" reset --hard "$PREVIOUS_COMMIT" 2>/dev/null; then
            cd "$PROJECT_DIR"
            warn "Rebuilding images from rolled-back code..."
            if $COMPOSE build 2>/dev/null; then
                warn "Restarting services from rolled-back code..."
                $COMPOSE up -d --remove-orphans 2>/dev/null \
                    && warn "Rollback complete — stack is running commit $PREVIOUS_COMMIT" \
                    || warn "Rollback restart failed — manual intervention required"
            else
                warn "Rollback build failed — manual intervention required"
            fi
        else
            warn "git reset failed — manual intervention required"
        fi
    else
        warn "No previous commit to roll back to — manual intervention required"
    fi

    exit "$exit_code"
}
trap '_rollback' ERR

# ── Work from project root ─────────────────────────────────────────────────────
cd "$PROJECT_DIR"

# ── Header ─────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  AI Market Intelligence Platform — Deploy${RESET}"
echo -e "${BOLD}═══════════════════════════════════════════════════${RESET}"
echo "  Commit  : $CURRENT_COMMIT"
printf "  Mode    : "
if   $RESET_DB;   then echo "RESET-DB  ⚠  destructive"
elif $SAFE_MODE;  then echo "SAFE  (restart only)"
elif $NO_CACHE;   then echo "NO-CACHE  (full rebuild)"
else                   echo "INCREMENTAL  (default)"
fi
[ -n "$PREVIOUS_COMMIT" ] && echo "  Prev OK : $PREVIOUS_COMMIT"
echo ""

# ── Step 1 — Environment validation ──────────────────────────────────────────
DEPLOY_STEP="environment validation"
log "Step 1/5 — Environment validation"

[ -f "$PROJECT_DIR/.env" ] || \
    die ".env not found. Copy .env.example and fill in all required values."

# Safe .env parser: reads key=value pairs without executing shell code.
# Never source .env directly — arbitrary values would be executed as commands.
_load_env() {
    local line key value
    while IFS= read -r line || [ -n "$line" ]; do
        [[ "$line" =~ ^[[:space:]]*# ]] && continue   # skip comment lines
        [[ -z "${line//[[:space:]]/}" ]] && continue   # skip blank lines
        [[ "$line" != *=* ]]             && continue   # skip non-assignments
        key="${line%%=*}"
        value="${line#*=}"
        [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
        export "$key=$value"
    done < "$PROJECT_DIR/.env"
}
_load_env

REQUIRED_ENV_VARS=(POSTGRES_PASSWORD)
for var in "${REQUIRED_ENV_VARS[@]}"; do
    val="${!var:-}"
    [ -n "$val" ]                                || die "$var is not set in .env"
    [ "$val" != "CHANGE_ME_STRONG_PASSWORD" ]    || die "$var is still the default placeholder — set a real value"
done

ok "Environment validated"

# ── Step 2 — Trading safety layer ────────────────────────────────────────────
DEPLOY_STEP="trading safety checks"
log "Step 2/5 — Trading safety layer"

TRADING_MODE="${TRADING_MODE:-paper}"
case "$TRADING_MODE" in
    paper)
        ok "Trading mode: paper (safe)"
        ;;
    live)
        [ -n "${MAX_DAILY_LOSS:-}" ] || \
            die "TRADING_MODE=live requires MAX_DAILY_LOSS to be set in .env"
        [ -n "${KILL_SWITCH:-}" ] || \
            die "TRADING_MODE=live requires KILL_SWITCH to be set in .env"
        warn "LIVE trading active — MAX_DAILY_LOSS=${MAX_DAILY_LOSS}  KILL_SWITCH=${KILL_SWITCH}"
        ;;
    *)
        die "TRADING_MODE must be 'paper' or 'live', got: '$TRADING_MODE'"
        ;;
esac

# ── Step 3 — Build images ─────────────────────────────────────────────────────
DEPLOY_STEP="image build"
log "Step 3/5 — Build"

if $SAFE_MODE; then
    log "Safe mode: skipping explicit build (docker compose will use cached layers)"
elif $NO_CACHE; then
    log "Building all images from scratch (--no-cache)..."
    $COMPOSE build --no-cache
else
    log "Building images incrementally..."
    $COMPOSE build
fi
ok "Build complete"

# ── Step 4 — Start infrastructure ─────────────────────────────────────────────
DEPLOY_STEP="infrastructure startup"
log "Step 4/5 — Infrastructure"

log "Starting postgres and redis..."
$COMPOSE up -d postgres redis

log "Waiting for postgres to be ready (timeout: ${POSTGRES_READY_TIMEOUT}s)..."
deadline=$((SECONDS + POSTGRES_READY_TIMEOUT))
until $COMPOSE exec -T postgres \
        pg_isready \
        -U "${POSTGRES_USER:-mktintel}" \
        -d "${POSTGRES_DB:-mktintel}" \
        >/dev/null 2>&1
do
    if (( SECONDS >= deadline )); then
        die "Postgres did not become ready within ${POSTGRES_READY_TIMEOUT}s — check: docker compose logs postgres"
    fi
    sleep 2
done
ok "Postgres ready"

# Database reset — only executed when --reset-db is explicitly passed
if $RESET_DB; then
    warn "Resetting database — ALL DATA WILL BE PERMANENTLY LOST"
    $COMPOSE exec -T postgres psql \
        -U "${POSTGRES_USER:-mktintel}" \
        -c "SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '${POSTGRES_DB:-mktintel}'
              AND pid <> pg_backend_pid();" \
        >/dev/null 2>&1 || true

    $COMPOSE exec -T postgres psql \
        -U "${POSTGRES_USER:-mktintel}" \
        -c "DROP DATABASE IF EXISTS \"${POSTGRES_DB:-mktintel}\";
            CREATE DATABASE \"${POSTGRES_DB:-mktintel}\";"

    ok "Database reset — migrations will run automatically on service startup"
fi

# ── Step 5 — Deploy all services ──────────────────────────────────────────────
DEPLOY_STEP="service deployment"
log "Step 5/5 — Deploy"

log "Bringing up all services (migrations run inside api/validation-api on start)..."
$COMPOSE up -d --remove-orphans
ok "All services started"

# ── Record successful deploy ───────────────────────────────────────────────────
echo "$CURRENT_COMMIT" > "$LAST_GOOD_COMMIT_FILE"

echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════${RESET}"
ok "Deployment successful — $CURRENT_COMMIT"
echo -e "${BOLD}═══════════════════════════════════════════════════${RESET}"
echo ""
echo "  API:        http://localhost:8000/health"
echo "  Validation: http://localhost:8010/health"
echo "  Grafana:    http://localhost:3001"
echo "  Logs:       docker compose logs -f"
echo ""
$COMPOSE ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
