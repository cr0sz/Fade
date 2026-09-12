SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
PROJECT  := fade

# ── Colours ────────────────────────────────────────────────────────────────────
BOLD  := \033[1m
RESET := \033[0m
GREEN := \033[0;32m
CYAN  := \033[0;36m

.PHONY: help preflight up down restart logs health reset build verify check-sql test

# ── Help ───────────────────────────────────────────────────────────────────────
help:
	@echo -e "$(BOLD)$(PROJECT) — available targets$(RESET)"
	@echo ""
	@echo -e "  $(CYAN)make preflight$(RESET)  Validate env, tools, ports, and compose file"
	@echo -e "  $(CYAN)make up$(RESET)         Run preflight then start all services (detached)"
	@echo -e "  $(CYAN)make down$(RESET)       Stop all services (volumes preserved)"
	@echo -e "  $(CYAN)make restart$(RESET)    Down then up"
	@echo -e "  $(CYAN)make logs$(RESET)       Tail logs for all services"
	@echo -e "  $(CYAN)make health$(RESET)     Hit live health endpoints and report status"
	@echo -e "  $(CYAN)make reset$(RESET)      DESTROY volumes and restart fresh  ⚠ irreversible"
	@echo -e "  $(CYAN)make build$(RESET)      Rebuild all images without cache"
	@echo -e "  $(CYAN)make verify$(RESET)     Run end-to-end pipeline verification report"
	@echo -e "  $(CYAN)make check-sql$(RESET)  Static-check embedded SQL against migration schema"
	@echo -e "  $(CYAN)make test$(RESET)       Run the unit test suite (no Docker needed)"
	@echo ""

# ── Preflight ─────────────────────────────────────────────────────────────────
preflight:
	@bash scripts/preflight.sh

# ── SQL schema check ──────────────────────────────────────────────────────────
# Verifies tables/columns referenced in Python SQL strings exist in migrations.
check-sql:
	@python3 scripts/check_sql_schema.py --strict

# ── Up ────────────────────────────────────────────────────────────────────────
up: preflight
	@echo -e "\n$(BOLD)Starting $(PROJECT)...$(RESET)"
	$(COMPOSE) up -d
	@echo ""
	@echo -e "$(GREEN)$(BOLD)Stack is up.$(RESET)"
	@echo -e "  API:        http://localhost:8000"
	@echo -e "  Validation: http://localhost:8010"
	@echo -e "  Prometheus: http://localhost:9091"
	@echo -e "  Dashboard:  http://localhost:3001"
	@echo ""
	@echo -e "Run $(CYAN)make health$(RESET) to verify all services are responding."

# ── Down ──────────────────────────────────────────────────────────────────────
down:
	@echo -e "$(BOLD)Stopping $(PROJECT) (volumes preserved)...$(RESET)"
	$(COMPOSE) down --remove-orphans

# ── Restart ───────────────────────────────────────────────────────────────────
restart: down up

# ── Build ─────────────────────────────────────────────────────────────────────
build:
	@echo -e "$(BOLD)Rebuilding all images (no cache)...$(RESET)"
	$(COMPOSE) build --no-cache

# ── Logs ──────────────────────────────────────────────────────────────────────
# Pass SERVICE=<name> to tail a single service: make logs SERVICE=api
logs:
	$(COMPOSE) logs -f $(SERVICE)

# ── Health ────────────────────────────────────────────────────────────────────
health:
	@echo -e "\n$(BOLD)Health check — $(PROJECT)$(RESET)\n"
	@bash scripts/health_check.sh

# ── Verify ────────────────────────────────────────────────────────────────────
# Queries live Postgres and reports pipeline health end-to-end.
# POSTGRES_HOST=localhost overrides the Docker-internal hostname.
# Exits non-zero (fails CI) when any stage is broken.
verify:
	@echo -e "\n$(BOLD)Pipeline verification — $(PROJECT)$(RESET)"
	@POSTGRES_HOST=localhost python3 scripts/pipeline_report.py

# ── Unit tests ────────────────────────────────────────────────────────────────
# Pure unit tests: no Docker, Postgres, Redis, or network required — but the
# tests import the real engine modules, so BOTH requirement files are needed:
#   pip install -r requirements.txt -r requirements-dev.txt
test:
	@echo -e "\n$(BOLD)Running unit tests...$(RESET)"
	python3 -m pytest tests/ -q

# ── Reset ─────────────────────────────────────────────────────────────────────
reset:
	@echo ""
	@echo -e "\033[0;31m$(BOLD)WARNING: This will permanently delete ALL data volumes.$(RESET)"
	@echo -e "\033[0;31mPostgres data, Redis cache, Prometheus metrics, and dashboard state$(RESET)"
	@echo -e "\033[0;31mwill all be erased and cannot be recovered.$(RESET)"
	@echo ""
	@printf "Type 'yes' to confirm: " && \
	 read -r CONFIRM && [ "$$CONFIRM" = "yes" ] || \
	 (echo -e "\nAborted — no changes made." && exit 1)
	@echo ""
	@echo -e "$(BOLD)Stopping stack and removing volumes...$(RESET)"
	$(COMPOSE) down -v --remove-orphans
	@echo -e "$(BOLD)Volumes removed. Rebuilding and starting fresh...$(RESET)"
	$(COMPOSE) build --no-cache
	@$(MAKE) up
