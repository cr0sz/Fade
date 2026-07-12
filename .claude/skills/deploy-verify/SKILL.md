---
name: deploy-verify
description: Deploy current code to the running containers and PROVE the config/code actually landed (Sprint 52 guardrail). Use after code or config changes that need to reach the live containers.
disable-model-invocation: true
---

# Deploy + Verify (config-landed proof)

History lesson: multiple "deployed" fixes never actually landed (max_hold reset on restart, trail percentages, Sprint 34). Never report success without step 4.

## 1. Pre-flight

```bash
cd /opt/market-intelligence
make check-sql
git status --short   # know exactly what is being deployed
```
If check-sql fails, STOP and report.

## 2. Deploy

```bash
bash scripts/deploy.sh   # or: docker compose up -d --build <changed services>
```
Prefer rebuilding only the services whose code changed (look at the diff paths: paper_trading_engine/ -> paper-trading-engine, src/telegram/ -> telegram-bot, etc.).

## 3. Health

```bash
bash scripts/health_check.sh || make health
docker compose ps --format "table {{.Name}}\t{{.Status}}" | grep -v healthy
```
Wait for restarted services to go healthy (retry up to ~90s).

## 4. PROVE it landed

- Effective config: `python3 scripts/show_effective_config.py` (or in-container variant). Diff every parameter the change touched against intended values. Check the Redis effective-config hash for drift alerts.
- Code changes: `docker exec <container> grep -n "<distinctive new line>" /app/<file>` - confirm the new code is inside the container, not just the repo.
- Behavior: tail the service logs for 1-2 poll cycles; confirm the new behavior appears (or at least no new errors/restart loops).

## 5. Report

State explicitly: what was deployed, which services restarted, proof that each intended change is live (quote the evidence), and anything that did NOT land.
