---
name: diagnose
description: Run the standard deep-diagnosis ritual for the trading system - trade performance breakdown, router/gate behavior, engine health, config-landed checks. Use when asked to diagnose the bot, check why it is not trading, analyze performance, or run the periodic check-in.
---

# Trading System Deep Diagnosis

Run the checks below **in order**, with cheap parallel Bash calls. Report findings as: TLDR verdict first, then per-section facts. Always distinguish "broken" from "quiet by design" (OOS guardrail blocks most trades since Sprint 53).

## 1. Liveness

```bash
docker compose ps --format "table {{.Name}}\t{{.State}}\t{{.Status}}"
```
Anything not `running`/`healthy` → top finding. Then check that container's last 50 log lines.

## 2. Trade performance (price truth, not engine opinion)

```bash
docker exec market-intelligence-postgres-1 psql -U mktintel -d mktintel -c "
SELECT asset, direction, close_reason, COUNT(*) n,
       ROUND(AVG(pnl)::numeric,2) avg_pnl, ROUND(SUM(pnl)::numeric,2) total_pnl,
       ROUND(100.0*COUNT(*) FILTER (WHERE pnl>0)/COUNT(*),1) win_pct
FROM paper_trades WHERE closed_at > NOW() - INTERVAL '14 days'
GROUP BY 1,2,3 ORDER BY total_pnl;"
```
Also: open positions (`closed_at IS NULL`), last trade time, balance vs peak.

## 3. Router / gate behavior (why trades are or aren't happening)

```bash
docker logs market-intelligence-paper-trading-engine-1 --since 12h 2>&1 \
  | grep -E "trade_rejected|router_reject_summary|oos_gate_summary|trade_opened" | tail -30
```
Build a histogram of `reason` values. `router_passthrough_blocked` and `reject_invert` are EXPECTED under the Sprint 53 OOS gate; only ICE short-confirm + ETF long-confirm segments are tradeable until proven otherwise.

## 4. Signal flow upstream

If zero signals reach the router, check signal generation: recent rows in `signal_trace` and `signal_scores` (per-hour counts last 24h). A dead engine upstream looks like a router problem downstream.

## 5. Engine truth metrics

```bash
docker exec market-intelligence-postgres-1 psql -U mktintel -d mktintel -c "
SELECT engine, direction, COUNT(*) n, ROUND(AVG(price_return_24h)::numeric*100,3) avg_ret_pct
FROM signal_outcomes WHERE created_at > NOW() - INTERVAL '7 days'
GROUP BY 1,2 ORDER BY 1,2;"
```
(Adjust column names against the schema if the query errors - run `\d signal_outcomes` first.)

## 6. Config-landed proof (Sprint 52 guardrail)

```bash
python3 scripts/show_effective_config.py 2>/dev/null || docker exec market-intelligence-paper-trading-engine-1 python3 /app/scripts/show_effective_config.py
```
Verify the values that bit us before: crypto short `max_hold` (6h not 2h), trail (0.5%/1.5%), `DISABLE_LONGS`, OOS gate flags. Intent vs reality - if a recent commit changed a param, prove it landed.

## 7. Watchdog & alerts

Recent `validation_alerts` rows (last 48h) and watchdog container logs. Stale-quote alerts, calibration drift, dead feeds (remember MATIC was silently dead 47h once).

## Debugging techniques (from feedback_debugging_playbook)

- **Log histograms**: count events by type/reason before reading individual lines.
- **Ground-truth verification**: never trust an engine's self-reported accuracy; check against price returns.
- **Schema-vs-SQL**: any SQL error → run `make check-sql`.
- **Config-landed check**: every "fix deployed" claim must be verified in the running container, not the repo.

## Output format

1. **Verdict line**: TRADING / QUIET-BY-DESIGN / DEGRADED / BROKEN + one-sentence reason.
2. Last trade, open positions, 14d P&L summary.
3. Reject-reason histogram.
4. Anomalies found (or "none").
5. Recommended actions, if any - but do NOT apply changes; report only.
