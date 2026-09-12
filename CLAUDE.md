# Market Intelligence — Claude Code instructions

## Model economy (user is on the $20 Pro plan — usage is scarce)
- Delegate mechanical work to the `grunt` subagent (Haiku): log greps, simple SQL queries via psql, file searches, running known commands, bulk reads. Do NOT do these inline on the big model when the task is a self-contained fetch-and-return.
- Use the `sql-reviewer` subagent (Sonnet) to review any diff containing SQL strings.
- Keep the main thread for reasoning, design, and synthesis only.

## Hard rules
- NEVER set TRADING_MODE=live or edit .env without explicit user confirmation (hooks enforce this — do not work around them).
- Every deploy must be verified with /deploy-verify (config-landed proof). "Committed" is not "deployed".
- Performance claims must use price truth (paper_trades pnl, signal_outcomes price returns), never engine self-reported accuracy.

## Quick facts
- DB: `docker exec fade-postgres-1 psql -U mktintel -d mktintel` (container name is `<compose-project>-postgres-1`; the project defaults to the repo directory name). Read-only MCP server `postgres` is also connected.
- There is no `signals` table; use `signals_registry`, `signal_trace`, `signal_scores`, `signal_outcomes`.
- `make check-sql` statically checks embedded SQL against migrations (runs automatically via hook after .py edits).
- /diagnose runs the standard system-health + performance ritual.
- Since Sprint 53 the OOS guardrail blocks most trades by design (only ICE short-confirm + ETF long-confirm); low trade frequency is expected, not a bug. Re-evaluate ~2026-06-19.

## Graphify (knowledge graph — second brain)
- **Session start**: read `graphify-out/GRAPH_REPORT.md` god-nodes + communities sections to orient before exploring code. Do NOT grep files broadly to understand architecture — the graph is the map.
- **Structural questions**: `graphify query "<question>"` from repo root instead of broad grep/read.
- **After every sprint commit**: run `graphify . --update --no-viz` from the repo root (incremental, fast). Run without `--no-viz` only when user wants HTML refreshed.
- **View graph**: http://localhost:8000/graphify (served by api container, bind-mounted from `./graphify-out`).
- Graph lives at `graphify-out/graph.json`. 3223 nodes, 6095 edges, 211 communities as of 2026-06-18.
