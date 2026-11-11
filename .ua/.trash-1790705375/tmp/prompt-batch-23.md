Analyze these files and produce GraphNode and GraphEdge objects.
Project root: `C:/Users/bekir/Documents/projeler/Fade`
Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`
Batch: `23/24`
Skill directory (for bundled scripts): `C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand`
Data directory ($UA_DIR): `C:/Users/bekir/Documents/projeler/Fade/.ua`
Output: write to `C:/Users/bekir/Documents/projeler/Fade/.ua/intermediate/batch-23.json` (single-file mode) OR `batch-23-part-<k>.json` (split mode, per Step B of your output protocol).

Pre-resolved import data for this batch (use directly — do NOT re-resolve imports from source):
```json
{"scripts/apply_optimization.py":[],"scripts/backtest.py":[],"scripts/backup_hooks/discord-notify.sh":[],"scripts/binance_income.py":[],"scripts/check_sql_schema.py":[],"scripts/critical_watchdog.py":[],"scripts/git-hooks/pre-push":[],"scripts/ingest_binance_history.py":[],"scripts/live_readiness.py":[],"scripts/meanrev_replay.py":[],"scripts/meanrev_seed_router.py":[],"scripts/param_sweep.py":[],"scripts/pipeline_report.py":[],"scripts/replay_signals.py":[],"scripts/router_walkforward.py":[],"scripts/show_effective_config.py":[],"signal_edge_analyzer/requirements.txt":[],"sql/create_analyst_role.sql":[],"src/__init__.py":[],"src/api/__init__.py":[],"src/api/static/dashboard.html":[],"src/cache/__init__.py":[],"src/core/http_server.py":[],"src/db/__init__.py":[],"src/discord_bot/__init__.py":[]}
```

Cross-batch neighbors with their exported symbols (confidence boost for cross-batch edges):
```json
{}
```

Files to analyze in this batch (every entry MUST be passed through to `batchFiles` with all four fields — `path`, `language`, `sizeLines`, `fileCategory`):
1. `scripts/apply_optimization.py` (144 lines, language: `python`, fileCategory: `code`)
2. `scripts/backtest.py` (301 lines, language: `python`, fileCategory: `code`)
3. `scripts/backup_hooks/discord-notify.sh` (28 lines, language: `shell`, fileCategory: `script`)
4. `scripts/binance_income.py` (184 lines, language: `python`, fileCategory: `code`)
5. `scripts/check_sql_schema.py` (592 lines, language: `python`, fileCategory: `code`)
6. `scripts/critical_watchdog.py` (161 lines, language: `python`, fileCategory: `code`)
7. `scripts/git-hooks/pre-push` (45 lines, language: `unknown`, fileCategory: `code`)
8. `scripts/ingest_binance_history.py` (274 lines, language: `python`, fileCategory: `code`)
9. `scripts/live_readiness.py` (206 lines, language: `python`, fileCategory: `code`)
10. `scripts/meanrev_replay.py` (175 lines, language: `python`, fileCategory: `code`)
11. `scripts/meanrev_seed_router.py` (174 lines, language: `python`, fileCategory: `code`)
12. `scripts/param_sweep.py` (276 lines, language: `python`, fileCategory: `code`)
13. `scripts/pipeline_report.py` (376 lines, language: `python`, fileCategory: `code`)
14. `scripts/replay_signals.py` (580 lines, language: `python`, fileCategory: `code`)
15. `scripts/router_walkforward.py` (316 lines, language: `python`, fileCategory: `code`)
16. `scripts/show_effective_config.py` (101 lines, language: `python`, fileCategory: `code`)
17. `signal_edge_analyzer/requirements.txt` (6 lines, language: `txt`, fileCategory: `docs`)
18. `sql/create_analyst_role.sql` (39 lines, language: `sql`, fileCategory: `data`)
19. `src/__init__.py` (0 lines, language: `python`, fileCategory: `code`)
20. `src/api/__init__.py` (0 lines, language: `python`, fileCategory: `code`)
21. `src/api/static/dashboard.html` (1341 lines, language: `html`, fileCategory: `markup`)
22. `src/cache/__init__.py` (0 lines, language: `python`, fileCategory: `code`)
23. `src/core/http_server.py` (74 lines, language: `python`, fileCategory: `code`)
24. `src/db/__init__.py` (0 lines, language: `python`, fileCategory: `code`)
25. `src/discord_bot/__init__.py` (0 lines, language: `python`, fileCategory: `code`)

**Additional context from main session:**

Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").

Never read `.env` or any secrets file; it is not in your batch.
