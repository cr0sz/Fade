Analyze these files and produce GraphNode and GraphEdge objects.
Project root: `C:/Users/bekir/Documents/projeler/Fade`
Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`
Batch: `24/24`
Skill directory (for bundled scripts): `C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand`
Data directory ($UA_DIR): `C:/Users/bekir/Documents/projeler/Fade/.ua`
Output: write to `C:/Users/bekir/Documents/projeler/Fade/.ua/intermediate/batch-24.json` (single-file mode) OR `batch-24-part-<k>.json` (split mode, per Step B of your output protocol).

Pre-resolved import data for this batch (use directly — do NOT re-resolve imports from source):
```json
{"src/engines/__init__.py":[],"src/market_data_logger/__init__.py":[],"src/scheduler/__init__.py":[],"src/scheduler/jobs/__init__.py":[],"src/validation/__init__.py":[],"src/validation/models.py":[],"tests/conftest.py":[],"tests/test_binance_futures.py":[],"tests/test_executor.py":[],"tests/test_exit_priority.py":[],"tests/test_portfolio.py":[],"tests/test_reconcile_ignore.py":[],"tests/test_risk.py":[],"tests/test_stale_flatten.py":[],"tests/test_truth_router.py":[]}
```

Cross-batch neighbors with their exported symbols (confidence boost for cross-batch edges):
```json
{}
```

Files to analyze in this batch (every entry MUST be passed through to `batchFiles` with all four fields — `path`, `language`, `sizeLines`, `fileCategory`):
1. `src/engines/__init__.py` (0 lines, language: `python`, fileCategory: `code`)
2. `src/market_data_logger/__init__.py` (7 lines, language: `python`, fileCategory: `code`)
3. `src/scheduler/__init__.py` (0 lines, language: `python`, fileCategory: `code`)
4. `src/scheduler/jobs/__init__.py` (0 lines, language: `python`, fileCategory: `code`)
5. `src/validation/__init__.py` (1 lines, language: `python`, fileCategory: `code`)
6. `src/validation/models.py` (74 lines, language: `python`, fileCategory: `code`)
7. `tests/conftest.py` (49 lines, language: `python`, fileCategory: `code`)
8. `tests/test_binance_futures.py` (285 lines, language: `python`, fileCategory: `code`)
9. `tests/test_executor.py` (241 lines, language: `python`, fileCategory: `code`)
10. `tests/test_exit_priority.py` (241 lines, language: `python`, fileCategory: `code`)
11. `tests/test_portfolio.py` (241 lines, language: `python`, fileCategory: `code`)
12. `tests/test_reconcile_ignore.py` (14 lines, language: `python`, fileCategory: `code`)
13. `tests/test_risk.py` (213 lines, language: `python`, fileCategory: `code`)
14. `tests/test_stale_flatten.py` (15 lines, language: `python`, fileCategory: `code`)
15. `tests/test_truth_router.py` (244 lines, language: `python`, fileCategory: `code`)

**Additional context from main session:**

Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").

Never read `.env` or any secrets file; it is not in your batch.
