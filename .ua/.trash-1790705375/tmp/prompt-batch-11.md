Analyze these files and produce GraphNode and GraphEdge objects.
Project root: `C:/Users/bekir/Documents/projeler/Fade`
Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`
Batch: `11/24`
Skill directory (for bundled scripts): `C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand`
Data directory ($UA_DIR): `C:/Users/bekir/Documents/projeler/Fade/.ua`
Output: write to `C:/Users/bekir/Documents/projeler/Fade/.ua/intermediate/batch-11.json` (single-file mode) OR `batch-11-part-<k>.json` (split mode, per Step B of your output protocol).

Pre-resolved import data for this batch (use directly — do NOT re-resolve imports from source):
```json
{"scripts/aggregate_stats.py":[],"scripts/governance_engine.py":[],"scripts/lessons_learned.py":[],"scripts/pipeline_smoke_test.py":["scripts/aggregate_stats.py","scripts/governance_engine.py","scripts/lessons_learned.py"],"scripts/run_lessons_pipeline.py":["scripts/aggregate_stats.py","scripts/governance_engine.py","scripts/lessons_learned.py"]}
```

Cross-batch neighbors with their exported symbols (confidence boost for cross-batch edges):
```json
{}
```

Files to analyze in this batch (every entry MUST be passed through to `batchFiles` with all four fields — `path`, `language`, `sizeLines`, `fileCategory`):
1. `scripts/aggregate_stats.py` (421 lines, language: `python`, fileCategory: `code`)
2. `scripts/governance_engine.py` (536 lines, language: `python`, fileCategory: `code`)
3. `scripts/lessons_learned.py` (1876 lines, language: `python`, fileCategory: `code`)
4. `scripts/pipeline_smoke_test.py` (42 lines, language: `python`, fileCategory: `code`)
5. `scripts/run_lessons_pipeline.py` (182 lines, language: `python`, fileCategory: `code`)

**Additional context from main session:**

Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").

Never read `.env` or any secrets file; it is not in your batch.
