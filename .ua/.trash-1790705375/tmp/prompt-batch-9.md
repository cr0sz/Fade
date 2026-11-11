Analyze these files and produce GraphNode and GraphEdge objects.
Project root: `C:/Users/bekir/Documents/projeler/Fade`
Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`
Batch: `9/24`
Skill directory (for bundled scripts): `C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand`
Data directory ($UA_DIR): `C:/Users/bekir/Documents/projeler/Fade/.ua`
Output: write to `C:/Users/bekir/Documents/projeler/Fade/.ua/intermediate/batch-9.json` (single-file mode) OR `batch-9-part-<k>.json` (split mode, per Step B of your output protocol).

Pre-resolved import data for this batch (use directly — do NOT re-resolve imports from source):
```json
{"signal_edge_analyzer/aggregator.py":["signal_edge_analyzer/collector.py","signal_edge_analyzer/metrics.py","signal_edge_analyzer/models.py","signal_edge_analyzer/outcomes.py"],"signal_edge_analyzer/collector.py":[],"signal_edge_analyzer/db.py":[],"signal_edge_analyzer/discord_client.py":[],"signal_edge_analyzer/main.py":["signal_edge_analyzer/db.py","signal_edge_analyzer/discord_client.py","signal_edge_analyzer/scheduler.py"],"signal_edge_analyzer/metrics.py":["signal_edge_analyzer/models.py"],"signal_edge_analyzer/models.py":[],"signal_edge_analyzer/outcomes.py":["signal_edge_analyzer/models.py"],"signal_edge_analyzer/scheduler.py":["signal_edge_analyzer/aggregator.py","signal_edge_analyzer/collector.py","signal_edge_analyzer/discord_client.py","signal_edge_analyzer/models.py","signal_edge_analyzer/walkforward.py"],"signal_edge_analyzer/walkforward.py":[]}
```

Cross-batch neighbors with their exported symbols (confidence boost for cross-batch edges):
```json
{}
```

Files to analyze in this batch (every entry MUST be passed through to `batchFiles` with all four fields — `path`, `language`, `sizeLines`, `fileCategory`):
1. `signal_edge_analyzer/aggregator.py` (211 lines, language: `python`, fileCategory: `code`)
2. `signal_edge_analyzer/collector.py` (116 lines, language: `python`, fileCategory: `code`)
3. `signal_edge_analyzer/db.py` (80 lines, language: `python`, fileCategory: `code`)
4. `signal_edge_analyzer/discord_client.py` (150 lines, language: `python`, fileCategory: `code`)
5. `signal_edge_analyzer/main.py` (251 lines, language: `python`, fileCategory: `code`)
6. `signal_edge_analyzer/metrics.py` (128 lines, language: `python`, fileCategory: `code`)
7. `signal_edge_analyzer/models.py` (81 lines, language: `python`, fileCategory: `code`)
8. `signal_edge_analyzer/outcomes.py` (61 lines, language: `python`, fileCategory: `code`)
9. `signal_edge_analyzer/scheduler.py` (503 lines, language: `python`, fileCategory: `code`)
10. `signal_edge_analyzer/walkforward.py` (163 lines, language: `python`, fileCategory: `code`)

**Additional context from main session:**

Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").

Never read `.env` or any secrets file; it is not in your batch.
