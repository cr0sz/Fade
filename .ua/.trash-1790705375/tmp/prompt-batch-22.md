Analyze these files and produce GraphNode and GraphEdge objects.
Project root: `C:/Users/bekir/Documents/projeler/Fade`
Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`
Batch: `22/24`
Skill directory (for bundled scripts): `C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand`
Data directory ($UA_DIR): `C:/Users/bekir/Documents/projeler/Fade/.ua`
Output: write to `C:/Users/bekir/Documents/projeler/Fade/.ua/intermediate/batch-22.json` (single-file mode) OR `batch-22-part-<k>.json` (split mode, per Step B of your output protocol).

Pre-resolved import data for this batch (use directly — do NOT re-resolve imports from source):
```json
{".claude/agents/grunt.md":[],".claude/agents/sql-reviewer.md":[],".claude/architecture.md":[],".claude/settings.json":[],".claude/skills/deploy-verify/SKILL.md":[],".claude/skills/diagnose/SKILL.md":[],".gitattributes":[],".ua/config.json":[],"backups/.gitkeep":[],"docker/api.Dockerfile":[],"docker/bot.Dockerfile":[],"docker/freetime_researcher.Dockerfile":[],"docker/jupyter.Dockerfile":[],"docker/market_data_logger.Dockerfile":[],"docker/validation.Dockerfile":[],"docker/watchdog.Dockerfile":[],"docker/worker.Dockerfile":[],"graphify-out/.gitkeep":[],"monitoring/homepage/custom.js":[],"monitoring/prometheus/prometheus.yml":[],"notebooks/.gitkeep":[],"paper_trading_engine/capital_injection_engine.py":[],"paper_trading_engine/main.py":[],"paper_trading_engine/meta_strategy.py":[],"paper_trading_engine/requirements.txt":[]}
```

Cross-batch neighbors with their exported symbols (confidence boost for cross-batch edges):
```json
{}
```

Files to analyze in this batch (every entry MUST be passed through to `batchFiles` with all four fields — `path`, `language`, `sizeLines`, `fileCategory`):
1. `.claude/agents/grunt.md` (14 lines, language: `markdown`, fileCategory: `docs`)
2. `.claude/agents/sql-reviewer.md` (13 lines, language: `markdown`, fileCategory: `docs`)
3. `.claude/architecture.md` (284 lines, language: `markdown`, fileCategory: `docs`)
4. `.claude/settings.json` (40 lines, language: `json`, fileCategory: `config`)
5. `.claude/skills/deploy-verify/SKILL.md` (43 lines, language: `markdown`, fileCategory: `docs`)
6. `.claude/skills/diagnose/SKILL.md` (75 lines, language: `markdown`, fileCategory: `docs`)
7. `.gitattributes` (26 lines, language: `unknown`, fileCategory: `code`)
8. `.ua/config.json` (1 lines, language: `json`, fileCategory: `config`)
9. `backups/.gitkeep` (0 lines, language: `unknown`, fileCategory: `code`)
10. `docker/api.Dockerfile` (20 lines, language: `dockerfile`, fileCategory: `code`)
11. `docker/bot.Dockerfile` (12 lines, language: `dockerfile`, fileCategory: `code`)
12. `docker/freetime_researcher.Dockerfile` (18 lines, language: `dockerfile`, fileCategory: `code`)
13. `docker/jupyter.Dockerfile` (43 lines, language: `dockerfile`, fileCategory: `code`)
14. `docker/market_data_logger.Dockerfile` (14 lines, language: `dockerfile`, fileCategory: `code`)
15. `docker/validation.Dockerfile` (19 lines, language: `dockerfile`, fileCategory: `code`)
16. `docker/watchdog.Dockerfile` (18 lines, language: `dockerfile`, fileCategory: `code`)
17. `docker/worker.Dockerfile` (16 lines, language: `dockerfile`, fileCategory: `code`)
18. `graphify-out/.gitkeep` (0 lines, language: `unknown`, fileCategory: `code`)
19. `monitoring/homepage/custom.js` (346 lines, language: `javascript`, fileCategory: `code`)
20. `monitoring/prometheus/prometheus.yml` (134 lines, language: `yaml`, fileCategory: `config`)
21. `notebooks/.gitkeep` (0 lines, language: `unknown`, fileCategory: `code`)
22. `paper_trading_engine/capital_injection_engine.py` (371 lines, language: `python`, fileCategory: `code`)
23. `paper_trading_engine/main.py` (206 lines, language: `python`, fileCategory: `code`)
24. `paper_trading_engine/meta_strategy.py` (342 lines, language: `python`, fileCategory: `code`)
25. `paper_trading_engine/requirements.txt` (5 lines, language: `txt`, fileCategory: `docs`)

**Additional context from main session:**

Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").

Never read `.env` or any secrets file; it is not in your batch.
