Analyze these files and produce GraphNode and GraphEdge objects.
Project root: `C:/Users/bekir/Documents/projeler/Fade`
Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`
Batch: `16/24`
Skill directory (for bundled scripts): `C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand`
Data directory ($UA_DIR): `C:/Users/bekir/Documents/projeler/Fade/.ua`
Output: write to `C:/Users/bekir/Documents/projeler/Fade/.ua/intermediate/batch-16.json` (single-file mode) OR `batch-16-part-<k>.json` (split mode, per Step B of your output protocol).

Pre-resolved import data for this batch (use directly — do NOT re-resolve imports from source):
```json
{".claude/runbooks/add_ai_model.md":[],".claude/runbooks/add_news_source.md":[],".claude/runbooks/add_provider.md":[],".claude/runbooks/add_social_source.md":[],".claude/runbooks/add_ticker_source.md":[],".claude/runbooks/backtesting_replay.md":[],".claude/runbooks/calibration_governance.md":[],".claude/runbooks/debug_pipeline.md":[],".claude/runbooks/deploy_production.md":[],".claude/runbooks/governance_optimization.md":[],".claude/runbooks/improve_sentiment_engine.md":[],".claude/runbooks/regime_transition_analysis.md":[],".claude/runbooks/strategic_memory.md":[],".claude/runbooks/validation_alert.md":[]}
```

Cross-batch neighbors with their exported symbols (confidence boost for cross-batch edges):
```json
{}
```

Files to analyze in this batch (every entry MUST be passed through to `batchFiles` with all four fields — `path`, `language`, `sizeLines`, `fileCategory`):
1. `.claude/runbooks/add_ai_model.md` (106 lines, language: `markdown`, fileCategory: `docs`)
2. `.claude/runbooks/add_news_source.md` (81 lines, language: `markdown`, fileCategory: `docs`)
3. `.claude/runbooks/add_provider.md` (88 lines, language: `markdown`, fileCategory: `docs`)
4. `.claude/runbooks/add_social_source.md` (88 lines, language: `markdown`, fileCategory: `docs`)
5. `.claude/runbooks/add_ticker_source.md` (121 lines, language: `markdown`, fileCategory: `docs`)
6. `.claude/runbooks/backtesting_replay.md` (192 lines, language: `markdown`, fileCategory: `docs`)
7. `.claude/runbooks/calibration_governance.md` (157 lines, language: `markdown`, fileCategory: `docs`)
8. `.claude/runbooks/debug_pipeline.md` (168 lines, language: `markdown`, fileCategory: `docs`)
9. `.claude/runbooks/deploy_production.md` (164 lines, language: `markdown`, fileCategory: `docs`)
10. `.claude/runbooks/governance_optimization.md` (211 lines, language: `markdown`, fileCategory: `docs`)
11. `.claude/runbooks/improve_sentiment_engine.md` (135 lines, language: `markdown`, fileCategory: `docs`)
12. `.claude/runbooks/regime_transition_analysis.md` (97 lines, language: `markdown`, fileCategory: `docs`)
13. `.claude/runbooks/strategic_memory.md` (170 lines, language: `markdown`, fileCategory: `docs`)
14. `.claude/runbooks/validation_alert.md` (215 lines, language: `markdown`, fileCategory: `docs`)

**Additional context from main session:**

Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").

Never read `.env` or any secrets file; it is not in your batch.
