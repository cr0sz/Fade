Analyze these files and produce GraphNode and GraphEdge objects.
Project root: `C:/Users/bekir/Documents/projeler/Fade`
Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`
Batch: `1/24`
Skill directory (for bundled scripts): `C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand`
Data directory ($UA_DIR): `C:/Users/bekir/Documents/projeler/Fade/.ua`
Output: write to `C:/Users/bekir/Documents/projeler/Fade/.ua/intermediate/batch-1.json` (single-file mode) OR `batch-1-part-<k>.json` (split mode, per Step B of your output protocol).

Pre-resolved import data for this batch (use directly — do NOT re-resolve imports from source):
```json
{"paper_trading_engine/adaptive_sizer.py":[],"paper_trading_engine/broker_factory.py":["paper_trading_engine/brokers/base.py","paper_trading_engine/brokers/binance.py","paper_trading_engine/brokers/binance_futures.py","paper_trading_engine/brokers/paper.py"],"paper_trading_engine/brokers/__init__.py":["paper_trading_engine/brokers/base.py","paper_trading_engine/brokers/binance.py","paper_trading_engine/brokers/paper.py"],"paper_trading_engine/brokers/alpaca.py":["paper_trading_engine/brokers/base.py"],"paper_trading_engine/brokers/base.py":[],"paper_trading_engine/brokers/binance.py":["paper_trading_engine/brokers/base.py"],"paper_trading_engine/brokers/binance_futures.py":["paper_trading_engine/brokers/base.py"],"paper_trading_engine/brokers/paper.py":["paper_trading_engine/brokers/base.py"],"paper_trading_engine/capital_growth.py":[],"paper_trading_engine/discord_notifier.py":[],"paper_trading_engine/discord_status.py":[],"paper_trading_engine/engine.py":["paper_trading_engine/adaptive_sizer.py","paper_trading_engine/broker_factory.py","paper_trading_engine/capital_growth.py","paper_trading_engine/discord_status.py","paper_trading_engine/equity_tracker.py","paper_trading_engine/gpt_validator.py","paper_trading_engine/llm_auditor.py","paper_trading_engine/models.py","paper_trading_engine/notifier.py","paper_trading_engine/oos_validator.py","paper_trading_engine/portfolio.py","paper_trading_engine/risk.py","paper_trading_engine/risk_governor.py","paper_trading_engine/risk_guard.py","paper_trading_engine/truth_router.py"],"paper_trading_engine/equity_tracker.py":[],"paper_trading_engine/executor.py":["paper_trading_engine/models.py","paper_trading_engine/risk.py"],"paper_trading_engine/gpt_validator.py":[],"paper_trading_engine/llm_auditor.py":[],"paper_trading_engine/models.py":[],"paper_trading_engine/notifier.py":["paper_trading_engine/discord_notifier.py"],"paper_trading_engine/oos_validator.py":[],"paper_trading_engine/portfolio.py":["paper_trading_engine/executor.py","paper_trading_engine/models.py"],"paper_trading_engine/risk.py":[],"paper_trading_engine/risk_governor.py":[],"paper_trading_engine/risk_guard.py":[],"paper_trading_engine/truth_router.py":[],"scripts/cluster_analytics.py":["paper_trading_engine/risk_governor.py"]}
```

Cross-batch neighbors with their exported symbols (confidence boost for cross-batch edges):
```json
{}
```

Files to analyze in this batch (every entry MUST be passed through to `batchFiles` with all four fields — `path`, `language`, `sizeLines`, `fileCategory`):
1. `paper_trading_engine/adaptive_sizer.py` (149 lines, language: `python`, fileCategory: `code`)
2. `paper_trading_engine/broker_factory.py` (79 lines, language: `python`, fileCategory: `code`)
3. `paper_trading_engine/brokers/__init__.py` (5 lines, language: `python`, fileCategory: `code`)
4. `paper_trading_engine/brokers/alpaca.py` (225 lines, language: `python`, fileCategory: `code`)
5. `paper_trading_engine/brokers/base.py` (64 lines, language: `python`, fileCategory: `code`)
6. `paper_trading_engine/brokers/binance.py` (370 lines, language: `python`, fileCategory: `code`)
7. `paper_trading_engine/brokers/binance_futures.py` (774 lines, language: `python`, fileCategory: `code`)
8. `paper_trading_engine/brokers/paper.py` (66 lines, language: `python`, fileCategory: `code`)
9. `paper_trading_engine/capital_growth.py` (176 lines, language: `python`, fileCategory: `code`)
10. `paper_trading_engine/discord_notifier.py` (170 lines, language: `python`, fileCategory: `code`)
11. `paper_trading_engine/discord_status.py` (99 lines, language: `python`, fileCategory: `code`)
12. `paper_trading_engine/engine.py` (4603 lines, language: `python`, fileCategory: `code`)
13. `paper_trading_engine/equity_tracker.py` (41 lines, language: `python`, fileCategory: `code`)
14. `paper_trading_engine/executor.py` (128 lines, language: `python`, fileCategory: `code`)
15. `paper_trading_engine/gpt_validator.py` (112 lines, language: `python`, fileCategory: `code`)
16. `paper_trading_engine/llm_auditor.py` (443 lines, language: `python`, fileCategory: `code`)
17. `paper_trading_engine/models.py` (67 lines, language: `python`, fileCategory: `code`)
18. `paper_trading_engine/notifier.py` (7 lines, language: `python`, fileCategory: `code`)
19. `paper_trading_engine/oos_validator.py` (269 lines, language: `python`, fileCategory: `code`)
20. `paper_trading_engine/portfolio.py` (195 lines, language: `python`, fileCategory: `code`)
21. `paper_trading_engine/risk.py` (329 lines, language: `python`, fileCategory: `code`)
22. `paper_trading_engine/risk_governor.py` (216 lines, language: `python`, fileCategory: `code`)
23. `paper_trading_engine/risk_guard.py` (164 lines, language: `python`, fileCategory: `code`)
24. `paper_trading_engine/truth_router.py` (540 lines, language: `python`, fileCategory: `code`)
25. `scripts/cluster_analytics.py` (114 lines, language: `python`, fileCategory: `code`)

**Additional context from main session:**

Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").

Never read `.env` or any secrets file; it is not in your batch.
