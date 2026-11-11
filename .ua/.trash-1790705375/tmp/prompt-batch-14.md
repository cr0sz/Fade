Analyze these files and produce GraphNode and GraphEdge objects.
Project root: `C:/Users/bekir/Documents/projeler/Fade`
Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`
Batch: `14/24`
Skill directory (for bundled scripts): `C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand`
Data directory ($UA_DIR): `C:/Users/bekir/Documents/projeler/Fade/.ua`
Output: write to `C:/Users/bekir/Documents/projeler/Fade/.ua/intermediate/batch-14.json` (single-file mode) OR `batch-14-part-<k>.json` (split mode, per Step B of your output protocol).

Pre-resolved import data for this batch (use directly — do NOT re-resolve imports from source):
```json
{"migrations/000_create_miniflux_db.sql":[],"migrations/001_initial.sql":[],"migrations/002_level2.sql":[],"migrations/003_level3.sql":[],"migrations/004_level4_validation.sql":[],"migrations/005_signal_truth_layer.sql":[],"migrations/006_replay_system.sql":[],"migrations/007_observability.sql":[],"migrations/008_governance.sql":[],"migrations/009_meta_intelligence.sql":[],"migrations/010_provider_ingestion_log.sql":[],"migrations/011_signal_trace.sql":[],"migrations/012_signal_feedback.sql":[],"migrations/013_signal_edge_analyzer.sql":[],"migrations/014_seed_tickers.sql":[],"migrations/015_ticker_quotes.sql":[],"migrations/016_paper_trades.sql":[],"migrations/017_narrative_persistence.sql":[],"migrations/018_technical_score.sql":[],"migrations/019_signal_edge_agg_uq.sql":[],"migrations/020_optimization_runs.sql":[],"migrations/021_governance_log.sql":[],"migrations/022_stale_quote_alert.sql":[],"migrations/023_funding_liquidations.sql":[],"migrations/024_research_runs.sql":[],"migrations/025_infra_watchdog_alerts.sql":[],"migrations/026_signal_trace_ticker.sql":[],"migrations/028_retired_strategies.sql":[],"migrations/029_personal_finance.sql":[],"migrations/030_daily_briefs.sql":[],"migrations/031_open_position_unique.sql":[],"migrations/032_open_position_unique_per_mode.sql":[],"migrations/033_news_impact_scoring.sql":[],"migrations/034_truth_router.sql":[],"migrations/035_trade_reflections.sql":[],"migrations/036_etf_tickers.sql":[],"migrations/037_fix_outcome_direction_correct.sql":[],"migrations/038_equity_snapshots.sql":[],"migrations/039_cide_decisions.sql":[],"migrations/040_trade_post_mortems.sql":[],"migrations/041_signal_trace_regime_filter.sql":[],"migrations/042_capital_growth_log.sql":[],"migrations/043_meta_strategy_runs.sql":[],"migrations/044_paper_trades_signal_id_unique.sql":[],"migrations/045_paper_trades_stop_order_ids.sql":[],"migrations/046_funding_payments.sql":[]}
```

Cross-batch neighbors with their exported symbols (confidence boost for cross-batch edges):
```json
{}
```

Files to analyze in this batch (every entry MUST be passed through to `batchFiles` with all four fields — `path`, `language`, `sizeLines`, `fileCategory`):
1. `migrations/000_create_miniflux_db.sql` (2 lines, language: `sql`, fileCategory: `data`)
2. `migrations/001_initial.sql` (200 lines, language: `sql`, fileCategory: `data`)
3. `migrations/002_level2.sql` (50 lines, language: `sql`, fileCategory: `data`)
4. `migrations/003_level3.sql` (100 lines, language: `sql`, fileCategory: `data`)
5. `migrations/004_level4_validation.sql` (167 lines, language: `sql`, fileCategory: `data`)
6. `migrations/005_signal_truth_layer.sql` (49 lines, language: `sql`, fileCategory: `data`)
7. `migrations/006_replay_system.sql` (81 lines, language: `sql`, fileCategory: `data`)
8. `migrations/007_observability.sql` (37 lines, language: `sql`, fileCategory: `data`)
9. `migrations/008_governance.sql` (118 lines, language: `sql`, fileCategory: `data`)
10. `migrations/009_meta_intelligence.sql` (223 lines, language: `sql`, fileCategory: `data`)
11. `migrations/010_provider_ingestion_log.sql` (30 lines, language: `sql`, fileCategory: `data`)
12. `migrations/011_signal_trace.sql` (25 lines, language: `sql`, fileCategory: `data`)
13. `migrations/012_signal_feedback.sql` (72 lines, language: `sql`, fileCategory: `data`)
14. `migrations/013_signal_edge_analyzer.sql` (88 lines, language: `sql`, fileCategory: `data`)
15. `migrations/014_seed_tickers.sql` (82 lines, language: `sql`, fileCategory: `data`)
16. `migrations/015_ticker_quotes.sql` (28 lines, language: `sql`, fileCategory: `data`)
17. `migrations/016_paper_trades.sql` (28 lines, language: `sql`, fileCategory: `data`)
18. `migrations/017_narrative_persistence.sql` (5 lines, language: `sql`, fileCategory: `data`)
19. `migrations/018_technical_score.sql` (10 lines, language: `sql`, fileCategory: `data`)
20. `migrations/019_signal_edge_agg_uq.sql` (23 lines, language: `sql`, fileCategory: `data`)
21. `migrations/020_optimization_runs.sql` (15 lines, language: `sql`, fileCategory: `data`)
22. `migrations/021_governance_log.sql` (36 lines, language: `sql`, fileCategory: `data`)
23. `migrations/022_stale_quote_alert.sql` (20 lines, language: `sql`, fileCategory: `data`)
24. `migrations/023_funding_liquidations.sql` (42 lines, language: `sql`, fileCategory: `data`)
25. `migrations/024_research_runs.sql` (32 lines, language: `sql`, fileCategory: `data`)
26. `migrations/025_infra_watchdog_alerts.sql` (27 lines, language: `sql`, fileCategory: `data`)
27. `migrations/026_signal_trace_ticker.sql` (5 lines, language: `sql`, fileCategory: `data`)
28. `migrations/028_retired_strategies.sql` (80 lines, language: `sql`, fileCategory: `data`)
29. `migrations/029_personal_finance.sql` (24 lines, language: `sql`, fileCategory: `data`)
30. `migrations/030_daily_briefs.sql` (11 lines, language: `sql`, fileCategory: `data`)
31. `migrations/031_open_position_unique.sql` (6 lines, language: `sql`, fileCategory: `data`)
32. `migrations/032_open_position_unique_per_mode.sql` (7 lines, language: `sql`, fileCategory: `data`)
33. `migrations/033_news_impact_scoring.sql` (20 lines, language: `sql`, fileCategory: `data`)
34. `migrations/034_truth_router.sql` (32 lines, language: `sql`, fileCategory: `data`)
35. `migrations/035_trade_reflections.sql` (24 lines, language: `sql`, fileCategory: `data`)
36. `migrations/036_etf_tickers.sql` (29 lines, language: `sql`, fileCategory: `data`)
37. `migrations/037_fix_outcome_direction_correct.sql` (20 lines, language: `sql`, fileCategory: `data`)
38. `migrations/038_equity_snapshots.sql` (16 lines, language: `sql`, fileCategory: `data`)
39. `migrations/039_cide_decisions.sql` (12 lines, language: `sql`, fileCategory: `data`)
40. `migrations/040_trade_post_mortems.sql` (18 lines, language: `sql`, fileCategory: `data`)
41. `migrations/041_signal_trace_regime_filter.sql` (1 lines, language: `sql`, fileCategory: `data`)
42. `migrations/042_capital_growth_log.sql` (10 lines, language: `sql`, fileCategory: `data`)
43. `migrations/043_meta_strategy_runs.sql` (10 lines, language: `sql`, fileCategory: `data`)
44. `migrations/044_paper_trades_signal_id_unique.sql` (7 lines, language: `sql`, fileCategory: `data`)
45. `migrations/045_paper_trades_stop_order_ids.sql` (5 lines, language: `sql`, fileCategory: `data`)
46. `migrations/046_funding_payments.sql` (10 lines, language: `sql`, fileCategory: `data`)

**Additional context from main session:**

Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").

Never read `.env` or any secrets file; it is not in your batch.
