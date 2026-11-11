Analyze these files and produce GraphNode and GraphEdge objects.
Project root: `C:/Users/bekir/Documents/projeler/Fade`
Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`
Batch: `10/24`
Skill directory (for bundled scripts): `C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand`
Data directory ($UA_DIR): `C:/Users/bekir/Documents/projeler/Fade/.ua`
Output: write to `C:/Users/bekir/Documents/projeler/Fade/.ua/intermediate/batch-10.json` (single-file mode) OR `batch-10-part-<k>.json` (split mode, per Step B of your output protocol).

Pre-resolved import data for this batch (use directly — do NOT re-resolve imports from source):
```json
{"src/api/main.py":["src/api/routes/__init__.py","src/api/routes/dashboard.py","src/api/routes/edge_intelligence.py","src/api/routes/health.py","src/api/routes/intelligence.py","src/api/routes/market.py","src/api/routes/news.py","src/api/routes/sentiment.py","src/api/routes/trends.py","src/cache/redis_client.py","src/core/config.py","src/core/cost_control.py","src/core/logging.py","src/core/metrics.py","src/db/connection.py"],"src/api/routes/__init__.py":[],"src/api/routes/dashboard.py":["src/core/logging.py"],"src/api/routes/edge_intelligence.py":["src/core/logging.py"],"src/api/routes/sentiment.py":["src/engines/sentiment_engine.py"],"src/core/metrics.py":[],"tests/test_autopsy.py":["src/api/routes/__init__.py"],"tests/test_dashboard_proxy.py":["src/api/routes/__init__.py","src/api/routes/dashboard.py"],"tests/test_graveyard.py":["src/api/routes/dashboard.py"]}
```

Cross-batch neighbors with their exported symbols (confidence boost for cross-batch edges):
```json
{"src/api/main.py":[{"path":"src/api/routes/health.py","batchIndex":5,"symbols":["HealthResponse","health_check","readiness_check","circuit_status"]},{"path":"src/api/routes/intelligence.py","batchIndex":8,"symbols":["get_macro_regime","get_institutional_flows","get_ticker_flow","get_confluence","get_ticker_confluence"]},{"path":"src/api/routes/market.py","batchIndex":5,"symbols":["get_quote","get_us_market","get_bist_market","get_cost_status","analyst_query","get_market_digest"]},{"path":"src/api/routes/news.py","batchIndex":5,"symbols":["get_news_impact","get_news_unscored_count","get_news","get_macro_news"]},{"path":"src/api/routes/trends.py","batchIndex":5,"symbols":["get_hot_trends","get_signals","get_narratives"]},{"path":"src/cache/redis_client.py","batchIndex":3,"symbols":["get_redis","close_redis","CacheClient","RateLimiter"]},{"path":"src/core/config.py","batchIndex":5,"symbols":["_FlexListMixin","_FlexEnvSource","_FlexDotEnvSource","Settings","get_settings"]},{"path":"src/core/cost_control.py","batchIndex":5,"symbols":["ProviderTier","BudgetStatus","CostController","get_cost_controller","init_cost_controller"]},{"path":"src/core/logging.py","batchIndex":2,"symbols":["utc_now","configure_logging","bind_service_context","bind_cycle_context","clear_cycle_context","get_logger"]},{"path":"src/db/connection.py","batchIndex":3,"symbols":["get_db_pool","close_db_pool"]}],"src/api/routes/dashboard.py":[{"path":"src/core/logging.py","batchIndex":2,"symbols":["utc_now","configure_logging","bind_service_context","bind_cycle_context","clear_cycle_context","get_logger"]}],"src/api/routes/edge_intelligence.py":[{"path":"src/core/logging.py","batchIndex":2,"symbols":["utc_now","configure_logging","bind_service_context","bind_cycle_context","clear_cycle_context","get_logger"]}],"src/api/routes/sentiment.py":[{"path":"src/engines/sentiment_engine.py","batchIndex":5,"symbols":["SentimentCycleResult","SentimentEngine"]}],"src/core/metrics.py":[{"path":"src/scheduler/worker.py","batchIndex":8,"symbols":["_MetricsHandler","_start_metrics_server","MarketIntelligenceWorker","main"]}]}
```

Files to analyze in this batch (every entry MUST be passed through to `batchFiles` with all four fields — `path`, `language`, `sizeLines`, `fileCategory`):
1. `src/api/main.py` (104 lines, language: `python`, fileCategory: `code`)
2. `src/api/routes/__init__.py` (0 lines, language: `python`, fileCategory: `code`)
3. `src/api/routes/dashboard.py` (1340 lines, language: `python`, fileCategory: `code`)
4. `src/api/routes/edge_intelligence.py` (213 lines, language: `python`, fileCategory: `code`)
5. `src/api/routes/sentiment.py` (25 lines, language: `python`, fileCategory: `code`)
6. `src/core/metrics.py` (80 lines, language: `python`, fileCategory: `code`)
7. `tests/test_autopsy.py` (324 lines, language: `python`, fileCategory: `code`)
8. `tests/test_dashboard_proxy.py` (161 lines, language: `python`, fileCategory: `code`)
9. `tests/test_graveyard.py` (153 lines, language: `python`, fileCategory: `code`)

**Additional context from main session:**

Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").

Never read `.env` or any secrets file; it is not in your batch.
