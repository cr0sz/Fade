const fs = require('fs');
const d = JSON.parse(fs.readFileSync('tmp/arch-input.json', 'utf8'));

const layers = [
  ['layer:api', 'API & Interfaces', 'FastAPI application, route handlers, dashboard page, validation API server, and the Discord bot that expose signals, scores, health and governance data to operators.'],
  ['layer:ingestion', 'Data Ingestion & Providers', 'News, social, market, smart-money and AI provider adapters with their shared base classes, plus the market data logger and history/backfill jobs that feed raw data into the pipeline.'],
  ['layer:engines', 'Signal Engines & Orchestration', 'Sentiment, narrative, regime, trend, confluence and governance engines that turn ingested data into scored signals, plus the event bus, task workers and scheduler that drive them.'],
  ['layer:validation', 'Validation & Governance', 'Signal edge analyzer, out-of-sample and walk-forward validation, backtest/replay tooling, and the governance/lessons pipelines that check signals against real price outcomes.'],
  ['layer:trading', 'Trading Execution', 'Paper and live trading engine with Binance/Alpaca broker adapters, truth router, OOS/risk guards, portfolio and executor logic that route proven signals into orders behind the safety layer.'],
  ['layer:core', 'Core Services', 'Shared runtime foundations such as settings, logging, Redis cache, LLM routing, cost guards, observability metrics and signal tracing used across every service.'],
  ['layer:data', 'Data Layer', 'PostgreSQL migrations and table definitions, repositories, database connection helpers, domain models, and SQL verification queries for the signal and outcome schema.'],
  ['layer:infrastructure', 'Infrastructure & Monitoring', 'Dockerfiles, docker-compose, Makefile, deploy/preflight scripts, the watchdog service, and Homepage/Prometheus monitoring configuration that run and watch the stack.'],
  ['layer:tests', 'Tests', 'Pytest suites and fixtures covering the broker, executor, risk, portfolio, truth router and dashboard behavior, plus the pipeline smoke test.'],
  ['layer:docs-config', 'Documentation & Project Config', 'README, architecture notes, runbooks, agent/skill definitions, environment templates, dependency manifests and Claude Code safety hooks and settings for the project.'],
];
const L = Object.fromEntries(layers.map(l => [l[0], []]));

function classify(n) {
  const p = n.filePath.replace(/\\/g, '/');
  const t = n.type;
  const ov = {
    'src/core/http_server.py': 'api',
    'scripts/embed_news_qdrant.py': 'ingestion', 'scripts/backfill_news.py': 'ingestion', 'scripts/ingest_binance_history.py': 'ingestion',
    'scripts/cluster_analytics.py': 'validation', 'scripts/oos_reeval.py': 'validation', 'scripts/aggregate_stats.py': 'validation',
    'scripts/backtest.py': 'validation', 'scripts/meanrev_replay.py': 'validation', 'scripts/param_sweep.py': 'validation',
    'scripts/replay_signals.py': 'validation', 'scripts/router_walkforward.py': 'validation', 'scripts/lessons_learned.py': 'validation',
    'scripts/governance_engine.py': 'validation', 'scripts/run_lessons_pipeline.py': 'validation', 'scripts/apply_optimization.py': 'validation',
    'scripts/meanrev_seed_router.py': 'trading', 'scripts/binance_income.py': 'trading', 'scripts/live_readiness.py': 'trading',
    'scripts/pipeline_smoke_test.py': 'tests',
    'scripts/critical_watchdog.py': 'infrastructure', 'scripts/pipeline_report.py': 'infrastructure', 'scripts/check_sql_schema.py': 'infrastructure',
    'scripts/show_effective_config.py': 'infrastructure', 'scripts/health_check.sh': 'infrastructure', 'scripts/nightly_claude_diagnose.sh': 'infrastructure',
    'scripts/deploy.sh': 'infrastructure', 'scripts/preflight.sh': 'infrastructure', 'scripts/install_autopush.sh': 'infrastructure',
    'scripts/git-hooks/pre-push': 'infrastructure', 'scripts/backup_hooks/discord-notify.sh': 'infrastructure',
    'scripts/sql_check_allow.txt': 'docs-config',
    'paper_trading_engine/requirements.txt': 'trading', 'signal_edge_analyzer/requirements.txt': 'validation',
    'requirements.txt': 'docs-config', 'requirements-dev.txt': 'docs-config',
    'src/__init__.py': 'core', 'src/validation/models.py': 'validation',
  };
  if (ov[p]) return ov[p];
  if (t === 'table' || t === 'schema') return 'data';
  if (p.startsWith('migrations/') || p.startsWith('sql/')) return 'data';
  if (p.startsWith('src/db/') || p.startsWith('src/models/')) return 'data';
  if (p.startsWith('src/api/') || p.startsWith('src/discord_bot/')) return 'api';
  if (p.startsWith('src/providers/') || p.startsWith('src/market_data_logger/')) return 'ingestion';
  if (p.startsWith('src/engines/') || p.startsWith('src/pipeline/') || p.startsWith('src/scheduler/')) return 'engines';
  if (p.startsWith('src/validation/') || p.startsWith('src/freetime_researcher/') || p.startsWith('signal_edge_analyzer/')) return t === 'service' ? 'infrastructure' : 'validation';
  if (p.startsWith('paper_trading_engine/')) return t === 'service' ? 'infrastructure' : 'trading';
  if (p.startsWith('src/core/') || p.startsWith('src/cache/')) return 'core';
  if (p.startsWith('src/watchdog/') || p.startsWith('monitoring/') || p.startsWith('docker/')) return 'infrastructure';
  if (p === 'docker-compose.yml' || p === 'Makefile') return 'infrastructure';
  if (p.startsWith('tests/')) return 'tests';
  if (p.startsWith('.claude/') || p.startsWith('docs/') || t === 'document' || t === 'config') return 'docs-config';
  if (p === '.gitattributes' || p.endsWith('.gitkeep') || p.startsWith('.ua/') || p.startsWith('graphify-out/') || p.startsWith('backups/')) return 'docs-config';
  return null;
}
const un = [];
for (const n of d.fileNodes) {
  const c = classify(n);
  if (!c) { un.push(n.id); continue; }
  L['layer:' + c].push(n.id);
}
console.log('unclassified', un);
const out = layers.map(([id, name, description]) => ({ id, name, description, nodeIds: L[id] }));
let total = 0;
for (const l of out) { total += l.nodeIds.length; console.log(l.id, l.nodeIds.length); }
const all = out.flatMap(l => l.nodeIds);
const ids = new Set(d.fileNodes.map(n => n.id));
console.log('total', total, 'input', ids.size, 'unique', new Set(all).size,
  'missing', [...ids].filter(i => !all.includes(i)).length, 'extra', all.filter(i => !ids.has(i)).length);
if (un.length === 0) {
  fs.mkdirSync('intermediate', { recursive: true });
  fs.writeFileSync('intermediate/layers.json', JSON.stringify(out, null, 2));
}
