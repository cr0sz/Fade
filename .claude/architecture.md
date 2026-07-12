# System Architecture

## Service Topology

```
                          ┌─────────────────────────────────────────┐
                          │           EXTERNAL DATA SOURCES          │
                          │                                          │
                          │  NewsAPI  Bloomberg  Reddit  X  Polygon  │
                          │  Finnhub  AlphaVantage  WhaleAlert EDGAR │
                          └───────────────┬─────────────────────────┘
                                          │
                                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        PROVIDER LAYER (src/providers/)               │
│                                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │  News        │  │  Social      │  │  Market      │              │
│  │  Providers   │  │  Providers   │  │  Providers   │              │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘              │
│         │                 │                  │                      │
│  ┌──────┴───────┐  ┌──────┴────────┐                               │
│  │  AI          │  │  Smart Money  │                               │
│  │  Providers   │  │  Providers    │                               │
│  └──────────────┘  └───────────────┘                               │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        ENGINE LAYER (src/engines/)                   │
│                                                                      │
│  ┌─────────────────────┐   ┌──────────────────────────┐            │
│  │  News Intelligence  │   │  Social Sentiment Engine  │           │
│  │  - Dedup (hash)     │   │  - Mention velocity       │           │
│  │  - Clustering       │   │  - Hype/panic detection   │           │
│  │  - Ticker extract   │   │  - Sentiment scoring      │           │
│  │  - AI summarization │   └──────────────────────────┘            │
│  └─────────────────────┘                                            │
│                                                                      │
│  ┌─────────────────────┐   ┌──────────────────────────┐            │
│  │  Smart Money Module │   │  AI Analyst Engine        │           │
│  │  - ETF flows        │   │  - Multi-provider router  │           │
│  │  - Insider trades   │   │  - Cost-aware selection   │           │
│  │  - Whale movements  │   │  - Response caching       │           │
│  └─────────────────────┘   │  - Structured prompts     │           │
│                             └──────────────────────────┘            │
│  ┌─────────────────────────────────────────────────────────┐       │
│  │  Trend Detection Engine                                  │       │
│  │  Composite Score = f(news_score, social_score,           │       │
│  │                      smart_money_score, volume_score)    │       │
│  └─────────────────────────────────────────────────────────┘       │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
┌─────────────────┐  ┌─────────────────┐  ┌──────────────────────┐
│  FastAPI        │  │  Scheduler      │  │  Telegram Bot        │
│  (src/api/)     │  │  (src/scheduler)│  │  (src/telegram/)     │
│                 │  │                 │  │                      │
│  /market        │  │  News: 5min     │  │  /market             │
│  /news          │  │  Social: 15min  │  │  /us                 │
│  /sentiment     │  │  Smart$: 1hr    │  │  /bist               │
│  /trends        │  │  AI digest: 1hr │  │  /hot                │
│  /health        │  │  Cleanup: daily │  │  /news               │
└────────┬────────┘  └────────┬────────┘  │  /macro              │
         │                    │           │  /watch <ticker>     │
         └────────────────────┘           └──────────────────────┘
                    │
         ┌──────────┴──────────┐
         ▼                     ▼
┌─────────────────┐   ┌─────────────────┐
│  PostgreSQL 16  │   │  Redis 7        │
│                 │   │                 │
│  news           │   │  AI cache       │
│  social_posts   │   │  rate limits    │
│  tickers        │   │  session data   │
│  ai_outputs     │   │  cost counters  │
│  trends         │   │  pub/sub        │
│  smart_money    │   └─────────────────┘
│  provider_logs  │
│  alerts         │
└─────────────────┘
```

## Signal Truth + Replay + Intelligence + Governance Layer (Phase 1–4)

```
signals_registry (immutable archive)
      │
      ├──▶ OutcomeTrackingEngine  — writes signal_outcomes (1h/6h/24h/72h/7d)
      │         port 8013
      │
      ├──▶ SignalScoringEngine    — writes signal_scores (strength/half-life)
      │         port 8014
      │
      ├──▶ BacktestingEngine     — reads signal_outcomes + signal_scores
      │         │                  writes replay_runs + replay_metrics
      │         │
      │         └── ReplayScheduler (Redis-backed queue)
      │                   port 8015
      │                   auto: weekly + monthly
      │                   POST /backtest/replay/trigger → API trigger
      │
      ├──▶ ValidationAlertEngine — reads replay_metrics, signal_scores,
      │         │                  replay_runs, signal_outcomes,
      │         │                  engine_trust_scores, narrative_stability_scores
      │         │                  writes validation_alerts (11 alert types)
      │         │                  delivers Telegram alerts
      │         │         port 8016
      │         │
      │         └── ValidationIntelligenceAPI (read-only aggregation layer)
      │             ReplayObservabilityDashboard (read-only analytics layer)
      │
      └──▶ EngineGovernanceEngine — computes trust scores every GOVERNANCE_INTERVAL
                │                   writes engine_trust_scores + governance_recommendations
                │                   orchestrates AdaptiveWeightingEngine + NarrativeStabilityEngine
                │         port 8017
                │
                ├── AdaptiveWeightingEngine (reads trust scores + calibration, writes recommendations)
                ├── NarrativeStabilityEngine (writes narrative_stability_scores)
                └── NoiseContributionEngine (on-demand, read-only — no DB writes)

Validation API (port 8010) exposes read-only endpoints:
  /api/v1/validation/*         — signal registry, outcomes, calibration, accuracy
  /api/v1/scores/*             — predictive strength, half-life
  /api/v1/backtest/*           — replay results, engine ranking, narratives, regimes
  /api/v1/validation/system-health    — overall system health snapshot (Phase 3)
  /api/v1/validation/engine-decay     — per-engine decay curves + is_decaying flag
  /api/v1/validation/calibration      — confidence tier calibration analysis
  /api/v1/validation/failures         — replay failures, coverage gaps, pending outcomes
  /api/v1/validation/reliability      — engine ranking, best/worst regime + narrative
  /api/v1/validation/observability    — full replay observability bundle

Alert State:
  validation_alerts table — fired_at, severity, alert_type, engine_name, is_active
  Redis alert:suppress:{type}:{engine} — manual cooldown override key (EX 14400)

Governance API (port 8010) exposes read-only governance endpoints:
  /api/v1/governance/trust         — engine trust ranking (0–100 composite score)
  /api/v1/governance/stability     — unstable engines + recovery analysis
  /api/v1/governance/noise         — FP contribution + amplification chains
  /api/v1/governance/narratives    — narrative persistence + degradation
  /api/v1/governance/recommendations — active unacknowledged governance recs
  /api/v1/governance/recovery      — per-engine dip/recovery history

      └──▶ StrategicMemoryEngine — reads engine_trust_scores, replay_metrics,
                │                   macro_regimes, signal_scores, signal_outcomes
                │                   writes strategic_memory_events,
                │                   regime_transition_records, narrative_lifecycle_records,
                │                   strategic_failure_clusters, meta_intelligence_snapshots
                │         port 8018
                │
                ├── RegimeTransitionIntelligence (reads macro_regimes, engine_trust_scores;
                │       writes regime_transition_records — transition fragility + danger)
                ├── NarrativeLifecycleEngine (reads replay_metrics narrative_breakdown;
                │       writes narrative_lifecycle_records — survivability, phases)
                └── StrategicFailureAnalysisEngine (reads replay_metrics, engine_trust_scores,
                        signal_scores; upserts strategic_failure_clusters)

Meta-Intelligence API (port 8010) exposes read-only endpoints:
  /api/v1/meta/failures    — Historical Failure Atlas + systemic risk
  /api/v1/meta/transitions  — Regime Transition Risk Report
  /api/v1/meta/narratives   — Narrative Lifecycle Intelligence Report
  /api/v1/meta/stability    — Strategic Stability Ranking + governance stress timeline
  /api/v1/meta/similarity   — Historical Similarity Detection
  /api/v1/meta/risk         — Composite risk dashboard (all Phase 5 signals)

Prometheus scrapes: :8010, :8011, :8012, :8013, :8014, :8015, :8016, :8017, :8018

Grafana Dashboards (auto-provisioned):
  mktintel-signal-accuracy       — legacy signal accuracy
  mktintel-engine-performance    — legacy engine performance
  mktintel-backtest-results      — legacy backtest results
  mktintel-confidence-calibration — legacy confidence calibration
  mktintel-engine-accuracy       — precision/recall/F1/FPR/calibration trends (Phase 3)
  mktintel-signal-decay          — half-life curves + predictive strength decay (Phase 3)
  mktintel-replay-health         — queue depth + duration + throughput + alerts (Phase 3)
  mktintel-system-reliability    — overall reliability + regime + engine ranking (Phase 3)
  mktintel-governance            — trust scores, trends, decay velocity, recovery (Phase 4)
  mktintel-stability             — stability, calibration, narrative persistence (Phase 4)
  mktintel-noise-intelligence    — FP contribution, FPR trends, amplification (Phase 4)
  mktintel-adaptive-recommendations — open recs, action distribution, governance rate (Phase 4)
  mktintel-strategic-stability      — instability index, governance stress, trust trends (Phase 5)
  mktintel-regime-transition        — transition risk matrix, engine fragility ranking (Phase 5)
  mktintel-narrative-lifecycle      — lifecycle table, emergence curves, collapse timeline (Phase 5)
  mktintel-historical-failure       — failure cluster atlas, FP environments, systemic degradation (Phase 5)
```

## Data Pipeline: News Intelligence

```
External News APIs
      │
      ▼
  fetch() — provider-specific HTTP calls
      │
      ▼
  normalize() — → NewsItem(Pydantic)
      │
      ▼
  dedup check — SHA256 hash of (title + source + date)
      │   └── duplicate? → discard
      │
      ▼
  ticker extraction — regex + NLP on title/content
      │
      ▼
  sector classification — ticker → sector lookup
      │
      ▼
  AI summarization — batch → OpenAI/Anthropic
      │
      ▼
  store in PostgreSQL news table
      │
      ▼
  update trend scores
```

## Data Pipeline: Social Sentiment

```
Reddit API + X API v2
      │
      ▼
  fetch() — subreddits + search queries
      │
      ▼
  normalize() — → SocialPost(Pydantic)
      │
      ▼
  ticker mention extraction
      │
      ▼
  sentiment scoring — VADER + AI scoring
      │
      ▼
  velocity calculation — mentions/hour vs 7d avg
      │
      ▼
  hype/panic signal — velocity + sentiment threshold
      │
      ▼
  store in PostgreSQL social_posts
```

## AI Provider Routing Logic

```
Request arrives
      │
      ▼
  Check Redis cache (TTL: 1hr for digests, 5min for live)
      │
  Cache HIT? → return cached response
      │
      ▼
  Check daily budget remaining
      │
  Budget > 80%? → use Anthropic Claude (cheaper)
  Budget > 50%? → use OpenAI GPT-4o-mini
  Budget < 50%? → use cached or skip
      │
      ▼
  Execute LLM call with structured prompt
      │
      ▼
  Store in Redis cache + PostgreSQL ai_outputs
      │
      ▼
  Return response + cost metadata
```

## Cost Control Architecture

```
Every LLM call:
  → log tokens used → provider_usage_logs
  → update daily_cost counter in Redis
  → if daily_cost > DAILY_BUDGET_USD:
      → block further LLM calls
      → serve from cache only
      → alert via Telegram admin notification
```
