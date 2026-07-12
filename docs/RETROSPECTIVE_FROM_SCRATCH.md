# Retrospective: What We'd Do Differently From Scratch

*Written 2026-06-12, after 51 sprints. This is a design critique of the system as
built, derived from the actual incident history — not hindsight about market edge.*

## The five structural lessons

### 1. Ground truth first, engines second
The single most expensive mistake: the outcome tracker measured engines' own
opinions instead of price returns for months (fixed Sprint 35). Every metric,
calibration, and governance decision built on top of it was corrupted, and when
real truth arrived (Sprint 38), most engines turned out to be anti-predictive —
the truth router now inverts or rejects most of what they say.

**From scratch:** define the label (forward price return at fixed horizons) on
day one, before writing a single engine. An engine that cannot be scored against
that label does not get built.

### 2. Backtest-first, one code path
The backtester, replay scripts, and live engine were built separately and
converged late (Sprints 19, 39 replays). Gates and thresholds were tuned on tiny
live samples (dozens of trades), producing the long string of
threshold-tweaking sprints (10, 11, 14, 16, 18, 19, 20, 29, 33).

**From scratch:** one event-driven core that consumes a market-data stream and
doesn't know whether the stream is historical or live. Every gate change ships
with a walk-forward replay result and a sample-size statement before it touches
the live loop.

### 3. State machine + persistence, not in-memory flags
Restart-loss bugs recurred: trail peaks lost (Sprint 31), `_partial_closed` set
dead (Sprint 21), max_hold resetting 6h→2h on restart (session 2026-06-08),
cooldowns and peak_balance only persisted in Sprint 22. Each was the same root
cause: position lifecycle state living in Python object attributes.

**From scratch:** a position is a row with an explicit state machine
(open → partial_1 → partial_2 → trailing → closed) in Postgres; the engine is a
stateless reducer over it. Restart safety becomes a property, not a patch series.

### 4. One config system with a landing proof
~125 scattered `os.getenv` calls across the codebase bypass the pydantic
settings module in `src/core/config.py`. Result: parameters that two sprints
claimed to deploy were not actually in effect (trail values, Sprint 34), and
nobody could see what config the engine was running.

**From scratch:** every tunable goes through one typed settings object; the
engine logs the full effective config + hash at startup and alerts on drift.
(Retrofit shipped 2026-06-12 — see `effective_config` event and
`scripts/show_effective_config.py`.)

### 5. Fewer moving parts
~25 Docker services, ~30 "engines", a 3,000-line `PaperTradingEngine` class, and
raw SQL strings scattered through 179 Python files. Consequences from the
record: three separate SQL-drift sprints (30, 32, 36), silent service deaths
(bot dead 10h Sprint 34, MATIC feed dead 47h Sprint 31, MATIC incident #3),
and a watchdog built in Sprint 49 to compensate.

**From scratch:** 4 services (ingest, signal/score, trade, API+dashboard); a
repository layer instead of inline SQL; engines as plug-in feature functions
inside one scorer process, not containers. The watchdog problem mostly
disappears when there are 4 things to watch instead of 25.

## Smaller things we'd keep
- Paper-first discipline and the live-mode confirmation rule.
- Redis-persisted risk guard / kill switch design.
- The truth router concept (score signal *sources* by measured lift) — we'd
  just build it on day one instead of sprint 38.
- The debugging playbook practices (log histograms, schema-vs-SQL scans,
  config-landed checks) — now partially automated:
  `scripts/check_sql_schema.py` and the effective-config proof.

## Standing guardrails added 2026-06-12
| Tool | Bug class it kills | How to run |
|---|---|---|
| `scripts/check_sql_schema.py` | SQL referencing nonexistent tables/columns (Sprints 30/32/36) | `make check-sql`, auto-runs warn-only in preflight |
| `effective_config` startup event | tuned params silently not landing / resetting on restart (Sprints 13/31/33/34) | `python3 scripts/show_effective_config.py`, Telegram alert on drift |
