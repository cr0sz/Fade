-- 010_provider_ingestion_log.sql
-- Per-provider, per-cycle ingestion audit log.
--
-- Answers without guessing from raw logs:
--   "Did Bloomberg actually fetch data this cycle?"
--   "Where are duplicates coming from?"
--   "Which provider is failing / slow?"
--
-- Written by NewsIntelligenceEngine.run_cycle() after each provider finishes.
-- cycle_id links every row back to the structlog trace for that job execution.

CREATE TABLE IF NOT EXISTS provider_ingestion_log (
    id           BIGSERIAL    PRIMARY KEY,
    cycle_id     TEXT         NOT NULL,          -- UUID string from worker cycle
    job          TEXT         NOT NULL,          -- e.g. 'news_cycle'
    provider     TEXT         NOT NULL,          -- e.g. 'bloomberg', 'newsapi'
    fetched      INT          NOT NULL DEFAULT 0,
    new_count    INT          NOT NULL DEFAULT 0,
    dup_exact    INT          NOT NULL DEFAULT 0,
    dup_fuzzy    INT          NOT NULL DEFAULT 0,
    dup_semantic INT          NOT NULL DEFAULT 0,
    errors       INT          NOT NULL DEFAULT 0,
    latency_ms   FLOAT        NOT NULL DEFAULT 0,
    status       TEXT         NOT NULL DEFAULT 'ok',  -- ok | failed | circuit_open
    ts           TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pil_cycle_id  ON provider_ingestion_log(cycle_id);
CREATE INDEX IF NOT EXISTS idx_pil_ts        ON provider_ingestion_log(ts DESC);
CREATE INDEX IF NOT EXISTS idx_pil_provider  ON provider_ingestion_log(provider, ts DESC);
