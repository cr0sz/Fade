-- Migration 028: Strategy Graveyard
-- Tracks engines and strategies that were retired, their reason, and performance context.
-- Pre-populated with known historical retirements from project memory.

CREATE TABLE IF NOT EXISTS retired_strategies (
    id                        BIGSERIAL    PRIMARY KEY,
    strategy_name             TEXT         NOT NULL,
    engine_key                TEXT,
    retired_at                TIMESTAMPTZ  NOT NULL,
    sprint                    TEXT,
    retirement_reason         TEXT         NOT NULL,
    retirement_category       TEXT         NOT NULL,
    win_rate_at_retirement    NUMERIC(6,4),
    notes                     TEXT,
    reactivated_at            TIMESTAMPTZ,
    reactivation_notes        TEXT,
    created_at                TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_retired_retired_at
    ON retired_strategies (retired_at DESC);

CREATE INDEX IF NOT EXISTS idx_retired_category
    ON retired_strategies (retirement_category, retired_at DESC);

INSERT INTO retired_strategies
    (strategy_name, engine_key, retired_at, sprint, retirement_reason,
     retirement_category, win_rate_at_retirement, notes, reactivated_at, reactivation_notes)
VALUES
    ('GPT Validator',
     'GPTValidatorEngine',
     '2026-01-15 00:00:00+00',
     'Sprint 8',
     'Disabled — LLM validation latency too high for signal cycle time; replaced by local Ollama shadow auditor.',
     'experiment',
     NULL,
     'Was used as a secondary gate on signals. API call latency disrupted signal freshness window.',
     NULL, NULL),

    ('NASDAQ Flow Engine',
     'NASDAQFlowEngine',
     '2026-03-01 00:00:00+00',
     'Sprint 14',
     'Disabled — 0% win rate over 30-day evaluation window on live paper trades.',
     'performance',
     0.0000,
     'NASDAQ institutional flow signals showed zero directional accuracy. All NASDAQ trades disabled.',
     NULL, NULL),

    ('Trend Detection Engine (first exclusion)',
     'TrendDetectionEngine',
     '2026-05-15 00:00:00+00',
     'Sprint 32',
     'Temporarily excluded — accuracy dropped below 45% threshold across all evaluation windows.',
     'accuracy',
     NULL,
     'Auto-excluded by engine lifecycle system after accuracy fell below floor. Re-enabled after recalibration confirmed recovery.',
     '2026-05-22 00:00:00+00',
     'Re-enabled Sprint 33 after accuracy recovery confirmed via replay validation.'),

    ('Institutional Flow Engine (IFE)',
     'InstitutionalFlowEngine',
     '2026-05-20 00:00:00+00',
     'Sprint 32',
     'Excluded — noise contribution score exceeded threshold; adding FP signals to confluence aggregation.',
     'noise',
     NULL,
     'High noise_contribution metric in engine_trust_scores. Awaiting investigation into data source quality.',
     NULL, NULL),

    ('DOT / ARB Asset Universe',
     NULL,
     '2026-05-10 00:00:00+00',
     'Sprint 30',
     'Asset pair banned — slippage and spread consistently exceeded measured edge on live fills.',
     'banned',
     NULL,
     'DOT and ARB removed from the tradeable universe after analysis showed spread cost > signal edge.',
     NULL, NULL)
ON CONFLICT DO NOTHING;
