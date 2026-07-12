-- 04_feedback_metrics.sql
-- Operational question: Are performance scores being computed and stored?
--
-- Returns the most recent engine score and provider score for each entity.
-- A healthy system shows rows updated today (or within the last 7 days).
--
-- Investigate if:
--   • metric_date is more than 2 days old (FeedbackAggregator stalled)
--   • win_rate_24h is NULL (signal_feedback has no completed outcomes yet)
--   • All providers show NULL win_rate (evaluation engine not writing feedback)

-- ── Engine performance metrics (latest per engine) ─────────────────────────
SELECT
    'engine'                              AS metric_source,
    engine_name                           AS name,
    metric_date,
    ROUND((win_rate_1h  * 100)::numeric, 1) AS win_rate_1h_pct,
    ROUND((win_rate_24h * 100)::numeric, 1) AS win_rate_24h_pct,
    ROUND((win_rate_7d  * 100)::numeric, 1) AS win_rate_7d_pct,
    ROUND(avg_pnl_score::numeric, 4)        AS avg_pnl,
    feedback_signals_total                  AS total_signals,
    computed_at
FROM (
    SELECT DISTINCT ON (engine_name) *
    FROM engine_performance_metrics
    ORDER BY engine_name, metric_date DESC
) latest_engine
WHERE win_rate_24h IS NOT NULL
   OR feedback_signals_total > 0

UNION ALL

-- ── Provider performance metrics (latest per provider) ─────────────────────
SELECT
    'provider'                             AS metric_source,
    provider                               AS name,
    metric_date,
    NULL                                   AS win_rate_1h_pct,
    ROUND((win_rate * 100)::numeric, 1)    AS win_rate_24h_pct,
    NULL                                   AS win_rate_7d_pct,
    ROUND(avg_pnl_score::numeric, 4)       AS avg_pnl,
    total_signals,
    computed_at
FROM (
    SELECT DISTINCT ON (provider) *
    FROM provider_performance_metrics
    ORDER BY provider, metric_date DESC
) latest_provider

ORDER BY metric_source, name;
