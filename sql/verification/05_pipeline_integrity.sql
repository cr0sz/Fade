-- 05_pipeline_integrity.sql
-- Operational question: Which cycle_ids completed the full pipeline lifecycle?
--
-- A complete lifecycle means the same cycle_id appears in:
--   1. provider_ingestion_log  (ingestion happened)
--   2. signal_trace            (signals were generated)
--   3. signal_feedback         (at least one evaluation completed)
--
-- Expected: recent cycles with lifecycle_stage = 'complete'.
-- Note: feedback takes time — cycles in 'signals_only' are normal if < 1 hour old.
--
-- Investigate if:
--   • No cycles ever reach 'complete' (evaluation engine broken)
--   • Cycles stay 'ingestion_only' for > 1 hour (signal tracing broken)
--   • No cycles at all (scheduler not running)

WITH ingestion_cycles AS (
    SELECT
        cycle_id,
        MAX(ts)      AS ingested_at,
        SUM(fetched) AS total_fetched,
        MAX(job)     AS job
    FROM provider_ingestion_log
    WHERE ts > now() - interval '48 hours'
    GROUP BY cycle_id
),
signal_cycles AS (
    SELECT
        cycle_id,
        COUNT(*)     AS signal_count,
        MAX(created_at) AS last_signal_at
    FROM signal_trace
    WHERE created_at > now() - interval '48 hours'
    GROUP BY cycle_id
),
feedback_cycles AS (
    SELECT
        cycle_id,
        COUNT(*) FILTER (WHERE outcome_type != 'pending') AS completed_outcomes
    FROM signal_feedback
    WHERE evaluated_at > now() - interval '72 hours'
    GROUP BY cycle_id
)
SELECT
    i.cycle_id,
    i.job,
    i.ingested_at,
    i.total_fetched,
    s.signal_count,
    f.completed_outcomes,
    CASE
        WHEN s.cycle_id IS NOT NULL AND COALESCE(f.completed_outcomes, 0) > 0
            THEN 'complete'
        WHEN s.cycle_id IS NOT NULL
            THEN 'signals_only'
        ELSE
            'ingestion_only'
    END                                        AS lifecycle_stage,
    -- Age of the cycle in minutes
    ROUND((EXTRACT(EPOCH FROM (now() - i.ingested_at)) / 60)::numeric, 0)
                                               AS age_minutes
FROM ingestion_cycles i
LEFT JOIN signal_cycles  s USING (cycle_id)
LEFT JOIN feedback_cycles f USING (cycle_id)
ORDER BY i.ingested_at DESC
LIMIT 50;
