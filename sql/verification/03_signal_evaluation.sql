-- 03_signal_evaluation.sql
-- Operational question: Are signals being evaluated and what do the outcomes look like?
--
-- Expected output: one row per (engine_name, signal_type, evaluation_window_h).
-- A healthy system shows non-zero completed evaluations for signals older
-- than the evaluation window (1h / 4h / 24h from signal creation).
--
-- Investigate if:
--   • pending_count is large and not decreasing (evaluation engine stalled)
--   • win_rate is near 0% (systematic mismatch between signal direction and market)
--   • A signal type has 0 rows (evaluation engine skipping it)

SELECT
    engine_name,
    signal_type,
    evaluation_window_h                                      AS window_h,
    COUNT(*)                                                 AS total,
    COUNT(*) FILTER (WHERE outcome_type = 'win')             AS wins,
    COUNT(*) FILTER (WHERE outcome_type = 'loss')            AS losses,
    COUNT(*) FILTER (WHERE outcome_type = 'neutral')         AS neutrals,
    COUNT(*) FILTER (WHERE outcome_type = 'pending')         AS pending,
    -- Win rate: wins / (wins + losses), excludes neutral and pending
    CASE
        WHEN COUNT(*) FILTER (WHERE outcome_type IN ('win','loss')) > 0
        THEN ROUND(
            100.0 * COUNT(*) FILTER (WHERE outcome_type = 'win')
                  / COUNT(*) FILTER (WHERE outcome_type IN ('win','loss')),
            1)
        ELSE NULL
    END                                                      AS win_rate_pct,
    ROUND(AVG(pnl_score) FILTER (WHERE outcome_type != 'pending')::numeric, 4)
                                                             AS avg_pnl,
    MIN(evaluated_at)                                        AS first_evaluation,
    MAX(evaluated_at)                                        AS last_evaluation
FROM signal_feedback
GROUP BY engine_name, signal_type, evaluation_window_h
ORDER BY engine_name, signal_type, evaluation_window_h;
