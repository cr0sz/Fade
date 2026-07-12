-- 02_signal_generation.sql
-- Operational question: Which engines are generating signals and at what rate?
--
-- Expected output: one row per (engine_name, signal_type) with count and
-- recency.  A healthy system shows each active engine with signal_count > 0
-- in the last 24 hours.
--
-- Investigate if:
--   • An engine that was active yesterday shows 0 signals today
--   • avg_strength is dramatically lower than usual (data quality degradation)
--   • last_signal is more than 2 × the engine's scheduled interval in the past

SELECT
    engine_name,
    signal_type,
    COUNT(*)                                AS signal_count,
    COUNT(DISTINCT cycle_id)                AS cycle_count,
    MAX(created_at)                         AS last_signal,
    ROUND((EXTRACT(EPOCH FROM (now() - MAX(created_at))) / 60)::numeric, 1)
                                            AS minutes_since_last,
    ROUND(AVG(signal_strength)::numeric, 2) AS avg_strength,
    ROUND(MIN(signal_strength)::numeric, 2) AS min_strength,
    ROUND(MAX(signal_strength)::numeric, 2) AS max_strength,
    COUNT(DISTINCT provider) FILTER (WHERE provider IS NOT NULL)
                                            AS distinct_providers,
    -- Direction breakdown
    COUNT(*) FILTER (WHERE signal_direction IN ('bullish','risk_on','inflow'))
                                            AS positive_signals,
    COUNT(*) FILTER (WHERE signal_direction IN ('bearish','risk_off','outflow'))
                                            AS negative_signals
FROM signal_trace
WHERE created_at > now() - interval '24 hours'
GROUP BY engine_name, signal_type
ORDER BY engine_name, signal_count DESC;
