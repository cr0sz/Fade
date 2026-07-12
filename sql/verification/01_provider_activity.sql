-- 01_provider_activity.sql
-- Operational question: Did providers ingest data in the last 2 hours?
--
-- Expected output: one row per provider that has been active recently.
-- A healthy system shows every configured provider with status = 'ok' and
-- fetched > 0 within the lookback window.
--
-- Investigate if:
--   • Any provider is absent (circuit breaker open or API key missing)
--   • errors > 0 consistently for a single provider
--   • avg_latency_ms is growing (upstream API degradation)

SELECT
    provider,
    COUNT(*)                                    AS cycles,
    MAX(ts)                                     AS last_seen,
    ROUND((EXTRACT(EPOCH FROM (now() - MAX(ts))) / 60)::numeric, 1)
                                                AS minutes_since_last,
    SUM(fetched)                                AS total_fetched,
    SUM(new_count)                              AS total_new,
    SUM(dup_exact + dup_fuzzy + dup_semantic)   AS total_dupes,
    SUM(errors)                                 AS total_errors,
    ROUND(AVG(latency_ms)::numeric, 0)          AS avg_latency_ms,
    -- Noise ratio: what fraction of fetched items were duplicates?
    CASE WHEN SUM(fetched) > 0
         THEN ROUND(
                  100.0 * SUM(dup_exact + dup_fuzzy + dup_semantic)
                        / SUM(fetched), 1)
         ELSE NULL
    END                                         AS noise_pct,
    MAX(status)                                 AS last_status
FROM provider_ingestion_log
WHERE ts > now() - interval '2 hours'
GROUP BY provider
ORDER BY last_seen DESC;
