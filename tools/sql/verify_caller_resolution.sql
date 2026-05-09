-- =============================================================================
-- verify_caller_resolution.sql
-- =============================================================================
-- Post-deploy verification for the track_api_call caller auto-resolution fix.
--
-- Background
-- ----------
-- Before this fix, 54 of 101 track_api_call() sites in app/ omitted the
-- caller= kwarg, so they all collapsed into the "unknown" bucket inside
-- api_usage_hourly.callers (JSONB) and the realtime top_callers list.
-- That made the [api_hotspots_24h] worker diagnostic useless for finding
-- hot collectors.
--
-- After deploy, _resolve_caller() walks the stack and records the calling
-- module's __name__ (with the "app." prefix stripped). New rows should show
-- values like "data_layer.peg_monitor", "collectors.coingecko", etc.
--
-- Latency notes
-- -------------
-- - The in-memory buffer flushes every ~5 s (background flusher) or 50 entries.
-- - The hourly rollup is updated on every flush, so api_usage_hourly should
--   reflect new rows within ~1 minute of deploy.
-- - The "unknown" bucket will not vanish instantly: hourly rows already
--   written before deploy are not rewritten. Expect the bucket share to
--   trend toward 0 over the hour following deploy and stay near 0 going
--   forward (modulo a small residual from third-party / external code paths
--   where __name__ truly cannot be resolved).
--
-- "Fix is working" looks like:
--   * Query 1: unknown_pct in the most recent hour is < ~5 % (vs ~50 % before).
--   * Query 2: top callers in the last 24 h include real module paths
--     (data_layer.*, collectors.*, services.*) — not just "unknown".
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Query 1: per-hour size of the "unknown" caller bucket vs total calls
-- across the last 24 h. Expect the unknown share to fall sharply at the
-- deploy boundary and stay near zero afterwards.
-- -----------------------------------------------------------------------------
SELECT
    h.hour,
    h.provider,
    h.total_calls,
    COALESCE((h.callers ->> 'unknown')::bigint, 0)               AS unknown_calls,
    h.total_calls - COALESCE((h.callers ->> 'unknown')::bigint, 0) AS resolved_calls,
    ROUND(
        100.0
        * COALESCE((h.callers ->> 'unknown')::bigint, 0)
        / NULLIF(h.total_calls, 0),
        2
    ) AS unknown_pct
FROM api_usage_hourly AS h
WHERE h.hour >= NOW() - INTERVAL '24 hours'
ORDER BY h.hour DESC, h.total_calls DESC;


-- -----------------------------------------------------------------------------
-- Query 2: top 20 distinct caller keys across the last 24 h, with totals.
-- After deploy, the table should be dominated by resolved module paths
-- (e.g. "data_layer.peg_monitor", "collectors.coingecko") rather than the
-- single "unknown" row that used to swallow everything.
-- -----------------------------------------------------------------------------
SELECT
    kv.key                          AS caller,
    SUM(kv.value::bigint)           AS total_calls,
    COUNT(DISTINCT h.provider)      AS providers_seen,
    MIN(h.hour)                     AS first_seen,
    MAX(h.hour)                     AS last_seen
FROM api_usage_hourly AS h
CROSS JOIN LATERAL jsonb_each_text(h.callers) AS kv
WHERE h.hour >= NOW() - INTERVAL '24 hours'
GROUP BY kv.key
ORDER BY total_calls DESC
LIMIT 20;
