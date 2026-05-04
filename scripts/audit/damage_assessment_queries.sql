-- Damage Assessment Queries: April 26 + May 4 Production Freezes
-- Run against production Neon DB. Copy-paste each query block individually.
-- Output is TSV-friendly (no formatting directives).
--
-- Freeze windows (best estimates from incident docs + git log):
--   Window A: 2026-04-24 ~16:00 UTC → 2026-04-27 ~13:00 UTC (69h, api_usage_tracker flush)
--   Window B: 2026-05-03 ~00:00 UTC → 2026-05-03 ~08:55 UTC (9h, PR-A1 NameError)
--   Window C: 2026-05-03 09:10 UTC → 2026-05-04 ~08:55 UTC (23h, holder_discovery wedge)

-- ============================================================================
-- TIER 1: ON-CHAIN INTEGRITY
-- ============================================================================

-- 1a. All keeper cycles during freeze windows
SELECT id, started_at, completed_at, duration_ms,
       sii_updates_base, sii_updates_arb, psi_updates,
       state_root_published, trigger_reason,
       array_to_string(errors, '; ') AS errors_text
FROM ops.keeper_cycles
WHERE started_at BETWEEN '2026-04-24 16:00' AND '2026-04-27 13:00'
   OR started_at BETWEEN '2026-05-03 00:00' AND '2026-05-04 09:00'
ORDER BY started_at;

-- 1b. State attestations during freeze windows (hash → fact linkage)
SELECT id, domain, entity_id, batch_hash, record_count,
       methodology_version, cycle_timestamp
FROM state_attestations
WHERE cycle_timestamp BETWEEN '2026-04-24 16:00' AND '2026-04-27 13:00'
   OR cycle_timestamp BETWEEN '2026-05-03 00:00' AND '2026-05-04 09:00'
ORDER BY cycle_timestamp;

-- 1c. Score staleness at each keeper cycle
--     For each keeper cycle, find the most recent score it could have used
SELECT kc.id AS keeper_cycle_id,
       kc.started_at AS publish_time,
       s.stablecoin_id,
       s.computed_at AS score_computed_at,
       EXTRACT(EPOCH FROM (kc.started_at - s.computed_at)) / 3600 AS staleness_hours
FROM ops.keeper_cycles kc
CROSS JOIN LATERAL (
    SELECT stablecoin_id, computed_at
    FROM scores
    WHERE computed_at <= kc.started_at
    ORDER BY computed_at DESC
    LIMIT 1
) s
WHERE kc.started_at BETWEEN '2026-04-24 16:00' AND '2026-04-27 13:00'
   OR kc.started_at BETWEEN '2026-05-03 00:00' AND '2026-05-04 09:00'
ORDER BY kc.started_at;

-- 1d. Expected vs actual keeper cadence (normal = every 60min)
SELECT date_trunc('hour', started_at) AS hour_bucket,
       COUNT(*) AS cycles_in_hour
FROM ops.keeper_cycles
WHERE started_at BETWEEN '2026-04-23' AND '2026-05-05'
GROUP BY hour_bucket
ORDER BY hour_bucket;


-- ============================================================================
-- TIER 2: SCORE ACCURACY
-- ============================================================================

-- 2a. Score gaps per stablecoin during freeze windows
SELECT st.symbol, st.id AS stablecoin_id,
       MAX(s.computed_at) FILTER (WHERE s.computed_at < '2026-04-24 16:00') AS last_pre_freeze_a,
       MIN(s.computed_at) FILTER (WHERE s.computed_at > '2026-04-27 13:00') AS first_post_freeze_a,
       MAX(s.computed_at) FILTER (WHERE s.computed_at < '2026-05-03 00:00') AS last_pre_freeze_bc,
       MIN(s.computed_at) FILTER (WHERE s.computed_at > '2026-05-04 09:00') AS first_post_freeze_bc
FROM stablecoins st
LEFT JOIN scores s ON s.stablecoin_id = st.id
WHERE st.scoring_enabled = TRUE
GROUP BY st.symbol, st.id
ORDER BY st.symbol;

-- 2b. Score deltas across freeze window A (Apr 24-27)
WITH pre AS (
    SELECT DISTINCT ON (stablecoin_id)
           stablecoin_id, overall_score, peg_score, liquidity_score,
           mint_burn_score, distribution_score, structural_score, computed_at
    FROM scores
    WHERE computed_at < '2026-04-24 16:00'
    ORDER BY stablecoin_id, computed_at DESC
),
post AS (
    SELECT DISTINCT ON (stablecoin_id)
           stablecoin_id, overall_score, peg_score, liquidity_score,
           mint_burn_score, distribution_score, structural_score, computed_at
    FROM scores
    WHERE computed_at > '2026-04-27 13:00'
    ORDER BY stablecoin_id, computed_at ASC
)
SELECT st.symbol,
       pre.overall_score AS pre_overall, post.overall_score AS post_overall,
       ROUND((post.overall_score - pre.overall_score)::numeric, 2) AS delta_overall,
       ROUND(ABS(post.overall_score - pre.overall_score) / NULLIF(pre.overall_score, 0) * 100, 1) AS delta_pct,
       pre.computed_at AS pre_time, post.computed_at AS post_time,
       EXTRACT(EPOCH FROM (post.computed_at - pre.computed_at)) / 3600 AS gap_hours
FROM pre
JOIN post USING (stablecoin_id)
JOIN stablecoins st ON st.id = pre.stablecoin_id
ORDER BY delta_pct DESC NULLS LAST;

-- 2c. Score deltas across freeze windows B+C (May 3-4)
WITH pre AS (
    SELECT DISTINCT ON (stablecoin_id)
           stablecoin_id, overall_score, peg_score, liquidity_score,
           mint_burn_score, distribution_score, structural_score, computed_at
    FROM scores
    WHERE computed_at < '2026-05-03 00:00'
    ORDER BY stablecoin_id, computed_at DESC
),
post AS (
    SELECT DISTINCT ON (stablecoin_id)
           stablecoin_id, overall_score, peg_score, liquidity_score,
           mint_burn_score, distribution_score, structural_score, computed_at
    FROM scores
    WHERE computed_at > '2026-05-04 09:00'
    ORDER BY stablecoin_id, computed_at ASC
)
SELECT st.symbol,
       pre.overall_score AS pre_overall, post.overall_score AS post_overall,
       ROUND((post.overall_score - pre.overall_score)::numeric, 2) AS delta_overall,
       ROUND(ABS(post.overall_score - pre.overall_score) / NULLIF(pre.overall_score, 0) * 100, 1) AS delta_pct,
       pre.computed_at AS pre_time, post.computed_at AS post_time,
       EXTRACT(EPOCH FROM (post.computed_at - pre.computed_at)) / 3600 AS gap_hours
FROM pre
JOIN post USING (stablecoin_id)
JOIN stablecoins st ON st.id = pre.stablecoin_id
ORDER BY delta_pct DESC NULLS LAST;

-- 2e. Score deltas across the keeper hang (May 1-3, Window B)
WITH pre AS (
    SELECT DISTINCT ON (stablecoin_id)
           stablecoin_id, overall_score, peg_score, liquidity_score,
           mint_burn_score, distribution_score, structural_score, computed_at
    FROM scores
    WHERE computed_at < '2026-05-01 19:47'
    ORDER BY stablecoin_id, computed_at DESC
),
post AS (
    SELECT DISTINCT ON (stablecoin_id)
           stablecoin_id, overall_score, peg_score, liquidity_score,
           mint_burn_score, distribution_score, structural_score, computed_at
    FROM scores
    WHERE computed_at > '2026-05-03 10:00'
    ORDER BY stablecoin_id, computed_at ASC
)
SELECT st.symbol,
       pre.overall_score AS pre_overall, post.overall_score AS post_overall,
       ROUND((post.overall_score - pre.overall_score)::numeric, 2) AS delta_overall,
       ROUND((ABS(post.overall_score - pre.overall_score) / NULLIF(pre.overall_score, 0) * 100)::numeric, 1) AS delta_pct,
       pre.computed_at AS pre_time, post.computed_at AS post_time,
       ROUND((EXTRACT(EPOCH FROM (post.computed_at - pre.computed_at)) / 3600)::numeric, 2) AS gap_hours
FROM pre
JOIN post USING (stablecoin_id)
JOIN stablecoins st ON st.id = pre.stablecoin_id
WHERE st.id != 'busd0'
ORDER BY delta_pct DESC NULLS LAST;

-- 2d. Backdated score rows (created_at significantly after computed_at)
SELECT stablecoin_id, computed_at, created_at,
       EXTRACT(EPOCH FROM (created_at - computed_at)) / 60 AS lag_minutes
FROM scores
WHERE computed_at BETWEEN '2026-04-24' AND '2026-05-05'
  AND created_at - computed_at > INTERVAL '5 minutes'
ORDER BY lag_minutes DESC
LIMIT 50;


-- ============================================================================
-- TIER 3: INDEXER COVERAGE
-- ============================================================================

-- 3a. Wallet indexer activity during freeze windows
SELECT date_trunc('hour', last_indexed_at) AS hour_bucket,
       COUNT(*) AS wallets_indexed
FROM wallet_graph.wallets
WHERE last_indexed_at BETWEEN '2026-04-24' AND '2026-05-05'
GROUP BY hour_bucket
ORDER BY hour_bucket;

-- 3b. Edge builder activity during freeze windows
SELECT date_trunc('hour', last_built_at) AS hour_bucket,
       chain,
       COUNT(*) AS edges_built
FROM wallet_graph.edge_build_status
WHERE last_built_at BETWEEN '2026-04-24' AND '2026-05-05'
GROUP BY hour_bucket, chain
ORDER BY hour_bucket;

-- 3c. Component readings freshness gap
SELECT category,
       MAX(collected_at) FILTER (WHERE collected_at < '2026-04-24 16:00') AS last_pre_freeze,
       MIN(collected_at) FILTER (WHERE collected_at > '2026-04-27 13:00') AS first_post_freeze,
       EXTRACT(EPOCH FROM (
           MIN(collected_at) FILTER (WHERE collected_at > '2026-04-27 13:00') -
           MAX(collected_at) FILTER (WHERE collected_at < '2026-04-24 16:00')
       )) / 3600 AS gap_hours
FROM component_readings
GROUP BY category
ORDER BY gap_hours DESC NULLS LAST;


-- ============================================================================
-- TIER 4: EXTERNAL INTEGRATION IMPACT
-- ============================================================================

-- 4a. API 5xx error count by hour during incident windows
SELECT date_trunc('hour', timestamp) AS hour_bucket,
       COUNT(*) AS total_requests,
       COUNT(*) FILTER (WHERE status_code >= 500) AS error_5xx,
       ROUND(COUNT(*) FILTER (WHERE status_code >= 500)::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS error_pct
FROM api_request_log
WHERE timestamp BETWEEN '2026-04-24' AND '2026-05-05'
GROUP BY hour_bucket
HAVING COUNT(*) FILTER (WHERE status_code >= 500) > 0
ORDER BY hour_bucket;

-- 4b. Unique affected IPs (external users who got 5xx)
SELECT COUNT(DISTINCT ip_address) AS unique_ips_affected,
       COUNT(*) AS total_5xx
FROM api_request_log
WHERE timestamp BETWEEN '2026-04-24' AND '2026-05-05'
  AND status_code >= 500
  AND is_internal = FALSE;

-- 4c. Most-affected endpoints
SELECT endpoint, COUNT(*) AS error_count
FROM api_request_log
WHERE timestamp BETWEEN '2026-04-24' AND '2026-05-05'
  AND status_code >= 500
GROUP BY endpoint
ORDER BY error_count DESC
LIMIT 20;


-- ============================================================================
-- TIER 5: PROVENANCE INTEGRITY
-- ============================================================================

-- 5a. Rows written during freeze with NULL provenance_proof_id
--     Check the 4 most important provenance-tracked tables
SELECT 'scores' AS table_name, COUNT(*) AS null_provenance_rows
FROM scores
WHERE computed_at BETWEEN '2026-04-24 16:00' AND '2026-04-27 13:00'
  AND provenance_proof_id IS NULL
UNION ALL
SELECT 'scores' AS table_name, COUNT(*) AS null_provenance_rows
FROM scores
WHERE computed_at BETWEEN '2026-05-03 00:00' AND '2026-05-04 09:00'
  AND provenance_proof_id IS NULL
UNION ALL
SELECT 'psi_scores', COUNT(*)
FROM psi_scores
WHERE computed_at BETWEEN '2026-04-24 16:00' AND '2026-05-04 09:00'
  AND provenance_proof_id IS NULL
UNION ALL
SELECT 'component_readings', COUNT(*)
FROM component_readings
WHERE collected_at BETWEEN '2026-04-24 16:00' AND '2026-05-04 09:00'
  AND provenance_proof_id IS NULL;

-- 5b. Orphan provenance proofs (proof with no corresponding scored row)
SELECT pp.id, pp.domain, pp.created_at, pp.input_hash
FROM provenance_proofs pp
WHERE pp.created_at BETWEEN '2026-04-24' AND '2026-05-05'
  AND NOT EXISTS (
      SELECT 1 FROM state_attestations sa
      WHERE sa.batch_hash = pp.input_hash
        AND sa.cycle_timestamp BETWEEN pp.created_at - INTERVAL '5 minutes'
                                   AND pp.created_at + INTERVAL '5 minutes'
  )
ORDER BY pp.created_at
LIMIT 50;

-- 5c. Proof/fact timestamp disagreement (>30s gap)
SELECT sa.id AS attestation_id, sa.domain, sa.cycle_timestamp,
       pp.id AS proof_id, pp.created_at AS proof_time,
       EXTRACT(EPOCH FROM (pp.created_at - sa.cycle_timestamp)) AS delta_seconds
FROM state_attestations sa
JOIN provenance_proofs pp ON pp.input_hash = sa.batch_hash
WHERE sa.cycle_timestamp BETWEEN '2026-04-24' AND '2026-05-05'
  AND ABS(EXTRACT(EPOCH FROM (pp.created_at - sa.cycle_timestamp))) > 30
ORDER BY ABS(EXTRACT(EPOCH FROM (pp.created_at - sa.cycle_timestamp))) DESC
LIMIT 50;
