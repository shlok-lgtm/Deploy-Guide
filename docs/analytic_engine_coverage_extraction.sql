-- ============================================================================
-- Basis Analytic Engine — Coverage Fixture Extraction
-- Referenced from: docs/analytic_engine_step_0_v0.2.md §4
--
-- Run this block SIX times, once per entity below, changing only the :entity
-- value at the top. Paste full JSON output back to the thread; fixtures get
-- formatted into tests/fixtures/canonical_coverage.py.
--
-- Entities to run:
--   1. drift
--   2. kelp-rseth
--   3. usdc
--   4. jupiter-perpetual-exchange
--   5. layerzero
--   6. this-entity-does-not-exist-xyz
--
-- Execution (psql):
--   \set entity 'drift'
--   \i docs/analytic_engine_coverage_extraction.sql
--
-- Or replace :'entity' inline if running from another client. The block is
-- read-only: six SELECTs, no writes.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1 / 5  —  generic_index_scores coverage
--
-- Returns one row per (index_id, entity_slug) the entity appears in.
-- `live` = most recent computed_at within the last 48h.
-- `density` derived from unique_days / window span.
-- ----------------------------------------------------------------------------

SELECT
    'generic_index_scores'                                            AS data_source,
    index_id,
    entity_slug,
    MIN(entity_name)                                                  AS entity_name,
    MIN(scored_date)                                                  AS earliest_record,
    MAX(scored_date)                                                  AS latest_record,
    COUNT(DISTINCT scored_date)                                       AS unique_days,
    MAX(computed_at)                                                  AS last_computed_at,
    (MAX(computed_at) >= NOW() - INTERVAL '48 hours')                 AS live,
    CASE
        WHEN COUNT(DISTINCT scored_date) = 1                          THEN 'single'
        WHEN COUNT(DISTINCT scored_date) <
             GREATEST(1, (MAX(scored_date) - MIN(scored_date))::int / 7)
                                                                      THEN 'sparse'
        WHEN COUNT(DISTINCT scored_date) * 7 <
             GREATEST(7, (MAX(scored_date) - MIN(scored_date))::int)  THEN 'weekly'
        WHEN COUNT(DISTINCT scored_date) >=
             GREATEST(1, (MAX(scored_date) - MIN(scored_date))::int)  THEN 'daily'
        ELSE 'multiple_daily'
    END                                                               AS density
FROM generic_index_scores
WHERE entity_slug = :'entity'
GROUP BY index_id, entity_slug
ORDER BY index_id;

-- ----------------------------------------------------------------------------
-- Query 2 / 5  —  historical_protocol_data coverage
--
-- Backfilled TVL / fees / revenue / mcap. Always `coverage_type=backfilled`.
-- Density computed over record_date span.
-- ----------------------------------------------------------------------------

SELECT
    'historical_protocol_data'                                        AS data_source,
    'psi_backfill'                                                    AS index_id,  -- logical label; feeds PSI temporal reconstruction
    protocol_slug                                                     AS entity_slug,
    MIN(record_date)                                                  AS earliest_record,
    MAX(record_date)                                                  AS latest_record,
    COUNT(DISTINCT record_date)                                       AS unique_days,
    MAX(created_at)                                                   AS last_ingested_at,
    FALSE                                                             AS live,  -- backfill is never "live"
    CASE
        WHEN COUNT(DISTINCT record_date) = 1                          THEN 'single'
        WHEN COUNT(DISTINCT record_date) * 7 <
             GREATEST(7, (MAX(record_date) - MIN(record_date))::int)  THEN 'sparse'
        WHEN COUNT(DISTINCT record_date) >=
             GREATEST(1, (MAX(record_date) - MIN(record_date))::int)  THEN 'daily'
        ELSE 'weekly'
    END                                                               AS density
FROM historical_protocol_data
WHERE protocol_slug = :'entity'
GROUP BY protocol_slug;

-- ----------------------------------------------------------------------------
-- Query 3 / 5  —  SII coverage via scores + score_history
--
-- If entity matches a stablecoin_id, it has SII coverage. score_history
-- gives density; scores gives live state.
-- ----------------------------------------------------------------------------

WITH sii_latest AS (
    SELECT
        stablecoin_id,
        computed_at,
        (computed_at >= NOW() - INTERVAL '48 hours') AS live
    FROM scores
    WHERE stablecoin_id = :'entity'
),
sii_history AS (
    SELECT
        stablecoin,
        MIN(score_date)  AS earliest_record,
        MAX(score_date)  AS latest_record,
        COUNT(DISTINCT score_date) AS unique_days
    FROM score_history
    WHERE stablecoin = :'entity'
    GROUP BY stablecoin
)
SELECT
    'scores+score_history'                                            AS data_source,
    'sii'                                                             AS index_id,
    COALESCE(sl.stablecoin_id, sh.stablecoin)                         AS entity_slug,
    sh.earliest_record,
    sh.latest_record,
    COALESCE(sh.unique_days, 0)                                       AS unique_days,
    sl.computed_at                                                    AS last_computed_at,
    COALESCE(sl.live, FALSE)                                          AS live,
    CASE
        WHEN sh.unique_days IS NULL OR sh.unique_days = 0              THEN 'single'
        WHEN sh.unique_days * 7 <
             GREATEST(7, (sh.latest_record - sh.earliest_record)::int) THEN 'sparse'
        WHEN sh.unique_days >=
             GREATEST(1, (sh.latest_record - sh.earliest_record)::int) THEN 'daily'
        ELSE 'weekly'
    END                                                               AS density
FROM sii_latest sl
FULL OUTER JOIN sii_history sh ON sl.stablecoin_id = sh.stablecoin;

-- ----------------------------------------------------------------------------
-- Query 4 / 5  —  Fuzzy match candidates (pg_trgm)
--
-- Finds entity_slug candidates across all three coverage tables that match
-- the input by trigram similarity. Threshold 0.3; Component 1's real
-- matcher will use 0.4 for production, but 0.3 here surfaces more candidates
-- for operator review of false-positive risk.
--
-- Requires: CREATE EXTENSION IF NOT EXISTS pg_trgm;  (should already be on)
-- ----------------------------------------------------------------------------

WITH candidates AS (
    SELECT DISTINCT entity_slug AS slug, 'generic_index_scores' AS source
    FROM generic_index_scores
    UNION ALL
    SELECT DISTINCT protocol_slug AS slug, 'historical_protocol_data' AS source
    FROM historical_protocol_data
    UNION ALL
    SELECT DISTINCT stablecoin_id AS slug, 'scores' AS source
    FROM scores
)
SELECT
    slug,
    source,
    similarity(slug, :'entity')                                       AS sim_score,
    (slug = :'entity')                                                AS exact_match
FROM candidates
WHERE similarity(slug, :'entity') > 0.3
   OR slug = :'entity'
ORDER BY exact_match DESC, sim_score DESC
LIMIT 25;

-- ----------------------------------------------------------------------------
-- Query 5 / 5  —  Adjacent-index negative space
--
-- ⚠️  KNOWN BUG — DO NOT USE FOR FIXTURE EXTRACTION
--
-- Symptom: returns covers_entity=false for indexes that DO cover the entity.
-- Confirmed against production on drift and jupiter-perpetual-exchange during
-- v0.2a fixture run (2026-04-24). Both entities have PSI coverage via
-- historical_protocol_data (temporal reconstruction) and dex_pool_data via
-- generic_index_scores; Q5 returned both as not-covering.
--
-- Root cause suspected in the `covering` CTE — mix of UNION semantics, a
-- stray `LIMIT 1` inside the psi_scores branch that clips multi-row matches,
-- and inconsistent treatment of historical_protocol_data (which surfaces as
-- a "psi" index logically but lives in a separate table). Specific fix not
-- attempted because Component 1 (P1 session) rewrites this logic from scratch
-- in Python against a single authoritative index registry. No production
-- consumer depends on this query — it exists only in this extraction file.
--
-- Action: fixture extraction ignores Query 5 output. The
-- adjacent_indexes_not_covering field in tests/fixtures/canonical_coverage.py
-- is populated manually from Q1+Q2+Q3 results per entity. P1's implementation
-- derives it correctly from the authoritative index list.
--
-- Do not debug this query. It is scheduled for deletion when P1 lands.
-- ----------------------------------------------------------------------------

WITH all_indexes AS (
    SELECT DISTINCT index_id FROM generic_index_scores
    UNION
    SELECT 'sii' AS index_id
    UNION
    SELECT 'psi' AS index_id  -- present only as backfill if no live psi table hits; shown for completeness
),
covering AS (
    SELECT DISTINCT index_id
    FROM generic_index_scores
    WHERE entity_slug = :'entity'

    UNION

    SELECT 'sii'
    FROM scores
    WHERE stablecoin_id = :'entity'

    UNION

    SELECT 'psi'
    FROM psi_scores
    WHERE protocol_slug = :'entity'
    LIMIT 1
)
SELECT
    a.index_id,
    (c.index_id IS NOT NULL) AS covers_entity
FROM all_indexes a
LEFT JOIN covering c ON a.index_id = c.index_id
ORDER BY covers_entity DESC, a.index_id;

-- ============================================================================
-- End of block. Rerun with next entity value.
-- ============================================================================
