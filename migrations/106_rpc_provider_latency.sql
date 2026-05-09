-- Migration 106 — Per-provider RPC latency observability.
--
-- The router at app/utils/rpc_provider.py records hourly *counts* of calls
-- per (provider, method, chain, status) in rpc_provider_usage, but no
-- latency. We can't currently answer "is Dwellir actually faster than
-- Alchemy?" — needed to validate the router is helping. This migration
-- adds rpc_provider_latency, a sibling table to rpc_provider_usage that
-- records latency observations per call.
--
-- rpc_provider_usage is intentionally NOT modified — it has downstream
-- consumers (app/worker.py:787 7d-usage report among others) and changing
-- its schema would break them.
--
-- CARDINALITY CHOICE: per-minute aggregation with a bounded reservoir of
-- raw samples.
--   * One row per (provider, method, chain, status, minute).
--   * Each row carries running aggregates: calls, sum_ms, min_ms, max_ms.
--   * `samples_ms` is a bounded INT[] capped at 100 per row by the
--     application writer — enough for accurate p50/p95 within a minute
--     while keeping row size bounded.
--   * Row count grows with active minutes × distinct (provider, method,
--     chain, status) tuples, NOT with raw traffic. At our current call
--     mix (~5 methods × 2 providers × 3 chains × 3 statuses × 1440 min
--     per day) the upper bound is ~130K rows/day, but in practice most
--     tuple-minutes are empty so it's far lower.
--
-- ALTERNATIVE CONSIDERED: per-call rows with 10% sampling — rejected
-- because it scales linearly with traffic and the router already adds
-- one DB write per call (rpc_provider_usage); doubling that with another
-- per-call insert (even sampled) felt worse than aggregating in place.
--
-- READING THE TABLE:
--   * For approximate p95 over a window: unnest(samples_ms) and use
--     percentile_cont(0.95). See the rpc_provider_latency_p95_24h view.
--   * For mean: sum(sum_ms) / sum(calls).
--   * For min/max: min(min_ms), max(max_ms).

CREATE TABLE IF NOT EXISTS rpc_provider_latency (
    id BIGSERIAL PRIMARY KEY,
    provider TEXT NOT NULL,           -- 'alchemy' | 'dwellir'
    method TEXT NOT NULL,             -- e.g. 'eth_call', 'eth_getLogs'
    chain TEXT NOT NULL,              -- 'ethereum' | 'base' | 'arbitrum'
    status TEXT NOT NULL,             -- 'ok' | 'error'  (success or failure)
    observed_at TIMESTAMPTZ NOT NULL, -- truncated to the minute
    calls INT NOT NULL DEFAULT 1,
    sum_ms BIGINT NOT NULL DEFAULT 0,
    min_ms INT NOT NULL,
    max_ms INT NOT NULL,
    samples_ms INT[] NOT NULL DEFAULT '{}'::INT[],
    UNIQUE (provider, method, chain, status, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_rpc_provider_latency_observed
    ON rpc_provider_latency (observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_rpc_provider_latency_provider
    ON rpc_provider_latency (provider, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_rpc_provider_latency_method
    ON rpc_provider_latency (method, observed_at DESC);

-- Convenience view: p50/p95/mean per (provider, method, chain) over the
-- last 24 hours of successful calls. Use this to compare Alchemy vs
-- Dwellir at a glance:
--   SELECT * FROM rpc_provider_latency_p95_24h
--    WHERE method = 'eth_call' ORDER BY p95_ms;
CREATE OR REPLACE VIEW rpc_provider_latency_p95_24h AS
SELECT
    provider,
    method,
    chain,
    SUM(calls)::BIGINT                                        AS calls,
    (SUM(sum_ms)::FLOAT / NULLIF(SUM(calls), 0))::INT          AS mean_ms,
    MIN(min_ms)                                                AS min_ms,
    MAX(max_ms)                                                AS max_ms,
    percentile_cont(0.50) WITHIN GROUP (
        ORDER BY s.sample
    )::INT                                                     AS p50_ms,
    percentile_cont(0.95) WITHIN GROUP (
        ORDER BY s.sample
    )::INT                                                     AS p95_ms
FROM rpc_provider_latency l,
     LATERAL unnest(l.samples_ms) AS s(sample)
WHERE status = 'ok'
  AND observed_at >= NOW() - INTERVAL '24 hours'
GROUP BY provider, method, chain;

INSERT INTO migrations (name) VALUES ('106_rpc_provider_latency') ON CONFLICT DO NOTHING;
