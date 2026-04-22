-- Migration 090 — Dwellir RPC integration: usage tracking + capability probe.
--
-- Adds two tables supporting the Alchemy ↔ Dwellir RPC router shipped in
-- app/utils/rpc_provider.py. This is infrastructure only — no routing or
-- LLL pipeline changes are sanctioned by this migration alone. See the
-- PR description for scope.
--
-- rpc_provider_usage — hourly-bucketed counter of calls per (provider,
-- method, chain, status). `status` ∈ {'ok', 'fallback', 'error'}. When a
-- primary-provider call fails and the router falls back to the alternate,
-- the failing call is recorded as status='fallback' with
-- fallback_reason, and the successful retry as status='ok' on the
-- alternate. A permanent failure (both providers errored) is logged as
-- status='error' on the original primary.
--
-- rpc_capabilities — persistent log of what each provider supports on the
-- free tier. Populated at worker startup by probe_rpc_capabilities(). One
-- row per (provider, chain, method) per probe run; query the latest row
-- per (provider, chain, method) for the current picture.

CREATE TABLE IF NOT EXISTS rpc_provider_usage (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL,           -- 'alchemy' | 'dwellir'
    method TEXT NOT NULL,             -- e.g. 'eth_call', 'debug_traceTransaction'
    chain TEXT NOT NULL,              -- 'ethereum' | 'base' | 'arbitrum'
    status TEXT NOT NULL,             -- 'ok' | 'fallback' | 'error'
    fallback_reason TEXT,
    hour TIMESTAMPTZ NOT NULL DEFAULT date_trunc('hour', NOW()),
    calls INT NOT NULL DEFAULT 1,
    UNIQUE (provider, method, chain, status, hour)
);

CREATE INDEX IF NOT EXISTS idx_rpc_provider_usage_hour
    ON rpc_provider_usage (hour DESC);

CREATE INDEX IF NOT EXISTS idx_rpc_provider_usage_provider
    ON rpc_provider_usage (provider, hour DESC);

CREATE TABLE IF NOT EXISTS rpc_capabilities (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    chain TEXT NOT NULL,
    method TEXT NOT NULL,
    status TEXT NOT NULL,             -- 'ok' | 'fail'
    error_message TEXT,               -- first ~200 chars of error body when status='fail'
    sample_params JSONB,              -- the params the probe used, for reproducibility
    tested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rpc_capabilities_provider
    ON rpc_capabilities (provider, chain, tested_at DESC);

INSERT INTO migrations (name) VALUES ('090_rpc_provider_usage') ON CONFLICT DO NOTHING;
