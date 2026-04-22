-- Migration 091: Protocol Trace Observations (LLL Phase 1 Pipeline 1)
-- Blockscout raw-trace data for PSI protocol transactions

CREATE TABLE IF NOT EXISTS protocol_trace_observations (
    id BIGSERIAL PRIMARY KEY,
    tx_hash TEXT NOT NULL,
    protocol_slug TEXT NOT NULL,
    chain TEXT NOT NULL DEFAULT 'ethereum',
    block_number BIGINT NOT NULL,
    value_usd NUMERIC,
    trace_json JSONB NOT NULL,
    trace_depth INTEGER,
    internal_call_count INTEGER,
    revert_reason TEXT,
    content_hash TEXT,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(tx_hash, chain)
);

CREATE INDEX IF NOT EXISTS idx_ptr_protocol_time
    ON protocol_trace_observations(protocol_slug, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_ptr_block
    ON protocol_trace_observations(chain, block_number DESC);

INSERT INTO migrations (name) VALUES ('091_protocol_trace_observations') ON CONFLICT DO NOTHING;
