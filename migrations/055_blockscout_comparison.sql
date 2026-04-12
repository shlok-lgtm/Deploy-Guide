-- Migration 055: Blockscout data source comparison table
-- Tracks parity between Etherscan V2 and Blockscout API responses
-- during the 1-week evaluation period.

CREATE TABLE IF NOT EXISTS data_source_comparisons (
    id SERIAL PRIMARY KEY,
    endpoint TEXT NOT NULL,
    params JSONB,
    etherscan_hash TEXT,
    blockscout_hash TEXT,
    match_status TEXT CHECK (match_status IN ('exact', 'close', 'mismatch', 'blockscout_error', 'etherscan_error')),
    etherscan_ms INTEGER,
    blockscout_ms INTEGER,
    compared_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dsc_match_status ON data_source_comparisons (match_status);
CREATE INDEX IF NOT EXISTS idx_dsc_endpoint ON data_source_comparisons (endpoint);
CREATE INDEX IF NOT EXISTS idx_dsc_compared_at ON data_source_comparisons (compared_at DESC);

-- Exchange health checks table (Sprint 3)
CREATE TABLE IF NOT EXISTS exchange_health_checks (
    id SERIAL PRIMARY KEY,
    exchange_slug TEXT NOT NULL,
    status_code INTEGER,
    response_time_ms INTEGER,
    is_healthy BOOLEAN DEFAULT FALSE,
    error TEXT,
    checked_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_exchange_health_slug_time
    ON exchange_health_checks (exchange_slug, checked_at DESC);
