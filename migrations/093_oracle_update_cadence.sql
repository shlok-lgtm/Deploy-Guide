-- Migration 093: Oracle Update Cadence (LLL Phase 1 Pipeline 3)
-- Tracks Chainlink round updates and inter-update gaps at 5-min sampling

CREATE TABLE IF NOT EXISTS oracle_update_cadence (
    id BIGSERIAL PRIMARY KEY,
    oracle_id TEXT NOT NULL,
    round_id BIGINT NOT NULL,
    answer NUMERIC,
    updated_at_block BIGINT NOT NULL,
    updated_at_timestamp TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    gap_from_previous_seconds INTEGER,
    content_hash TEXT,
    UNIQUE(oracle_id, round_id)
);

CREATE INDEX IF NOT EXISTS idx_ouc_oracle_time
    ON oracle_update_cadence(oracle_id, observed_at DESC);

INSERT INTO migrations (name) VALUES ('093_oracle_update_cadence') ON CONFLICT DO NOTHING;
