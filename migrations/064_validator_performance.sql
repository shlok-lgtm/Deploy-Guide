-- Migration 064: Validator Performance Snapshots (Pipeline 17)
-- Captures Ethereum validator performance data from Rated Network for LSTI scoring.

CREATE TABLE IF NOT EXISTS validator_performance_snapshots (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    operator_name VARCHAR(200),
    operator_id VARCHAR(200),
    entity_type VARCHAR(50),
    validators_count INTEGER,
    effectiveness_score DECIMAL(6,4),
    attestation_effectiveness DECIMAL(6,4),
    proposal_luck DECIMAL(8,4),
    slashing_penalty_eth DECIMAL(20,8) DEFAULT 0,
    avg_validator_age_days INTEGER,
    network_penetration DECIMAL(8,6),
    lsti_entity_slug VARCHAR(100),
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ,
    UNIQUE (snapshot_date, operator_id)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_validator_lsti_date
    ON validator_performance_snapshots (lsti_entity_slug, snapshot_date DESC);


CREATE TABLE IF NOT EXISTS validator_slashing_events (
    id SERIAL PRIMARY KEY,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    operator_id VARCHAR(200),
    lsti_entity_slug VARCHAR(100),
    validators_slashed INTEGER,
    estimated_penalty_eth DECIMAL(20,8),
    epoch BIGINT,
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_slashing_lsti
    ON validator_slashing_events (lsti_entity_slug, detected_at DESC);


INSERT INTO migrations (name) VALUES ('064_validator_performance') ON CONFLICT DO NOTHING;
