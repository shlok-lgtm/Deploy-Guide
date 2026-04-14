-- Migration 062: Contract Upgrade Delta Tracker (Pipeline 3)
-- Tracks bytecode changes for all scored contracts as permanent attested state.

CREATE TABLE IF NOT EXISTS contract_upgrade_history (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(20) NOT NULL,
    entity_id INTEGER NOT NULL,
    entity_symbol VARCHAR(20),
    contract_address VARCHAR(42) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    previous_bytecode_hash VARCHAR(66),
    current_bytecode_hash VARCHAR(66) NOT NULL,
    previous_implementation VARCHAR(42),
    current_implementation VARCHAR(42),
    block_number BIGINT,
    upgrade_detected_at TIMESTAMPTZ DEFAULT NOW(),
    slither_queued BOOLEAN DEFAULT FALSE,
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_upgrade_entity
    ON contract_upgrade_history (entity_type, entity_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_upgrade_contract
    ON contract_upgrade_history (contract_address, chain);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_upgrade_detected_at
    ON contract_upgrade_history (upgrade_detected_at DESC);


CREATE TABLE IF NOT EXISTS contract_bytecode_snapshots (
    id SERIAL PRIMARY KEY,
    contract_address VARCHAR(42) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    bytecode_hash VARCHAR(66) NOT NULL,
    implementation_address VARCHAR(42),
    is_proxy BOOLEAN DEFAULT FALSE,
    is_verified BOOLEAN DEFAULT FALSE,
    abi_hash VARCHAR(66),
    captured_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (contract_address, chain, bytecode_hash)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshot_contract_chain
    ON contract_bytecode_snapshots (contract_address, chain, captured_at DESC);


INSERT INTO migrations (name) VALUES ('062_contract_upgrade_history') ON CONFLICT DO NOTHING;
