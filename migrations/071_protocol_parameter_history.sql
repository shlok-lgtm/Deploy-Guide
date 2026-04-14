-- Migration 071: Protocol Parameter History (Pipeline 9)
-- Captures on-chain governance parameter changes with concurrent score state.

CREATE TABLE IF NOT EXISTS protocol_parameters (
    id SERIAL PRIMARY KEY,
    protocol_slug VARCHAR(100) NOT NULL,
    protocol_id INTEGER,
    parameter_name VARCHAR(200) NOT NULL,
    parameter_key VARCHAR(200) NOT NULL,
    asset_address VARCHAR(42),
    asset_symbol VARCHAR(20),
    contract_address VARCHAR(42) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    current_value DECIMAL(30,8),
    current_value_raw VARCHAR(200),
    value_unit VARCHAR(50),
    last_updated_at TIMESTAMPTZ DEFAULT NOW(),
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (protocol_slug, parameter_key, asset_address, chain)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_protocol_params_slug
    ON protocol_parameters (protocol_slug);


CREATE TABLE IF NOT EXISTS protocol_parameter_changes (
    id SERIAL PRIMARY KEY,
    protocol_slug VARCHAR(100) NOT NULL,
    protocol_id INTEGER,
    parameter_name VARCHAR(200) NOT NULL,
    parameter_key VARCHAR(200) NOT NULL,
    asset_address VARCHAR(42),
    asset_symbol VARCHAR(20),
    contract_address VARCHAR(42) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    previous_value DECIMAL(30,8),
    previous_value_raw VARCHAR(200),
    new_value DECIMAL(30,8),
    new_value_raw VARCHAR(200),
    value_unit VARCHAR(50),
    change_magnitude DECIMAL(10,4),
    change_direction VARCHAR(10),
    block_number BIGINT,
    transaction_hash VARCHAR(66),
    changed_at TIMESTAMPTZ NOT NULL,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    concurrent_sii_score DECIMAL(6,2),
    concurrent_psi_score DECIMAL(6,2),
    hours_since_last_sii_change DECIMAL(8,2),
    sii_trend_7d DECIMAL(6,2),
    change_context VARCHAR(100),
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_param_changes_slug_time
    ON protocol_parameter_changes (protocol_slug, changed_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_param_changes_asset
    ON protocol_parameter_changes (asset_address);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_param_changes_time
    ON protocol_parameter_changes (changed_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_param_changes_context
    ON protocol_parameter_changes (change_context);


CREATE TABLE IF NOT EXISTS protocol_parameter_snapshots (
    id SERIAL PRIMARY KEY,
    protocol_slug VARCHAR(100) NOT NULL,
    protocol_id INTEGER,
    snapshot_date DATE NOT NULL,
    parameters JSONB,
    parameter_count INTEGER,
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ,
    UNIQUE (protocol_slug, snapshot_date)
);


INSERT INTO migrations (name) VALUES ('071_protocol_parameter_history') ON CONFLICT DO NOTHING;
