-- Migration 065: Sanctions Screening (Pipeline 19)
-- Daily screening against OpenSanctions consolidated dataset.

CREATE TABLE IF NOT EXISTS sanctions_screening_results (
    id SERIAL PRIMARY KEY,
    screened_at TIMESTAMPTZ DEFAULT NOW(),
    entity_type VARCHAR(20),
    entity_id INTEGER,
    entity_symbol VARCHAR(20),
    screen_target VARCHAR(200),
    screen_target_type VARCHAR(20),
    is_match BOOLEAN DEFAULT FALSE,
    match_score DECIMAL(5,4),
    match_dataset VARCHAR(100),
    match_entity_id VARCHAR(200),
    match_details JSONB,
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sanctions_entity
    ON sanctions_screening_results (entity_type, entity_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sanctions_is_match
    ON sanctions_screening_results (is_match) WHERE is_match = TRUE;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sanctions_screened_at
    ON sanctions_screening_results (screened_at DESC);


CREATE TABLE IF NOT EXISTS sanctions_screen_targets (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(20),
    entity_id INTEGER,
    entity_symbol VARCHAR(20),
    target_name VARCHAR(200),
    target_type VARCHAR(20),
    active BOOLEAN DEFAULT TRUE,
    added_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed initial screening targets: stablecoin issuers
INSERT INTO sanctions_screen_targets (entity_type, entity_symbol, target_name, target_type) VALUES
    ('stablecoin_issuer', 'usdt', 'Tether Limited', 'company'),
    ('stablecoin_issuer', 'usdt', 'iFinex Inc', 'company'),
    ('stablecoin_issuer', 'usdc', 'Circle Internet Financial', 'company'),
    ('stablecoin_issuer', 'fdusd', 'First Digital Labs', 'company'),
    ('stablecoin_issuer', 'fdusd', 'First Digital Trust', 'company'),
    ('stablecoin_issuer', 'pyusd', 'Paxos Trust Company', 'company'),
    ('stablecoin_issuer', 'tusd', 'TrueUSD', 'company'),
    ('stablecoin_issuer', 'tusd', 'Archblock', 'company'),
    ('stablecoin_issuer', 'tusd', 'Techteryx', 'company'),
    ('stablecoin_issuer', 'usd1', 'World Liberty Financial', 'company'),
    ('stablecoin_issuer', 'dai', 'MakerDAO', 'company'),
    ('stablecoin_issuer', 'frax', 'Frax Finance', 'company'),
    ('stablecoin_issuer', 'usde', 'Ethena Labs', 'company'),
    ('stablecoin_issuer', 'usdd', 'TRON DAO Reserve', 'company')
ON CONFLICT DO NOTHING;


INSERT INTO migrations (name) VALUES ('065_sanctions_screening') ON CONFLICT DO NOTHING;
