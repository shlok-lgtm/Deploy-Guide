-- Basis Protocol - Initial Schema
-- Migration 001: Core tables for SII scoring system
-- Run with: psql $DATABASE_URL < 001_initial_schema.sql

BEGIN;

-- ============================================================================
-- Table 1: stablecoins - Registry of tracked stablecoins
-- ============================================================================
CREATE TABLE IF NOT EXISTS stablecoins (
    id VARCHAR(20) PRIMARY KEY,              -- e.g. 'usdc', 'usdt'
    name VARCHAR(100) NOT NULL,              -- e.g. 'USD Coin'
    symbol VARCHAR(20) NOT NULL,             -- e.g. 'USDC'
    issuer VARCHAR(100),                     -- e.g. 'Circle'
    coingecko_id VARCHAR(100) NOT NULL,      -- e.g. 'usd-coin'
    contract VARCHAR(100),                   -- Ethereum mainnet address
    decimals INTEGER DEFAULT 18,
    scoring_enabled BOOLEAN DEFAULT FALSE,
    status VARCHAR(20) DEFAULT 'active',
    attestation_config JSONB,                -- auditor, frequency, transparency_url
    regulatory_licenses TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- Table 2: component_readings - Raw data points from collectors
-- ============================================================================
CREATE TABLE IF NOT EXISTS component_readings (
    id BIGSERIAL PRIMARY KEY,
    stablecoin_id VARCHAR(20) NOT NULL REFERENCES stablecoins(id),
    component_id VARCHAR(80) NOT NULL,       -- e.g. 'peg_current_deviation'
    category VARCHAR(50) NOT NULL,           -- e.g. 'peg_stability'
    raw_value DOUBLE PRECISION,
    normalized_score DOUBLE PRECISION,        -- 0-100
    data_source VARCHAR(100),                -- e.g. 'coingecko', 'defillama'
    is_stale BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    collected_at TIMESTAMPTZ DEFAULT NOW()
);

-- Immutable date extraction for use in unique index
CREATE OR REPLACE FUNCTION immutable_date(ts timestamptz) RETURNS date AS $$
  SELECT (ts AT TIME ZONE 'UTC')::date;
$$ LANGUAGE sql IMMUTABLE;

-- Expression-based unique index for dedup (one reading per component per day)
DROP INDEX IF EXISTS idx_readings_unique_per_day;
CREATE UNIQUE INDEX idx_readings_unique_per_day ON component_readings(stablecoin_id, component_id, immutable_date(collected_at));

CREATE INDEX IF NOT EXISTS idx_readings_stablecoin ON component_readings(stablecoin_id);
CREATE INDEX IF NOT EXISTS idx_readings_component ON component_readings(component_id);
CREATE INDEX IF NOT EXISTS idx_readings_collected ON component_readings(collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_readings_stablecoin_date ON component_readings(stablecoin_id, collected_at DESC);

-- ============================================================================
-- Table 3: scores - Current computed SII scores (latest snapshot)
-- ============================================================================
CREATE TABLE IF NOT EXISTS scores (
    stablecoin_id VARCHAR(20) PRIMARY KEY REFERENCES stablecoins(id),
    overall_score DECIMAL(5,2) NOT NULL,
    grade VARCHAR(2) NOT NULL,
    
    -- Category scores
    peg_score DECIMAL(5,2),
    liquidity_score DECIMAL(5,2),
    mint_burn_score DECIMAL(5,2),
    distribution_score DECIMAL(5,2),
    structural_score DECIMAL(5,2),
    
    -- Structural sub-scores
    reserves_score DECIMAL(5,2),
    contract_score DECIMAL(5,2),
    oracle_score DECIMAL(5,2),
    governance_score DECIMAL(5,2),
    network_score DECIMAL(5,2),
    
    -- Metadata
    component_count INTEGER,
    formula_version VARCHAR(20) DEFAULT 'v1.0.0',
    data_freshness_pct DECIMAL(5,2),
    
    -- Price context
    current_price DECIMAL(10,6),
    market_cap BIGINT,
    volume_24h BIGINT,
    
    -- Change tracking
    daily_change DECIMAL(6,3),
    weekly_change DECIMAL(6,3),
    
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- Table 4: score_history - Daily score snapshots (migrated from old system)
-- ============================================================================
CREATE TABLE IF NOT EXISTS score_history (
    id SERIAL PRIMARY KEY,
    stablecoin VARCHAR(20) NOT NULL,
    score_date DATE NOT NULL,
    overall_score DECIMAL(5,2) NOT NULL,
    grade VARCHAR(2),
    
    -- Category scores
    peg_score DECIMAL(5,2),
    liquidity_score DECIMAL(5,2),
    mint_burn_score DECIMAL(5,2),
    distribution_score DECIMAL(5,2),
    structural_score DECIMAL(5,2),
    
    -- Structural sub-scores
    reserves_score DECIMAL(5,2),
    contract_score DECIMAL(5,2),
    oracle_score DECIMAL(5,2),
    governance_score DECIMAL(5,2),
    network_score DECIMAL(5,2),
    
    -- Metadata
    component_count INTEGER,
    formula_version VARCHAR(20) DEFAULT 'v1.0.0',
    data_freshness_pct DECIMAL(5,2),
    
    -- Change metrics
    daily_change DECIMAL(6,3),
    weekly_change DECIMAL(6,3),
    
    -- AI explanation (from old system, keep for compatibility)
    ai_explanation TEXT,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(stablecoin, score_date)
);

CREATE INDEX IF NOT EXISTS idx_score_history_stablecoin ON score_history(stablecoin);
CREATE INDEX IF NOT EXISTS idx_score_history_date ON score_history(score_date DESC);
CREATE INDEX IF NOT EXISTS idx_score_history_stablecoin_date ON score_history(stablecoin, score_date DESC);

-- ============================================================================
-- Table 5: score_events - Crisis events and annotations
-- ============================================================================
CREATE TABLE IF NOT EXISTS score_events (
    id SERIAL PRIMARY KEY,
    event_date DATE NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    affected_stablecoins TEXT[],
    description TEXT,
    severity VARCHAR(20) DEFAULT 'info',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_score_events_date ON score_events(event_date);

-- ============================================================================
-- Table 6: historical_prices - Hourly price data for backtesting
-- ============================================================================
CREATE TABLE IF NOT EXISTS historical_prices (
    id SERIAL PRIMARY KEY,
    coingecko_id VARCHAR(50) NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    market_cap DOUBLE PRECISION,
    volume_24h DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_histprices_coin ON historical_prices(coingecko_id);
CREATE INDEX IF NOT EXISTS idx_histprices_time ON historical_prices("timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_histprices_coin_time ON historical_prices(coingecko_id, "timestamp" DESC);

-- ============================================================================
-- Table 7: deviation_events - Detected peg deviations
-- ============================================================================
CREATE TABLE IF NOT EXISTS deviation_events (
    id SERIAL PRIMARY KEY,
    coingecko_id VARCHAR(50) NOT NULL,
    event_start TIMESTAMPTZ NOT NULL,
    event_end TIMESTAMPTZ,
    duration_hours INTEGER,
    max_deviation_pct DOUBLE PRECISION,
    avg_deviation_pct DOUBLE PRECISION,
    direction VARCHAR(10),                   -- 'above' or 'below'
    recovery_complete BOOLEAN DEFAULT FALSE,
    market_cap_at_start DOUBLE PRECISION,
    volume_during_event DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_deviation_coin ON deviation_events(coingecko_id);
CREATE INDEX IF NOT EXISTS idx_deviation_start ON deviation_events(event_start DESC);

-- ============================================================================
-- Table 8: data_provenance - Source tracking per component reading
-- ============================================================================
CREATE TABLE IF NOT EXISTS data_provenance (
    id BIGSERIAL PRIMARY KEY,
    stablecoin_id VARCHAR(20),
    component_id VARCHAR(80),
    category VARCHAR(50),
    raw_value DOUBLE PRECISION,
    normalized_score DOUBLE PRECISION,
    data_source VARCHAR(100),
    is_stale BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    metadata JSONB,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_provenance_stablecoin ON data_provenance(stablecoin_id);
CREATE INDEX IF NOT EXISTS idx_provenance_component ON data_provenance(component_id);
CREATE INDEX IF NOT EXISTS idx_provenance_date ON data_provenance(recorded_at DESC);

-- ============================================================================
-- Seed stablecoin registry
-- ============================================================================
INSERT INTO stablecoins (id, name, symbol, issuer, coingecko_id, contract, decimals, scoring_enabled, attestation_config, regulatory_licenses)
VALUES
    ('usdc', 'USD Coin', 'USDC', 'Circle', 'usd-coin', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 6, TRUE,
     '{"auditor": "Deloitte", "frequency": "monthly", "frequency_days": 35, "transparency_url": "https://www.circle.com/en/transparency"}'::jsonb,
     ARRAY['NY BitLicense', 'UK EMI', 'Singapore MPI']),
    
    ('usdt', 'Tether', 'USDT', 'Tether', 'tether', '0xdac17f958d2ee523a2206206994597c13d831ec7', 6, TRUE,
     '{"auditor": "BDO Italia", "frequency": "quarterly", "frequency_days": 100, "transparency_url": "https://tether.to/en/transparency/"}'::jsonb,
     ARRAY['El Salvador VASP']),
    
    ('dai', 'Dai', 'DAI', 'MakerDAO', 'dai', '0x6b175474e89094c44da98b954eedeac495271d0f', 18, TRUE,
     '{"auditor": "N/A (on-chain)", "frequency": "real-time", "frequency_days": 1, "transparency_url": "https://daistats.com/"}'::jsonb,
     ARRAY['Decentralized - N/A']),
    
    ('frax', 'Frax', 'FRAX', 'Frax Finance', 'frax', '0x853d955acef822db058eb8505911ed77f175b99e', 18, TRUE,
     '{"auditor": "N/A (algorithmic)", "frequency": "real-time", "frequency_days": 1, "transparency_url": "https://facts.frax.finance/"}'::jsonb,
     ARRAY['Decentralized - N/A']),
    
    ('pyusd', 'PayPal USD', 'PYUSD', 'Paxos', 'paypal-usd', '0x6c3ea9036406852006290770bedfcaba0e23a0e8', 6, TRUE,
     '{"auditor": "WithumSmith+Brown", "frequency": "monthly", "frequency_days": 35, "transparency_url": "https://www.paxos.com/pyusd-transparency/"}'::jsonb,
     ARRAY['NY Trust Company Charter', 'NY BitLicense']),
    
    ('fdusd', 'First Digital USD', 'FDUSD', 'First Digital', 'first-digital-usd', '0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409', 18, TRUE,
     '{"auditor": "Prescient Assurance", "frequency": "monthly", "frequency_days": 35, "transparency_url": "https://www.firstdigitallabs.com/transparency"}'::jsonb,
     ARRAY['Hong Kong TCSP']),
    
    ('tusd', 'TrueUSD', 'TUSD', 'Archblock', 'true-usd', '0x0000000000085d4780B73119b644AE5ecd22b376', 18, TRUE,
     '{"auditor": "The Network Firm", "frequency": "monthly", "frequency_days": 35, "transparency_url": "https://real-time-attest.trustexplorer.io/truecurrencies"}'::jsonb,
     ARRAY['Various state licenses']),
    
    ('usdd', 'USDD', 'USDD', 'TRON DAO', 'usdd', '0x0C10bF8FcB7Bf5412187A595ab97a3609160b5c6', 18, TRUE,
     '{"auditor": "N/A", "frequency": "quarterly", "frequency_days": 100, "transparency_url": "https://usdd.io/#/"}'::jsonb,
     ARRAY[]::TEXT[]),
    
    ('usde', 'Ethena USDe', 'USDe', 'Ethena Labs', 'ethena-usde', '0x4c9EDD5852cd905f086C759E8383e09bff1E68B3', 18, TRUE,
     '{"auditor": "Various Custodians", "frequency": "weekly", "frequency_days": 7, "transparency_url": "https://docs.ethena.fi/resources/custodian-attestations"}'::jsonb,
     ARRAY[]::TEXT[])
ON CONFLICT (id) DO NOTHING;

-- ============================================================================
-- Migration tracking
-- ============================================================================
CREATE TABLE IF NOT EXISTS migrations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL UNIQUE,
    applied_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO migrations (name) VALUES ('001_initial_schema') ON CONFLICT DO NOTHING;

COMMIT;
