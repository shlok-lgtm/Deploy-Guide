-- Basis Protocol - Wallet Risk Graph Schema
-- Migration 007: wallet_graph schema with wallets, holdings, risk scores, and unscored assets
-- Run with: psql $DATABASE_URL < 007_wallet_graph.sql

BEGIN;

-- Separate schema for wallet graph data
CREATE SCHEMA IF NOT EXISTS wallet_graph;

-- ============================================================================
-- Table 1: wallets - Indexed Ethereum wallets
-- ============================================================================
CREATE TABLE IF NOT EXISTS wallet_graph.wallets (
    address VARCHAR(42) PRIMARY KEY,              -- 0x-prefixed, checksummed
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_indexed_at TIMESTAMPTZ,
    total_stablecoin_value DOUBLE PRECISION,      -- USD total across all stablecoin holdings
    size_tier VARCHAR(20),                        -- 'whale', 'institutional', 'retail'
    source VARCHAR(50),                           -- how we discovered this wallet
    is_contract BOOLEAN DEFAULT FALSE,
    label VARCHAR(200),                           -- optional: 'Circle Treasury', 'Aave V3', etc.
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- Table 2: wallet_holdings - One row per wallet per token per day
-- ============================================================================
CREATE TABLE IF NOT EXISTS wallet_graph.wallet_holdings (
    id BIGSERIAL PRIMARY KEY,
    wallet_address VARCHAR(42) NOT NULL REFERENCES wallet_graph.wallets(address),
    token_address VARCHAR(42) NOT NULL,           -- ERC-20 contract address
    symbol VARCHAR(20),
    balance DOUBLE PRECISION,                     -- token balance adjusted for decimals
    value_usd DOUBLE PRECISION,                   -- balance × current price
    is_scored BOOLEAN DEFAULT FALSE,              -- do we have an SII score for this asset?
    sii_score DOUBLE PRECISION,                   -- SII score at time of indexing
    sii_grade VARCHAR(2),                         -- grade at time of indexing
    pct_of_wallet DOUBLE PRECISION,               -- this holding as % of wallet total stablecoin value
    indexed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_holdings_unique_per_day
    ON wallet_graph.wallet_holdings(wallet_address, token_address, public.immutable_date(indexed_at));
CREATE INDEX IF NOT EXISTS idx_holdings_wallet ON wallet_graph.wallet_holdings(wallet_address);
CREATE INDEX IF NOT EXISTS idx_holdings_token ON wallet_graph.wallet_holdings(token_address);
CREATE INDEX IF NOT EXISTS idx_holdings_indexed ON wallet_graph.wallet_holdings(indexed_at DESC);

-- ============================================================================
-- Table 3: wallet_risk_scores - One row per wallet per scoring run per day
-- ============================================================================
CREATE TABLE IF NOT EXISTS wallet_graph.wallet_risk_scores (
    id BIGSERIAL PRIMARY KEY,
    wallet_address VARCHAR(42) NOT NULL REFERENCES wallet_graph.wallets(address),

    -- Core score
    risk_score DOUBLE PRECISION,                  -- value-weighted avg SII (0-100)
    risk_grade VARCHAR(2),                        -- A+ through F

    -- Enrichment signals
    concentration_hhi DOUBLE PRECISION,           -- Herfindahl index (0-10000)
    concentration_grade VARCHAR(2),
    unscored_pct DOUBLE PRECISION,                -- % of stablecoin value in unscored assets
    coverage_quality VARCHAR(20),                 -- 'full', 'high', 'partial', 'low'

    -- Composition summary
    num_scored_holdings INTEGER,
    num_unscored_holdings INTEGER,
    num_total_holdings INTEGER,
    dominant_asset VARCHAR(20),                   -- symbol of largest holding
    dominant_asset_pct DOUBLE PRECISION,          -- % of wallet in dominant asset

    -- Metadata
    total_stablecoin_value DOUBLE PRECISION,
    size_tier VARCHAR(20),
    formula_version VARCHAR(20) DEFAULT 'wallet-v1.0.0',
    computed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_wrs_unique_per_day
    ON wallet_graph.wallet_risk_scores(wallet_address, public.immutable_date(computed_at));
CREATE INDEX IF NOT EXISTS idx_wrs_wallet ON wallet_graph.wallet_risk_scores(wallet_address);
CREATE INDEX IF NOT EXISTS idx_wrs_score ON wallet_graph.wallet_risk_scores(risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_wrs_computed ON wallet_graph.wallet_risk_scores(computed_at DESC);

-- ============================================================================
-- Table 4: unscored_assets - Backlog of stablecoins without SII scores
-- ============================================================================
CREATE TABLE IF NOT EXISTS wallet_graph.unscored_assets (
    token_address VARCHAR(42) PRIMARY KEY,
    symbol VARCHAR(20),
    name VARCHAR(100),
    decimals INTEGER,
    coingecko_id VARCHAR(100),                    -- NULL until mapped

    -- Demand signals (updated each indexing run)
    wallets_holding INTEGER DEFAULT 0,
    total_value_held DOUBLE PRECISION DEFAULT 0,
    avg_holding_value DOUBLE PRECISION DEFAULT 0,
    max_single_holding DOUBLE PRECISION DEFAULT 0,

    -- Scoring pipeline status
    scoring_status VARCHAR(20) DEFAULT 'unscored',
    scoring_priority INTEGER,
    notes TEXT,

    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_unscored_priority ON wallet_graph.unscored_assets(total_value_held DESC);
CREATE INDEX IF NOT EXISTS idx_unscored_status ON wallet_graph.unscored_assets(scoring_status);

-- ============================================================================
-- Track migration
-- ============================================================================
INSERT INTO migrations (name) VALUES ('007_wallet_graph') ON CONFLICT DO NOTHING;

COMMIT;
