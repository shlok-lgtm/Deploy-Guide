-- Migration 074: Morpho Blue market discovery cache
-- Stores per-market metadata for Morpho Blue isolated markets.
-- Populated by app/collectors/morpho_blue.py. Downstream exposure is
-- written to the existing protocol_collateral_exposure table (one row per
-- (chain, loan_token) aggregate) for compatibility with report.py
-- queries that use DISTINCT ON (protocol_slug, chain).

CREATE TABLE IF NOT EXISTS morpho_markets (
    market_id TEXT PRIMARY KEY,                         -- 0x-prefixed bytes32 uniqueKey
    chain VARCHAR(32) NOT NULL,                         -- 'ethereum' | 'base'
    loan_token TEXT NOT NULL,                           -- loan token address (lowercase hex)
    loan_token_symbol VARCHAR(50) NOT NULL,
    loan_token_decimals SMALLINT,
    collateral_token TEXT,                              -- nullable: idle-liquidity markets have no collateral asset
    collateral_token_symbol VARCHAR(50),
    oracle TEXT,                                        -- nullable: idle markets have no oracle
    lltv NUMERIC,                                       -- liquidation LTV, 1e18-scaled
    irm TEXT,                                           -- interest rate model address
    created_at_block BIGINT,                            -- optional, populated if event-scan discovery is used
    supply_assets_usd DOUBLE PRECISION,
    borrow_assets_usd DOUBLE PRECISION,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_read_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_morpho_markets_chain
    ON morpho_markets(chain);

CREATE INDEX IF NOT EXISTS idx_morpho_markets_loan_symbol
    ON morpho_markets(loan_token_symbol);

CREATE INDEX IF NOT EXISTS idx_morpho_markets_last_read
    ON morpho_markets(last_read_at DESC);
