BEGIN;

-- Add chain columns first
ALTER TABLE wallet_graph.wallets ADD COLUMN IF NOT EXISTS chain VARCHAR(20) DEFAULT 'ethereum';
ALTER TABLE wallet_graph.wallet_holdings ADD COLUMN IF NOT EXISTS chain VARCHAR(20) DEFAULT 'ethereum';
ALTER TABLE wallet_graph.wallet_risk_scores ADD COLUMN IF NOT EXISTS chain VARCHAR(20) DEFAULT 'ethereum';

-- Drop FK constraints that depend on wallets PK
ALTER TABLE wallet_graph.wallet_holdings DROP CONSTRAINT IF EXISTS wallet_holdings_wallet_address_fkey;
ALTER TABLE wallet_graph.wallet_risk_scores DROP CONSTRAINT IF EXISTS wallet_risk_scores_wallet_address_fkey;

-- Wallets: change PK from (address) to (address, chain)
ALTER TABLE wallet_graph.wallets DROP CONSTRAINT IF EXISTS wallets_pkey;
ALTER TABLE wallet_graph.wallets ADD CONSTRAINT wallets_pkey PRIMARY KEY (address, chain);

-- Update the unique-per-day index to be chain-aware
DROP INDEX IF EXISTS wallet_graph.idx_holdings_unique_per_day;
CREATE UNIQUE INDEX idx_holdings_unique_per_day
    ON wallet_graph.wallet_holdings (wallet_address, token_address, chain, immutable_date(indexed_at));

DROP INDEX IF EXISTS wallet_graph.idx_holdings_wallet;
CREATE INDEX IF NOT EXISTS idx_holdings_wallet ON wallet_graph.wallet_holdings(wallet_address, chain);

-- Unified cross-chain profile table
CREATE TABLE IF NOT EXISTS wallet_graph.wallet_profiles (
    address VARCHAR(42) PRIMARY KEY,
    is_contract BOOLEAN DEFAULT FALSE,
    chains_active JSONB DEFAULT '[]',
    total_value_all_chains DOUBLE PRECISION DEFAULT 0,
    holdings_by_chain JSONB DEFAULT '{}',
    edge_count_all_chains INTEGER DEFAULT 0,
    risk_grade_aggregate VARCHAR(5),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_profiles_value
    ON wallet_graph.wallet_profiles(total_value_all_chains DESC);

INSERT INTO migrations (name) VALUES ('025_multichain_wallets') ON CONFLICT DO NOTHING;

COMMIT;
