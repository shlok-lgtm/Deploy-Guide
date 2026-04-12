-- Migration 055: Protocol pool wallet discovery
-- Links wallets to specific protocol-stablecoin pools via receipt tokens
-- (e.g. aUSDC holders → Aave USDC pool)

BEGIN;

CREATE TABLE IF NOT EXISTS protocol_pool_wallets (
    id BIGSERIAL PRIMARY KEY,
    protocol_slug VARCHAR(100) NOT NULL,
    stablecoin_symbol VARCHAR(50) NOT NULL,
    chain VARCHAR(20) NOT NULL DEFAULT 'ethereum',
    wallet_address VARCHAR(42) NOT NULL,
    pool_contract_address VARCHAR(42) NOT NULL,
    balance DOUBLE PRECISION,
    discovered_at TIMESTAMPTZ DEFAULT NOW(),
    last_seen TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_pool_wallet
        UNIQUE (protocol_slug, stablecoin_symbol, chain, wallet_address)
);

CREATE INDEX IF NOT EXISTS idx_pool_wallets_protocol
    ON protocol_pool_wallets(protocol_slug, stablecoin_symbol);
CREATE INDEX IF NOT EXISTS idx_pool_wallets_wallet
    ON protocol_pool_wallets(wallet_address);
CREATE INDEX IF NOT EXISTS idx_pool_wallets_contract
    ON protocol_pool_wallets(pool_contract_address);

INSERT INTO migrations (name) VALUES ('055_protocol_pool_wallets')
    ON CONFLICT DO NOTHING;

COMMIT;
