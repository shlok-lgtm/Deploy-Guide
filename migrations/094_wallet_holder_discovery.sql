-- Migration 094: Wallet Holder Discovery (Phase 2 Sprint 1)
-- Tracks which wallets hold scored assets for wallet_graph breadth expansion

CREATE TABLE IF NOT EXISTS wallet_holder_discovery (
    id BIGSERIAL PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    entity_contract TEXT NOT NULL,
    chain TEXT NOT NULL DEFAULT 'ethereum',
    balance_raw NUMERIC,
    balance_usd NUMERIC,
    rank_in_entity INTEGER,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL DEFAULT 'etherscan_pro',
    UNIQUE(wallet_address, entity_id, entity_contract, chain)
);

CREATE INDEX IF NOT EXISTS idx_whd_wallet
    ON wallet_holder_discovery(wallet_address);
CREATE INDEX IF NOT EXISTS idx_whd_entity_time
    ON wallet_holder_discovery(entity_id, discovered_at DESC);
CREATE INDEX IF NOT EXISTS idx_whd_entity_type
    ON wallet_holder_discovery(entity_type, entity_id);

INSERT INTO migrations (name) VALUES ('094_wallet_holder_discovery') ON CONFLICT DO NOTHING;
