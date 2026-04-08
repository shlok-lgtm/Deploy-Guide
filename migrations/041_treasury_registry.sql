-- Migration 041: Treasury Registry + Treasury Events
-- Labeled treasury wallets and behavioral event detection.

-- Treasury registry — labeled wallet addresses for behavioral monitoring
CREATE TABLE IF NOT EXISTS wallet_graph.treasury_registry (
    address         TEXT PRIMARY KEY,
    chain           TEXT NOT NULL DEFAULT 'ethereum',
    entity_name     TEXT NOT NULL,
    entity_type     TEXT NOT NULL,           -- foundation, protocol_treasury, corporate, dao, vc
    label_source    TEXT NOT NULL,           -- arkham, nansen, manual, etherscan_label
    label_confidence TEXT DEFAULT 'high',
    wallet_purpose  TEXT,                    -- defi_operations, grants, staking, trading
    related_addresses TEXT[],
    monitoring_enabled BOOLEAN DEFAULT TRUE,
    added_at        TIMESTAMPTZ DEFAULT NOW(),
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_treasury_entity ON wallet_graph.treasury_registry(entity_name);
CREATE INDEX IF NOT EXISTS idx_treasury_type ON wallet_graph.treasury_registry(entity_type);

-- Treasury behavioral events
CREATE TABLE IF NOT EXISTS wallet_graph.treasury_events (
    id              SERIAL PRIMARY KEY,
    wallet_address  TEXT NOT NULL,
    event_type      TEXT NOT NULL,           -- twap_conversion, rebalance, concentration_drift, quality_shift, large_transfer
    event_data      JSONB NOT NULL,
    severity        TEXT NOT NULL DEFAULT 'info',
    confidence      TEXT NOT NULL DEFAULT 'medium',
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged    BOOLEAN DEFAULT FALSE,
    published       BOOLEAN DEFAULT FALSE,
    stablecoins_involved TEXT[],
    protocols_involved   TEXT[],
    risk_score_before NUMERIC,
    risk_score_after  NUMERIC,
    risk_score_delta  NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_treasury_events_wallet ON wallet_graph.treasury_events(wallet_address);
CREATE INDEX IF NOT EXISTS idx_treasury_events_type ON wallet_graph.treasury_events(event_type);
CREATE INDEX IF NOT EXISTS idx_treasury_events_detected ON wallet_graph.treasury_events(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_treasury_events_severity ON wallet_graph.treasury_events(severity);

-- Seed treasury registry with known high-value wallets
INSERT INTO wallet_graph.treasury_registry (address, chain, entity_name, entity_type, label_source, wallet_purpose, notes) VALUES
    -- P0: Ethereum Foundation (demo wallets)
    ('0x9fc3dc011b461664c835f2527fffb1169b3c213e', 'ethereum', 'Ethereum Foundation DeFi Ecosystem', 'foundation', 'arkham', 'defi_operations', 'P0 demo wallet. 3-of-5 Safe multisig, created Jan 2025. Seeded 50K ETH. Deployed 45K to Aave/Spark/Compound Feb 2025. Executing 5K ETH TWAP via CoWSwap, 47K staked toward 70K goal.'),
    ('0x9ee457023bb3de16d51a003a247baead7fce313d', 'ethereum', 'Ethereum Foundation Main', 'foundation', 'arkham', 'grants', 'P0 secondary. Holds ~102K ETH, 21K AETHWETH, 6K WETH, ~$1M DAI+USDC.'),
    -- P1: Protocol treasuries
    ('0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c', 'ethereum', 'Aave Treasury', 'protocol_treasury', 'arkham', 'operations', 'Aave DAO collector'),
    ('0x1a9c8182c09f50c8318d769245bea52c32be35bc', 'ethereum', 'Uniswap Treasury', 'protocol_treasury', 'arkham', 'operations', 'Uniswap DAO timelock'),
    ('0x36928500bc1dcd7af6a2b4008875cc336b927d57', 'ethereum', 'MakerDAO Pause Proxy', 'protocol_treasury', 'etherscan_label', 'governance', 'Sky/MakerDAO governance proxy. Already in etherscan.py KNOWN_HOLDERS.'),
    ('0x3e40d73eb977dc6a537af587d48316fee66e9c8c', 'ethereum', 'Lido Treasury', 'protocol_treasury', 'arkham', 'operations', 'Lido DAO treasury'),
    -- P2: Other
    ('0x55fe002aeff02f77364de339a1292923a15844b8', 'ethereum', 'Circle Reserve', 'corporate', 'etherscan_label', 'operations', 'Circle USDC reserve'),
    ('0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503', 'ethereum', 'Binance Cold Wallet', 'exchange', 'arkham', 'trading', 'Binance 14')
ON CONFLICT (address) DO NOTHING;

-- Ensure P0 treasury wallets are in the wallet indexer scan set
INSERT INTO wallet_graph.wallets (address, chain, source, label)
VALUES
    ('0x9fc3dc011b461664c835f2527fffb1169b3c213e', 'ethereum', 'treasury_registry', 'Ethereum Foundation DeFi Ecosystem'),
    ('0x9ee457023bb3de16d51a003a247baead7fce313d', 'ethereum', 'treasury_registry', 'Ethereum Foundation Main'),
    ('0x36928500bc1dcd7af6a2b4008875cc336b927d57', 'ethereum', 'treasury_registry', 'MakerDAO Pause Proxy'),
    ('0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c', 'ethereum', 'treasury_registry', 'Aave Treasury'),
    ('0x1a9c8182c09f50c8318d769245bea52c32be35bc', 'ethereum', 'treasury_registry', 'Uniswap Treasury'),
    ('0x3e40d73eb977dc6a537af587d48316fee66e9c8c', 'ethereum', 'treasury_registry', 'Lido Treasury')
ON CONFLICT (address, chain) DO UPDATE SET label = EXCLUDED.label, source = EXCLUDED.source;

INSERT INTO migrations (name) VALUES ('041_treasury_registry') ON CONFLICT DO NOTHING;
