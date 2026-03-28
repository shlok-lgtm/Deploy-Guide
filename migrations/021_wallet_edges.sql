-- Migration 021: Wallet relationship edges derived from stablecoin transfers
BEGIN;

CREATE TABLE IF NOT EXISTS wallet_graph.wallet_edges (
    id BIGSERIAL PRIMARY KEY,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42) NOT NULL,

    -- Edge weight signals
    transfer_count INTEGER DEFAULT 0,
    total_value_usd DOUBLE PRECISION DEFAULT 0,
    first_transfer_at TIMESTAMPTZ,
    last_transfer_at TIMESTAMPTZ,

    -- Derived weight (recomputed on update)
    weight DOUBLE PRECISION DEFAULT 0,

    -- Which stablecoins flow between them
    tokens_transferred JSONB DEFAULT '{}',

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_edge UNIQUE (from_address, to_address)
);

CREATE INDEX IF NOT EXISTS idx_edges_from ON wallet_graph.wallet_edges(from_address);
CREATE INDEX IF NOT EXISTS idx_edges_to ON wallet_graph.wallet_edges(to_address);
CREATE INDEX IF NOT EXISTS idx_edges_weight ON wallet_graph.wallet_edges(weight DESC);
CREATE INDEX IF NOT EXISTS idx_edges_last_transfer ON wallet_graph.wallet_edges(last_transfer_at DESC);
CREATE INDEX IF NOT EXISTS idx_edges_value ON wallet_graph.wallet_edges(total_value_usd DESC);

-- Track edge-building progress per wallet
CREATE TABLE IF NOT EXISTS wallet_graph.edge_build_status (
    wallet_address VARCHAR(42) PRIMARY KEY,
    last_built_at TIMESTAMPTZ,
    transfers_processed INTEGER DEFAULT 0,
    edges_created INTEGER DEFAULT 0,
    pages_fetched INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'pending'
);

INSERT INTO migrations (name) VALUES ('021_wallet_edges') ON CONFLICT DO NOTHING;

COMMIT;
