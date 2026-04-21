-- Migration 089: Methodology hash registry for on-chain anchoring
-- Stores immutable methodology definitions with content hashes

CREATE TABLE IF NOT EXISTS methodology_hashes (
    methodology_id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    description TEXT,
    committed_on_chain_base BOOLEAN DEFAULT FALSE,
    committed_on_chain_arbitrum BOOLEAN DEFAULT FALSE,
    on_chain_tx_hash_base TEXT,
    on_chain_tx_hash_arbitrum TEXT,
    committed_at TIMESTAMPTZ,
    registered_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_meth_pending_base
    ON methodology_hashes(committed_on_chain_base)
    WHERE committed_on_chain_base = FALSE;

CREATE INDEX IF NOT EXISTS idx_meth_pending_arb
    ON methodology_hashes(committed_on_chain_arbitrum)
    WHERE committed_on_chain_arbitrum = FALSE;

INSERT INTO migrations (name) VALUES ('089_methodology_hashes') ON CONFLICT DO NOTHING;
