-- Migration 099: Add edge_type column to wallet_edges for semantic distinction

ALTER TABLE wallet_graph.wallet_edges
    ADD COLUMN IF NOT EXISTS edge_type TEXT NOT NULL DEFAULT 'transfer';

CREATE INDEX IF NOT EXISTS idx_wallet_edges_edge_type
    ON wallet_graph.wallet_edges(edge_type);

-- Backfill: existing rows from edge_builder are shared_holder edges
UPDATE wallet_graph.wallet_edges
    SET edge_type = 'shared_holder'
    WHERE edge_type = 'transfer'
      AND created_at < '2026-04-25 00:00:00';

-- Update unique constraint to include edge_type
ALTER TABLE wallet_graph.wallet_edges DROP CONSTRAINT IF EXISTS unique_edge;
ALTER TABLE wallet_graph.wallet_edges
    ADD CONSTRAINT unique_edge UNIQUE (from_address, to_address, chain, edge_type);

INSERT INTO migrations (name) VALUES ('099_wallet_edge_type') ON CONFLICT DO NOTHING;
