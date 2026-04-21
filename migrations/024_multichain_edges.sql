BEGIN;

-- Add chain column to wallet_edges
ALTER TABLE wallet_graph.wallet_edges
    ADD COLUMN IF NOT EXISTS chain VARCHAR(20) DEFAULT 'ethereum';

-- Drop old unique constraint and recreate with chain
ALTER TABLE wallet_graph.wallet_edges DROP CONSTRAINT IF EXISTS unique_edge;
ALTER TABLE wallet_graph.wallet_edges
    ADD CONSTRAINT unique_edge UNIQUE (from_address, to_address, chain);

-- Update indexes to be chain-aware
DROP INDEX IF EXISTS wallet_graph.idx_edges_from;
CREATE INDEX IF NOT EXISTS idx_edges_from ON wallet_graph.wallet_edges(from_address, chain);

DROP INDEX IF EXISTS wallet_graph.idx_edges_to;
CREATE INDEX IF NOT EXISTS idx_edges_to ON wallet_graph.wallet_edges(to_address, chain);

-- Same for archive table
ALTER TABLE wallet_graph.wallet_edges_archive
    ADD COLUMN IF NOT EXISTS chain VARCHAR(20) DEFAULT 'ethereum';

DROP INDEX IF EXISTS wallet_graph.idx_edges_archive_from;
CREATE INDEX IF NOT EXISTS idx_edges_archive_from ON wallet_graph.wallet_edges_archive(from_address, chain);

DROP INDEX IF EXISTS wallet_graph.idx_edges_archive_to;
CREATE INDEX IF NOT EXISTS idx_edges_archive_to ON wallet_graph.wallet_edges_archive(to_address, chain);

-- Add chain to edge_build_status
ALTER TABLE wallet_graph.edge_build_status
    ADD COLUMN IF NOT EXISTS chain VARCHAR(20) DEFAULT 'ethereum';

-- Recreate PK as (wallet_address, chain)
ALTER TABLE wallet_graph.edge_build_status DROP CONSTRAINT IF EXISTS edge_build_status_pkey;
ALTER TABLE wallet_graph.edge_build_status
    ADD CONSTRAINT edge_build_status_pkey PRIMARY KEY (wallet_address, chain);

INSERT INTO migrations (name) VALUES ('024_multichain_edges') ON CONFLICT DO NOTHING;

COMMIT;
