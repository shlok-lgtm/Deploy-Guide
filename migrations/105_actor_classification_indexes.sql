-- Migration 105: Indexes for actor_classification candidate query
--
-- The candidate query at app/actor_classification.py:322-345 uses
-- correlated EXISTS with LOWER() on wallet_edges. Without indexes,
-- this forces a sequential scan over 710K wallets × 256K edges.
-- Query hung in production since April 12 (23+ days stale).
--
-- CONCURRENTLY avoids locking wallet_edges during build.
-- IF NOT EXISTS makes this idempotent and rerunnable.

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_wallet_edges_from_lower_last
    ON wallet_graph.wallet_edges (LOWER(from_address), last_transfer_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_wallet_edges_to_lower_last
    ON wallet_graph.wallet_edges (LOWER(to_address), last_transfer_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_actor_classifications_wallet_classified
    ON wallet_graph.actor_classifications (wallet_address, classified_at);

INSERT INTO migrations (name) VALUES ('105_actor_classification_indexes') ON CONFLICT DO NOTHING;
