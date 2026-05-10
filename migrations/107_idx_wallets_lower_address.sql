-- Migration 107: Functional index on LOWER(address) for wallets
--
-- Applied to production manually (out-of-band) to unblock case-insensitive
-- address lookups against wallet_graph.wallets. Registering as a file so
-- fresh environments get the same index when migrations are replayed.
--
-- CONCURRENTLY avoids locking wallet_graph.wallets during build.
-- IF NOT EXISTS makes this idempotent and rerunnable (production already
-- has the index; this is a no-op there).
--
-- Note: applied to production manually under prior name '106_idx_wallets_lower_address';
-- formalized here as migration 107 due to #112 occupying 106.

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_wallets_lower_address
    ON wallet_graph.wallets (LOWER(address));

INSERT INTO migrations (name) VALUES ('107_idx_wallets_lower_address') ON CONFLICT DO NOTHING;
