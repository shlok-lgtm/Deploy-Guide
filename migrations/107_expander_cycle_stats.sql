-- Migration 107: Expander cycle stats for dual-provider observability
-- Tracks per-cycle per-stablecoin provider yield for the dual-provider expander.

CREATE TABLE IF NOT EXISTS expander_cycle_stats (
    cycle_id BIGSERIAL PRIMARY KEY,
    stablecoin_id TEXT NOT NULL,
    etherscan_pages_fetched INT NOT NULL DEFAULT 0,
    blockscout_pages_fetched INT NOT NULL DEFAULT 0,
    etherscan_addresses_returned INT NOT NULL DEFAULT 0,
    blockscout_addresses_returned INT NOT NULL DEFAULT 0,
    new_wallets_persisted INT NOT NULL DEFAULT 0,
    duplicates_skipped INT NOT NULL DEFAULT 0,
    cursor_advanced_to INT,
    cycle_duration_ms INT,
    ran_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_expander_cycle_stats_ran_at
    ON expander_cycle_stats (ran_at DESC);

INSERT INTO migrations (name) VALUES ('107_expander_cycle_stats') ON CONFLICT DO NOTHING;
