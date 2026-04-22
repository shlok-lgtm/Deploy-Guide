-- Migration 091 — Mempool observation capture.
--
-- Supports app/data_layer/mempool_watcher.py, which subscribes to Alchemy's
-- alchemy_pendingTransactions WebSocket feed with server-side address
-- filtering. Each pending tx that targets a watchlisted address lands here,
-- then a background reconciliation loop updates confirmed_block /
-- confirmed_at / confirmation_latency_ms once the tx lands in a block
-- (or marks it dropped after 10 min of no confirmation).
--
-- The task spec in the mempool-watcher prompt called for migration 086,
-- but 086..090 are already taken (track_record_rpi_delta_trigger through
-- rpc_provider_usage). This migration uses the next free slot.
--
-- Partial index on unconfirmed rows keeps the reconciliation loop cheap
-- even as the table grows to the expected 1K-20K rows/day.

CREATE TABLE IF NOT EXISTS mempool_observations (
    id BIGSERIAL PRIMARY KEY,
    tx_hash TEXT NOT NULL,
    from_address TEXT,
    to_address TEXT,
    value_wei NUMERIC,
    value_usd NUMERIC,
    gas_price_wei NUMERIC,
    nonce INT,
    input_data_truncated TEXT,      -- first 512 bytes of input calldata, hex
    function_selector TEXT,         -- first 4 bytes of input calldata, hex
    source TEXT NOT NULL DEFAULT 'alchemy',
    seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    seen_at_ms BIGINT NOT NULL,     -- unix epoch ms for sequence analysis
    confirmed_block INT,
    confirmed_at TIMESTAMPTZ,
    confirmation_latency_ms INT,
    dropped BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (tx_hash)
);

CREATE INDEX IF NOT EXISTS idx_mempool_obs_to_time
    ON mempool_observations (to_address, seen_at DESC);

-- Partial index keyed only on unconfirmed observations — the
-- reconciliation loop filters by (confirmed_block IS NULL AND dropped=FALSE)
-- every 60s.
CREATE INDEX IF NOT EXISTS idx_mempool_obs_unconfirmed
    ON mempool_observations (seen_at)
    WHERE confirmed_block IS NULL AND dropped = FALSE;

CREATE INDEX IF NOT EXISTS idx_mempool_obs_seen_at
    ON mempool_observations (seen_at DESC);

INSERT INTO migrations (name) VALUES ('091_mempool_observations') ON CONFLICT DO NOTHING;
