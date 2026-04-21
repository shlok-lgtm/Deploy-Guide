-- Migration 087: On-chain anchoring columns for track_record_entries
-- Supports keeper polling for pending commits and recording tx hashes

ALTER TABLE track_record_entries ADD COLUMN IF NOT EXISTS committed_on_chain_base BOOLEAN DEFAULT FALSE;
ALTER TABLE track_record_entries ADD COLUMN IF NOT EXISTS committed_on_chain_arbitrum BOOLEAN DEFAULT FALSE;
ALTER TABLE track_record_entries ADD COLUMN IF NOT EXISTS on_chain_tx_hash_base TEXT;
ALTER TABLE track_record_entries ADD COLUMN IF NOT EXISTS on_chain_tx_hash_arbitrum TEXT;
ALTER TABLE track_record_entries ADD COLUMN IF NOT EXISTS committed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_tr_pending_base
    ON track_record_entries(committed_on_chain_base)
    WHERE committed_on_chain_base = FALSE;

CREATE INDEX IF NOT EXISTS idx_tr_pending_arb
    ON track_record_entries(committed_on_chain_arbitrum)
    WHERE committed_on_chain_arbitrum = FALSE;

INSERT INTO migrations (name) VALUES ('087_track_record_on_chain_columns') ON CONFLICT DO NOTHING;
