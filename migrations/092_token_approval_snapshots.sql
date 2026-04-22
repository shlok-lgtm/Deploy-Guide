-- Migration 092: Token Approval Snapshots (LLL Phase 1 Pipeline 2)
-- Diff-capture of ERC-20 approval state for top wallets

CREATE TABLE IF NOT EXISTS token_approval_snapshots (
    id BIGSERIAL PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    token_address TEXT NOT NULL,
    spender_address TEXT NOT NULL,
    allowance NUMERIC NOT NULL,
    allowance_usd NUMERIC,
    chain TEXT NOT NULL DEFAULT 'ethereum',
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    previous_allowance NUMERIC,
    UNIQUE(wallet_address, token_address, spender_address, chain, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_tas_wallet_time
    ON token_approval_snapshots(wallet_address, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_tas_spender_time
    ON token_approval_snapshots(spender_address, snapshot_at DESC);

INSERT INTO migrations (name) VALUES ('092_token_approval_snapshots') ON CONFLICT DO NOTHING;
