-- Migration 106: Chain-aware columns for Solana-native stablecoin onboarding
--
-- Adds:
--   - wallet_graph.unscored_assets.chain  (Ethereum or Solana scope)
--   - widens unscored_assets.token_address from VARCHAR(42) to VARCHAR(64)
--     (Solana base58 mints are 44 chars; EVM hex is 42)
--   - stablecoins.chain, .solana_mint, .spl_token_program
--
-- Companion to /tmp/usdpt_readiness_audit.md Task 1.
-- DEFAULT 'ethereum' makes existing rows backfill correctly.
-- spl_token_program is strictly typed; no 'unknown' allowed.

BEGIN;

ALTER TABLE wallet_graph.unscored_assets
    ALTER COLUMN token_address TYPE VARCHAR(64);

ALTER TABLE wallet_graph.unscored_assets
    ADD COLUMN IF NOT EXISTS chain TEXT NOT NULL DEFAULT 'ethereum';

ALTER TABLE stablecoins
    ADD COLUMN IF NOT EXISTS chain TEXT NOT NULL DEFAULT 'ethereum';

ALTER TABLE stablecoins
    ADD COLUMN IF NOT EXISTS solana_mint TEXT;

ALTER TABLE stablecoins
    ADD COLUMN IF NOT EXISTS spl_token_program TEXT
    CHECK (spl_token_program IS NULL OR spl_token_program IN ('spl-token', 'token-2022'));

INSERT INTO migrations (name) VALUES ('106_solana_stablecoin_columns') ON CONFLICT DO NOTHING;

COMMIT;
