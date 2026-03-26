-- Migration 011: token_type column on unscored_assets
-- Classifies discovered tokens as stablecoin, non_stablecoin, or unknown.
-- promote_eligible_assets() only promotes rows where token_type = 'stablecoin'.
-- The wallet risk graph tracks everything; only stablecoins enter SII scoring.

ALTER TABLE wallet_graph.unscored_assets
    ADD COLUMN IF NOT EXISTS token_type VARCHAR(20) DEFAULT 'unknown';

CREATE INDEX IF NOT EXISTS idx_unscored_token_type
    ON wallet_graph.unscored_assets(token_type);

-- Track migration
INSERT INTO migrations (name) VALUES ('011_token_type_column') ON CONFLICT DO NOTHING;
