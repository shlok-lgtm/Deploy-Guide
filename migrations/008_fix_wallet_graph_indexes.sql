-- Basis Protocol - Fix wallet_graph indexes
-- Migration 008: Recreate unique indexes with public.immutable_date() so they resolve
-- correctly regardless of search_path.
-- Run with: psql $DATABASE_URL < 008_fix_wallet_graph_indexes.sql

BEGIN;

-- Drop old indexes (may reference unqualified immutable_date)
DROP INDEX IF EXISTS wallet_graph.idx_holdings_unique_per_day;
DROP INDEX IF EXISTS wallet_graph.idx_wrs_unique_per_day;

-- Recreate with explicit public.immutable_date()
CREATE UNIQUE INDEX idx_holdings_unique_per_day
    ON wallet_graph.wallet_holdings(wallet_address, token_address, public.immutable_date(indexed_at));

CREATE UNIQUE INDEX idx_wrs_unique_per_day
    ON wallet_graph.wallet_risk_scores(wallet_address, public.immutable_date(computed_at));

-- Track migration
INSERT INTO migrations (name) VALUES ('008_fix_wallet_graph_indexes') ON CONFLICT DO NOTHING;

COMMIT;
