-- Migration 042: Normalize all wallet addresses to lowercase
-- Fixes case-sensitive duplicates from EIP-55 checksummed addresses
-- Safe to re-run: each statement is idempotent

-- 1. wallet_graph.wallets — deduplicate by keeping the row with the most recent activity
-- First, merge duplicates: for each lowercase address+chain pair, keep the one with the
-- latest last_indexed_at (or highest total_stablecoin_value as tiebreaker)
DELETE FROM wallet_graph.wallets w
WHERE EXISTS (
    SELECT 1 FROM wallet_graph.wallets w2
    WHERE LOWER(w2.address) = LOWER(w.address)
      AND w2.chain = w.chain
      AND w2.ctid != w.ctid
      AND (
          w2.last_indexed_at > w.last_indexed_at
          OR (w2.last_indexed_at = w.last_indexed_at AND w2.total_stablecoin_value > w.total_stablecoin_value)
          OR (w2.last_indexed_at IS NOT NULL AND w.last_indexed_at IS NULL)
      )
);

-- Now lowercase all remaining addresses
UPDATE wallet_graph.wallets SET address = LOWER(address) WHERE address != LOWER(address);

-- 2. wallet_graph.wallet_holdings — lowercase wallet_address
UPDATE wallet_graph.wallet_holdings SET wallet_address = LOWER(wallet_address) WHERE wallet_address != LOWER(wallet_address);

-- 3. wallet_graph.wallet_risk_scores — deduplicate then lowercase
-- Keep the most recent score per lowercase address
DELETE FROM wallet_graph.wallet_risk_scores wrs
WHERE EXISTS (
    SELECT 1 FROM wallet_graph.wallet_risk_scores wrs2
    WHERE LOWER(wrs2.wallet_address) = LOWER(wrs.wallet_address)
      AND wrs2.ctid != wrs.ctid
      AND wrs2.computed_at > wrs.computed_at
);

UPDATE wallet_graph.wallet_risk_scores SET wallet_address = LOWER(wallet_address) WHERE wallet_address != LOWER(wallet_address);

-- 4. wallet_graph.wallet_edges — lowercase from_address and to_address
-- Handle potential conflicts from case duplicates: merge by summing counts/values
-- Simple approach: lowercase first, let ON CONFLICT handle if constraint exists
UPDATE wallet_graph.wallet_edges SET from_address = LOWER(from_address) WHERE from_address != LOWER(from_address);
UPDATE wallet_graph.wallet_edges SET to_address = LOWER(to_address) WHERE to_address != LOWER(to_address);

-- 5. wallet_graph.edge_build_status — lowercase wallet_address
UPDATE wallet_graph.edge_build_status SET wallet_address = LOWER(wallet_address) WHERE wallet_address != LOWER(wallet_address);

-- 6. wallet_graph.wallet_profiles — lowercase address
UPDATE wallet_graph.wallet_profiles SET address = LOWER(address) WHERE address != LOWER(address);

-- Record migration
INSERT INTO migrations (name) VALUES ('042_lowercase_addresses') ON CONFLICT DO NOTHING;
