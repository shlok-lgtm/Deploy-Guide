-- Migration 038: Partial index on wallet_holdings for leaderboard queries
-- Date: 2026-04-07
-- Speeds up queries that filter recent holdings above dust threshold

CREATE INDEX IF NOT EXISTS idx_wallet_holdings_recent
ON wallet_graph.wallet_holdings (wallet_address, indexed_at DESC, value_usd)
WHERE value_usd >= 0.01;
