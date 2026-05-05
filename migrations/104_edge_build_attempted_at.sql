-- Migration 104: Add build_attempted_at to edge_build_status
-- Distinguishes "we tried and it failed" from "we succeeded".
-- Gate at worker.py:2018 reads last_built_at (success only).
-- build_attempted_at tracks every attempt regardless of outcome.

ALTER TABLE wallet_graph.edge_build_status
    ADD COLUMN IF NOT EXISTS build_attempted_at TIMESTAMPTZ;

-- Backfill: existing rows with last_built_at get the same value
UPDATE wallet_graph.edge_build_status
SET build_attempted_at = last_built_at
WHERE last_built_at IS NOT NULL AND build_attempted_at IS NULL;

INSERT INTO migrations (name) VALUES ('104_edge_build_attempted_at') ON CONFLICT DO NOTHING;
