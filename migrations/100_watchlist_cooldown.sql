-- Migration 100: Watchlist cooldown + measure_name + notes columns
--
-- C4 (detection pipeline) needs three additional columns on
-- engine_watchlist beyond what migration 098 provided:
--
--   last_triggered_at  — tracks when the threshold last fired so the
--                         24h cooldown logic can prevent re-triggering.
--   measure_name       — the specific component or category to evaluate
--                         (e.g. "security" inside the PSI category_scores
--                         JSONB). Required for score_below/score_above/
--                         score_drop_abs thresholds; optional for
--                         tvl_drop_pct (which uses the implicit `tvl`
--                         measure on PSI).
--   notes              — free-text operator note attached to the row
--                         for context in admin UIs.
--
-- All columns added with IF NOT EXISTS for idempotency. No NOT NULL
-- constraint — existing rows from migration 098 (none yet in production,
-- but defensive) keep their NULLs and the C4 evaluator handles missing
-- measure_name by falling back to the implicit measure for the threshold
-- type.
--
-- Note on numbering: migration 099 was reserved in the v0.2a doc for C4
-- but was taken by 099_wallet_edge_type during S2c work. C4 ships as
-- 100 instead.
-- ============================================================================

BEGIN;

ALTER TABLE engine_watchlist
  ADD COLUMN IF NOT EXISTS last_triggered_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS measure_name TEXT,
  ADD COLUMN IF NOT EXISTS notes TEXT;

INSERT INTO migrations (name) VALUES ('100_watchlist_cooldown')
  ON CONFLICT DO NOTHING;

COMMIT;
