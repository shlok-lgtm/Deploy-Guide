-- Migration 109: Replay 4 columns dropped from exchange_snapshots
--   by pg_dump silent drift.
--
-- 2026-05-12 substrate audit (Blocker 2 of the v9.12 P0 sweep — see
-- docs/audits/2026-05-11-module-canonical-migration-plan.md):
--
--   Migration 058 (`058_universal_data_layer.sql:208-224`) declared
--   exchange_snapshots with 14 columns:
--       id, exchange_id, name, trust_score, trust_score_rank,
--       trade_volume_24h_btc, trade_volume_24h_usd, year_established,
--       country, trading_pairs, has_trading_incentive,
--       stablecoin_pairs, raw_data, snapshot_at
--
--   `information_schema.columns` on prod (2026-05-12 09:15Z) shows 11:
--   the four below are MISSING:
--       trade_volume_24h_usd  NUMERIC
--       has_trading_incentive BOOLEAN
--       stablecoin_pairs      JSONB
--       raw_data              JSONB
--
--   `migrations` records `058_universal_data_layer` as applied
--   2026-04-13. Same root cause as the 2026-05-10 / Wave 9c incident:
--   the Replit→owned-Neon pg_dump preserved the `migrations` tracking
--   table but silently lost partial DDL content for tables created
--   outside Replit's UI. Mirrors migration 108's pattern; this one
--   replays columns instead of tables.
--
-- Consumer impact (lesson 10):
--   - app/server.py:7886 (`/api/data/exchanges` list mode) selects
--     `trade_volume_24h_usd` and ORDER BYs it — currently throws
--     `column "trade_volume_24h_usd" does not exist` on every call.
--     Confirmed by running the exact query against prod Neon
--     (2026-05-12 09:10Z).
--   - app/data_layer/exchange_collector.py:153-171 inserts all 4 of
--     these columns; failures are silently swallowed by
--     `main.py:342-344` (try/except without _record_cycle_error)
--     and gated out of enrichment_worker.py:880 in steady state.
--
-- After this migration runs:
--   - server.py:7886 unbroken.
--   - The exchange_collector module's INSERT path no longer raises
--     missing-column errors. (List-reconciliation + _EX_FIX port
--     for the canonical-writer refactor stay separate concerns —
--     handled by the v9.12 module-canonical refactor PR that
--     follows.)
--
-- Idempotency: each ALTER uses ADD COLUMN IF NOT EXISTS (PostgreSQL
-- 9.6+). Safe to re-run; safe on prod whether or not any of the
-- columns happen to already be present.

ALTER TABLE exchange_snapshots
  ADD COLUMN IF NOT EXISTS trade_volume_24h_usd NUMERIC,
  ADD COLUMN IF NOT EXISTS has_trading_incentive BOOLEAN,
  ADD COLUMN IF NOT EXISTS stablecoin_pairs JSONB,
  ADD COLUMN IF NOT EXISTS raw_data JSONB;

INSERT INTO migrations (name) VALUES ('109_exchange_snapshots_drift_replay') ON CONFLICT DO NOTHING;
