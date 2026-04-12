-- Migration 051: Keeper cycle operational logging
-- Tracks every keeper publish cycle for observability and anomaly detection.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.keeper_cycles (
  id SERIAL PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  duration_ms INTEGER,
  sii_updates_base INTEGER DEFAULT 0,
  sii_updates_arb INTEGER DEFAULT 0,
  psi_updates INTEGER DEFAULT 0,
  report_hashes_published INTEGER DEFAULT 0,
  state_root_published BOOLEAN DEFAULT FALSE,
  gas_used_base_wei NUMERIC,
  gas_used_arb_wei NUMERIC,
  errors TEXT[],
  trigger_reason TEXT DEFAULT 'scheduled'
);

CREATE INDEX IF NOT EXISTS idx_keeper_cycles_started
  ON ops.keeper_cycles(started_at DESC);

INSERT INTO migrations (name) VALUES ('051_keeper_cycles') ON CONFLICT DO NOTHING;
