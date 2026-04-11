-- Migration 050: Expand metrics rollup for reports, x402, attestations
-- Date: 2026-04-11

ALTER TABLE metrics_daily_rollup
  ADD COLUMN IF NOT EXISTS report_requests INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS x402_payments INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS x402_revenue_usd DECIMAL(10,4) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS state_attestations INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS report_attestations INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS query_requests INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS cqi_requests INT DEFAULT 0;

INSERT INTO migrations (name) VALUES ('050_metrics_expansion') ON CONFLICT DO NOTHING;
