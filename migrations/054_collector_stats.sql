-- Migration 054: Collector cycle stats
-- Tracks per-collector performance across scoring cycles for observability.

CREATE TABLE IF NOT EXISTS collector_cycle_stats (
    id SERIAL PRIMARY KEY,
    cycle_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    collector_name VARCHAR(30) NOT NULL,
    coins_ok INT DEFAULT 0,
    coins_timeout INT DEFAULT 0,
    coins_error INT DEFAULT 0,
    avg_latency_ms INT DEFAULT 0,
    total_components INT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_collector_stats_ts
    ON collector_cycle_stats(cycle_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_collector_stats_name
    ON collector_cycle_stats(collector_name, cycle_timestamp DESC);

INSERT INTO migrations (name) VALUES ('054_collector_stats') ON CONFLICT DO NOTHING;
