-- Migration 003: Create legacy tables for historical data import
-- These tables store data from the old SII prototype (Neon + Replit databases)

CREATE TABLE IF NOT EXISTS legacy_score_history (
    id INTEGER PRIMARY KEY,
    stablecoin VARCHAR(50),
    score_date DATE,
    overall_score NUMERIC(6,2),
    grade VARCHAR(5),
    peg_score NUMERIC(6,2),
    liquidity_score NUMERIC(6,2),
    mint_burn_score NUMERIC(6,2),
    distribution_score NUMERIC(6,2),
    structural_score NUMERIC(6,2),
    reserves_score NUMERIC(6,2),
    contract_score NUMERIC(6,2),
    oracle_score NUMERIC(6,2),
    governance_score NUMERIC(6,2),
    network_score NUMERIC(6,2),
    component_count INTEGER,
    formula_version VARCHAR(30),
    data_freshness_pct NUMERIC(6,2),
    daily_change NUMERIC(8,3),
    weekly_change NUMERIC(8,3),
    created_at TIMESTAMPTZ,
    ai_explanation TEXT
);

CREATE TABLE IF NOT EXISTS legacy_score_events (
    id INTEGER PRIMARY KEY,
    event_date DATE,
    event_name TEXT,
    event_type VARCHAR(50),
    affected_stablecoins TEXT[],
    description TEXT,
    severity VARCHAR(20),
    created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS legacy_deviation_events (
    id INTEGER PRIMARY KEY,
    coingecko_id VARCHAR(100),
    event_start TIMESTAMPTZ,
    event_end TIMESTAMPTZ,
    duration_hours INTEGER,
    max_deviation_pct NUMERIC(10,4),
    avg_deviation_pct NUMERIC(10,4),
    direction VARCHAR(10),
    recovery_complete BOOLEAN,
    market_cap_at_start NUMERIC(20,2),
    volume_during_event NUMERIC(20,2),
    created_at TIMESTAMPTZ
);

INSERT INTO migrations (name, applied_at) VALUES ('003_legacy_tables', NOW()) ON CONFLICT DO NOTHING;
