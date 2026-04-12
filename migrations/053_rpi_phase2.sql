-- Migration 053: RPI Phase 2 — forum posts, protocol config, historical data
-- Supports lens automation, auto-expansion, and historical reconstruction.

BEGIN;

-- Governance forum posts scraped from Discourse-based governance forums
CREATE TABLE IF NOT EXISTS governance_forum_posts (
    id SERIAL PRIMARY KEY,
    protocol_slug TEXT NOT NULL,
    forum_url TEXT NOT NULL,
    post_id TEXT NOT NULL,
    topic_id TEXT,
    title TEXT,
    body_excerpt TEXT,
    author TEXT,
    category TEXT,
    mentions_risk_vendor BOOLEAN DEFAULT FALSE,
    mentioned_vendors JSONB,
    mentions_incident BOOLEAN DEFAULT FALSE,
    mentions_budget BOOLEAN DEFAULT FALSE,
    extracted_budget_amount NUMERIC,
    posted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(protocol_slug, post_id)
);

CREATE INDEX IF NOT EXISTS idx_forum_posts_slug_date
    ON governance_forum_posts(protocol_slug, posted_at DESC);
CREATE INDEX IF NOT EXISTS idx_forum_posts_vendor
    ON governance_forum_posts(protocol_slug, mentions_risk_vendor)
    WHERE mentions_risk_vendor = TRUE;

-- Per-protocol RPI configuration (replaces hardcoded dicts)
CREATE TABLE IF NOT EXISTS rpi_protocol_config (
    id SERIAL PRIMARY KEY,
    protocol_slug TEXT NOT NULL UNIQUE,
    protocol_name TEXT,
    snapshot_space TEXT,
    tally_org_id TEXT,
    governance_forum_url TEXT,
    docs_url TEXT,
    admin_contracts JSONB,
    discovery_source TEXT DEFAULT 'manual',
    coverage_level TEXT DEFAULT 'full',
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Historical RPI component data for temporal reconstruction
CREATE TABLE IF NOT EXISTS historical_rpi_data (
    id SERIAL PRIMARY KEY,
    protocol_slug TEXT NOT NULL,
    record_date DATE NOT NULL,
    spend_ratio NUMERIC,
    parameter_velocity INTEGER,
    parameter_recency INTEGER,
    incident_severity NUMERIC,
    governance_health NUMERIC,
    proposal_count INTEGER,
    risk_proposal_count INTEGER,
    risk_budget_total NUMERIC,
    participation_avg NUMERIC,
    data_source TEXT DEFAULT 'backfill',
    confidence TEXT DEFAULT 'standard',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(protocol_slug, record_date)
);

CREATE INDEX IF NOT EXISTS idx_hist_rpi_slug_date
    ON historical_rpi_data(protocol_slug, record_date);

-- Documentation score evidence (automated rubric scoring)
CREATE TABLE IF NOT EXISTS rpi_doc_scores (
    id SERIAL PRIMARY KEY,
    protocol_slug TEXT NOT NULL,
    criterion TEXT NOT NULL,
    score INTEGER NOT NULL DEFAULT 0,
    evidence_url TEXT,
    evidence_snippet TEXT,
    scored_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(protocol_slug, criterion)
);

INSERT INTO migrations (name) VALUES ('053_rpi_phase2') ON CONFLICT DO NOTHING;

COMMIT;
