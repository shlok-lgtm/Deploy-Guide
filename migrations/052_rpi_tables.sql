-- Migration 052: Risk Posture Index (RPI) tables
-- RPI (Primitive #22) measures how well a protocol manages risk:
-- governance spending, parameter changes, vendor relationships, incident history.

BEGIN;

-- Core RPI component readings (per-protocol, per-component)
CREATE TABLE IF NOT EXISTS rpi_components (
    id SERIAL PRIMARY KEY,
    protocol_slug VARCHAR(100) NOT NULL,
    component_id VARCHAR(100) NOT NULL,
    raw_value DOUBLE PRECISION,
    normalized_score DOUBLE PRECISION,
    source VARCHAR(100),
    source_url TEXT,
    metadata JSONB DEFAULT '{}',
    collected_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rpi_components_slug ON rpi_components(protocol_slug);
CREATE INDEX IF NOT EXISTS idx_rpi_components_component ON rpi_components(component_id);
CREATE INDEX IF NOT EXISTS idx_rpi_components_collected ON rpi_components(collected_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_rpi_components_unique_per_day
    ON rpi_components(protocol_slug, component_id, (collected_at::date));

-- Computed RPI scores (composite, one per protocol per day)
CREATE TABLE IF NOT EXISTS rpi_scores (
    id SERIAL PRIMARY KEY,
    protocol_slug VARCHAR(100) NOT NULL,
    protocol_name VARCHAR(200),
    overall_score DOUBLE PRECISION,
    grade VARCHAR(2),
    category_scores JSONB,
    component_scores JSONB,
    raw_values JSONB,
    formula_version VARCHAR(20) DEFAULT 'rpi-v1.0.0',
    inputs_hash VARCHAR(66),
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    scored_date DATE DEFAULT CURRENT_DATE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_rpi_scores_unique_per_day
    ON rpi_scores(protocol_slug, scored_date);

ALTER TABLE rpi_scores ADD CONSTRAINT rpi_scores_protocol_slug_scored_date_key
    UNIQUE USING INDEX idx_rpi_scores_unique_per_day;

-- Governance proposals scraped from Snapshot/Tally
CREATE TABLE IF NOT EXISTS governance_proposals (
    id SERIAL PRIMARY KEY,
    protocol_slug VARCHAR(100) NOT NULL,
    proposal_id VARCHAR(200) NOT NULL,
    source VARCHAR(50) NOT NULL,          -- 'snapshot' or 'tally'
    title TEXT,
    body TEXT,
    state VARCHAR(50),                    -- 'active', 'closed', 'pending'
    is_risk_related BOOLEAN DEFAULT FALSE,
    risk_keywords TEXT[],
    budget_amount DOUBLE PRECISION,
    budget_currency VARCHAR(20),
    votes_for DOUBLE PRECISION DEFAULT 0,
    votes_against DOUBLE PRECISION DEFAULT 0,
    votes_abstain DOUBLE PRECISION DEFAULT 0,
    voter_count INTEGER DEFAULT 0,
    participation_rate DOUBLE PRECISION,
    quorum_reached BOOLEAN,
    created_at TIMESTAMPTZ,
    start_at TIMESTAMPTZ,
    end_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_gov_proposals_unique
    ON governance_proposals(protocol_slug, proposal_id, source);
CREATE INDEX IF NOT EXISTS idx_gov_proposals_slug ON governance_proposals(protocol_slug);
CREATE INDEX IF NOT EXISTS idx_gov_proposals_risk ON governance_proposals(is_risk_related) WHERE is_risk_related = TRUE;
CREATE INDEX IF NOT EXISTS idx_gov_proposals_created ON governance_proposals(created_at DESC);

-- On-chain parameter change events
CREATE TABLE IF NOT EXISTS parameter_changes (
    id SERIAL PRIMARY KEY,
    protocol_slug VARCHAR(100) NOT NULL,
    tx_hash VARCHAR(66),
    block_number BIGINT,
    parameter_type VARCHAR(100),          -- e.g., 'interest_rate', 'collateral_factor', 'ltv'
    parameter_name VARCHAR(200),
    old_value TEXT,
    new_value TEXT,
    contract_address VARCHAR(42),
    function_signature VARCHAR(100),
    chain VARCHAR(50) DEFAULT 'ethereum',
    changed_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_param_changes_slug ON parameter_changes(protocol_slug);
CREATE INDEX IF NOT EXISTS idx_param_changes_changed ON parameter_changes(changed_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_param_changes_unique
    ON parameter_changes(protocol_slug, tx_hash) WHERE tx_hash IS NOT NULL;

-- Curated risk incident records
CREATE TABLE IF NOT EXISTS risk_incidents (
    id SERIAL PRIMARY KEY,
    protocol_slug VARCHAR(100) NOT NULL,
    incident_date DATE NOT NULL,
    title VARCHAR(500) NOT NULL,
    description TEXT,
    severity VARCHAR(20) NOT NULL,        -- 'critical', 'major', 'moderate', 'minor'
    severity_weight DOUBLE PRECISION DEFAULT 1.0,
    funds_at_risk_usd DOUBLE PRECISION DEFAULT 0,
    funds_recovered_usd DOUBLE PRECISION DEFAULT 0,
    recovery_ratio DOUBLE PRECISION,
    root_cause VARCHAR(200),
    source_url TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_risk_incidents_slug ON risk_incidents(protocol_slug);
CREATE INDEX IF NOT EXISTS idx_risk_incidents_date ON risk_incidents(incident_date DESC);
CREATE INDEX IF NOT EXISTS idx_risk_incidents_severity ON risk_incidents(severity);

INSERT INTO migrations (name) VALUES ('052_rpi_tables') ON CONFLICT DO NOTHING;

COMMIT;
