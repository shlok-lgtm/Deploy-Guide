-- Migration 052: Circle 7 Collectors — new index tables
-- Creates tables for governance events, generic index scores,
-- and all Circle 7 index collectors (LSTI, BRI, DOHI, VSRI, CXRI, TTI).

-- ============================================================================
-- Governance Events table (Prompt 7 — RPI delta)
-- ============================================================================

CREATE TABLE IF NOT EXISTS governance_events (
    id SERIAL PRIMARY KEY,
    protocol_slug TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    title TEXT,
    description TEXT,
    outcome TEXT,                -- passed, defeated, executed, cancelled, active
    contributor_tag TEXT,        -- nullable, for attribution (e.g. "chaos-labs")
    source TEXT NOT NULL,        -- snapshot, tally, manual
    source_id TEXT,              -- proposal ID from source
    metadata JSONB,              -- full proposal data if needed
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gov_events_protocol ON governance_events (protocol_slug);
CREATE INDEX IF NOT EXISTS idx_gov_events_timestamp ON governance_events (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_gov_events_contributor ON governance_events (contributor_tag) WHERE contributor_tag IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_gov_events_source_uniq ON governance_events (source, source_id);

-- ============================================================================
-- Generic Index Scores table — unified storage for all Circle 7 indices
-- ============================================================================
-- All new indices (LSTI, BRI, DOHI, VSRI, CXRI, TTI) share this table.
-- Follows the same pattern as psi_scores but generalized.

CREATE TABLE IF NOT EXISTS generic_index_scores (
    id SERIAL PRIMARY KEY,
    index_id TEXT NOT NULL,         -- lsti, bri, dohi, vsri, cxri, tti
    entity_slug TEXT NOT NULL,
    entity_name TEXT,
    overall_score NUMERIC(6,2),
    category_scores JSONB,
    component_scores JSONB,
    raw_values JSONB,
    formula_version TEXT,
    inputs_hash TEXT,
    confidence TEXT DEFAULT 'limited',
    confidence_tag TEXT,
    scored_date DATE DEFAULT CURRENT_DATE,
    computed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_gis_index_entity_date
    ON generic_index_scores (index_id, entity_slug, scored_date);
CREATE INDEX IF NOT EXISTS idx_gis_index_id ON generic_index_scores (index_id);
CREATE INDEX IF NOT EXISTS idx_gis_entity ON generic_index_scores (entity_slug);
CREATE INDEX IF NOT EXISTS idx_gis_computed ON generic_index_scores (computed_at);
