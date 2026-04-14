-- Migration 066: Enforcement History (Pipeline 20)
-- CourtListener and SEC EDGAR enforcement record archive.

CREATE TABLE IF NOT EXISTS enforcement_records (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(20),
    entity_id INTEGER,
    entity_symbol VARCHAR(20),
    search_term VARCHAR(200),
    record_source VARCHAR(50),
    case_name VARCHAR(500),
    case_date DATE,
    court VARCHAR(200),
    docket_number VARCHAR(100),
    record_type VARCHAR(50),
    summary TEXT,
    case_url VARCHAR(500),
    absolute_url VARCHAR(500),
    is_relevant BOOLEAN,
    relevance_notes TEXT,
    discovered_at TIMESTAMPTZ DEFAULT NOW(),
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ,
    UNIQUE (docket_number, record_source)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enforcement_entity
    ON enforcement_records (entity_type, entity_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enforcement_case_date
    ON enforcement_records (case_date DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enforcement_unreviewed
    ON enforcement_records (is_relevant) WHERE is_relevant IS NULL;


INSERT INTO migrations (name) VALUES ('066_enforcement_history') ON CONFLICT DO NOTHING;
