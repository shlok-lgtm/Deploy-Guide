-- Migration 052: Data source registry for provenance auto-discovery
-- Every collector registers the external HTTP calls it makes.
-- The provenance service reads this registry and proves whatever's in it.

CREATE TABLE IF NOT EXISTS data_source_registry (
    id SERIAL PRIMARY KEY,
    source_domain TEXT NOT NULL,
    source_endpoint TEXT NOT NULL,
    method TEXT DEFAULT 'GET',
    description TEXT,
    collector TEXT NOT NULL,
    params_template JSONB,
    response_size_estimate INTEGER,
    first_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    prove BOOLEAN DEFAULT TRUE,
    prove_frequency TEXT DEFAULT 'hourly',
    notes TEXT,
    UNIQUE(source_domain, source_endpoint, method)
);

CREATE INDEX IF NOT EXISTS idx_dsr_domain ON data_source_registry(source_domain);
CREATE INDEX IF NOT EXISTS idx_dsr_prove ON data_source_registry(prove);
CREATE INDEX IF NOT EXISTS idx_dsr_collector ON data_source_registry(collector);
