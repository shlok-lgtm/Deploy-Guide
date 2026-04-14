-- Migration 070: Contract Dependency Graph (Pipeline 6)
-- Maps which external contracts each scored protocol calls, versioned over time.
-- Pre-exploit dependency graph is critical forensic record.

CREATE TABLE IF NOT EXISTS contract_dependencies (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(20) NOT NULL,
    entity_id INTEGER NOT NULL,
    entity_slug VARCHAR(100),
    source_contract VARCHAR(42) NOT NULL,
    source_chain VARCHAR(20) NOT NULL,
    depends_on_address VARCHAR(42) NOT NULL,
    depends_on_chain VARCHAR(20) NOT NULL,
    depends_on_label VARCHAR(200),
    depends_on_type VARCHAR(50),
    call_type VARCHAR(50),
    detected_via VARCHAR(50),
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_confirmed_at TIMESTAMPTZ DEFAULT NOW(),
    removed_at TIMESTAMPTZ,
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ,
    UNIQUE (source_contract, source_chain, depends_on_address, depends_on_chain)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_deps_entity
    ON contract_dependencies (entity_type, entity_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_deps_reverse
    ON contract_dependencies (depends_on_address);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_deps_first_seen
    ON contract_dependencies (first_seen_at DESC);


CREATE TABLE IF NOT EXISTS dependency_graph_snapshots (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(20),
    entity_id INTEGER,
    entity_slug VARCHAR(100),
    snapshot_date DATE NOT NULL,
    dependency_count INTEGER,
    dependency_addresses JSONB,
    dependency_hashes JSONB,
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ,
    UNIQUE (entity_type, entity_id, snapshot_date)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dep_snapshots_date
    ON dependency_graph_snapshots (snapshot_date DESC);


INSERT INTO migrations (name) VALUES ('070_contract_dependency_graph') ON CONFLICT DO NOTHING;
