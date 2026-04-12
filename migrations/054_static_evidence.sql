-- Static Component Evidence
-- Stores provenance evidence for manually assessed static component values.
-- Each row records: what value was used, where it came from, the content hash
-- of the captured evidence, and paths to stored artifacts (snapshots, screenshots, proofs).

CREATE TABLE IF NOT EXISTS static_evidence (
    id SERIAL PRIMARY KEY,
    index_id TEXT NOT NULL,
    entity_slug TEXT NOT NULL,
    component_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_section TEXT,
    captured_value TEXT NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    proof_r2_path TEXT,
    screenshot_r2_path TEXT,
    snapshot_r2_path TEXT,
    extracted_text TEXT,
    captured_at TIMESTAMPTZ NOT NULL,
    stale_detected_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_static_evidence_lookup
    ON static_evidence(index_id, entity_slug, component_name);

CREATE INDEX IF NOT EXISTS idx_static_evidence_staleness
    ON static_evidence(stale_detected_at)
    WHERE stale_detected_at IS NOT NULL;

-- Track migration
INSERT INTO migrations (name, applied_at)
VALUES ('054_static_evidence', NOW())
ON CONFLICT DO NOTHING;
