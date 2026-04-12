-- Migration 054: Static evidence — Firecrawl screenshots + markdown snapshots
-- Stores per-component evidence captures for the Witness page.
-- screenshot_data and snapshot_content hold the actual bytes/text in Postgres;
-- screenshot_r2_path and snapshot_r2_path hold the planned R2 object keys
-- (populated when R2 upload is configured).

BEGIN;

CREATE TABLE IF NOT EXISTS static_evidence (
    id SERIAL PRIMARY KEY,
    source_url TEXT NOT NULL,
    entity_slug TEXT NOT NULL,          -- e.g. 'wormhole', 'usdc'
    component_slug TEXT NOT NULL,       -- e.g. 'guardian_set', 'reserves'
    index_id TEXT,                      -- e.g. 'bri', 'sii'

    -- Screenshot (rendered PNG via Firecrawl)
    screenshot_r2_path TEXT,            -- R2 object key when uploaded
    screenshot_data BYTEA,              -- raw PNG bytes (DB fallback)

    -- Markdown snapshot (clean page content via Firecrawl)
    snapshot_r2_path TEXT,              -- R2 object key when uploaded
    snapshot_content TEXT,              -- markdown text

    -- Integrity
    content_hash VARCHAR(64),           -- SHA-256(screenshot_bytes + markdown)

    -- Metadata
    capture_method TEXT DEFAULT 'firecrawl',
    captured_at TIMESTAMPTZ DEFAULT NOW(),
    is_stale BOOLEAN DEFAULT FALSE,
    stale_since TIMESTAMPTZ,
    previous_content_hash VARCHAR(64),  -- for change detection

    UNIQUE(source_url, entity_slug, component_slug)
);

-- Fast lookup by entity (witness page)
CREATE INDEX IF NOT EXISTS idx_static_evidence_entity
    ON static_evidence(entity_slug, component_slug);

-- Staleness scan
CREATE INDEX IF NOT EXISTS idx_static_evidence_stale
    ON static_evidence(is_stale, captured_at)
    WHERE is_stale = TRUE;

-- Deduplication: find all components sharing a URL
CREATE INDEX IF NOT EXISTS idx_static_evidence_url
    ON static_evidence(source_url);

INSERT INTO migrations (name) VALUES ('054_static_evidence') ON CONFLICT DO NOTHING;

COMMIT;
