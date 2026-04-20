-- Migration 079: Track Record Entries
-- Auto-logged and manually curated risk calls with frozen baseline state

CREATE TABLE IF NOT EXISTS track_record_entries (
    entry_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entry_type TEXT NOT NULL CHECK (entry_type IN ('auto', 'featured')),
    entity_slug TEXT NOT NULL,
    index_name TEXT NOT NULL,
    trigger_kind TEXT NOT NULL CHECK (trigger_kind IN (
        'score_change', 'divergence', 'coherence_drop',
        'oracle_stress', 'governance_edit', 'contract_upgrade',
        'manual'
    )),
    trigger_detail JSONB NOT NULL,
    triggered_at TIMESTAMPTZ NOT NULL,
    state_root_at_trigger TEXT,
    source_attestation_domain TEXT,
    baseline_snapshot JSONB NOT NULL,
    narrative_markdown TEXT,
    featured BOOLEAN DEFAULT FALSE,
    featured_by TEXT,
    featured_at TIMESTAMPTZ,
    content_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tr_entries_entity
    ON track_record_entries(entity_slug, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_tr_entries_trigger
    ON track_record_entries(trigger_kind, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_tr_entries_featured
    ON track_record_entries(featured, triggered_at DESC)
    WHERE featured = TRUE;
CREATE UNIQUE INDEX IF NOT EXISTS idx_tr_entries_hash
    ON track_record_entries(content_hash);

INSERT INTO migrations (name) VALUES ('079_track_record_entries') ON CONFLICT DO NOTHING;
