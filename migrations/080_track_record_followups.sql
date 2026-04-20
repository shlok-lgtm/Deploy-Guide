-- Migration 080: Track Record Follow-ups
-- Automated 30/60/90-day outcome checks against frozen baselines

CREATE TABLE IF NOT EXISTS track_record_followups (
    followup_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entry_id UUID NOT NULL REFERENCES track_record_entries(entry_id) ON DELETE CASCADE,
    checkpoint TEXT NOT NULL CHECK (checkpoint IN ('30d', '60d', '90d')),
    evaluated_at TIMESTAMPTZ NOT NULL,
    current_snapshot JSONB NOT NULL,
    outcome_category TEXT NOT NULL CHECK (outcome_category IN (
        'validated', 'mixed', 'not_borne_out', 'insufficient_data'
    )),
    outcome_detail JSONB NOT NULL,
    narrative_markdown TEXT,
    content_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (entry_id, checkpoint)
);

CREATE INDEX IF NOT EXISTS idx_tr_followups_entry
    ON track_record_followups(entry_id);
CREATE INDEX IF NOT EXISTS idx_tr_followups_pending
    ON track_record_followups(evaluated_at DESC);

INSERT INTO migrations (name) VALUES ('080_track_record_followups') ON CONFLICT DO NOTHING;
