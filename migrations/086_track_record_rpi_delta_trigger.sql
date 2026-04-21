-- Migration 086: Add rpi_delta to track_record_entries trigger_kind CHECK constraint

-- Drop the existing CHECK constraint on trigger_kind and recreate with rpi_delta added
ALTER TABLE track_record_entries DROP CONSTRAINT IF EXISTS track_record_entries_trigger_kind_check;

ALTER TABLE track_record_entries ADD CONSTRAINT track_record_entries_trigger_kind_check
    CHECK (trigger_kind IN (
        'score_change', 'divergence', 'coherence_drop',
        'oracle_stress', 'governance_edit', 'contract_upgrade',
        'rpi_delta', 'manual'
    ));

INSERT INTO migrations (name) VALUES ('086_track_record_rpi_delta_trigger') ON CONFLICT DO NOTHING;
