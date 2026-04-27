-- Migration 101: engine_events FK cascade fix
--
-- Original migration 098 declared:
--   engine_events.analysis_id UUID REFERENCES engine_analyses(id)
--   engine_events.artifact_id UUID REFERENCES engine_artifacts(id)
-- with no explicit ON DELETE clause, defaulting to NO ACTION (effectively
-- RESTRICT). That blocks any DELETE on engine_analyses or engine_artifacts
-- when a referencing event row exists — which trips the C4 test cleanup
-- path with:
--
--   psycopg2.errors.ForeignKeyViolation: update or delete on table
--   "engine_analyses" violates foreign key constraint
--   "engine_events_analysis_id_fkey" on table "engine_events"
--
-- Fix: switch both constraints to ON DELETE SET NULL. When an upstream row
-- is deleted, the engine_events row keeps its other fields (audit trail
-- preserved — source, entity, event_date, severity, raw_event_data, etc.)
-- but the FK column becomes NULL. Maintenance ops + test cleanup unblocked.
--
-- Idempotency: each ALTER is wrapped in a DO block that checks pg_constraint
-- before dropping. Re-running the migration is a no-op once applied.
-- ============================================================================

BEGIN;

-- engine_events.analysis_id → engine_analyses(id)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'engine_events_analysis_id_fkey'
          AND conrelid = 'engine_events'::regclass
    ) THEN
        ALTER TABLE engine_events
            DROP CONSTRAINT engine_events_analysis_id_fkey;
    END IF;

    ALTER TABLE engine_events
        ADD CONSTRAINT engine_events_analysis_id_fkey
            FOREIGN KEY (analysis_id)
            REFERENCES engine_analyses(id)
            ON DELETE SET NULL;
END $$;

-- engine_events.artifact_id → engine_artifacts(id)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'engine_events_artifact_id_fkey'
          AND conrelid = 'engine_events'::regclass
    ) THEN
        ALTER TABLE engine_events
            DROP CONSTRAINT engine_events_artifact_id_fkey;
    END IF;

    ALTER TABLE engine_events
        ADD CONSTRAINT engine_events_artifact_id_fkey
            FOREIGN KEY (artifact_id)
            REFERENCES engine_artifacts(id)
            ON DELETE SET NULL;
END $$;

INSERT INTO migrations (name) VALUES ('101_engine_events_fk_cascade')
    ON CONFLICT DO NOTHING;

COMMIT;

-- Verify with:
--   SELECT conname, confrelid::regclass, confdeltype
--   FROM pg_constraint
--   WHERE conname LIKE 'engine_events%fkey';
-- Expected: confdeltype = 'n' (SET NULL) for both rows.
