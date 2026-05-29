-- Migration 109: engine_artifacts FK cascade fix
--
-- Companion to migration 101 (engine_events FK cascade).
--
-- Migration 098 declared:
--   engine_artifacts.analysis_id UUID NOT NULL REFERENCES engine_analyses(id)
-- with no explicit ON DELETE clause, so the FK defaults to NO ACTION
-- (confdeltype 'a'). Migration 101 fixed the same defect on
-- engine_events.analysis_id but used SET NULL — semantically correct
-- there because engine_events rows have independent existence
-- (DeFiLlama poller writes them whether or not an analysis ever runs).
--
-- engine_artifacts is different: an artifact has no independent
-- existence apart from the analysis it was rendered from. When an
-- analysis is deleted (test teardown, manual cleanup), the rendered
-- artifact is orphan output and should be deleted with it. CASCADE
-- is the correct semantics.
--
-- Without this, C5 lifecycle test teardown trips:
--   psycopg2.errors.ForeignKeyViolation: update or delete on table
--   "engine_analyses" violates foreign key constraint
--   "engine_artifacts_analysis_id_fkey" on table "engine_artifacts"
-- which keeps the full 77-test suite at 72-74 passing despite C5
-- workflow tests being 12/12 in isolation.
--
-- Idempotency: DO block checks pg_constraint before the DROP. Re-running
-- after apply is a no-op (the DROP path skips, the ADD re-asserts the
-- same definition that already exists).
-- ============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'engine_artifacts_analysis_id_fkey'
          AND conrelid = 'engine_artifacts'::regclass
    ) THEN
        ALTER TABLE engine_artifacts
            DROP CONSTRAINT engine_artifacts_analysis_id_fkey;
    END IF;

    ALTER TABLE engine_artifacts
        ADD CONSTRAINT engine_artifacts_analysis_id_fkey
            FOREIGN KEY (analysis_id)
            REFERENCES engine_analyses(id)
            ON DELETE CASCADE;
END $$;

INSERT INTO migrations (name) VALUES ('109_engine_artifacts_fk_cascade')
    ON CONFLICT DO NOTHING;

-- Verify with:
--   SELECT conname, confdeltype, pg_get_constraintdef(oid)
--   FROM pg_constraint
--   WHERE conname = 'engine_artifacts_analysis_id_fkey';
-- Expected: confdeltype = 'c' (CASCADE).
