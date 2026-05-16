-- Migration 112: index for writer_id (per #235 Option A, W2.4).
--
-- Plain CREATE INDEX (NOT CONCURRENTLY) because:
--   (a) state_attestations is ~3,100 rows; AccessShare lock < 1s.
--   (b) app/database.py:280-293 wraps every migration in a
--       transaction via `with conn:`. CONCURRENTLY requires no
--       transaction and would error. Worse, main.py:769-776
--       silently records the migration as applied on failure,
--       leaving the index missing and unretryable. (Wave-N entry
--       in migration plan doc.)
--
-- Partial index: WHERE writer_id IS NOT NULL excludes legacy
-- pre-W2.2 NULL rows, reducing storage overhead.
--
-- Idempotent: IF NOT EXISTS gate. Safe to re-run.

CREATE INDEX IF NOT EXISTS idx_state_attestations_writer_id
  ON state_attestations (writer_id)
  WHERE writer_id IS NOT NULL;

INSERT INTO migrations (name) VALUES ('112_state_attestations_writer_id_index') ON CONFLICT DO NOTHING;
