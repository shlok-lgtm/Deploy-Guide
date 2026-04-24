-- Migration 098: Analytic Engine core tables
--
-- Creates the six tables required by the analytic engine. This migration
-- is additive — no existing tables are modified. See
-- docs/analytic_engine_step_0_v0.2.md §1 (schema) and §3 (LLM integration)
-- for the full contract.
--
-- Tables:
--   engine_analyses              — every analysis the engine produces
--   engine_artifacts             — rendered artifacts linked to an analysis
--   engine_events                — events detected by Component 4 (future)
--   engine_watchlist             — entities monitored by Component 4 (future)
--   engine_prompts               — prompt version registry (Component 2 use)
--   engine_interpretation_cache  — LLM output cache (Component 2 use)
--
-- Only engine_analyses and engine_artifacts are exercised by Component 1
-- (this session). The remaining tables land now for schema consistency so
-- later sessions don't bundle them in separate migrations.
--
-- Constraints of note:
--   - engine_analyses UNIQUE(entity, event_date) WHERE status != 'archived'
--     enforces "one active analysis per (entity, event_date)". force_new=true
--     on /analyze archives the existing row before inserting the new one.
--   - engine_events UNIQUE(source, entity, event_date, event_type) enforces
--     idempotency: the same detected event never creates duplicate rows.
--
-- Note on migration number: the v0.2a doc reserved 088; the S0 prompt
-- specifies 098 to leave room for other work-branches that may land
-- migrations between now and this commit. Taking 098 as explicitly
-- instructed. If 088 remains free at the time this is applied, that's fine.
-- ============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- engine_analyses
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS engine_analyses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  analysis_version TEXT NOT NULL,
  entity TEXT NOT NULL,
  event_date DATE,
  peer_set JSONB NOT NULL DEFAULT '[]'::jsonb,
  context TEXT,
  coverage JSONB NOT NULL,
  signal JSONB NOT NULL,
  interpretation JSONB NOT NULL,
  methodology_observations JSONB NOT NULL DEFAULT '[]'::jsonb,
  follow_ups JSONB NOT NULL DEFAULT '[]'::jsonb,
  artifact_recommendation JSONB NOT NULL,
  inputs_hash TEXT NOT NULL,
  previous_analysis_id UUID REFERENCES engine_analyses(id),
  superseded_by_id UUID REFERENCES engine_analyses(id),
  supersedes_reason TEXT,
  archived_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'draft',
  human_reviewer TEXT,
  review_notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_engine_analyses_entity
  ON engine_analyses(entity);
CREATE INDEX IF NOT EXISTS idx_engine_analyses_created
  ON engine_analyses(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_engine_analyses_status
  ON engine_analyses(status);

-- One active analysis per (entity, event_date). Archived rows are exempt so
-- force_new=true can archive the prior row and insert a new one atomically.
CREATE UNIQUE INDEX IF NOT EXISTS idx_engine_analyses_active_unique
  ON engine_analyses(entity, event_date)
  WHERE status <> 'archived';

-- ---------------------------------------------------------------------------
-- engine_artifacts
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS engine_artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  analysis_id UUID NOT NULL REFERENCES engine_analyses(id),
  artifact_type TEXT NOT NULL,
  rendered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  content_markdown TEXT NOT NULL,
  suggested_path TEXT,
  suggested_url TEXT,
  status TEXT NOT NULL DEFAULT 'draft',
  published_url TEXT,
  warnings JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_engine_artifacts_analysis
  ON engine_artifacts(analysis_id);
CREATE INDEX IF NOT EXISTS idx_engine_artifacts_type
  ON engine_artifacts(artifact_type);
CREATE INDEX IF NOT EXISTS idx_engine_artifacts_status
  ON engine_artifacts(status);

-- ---------------------------------------------------------------------------
-- engine_events
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS engine_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source TEXT NOT NULL,
  event_type TEXT NOT NULL,
  entity TEXT NOT NULL,
  event_date DATE,
  severity TEXT,
  raw_event_data JSONB NOT NULL,
  analysis_id UUID REFERENCES engine_analyses(id),
  artifact_id UUID REFERENCES engine_artifacts(id),
  status TEXT NOT NULL DEFAULT 'new',
  delivered_at TIMESTAMPTZ,
  operator_response TEXT
);

CREATE INDEX IF NOT EXISTS idx_engine_events_status
  ON engine_events(status);
CREATE INDEX IF NOT EXISTS idx_engine_events_detected
  ON engine_events(detected_at DESC);

-- Idempotency: the same event from the same source never duplicates.
CREATE UNIQUE INDEX IF NOT EXISTS idx_engine_events_idempotency
  ON engine_events(source, entity, event_date, event_type);

-- ---------------------------------------------------------------------------
-- engine_watchlist
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS engine_watchlist (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_slug TEXT NOT NULL,
  index_id TEXT,
  threshold_type TEXT NOT NULL,
  threshold_value NUMERIC NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_engine_watchlist_active
  ON engine_watchlist(active) WHERE active = TRUE;

-- ---------------------------------------------------------------------------
-- engine_prompts  (Component 2 use; created now for schema consistency)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS engine_prompts (
  version TEXT PRIMARY KEY,
  file_path TEXT NOT NULL,
  activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deprecated_at TIMESTAMPTZ,
  sha256 TEXT NOT NULL,
  notes TEXT
);

-- ---------------------------------------------------------------------------
-- engine_interpretation_cache  (Component 2 use; created now for schema consistency)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS engine_interpretation_cache (
  input_hash TEXT PRIMARY KEY,
  interpretation_json JSONB NOT NULL,
  prompt_version TEXT NOT NULL REFERENCES engine_prompts(version),
  model_id TEXT NOT NULL,
  token_input_count INT,
  token_output_count INT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  hit_count INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_interpretation_cache_prompt
  ON engine_interpretation_cache(prompt_version);

-- ---------------------------------------------------------------------------
-- Migration registry
-- ---------------------------------------------------------------------------

INSERT INTO migrations (name) VALUES ('098_engine_tables')
  ON CONFLICT DO NOTHING;

COMMIT;
