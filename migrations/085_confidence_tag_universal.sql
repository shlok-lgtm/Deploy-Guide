-- Migration 084: V7.3 Confidence Tag System — universal across all scored indices
--
-- The V7.3 confidence tag fields (confidence, confidence_tag, component_coverage,
-- components_populated, components_total, missing_categories) were only being
-- emitted at the API layer for SII and PSI, synthesized from other stored data.
-- RPI and the six Circle 7 indices (LSTI, BRI, DOHI, VSRI, CXRI, TTI) did not
-- surface these fields at all, so the ops rankings Coverage column rendered "—"
-- for those seven indices.
--
-- This migration adds the confidence columns to every score table so every
-- scorer can persist them uniformly, and the API can read them directly instead
-- of recomputing per request.
--
-- Scope: visibility only. No formula, weight, category, or promotion-gate
-- change. Circle 7 remains accruing. is_category_complete is untouched.

BEGIN;

-- SII (scores table, migration 001)
ALTER TABLE scores
    ADD COLUMN IF NOT EXISTS confidence TEXT,
    ADD COLUMN IF NOT EXISTS confidence_tag TEXT,
    ADD COLUMN IF NOT EXISTS component_coverage NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS components_populated INTEGER,
    ADD COLUMN IF NOT EXISTS components_total INTEGER,
    ADD COLUMN IF NOT EXISTS missing_categories JSONB;

-- PSI (psi_scores table, migration 017)
ALTER TABLE psi_scores
    ADD COLUMN IF NOT EXISTS confidence TEXT,
    ADD COLUMN IF NOT EXISTS confidence_tag TEXT,
    ADD COLUMN IF NOT EXISTS component_coverage NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS components_populated INTEGER,
    ADD COLUMN IF NOT EXISTS components_total INTEGER,
    ADD COLUMN IF NOT EXISTS missing_categories JSONB;

-- RPI (rpi_scores table, migration 052_rpi_tables — base components only;
-- lens variants remain ephemeral and out of scope for confidence persistence)
ALTER TABLE rpi_scores
    ADD COLUMN IF NOT EXISTS confidence TEXT,
    ADD COLUMN IF NOT EXISTS confidence_tag TEXT,
    ADD COLUMN IF NOT EXISTS component_coverage NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS components_populated INTEGER,
    ADD COLUMN IF NOT EXISTS components_total INTEGER,
    ADD COLUMN IF NOT EXISTS missing_categories JSONB;

-- Circle 7 shared table (generic_index_scores, migration 052_circle7_collectors)
-- confidence and confidence_tag already exist; add the remaining four.
ALTER TABLE generic_index_scores
    ADD COLUMN IF NOT EXISTS component_coverage NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS components_populated INTEGER,
    ADD COLUMN IF NOT EXISTS components_total INTEGER,
    ADD COLUMN IF NOT EXISTS missing_categories JSONB;

INSERT INTO migrations (name) VALUES ('085_confidence_tag_universal') ON CONFLICT DO NOTHING;

COMMIT;
