-- Migration 084 — additive aggregation-formula columns on score tables.
--
-- Adds the columns needed to persist which aggregation formula produced each
-- score and under what parameters. Historical rows keep NULL / FALSE values —
-- never backfilled; legacy_renormalize is assumed for any score_row where
-- aggregation_method is NULL.
--
-- Scope: scores (SII), psi_scores, rpi_scores, generic_index_scores. RQS is
-- computed on-demand and not persisted in its own table; its threshold is
-- carried in the API response only, so no RQS migration is needed here.
--
-- See:
--   - app/composition.py (AGGREGATION_FORMULAS, aggregate())
--   - app/index_definitions/schema.py (aggregation block docs)
--   - docs/methodology/aggregation_impact_analysis.md (TBD — populates first run)

-- ---------------------------------------------------------------------------
-- scores (SII)
-- ---------------------------------------------------------------------------
ALTER TABLE scores
    ADD COLUMN IF NOT EXISTS aggregation_method TEXT,
    ADD COLUMN IF NOT EXISTS aggregation_params JSONB,
    ADD COLUMN IF NOT EXISTS aggregation_formula_version TEXT,
    ADD COLUMN IF NOT EXISTS effective_category_weights JSONB,
    ADD COLUMN IF NOT EXISTS coverage NUMERIC,
    ADD COLUMN IF NOT EXISTS withheld BOOLEAN NOT NULL DEFAULT FALSE;

-- ---------------------------------------------------------------------------
-- psi_scores
-- ---------------------------------------------------------------------------
ALTER TABLE psi_scores
    ADD COLUMN IF NOT EXISTS aggregation_method TEXT,
    ADD COLUMN IF NOT EXISTS aggregation_params JSONB,
    ADD COLUMN IF NOT EXISTS aggregation_formula_version TEXT,
    ADD COLUMN IF NOT EXISTS effective_category_weights JSONB,
    ADD COLUMN IF NOT EXISTS coverage NUMERIC,
    ADD COLUMN IF NOT EXISTS withheld BOOLEAN NOT NULL DEFAULT FALSE;

-- ---------------------------------------------------------------------------
-- rpi_scores
-- ---------------------------------------------------------------------------
ALTER TABLE rpi_scores
    ADD COLUMN IF NOT EXISTS aggregation_method TEXT,
    ADD COLUMN IF NOT EXISTS aggregation_params JSONB,
    ADD COLUMN IF NOT EXISTS aggregation_formula_version TEXT,
    ADD COLUMN IF NOT EXISTS effective_category_weights JSONB,
    ADD COLUMN IF NOT EXISTS coverage NUMERIC,
    ADD COLUMN IF NOT EXISTS withheld BOOLEAN NOT NULL DEFAULT FALSE;

-- ---------------------------------------------------------------------------
-- generic_index_scores (LSTI, BRI, DOHI, VSRI, CXRI, TTI — 6 accruing indices)
-- ---------------------------------------------------------------------------
ALTER TABLE generic_index_scores
    ADD COLUMN IF NOT EXISTS aggregation_method TEXT,
    ADD COLUMN IF NOT EXISTS aggregation_params JSONB,
    ADD COLUMN IF NOT EXISTS aggregation_formula_version TEXT,
    ADD COLUMN IF NOT EXISTS effective_category_weights JSONB,
    ADD COLUMN IF NOT EXISTS coverage NUMERIC,
    ADD COLUMN IF NOT EXISTS withheld BOOLEAN NOT NULL DEFAULT FALSE;
