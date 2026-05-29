-- Migration 112: Publication gate for SII and PSI entities.
--
-- Background
-- ----------
-- Phase 0 of the SII/PSI auto-discovery investigation (May 27, branch
-- claude/adoring-franklin-HuNxw) found that the deck-claimed policy
-- "discover, score, add to coverage automatically" is unenforceable
-- today because no visibility flag exists on the serving path. 82
-- distinct read sites across server.py / report.py / pulse_generator.py
-- / divergence.py / ops/entity_views.py serve scores purely by
-- "row exists in scored table" — auto-discovery without a publication
-- gate would leak unapproved entities to every public surface on the
-- first cycle.
--
-- This migration adds the load-bearing piece. Discovery PRs (Phase 2
-- for PSI, Phase 3 for SII) layer on top of this gate; without it they
-- cannot ship.
--
-- Architect-decided policy (May 27): the automation boundary is
-- PUBLICATION, not scoring. Newly-discovered entities are scored and
-- generate score history immediately, but carry published=FALSE and
-- must not appear in any user-facing surface until an operator flips
-- the flag.
--
-- Schema changes
-- --------------
-- 1. stablecoins.published BOOLEAN NOT NULL DEFAULT FALSE
--    Source of truth for SII visibility. Default FALSE so any
--    insert path (auto-discovery in Phase 3, or anything else that
--    INSERTs into stablecoins) defaults to invisible.
--
-- 2. CREATE TABLE protocol_publication_state
--    PSI's serving path joins psi_scores against this table. The
--    publication bit belongs to the entity (one row per protocol),
--    not the daily score row — psi_scores is per-day and re-derived,
--    while publication is an editorial decision that should persist
--    across re-scores.
--
-- Backfill
-- --------
-- Every currently-scored entity is marked published=TRUE so the
-- existing public surface is unchanged the moment this lands. New
-- inserts (from any path — discovery, manual seed, ad-hoc) default
-- to FALSE.
--
-- Why this is safe on prod (PG 17)
-- ---------------------------------
-- ADD COLUMN ... NOT NULL DEFAULT FALSE requires a rewrite on
-- PostgreSQL < 11, but PG 11+ stores the default in pg_attribute and
-- skips the rewrite. The lock taken is AccessExclusive but is
-- released in microseconds because there is no per-row work.
--
-- The UPDATE on stablecoins backfilling existing rows is per-row work
-- but the table is small (~36 rows currently) — completes in <1ms.
--
-- protocol_publication_state is a new empty table; no contention.
--
-- Idempotent: IF NOT EXISTS gates. Safe to re-run.

BEGIN;

ALTER TABLE stablecoins
  ADD COLUMN IF NOT EXISTS published BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN stablecoins.published IS
  'Publication gate (migration 112). FALSE = entity exists and may be scored, but is invisible to every public read path. Auto-discovery defaults FALSE. Operator approval (POST /api/admin/publish/sii/{slug} or SQL) flips TRUE. See app/publication_gate.py for the read-path enforcement.';

-- Backfill existing entities. Every row already in stablecoins at the
-- time of this migration is treated as architect-approved (it was
-- manually added before the gate existed).
UPDATE stablecoins SET published = TRUE WHERE published IS NOT TRUE;

CREATE TABLE IF NOT EXISTS protocol_publication_state (
    protocol_slug TEXT PRIMARY KEY,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    published_at TIMESTAMPTZ,
    published_by TEXT,
    unpublished_at TIMESTAMPTZ,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE protocol_publication_state IS
  'PSI publication gate (migration 112). One row per protocol. Joined into every public PSI read path via the psi_scores_published view. Default FALSE means a freshly-promoted protocol from psi_collector.discover_protocols() is scored but invisible until approved.';

-- Backfill existing PSI protocols. Any protocol with at least one row
-- in psi_scores at the time of this migration is treated as
-- architect-approved.
INSERT INTO protocol_publication_state (protocol_slug, published, published_at, published_by, notes)
SELECT DISTINCT protocol_slug, TRUE, NOW(), 'migration_112_backfill',
       'Pre-gate entity backfilled to published. Existed in psi_scores before the publication gate landed.'
FROM psi_scores
ON CONFLICT (protocol_slug) DO NOTHING;

-- Helper views. Public read paths SELECT from these instead of the
-- raw tables. A new (unpublished) row in stablecoins or psi_scores
-- is automatically excluded; flipping the gate makes it visible on
-- the next request.
--
-- stablecoins_published is a thin filter — public endpoints join
-- this in place of `stablecoins`.
CREATE OR REPLACE VIEW stablecoins_published AS
SELECT *
FROM stablecoins
WHERE published = TRUE;

COMMENT ON VIEW stablecoins_published IS
  'Publication-gated view of stablecoins (migration 112). Use this in every public read path. Ops/admin routes that need to see unpublished entities for approval continue to SELECT from `stablecoins` directly.';

-- psi_scores_published joins per-day score rows against the
-- per-entity publication state. DISTINCT ON is left to the caller
-- because some endpoints want the latest score, some want history,
-- some want a specific date.
CREATE OR REPLACE VIEW psi_scores_published AS
SELECT ps.*
FROM psi_scores ps
JOIN protocol_publication_state pps
  ON pps.protocol_slug = ps.protocol_slug
WHERE pps.published = TRUE;

COMMENT ON VIEW psi_scores_published IS
  'Publication-gated view of psi_scores (migration 112). Joins against protocol_publication_state. Use this in every public PSI read path. Ops/admin routes that need to see unpublished entities continue to SELECT from `psi_scores` directly.';

CREATE INDEX IF NOT EXISTS idx_pps_published
  ON protocol_publication_state(published)
  WHERE published = TRUE;

INSERT INTO migrations (name) VALUES ('112_publication_gate') ON CONFLICT DO NOTHING;

COMMIT;
