-- Migration 111: writer_id column for state_attestations (per #235 Option A).
--
-- Background
-- ----------
-- Issue #235 Problem 2: state_attestations rows are written by multiple
-- code paths (module-canonical wrappers, dispatcher heartbeat in
-- worker.py:2650, inline cycle work). Today every writer lands in the
-- same `domain` bucket with no provenance discriminator — distinguishing
-- "heartbeat wrote this" vs "wrapper wrote this" requires parsing
-- payloads row-by-row, which is not query-friendly.
--
-- A5's recommendation on #235 selected Option A: add a `writer_id`
-- VARCHAR(64) column. This unblocks Phase 2.3 staged heartbeat
-- dissolution by making "wrapper is firing independently for 24-48h"
-- a single-query check.
--
-- Per the v9.12 W2.1/W2.2/W2.3/W2.4 split:
--   W2.1 (this PR)  — add column NULL-able, thread kwarg through
--                     attest_state() / attest_data_batch(). New rows
--                     default NULL until W2.2 fills them in.
--   W2.2            — ~20-25 call-site updates to populate
--                     writer_id labels (e.g. "module.peg_monitor",
--                     "heartbeat.slow_cycle", "worker.inline.psi_components").
--   W2.3            — PR template + migration plan doc update to
--                     reference the discriminator.
--   W2.4            — CREATE INDEX CONCURRENTLY in a separate
--                     migration once writer_id has been populated and
--                     soaked for 24h.
--
-- Why this is safe on prod (PG 17, no lock)
-- -----------------------------------------
-- ADD COLUMN ... NULL with no DEFAULT is a metadata-only change on
-- PostgreSQL 11+. The table is NOT rewritten; only pg_attribute is
-- updated. The lock taken is AccessExclusive but is released in
-- microseconds because there is no per-row work.
--
-- Reference: PostgreSQL 17 docs, "Notes" on ALTER TABLE ADD COLUMN —
-- "When you add a column to the table, ... if no default is specified,
-- NULL is used. In neither case is a rewrite of the table required."
--
-- Idempotent: IF NOT EXISTS gate. Safe to re-run.

ALTER TABLE state_attestations
  ADD COLUMN IF NOT EXISTS writer_id VARCHAR(64) NULL;

COMMENT ON COLUMN state_attestations.writer_id IS
  'Writer provenance label per #235 Option A. Examples: module.peg_monitor, heartbeat.slow_cycle, worker.inline.psi_components, enrichment.<task>. NULL acceptable for legacy rows and writers that have not yet been labeled (W2.2 will fill them in).';

INSERT INTO migrations (name) VALUES ('111_state_attestations_writer_id') ON CONFLICT DO NOTHING;
