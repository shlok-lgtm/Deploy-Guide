-- Migration 107: Widen attestation-related columns to TEXT
--
-- 2026-05-11 silent data loss class:
--   state_attestations.domain VARCHAR(30) was truncating three
--   collector-generated domain names that exceed 30 chars:
--     data_layer:entity_snapshots_hourly (34)
--     data_layer:market_chart_history (31)
--     data_layer:wallet_chain_presence (32)
--   2 silent `data_layer_attest_data_batch_failure` "value too long"
--   errors in last 24h (more historically).
--
-- discovery_signals.domain shares the same 30-char limit. Widening
-- preventively to avoid the same trap when discovery domains get
-- longer names.
--
-- methodology_version VARCHAR(20) across 8 attestation tables is also
-- restrictive — current values fit ("v1.0.0", "mempool-v0.1.0") but
-- future versioning schemes (e.g. semantic-version + suffix) could
-- exceed it. Widen now while the migration is cheap.
--
-- Per v9.10 amendment: ALTER COLUMN TYPE is safe on the pooler
-- endpoint (no advisory locks, no session state required). Postgres
-- TEXT and VARCHAR(n) share identical storage — the type change is
-- O(1) in metadata, no table rewrite needed.
--
-- Idempotent: each ALTER is gated by an information_schema check so
-- re-running is a no-op.

DO $$
BEGIN
    -- state_attestations.domain
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'state_attestations'
          AND column_name = 'domain'
          AND data_type = 'character varying'
    ) THEN
        ALTER TABLE state_attestations ALTER COLUMN domain TYPE TEXT;
    END IF;

    -- state_attestations.methodology_version
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'state_attestations'
          AND column_name = 'methodology_version'
          AND data_type = 'character varying'
    ) THEN
        ALTER TABLE state_attestations ALTER COLUMN methodology_version TYPE TEXT;
    END IF;

    -- discovery_signals.domain
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'discovery_signals'
          AND column_name = 'domain'
          AND data_type = 'character varying'
    ) THEN
        ALTER TABLE discovery_signals ALTER COLUMN domain TYPE TEXT;
    END IF;

    -- discovery_signals.methodology_version
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'discovery_signals'
          AND column_name = 'methodology_version'
          AND data_type = 'character varying'
    ) THEN
        ALTER TABLE discovery_signals ALTER COLUMN methodology_version TYPE TEXT;
    END IF;

    -- assessment_events.methodology_version
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'assessment_events'
          AND column_name = 'methodology_version'
          AND data_type = 'character varying'
    ) THEN
        ALTER TABLE assessment_events ALTER COLUMN methodology_version TYPE TEXT;
    END IF;

    -- component_batch_hashes.methodology_version
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'component_batch_hashes'
          AND column_name = 'methodology_version'
          AND data_type = 'character varying'
    ) THEN
        ALTER TABLE component_batch_hashes ALTER COLUMN methodology_version TYPE TEXT;
    END IF;

    -- cqi_attestations.methodology_version
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'cqi_attestations'
          AND column_name = 'methodology_version'
          AND data_type = 'character varying'
    ) THEN
        ALTER TABLE cqi_attestations ALTER COLUMN methodology_version TYPE TEXT;
    END IF;

    -- report_attestations.methodology_version
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'report_attestations'
          AND column_name = 'methodology_version'
          AND data_type = 'character varying'
    ) THEN
        ALTER TABLE report_attestations ALTER COLUMN methodology_version TYPE TEXT;
    END IF;

    -- rpi_score_history.methodology_version
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'rpi_score_history'
          AND column_name = 'methodology_version'
          AND data_type = 'character varying'
    ) THEN
        ALTER TABLE rpi_score_history ALTER COLUMN methodology_version TYPE TEXT;
    END IF;

    -- rpi_scores.methodology_version
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'rpi_scores'
          AND column_name = 'methodology_version'
          AND data_type = 'character varying'
    ) THEN
        ALTER TABLE rpi_scores ALTER COLUMN methodology_version TYPE TEXT;
    END IF;
END $$;

INSERT INTO migrations (name) VALUES ('107_widen_attestation_columns_to_text') ON CONFLICT DO NOTHING;
