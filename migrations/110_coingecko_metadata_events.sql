-- Migration 110: CoinGecko metadata-change webhook events
--
-- Net-new attestation domain for CoinGecko's `cg.coin.info.updated`
-- webhook stream. Same shape as v9.9.8 chain_variant_state — additive
-- schema, new attestation domain, no SII/PSI methodology change.
--
-- Purpose: capture constituent metadata mutations (contract migrations,
-- symbol / logo / name changes, platform additions) at the moment
-- CoinGecko sees them, so we no longer discover them reactively via a
-- silently-failing parser. The table is provenance-adjacent — alert +
-- audit trail, NOT a writer to the stablecoins or rpi_protocol_config
-- registries. Auto-application of contract migrations is a deliberate
-- follow-up gated on operator review of the first N events.
--
-- Columns:
--   event_id            uuid PK     — server-generated, idempotent insert
--   coingecko_id        text        — slug from payload (e.g. "usd-coin")
--   basis_entity_type   text        — 'stablecoin' | 'protocol' | NULL
--   basis_entity_id     text        — stablecoins.id or protocol_slug
--   event_type          text        — CG passthrough (e.g. "cg.coin.info.updated")
--   changed_fields      jsonb       — diff if CG sends one, else full payload
--   raw_payload         jsonb       — verbatim body for replay
--   received_at         timestamptz — server receive time
--   content_hash        text        — sha256 of raw_payload, prevents dup
--
-- Idempotency: content_hash is UNIQUE. Re-delivery from CG (their docs
-- mention at-least-once) inserts no row on duplicate hash.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'coingecko_metadata_events'
    ) THEN
        CREATE TABLE coingecko_metadata_events (
            event_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            coingecko_id       TEXT NOT NULL,
            basis_entity_type  TEXT,
            basis_entity_id    TEXT,
            event_type         TEXT,
            changed_fields     JSONB,
            raw_payload        JSONB NOT NULL,
            received_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            content_hash       TEXT NOT NULL UNIQUE
        );

        CREATE INDEX IF NOT EXISTS idx_cg_meta_events_cgid
            ON coingecko_metadata_events (coingecko_id, received_at DESC);
        CREATE INDEX IF NOT EXISTS idx_cg_meta_events_entity
            ON coingecko_metadata_events (basis_entity_type, basis_entity_id, received_at DESC);
        CREATE INDEX IF NOT EXISTS idx_cg_meta_events_received
            ON coingecko_metadata_events (received_at DESC);
    END IF;
END $$;

INSERT INTO migrations (name) VALUES ('110_coingecko_metadata_events') ON CONFLICT DO NOTHING;
