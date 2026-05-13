# Domain: `coingecko_metadata_events` — CoinGecko Webhook Subscriber

**Date:** 2026-05-13
**Status:** Proposed
**Scope:** Additive — new attestation domain, no SII/PSI methodology change.
**Closest precedent:** v9.9.8 chain-variant amendment (new attestation domain, no scoring path change).

## Forcing function

Basis ingests CoinGecko via hourly REST in the SII / PSI / CQI cycle. We
have no signal when CoinGecko mutates constituent metadata (contract
migrations, symbol / logo / name changes, platform additions). Today
these are discovered reactively — the canonical case is the
2026-05-05 punchlist Section C: the `exchange_trust_ratio` collector
silently returning zero because a CoinGecko response shape changed
and our parser swallowed it.

The webhook stream is the substrate CoinGecko already maintains; we
were not subscribed.

## Decision

A new attestation domain, `coingecko_metadata_events`, captures every
`cg.coin.info.updated` delivery on the 36 SII slugs and 13 PSI
governance-token slugs we currently track. The handler:

1. **Verifies HMAC-SHA256** of the raw body against
   `COINGECKO_WEBHOOK_SECRET`. Mismatches return 401 and write a
   structured `cycle_errors` row with `error_type=webhook_signature_invalid`.

2. **Persists** the event to `coingecko_metadata_events` (event_id,
   coingecko_id, basis_entity_type, basis_entity_id, event_type,
   changed_fields, raw_payload, received_at, content_hash). The table
   uses `content_hash UNIQUE` for idempotency — CoinGecko's docs
   describe at-least-once delivery, and re-deliveries are no-ops.

3. **Resolves** `coingecko_id` to a Basis entity:
   - Stablecoins: `SELECT id FROM stablecoins WHERE coingecko_id = %s`
     → `("stablecoin", id)`.
   - Protocols: reverse-lookup against
     `app.collectors.psi_collector.PROTOCOL_GOVERNANCE_TOKENS`
     (governance-token CG-id → protocol_slug) →
     `("protocol", slug)`.
   - Unmapped: writes the event with `basis_entity_type=NULL` and a
     `cycle_errors` row with `error_type=webhook_unmapped_slug`. The
     audit trail is still valuable; the row carries the raw payload
     for forensic replay.

4. **Attests state** inline via
   `attest_state("coingecko_metadata_events", [record],
   entity_id=basis_entity_id)`. The record contains the event_id,
   coingecko_id, entity tuple, and content_hash — sufficient to
   re-derive the receipt from raw_payload at any time.

5. **Alerts** if `changed_fields` touches `contract_address` or
   `platforms`. Sent through the existing
   `app.ops.tools.alerter.send_alert` path with
   `alert_type=coingecko_metadata_change`, `severity=critical`.
   `json.dumps(context, default=str)` per the v9.12
   Decimal/datetime-safety lesson.

The handler returns 200 immediately on success; alert and attestation
both run inside the request but are independently fault-isolated —
each failure writes its own cycle_error and the 200 still goes back
so CoinGecko doesn't redeliver unnecessarily.

## What the handler explicitly does NOT do

This is alert + audit trail, not a writer to constituent registries.

- The `stablecoins` table is not touched. A new `contract_address` on
  USDC does not update `stablecoins.contract`. A human review step is
  intentional for now — auto-application is a follow-up, gated on
  operator validation of the first N event payload shapes.
- `rpi_protocol_config` is not touched. PSI scoring is unchanged.
- The SII / PSI scoring path is unaffected. This domain is
  provenance-adjacent metadata, not a scored component.
- The hourly REST polling cadence is unchanged. Webhook is additive,
  not a replacement.

## Architecture fit

| Aspect | Choice | Why |
|--------|--------|-----|
| Service | api-server (`app/server.py`) | Webhooks are request/response, not cycle-driven. The Scoring-Worker has no inbound HTTP. |
| Module location | `app/webhooks/coingecko.py` | New package; matches the precedent that handlers are module-canonical (v9.12) and own their attestation. No dispatcher fallback. |
| Attestation | Inline `attest_state` in the handler | Module-canonical per v9.12 — no orphan dispatcher heartbeat path. |
| Error handling | Structured `cycle_errors` per failure mode | v9.12's silent-except-on-debug rule. No bare excepts. |
| Idempotency | `content_hash UNIQUE` on the table | At-least-once delivery from CG; replay-safe. |

## Schema

Migration `110_coingecko_metadata_events.sql`:

```sql
CREATE TABLE coingecko_metadata_events (
    event_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    coingecko_id       TEXT NOT NULL,
    basis_entity_type  TEXT,            -- 'stablecoin' | 'protocol' | NULL
    basis_entity_id    TEXT,            -- stablecoins.id or protocol_slug
    event_type         TEXT,            -- CG passthrough
    changed_fields     JSONB,           -- diff if CG sends one, else full payload
    raw_payload        JSONB NOT NULL,  -- verbatim, for replay
    received_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content_hash       TEXT NOT NULL UNIQUE  -- sha256(raw_payload), idempotency
);
CREATE INDEX idx_cg_meta_events_cgid     ON coingecko_metadata_events (coingecko_id, received_at DESC);
CREATE INDEX idx_cg_meta_events_entity   ON coingecko_metadata_events (basis_entity_type, basis_entity_id, received_at DESC);
CREATE INDEX idx_cg_meta_events_received ON coingecko_metadata_events (received_at DESC);
```

## Configuration

| Env var | Purpose | Required |
|---------|---------|----------|
| `COINGECKO_WEBHOOK_SECRET` | HMAC-SHA256 key shared with the CoinGecko subscription. | Yes — if unset the handler rejects all calls as unsigned. |
| `COINGECKO_WEBHOOK_SIGNATURE_HEADER` | HTTP header name carrying the signature. | No — defaults to `x-cg-webhook-signature`. Configurable for CG header-name drift. |
| `COINGECKO_WEBHOOK_CALLBACK_URL` | Public URL of `POST /api/webhooks/coingecko`. | Used only by the subscription script. |
| `COINGECKO_API_KEY` | Existing pro-api key, reused by the subscription script. | Yes for `subscribe_webhooks.py`. |

## Pre-flight (operator)

Before running the subscription script:

1. Confirm the production CoinGecko pro-api plan includes webhooks.
   CoinGecko's product post claims access is already enabled for pro
   tier; the script's `/api/v3/key` probe surfaces plan metadata if
   the response includes it.
2. Confirm CoinGecko's webhook signing scheme matches the verifier
   (HMAC-SHA256, hex digest, raw body). The verifier accepts both the
   bare hex digest and a `sha256=` prefix.

## Subscription

`scripts/coingecko/subscribe_webhooks.py` registers the subscription
for every scoring-enabled stablecoin slug (from the DB; falls back to
`STABLECOIN_REGISTRY`) plus every value in
`PROTOCOL_GOVERNANCE_TOKENS`. Idempotent — existing subscriptions on
the same `(event, coin_id, callback_url)` tuple are detected via a
GET probe and skipped. Safe to re-run after coin promotions.

```bash
export COINGECKO_API_KEY=...
export COINGECKO_WEBHOOK_SECRET=...
export COINGECKO_WEBHOOK_CALLBACK_URL=https://hub.basis.../api/webhooks/coingecko
python scripts/coingecko/subscribe_webhooks.py --dry-run
python scripts/coingecko/subscribe_webhooks.py
```

## Halt criteria (substrate-verifiable, 24h post-merge)

```sql
-- Deliveries are arriving. Zero is acceptable if no constituent metadata
-- changed in 24h; log a note in the wave summary if zero.
SELECT COUNT(*) FROM coingecko_metadata_events
WHERE received_at > NOW() - INTERVAL '24 hours';

-- Signature verifier is correct. Non-zero means either an attacker is
-- probing the endpoint or our verifier disagrees with CG's signing.
SELECT COUNT(*) FROM cycle_errors
WHERE error_type = 'webhook_signature_invalid'
  AND occurred_at > NOW() - INTERVAL '24 hours';
-- Expected: 0.

-- Attestation domain is wired and reaching state_attestations.
SELECT MAX(cycle_timestamp), NOW() - MAX(cycle_timestamp)
FROM state_attestations
WHERE domain = 'coingecko_metadata_events';
-- Expected: non-null. Null means attestation isn't firing.
```

## Out-of-scope follow-ups

- Auto-application of contract migrations to `stablecoins` / `rpi_protocol_config`. Gated on operator review of the first N events to validate payload shape.
- Webhook subscription for chain-variant changes (the v9.9.8 surface) — natural extension once the canonical flow is stable.
- Equivalent subscribers for DeFiLlama or Etherscan if either publishes a comparable event stream.

## References

- v9.9.8 chain-variant constitution amendment — closest precedent for additive attestation domain.
- v9.12 module-canonical live path — handler owns its own attestation; no dispatcher fallback.
- 2026-05-05 punchlist Section C — the reactive-discovery incident that forced this.
- Migration: `migrations/110_coingecko_metadata_events.sql`.
- Handler: `app/webhooks/coingecko.py`.
- Route: `app/server.py` — `POST /api/webhooks/coingecko`.
- Subscriber: `scripts/coingecko/subscribe_webhooks.py`.
- Tests: `tests/test_coingecko_webhook.py`.
