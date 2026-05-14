"""
CoinGecko webhook handler — `cg.coin.info.updated`.

Receives mutation notifications for constituent metadata (contract
migrations, symbol / logo / name changes, platform additions) on the
36 SII and 13 PSI slugs we track, persists them to
coingecko_metadata_events, resolves the basis entity, attests state
inline (v9.12 module-canonical — handler owns its attestation), and
fires an ops alert if the change touches `contract_address` or
`platforms`.

This is provenance-adjacent: the registry rewrite is deliberately
manual. The handler does not modify `stablecoins` or
`rpi_protocol_config`.

Architecture fit (v9.9.8 / v9.12):
- New attestation domain `coingecko_metadata_events`.
- Module-canonical writer: insert + attest happen in this module,
  no dispatcher / heartbeat fallback.
- Signature mismatch and unmapped slug both write structured
  cycle_errors per v9.12's silent-except-on-debug rule.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import traceback
import uuid as uuid_mod
from typing import Any, Optional

from fastapi import Request
from fastapi.responses import JSONResponse

from app.database import (
    execute,
    execute_async,
    fetch_one,
    fetch_one_async,
)

logger = logging.getLogger(__name__)

# Header name used by CoinGecko to carry the HMAC. CoinGecko's docs
# describe a single hex HMAC-SHA256 of the raw request body, keyed by
# the per-subscription secret. The header name is the value most
# commonly used by their webhook product; configurable so we can
# follow CG's evolving header naming without a code change.
SIGNATURE_HEADER = os.environ.get(
    "COINGECKO_WEBHOOK_SIGNATURE_HEADER", "x-cg-webhook-signature"
)

# Shared secret configured on the CoinGecko subscription. Required —
# if unset the handler rejects all calls as unsigned (this is the
# safe default; missing secret in prod is itself a deploy bug).
WEBHOOK_SECRET_ENV = "COINGECKO_WEBHOOK_SECRET"

# Fields whose mutation triggers an ops alert. Everything else is
# logged only (already captured in the table). The task spec calls
# out contract_address + platforms explicitly.
ALERTING_FIELDS = ("contract_address", "platforms")


# ---------------------------------------------------------------------------
# cycle_errors writer (local copy — v9.12 module-canonical, no import from
# app.worker which pulls scoring deps we don't need on the API server)
# ---------------------------------------------------------------------------

def _record_cycle_error(
    error_type: str,
    error_message: str,
    cycle_phase: str = "webhook_coingecko",
    severity: str = "caught",
) -> None:
    """Record a webhook handler error to cycle_errors. Never raises."""
    try:
        tb = traceback.format_exc()
        execute(
            """INSERT INTO cycle_errors
                (error_type, error_message, traceback, cycle_phase, severity)
            VALUES (%s, %s, %s, %s, %s)""",
            (error_type, error_message[:2000], tb[:8000], cycle_phase, severity),
        )
    except Exception as e:
        logger.warning(f"[cycle_errors] failed to record error: {e}")


# ---------------------------------------------------------------------------
# Signature verification
# ---------------------------------------------------------------------------

def _expected_signature(secret: str, body: bytes) -> str:
    """HMAC-SHA256(secret, body) → hex string."""
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def verify_signature(secret: str, body: bytes, provided: Optional[str]) -> bool:
    """
    Constant-time HMAC-SHA256 verification.

    Accepts either the bare hex digest or a `sha256=<hex>` prefix
    (some CG webhook samples use the prefixed form).
    """
    if not secret or not provided:
        return False

    candidate = provided.strip()
    if candidate.lower().startswith("sha256="):
        candidate = candidate.split("=", 1)[1].strip()

    expected = _expected_signature(secret, body)
    return hmac.compare_digest(expected, candidate)


# ---------------------------------------------------------------------------
# coingecko_id → basis entity resolution
# ---------------------------------------------------------------------------

def resolve_basis_entity(coingecko_id: str) -> tuple[Optional[str], Optional[str]]:
    """
    Map CoinGecko slug → (basis_entity_type, basis_entity_id).

    1. Stablecoins: `SELECT id FROM stablecoins WHERE coingecko_id = %s`.
       Returns ("stablecoin", id) on hit.
    2. Protocols: PSI uses governance-token coingecko_ids
       (PROTOCOL_GOVERNANCE_TOKENS in app/collectors/psi_collector.py)
       which map back to protocol_slug. Returns ("protocol", slug) on hit.
    3. Otherwise (None, None) and the caller logs cycle_errors.
    """
    if not coingecko_id:
        return (None, None)

    try:
        row = fetch_one(
            "SELECT id FROM stablecoins WHERE coingecko_id = %s LIMIT 1",
            (coingecko_id,),
        )
        if row:
            return ("stablecoin", row["id"])
    except Exception as e:
        logger.warning(f"[coingecko_webhook] stablecoin lookup failed: {e}")

    try:
        from app.collectors.psi_collector import PROTOCOL_GOVERNANCE_TOKENS
        for protocol_slug, gov_token_cg_id in PROTOCOL_GOVERNANCE_TOKENS.items():
            if gov_token_cg_id == coingecko_id:
                return ("protocol", protocol_slug)
    except Exception as e:
        logger.warning(f"[coingecko_webhook] protocol token map import failed: {e}")

    return (None, None)


# ---------------------------------------------------------------------------
# Payload normalization
# ---------------------------------------------------------------------------

def extract_coingecko_id(payload: dict) -> str:
    """
    CoinGecko's webhook payload shape isn't fully stable across event
    versions. Probe known locations in order of how the documented
    samples describe the field.
    """
    for key in ("coin_id", "coingecko_id", "id", "slug"):
        v = payload.get(key)
        if isinstance(v, str) and v:
            return v
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("coin_id", "coingecko_id", "id", "slug"):
            v = data.get(key)
            if isinstance(v, str) and v:
                return v
    return ""


def extract_changed_fields(payload: dict) -> dict:
    """
    Return the diff if CoinGecko provided one (`changes`, `diff`,
    `changed_fields`). Otherwise pass the full payload through —
    downstream queries can still inspect e.g. `payload->'platforms'`.
    """
    for key in ("changed_fields", "changes", "diff"):
        v = payload.get(key)
        if isinstance(v, dict):
            return v
    return payload


def _alert_fields_touched(changed: dict) -> list[str]:
    """Return the subset of ALERTING_FIELDS present in the diff."""
    touched = []
    for f in ALERTING_FIELDS:
        if f in changed:
            touched.append(f)
            continue
        data = changed.get("data")
        if isinstance(data, dict) and f in data:
            touched.append(f)
    return touched


# ---------------------------------------------------------------------------
# Insert + attest
# ---------------------------------------------------------------------------

def _content_hash(raw_payload: bytes) -> str:
    """SHA-256 of the exact bytes signed by the sender."""
    return hashlib.sha256(raw_payload).hexdigest()


def persist_event(
    coingecko_id: str,
    basis_entity_type: Optional[str],
    basis_entity_id: Optional[str],
    event_type: str,
    changed_fields: dict,
    raw_payload: dict,
    content_hash: str,
) -> Optional[str]:
    """
    Insert a coingecko_metadata_events row. Idempotent on content_hash —
    a duplicate delivery returns the existing event_id.
    """
    event_id = str(uuid_mod.uuid4())
    try:
        existing = fetch_one(
            "SELECT event_id FROM coingecko_metadata_events WHERE content_hash = %s",
            (content_hash,),
        )
        if existing:
            return str(existing["event_id"])
        execute(
            """
            INSERT INTO coingecko_metadata_events
                (event_id, coingecko_id, basis_entity_type, basis_entity_id,
                 event_type, changed_fields, raw_payload, received_at, content_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
            ON CONFLICT (content_hash) DO NOTHING
            """,
            (
                event_id,
                coingecko_id,
                basis_entity_type,
                basis_entity_id,
                event_type,
                json.dumps(changed_fields, default=str),
                json.dumps(raw_payload, default=str),
                content_hash,
            ),
        )
        return event_id
    except Exception as e:
        _record_cycle_error(
            error_type="webhook_persist_failure",
            error_message=str(e)[:2000],
            cycle_phase="webhook_coingecko",
        )
        return None


def attest_inline(
    event_id: str,
    coingecko_id: str,
    basis_entity_type: Optional[str],
    basis_entity_id: Optional[str],
    content_hash: str,
) -> None:
    """
    v9.12 module-canonical: the handler owns the attest_state call
    for its domain. Single record per webhook delivery — the
    content_hash + entity tuple is the canonical state.
    """
    from app.state_attestation import attest_state

    record = {
        "event_id": event_id,
        "coingecko_id": coingecko_id,
        "basis_entity_type": basis_entity_type,
        "basis_entity_id": basis_entity_id,
        "content_hash": content_hash,
    }
    try:
        attest_state(
            "coingecko_metadata_events",
            [record],
            entity_id=basis_entity_id,
            writer_id="webhook.coingecko",
        )
    except Exception as e:
        _record_cycle_error(
            error_type="webhook_attestation_failure",
            error_message=str(e)[:2000],
            cycle_phase="webhook_coingecko",
        )


async def maybe_alert(
    coingecko_id: str,
    basis_entity_type: Optional[str],
    basis_entity_id: Optional[str],
    changed_fields: dict,
) -> None:
    """Fire ops alert if `contract_address` or `platforms` changed."""
    touched = _alert_fields_touched(changed_fields)
    if not touched:
        return
    try:
        from app.ops.tools.alerter import send_alert

        message = (
            f"CoinGecko metadata change on {coingecko_id}: "
            f"{', '.join(touched)} mutated. "
            f"basis_entity={basis_entity_type}:{basis_entity_id}. "
            f"Review required — no auto-application."
        )
        # default=str per v9.12 lesson (Decimal/datetime safety in
        # the alerter's own json.dumps path is already covered, but
        # we pass primitives through our context to keep the
        # serialization local).
        context = {
            "coingecko_id": coingecko_id,
            "basis_entity_type": basis_entity_type,
            "basis_entity_id": basis_entity_id,
            "fields": touched,
            "changed_fields": json.loads(
                json.dumps(changed_fields, default=str)
            ),
        }
        await send_alert(
            alert_type="coingecko_metadata_change",
            message=message,
            context=context,
            severity="critical",
        )
    except Exception as e:
        _record_cycle_error(
            error_type="webhook_alert_failure",
            error_message=str(e)[:2000],
            cycle_phase="webhook_coingecko",
        )


# ---------------------------------------------------------------------------
# FastAPI handler
# ---------------------------------------------------------------------------

async def handle_webhook(request: Request) -> JSONResponse:
    """
    POST /api/webhooks/coingecko

    - Reads raw body and verifies HMAC-SHA256 against
      COINGECKO_WEBHOOK_SECRET. Mismatch → 401 + cycle_errors row.
    - Parses JSON, persists row, resolves basis entity, attests
      state, and (if applicable) fires ops alert.
    - Returns 200 fast — the alert path runs async and never blocks
      the ack.
    """
    raw = await request.body()
    provided_sig = request.headers.get(SIGNATURE_HEADER) or request.headers.get(
        SIGNATURE_HEADER.lower()
    )
    secret = os.environ.get(WEBHOOK_SECRET_ENV, "")

    if not verify_signature(secret, raw, provided_sig):
        _record_cycle_error(
            error_type="webhook_signature_invalid",
            error_message=(
                f"CoinGecko webhook HMAC mismatch from "
                f"{request.client.host if request.client else 'unknown'} "
                f"sig_present={bool(provided_sig)} secret_configured={bool(secret)}"
            ),
            cycle_phase="webhook_coingecko",
            severity="alert",
        )
        return JSONResponse(status_code=401, content={"status": "invalid_signature"})

    try:
        payload = json.loads(raw.decode("utf-8") or "{}")
    except Exception as e:
        _record_cycle_error(
            error_type="webhook_payload_invalid",
            error_message=f"json decode: {e}"[:2000],
            cycle_phase="webhook_coingecko",
        )
        return JSONResponse(status_code=400, content={"status": "invalid_payload"})

    coingecko_id = extract_coingecko_id(payload)
    event_type = (
        payload.get("event") or payload.get("event_type") or "cg.coin.info.updated"
    )
    changed_fields = extract_changed_fields(payload)
    content_hash = _content_hash(raw)

    basis_entity_type, basis_entity_id = resolve_basis_entity(coingecko_id)

    event_id = persist_event(
        coingecko_id=coingecko_id,
        basis_entity_type=basis_entity_type,
        basis_entity_id=basis_entity_id,
        event_type=event_type,
        changed_fields=changed_fields,
        raw_payload=payload,
        content_hash=content_hash,
    )

    if event_id is None:
        return JSONResponse(
            status_code=500, content={"status": "persist_failed"}
        )

    if basis_entity_type is None:
        _record_cycle_error(
            error_type="webhook_unmapped_slug",
            error_message=(
                f"coingecko_id={coingecko_id!r} not present in stablecoins "
                f"or PROTOCOL_GOVERNANCE_TOKENS — event stored unmapped"
            ),
            cycle_phase="webhook_coingecko",
        )

    attest_inline(
        event_id=event_id,
        coingecko_id=coingecko_id,
        basis_entity_type=basis_entity_type,
        basis_entity_id=basis_entity_id,
        content_hash=content_hash,
    )

    await maybe_alert(
        coingecko_id=coingecko_id,
        basis_entity_type=basis_entity_type,
        basis_entity_id=basis_entity_id,
        changed_fields=changed_fields,
    )

    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "event_id": event_id,
            "basis_entity_type": basis_entity_type,
            "basis_entity_id": basis_entity_id,
        },
    )
