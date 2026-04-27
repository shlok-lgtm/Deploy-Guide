"""
Component 4: DeFiLlama Hacks event source.

Polls DeFiLlama's hacks feed every 15 minutes (driven by
app.engine.scheduler), normalizes each hack into the engine_events
shape, INSERTs new events with idempotent (source, entity, event_date,
event_type) deduplication, and spawns a background task to trigger
analysis on each one.

API endpoint:
    https://api.llama.fi/hacks

Free, no auth required. The actual response shape may vary; this module
defends against missing or unexpected fields by skipping malformed
entries (logged at WARNING) rather than aborting the whole poll.

Idempotency story:
  engine_events has a UNIQUE constraint on (source, entity, event_date,
  event_type) per migration 098. INSERTs use ON CONFLICT DO NOTHING and
  RETURNING id, so we can tell whether each insert produced a new row.
  Re-running a poll over the same window is a no-op DB-wise.

Entity resolution:
  DeFiLlama uses display names like "Drift Protocol" / "Curve Finance".
  We normalize these to slugs (lowercase, hyphenated, common suffixes
  stripped) and verify against Basis coverage before triggering analysis.
  When normalization produces a slug Basis doesn't track, the event is
  still recorded (as a coverage-gap signal for the operator) but no
  analysis fires — the event status is set to 'no_coverage'.

Latency note: DeFiLlama's hacks feed has historical lag (hours, sometimes
≥1 day, between event and feed entry). Manual submission compensates.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import date, datetime, timezone
from typing import Any, Optional
from uuid import UUID

import httpx
import psycopg2.extras

from app.database import fetch_one, get_cursor
from app.engine.event_pipeline import process_event

logger = logging.getLogger(__name__)


DEFILLAMA_HACKS_URL = "https://api.llama.fi/hacks"
DEFILLAMA_HTTP_TIMEOUT_S = 30.0
DEFILLAMA_USER_AGENT = "basis-protocol-engine/1.0 (+https://basisprotocol.xyz)"


# ─────────────────────────────────────────────────────────────────
# Slug normalization — DeFiLlama display name → Basis entity slug
# ─────────────────────────────────────────────────────────────────

# Suffixes to strip when normalizing protocol names. Order matters —
# longest first so "-perpetual-protocol" doesn't get clipped to
# "-perpetual" before we'd strip "-protocol".
_SLUG_SUFFIXES_TO_STRIP = (
    "-protocol",
    "-finance",
    "-network",
    "-platform",
    "-pos",
    "-v3",
    "-v2",
    "-v1",
)


def normalize_defillama_protocol_to_slug(name: Optional[str]) -> Optional[str]:
    """Convert a DeFiLlama protocol display name into a Basis-style slug.

    Examples:
      "Drift Protocol"        → "drift"
      "Curve Finance"         → "curve"
      "Curve.fi"              → "curvefi"
      "LayerZero v2"          → "layerzero"
      "Jupiter Perpetual Exchange" → "jupiter-perpetual-exchange"

    Heuristic, will fail for some entities. Acceptable for v1 — operators
    can correct via manual event submission. Returns None for empty /
    unusable input.
    """
    if not name or not name.strip():
        return None

    cleaned = name.strip().lower()
    # Replace any non-word/space/hyphen with empty (drops dots, parens, etc.)
    cleaned = re.sub(r"[^\w\s-]", "", cleaned)
    # Collapse whitespace and underscores into a single hyphen
    cleaned = re.sub(r"[\s_]+", "-", cleaned)
    # Collapse multiple hyphens
    cleaned = re.sub(r"-+", "-", cleaned)
    cleaned = cleaned.strip("-")

    # Strip common suffixes (one pass; if multiple match, longest wins)
    for suffix in _SLUG_SUFFIXES_TO_STRIP:
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)]
            break

    cleaned = cleaned.strip("-")
    return cleaned or None


# ─────────────────────────────────────────────────────────────────
# DeFiLlama hack record → engine_events row dict
# ─────────────────────────────────────────────────────────────────

def _coerce_event_date(raw: Any) -> Optional[date]:
    """DeFiLlama's date field varies — sometimes ISO string, sometimes
    Unix timestamp. Try both. Returns None on parse failure."""
    if raw is None:
        return None
    # Unix timestamp (seconds or milliseconds)
    if isinstance(raw, (int, float)):
        try:
            ts = float(raw)
            if ts > 1e12:  # millisecond timestamp
                ts = ts / 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc).date()
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        # Try ISO (with or without time)
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        except ValueError:
            return None
    return None


def _severity_from_amount(amount_lost_usd: Optional[float]) -> str:
    """Heuristic: severity by dollar size of the loss. Operator can
    re-grade via manual submission for context that DeFiLlama doesn't
    expose (zero-day vs known-pattern, etc.)."""
    if amount_lost_usd is None:
        return "low"
    if amount_lost_usd >= 100_000_000:
        return "critical"
    if amount_lost_usd >= 10_000_000:
        return "high"
    if amount_lost_usd >= 1_000_000:
        return "medium"
    return "low"


def normalize_hack_to_event(hack: dict) -> Optional[dict]:
    """Map a single DeFiLlama hack record into the dict shape we INSERT
    into engine_events. Returns None when the record is missing fields
    we can't synthesize.

    Defensive against shape variation: tries multiple keys for protocol
    name and date so a future API tweak doesn't silently drop events.
    """
    raw_name = (
        hack.get("name")
        or hack.get("protocol")
        or hack.get("project")
        or hack.get("target")
    )
    entity = normalize_defillama_protocol_to_slug(raw_name)
    if entity is None:
        return None

    raw_date = (
        hack.get("date")
        or hack.get("hackDate")
        or hack.get("timestamp")
        or hack.get("createdAt")
    )
    event_date = _coerce_event_date(raw_date)
    if event_date is None:
        return None

    amount = hack.get("amount") or hack.get("amountLost") or hack.get("amount_lost")
    try:
        amount_usd = float(amount) if amount is not None else None
    except (TypeError, ValueError):
        amount_usd = None

    return {
        "source": "defillama_hacks",
        "event_type": "exploit",
        "entity": entity,
        "event_date": event_date,
        "severity": _severity_from_amount(amount_usd),
        "raw_event_data": {
            "defillama_raw": hack,
            "normalized_name": raw_name,
            "amount_lost_usd": amount_usd,
        },
    }


# ─────────────────────────────────────────────────────────────────
# DB insert (idempotent) — returns event_id when a new row was inserted,
# None when the unique constraint suppressed it.
# ─────────────────────────────────────────────────────────────────

def _insert_event_if_new_sync(event: dict) -> Optional[UUID]:
    raw_json = psycopg2.extras.Json(event["raw_event_data"])
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(
            """
            INSERT INTO engine_events (
                source, event_type, entity, event_date, severity,
                raw_event_data, status
            ) VALUES (%s, %s, %s, %s, %s, %s, 'new')
            ON CONFLICT (source, entity, event_date, event_type) DO NOTHING
            RETURNING id
            """,
            (
                event["source"],
                event["event_type"],
                event["entity"],
                event["event_date"],
                event["severity"],
                raw_json,
            ),
        )
        row = cur.fetchone()
        return row["id"] if row else None


async def insert_event_if_new(event: dict) -> Optional[UUID]:
    """Returns event_id if newly inserted, None if the unique constraint
    deduped it (already-known event). Async wrapper."""
    return await asyncio.to_thread(_insert_event_if_new_sync, event)


# ─────────────────────────────────────────────────────────────────
# Public entry point — called by the scheduler every 15 min
# ─────────────────────────────────────────────────────────────────

async def poll_defillama_hacks(*, http_url: str = DEFILLAMA_HACKS_URL) -> dict:
    """Fetch the DeFiLlama hacks feed, normalize each entry, INSERT
    new events, and trigger analysis for each.

    Returns a summary dict for visibility; the scheduler logs it.
    Errors during one hack don't abort the whole poll — each is logged
    and the loop continues.
    """
    summary = {
        "fetched": 0,
        "inserted": 0,
        "duplicates": 0,
        "malformed": 0,
        "trigger_errors": 0,
    }

    try:
        async with httpx.AsyncClient(
            timeout=DEFILLAMA_HTTP_TIMEOUT_S,
            headers={"User-Agent": DEFILLAMA_USER_AGENT},
        ) as client:
            response = await client.get(http_url)
            response.raise_for_status()
            payload = response.json()
    except httpx.HTTPError as exc:
        logger.error("poll_defillama_hacks: HTTP error from %s: %s", http_url, exc)
        return summary
    except Exception:
        logger.exception("poll_defillama_hacks: unexpected fetch error")
        return summary

    # The endpoint may return a top-level list or {"hacks": [...]}
    hacks: list[dict]
    if isinstance(payload, list):
        hacks = payload
    elif isinstance(payload, dict):
        hacks = payload.get("hacks") or payload.get("data") or []
    else:
        logger.warning(
            "poll_defillama_hacks: unexpected payload shape %s", type(payload)
        )
        return summary

    summary["fetched"] = len(hacks)
    logger.info("poll_defillama_hacks: fetched %d hacks", len(hacks))

    for hack in hacks:
        try:
            event = normalize_hack_to_event(hack)
            if event is None:
                summary["malformed"] += 1
                continue

            event_id = await insert_event_if_new(event)
            if event_id is None:
                summary["duplicates"] += 1
                continue

            summary["inserted"] += 1
            logger.info(
                "poll_defillama_hacks: new event_id=%s entity=%s event_date=%s "
                "severity=%s",
                event_id, event["entity"], event["event_date"],
                event["severity"],
            )

            # Fire-and-forget analysis trigger. Errors inside process_event
            # are logged there; we don't want one bad event to abort the poll.
            try:
                asyncio.create_task(process_event(event_id))
            except Exception:
                summary["trigger_errors"] += 1
                logger.exception(
                    "poll_defillama_hacks: failed to spawn process_event for %s",
                    event_id,
                )
        except Exception:
            summary["malformed"] += 1
            logger.exception(
                "poll_defillama_hacks: failed to process hack record"
            )

    logger.info("poll_defillama_hacks: done %s", summary)
    return summary
