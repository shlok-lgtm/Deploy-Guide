"""
Component 4: Manual event source.

Operator-submitted events via POST /api/engine/events. The HTTP layer
lives in events_router.py; this module is the thin DB-insert helper
that the router calls.

Same idempotency contract as defillama_hacks: ON CONFLICT DO NOTHING on
the (source, entity, event_date, event_type) unique index. A re-submit
of the same manual event returns the existing row id rather than
creating a duplicate.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Any, Optional
from uuid import UUID

import psycopg2.extras

from app.database import fetch_one, get_cursor

logger = logging.getLogger(__name__)


def _insert_manual_event_sync(
    *,
    event_type: str,
    entity: str,
    event_date: Optional[date],
    severity: Optional[str],
    raw_event_data: dict,
) -> tuple[Optional[UUID], bool]:
    """Returns (event_id, was_new). was_new is False when the unique
    constraint dedup'd; in that case event_id refers to the pre-existing
    row so the caller can still link to it."""
    raw_json = psycopg2.extras.Json(raw_event_data)
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(
            """
            INSERT INTO engine_events (
                source, event_type, entity, event_date, severity,
                raw_event_data, status
            ) VALUES ('manual', %s, %s, %s, %s, %s, 'new')
            ON CONFLICT (source, entity, event_date, event_type) DO NOTHING
            RETURNING id
            """,
            (
                event_type,
                entity,
                event_date,
                severity,
                raw_json,
            ),
        )
        row = cur.fetchone()

    if row is not None:
        return row["id"], True

    # Conflict — fetch the existing row's id
    existing = fetch_one(
        """
        SELECT id FROM engine_events
        WHERE source = 'manual'
          AND event_type = %s
          AND entity = %s
          AND event_date IS NOT DISTINCT FROM %s
        LIMIT 1
        """,
        (event_type, entity, event_date),
    )
    if existing is not None:
        return existing["id"], False
    return None, False


async def insert_manual_event(
    *,
    event_type: str,
    entity: str,
    event_date: Optional[date],
    severity: Optional[str],
    raw_event_data: dict,
) -> tuple[Optional[UUID], bool]:
    """Insert a manual event. Returns (event_id, was_new). was_new=False
    means the unique constraint dedup'd to an existing row whose id is
    returned so the caller can still link the operator's submission to
    its persisted form."""
    return await asyncio.to_thread(
        _insert_manual_event_sync,
        event_type=event_type,
        entity=entity,
        event_date=event_date,
        severity=severity,
        raw_event_data=raw_event_data,
    )
