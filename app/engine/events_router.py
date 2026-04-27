"""
Component 4 router: /api/engine/events + /api/engine/watchlist + a
diagnostic /api/engine/scheduler endpoint.

Endpoints:
  POST   /api/engine/events                               — manual event submission
  GET    /api/engine/events                               — list, with filters
  GET    /api/engine/events/{event_id}                    — fetch one
  DELETE /api/engine/events/{event_id}                    — soft delete (status='dismissed')
  POST   /api/engine/watchlist                            — add a watchlist row
  GET    /api/engine/watchlist                            — list rows
  DELETE /api/engine/watchlist/{watchlist_id}             — soft delete (active=false)
  GET    /api/engine/scheduler                            — diagnostic: scheduler state

All admin-only via the inline _check_admin_key pattern. Same convention
as analyze_router / render_router / budget_router.

Manual event submission with trigger_analysis=true skips the HTTP
roundtrip into POST /analyze and goes through analysis_pipeline.
trigger_analysis directly — bypasses rate limit + auth checks since the
engine itself is the caller.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
from datetime import date, datetime
from typing import Any, Literal, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.database import fetch_all, fetch_one, get_cursor
from app.engine.analysis_pipeline import trigger_analysis
from app.engine.coverage import FULL_INDEX_UNIVERSE
from app.engine.event_pipeline import process_event
from app.engine.event_sources.manual import insert_manual_event
from app.engine.scheduler import is_running as scheduler_is_running
from app.engine.scheduler import list_jobs as scheduler_list_jobs
from app.engine.watchlist import VALID_THRESHOLD_TYPES

logger = logging.getLogger(__name__)

router = APIRouter()


# ─────────────────────────────────────────────────────────────────
# Admin-key check
# ─────────────────────────────────────────────────────────────────

def _check_admin_key(request: Request) -> None:
    admin_key = os.environ.get("ADMIN_KEY", "")
    provided = (
        request.query_params.get("key", "")
        or request.headers.get("x-admin-key", "")
    )
    if not admin_key or not provided or not hmac.compare_digest(provided, admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ─────────────────────────────────────────────────────────────────
# Pydantic models — local to C4 (don't pollute schemas.py)
# ─────────────────────────────────────────────────────────────────

ManualEventType = Literal["exploit", "governance", "depeg", "other"]
ManualSeverity = Literal["low", "medium", "high", "critical"]


class ManualEventRequest(BaseModel):
    source: Literal["manual"] = "manual"
    event_type: ManualEventType
    entity: str
    event_date: Optional[date] = None
    severity: Optional[ManualSeverity] = None
    raw_event_data: dict[str, Any] = Field(default_factory=dict)
    trigger_analysis: bool = True


class ManualEventAccepted(BaseModel):
    event_id: UUID
    was_new: bool
    analysis_id: Optional[UUID] = None
    analysis_outcome: Optional[str] = None
    detail: Optional[str] = None


class WatchlistAddRequest(BaseModel):
    entity_slug: str
    index_id: str
    threshold_type: str
    threshold_value: float
    measure_name: Optional[str] = None
    active: bool = True
    notes: Optional[str] = None


class WatchlistRow(BaseModel):
    id: UUID
    entity_slug: str
    index_id: Optional[str]
    threshold_type: str
    threshold_value: float
    measure_name: Optional[str]
    notes: Optional[str]
    active: bool
    last_triggered_at: Optional[datetime]
    created_at: datetime


class EventRow(BaseModel):
    id: UUID
    detected_at: datetime
    source: str
    event_type: str
    entity: str
    event_date: Optional[date]
    severity: Optional[str]
    raw_event_data: dict[str, Any]
    analysis_id: Optional[UUID]
    artifact_id: Optional[UUID]
    status: str
    delivered_at: Optional[datetime]
    operator_response: Optional[str]


# ─────────────────────────────────────────────────────────────────
# POST /api/engine/events — manual event submission
# ─────────────────────────────────────────────────────────────────

@router.post("/api/engine/events", status_code=202)
async def post_event(
    request: Request, payload: ManualEventRequest
) -> JSONResponse:
    _check_admin_key(request)

    event_id, was_new = await insert_manual_event(
        event_type=payload.event_type,
        entity=payload.entity,
        event_date=payload.event_date,
        severity=payload.severity,
        raw_event_data=payload.raw_event_data,
    )
    if event_id is None:
        # Insert failed AND the lookup couldn't find an existing row
        raise HTTPException(
            status_code=500,
            detail="failed to persist manual event",
        )

    response = ManualEventAccepted(event_id=event_id, was_new=was_new)

    if payload.trigger_analysis:
        # Direct internal trigger — bypasses HTTP/auth/rate-limit. The
        # engine is its own caller here.
        result = await trigger_analysis(
            entity=payload.entity,
            event_date=payload.event_date,
            peer_set=[],
            context=(
                f"Manually submitted {payload.event_type} event "
                f"for {payload.entity} on "
                f"{payload.event_date.isoformat() if payload.event_date else 'no event date'}"
            ),
            force_new=False,
        )
        # Update the event row with the linked analysis + outcome
        from app.engine.event_pipeline import update_event
        new_status = (
            "analyzed" if result.analysis_id else (
                "no_coverage" if result.outcome == "no_coverage" else "error"
            )
        )
        await update_event(
            event_id,
            status=new_status,
            analysis_id=result.analysis_id,
            operator_response=result.detail,
        )
        response.analysis_id = result.analysis_id
        response.analysis_outcome = result.outcome
        response.detail = result.detail

    return JSONResponse(
        status_code=202,
        content=response.model_dump(mode="json"),
    )


# ─────────────────────────────────────────────────────────────────
# GET /api/engine/events
# ─────────────────────────────────────────────────────────────────

def _list_events_sync(
    *,
    status: Optional[str],
    entity: Optional[str],
    since: Optional[date],
    limit: int,
) -> list[dict]:
    clauses: list[str] = []
    params: list[Any] = []
    if status:
        clauses.append("status = %s")
        params.append(status)
    if entity:
        clauses.append("entity = %s")
        params.append(entity)
    if since:
        clauses.append("detected_at >= %s")
        params.append(since)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)
    return fetch_all(
        f"""
        SELECT id, detected_at, source, event_type, entity, event_date,
               severity, raw_event_data, analysis_id, artifact_id,
               status, delivered_at, operator_response
        FROM engine_events
        {where}
        ORDER BY detected_at DESC
        LIMIT %s
        """,
        tuple(params),
    )


@router.get("/api/engine/events", response_model=list[EventRow])
async def list_events(
    request: Request,
    status: Optional[str] = Query(None),
    entity: Optional[str] = Query(None),
    since: Optional[date] = Query(None),
    limit: int = Query(50, ge=1, le=200),
) -> list[EventRow]:
    _check_admin_key(request)
    rows = await asyncio.to_thread(
        _list_events_sync,
        status=status, entity=entity, since=since, limit=limit,
    )
    return [EventRow.model_validate(r) for r in rows]


# ─────────────────────────────────────────────────────────────────
# GET /api/engine/events/{event_id}
# ─────────────────────────────────────────────────────────────────

def _get_event_sync(event_id: UUID) -> Optional[dict]:
    return fetch_one(
        """
        SELECT id, detected_at, source, event_type, entity, event_date,
               severity, raw_event_data, analysis_id, artifact_id,
               status, delivered_at, operator_response
        FROM engine_events
        WHERE id = %s
        """,
        (str(event_id),),
    )


@router.get("/api/engine/events/{event_id}", response_model=EventRow)
async def get_event(request: Request, event_id: UUID) -> EventRow:
    _check_admin_key(request)
    row = await asyncio.to_thread(_get_event_sync, event_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"No event with id {event_id}")
    return EventRow.model_validate(row)


# ─────────────────────────────────────────────────────────────────
# DELETE /api/engine/events/{event_id} — soft delete
# ─────────────────────────────────────────────────────────────────

def _dismiss_event_sync(event_id: UUID) -> bool:
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(
            """
            UPDATE engine_events
            SET status = 'dismissed'
            WHERE id = %s
            RETURNING id
            """,
            (str(event_id),),
        )
        return cur.fetchone() is not None


@router.delete("/api/engine/events/{event_id}")
async def dismiss_event(request: Request, event_id: UUID) -> JSONResponse:
    _check_admin_key(request)
    ok = await asyncio.to_thread(_dismiss_event_sync, event_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"No event with id {event_id}")
    return JSONResponse(
        status_code=200,
        content={"status": "dismissed", "id": str(event_id)},
    )


# ─────────────────────────────────────────────────────────────────
# POST /api/engine/watchlist — add a watchlist row
# ─────────────────────────────────────────────────────────────────

def _insert_watchlist_sync(payload: WatchlistAddRequest) -> UUID:
    import psycopg2.extras  # local — module-level register_uuid is in analysis_persistence
    psycopg2.extras.register_uuid()
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(
            """
            INSERT INTO engine_watchlist (
                entity_slug, index_id, threshold_type, threshold_value,
                measure_name, notes, active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                payload.entity_slug,
                payload.index_id,
                payload.threshold_type,
                payload.threshold_value,
                payload.measure_name,
                payload.notes,
                payload.active,
            ),
        )
        row = cur.fetchone()
        return row["id"]


@router.post("/api/engine/watchlist", status_code=201)
async def add_watchlist(
    request: Request, payload: WatchlistAddRequest
) -> JSONResponse:
    _check_admin_key(request)

    if payload.threshold_type not in VALID_THRESHOLD_TYPES:
        raise HTTPException(
            status_code=422,
            detail=(
                f"unknown threshold_type {payload.threshold_type!r}. "
                f"Valid: {sorted(VALID_THRESHOLD_TYPES)}"
            ),
        )

    if payload.index_id not in FULL_INDEX_UNIVERSE:
        raise HTTPException(
            status_code=422,
            detail=(
                f"unknown index_id {payload.index_id!r}. "
                f"Valid: {sorted(FULL_INDEX_UNIVERSE)}"
            ),
        )

    # Score thresholds need a measure_name to read.
    if (
        payload.threshold_type in ("score_below", "score_above", "score_drop_abs")
        and not payload.measure_name
    ):
        raise HTTPException(
            status_code=422,
            detail=(
                f"threshold_type {payload.threshold_type!r} requires "
                "a measure_name (e.g., 'security' for PSI category, "
                "'overall_score' for the index headline)."
            ),
        )

    # Verify entity has coverage. Fail loud (404) if not — operator can
    # fix the slug or pick a different entity.
    from app.engine.coverage import get_entity_coverage
    coverage = await asyncio.to_thread(get_entity_coverage, payload.entity_slug)
    if coverage is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No Basis coverage found for entity_slug "
                f"{payload.entity_slug!r}; cannot watchlist an entity "
                "we don't track."
            ),
        )

    new_id = await asyncio.to_thread(_insert_watchlist_sync, payload)

    return JSONResponse(
        status_code=201,
        content={"watchlist_id": str(new_id)},
    )


# ─────────────────────────────────────────────────────────────────
# GET /api/engine/watchlist
# ─────────────────────────────────────────────────────────────────

def _list_watchlist_sync(
    *, entity_slug: Optional[str], active: Optional[bool]
) -> list[dict]:
    clauses: list[str] = []
    params: list[Any] = []
    if entity_slug:
        clauses.append("entity_slug = %s")
        params.append(entity_slug)
    if active is not None:
        clauses.append("active = %s")
        params.append(active)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return fetch_all(
        f"""
        SELECT id, entity_slug, index_id, threshold_type, threshold_value,
               measure_name, notes, active, last_triggered_at, created_at
        FROM engine_watchlist
        {where}
        ORDER BY created_at DESC
        """,
        tuple(params),
    )


@router.get("/api/engine/watchlist", response_model=list[WatchlistRow])
async def list_watchlist(
    request: Request,
    entity_slug: Optional[str] = Query(None),
    active: Optional[bool] = Query(None),
) -> list[WatchlistRow]:
    _check_admin_key(request)
    rows = await asyncio.to_thread(
        _list_watchlist_sync, entity_slug=entity_slug, active=active,
    )
    return [WatchlistRow.model_validate(r) for r in rows]


# ─────────────────────────────────────────────────────────────────
# DELETE /api/engine/watchlist/{watchlist_id} — soft delete
# ─────────────────────────────────────────────────────────────────

def _deactivate_watchlist_sync(watchlist_id: UUID) -> bool:
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(
            """
            UPDATE engine_watchlist
            SET active = FALSE
            WHERE id = %s
            RETURNING id
            """,
            (str(watchlist_id),),
        )
        return cur.fetchone() is not None


@router.delete("/api/engine/watchlist/{watchlist_id}")
async def deactivate_watchlist(
    request: Request, watchlist_id: UUID
) -> JSONResponse:
    _check_admin_key(request)
    ok = await asyncio.to_thread(_deactivate_watchlist_sync, watchlist_id)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail=f"No watchlist row with id {watchlist_id}",
        )
    return JSONResponse(
        status_code=200,
        content={"status": "deactivated", "id": str(watchlist_id)},
    )


# ─────────────────────────────────────────────────────────────────
# GET /api/engine/scheduler — diagnostic
# ─────────────────────────────────────────────────────────────────

@router.get("/api/engine/scheduler")
async def get_scheduler_state(request: Request) -> dict:
    """Diagnostic. Returns whether the in-process scheduler is running
    and the next-run time of each registered job. Useful to confirm
    multi-worker hygiene (the scheduler should be running in exactly
    one worker; if you see this report differently across requests,
    your worker count is wrong)."""
    _check_admin_key(request)
    return {
        "running": scheduler_is_running(),
        "pid": os.getpid(),
        "jobs": scheduler_list_jobs(),
    }
