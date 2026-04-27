"""
Component 2: Analysis persistence — async DB layer for engine_analyses.

Wraps the existing synchronous psycopg2 helpers in app.database with
asyncio.to_thread so route handlers can await without blocking the event
loop. Keeps the sync DB convention intact while fitting FastAPI's async
handler style.

All Pydantic → JSONB serialization goes through model.model_dump(mode='json')
which normalizes dates/datetimes/UUIDs to JSON-safe primitives. All JSONB →
Pydantic deserialization uses Model.model_validate(dict) which also handles
the inverse normalization.

S2a scope: operations to support POST /analyze, GET /analyses/{id},
GET /analyses, DELETE /analyses/{id}, plus the force_new archival path.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime, timezone
from typing import Optional
from uuid import UUID

import psycopg2.extras

from app.database import fetch_all, fetch_one, get_cursor
from app.engine.schemas import (
    Analysis,
    AnalysisCreate,
    AnalysisStatus,
    AnalysisSummary,
)

logger = logging.getLogger(__name__)

# Register psycopg2's UUID adapter at module import time so cursor.execute
# can accept Python uuid.UUID objects directly as query parameters. Without
# this, INSERTs with a non-None previous_analysis_id raise
# ProgrammingError: can't adapt type 'UUID' — observed in production when
# the force_new=true archival path fires. register_uuid() is idempotent and
# registers a global adapter; safe to call here. Regression guard:
# tests/test_engine_analyze.py::test_analyze_force_new_archives_previous_uuid_adapter.
psycopg2.extras.register_uuid()


# ─────────────────────────────────────────────────────────────────
# Row → Analysis conversion
# ─────────────────────────────────────────────────────────────────

_ANALYSIS_COLUMNS = """
    id, created_at, analysis_version, entity, event_date, peer_set,
    context, coverage, signal, interpretation, methodology_observations,
    follow_ups, artifact_recommendation, inputs_hash, previous_analysis_id,
    superseded_by_id, supersedes_reason, archived_at, status, human_reviewer,
    review_notes
"""


def _row_to_analysis(row: dict) -> Analysis:
    """Convert a psycopg2 RealDictCursor row into an Analysis model.

    psycopg2 already decodes JSONB columns to Python dicts/lists, so we can
    hand them straight to Pydantic without re-parsing JSON.
    """
    payload = {
        "id": row["id"],
        "created_at": row["created_at"],
        "analysis_version": row["analysis_version"],
        "entity": row["entity"],
        "event_date": row["event_date"],
        "peer_set": row["peer_set"] or [],
        "context": row["context"],
        "coverage": row["coverage"],
        "signal": row["signal"],
        "interpretation": row["interpretation"],
        "methodology_observations": row["methodology_observations"] or [],
        "follow_ups": row["follow_ups"] or [],
        "artifact_recommendation": row["artifact_recommendation"],
        "inputs_hash": row["inputs_hash"],
        "previous_analysis_id": row["previous_analysis_id"],
        "superseded_by_id": row["superseded_by_id"],
        "supersedes_reason": row["supersedes_reason"],
        "archived_at": row["archived_at"],
        "status": row["status"],
        "human_reviewer": row["human_reviewer"],
        "review_notes": row["review_notes"],
    }
    return Analysis.model_validate(payload)


def _summary_row_to_summary(row: dict) -> AnalysisSummary:
    """Convert a compact row into AnalysisSummary. Reads the `recommended`
    key out of the artifact_recommendation JSONB and the `headline` +
    `confidence` keys out of the interpretation JSONB without loading the
    full objects."""
    recommendation = row["artifact_recommendation"] or {}
    interpretation = row["interpretation"] or {}
    return AnalysisSummary(
        id=row["id"],
        entity=row["entity"],
        event_date=row["event_date"],
        created_at=row["created_at"],
        status=row["status"],
        recommended_artifact_type=recommendation.get("recommended", "nothing"),
        confidence=interpretation.get("confidence", "insufficient"),
        headline=interpretation.get("headline", ""),
        previous_analysis_id=row["previous_analysis_id"],
        superseded_by_id=row["superseded_by_id"],
    )


# ─────────────────────────────────────────────────────────────────
# Insert
# ─────────────────────────────────────────────────────────────────

def _insert_analysis_sync(analysis: AnalysisCreate, status: AnalysisStatus) -> UUID:
    """Synchronous INSERT. Returns the new id. Caller is responsible for
    running this in a thread via asyncio.to_thread."""
    # Serialize Pydantic nested objects to JSON-compatible dicts, then wrap
    # with psycopg2.extras.Json so the driver inserts them as JSONB.
    coverage_json = psycopg2.extras.Json(analysis.coverage.model_dump(mode="json"))
    signal_json = psycopg2.extras.Json(analysis.signal.model_dump(mode="json"))
    interpretation_json = psycopg2.extras.Json(
        analysis.interpretation.model_dump(mode="json")
    )
    methodology_json = psycopg2.extras.Json(
        [m.model_dump(mode="json") for m in analysis.methodology_observations]
    )
    follow_ups_json = psycopg2.extras.Json(
        [f.model_dump(mode="json") for f in analysis.follow_ups]
    )
    artifact_rec_json = psycopg2.extras.Json(
        analysis.artifact_recommendation.model_dump(mode="json")
    )
    peer_set_json = psycopg2.extras.Json(list(analysis.peer_set))

    with get_cursor(dict_cursor=True) as cur:
        cur.execute(
            """
            INSERT INTO engine_analyses (
                analysis_version, entity, event_date, peer_set, context,
                coverage, signal, interpretation, methodology_observations,
                follow_ups, artifact_recommendation, inputs_hash,
                previous_analysis_id, supersedes_reason, status
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            RETURNING id
            """,
            (
                analysis.analysis_version,
                analysis.entity,
                analysis.event_date,
                peer_set_json,
                analysis.context,
                coverage_json,
                signal_json,
                interpretation_json,
                methodology_json,
                follow_ups_json,
                artifact_rec_json,
                analysis.inputs_hash,
                analysis.previous_analysis_id,
                analysis.supersedes_reason,
                status,
            ),
        )
        row = cur.fetchone()
        return row["id"]


async def insert_analysis(
    analysis: AnalysisCreate,
    status: AnalysisStatus = "pending",
) -> UUID:
    return await asyncio.to_thread(_insert_analysis_sync, analysis, status)


# ─────────────────────────────────────────────────────────────────
# Read
# ─────────────────────────────────────────────────────────────────

def _get_analysis_sync(analysis_id: UUID) -> Optional[Analysis]:
    row = fetch_one(
        f"SELECT {_ANALYSIS_COLUMNS} FROM engine_analyses WHERE id = %s",
        (str(analysis_id),),
    )
    if row is None:
        return None
    return _row_to_analysis(row)


async def get_analysis(analysis_id: UUID) -> Optional[Analysis]:
    return await asyncio.to_thread(_get_analysis_sync, analysis_id)


# ─────────────────────────────────────────────────────────────────
# List
# ─────────────────────────────────────────────────────────────────

def _list_analyses_sync(
    entity: Optional[str],
    status: Optional[str],
    limit: int,
    offset: int,
) -> list[AnalysisSummary]:
    clauses = []
    params: list = []
    if entity:
        clauses.append("entity = %s")
        params.append(entity)
    if status:
        clauses.append("status = %s")
        params.append(status)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.extend([limit, offset])
    rows = fetch_all(
        f"""
        SELECT id, entity, event_date, created_at, status,
               interpretation, artifact_recommendation,
               previous_analysis_id, superseded_by_id
        FROM engine_analyses
        {where}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
        """,
        tuple(params),
    )
    return [_summary_row_to_summary(r) for r in rows]


async def list_analyses(
    entity: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> list[AnalysisSummary]:
    limit = max(1, min(limit, 100))
    offset = max(0, offset)
    return await asyncio.to_thread(
        _list_analyses_sync, entity, status, limit, offset
    )


# ─────────────────────────────────────────────────────────────────
# Uniqueness lookup (for POST /analyze idempotency)
# ─────────────────────────────────────────────────────────────────

def _find_active_analysis_sync(
    entity: str, event_date: Optional[date]
) -> Optional[Analysis]:
    if event_date is None:
        row = fetch_one(
            f"""
            SELECT {_ANALYSIS_COLUMNS} FROM engine_analyses
            WHERE entity = %s AND event_date IS NULL AND status <> 'archived'
            LIMIT 1
            """,
            (entity,),
        )
    else:
        row = fetch_one(
            f"""
            SELECT {_ANALYSIS_COLUMNS} FROM engine_analyses
            WHERE entity = %s AND event_date = %s AND status <> 'archived'
            LIMIT 1
            """,
            (entity, event_date),
        )
    if row is None:
        return None
    return _row_to_analysis(row)


async def find_active_analysis(
    entity: str, event_date: Optional[date]
) -> Optional[Analysis]:
    return await asyncio.to_thread(_find_active_analysis_sync, entity, event_date)


# ─────────────────────────────────────────────────────────────────
# Status updates
# ─────────────────────────────────────────────────────────────────

def _update_analysis_status_sync(
    analysis_id: UUID, new_status: str, review_notes: Optional[str]
) -> None:
    with get_cursor() as cur:
        if review_notes is None:
            cur.execute(
                "UPDATE engine_analyses SET status = %s WHERE id = %s",
                (new_status, str(analysis_id)),
            )
        else:
            cur.execute(
                """
                UPDATE engine_analyses
                SET status = %s, review_notes = %s
                WHERE id = %s
                """,
                (new_status, review_notes, str(analysis_id)),
            )


async def update_analysis_status(
    analysis_id: UUID,
    new_status: str,
    review_notes: Optional[str] = None,
) -> None:
    await asyncio.to_thread(
        _update_analysis_status_sync, analysis_id, new_status, review_notes
    )


# ─────────────────────────────────────────────────────────────────
# Background-task finalization
#
# Atomically swaps the stub signal + stub interpretation with real values
# and flips status pending → draft. Used by background_tasks.finalize_analysis
# after build_signal + get_or_call_interpretation complete.
# ─────────────────────────────────────────────────────────────────

def _update_analysis_finalization_sync(
    analysis_id: UUID,
    signal: "Signal",  # forward type — avoids extra import
    interpretation: "Interpretation",
    artifact_recommendation: "ArtifactRecommendation",
) -> None:
    from app.engine.schemas import (  # local to defer cycle
        ArtifactRecommendation,
        Interpretation,
        Signal,
    )
    signal_json = psycopg2.extras.Json(signal.model_dump(mode="json"))
    interp_json = psycopg2.extras.Json(interpretation.model_dump(mode="json"))
    rec_json = psycopg2.extras.Json(
        artifact_recommendation.model_dump(mode="json")
    )
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE engine_analyses
            SET signal = %s,
                interpretation = %s,
                artifact_recommendation = %s,
                status = 'draft'
            WHERE id = %s
            """,
            (signal_json, interp_json, rec_json, str(analysis_id)),
        )


async def update_analysis_finalization(
    analysis_id: UUID,
    signal,
    interpretation,
    artifact_recommendation,
) -> None:
    """Replace the stub signal + interpretation + artifact_recommendation
    with real values and flip status pending → draft, in a single UPDATE.
    All three derived fields land atomically so a reader never sees a
    half-finalized row (real signal, stub recommendation, etc.)."""
    await asyncio.to_thread(
        _update_analysis_finalization_sync,
        analysis_id, signal, interpretation, artifact_recommendation,
    )


# ─────────────────────────────────────────────────────────────────
# Archive (for force_new flow) + link superseded_by_id on old row
# ─────────────────────────────────────────────────────────────────

def _archive_analysis_sync(analysis_id: UUID, reason: str) -> None:
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE engine_analyses
            SET status = 'archived',
                archived_at = NOW(),
                supersedes_reason = COALESCE(supersedes_reason, %s)
            WHERE id = %s
            """,
            (reason, str(analysis_id)),
        )


async def archive_analysis(analysis_id: UUID, reason: str) -> None:
    await asyncio.to_thread(_archive_analysis_sync, analysis_id, reason)


def _link_superseded_by_sync(old_id: UUID, new_id: UUID) -> None:
    with get_cursor() as cur:
        cur.execute(
            "UPDATE engine_analyses SET superseded_by_id = %s WHERE id = %s",
            (str(new_id), str(old_id)),
        )


async def link_superseded_by(old_id: UUID, new_id: UUID) -> None:
    await asyncio.to_thread(_link_superseded_by_sync, old_id, new_id)


# ─────────────────────────────────────────────────────────────────
# Delete (cleanup / operator correction)
#
# Refuses to delete if any engine_artifacts rows reference this analysis.
# S2a doesn't create artifacts, so this is a defensive guard for later
# stages. Returns one of:
#   "deleted"         — row removed
#   "not_found"       — no row with that id
#   "has_artifacts"   — FK references prevent deletion
# ─────────────────────────────────────────────────────────────────

def _delete_analysis_sync(analysis_id: UUID) -> str:
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM engine_artifacts WHERE analysis_id = %s",
            (str(analysis_id),),
        )
        row = cur.fetchone()
        if row and row["n"] > 0:
            return "has_artifacts"

        cur.execute(
            "DELETE FROM engine_analyses WHERE id = %s RETURNING id",
            (str(analysis_id),),
        )
        deleted = cur.fetchone()
        return "deleted" if deleted else "not_found"


async def delete_analysis(analysis_id: UUID) -> str:
    return await asyncio.to_thread(_delete_analysis_sync, analysis_id)
