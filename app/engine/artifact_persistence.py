"""
Component 3: Artifact persistence — async DB layer for engine_artifacts.

Mirrors app/engine/analysis_persistence.py: sync psycopg2 helpers wrapped
in asyncio.to_thread so route handlers can await without blocking the
event loop.

S3 scope:
  - insert_artifact          POST /render writes new rows here
  - get_artifact             GET /artifacts/{id}
  - list_artifacts_for       GET /analyses/{id}/artifacts
  - find_active_artifact     dedup check inside the render endpoint
                             (status != 'discarded' for the same
                             analysis_id + artifact_type)
  - discard_artifacts_for    used by force=true to push prior duplicates
                             out of the way before INSERTing a fresh one

Uses psycopg2.extras.register_uuid() at module import (same precedent as
analysis_persistence.py) so UUIDs serialize correctly.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional
from uuid import UUID

import psycopg2.extras

from app.database import fetch_all, fetch_one, get_cursor
from app.engine.schemas import ArtifactResponse, ArtifactStatus

logger = logging.getLogger(__name__)

# Idempotent — analysis_persistence.py also calls this; both module-imports
# add the same global adapter. Belt-and-suspenders: a future refactor that
# moves the import out of analysis_persistence shouldn't break artifact
# inserts.
psycopg2.extras.register_uuid()


_ARTIFACT_COLUMNS = """
    id, analysis_id, artifact_type, rendered_at, content_markdown,
    suggested_path, suggested_url, status, published_url, warnings
"""


def _row_to_artifact(row: dict) -> ArtifactResponse:
    return ArtifactResponse(
        id=row["id"],
        analysis_id=row["analysis_id"],
        artifact_type=row["artifact_type"],
        rendered_at=row["rendered_at"],
        content_markdown=row["content_markdown"],
        suggested_path=row["suggested_path"],
        suggested_url=row["suggested_url"],
        status=row["status"],
        published_url=row["published_url"],
        warnings=row["warnings"] or [],
    )


# ─────────────────────────────────────────────────────────────────
# Insert
# ─────────────────────────────────────────────────────────────────

def _insert_artifact_sync(artifact: ArtifactResponse) -> None:
    warnings_json = psycopg2.extras.Json(list(artifact.warnings))
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO engine_artifacts (
                id, analysis_id, artifact_type, rendered_at,
                content_markdown, suggested_path, suggested_url,
                status, published_url, warnings
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            """,
            (
                artifact.id,
                artifact.analysis_id,
                artifact.artifact_type,
                artifact.rendered_at,
                artifact.content_markdown,
                artifact.suggested_path,
                artifact.suggested_url,
                artifact.status,
                artifact.published_url,
                warnings_json,
            ),
        )


async def insert_artifact(artifact: ArtifactResponse) -> None:
    await asyncio.to_thread(_insert_artifact_sync, artifact)


# ─────────────────────────────────────────────────────────────────
# Read
# ─────────────────────────────────────────────────────────────────

def _get_artifact_sync(artifact_id: UUID) -> Optional[ArtifactResponse]:
    row = fetch_one(
        f"SELECT {_ARTIFACT_COLUMNS} FROM engine_artifacts WHERE id = %s",
        (str(artifact_id),),
    )
    if row is None:
        return None
    return _row_to_artifact(row)


async def get_artifact(artifact_id: UUID) -> Optional[ArtifactResponse]:
    return await asyncio.to_thread(_get_artifact_sync, artifact_id)


def _list_artifacts_for_sync(analysis_id: UUID) -> list[ArtifactResponse]:
    rows = fetch_all(
        f"""
        SELECT {_ARTIFACT_COLUMNS}
        FROM engine_artifacts
        WHERE analysis_id = %s
        ORDER BY rendered_at DESC
        """,
        (str(analysis_id),),
    )
    return [_row_to_artifact(r) for r in rows]


async def list_artifacts_for(analysis_id: UUID) -> list[ArtifactResponse]:
    return await asyncio.to_thread(_list_artifacts_for_sync, analysis_id)


# ─────────────────────────────────────────────────────────────────
# Active-artifact lookup (for dedup gate in POST /render)
# ─────────────────────────────────────────────────────────────────

def _find_active_artifact_sync(
    analysis_id: UUID, artifact_type: str
) -> Optional[ArtifactResponse]:
    """An "active" artifact is one whose status isn't 'discarded'. The
    render endpoint uses this to enforce one-active-artifact-per-type
    per-analysis: 409 if found and force=false; force=true marks any
    matching rows discarded before INSERTing fresh."""
    row = fetch_one(
        f"""
        SELECT {_ARTIFACT_COLUMNS}
        FROM engine_artifacts
        WHERE analysis_id = %s
          AND artifact_type = %s
          AND status <> 'discarded'
        ORDER BY rendered_at DESC
        LIMIT 1
        """,
        (str(analysis_id), artifact_type),
    )
    if row is None:
        return None
    return _row_to_artifact(row)


async def find_active_artifact(
    analysis_id: UUID, artifact_type: str
) -> Optional[ArtifactResponse]:
    return await asyncio.to_thread(
        _find_active_artifact_sync, analysis_id, artifact_type
    )


def _discard_artifacts_for_sync(
    analysis_id: UUID, artifact_type: str
) -> int:
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE engine_artifacts
            SET status = 'discarded'
            WHERE analysis_id = %s
              AND artifact_type = %s
              AND status <> 'discarded'
            """,
            (str(analysis_id), artifact_type),
        )
        return cur.rowcount


async def discard_artifacts_for(
    analysis_id: UUID, artifact_type: str
) -> int:
    """Mark every non-discarded artifact for (analysis_id, artifact_type)
    as discarded. Returns the row count touched. Called from the render
    endpoint when force=true is set and a duplicate exists."""
    return await asyncio.to_thread(
        _discard_artifacts_for_sync, analysis_id, artifact_type
    )


# ─────────────────────────────────────────────────────────────────
# Test cleanup helper — used by tests/test_engine_renderers.py
# ─────────────────────────────────────────────────────────────────

def _delete_artifacts_for_analysis_sync(analysis_id: UUID) -> int:
    with get_cursor() as cur:
        cur.execute(
            "DELETE FROM engine_artifacts WHERE analysis_id = %s",
            (str(analysis_id),),
        )
        return cur.rowcount


async def delete_artifacts_for_analysis(analysis_id: UUID) -> int:
    """Hard-delete every artifact for an analysis. Used by test cleanup
    so the FK on engine_artifacts.analysis_id doesn't block the
    subsequent engine_analyses delete."""
    return await asyncio.to_thread(
        _delete_artifacts_for_analysis_sync, analysis_id
    )
