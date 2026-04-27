"""
Component 3 router: /api/engine/render + /api/engine/artifacts/* +
                    /api/engine/analyses/{id}/artifacts.

Endpoints:
  POST   /api/engine/render                                 — start a render (202)
  GET    /api/engine/artifacts/{id}                         — fetch full Artifact
  GET    /api/engine/analyses/{id}/artifacts                — list artifacts for analysis

Auth: all four routes are admin-only per Step 0 §5. Inline
_check_admin_key matches the pattern in analyze_router.py + budget_router.py
+ ops/routes.py (each module has its own copy; we don't introduce a
shared dependency module).

Gating logic on POST /render (per Step 0 §2.4):
  - artifact_type unknown to the engine → 422
  - artifact_type in recommendation.blocked → 422 (force=true cannot
    override; V9.6 constitutional)
  - artifact_type not in {recommendation.recommended} ∪ recommendation.supports
    → 422 (no path to override; supports is empty in v1)
  - Active duplicate (same analysis_id + artifact_type, status != discarded)
    → 409 unless force=true; force=true marks the duplicate discarded
    before inserting fresh.

Render itself is synchronous (string concat + INSERT). No background
task — the entire POST handler returns the persisted ArtifactResponse
in <100ms typically.
"""

from __future__ import annotations

import hmac
import logging
import os
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.engine.analysis_persistence import get_analysis
from app.engine.artifact_persistence import (
    discard_artifacts_for,
    find_active_artifact,
    get_artifact,
    list_artifacts_for,
)
from app.engine.render import (
    KNOWN_ARTIFACT_TYPES,
    UnknownArtifactType,
    render_artifact,
)
from app.engine.schemas import AnalysisType, ArtifactResponse

logger = logging.getLogger(__name__)

router = APIRouter()


# ─────────────────────────────────────────────────────────────────
# Admin-key check (mirrors app.engine.analyze_router._check_admin_key)
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
# Request body for POST /render
# ─────────────────────────────────────────────────────────────────

class RenderRequest(BaseModel):
    analysis_id: UUID
    artifact_type: AnalysisType
    force: bool = Field(
        default=False,
        description=(
            "If true and a non-discarded artifact already exists for "
            "(analysis_id, artifact_type), the existing rows are marked "
            "discarded and a fresh artifact is inserted. Cannot override "
            "constitutional blocks (incident_page on backfilled coverage)."
        ),
    )


# ─────────────────────────────────────────────────────────────────
# POST /api/engine/render
# ─────────────────────────────────────────────────────────────────

@router.post("/api/engine/render", status_code=202)
async def render(request: Request, payload: RenderRequest) -> ArtifactResponse:
    _check_admin_key(request)

    # 1. Load the target analysis
    analysis = await get_analysis(payload.analysis_id)
    if analysis is None:
        raise HTTPException(
            status_code=404,
            detail=f"No analysis with id {payload.analysis_id}",
        )
    if analysis.status == "pending":
        raise HTTPException(
            status_code=409,
            detail=(
                "Analysis is still in 'pending' state — wait for the "
                "background task to finalize signal + interpretation "
                "before rendering. Poll GET /api/engine/analyses/"
                f"{payload.analysis_id}."
            ),
        )

    # 2. Validate artifact_type and apply recommendation gate
    if payload.artifact_type not in KNOWN_ARTIFACT_TYPES:
        raise HTTPException(
            status_code=422,
            detail=(
                f"unknown artifact_type {payload.artifact_type!r}. "
                f"Valid: {sorted(KNOWN_ARTIFACT_TYPES)}"
            ),
        )

    recommendation = analysis.artifact_recommendation
    if payload.artifact_type in recommendation.blocked:
        # V9.6 constitutional — force cannot override.
        reasons = "; ".join(recommendation.blocked_reasons) or "no specific reason recorded"
        raise HTTPException(
            status_code=422,
            detail={
                "error": "artifact_type_blocked",
                "message": (
                    f"artifact_type {payload.artifact_type!r} is blocked for this "
                    f"analysis. Force=true cannot override (V9.6 constitutional). "
                    f"Reason: {reasons}"
                ),
                "blocked": list(recommendation.blocked),
                "recommended": recommendation.recommended,
                "supports": list(recommendation.supports),
            },
        )

    allowed_set: set[str] = set(recommendation.supports) | {recommendation.recommended}
    if payload.artifact_type not in allowed_set:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "artifact_type_not_recommended",
                "message": (
                    f"artifact_type {payload.artifact_type!r} is not in the "
                    f"recommendation for this analysis. Recommended: "
                    f"{recommendation.recommended}. Supports: "
                    f"{list(recommendation.supports)}."
                ),
                "recommended": recommendation.recommended,
                "supports": list(recommendation.supports),
                "blocked": list(recommendation.blocked),
            },
        )

    # 3. Dedup check
    existing = await find_active_artifact(payload.analysis_id, payload.artifact_type)
    if existing is not None:
        if not payload.force:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "active_artifact_exists",
                    "message": (
                        f"An active (non-discarded) artifact already exists for "
                        f"analysis {payload.analysis_id} and artifact_type "
                        f"{payload.artifact_type}. Either delete it or pass "
                        f"force=true to discard the existing row and render a fresh one."
                    ),
                    "existing_artifact_id": str(existing.id),
                    "existing_status": existing.status,
                },
            )
        # force=true — discard the existing artifacts before inserting fresh
        discarded_count = await discard_artifacts_for(
            payload.analysis_id, payload.artifact_type
        )
        logger.info(
            "render: force=true marked %d existing %s artifact(s) discarded "
            "for analysis_id=%s",
            discarded_count, payload.artifact_type, payload.analysis_id,
        )

    # 4. Render + persist
    try:
        artifact = await render_artifact(analysis, payload.artifact_type)
    except UnknownArtifactType as exc:
        # Already validated at step 2 — defensive belt-and-suspenders
        raise HTTPException(status_code=422, detail=str(exc))

    return artifact


# ─────────────────────────────────────────────────────────────────
# GET /api/engine/artifacts/{id}
# ─────────────────────────────────────────────────────────────────

@router.get("/api/engine/artifacts/{artifact_id}", response_model=ArtifactResponse)
async def get_one_artifact(
    request: Request, artifact_id: UUID
) -> ArtifactResponse:
    _check_admin_key(request)
    artifact = await get_artifact(artifact_id)
    if artifact is None:
        raise HTTPException(
            status_code=404,
            detail=f"No artifact with id {artifact_id}",
        )
    return artifact


# ─────────────────────────────────────────────────────────────────
# GET /api/engine/analyses/{analysis_id}/artifacts
# ─────────────────────────────────────────────────────────────────

@router.get(
    "/api/engine/analyses/{analysis_id}/artifacts",
    response_model=list[ArtifactResponse],
)
async def list_artifacts_for_analysis(
    request: Request, analysis_id: UUID
) -> list[ArtifactResponse]:
    _check_admin_key(request)
    # Confirm the analysis exists so callers get a clean 404 instead of
    # an empty list when they ask for an unknown analysis_id.
    analysis = await get_analysis(analysis_id)
    if analysis is None:
        raise HTTPException(
            status_code=404,
            detail=f"No analysis with id {analysis_id}",
        )
    return await list_artifacts_for(analysis_id)
