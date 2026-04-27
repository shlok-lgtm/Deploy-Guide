"""
Component 3: Render orchestrator.

Single public entry: render_artifact(analysis, artifact_type) returns the
rendered ArtifactResponse and persists it to engine_artifacts. Called by
the POST /api/engine/render handler after the gating check.

The renderer registry maps artifact_type → renderer function. Adding a
new artifact type means adding one entry here AND a renderer module.
There's no plugin architecture or dynamic discovery — the registry is
intentionally explicit so a reader can see the full surface in one
place.

Synchronous rendering. No LLM calls (those are upstream in C2c). A
typical render takes <50ms — string concatenation + a single INSERT.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable
from uuid import uuid4

from app.engine.artifact_persistence import insert_artifact
from app.engine.renderers.case_study import render_case_study
from app.engine.renderers.incident_page import render_incident_page
from app.engine.renderers.internal_memo import render_internal_memo
from app.engine.renderers.one_pager import render_one_pager
from app.engine.renderers.retrospective_internal import render_retrospective_internal
from app.engine.renderers.talking_points import render_talking_points
from app.engine.schemas import Analysis, AnalysisType, ArtifactResponse

logger = logging.getLogger(__name__)


# Registry — single source of truth for renderable artifact types. Each
# entry is a pure function: Analysis → RenderedArtifact (a dataclass with
# content_markdown, suggested_path, suggested_url).
_RENDERERS: dict[str, Callable] = {
    "incident_page": render_incident_page,
    "retrospective_internal": render_retrospective_internal,
    "case_study": render_case_study,
    "internal_memo": render_internal_memo,
    "talking_points": render_talking_points,
    "one_pager": render_one_pager,
}


# The set of types known to the engine. Used by the render endpoint to
# distinguish "unknown type" (422) from "blocked by recommendation"
# (also 422 but with a different detail).
KNOWN_ARTIFACT_TYPES: set[str] = set(_RENDERERS.keys())


class UnknownArtifactType(ValueError):
    """Raised when artifact_type isn't in the registry. Endpoint maps to
    422 with a message listing the valid types."""


async def render_artifact(
    analysis: Analysis,
    artifact_type: str,
) -> ArtifactResponse:
    """Render the artifact, persist it, return the ArtifactResponse.

    The endpoint is responsible for gating (recommendation/blocked
    checks); this function assumes the gate has passed.
    """
    if artifact_type not in _RENDERERS:
        raise UnknownArtifactType(
            f"unknown artifact_type {artifact_type!r}; "
            f"valid types: {sorted(KNOWN_ARTIFACT_TYPES)}"
        )

    renderer = _RENDERERS[artifact_type]
    rendered = renderer(analysis)
    logger.info(
        "render_artifact: rendered %s for analysis_id=%s "
        "(markdown_chars=%d, suggested_path=%s)",
        artifact_type, analysis.id,
        len(rendered.content_markdown),
        rendered.suggested_path,
    )

    artifact = ArtifactResponse(
        id=uuid4(),
        analysis_id=analysis.id,
        artifact_type=artifact_type,  # type: ignore[arg-type]
        rendered_at=datetime.now(timezone.utc),
        content_markdown=rendered.content_markdown,
        suggested_path=rendered.suggested_path,
        suggested_url=rendered.suggested_url,
        status="draft",
        published_url=None,
        warnings=[],
    )
    await insert_artifact(artifact)
    logger.info(
        "render_artifact: persisted artifact_id=%s analysis_id=%s type=%s",
        artifact.id, analysis.id, artifact_type,
    )
    return artifact
