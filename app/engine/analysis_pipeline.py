"""
Component 4: Internal entry point for triggering an analysis.

Mirrors the steps inside POST /api/engine/analyze (analyze_router.py)
without going through HTTP / FastAPI / auth. Used by the C4 event
pipeline so that:

  - Auto-triggered analyses bypass rate limits (the engine itself is
    the caller; rate-limiting it makes no sense)
  - Auto-triggered analyses bypass admin-key auth (the trigger path
    is internal, not user-facing)
  - The router stays untouched per the C2 contract, but the canonical
    "create an Analysis from inputs" logic is reusable

This module is intentionally narrow:
  - One public async function: trigger_analysis(...)
  - Same semantics as the router for coverage check, dedup, force_new,
    background-task spawn
  - Returns the analysis_id on success or None when the entity has no
    coverage (caller marks the event as no_coverage and skips)

Future cleanup: analyze_router.py could call this same function for
the request-handler path. Out of scope for C4.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional
from uuid import UUID

from app.engine.analysis import build_stub_analysis, fetch_coverage
from app.engine.analysis_persistence import (
    archive_analysis,
    find_active_analysis,
    insert_analysis,
    link_superseded_by,
)
from app.engine.background_tasks import spawn_finalize_task

logger = logging.getLogger(__name__)


class TriggerResult:
    """Outcome bundle for trigger_analysis. Lightweight dataclass-like
    container; the caller maps these into engine_events row updates."""

    __slots__ = ("analysis_id", "outcome", "detail")

    def __init__(self, analysis_id: Optional[UUID], outcome: str, detail: str = ""):
        self.analysis_id = analysis_id
        self.outcome = outcome  # "created" | "existing" | "no_coverage" | "error"
        self.detail = detail


async def trigger_analysis(
    *,
    entity: str,
    event_date: Optional[date],
    peer_set: Optional[list[str]] = None,
    context: Optional[str] = None,
    force_new: bool = False,
) -> TriggerResult:
    """Auto-trigger an analysis. Async; safe to call from the event
    pipeline or scheduler-driven evaluators.

    Behavior:
      - Resolves entity slug via fetch_coverage (handles fuzzy match).
      - If no coverage: returns TriggerResult(None, "no_coverage").
      - If an active analysis already exists for (entity, event_date)
        and force_new is False: returns the existing analysis_id with
        outcome="existing" — caller can link the event to that ID
        rather than creating a duplicate.
      - If force_new is True: archives any existing active analysis
        and creates a new one, doubly-linked via previous_analysis_id /
        superseded_by_id (matches the manual force_new path in the
        router).
      - Insert is at status='pending'; the background finalize task
        builds real signal + interpretation and flips to 'draft'.
    """
    if peer_set is None:
        peer_set = []

    coverage = await fetch_coverage(entity)
    if coverage is None:
        logger.info(
            "trigger_analysis: no coverage for entity=%r — skipping", entity,
        )
        return TriggerResult(
            analysis_id=None,
            outcome="no_coverage",
            detail=f"No Basis coverage for entity {entity!r}",
        )

    canonical_entity = coverage.identifier  # normalize (fuzzy match handled here)

    existing = await find_active_analysis(canonical_entity, event_date)
    previous_analysis_id: Optional[UUID] = None
    supersedes_reason: Optional[str] = None

    if existing is not None and not force_new:
        logger.info(
            "trigger_analysis: existing active analysis %s for %s/%s — reusing",
            existing.id, canonical_entity, event_date,
        )
        return TriggerResult(
            analysis_id=existing.id,
            outcome="existing",
            detail="reused existing active analysis",
        )

    if existing is not None and force_new:
        await archive_analysis(
            existing.id, reason="auto-trigger superseded existing analysis"
        )
        previous_analysis_id = existing.id
        supersedes_reason = "auto-trigger force_new from C4 event pipeline"

    analysis_create = build_stub_analysis(
        entity=canonical_entity,
        peer_set=peer_set,
        event_date=event_date,
        context=context,
        coverage=coverage,
        previous_analysis_id=previous_analysis_id,
        supersedes_reason=supersedes_reason,
    )

    new_id = await insert_analysis(analysis_create, status="pending")

    if previous_analysis_id is not None:
        await link_superseded_by(previous_analysis_id, new_id)

    # Spawn background task. Same pattern as the router — task held in
    # the module-level set inside background_tasks.py to survive GC.
    spawn_finalize_task(new_id)

    logger.info(
        "trigger_analysis: created analysis_id=%s for %s/%s "
        "(force_new=%s, previous=%s)",
        new_id, canonical_entity, event_date, force_new, previous_analysis_id,
    )
    return TriggerResult(
        analysis_id=new_id,
        outcome="created",
        detail=f"created new analysis for {canonical_entity}/{event_date}",
    )
