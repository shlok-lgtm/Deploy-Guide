"""
Component 2: Background tasks.

S2c-async-fix: replaces the S2a sleep-only stub with real finalization.
The POST /api/engine/analyze handler INSERTs a skeleton row with stub
signal + stub interpretation at status='pending' and returns 202 in <1s.
This task does the heavy work asynchronously:

  1. Load the persisted Analysis (gives us coverage, entity, event_date,
     peer_set, context — everything needed for finalization)
  2. Build real signal via app.engine.observation_builder.build_signal
  3. Build real interpretation via
     app.engine.interpretation.get_or_call_interpretation (LLM call or
     cache hit; fallback template on API failure or budget exhaustion)
  4. UPDATE the row with real signal + interpretation, flip status
     pending → draft

Spawned via asyncio.create_task() from the async POST handler. Runs in
the same event loop as the request handler; no external queue.

Failure handling:
  - get_or_call_interpretation already returns a fallback template on
    API failure / budget exhaustion — won't raise.
  - build_signal can raise on DB connection issues. We catch all
    exceptions, log them with the analysis_id, and flip the row to
    'draft' anyway so it doesn't stay stuck at 'pending' forever.
    The persisted signal/interpretation will be the stubs from INSERT
    time; the operator sees template:stub model_id and can re-run via
    force_new=true.

Known limitation (acknowledged in Step 0 §3 and S2a prompt): in-process
tasks don't survive worker restart. If uvicorn restarts a worker between
the INSERT and the scheduled finalization, the row stays pending forever.
S0's reaper job (separate concern) sweeps orphaned pending rows older
than 10 minutes.

Exceptions from the background task are caught and logged via an
add_done_callback so they don't vanish silently.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from app.engine.analysis_persistence import (
    get_analysis,
    update_analysis_finalization,
    update_analysis_status,
)
from app.engine.interpretation import get_or_call_interpretation
from app.engine.observation_builder import build_signal

logger = logging.getLogger(__name__)


async def finalize_analysis(analysis_id: UUID) -> None:
    """Replace the skeleton signal + interpretation with real values and
    flip status pending → draft. The slow path here is the LLM call
    inside get_or_call_interpretation — typically 5–15s on a cache miss,
    sub-second on a cache hit, near-instant on the API-unavailable
    fallback path."""
    try:
        analysis = await get_analysis(analysis_id)
        if analysis is None:
            logger.error(
                "finalize_analysis: row %s vanished before finalization",
                analysis_id,
            )
            return

        # build_signal is sync (DB queries via psycopg2). Wrap in to_thread
        # so the event loop is free during the ~50–100ms of DB work.
        signal = await asyncio.to_thread(
            build_signal,
            entity=analysis.entity,
            event_date=analysis.event_date,
            peer_set=analysis.peer_set,
            coverage=analysis.coverage,
        )

        # get_or_call_interpretation is also sync — wraps an Anthropic
        # SDK call. Wrap in to_thread for the same reason.
        interpretation = await asyncio.to_thread(
            get_or_call_interpretation,
            entity=analysis.entity,
            event_date=analysis.event_date,
            peer_set=analysis.peer_set,
            coverage=analysis.coverage,
            signal=signal,
            context=analysis.context,
        )

        await update_analysis_finalization(analysis_id, signal, interpretation)
    except Exception as exc:
        logger.exception(
            "finalize_analysis: failed for id=%s: %s — flipping to "
            "draft with stub data so the row isn't stuck at pending",
            analysis_id, exc,
        )
        # Best-effort: don't leave the row stuck. Stubs persist; operator
        # can retry via force_new=true.
        try:
            await update_analysis_status(
                analysis_id,
                new_status="draft",
                review_notes=f"finalize_analysis failed: {type(exc).__name__}",
            )
        except Exception:
            logger.exception(
                "finalize_analysis: status flip also failed for id=%s",
                analysis_id,
            )


def spawn_finalize_task(analysis_id: UUID) -> asyncio.Task:
    """Schedule finalize_analysis on the running event loop and attach a
    done-callback that logs exceptions. Public API used by analyze_router;
    name preserved across the S2a → S2c-async-fix transition so the
    router doesn't need to change."""
    task = asyncio.create_task(finalize_analysis(analysis_id))

    def _on_done(t: asyncio.Task) -> None:
        if t.cancelled():
            logger.warning(
                "finalize_analysis cancelled for id=%s", analysis_id
            )
            return
        exc = t.exception()
        if exc is not None:
            logger.exception(
                "finalize_analysis failed for id=%s: %s", analysis_id, exc
            )

    task.add_done_callback(_on_done)
    return task
