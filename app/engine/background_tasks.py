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

Task GC safety:
  asyncio.create_task() only registers a WEAK reference in the event
  loop. If the only strong reference is the local Task variable in
  spawn_finalize_task, Python can garbage-collect the Task object the
  moment that function returns — interrupting work mid-flight. The
  S2a sleep-only task dodged this by accident (2s sleep finished
  before GC ran); the S2c background task does 5–15s of LLM work and
  hit the GC reliably, leaving rows stuck at status='pending'.

  Fix: keep a module-level set of in-flight tasks, add each task on
  spawn, remove via add_done_callback when the task completes. This
  is the standard pattern documented in the asyncio.create_task docs.

Failure handling:
  - get_or_call_interpretation already returns a fallback template on
    API failure / budget exhaustion — won't raise.
  - build_signal can raise on DB connection issues. We catch all
    exceptions, log them with the analysis_id, and flip the row to
    'draft' anyway so it doesn't stay stuck at 'pending' forever.
    The persisted signal/interpretation will be the stubs from INSERT
    time; the operator sees template:stub model_id and can re-run via
    force_new=true.
  - Pre/post-UPDATE logging makes background-task progress visible in
    Railway logs so the operator can confirm the task ran without
    polling the DB directly.

Known limitation (acknowledged in Step 0 §3 and S2a prompt): in-process
tasks don't survive worker restart. If uvicorn restarts a worker between
the INSERT and the scheduled finalization, the row stays pending forever.
S0's reaper job (separate concern) sweeps orphaned pending rows older
than 10 minutes. Not blocking for v1; tracked as a follow-up.
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


# Strong references to in-flight finalize tasks. Without this, Python
# garbage-collects asyncio.Tasks whose only reference is local to the
# spawning function — the S2c bug (PR #47) where analyses got stuck at
# status='pending' because the LLM-roundtrip-bearing task disappeared
# mid-flight. add_done_callback below removes entries on completion so
# the set doesn't grow unbounded.
_IN_FLIGHT_TASKS: set[asyncio.Task] = set()


async def finalize_analysis(analysis_id: UUID) -> None:
    """Replace the skeleton signal + interpretation with real values and
    flip status pending → draft. The slow path here is the LLM call
    inside get_or_call_interpretation — typically 5–15s on a cache miss,
    sub-second on a cache hit, near-instant on the API-unavailable
    fallback path."""
    logger.info("finalize_analysis: starting for id=%s", analysis_id)
    try:
        analysis = await get_analysis(analysis_id)
        if analysis is None:
            logger.error(
                "finalize_analysis: row %s vanished before finalization",
                analysis_id,
            )
            return
        logger.info(
            "finalize_analysis: loaded analysis id=%s entity=%s event_date=%s",
            analysis_id, analysis.entity, analysis.event_date,
        )

        # build_signal is sync (DB queries via psycopg2). Wrap in to_thread
        # so the event loop is free during the ~50–100ms of DB work.
        signal = await asyncio.to_thread(
            build_signal,
            entity=analysis.entity,
            event_date=analysis.event_date,
            peer_set=analysis.peer_set,
            coverage=analysis.coverage,
        )
        signal_obs_total = (
            len(signal.baseline) + len(signal.pre_event)
            + len(signal.event_window) + len(signal.post_event)
        )
        logger.info(
            "finalize_analysis: built signal id=%s observations=%d",
            analysis_id, signal_obs_total,
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
        logger.info(
            "finalize_analysis: built interpretation id=%s model_id=%s "
            "from_cache=%s confidence=%s",
            analysis_id, interpretation.model_id,
            interpretation.from_cache, interpretation.confidence,
        )

        logger.info(
            "finalize_analysis: about to UPDATE id=%s (signal + interpretation, status=draft)",
            analysis_id,
        )
        await update_analysis_finalization(analysis_id, signal, interpretation)
        logger.info(
            "finalize_analysis: UPDATE complete id=%s — analysis is now status=draft",
            analysis_id,
        )
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
    """Schedule finalize_analysis on the running event loop. Holds a
    strong reference in _IN_FLIGHT_TASKS so the task survives until it
    completes; the done callback removes it so the set doesn't leak.

    Public API used by analyze_router; name preserved across the
    S2a → S2c-async-fix → S2c-bg-task-fix transitions so the router
    doesn't need to change.
    """
    task = asyncio.create_task(finalize_analysis(analysis_id))
    _IN_FLIGHT_TASKS.add(task)

    def _on_done(t: asyncio.Task) -> None:
        # First: drop the strong ref so the Task is collectable now that
        # it's done. Independent of success/failure path below.
        _IN_FLIGHT_TASKS.discard(t)

        if t.cancelled():
            logger.warning(
                "finalize_analysis cancelled for id=%s", analysis_id
            )
            return
        exc = t.exception()
        if exc is not None:
            logger.exception(
                "finalize_analysis raised (caught at done-callback) for id=%s: %s",
                analysis_id, exc,
            )

    task.add_done_callback(_on_done)
    return task
