"""
Component 4: APScheduler setup.

Two scheduled jobs run inside the api-server uvicorn worker:

  poll_defillama_hacks   — every 15 minutes
  evaluate_watchlist     — every 15 minutes (offset start so the two jobs
                            don't overlap on the first tick)

Started via @app.on_event("startup") in app/server.py and stopped on
shutdown. Module-level singleton so re-init is a no-op.

⚠ Multi-worker note ⚠

This scheduler runs IN-PROCESS. If uvicorn spawns multiple workers,
each worker runs its own copy of the scheduler — DeFiLlama gets polled
N times every 15 min and watchlist crossings fire N analyses per
event. Both endpoints are idempotent at the DB layer (engine_events
has a unique constraint), so duplicate triggers won't create
duplicate rows; but the wasted API calls and log noise scale with N.

Mitigation for v1: set Railway env var WEB_CONCURRENCY=1 so uvicorn
runs a single worker. The api-server's traffic doesn't yet warrant
horizontal scaling.

When traffic grows enough to need multi-worker:
  - Move the scheduler into a separate Railway service (worker-only),
    or
  - Use a Postgres advisory lock around each scheduled tick so only
    one worker actually runs the job per cycle.

The startup hook logs the worker's PID so multi-process duplication is
visible in Railway logs even if the env var slips.
"""

from __future__ import annotations

import logging
import os

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.engine.event_sources.defillama_hacks import poll_defillama_hacks
from app.engine.watchlist import evaluate_watchlist

logger = logging.getLogger(__name__)


# Module-level singleton. Holding a reference here also keeps the
# scheduler alive (apscheduler doesn't keep its own root reference).
_SCHEDULER: AsyncIOScheduler | None = None


# Cadence is environment-overridable so the operator can throttle without
# a deploy if DeFiLlama starts rate-limiting or if the watchlist gets
# expensive.
def _interval_minutes(env_var: str, default: int) -> int:
    raw = os.environ.get(env_var)
    if raw is None:
        return default
    try:
        v = int(raw)
        return v if v > 0 else default
    except ValueError:
        logger.warning(
            "scheduler: invalid integer in %s=%r; using default %s",
            env_var, raw, default,
        )
        return default


async def start_scheduler() -> None:
    """Initialize and start the engine scheduler. No-op if already
    started — safe to call from multiple startup hooks."""
    global _SCHEDULER
    if _SCHEDULER is not None:
        logger.info("engine scheduler: already started, skipping")
        return

    poll_minutes = _interval_minutes("BASIS_ENGINE_DEFILLAMA_POLL_MINUTES", 15)
    watch_minutes = _interval_minutes("BASIS_ENGINE_WATCHLIST_POLL_MINUTES", 15)

    scheduler = AsyncIOScheduler(timezone="UTC")

    # DeFiLlama hacks polling. max_instances=1 prevents overlap if a
    # single poll exceeds the interval (rare — typical poll <30s).
    scheduler.add_job(
        poll_defillama_hacks,
        "interval",
        minutes=poll_minutes,
        id="poll_defillama_hacks",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Watchlist evaluator. Offset the first run by a few minutes so it
    # doesn't collide with the DeFiLlama poll on startup; subsequent
    # runs interleave naturally on the 15-min cadence.
    scheduler.add_job(
        evaluate_watchlist,
        "interval",
        minutes=watch_minutes,
        id="evaluate_watchlist",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()
    _SCHEDULER = scheduler
    logger.info(
        "engine scheduler: started in pid=%s (poll_defillama=%dm, "
        "evaluate_watchlist=%dm). If you see this log line N times, "
        "set WEB_CONCURRENCY=1 to avoid duplicate scheduling.",
        os.getpid(), poll_minutes, watch_minutes,
    )


async def stop_scheduler() -> None:
    """Shut down the scheduler cleanly. Safe to call when not running."""
    global _SCHEDULER
    if _SCHEDULER is None:
        return
    try:
        _SCHEDULER.shutdown(wait=False)
        logger.info("engine scheduler: stopped")
    except Exception:
        logger.exception("engine scheduler: error during shutdown")
    finally:
        _SCHEDULER = None


# ─────────────────────────────────────────────────────────────────
# Diagnostic helpers — used by the events_router /admin/run endpoints
# ─────────────────────────────────────────────────────────────────

def is_running() -> bool:
    return _SCHEDULER is not None


def list_jobs() -> list[dict]:
    """Return the current job list for the /api/engine/scheduler
    diagnostic endpoint. Empty list when not running."""
    if _SCHEDULER is None:
        return []
    out: list[dict] = []
    for job in _SCHEDULER.get_jobs():
        out.append({
            "id": job.id,
            "next_run_time": (
                job.next_run_time.isoformat() if job.next_run_time else None
            ),
            "trigger": str(job.trigger),
        })
    return out
