"""
Cancellation Watchdog
=====================
Detects asyncio tasks stuck in CANCELLING state. When asyncio cancels a task
blocked in sync code (psycopg2, requests, subprocess), cancellation can't
propagate through C-level blocking calls. The task wedges, holds the event
loop hostage, and everything else starves.

This watchdog scans every 30s. Any task cancelling for >2min gets logged
with its coroutine location. Any task cancelling for >5min triggers process
exit so Railway/container orchestrator restarts the service.

Usage:
    import asyncio
    from app.lib.watchdog import cancellation_watchdog

    asyncio.create_task(cancellation_watchdog(), name="cancellation_watchdog")

Note: This only covers Python asyncio processes. The keeper service is
TypeScript/Node.js and requires a separate liveness mechanism (see
/api/health/keeper endpoint and check_keeper_freshness in health_checker.py).
"""

import asyncio
import logging
import os
import time

logger = logging.getLogger(__name__)


async def cancellation_watchdog():
    """Detect tasks stuck in CANCELLING state.

    Logs WEDGED TASK alert at 2min, force-exits at 5min for auto-restart.
    """
    cancelling_since: dict[int, float] = {}
    while True:
        try:
            await asyncio.sleep(30)
            now = time.time()
            live_ids = set()

            for t in asyncio.all_tasks():
                tid = id(t)
                live_ids.add(tid)
                state = getattr(t, "_state", None)

                if state == "CANCELLING":
                    first_seen = cancelling_since.setdefault(tid, now)
                    age = now - first_seen

                    if age > 120:
                        coro = t.get_coro()
                        loc = "unknown"
                        try:
                            if coro and coro.cr_frame:
                                loc = f"{coro.cr_code.co_filename}:{coro.cr_frame.f_lineno}"
                        except Exception:
                            pass

                        logger.error(
                            f"WEDGED TASK: name={t.get_name()} "
                            f"cancelling_for={age:.0f}s coro_at={loc}"
                        )

                        if age > 300:
                            logger.error(
                                f"WEDGED TASK exceeded 5min ({t.get_name()}) — "
                                f"exiting worker for restart"
                            )
                            for h in logging.getLogger().handlers:
                                try:
                                    getattr(h, "flush")()
                                except Exception:
                                    pass
                            os._exit(1)
                else:
                    cancelling_since.pop(tid, None)

            for tid in list(cancelling_since.keys()):
                if tid not in live_ids:
                    cancelling_since.pop(tid)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"cancellation_watchdog error: {e}")
            await asyncio.sleep(30)
