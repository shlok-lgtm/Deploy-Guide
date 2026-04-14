"""
Prover Source Registry — DB-Backed
=====================================
Manages the provenance source registry that the external prover reads from.

Previously the prover read sources from a local YAML/config file and wrote
health state to /tmp/basis_source_health.json. This module replaces both:

- **Read sources** from the `provenance_sources` table at cycle start
- **Write health state** (success/failure/disable/heal) back to the same table
- **Seed** missing sources from the local PROVENANCE_SOURCES dict on first run
- **Daily re-check** disabled sources and re-enable if they've recovered
- **Fall back** to local config if the DB is unreachable

The external prover service calls these functions via the hub API or directly
if co-located. The hub worker also calls `run_provenance_health_recheck()`
in the slow cycle.
"""

import json
import logging
from datetime import datetime, timezone

from app.database import fetch_one, fetch_all, execute

logger = logging.getLogger(__name__)

# Auto-disable threshold: disable a source after this many consecutive failures
AUTO_DISABLE_THRESHOLD = 3

# Re-check interval: probe disabled sources after this many hours
RECHECK_INTERVAL_HOURS = 24


# =============================================================================
# Read sources from DB
# =============================================================================

def get_active_sources(schedule_filter: str = None) -> list[dict]:
    """
    Read enabled provenance sources from the DB.

    Schedule filtering:
    - 'hourly': include in every cycle
    - 'weekly': include only if last_success is >7 days ago or NULL

    Returns list of source dicts ready for the prover cycle.
    """
    try:
        if schedule_filter == "hourly":
            rows = fetch_all(
                """SELECT id, entity, component, source_type, url, schedule,
                          consecutive_failures, last_success, last_failure,
                          last_error, disabled_reason
                   FROM provenance_sources
                   WHERE enabled = true AND schedule = 'hourly'
                   ORDER BY entity, component"""
            )
        elif schedule_filter == "weekly":
            rows = fetch_all(
                """SELECT id, entity, component, source_type, url, schedule,
                          consecutive_failures, last_success, last_failure,
                          last_error, disabled_reason
                   FROM provenance_sources
                   WHERE enabled = true AND schedule = 'weekly'
                     AND (last_success IS NULL
                          OR last_success < NOW() - INTERVAL '7 days')
                   ORDER BY entity, component"""
            )
        else:
            # All enabled sources, with weekly schedule filtering
            rows = fetch_all(
                """SELECT id, entity, component, source_type, url, schedule,
                          consecutive_failures, last_success, last_failure,
                          last_error, disabled_reason
                   FROM provenance_sources
                   WHERE enabled = true
                     AND (schedule = 'hourly'
                          OR last_success IS NULL
                          OR last_success < NOW() - INTERVAL '7 days')
                   ORDER BY entity, component"""
            )
        return [dict(r) for r in (rows or [])]
    except Exception as e:
        logger.warning(f"Failed to read provenance sources from DB: {e}")
        return []


def get_all_sources() -> list[dict]:
    """Read all provenance sources (enabled and disabled) from the DB."""
    try:
        rows = fetch_all(
            """SELECT id, entity, component, source_type, url, schedule,
                      enabled, consecutive_failures, last_success, last_failure,
                      last_error, disabled_reason
               FROM provenance_sources
               ORDER BY entity, component"""
        )
        return [dict(r) for r in (rows or [])]
    except Exception as e:
        logger.warning(f"Failed to read all provenance sources: {e}")
        return []


def get_cycle_sources() -> list[dict]:
    """
    Get sources for the current cycle, with DB fallback to local config.

    This is the main entry point the prover calls at cycle start.
    Returns active sources from DB, falling back to PROVENANCE_SOURCES
    if the DB is unreachable.
    """
    sources = get_active_sources()
    if sources:
        logger.info(f"Loaded {len(sources)} active provenance sources from DB")
        return sources

    # Fallback to local config
    logger.warning("Failed to read provenance_sources from DB. "
                    "Falling back to local config.")
    return _get_local_sources()


# =============================================================================
# Health state write-back
# =============================================================================

def record_success(source_id: str):
    """Record a successful notarization for a source."""
    try:
        execute(
            """UPDATE provenance_sources SET
                   consecutive_failures = 0,
                   last_success = NOW(),
                   last_error = NULL,
                   disabled_reason = NULL,
                   updated_at = NOW()
               WHERE id = %s""",
            (source_id,),
        )
    except Exception as e:
        logger.warning(f"Failed to record success for {source_id}: {e}")


def record_failure(source_id: str, error: str) -> bool:
    """
    Record a failed notarization attempt.

    Returns True if the source was auto-disabled (hit threshold).
    """
    try:
        # Increment failures and get the new count
        row = fetch_one(
            """UPDATE provenance_sources SET
                   consecutive_failures = consecutive_failures + 1,
                   last_failure = NOW(),
                   last_error = %s,
                   updated_at = NOW()
               WHERE id = %s
               RETURNING consecutive_failures""",
            (error, source_id),
        )
        if row and row["consecutive_failures"] >= AUTO_DISABLE_THRESHOLD:
            _auto_disable(source_id, error)
            return True
        return False
    except Exception as e:
        logger.warning(f"Failed to record failure for {source_id}: {e}")
        return False


def _auto_disable(source_id: str, reason: str):
    """Disable a source after too many consecutive failures."""
    try:
        execute(
            """UPDATE provenance_sources SET
                   enabled = false,
                   disabled_reason = %s,
                   updated_at = NOW()
               WHERE id = %s""",
            (f"Auto-disabled after {AUTO_DISABLE_THRESHOLD} consecutive failures: {reason}",
             source_id),
        )
        logger.warning(f"Auto-disabled provenance source {source_id}: {reason}")

        # Report alert to provenance_health_alerts
        _report_alert(source_id, "auto_disabled", details={
            "reason": reason,
            "threshold": AUTO_DISABLE_THRESHOLD,
        })
    except Exception as e:
        logger.warning(f"Failed to auto-disable {source_id}: {e}")


def record_auto_heal(source_id: str, old_url: str, new_url: str):
    """
    Record an auto-heal: source URL changed (same-domain redirect).
    Updates the URL and re-enables the source.
    """
    try:
        execute(
            """UPDATE provenance_sources SET
                   url = %s,
                   enabled = true,
                   consecutive_failures = 0,
                   last_error = NULL,
                   disabled_reason = NULL,
                   last_success = NOW(),
                   updated_at = NOW()
               WHERE id = %s""",
            (new_url, source_id),
        )
        logger.info(f"Auto-healed provenance source {source_id}: {old_url} -> {new_url}")

        _report_alert(source_id, "auto_healed", old_url=old_url, redirect_url=new_url)
    except Exception as e:
        logger.warning(f"Failed to record auto-heal for {source_id}: {e}")


def record_re_enable(source_id: str):
    """Re-enable a previously disabled source after successful re-check."""
    try:
        execute(
            """UPDATE provenance_sources SET
                   enabled = true,
                   consecutive_failures = 0,
                   disabled_reason = NULL,
                   last_success = NOW(),
                   updated_at = NOW()
               WHERE id = %s""",
            (source_id,),
        )
        logger.info(f"Re-enabled provenance source {source_id}")

        _report_alert(source_id, "re_enabled")
    except Exception as e:
        logger.warning(f"Failed to re-enable {source_id}: {e}")


def _report_alert(source_id: str, event: str, old_url: str = None,
                  redirect_url: str = None, details: dict = None):
    """Write a health alert to provenance_health_alerts."""
    try:
        execute(
            """INSERT INTO provenance_health_alerts
               (source_id, event, old_url, redirect_url, details)
               VALUES (%s, %s, %s, %s, %s)""",
            (source_id, event, old_url, redirect_url,
             json.dumps(details) if details else None),
        )
    except Exception as e:
        logger.debug(f"Failed to write health alert for {source_id}: {e}")


# =============================================================================
# Daily re-check of disabled sources
# =============================================================================

def get_sources_for_recheck() -> list[dict]:
    """
    Get disabled sources that haven't been checked in RECHECK_INTERVAL_HOURS.
    These should be probed (HEAD request) to see if they've recovered.
    """
    try:
        rows = fetch_all(
            """SELECT id, entity, component, source_type, url, last_failure
               FROM provenance_sources
               WHERE enabled = false
                 AND (last_failure IS NULL
                      OR last_failure < NOW() - INTERVAL '%s hours')
               ORDER BY last_failure ASC NULLS FIRST""",
            (RECHECK_INTERVAL_HOURS,),
        )
        return [dict(r) for r in (rows or [])]
    except Exception as e:
        logger.warning(f"Failed to get sources for recheck: {e}")
        return []


def bump_recheck_timestamp(source_id: str):
    """
    Update last_failure to now() so the source isn't re-checked for another
    RECHECK_INTERVAL_HOURS. Called when re-check fails.
    """
    try:
        execute(
            "UPDATE provenance_sources SET last_failure = NOW() WHERE id = %s",
            (source_id,),
        )
    except Exception as e:
        logger.debug(f"Failed to bump recheck timestamp for {source_id}: {e}")


async def run_provenance_health_recheck():
    """
    Daily re-check of disabled provenance sources.

    For each disabled source past the re-check interval:
    1. HEAD probe the URL
    2. If 200: re-enable
    3. If same-domain redirect: auto-heal + re-enable
    4. If still broken: bump last_failure timestamp

    Called from the worker slow cycle.
    """
    import httpx

    sources = get_sources_for_recheck()
    if not sources:
        logger.debug("No disabled provenance sources due for re-check")
        return {"checked": 0, "re_enabled": 0, "healed": 0, "still_down": 0}

    logger.info(f"Re-checking {len(sources)} disabled provenance sources")

    re_enabled = 0
    healed = 0
    still_down = 0

    async with httpx.AsyncClient(timeout=15, follow_redirects=False) as client:
        for src in sources:
            source_id = src["id"]
            url = src.get("url", "")

            # Skip dynamic URLs (e.g. CDA issuer PDFs)
            if url.startswith("dynamic:") or not url.startswith("http"):
                bump_recheck_timestamp(source_id)
                still_down += 1
                continue

            try:
                resp = await client.head(url)

                if resp.status_code == 200:
                    record_re_enable(source_id)
                    re_enabled += 1
                elif resp.status_code in (301, 302, 307, 308):
                    redirect_url = str(resp.headers.get("location", ""))
                    if redirect_url and _is_same_domain(url, redirect_url):
                        record_auto_heal(source_id, url, redirect_url)
                        healed += 1
                    else:
                        # Cross-domain redirect — report but don't auto-heal
                        _report_alert(source_id, "domain_change_detected",
                                      old_url=url, redirect_url=redirect_url)
                        bump_recheck_timestamp(source_id)
                        still_down += 1
                else:
                    bump_recheck_timestamp(source_id)
                    still_down += 1

            except Exception as e:
                logger.debug(f"Re-check probe failed for {source_id}: {e}")
                bump_recheck_timestamp(source_id)
                still_down += 1

    result = {
        "checked": len(sources),
        "re_enabled": re_enabled,
        "healed": healed,
        "still_down": still_down,
    }
    logger.info(f"Provenance re-check: {result}")
    return result


def _is_same_domain(url1: str, url2: str) -> bool:
    """Check if two URLs share the same domain."""
    try:
        from urllib.parse import urlparse
        return urlparse(url1).netloc == urlparse(url2).netloc
    except Exception:
        return False


# =============================================================================
# Seed from local config
# =============================================================================

def seed_from_local_config() -> int:
    """
    Seed provenance_sources from the local PROVENANCE_SOURCES dict.
    Uses ON CONFLICT DO NOTHING so existing DB rows are preserved.

    Called on startup before the first cycle. Returns count of newly seeded sources.
    """
    local_sources = _get_local_sources()
    if not local_sources:
        return 0

    seeded = 0
    for src in local_sources:
        try:
            row = fetch_one(
                """INSERT INTO provenance_sources
                   (id, entity, component, source_type, url, schedule)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO NOTHING
                   RETURNING id""",
                (src["id"], src["entity"], src["component"],
                 src["source_type"], src["url"], src["schedule"]),
            )
            if row:
                seeded += 1
        except Exception as e:
            logger.debug(f"Failed to seed source {src.get('id')}: {e}")

    if seeded:
        logger.info(f"Seeded {seeded} sources from local config into DB")
    return seeded


def _get_local_sources() -> list[dict]:
    """
    Build source list from the local PROVENANCE_SOURCES dict.
    This is the fallback when the DB is unreachable.
    """
    try:
        from app.data_layer.provenance_scaling import PROVENANCE_SOURCES
        from app.collectors.registry import _build_url, _infer_source_type
    except ImportError:
        logger.debug("Local provenance config not available")
        return []

    sources = []
    for source_id, src in PROVENANCE_SOURCES.items():
        provider = src.get("provider", "unknown")
        endpoint = src.get("endpoint", "")
        url = _build_url(provider, endpoint)
        component = source_id.replace(f"{provider}_", "", 1) \
            if source_id.startswith(f"{provider}_") else source_id

        sources.append({
            "id": source_id,
            "entity": provider,
            "component": component,
            "source_type": _infer_source_type(url),
            "url": url,
            "schedule": "hourly",
            "consecutive_failures": 0,
            "last_success": None,
            "last_failure": None,
            "last_error": None,
            "disabled_reason": None,
        })

    return sources


# =============================================================================
# Cycle summary
# =============================================================================

def get_source_counts() -> dict:
    """
    Get aggregate source counts for cycle summary logging.
    Returns {active, disabled, total}.
    """
    try:
        row = fetch_one(
            """SELECT
                   COUNT(*) FILTER (WHERE enabled = true) AS active,
                   COUNT(*) FILTER (WHERE enabled = false) AS disabled,
                   COUNT(*) AS total
               FROM provenance_sources"""
        )
        if row:
            return {
                "active": row["active"],
                "disabled": row["disabled"],
                "total": row["total"],
            }
    except Exception as e:
        logger.debug(f"Failed to get source counts: {e}")
    return {"active": 0, "disabled": 0, "total": 0}
