"""
System Health Dashboard (rebuilt)
==================================
The state-of-system dashboard, rebuilt to report what the monitors
actually say.

The top-level health verdict is derived strictly from the project's
source-of-truth monitor tables:

  - ops_health_checks  — latest row per system (DISTINCT ON)
  - ops_alert_log      — unacknowledged alerts

It is NOT derived from collector cycle counts. The legacy state-growth
view concluded "all green" from collector success counts while
ops_health_checks showed systems down — this view does not second-guess
the monitors.

It also fixes the row-count mislabel the legacy view shipped
(COUNT(DISTINCT key) presented as a table's row count) and replaces the
hardcoded 66-table catalog with live discovery over pg_stat_user_tables.

GET /api/ops/system-status
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from app.database import fetch_one_async, fetch_all_async
from app.data_layer.state_growth import TRACKED_TABLES

logger = logging.getLogger(__name__)

# status -> UI color. Unknown statuses render gray and are surfaced as-is
# (the dashboard reports what the monitor says; it does not filter).
_STATUS_COLOR = {"healthy": "green", "degraded": "yellow", "down": "red"}

# Concurrency cap for per-table +24h delta queries.
_DELTA_CONCURRENCY = 8


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _aware(ts):
    """Coerce a possibly-naive timestamp to aware UTC."""
    if ts is None:
        return None
    if hasattr(ts, "tzinfo") and ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def _as_dict(jsonb_val):
    """JSONB columns come back as dict (psycopg2) or str depending on driver
    config — normalize to dict."""
    if isinstance(jsonb_val, dict):
        return jsonb_val
    if isinstance(jsonb_val, str):
        try:
            parsed = json.loads(jsonb_val)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _summarize_details(details) -> str:
    """One-line summary of an ops_health_checks.details blob."""
    d = _as_dict(details)
    if not d:
        return ""
    parts = []
    for k in ("error", "reason", "message", "age_hours", "last_built"):
        if k in d:
            parts.append(f"{k}={d[k]}")
    if not parts:
        for k, v in list(d.items())[:3]:
            if not isinstance(v, (dict, list)):
                parts.append(f"{k}={v}")
    return ", ".join(str(p) for p in parts)[:240]


def _category_for(schema: str, relname: str) -> str:
    """Heuristic category for a newly-discovered (untracked) table."""
    if schema == "wallet_graph":
        return "wallet"
    if schema.startswith("public_discovery"):
        return "discovery"
    if schema == "ops":
        return "ops"
    for prefix, cat in (
        ("rpi_", "rpi"), ("psi_", "psi"), ("cda_", "cda"), ("ops_", "ops"),
        ("governance_", "governance"), ("gov_", "governance"),
        ("oracle_", "oracle"), ("wallet_", "wallet"), ("keeper_", "keeper"),
        ("api_", "infra"), ("rpc_", "infra"), ("coherence_", "infra"),
    ):
        if relname.startswith(prefix):
            return cat
    return "other"


# ---------------------------------------------------------------------------
# Verdict — pure function, unit-tested without a DB
# ---------------------------------------------------------------------------

def compute_verdict(systems: list, unacked_24h: int) -> dict:
    """Top-of-page health verdict.

    red    if any system in ops_health_checks is `down`
    yellow if any system is `degraded`, OR every system is healthy but
           there are unacknowledged alerts in the last 24h
    green  only if every monitored system is `healthy` AND there are zero
           unacknowledged alerts in the last 24h
    """
    down = sorted(s["system"] for s in systems if s["status"] == "down")
    degraded = sorted(s["system"] for s in systems if s["status"] == "degraded")
    healthy = [s for s in systems if s["status"] == "healthy"]
    total = len(systems)

    if down:
        level = "red"
        reason = f"{len(down)} system(s) down: {', '.join(down)}"
    elif degraded:
        level = "yellow"
        reason = f"{len(degraded)} system(s) degraded: {', '.join(degraded)}"
    elif unacked_24h > 0:
        level = "yellow"
        reason = (f"all monitored systems healthy, but {unacked_24h} "
                  f"unacknowledged alert(s) in the last 24h")
    else:
        level = "green"
        reason = "all monitored systems healthy, no unacknowledged alerts"

    return {
        "level": level,
        "headline": (f"{len(healthy)}/{total} systems healthy"
                     if total else "no monitored systems found"),
        "reason": reason,
        "systems_total": total,
        "systems_healthy": len(healthy),
        "systems_degraded": len(degraded),
        "systems_down": len(down),
        "unacknowledged_alerts_24h": unacked_24h,
    }


# ---------------------------------------------------------------------------
# Source-of-truth queries
# ---------------------------------------------------------------------------

async def _system_health() -> list:
    """Latest row per system from ops_health_checks."""
    try:
        rows = await fetch_all_async(
            """
            SELECT DISTINCT ON (system) system, status, details, checked_at
            FROM ops_health_checks
            ORDER BY system, checked_at DESC
            """
        ) or []
    except Exception as e:
        logger.warning(f"[system_health] ops_health_checks query failed: {e}")
        return []

    now = datetime.now(timezone.utc)
    systems = []
    for r in rows:
        status = (r.get("status") or "unknown").lower()
        checked = _aware(r.get("checked_at"))
        age_h = round((now - checked).total_seconds() / 3600, 1) if checked else None
        systems.append({
            "system": r["system"],
            "status": status,
            "color": _STATUS_COLOR.get(status, "gray"),
            "age_hours": age_h,
            "checked_at": checked.isoformat() if checked else None,
            "detail": _summarize_details(r.get("details")),
        })
    systems.sort(key=lambda s: s["system"])
    return systems


async def _active_alerts() -> dict:
    """Unacknowledged alerts from ops_alert_log, grouped by type."""
    by_type = []
    total = 0
    try:
        rows = await fetch_all_async(
            """
            SELECT alert_type,
                   COUNT(*) AS cnt,
                   MAX(sent_at) AS latest_at,
                   (ARRAY_AGG(message ORDER BY sent_at DESC))[1] AS latest_message
            FROM ops_alert_log
            WHERE acknowledged = FALSE
            GROUP BY alert_type
            ORDER BY cnt DESC
            """
        ) or []
        for r in rows:
            cnt = int(r["cnt"])
            total += cnt
            latest = _aware(r.get("latest_at"))
            by_type.append({
                "alert_type": r["alert_type"],
                "count": cnt,
                "latest_at": latest.isoformat() if latest else None,
                "latest_message": (r.get("latest_message") or "")[:300],
            })
    except Exception as e:
        logger.warning(f"[system_health] ops_alert_log query failed: {e}")

    unacked_24h = 0
    try:
        row = await fetch_one_async(
            "SELECT COUNT(*) AS cnt FROM ops_alert_log "
            "WHERE acknowledged = FALSE AND sent_at > NOW() - INTERVAL '24 hours'"
        )
        unacked_24h = int(row["cnt"]) if row and row.get("cnt") is not None else 0
    except Exception as e:
        logger.warning(f"[system_health] unacked-24h count failed: {e}")

    return {
        "by_type": by_type,
        "total_unacknowledged": total,
        "unacknowledged_24h": unacked_24h,
    }


async def _headline_metrics() -> dict:
    """True row counts plus distinct-key counts for the two tables the legacy
    dashboard mislabeled. `rows` is COUNT(*); the distinct counts are a
    separate, explicitly-labeled secondary metric."""
    async def _scalar(sql: str) -> int:
        try:
            row = await fetch_one_async(sql)
            return int(row["cnt"]) if row and row.get("cnt") is not None else 0
        except Exception as e:
            logger.warning(f"[system_health] headline query failed: {e}")
            return 0

    (cr_rows, cr_components, cr_24h,
     wrs_rows, wrs_wallets, wrs_24h) = await asyncio.gather(
        _scalar("SELECT COUNT(*) AS cnt FROM component_readings"),
        _scalar("SELECT COUNT(*) AS cnt FROM "
                "(SELECT DISTINCT stablecoin_id, component_id FROM component_readings) s"),
        _scalar("SELECT COUNT(*) AS cnt FROM component_readings "
                "WHERE collected_at > NOW() - INTERVAL '24 hours'"),
        _scalar("SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_risk_scores"),
        _scalar("SELECT COUNT(*) AS cnt FROM "
                "(SELECT DISTINCT wallet_address, chain FROM wallet_graph.wallet_risk_scores) s"),
        _scalar("SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_risk_scores "
                "WHERE computed_at > NOW() - INTERVAL '24 hours'"),
    )
    return {
        "component_readings": {
            "rows": cr_rows,
            "rows_label": "rows — COUNT(*)",
            "distinct_components": cr_components,
            "distinct_label": "components tracked — distinct (stablecoin_id, component_id)",
            "rows_24h": cr_24h,
            "rows_24h_label": "rows added in 24h (collected_at)",
        },
        "wallet_risk_scores": {
            "rows": wrs_rows,
            "rows_label": "rows — COUNT(*)",
            "distinct_wallets": wrs_wallets,
            "distinct_label": "distinct (wallet_address, chain)",
            "rows_24h": wrs_24h,
            "rows_24h_label": "rows added in 24h (computed_at)",
        },
    }


async def _table_catalog() -> dict:
    """Live table catalog over pg_stat_user_tables (all schemas except
    internal harness bookkeeping). Tables present in the DB but absent from
    TRACKED_TABLES are flagged as schema drift."""
    try:
        rows = await fetch_all_async(
            """
            SELECT schemaname AS schema,
                   relname,
                   schemaname || '.' || relname AS full_name,
                   n_live_tup AS est_rows
            FROM pg_stat_user_tables
            WHERE schemaname <> '_system'
            ORDER BY n_live_tup DESC
            """
        ) or []
    except Exception as e:
        logger.warning(f"[system_health] pg_stat_user_tables query failed: {e}")
        return {"tables": [], "drift": []}

    # TRACKED_TABLES keys are sometimes schema-qualified, sometimes bare —
    # index both ways so lookup works regardless.
    tracked = {}
    for key, cfg in TRACKED_TABLES.items():
        tracked[key] = cfg
        if "." in key:
            tracked.setdefault(key.split(".")[-1], cfg)

    sem = asyncio.Semaphore(_DELTA_CONCURRENCY)

    async def _delta_24h(full_name: str, time_col: str):
        async with sem:
            try:
                row = await fetch_one_async(
                    f"SELECT COUNT(*) AS cnt FROM {full_name} "
                    f"WHERE {time_col} > NOW() - INTERVAL '24 hours'"
                )
                return int(row["cnt"]) if row and row.get("cnt") is not None else None
            except Exception:
                return None  # table has no such column / not queryable

    catalog = []
    drift = []
    delta_tasks = {}
    for r in rows:
        est = int(r.get("est_rows") or 0)
        if est <= 0:
            continue  # skip empty tables
        full = r["full_name"]
        bare = r["relname"]
        cfg = tracked.get(full) or tracked.get(bare)
        is_tracked = cfg is not None
        time_col = cfg.get("time_col") if cfg else None

        entry = {
            "table": full,
            "schema": r["schema"],
            "est_rows": est,
            "rows_are_estimate": True,
            "tracked": is_tracked,
            "category": cfg.get("category") if cfg else _category_for(r["schema"], bare),
            "time_col": time_col,
            "rows_24h": None,  # filled for tracked tables with a timestamp col
        }
        catalog.append(entry)
        if is_tracked and time_col:
            delta_tasks[full] = asyncio.create_task(_delta_24h(full, time_col))
        if not is_tracked:
            drift.append({"table": full, "schema": r["schema"], "est_rows": est})

    for entry in catalog:
        task = delta_tasks.get(entry["table"])
        if task is not None:
            entry["rows_24h"] = await task

    return {"tables": catalog, "drift": drift}


# ---------------------------------------------------------------------------
# Top-level assembly
# ---------------------------------------------------------------------------

async def get_system_status() -> dict:
    """Assemble the rebuilt state-of-system dashboard."""
    now = datetime.now(timezone.utc)

    systems, alerts, headline, catalog = await asyncio.gather(
        _system_health(),
        _active_alerts(),
        _headline_metrics(),
        _table_catalog(),
    )

    # Overlay exact COUNT(*) onto the two headline tables in the catalog —
    # the catalog itself uses cheap pg_stat estimates everywhere else.
    _exact = {
        "component_readings": headline["component_readings"]["rows"],
        "wallet_graph.wallet_risk_scores": headline["wallet_risk_scores"]["rows"],
    }
    for entry in catalog["tables"]:
        if entry["table"] in _exact:
            entry["exact_rows"] = _exact[entry["table"]]
            entry["rows_are_estimate"] = False

    return {
        "generated_at": now.isoformat(),
        "verdict": compute_verdict(systems, alerts["unacknowledged_24h"]),
        "systems": systems,
        "active_alerts": alerts,
        "headline_metrics": headline,
        "table_catalog": catalog["tables"],
        "schema_drift": {
            "count": len(catalog["drift"]),
            "tables": catalog["drift"],
            "note": (
                "Tables present in the database but absent from the dashboard's "
                "known-tables list (TRACKED_TABLES in app/data_layer/state_growth.py). "
                "This is the catalog gap that let wallet_graph.wallet_edges_archive "
                "go unnoticed."
            ),
        },
        "catalog_summary": {
            "tables_with_rows": len(catalog["tables"]),
            "tracked": sum(1 for t in catalog["tables"] if t["tracked"]),
            "discovered": len(catalog["drift"]),
            "row_counts_note": (
                "est_rows is the pg_stat n_live_tup estimate; exact_rows "
                "(COUNT(*)) is provided only for headline tables."
            ),
        },
    }
