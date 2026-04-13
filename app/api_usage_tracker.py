"""
API Usage Tracker
=================
Centralized tracking of all external API calls across the platform.
Buffered writes — calls are batched and flushed periodically to avoid
per-request DB overhead.

Usage:
    from app.api_usage_tracker import track_api_call, get_usage_summary

    # In any collector/service:
    track_api_call("coingecko", "/coins/usdc", caller="coingecko_collector",
                   status=200, latency_ms=342)

    # Dashboard:
    summary = get_usage_summary()
"""

import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Buffer for batched writes
_buffer: list[dict] = []
_buffer_lock = threading.Lock()
_FLUSH_THRESHOLD = 50
_FLUSH_INTERVAL = 30  # seconds
_last_flush = time.time()

# In-memory counters for real-time dashboard (no DB hit)
_counters: dict[str, dict] = defaultdict(lambda: {
    "calls_today": 0,
    "calls_this_hour": 0,
    "errors_today": 0,
    "total_latency_ms": 0,
    "last_call_at": None,
    "hour_reset": None,
    "day_reset": None,
    "callers": defaultdict(int),
})
_counters_lock = threading.Lock()


def track_api_call(
    provider: str,
    endpoint: str,
    caller: str = "unknown",
    status: Optional[int] = None,
    latency_ms: Optional[int] = None,
    count: int = 1,
):
    """
    Track an external API call. Buffered for batch DB insert.

    Args:
        provider: API provider name (coingecko, etherscan, defillama, etc.)
        endpoint: API endpoint path
        caller: Module/collector that made the call
        status: HTTP response status code
        latency_ms: Response time in milliseconds
        count: Number of calls (for batch tracking)
    """
    now = time.time()
    now_dt = datetime.now(timezone.utc)
    current_hour = now_dt.replace(minute=0, second=0, microsecond=0)
    current_day = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    entry = {
        "provider": provider,
        "endpoint": endpoint,
        "calls_count": count,
        "caller": caller,
        "response_status": status,
        "latency_ms": latency_ms,
        "recorded_at": now_dt.isoformat(),
    }

    # Update in-memory counters
    with _counters_lock:
        c = _counters[provider]

        # Reset hourly counter if hour changed
        if c["hour_reset"] != current_hour:
            c["calls_this_hour"] = 0
            c["hour_reset"] = current_hour

        # Reset daily counter if day changed
        if c["day_reset"] != current_day:
            c["calls_today"] = 0
            c["errors_today"] = 0
            c["total_latency_ms"] = 0
            c["callers"] = defaultdict(int)
            c["day_reset"] = current_day

        c["calls_today"] += count
        c["calls_this_hour"] += count
        c["last_call_at"] = now_dt.isoformat()
        c["callers"][caller] += count
        if latency_ms:
            c["total_latency_ms"] += latency_ms * count
        if status and status >= 400:
            c["errors_today"] += count

    # Check daily hard cap alerts
    DAILY_CAPS = {
        "etherscan": 200_000,    # Standard plan hard cap
        "blockscout": 100_000,   # Free tier per-chain (conservative — per instance is separate)
    }
    cap = DAILY_CAPS.get(provider)
    if cap:
        with _counters_lock:
            today_count = _counters[provider]["calls_today"]
        if today_count >= int(cap * 0.95):
            logger.error(
                f"{provider.upper()} 95% CAP ALERT: {today_count:,}/{cap:,} calls today. "
                f"Stop non-critical {provider} calls to avoid hitting hard cap."
            )
        elif today_count >= int(cap * 0.80):
            logger.warning(
                f"{provider} 80% cap warning: {today_count:,}/{cap:,} calls today."
            )

    # Buffer for DB write
    with _buffer_lock:
        _buffer.append(entry)
        should_flush = (
            len(_buffer) >= _FLUSH_THRESHOLD
            or (now - _last_flush) >= _FLUSH_INTERVAL
        )
        if should_flush:
            _flush_buffer()


def _flush_buffer():
    """Bulk insert buffered entries to DB and update hourly rollup."""
    global _last_flush

    with _buffer_lock:
        if not _buffer:
            return
        batch = list(_buffer)
        _buffer.clear()
        _last_flush = time.time()

    try:
        from app.database import get_cursor
        with get_cursor() as cur:
            for entry in batch:
                cur.execute(
                    """INSERT INTO api_usage_tracker
                       (provider, endpoint, calls_count, caller, response_status, latency_ms, recorded_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (
                        entry["provider"],
                        entry["endpoint"],
                        entry["calls_count"],
                        entry["caller"],
                        entry["response_status"],
                        entry["latency_ms"],
                        entry["recorded_at"],
                    ),
                )
    except Exception as e:
        logger.debug(f"API usage flush failed (table may not exist): {e}")

    # Update hourly rollup
    try:
        _update_hourly_rollup(batch)
    except Exception as e:
        logger.debug(f"Hourly rollup update failed: {e}")


def _update_hourly_rollup(batch: list[dict]):
    """Aggregate batch entries into hourly rollup table."""
    from app.database import get_cursor
    from datetime import datetime as dt

    # Group by provider + hour
    hourly: dict[tuple, dict] = {}
    for entry in batch:
        recorded = entry["recorded_at"]
        if isinstance(recorded, str):
            recorded = dt.fromisoformat(recorded)
        hour = recorded.replace(minute=0, second=0, microsecond=0)
        key = (entry["provider"], hour)

        if key not in hourly:
            hourly[key] = {
                "total": 0,
                "success": 0,
                "error": 0,
                "latencies": [],
                "callers": defaultdict(int),
            }

        h = hourly[key]
        h["total"] += entry["calls_count"]
        status = entry.get("response_status")
        if status and status < 400:
            h["success"] += entry["calls_count"]
        elif status and status >= 400:
            h["error"] += entry["calls_count"]
        else:
            h["success"] += entry["calls_count"]  # assume success if no status

        if entry.get("latency_ms"):
            h["latencies"].append(entry["latency_ms"])

        h["callers"][entry.get("caller", "unknown")] += entry["calls_count"]

    import json
    with get_cursor() as cur:
        for (provider, hour), data in hourly.items():
            avg_lat = None
            p95_lat = None
            if data["latencies"]:
                avg_lat = int(sum(data["latencies"]) / len(data["latencies"]))
                sorted_lats = sorted(data["latencies"])
                p95_idx = int(len(sorted_lats) * 0.95)
                p95_lat = sorted_lats[min(p95_idx, len(sorted_lats) - 1)]

            callers_json = json.dumps(dict(data["callers"]))

            cur.execute(
                """INSERT INTO api_usage_hourly
                   (provider, hour, total_calls, success_calls, error_calls,
                    avg_latency_ms, p95_latency_ms, callers)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                   ON CONFLICT (provider, hour) DO UPDATE SET
                       total_calls = api_usage_hourly.total_calls + EXCLUDED.total_calls,
                       success_calls = api_usage_hourly.success_calls + EXCLUDED.success_calls,
                       error_calls = api_usage_hourly.error_calls + EXCLUDED.error_calls,
                       avg_latency_ms = COALESCE(EXCLUDED.avg_latency_ms, api_usage_hourly.avg_latency_ms),
                       p95_latency_ms = COALESCE(EXCLUDED.p95_latency_ms, api_usage_hourly.p95_latency_ms),
                       callers = api_usage_hourly.callers || EXCLUDED.callers""",
                (provider, hour, data["total"], data["success"], data["error"],
                 avg_lat, p95_lat, callers_json),
            )


def flush():
    """Force flush the buffer. Call at shutdown or end of cycle."""
    _flush_buffer()


def get_realtime_counters() -> dict:
    """Return in-memory counters for all providers. No DB hit."""
    with _counters_lock:
        result = {}
        for provider, c in _counters.items():
            result[provider] = {
                "calls_today": c["calls_today"],
                "calls_this_hour": c["calls_this_hour"],
                "errors_today": c["errors_today"],
                "avg_latency_ms": (
                    round(c["total_latency_ms"] / c["calls_today"])
                    if c["calls_today"] > 0 else None
                ),
                "last_call_at": c["last_call_at"],
                "top_callers": dict(
                    sorted(c["callers"].items(), key=lambda x: -x[1])[:10]
                ),
            }
        return result


def get_usage_summary(days: int = 1) -> dict:
    """
    Full usage summary combining DB history + provider limits.
    Returns per-provider: calls today, remaining budget, utilization %.
    """
    try:
        from app.database import fetch_all, fetch_one
        import json

        # Get provider limits
        limits_rows = fetch_all("SELECT * FROM api_provider_limits")
        limits = {}
        if limits_rows:
            for row in limits_rows:
                limits[row["provider"]] = dict(row)

        # Get today's usage from hourly rollup
        usage_rows = fetch_all(
            """SELECT provider,
                      SUM(total_calls) as total,
                      SUM(success_calls) as success,
                      SUM(error_calls) as errors
               FROM api_usage_hourly
               WHERE hour >= NOW() - INTERVAL '%s days'
               GROUP BY provider
               ORDER BY total DESC""",
            (days,),
        )

        # Get this month's usage
        monthly_rows = fetch_all(
            """SELECT provider, SUM(total_calls) as total
               FROM api_usage_hourly
               WHERE hour >= DATE_TRUNC('month', NOW())
               GROUP BY provider"""
        )
        monthly = {}
        if monthly_rows:
            for row in monthly_rows:
                monthly[row["provider"]] = row["total"]

        result = {"providers": {}, "generated_at": datetime.now(timezone.utc).isoformat()}

        # Merge real-time counters
        rt = get_realtime_counters()

        all_providers = set()
        if limits_rows:
            all_providers.update(r["provider"] for r in limits_rows)
        if usage_rows:
            all_providers.update(r["provider"] for r in usage_rows)
        all_providers.update(rt.keys())

        for provider in sorted(all_providers):
            limit_info = limits.get(provider, {})
            daily_limit = limit_info.get("calls_per_day")
            monthly_limit = limit_info.get("calls_per_month")

            # DB-based usage
            db_today = 0
            db_errors = 0
            if usage_rows:
                for row in usage_rows:
                    if row["provider"] == provider:
                        db_today = row["total"] or 0
                        db_errors = row["errors"] or 0

            # Real-time override (more current)
            rt_data = rt.get(provider, {})
            calls_today = rt_data.get("calls_today", db_today)
            calls_month = monthly.get(provider, 0)

            daily_remaining = None
            daily_utilization = None
            if daily_limit:
                daily_remaining = max(0, int(daily_limit) - calls_today)
                daily_utilization = round(calls_today / int(daily_limit) * 100, 1)

            monthly_remaining = None
            monthly_utilization = None
            if monthly_limit:
                monthly_remaining = max(0, int(monthly_limit) - calls_month)
                monthly_utilization = round(calls_month / int(monthly_limit) * 100, 1)

            # Projected monthly at current rate
            projected_monthly = None
            if calls_today > 0:
                projected_monthly = calls_today * 30

            result["providers"][provider] = {
                "plan_tier": limit_info.get("plan_tier"),
                "calls_today": calls_today,
                "calls_this_hour": rt_data.get("calls_this_hour", 0),
                "errors_today": rt_data.get("errors_today", db_errors),
                "avg_latency_ms": rt_data.get("avg_latency_ms"),
                "last_call_at": rt_data.get("last_call_at"),
                "daily_limit": int(daily_limit) if daily_limit else None,
                "daily_remaining": daily_remaining,
                "daily_utilization_pct": daily_utilization,
                "monthly_limit": int(monthly_limit) if monthly_limit else None,
                "calls_this_month": calls_month,
                "monthly_remaining": monthly_remaining,
                "monthly_utilization_pct": monthly_utilization,
                "projected_monthly": projected_monthly,
                "top_callers": rt_data.get("top_callers", {}),
                "rate_limit": {
                    "per_second": float(limit_info["calls_per_second"]) if limit_info.get("calls_per_second") else None,
                    "per_minute": float(limit_info["calls_per_minute"]) if limit_info.get("calls_per_minute") else None,
                },
            }

        return result

    except Exception as e:
        logger.warning(f"Usage summary failed, returning realtime only: {e}")
        return {
            "providers": get_realtime_counters(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "note": "DB unavailable, showing realtime counters only",
        }


def get_usage_history(provider: str, hours: int = 24) -> list[dict]:
    """Get hourly usage history for a specific provider."""
    try:
        from app.database import fetch_all
        rows = fetch_all(
            """SELECT hour, total_calls, success_calls, error_calls,
                      avg_latency_ms, p95_latency_ms, callers
               FROM api_usage_hourly
               WHERE provider = %s AND hour >= NOW() - INTERVAL '%s hours'
               ORDER BY hour DESC""",
            (provider, hours),
        )
        return [dict(r) for r in rows] if rows else []
    except Exception as e:
        logger.warning(f"Usage history query failed: {e}")
        return []
