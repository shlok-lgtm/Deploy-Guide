"""
API Call Tracker — in-memory counters, flush to DB per cycle.
No per-call DB writes. Singleton module.
"""

import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from threading import Lock

logger = logging.getLogger(__name__)


class APITracker:
    def __init__(self):
        self._lock = Lock()
        self._counters = defaultdict(lambda: {
            "total": 0, "success": 0, "error": 0,
            "latencies": [], "callers": defaultdict(int),
        })

    def record(self, provider: str, endpoint: str, status: int,
               latency_ms: int, caller: str = "unknown"):
        hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        key = (provider, hour)
        with self._lock:
            c = self._counters[key]
            c["total"] += 1
            if 200 <= status < 400:
                c["success"] += 1
            else:
                c["error"] += 1
            if latency_ms:
                c["latencies"].append(latency_ms)
            c["callers"][caller] += 1

    def get_budget_summary(self) -> dict:
        """Return per-provider call totals for the current day (in-memory)."""
        today = datetime.now(timezone.utc).date()
        totals = defaultdict(int)
        with self._lock:
            for (provider, hour), stats in self._counters.items():
                if hour.date() == today:
                    totals[provider] += stats["total"]
        return dict(totals)

    def flush(self) -> int:
        """Flush to api_usage_hourly. Returns rows written."""
        with self._lock:
            if not self._counters:
                return 0
            snapshot = dict(self._counters)
            self._counters.clear()

        rows = []
        for (provider, hour), stats in snapshot.items():
            latencies = stats["latencies"]
            avg_ms = int(sum(latencies) / len(latencies)) if latencies else 0
            p95_ms = 0
            if latencies:
                s = sorted(latencies)
                p95_ms = s[min(int(len(s) * 0.95), len(s) - 1)]
            rows.append((
                provider, hour, stats["total"], stats["success"], stats["error"],
                avg_ms, p95_ms, json.dumps(dict(stats["callers"])),
            ))

        if not rows:
            return 0

        try:
            from psycopg2.extras import execute_values
            from app.database import get_cursor
            with get_cursor() as cur:
                execute_values(cur, """
                    INSERT INTO api_usage_hourly
                        (provider, hour, total_calls, success_calls, error_calls,
                         avg_latency_ms, p95_latency_ms, callers)
                    VALUES %s
                    ON CONFLICT (provider, hour) DO UPDATE SET
                        total_calls = api_usage_hourly.total_calls + EXCLUDED.total_calls,
                        success_calls = api_usage_hourly.success_calls + EXCLUDED.success_calls,
                        error_calls = api_usage_hourly.error_calls + EXCLUDED.error_calls
                """, rows)
            return len(rows)
        except Exception as e:
            logger.error(f"[api_tracker] flush failed: {e}")
            return 0


# Module-level singleton
tracker = APITracker()
