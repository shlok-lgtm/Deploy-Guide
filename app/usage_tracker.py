"""
Usage Tracker
=============
Buffers request logs in memory and bulk-flushes them to api_request_log
every 30 seconds or after 100 buffered entries (whichever comes first).

Public API:
  log_request(...)       — non-blocking, appends to buffer
  flush()                — bulk-inserts buffer into DB; safe when empty
  get_usage_stats(days)  — query aggregated stats from DB
  validate_api_key(key)  — returns key id (int) or None
  create_api_key(name)   — generates and stores a new key, returns key string
  list_api_keys()        — list all keys with usage counts
  hash_api_key(key)      — SHA-256 hex digest of raw key string
"""

import hashlib
import logging
import secrets
import threading
import time
from collections import Counter
from typing import Optional

logger = logging.getLogger(__name__)

FLUSH_INTERVAL_SECONDS = 30
FLUSH_BATCH_SIZE = 100

_buffer: list[dict] = []
_buffer_lock = threading.Lock()
_flush_thread: Optional[threading.Thread] = None
_running = True


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def hash_api_key(key_string: str) -> str:
    return hashlib.sha256(key_string.encode()).hexdigest()


import time as _time

# In-memory cache for API key validation — avoids DB round-trip on every request
_key_cache: dict[str, tuple] = {}  # key_string -> (key_id, timestamp)
_KEY_CACHE_TTL = 300  # 5 minutes


def validate_api_key(key_string: str) -> Optional[int]:
    """Returns the api_keys.id if the key exists and is active, else None.
    Results cached for 5 minutes to avoid DB query on every request."""
    if not key_string:
        return None

    now = _time.time()
    cached = _key_cache.get(key_string)
    if cached is not None and (now - cached[1]) < _KEY_CACHE_TTL:
        return cached[0]

    try:
        from app.database import fetch_one
        row = fetch_one(
            "SELECT id FROM api_keys WHERE key = %s AND is_active = TRUE",
            (key_string,)
        )
        result = row["id"] if row else None
    except Exception as e:
        logger.debug(f"validate_api_key error: {e}")
        result = None

    _key_cache[key_string] = (result, now)
    return result


def create_api_key(name: str) -> str:
    """Generates a new random API key, stores it, and returns the raw string."""
    from app.database import get_conn
    key_string = secrets.token_urlsafe(32)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO api_keys (key, name) VALUES (%s, %s) RETURNING id",
                (key_string, name)
            )
            conn.commit()
    return key_string


def list_api_keys() -> list[dict]:
    """Returns all API keys with usage stats."""
    try:
        from app.database import fetch_all
        rows = fetch_all("""
            SELECT
                k.id,
                k.name,
                k.created_at,
                k.last_used_at,
                k.total_requests,
                k.is_active,
                COUNT(l.id) AS requests_last_7d
            FROM api_keys k
            LEFT JOIN api_request_log l
                ON l.api_key_id = k.id
                AND l.timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY k.id
            ORDER BY k.created_at DESC
        """)
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning(f"list_api_keys error: {e}")
        return []


def get_usage_stats(days: int = 7) -> dict:
    """Aggregated usage stats from the request log."""
    try:
        from app.database import fetch_one, fetch_all

        since = f"NOW() - INTERVAL '{days} days'"

        total_today = fetch_one(
            "SELECT COUNT(*) AS c FROM api_request_log WHERE timestamp >= NOW() - INTERVAL '1 day'"
        )
        total_week = fetch_one(
            "SELECT COUNT(*) AS c FROM api_request_log WHERE timestamp >= NOW() - INTERVAL '7 days'"
        )
        total_month = fetch_one(
            "SELECT COUNT(*) AS c FROM api_request_log WHERE timestamp >= NOW() - INTERVAL '30 days'"
        )

        by_endpoint = fetch_all(f"""
            SELECT endpoint, COUNT(*) AS requests, AVG(response_time_ms) AS avg_ms
            FROM api_request_log
            WHERE timestamp >= {since}
            GROUP BY endpoint
            ORDER BY requests DESC
            LIMIT 20
        """)

        by_key = fetch_all(f"""
            SELECT
                COALESCE(k.name, 'unauthenticated') AS key_name,
                l.api_key_hash,
                COUNT(*) AS requests
            FROM api_request_log l
            LEFT JOIN api_keys k ON k.id = l.api_key_id
            WHERE l.timestamp >= {since}
            GROUP BY k.name, l.api_key_hash
            ORDER BY requests DESC
            LIMIT 20
        """)

        top_ips = fetch_all(f"""
            SELECT ip_address, COUNT(*) AS requests
            FROM api_request_log
            WHERE timestamp >= {since}
            GROUP BY ip_address
            ORDER BY requests DESC
            LIMIT 20
        """)

        return {
            "period_days": days,
            "total_today": total_today["c"] if total_today else 0,
            "total_week": total_week["c"] if total_week else 0,
            "total_month": total_month["c"] if total_month else 0,
            "by_endpoint": [dict(r) for r in by_endpoint],
            "by_key": [dict(r) for r in by_key],
            "top_ips": [dict(r) for r in top_ips],
        }
    except Exception as e:
        logger.warning(f"get_usage_stats error: {e}")
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Buffering + flush
# ---------------------------------------------------------------------------

def log_request(
    endpoint: str,
    method: str,
    status_code: Optional[int],
    response_time_ms: Optional[int],
    ip: str,
    api_key_id: Optional[int],
    api_key_hash: Optional[str],
    user_agent: str,
    accept_header: str = "",
    referer: str = "",
    is_internal: bool = False,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
) -> None:
    """Non-blocking append to the in-memory buffer."""
    entry = {
        "endpoint": endpoint,
        "method": method,
        "status_code": status_code,
        "response_time_ms": response_time_ms,
        "ip_address": ip,
        "api_key_id": api_key_id,
        "api_key_hash": api_key_hash,
        "user_agent": (user_agent or "")[:500],
        "accept_header": (accept_header or "")[:255],
        "referer": (referer or "")[:500],
        "is_internal": is_internal,
        "entity_type": entity_type,
        "entity_id": (entity_id or "")[:100] if entity_id else None,
    }
    with _buffer_lock:
        _buffer.append(entry)
        should_flush = len(_buffer) >= FLUSH_BATCH_SIZE

    if should_flush:
        _flush_sync()


def flush() -> None:
    """Flush the buffer to the database. Safe to call when empty."""
    _flush_sync()


def _flush_sync() -> None:
    with _buffer_lock:
        if not _buffer:
            return
        batch = _buffer[:]
        _buffer.clear()

    if not batch:
        return

    try:
        from app.database import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.executemany(
                        """
                        INSERT INTO api_request_log
                            (endpoint, method, status_code, response_time_ms,
                             ip_address, api_key_id, api_key_hash, user_agent,
                             accept_header, referer, is_internal, entity_type, entity_id)
                        VALUES
                            (%(endpoint)s, %(method)s, %(status_code)s, %(response_time_ms)s,
                             %(ip_address)s, %(api_key_id)s, %(api_key_hash)s, %(user_agent)s,
                             %(accept_header)s, %(referer)s, %(is_internal)s, %(entity_type)s, %(entity_id)s)
                        """,
                        batch
                    )
                except Exception:
                    # Fallback if new columns don't exist yet (migration not run)
                    conn.rollback()
                    cur.executemany(
                        """
                        INSERT INTO api_request_log
                            (endpoint, method, status_code, response_time_ms,
                             ip_address, api_key_id, api_key_hash, user_agent)
                        VALUES
                            (%(endpoint)s, %(method)s, %(status_code)s, %(response_time_ms)s,
                             %(ip_address)s, %(api_key_id)s, %(api_key_hash)s, %(user_agent)s)
                        """,
                        batch
                    )
                key_counts = Counter(e["api_key_id"] for e in batch if e["api_key_id"])
                for key_id, count in key_counts.items():
                    cur.execute(
                        "UPDATE api_keys SET total_requests = total_requests + %s, last_used_at = NOW() WHERE id = %s",
                        (count, key_id)
                    )
                conn.commit()
        logger.debug(f"Flushed {len(batch)} request log entries")
    except Exception as e:
        logger.warning(f"usage_tracker flush error: {e}")
        with _buffer_lock:
            _buffer[:0] = batch


def _flush_loop() -> None:
    global _running
    _cleanup_counter = 0
    while _running:
        time.sleep(FLUSH_INTERVAL_SECONDS)
        try:
            _flush_sync()
        except Exception as e:
            logger.warning(f"Periodic flush error: {e}")
        _cleanup_counter += 1
        if _cleanup_counter % 10 == 0:
            try:
                from app.rate_limiter import rate_limiter
                rate_limiter.cleanup()
            except Exception:
                pass


def _start_flush_thread() -> None:
    global _flush_thread
    _flush_thread = threading.Thread(target=_flush_loop, daemon=True, name="usage-tracker-flush")
    _flush_thread.start()


_start_flush_thread()
