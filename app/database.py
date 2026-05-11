"""
Basis Protocol - Database
Simple PostgreSQL connection management with connection pooling.
"""

import asyncio
import os
import logging
import traceback
from contextlib import contextmanager
from typing import Optional, Any

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)

# Global counter to dedupe identical call sites — log each unique
# file:line at most once per process. Otherwise a hot loop floods logs.
_sync_in_async_seen: set[tuple[str, int]] = set()


def _warn_if_async_context(helper_name: str) -> None:
    """If called from a running event loop, log a deduped warning with the caller's file:line.

    Does NOT raise — Phase 1 is observation only. Phase 3 will promote
    this to RuntimeError.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return  # No running loop — sync caller, fine.

    stack = traceback.extract_stack()
    if len(stack) < 3:
        return
    caller = stack[-3]
    key = (caller.filename, caller.lineno)
    if key in _sync_in_async_seen:
        return
    _sync_in_async_seen.add(key)
    logger.error(
        f"[sync-in-async] {helper_name} called from async context at "
        f"{caller.filename}:{caller.lineno} ({caller.name})"
    )

_pool: Optional[ThreadedConnectionPool] = None

# Neon's -pooler endpoint is pgbouncer in transaction mode and rejects libpq
# startup `options` (incl. `-c statement_timeout=...`). We apply the timeout
# per-transaction via SET LOCAL inside get_conn() instead.
_STATEMENT_TIMEOUT_MS = 120000  # 120 s query timeout


def init_pool(database_url: Optional[str] = None, min_conn: int = 5, max_conn: int = 50):
    """Initialize the connection pool. Call once at startup."""
    global _pool
    url = database_url or os.environ.get("DATABASE_URL", "")
    if not url:
        logger.error("DATABASE_URL not set — database unavailable")
        return
    logger.info(f"Database URL prefix: {url[:50]}...")
    try:
        # TCP keepalives: prevent the OS from silently dropping idle connections
        # after PostgreSQL's idle-session timeout. Without this, connections that
        # sit in the pool for >~30 min get closed server-side and the next caller
        # gets a "connection already closed" error.
        #
        # NOTE: do NOT pass `options="-c statement_timeout=..."` here — Neon's
        # -pooler endpoint (pgbouncer, transaction mode) rejects any libpq
        # startup `options` parameter. Statement timeout is applied per
        # transaction inside get_conn() via SET LOCAL.
        _pool = ThreadedConnectionPool(
            min_conn, max_conn, url,
            keepalives=1,
            keepalives_idle=30,     # start probing after 30 s idle
            keepalives_interval=10, # retry probe every 10 s
            keepalives_count=5,     # drop after 5 failed probes
            connect_timeout=10,     # 10s connection timeout
        )
        logger.info(f"Database pool initialized (min={min_conn}, max={max_conn}, keepalives=on)")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        _pool = None


def close_pool():
    """Close all connections in the pool. Call at shutdown."""
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None
        logger.info("Database pool closed")


@contextmanager
def get_conn():
    """Get a database connection from the pool. Auto-returns on exit.

    Validates the connection before yielding: if psycopg2 has already marked it
    closed (conn.closed != 0) — which happens when PostgreSQL drops an idle
    connection server-side — the dead connection is discarded and a fresh one is
    obtained.  This prevents "connection already closed" errors in long-running
    background tasks (e.g. the wallet indexer pipeline).

    Additionally performs a SELECT 1 liveness ping to detect connections that
    appear open (conn.closed == 0) but are actually broken at the TCP level.
    psycopg2 does not update conn.closed when PostgreSQL drops the connection
    server-side, so the ping catches these stale connections before they cause a
    500 on the first real query.
    """
    if _pool is None:
        raise RuntimeError("Database pool not initialized. Call init_pool() first.")
    conn = _pool.getconn()
    # conn.closed: 0 = open, 1 = closed cleanly, 2 = broken/lost
    if conn.closed:
        logger.warning("Stale connection in pool (closed=%d) — discarding and replacing", conn.closed)
        try:
            _pool.putconn(conn, close=True)
        except Exception:
            pass
        conn = _pool.getconn()
    # Liveness ping: conn.closed may still be 0 even when the server has dropped
    # the connection (TCP-level drop is invisible to psycopg2 until a round-trip).
    # A cheap SELECT 1 catches these broken connections before the caller's query.
    # Retry up to 2 times so the replacement connection is also validated.
    _max_ping_attempts = 3
    _ping_ok = False
    for _attempt in range(_max_ping_attempts):
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            _ping_ok = True
            break  # connection is healthy
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as ping_err:
            logger.error(
                "Connection failed liveness ping (attempt %d/%d) — discarding and replacing: %s",
                _attempt + 1, _max_ping_attempts, ping_err,
            )
            try:
                _pool.putconn(conn, close=True)
            except Exception:
                pass
            conn = _pool.getconn()

    if not _ping_ok:
        logger.critical(
            "All %d liveness pings failed — Neon SSL connection unrecoverable. "
            "Raising to force caller retry.",
            _max_ping_attempts,
        )
        try:
            _pool.putconn(conn, close=True)
        except Exception:
            pass
        raise psycopg2.OperationalError("Database connection unrecoverable after liveness ping failures")

    try:
        # SET LOCAL is scoped to the current transaction, so it survives
        # pgbouncer transaction-mode multiplexing (unlike a session-level SET).
        # The liveness-ping SELECT above already opened an implicit transaction,
        # so SET LOCAL applies for the rest of this checkout.
        try:
            _to_cur = conn.cursor()
            _to_cur.execute("SET LOCAL statement_timeout = %s", (_STATEMENT_TIMEOUT_MS,))
            _to_cur.close()
        except Exception as _to_err:
            logger.warning("Could not SET LOCAL statement_timeout: %s", _to_err)
        yield conn
        conn.commit()
    except Exception as exc:
        # UndefinedTable is expected when optional dbt models haven't been
        # materialized yet — log at DEBUG to avoid noisy ERROR-level alerts.
        try:
            from psycopg2.errors import UndefinedTable
            if isinstance(exc, UndefinedTable):
                logger.debug("Query referenced a missing table (expected if dbt models not yet built): %s", exc)
            else:
                logger.error("Database error in get_conn(): %s", exc, exc_info=True)
        except ImportError:
            logger.error("Database error in get_conn(): %s", exc, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            _pool.putconn(conn)
        except Exception:
            pass


@contextmanager
def get_cursor(dict_cursor: bool = False):
    """Get a cursor with auto-commit/rollback. Most common usage pattern."""
    _warn_if_async_context("get_cursor")
    with get_conn() as conn:
        cursor_factory = psycopg2.extras.RealDictCursor if dict_cursor else None
        cur = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cur
        finally:
            cur.close()


def execute(sql: str, params: tuple = None) -> None:
    """Execute a single statement (INSERT, UPDATE, DELETE)."""
    _warn_if_async_context("execute")
    with get_cursor() as cur:
        cur.execute(sql, params)


def fetch_one(sql: str, params: tuple = None) -> Optional[dict]:
    """Fetch a single row as a dict."""
    _warn_if_async_context("fetch_one")
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def fetch_all(sql: str, params: tuple = None) -> list[dict]:
    """Fetch all rows as a list of dicts."""
    _warn_if_async_context("fetch_all")
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


# =============================================================================
# Async wrappers — run sync psycopg2 calls in thread pool so they don't
# block the asyncio event loop. Use these from async functions.
# =============================================================================

async def fetch_one_async(sql: str, params: tuple = None) -> Optional[dict]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, fetch_one, sql, params)

async def fetch_all_async(sql: str, params: tuple = None) -> list[dict]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, fetch_all, sql, params)

async def execute_async(sql: str, params: tuple = None) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, execute, sql, params)


def run_migration(migration_path: str) -> bool:
    """Run a SQL migration file."""
    try:
        with open(migration_path, 'r') as f:
            sql = f.read()
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
        logger.info(f"Migration applied: {migration_path}")
        return True
    except Exception as e:
        logger.error(f"Migration failed ({migration_path}): {e}")
        return False


def health_check() -> dict:
    """Check database connectivity and return status."""
    try:
        result = fetch_one("SELECT COUNT(*) as stablecoin_count FROM stablecoins")
        return {
            "status": "healthy",
            "stablecoin_count": result["stablecoin_count"] if result else 0,
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }
