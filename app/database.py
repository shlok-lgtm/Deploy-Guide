"""
Basis Protocol - Database
Simple PostgreSQL connection management with connection pooling.
"""

import os
import logging
from contextlib import contextmanager
from typing import Optional, Any

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)

_pool: Optional[ThreadedConnectionPool] = None


def init_pool(database_url: Optional[str] = None, min_conn: int = 2, max_conn: int = 10):
    """Initialize the connection pool. Call once at startup."""
    global _pool
    url = database_url or os.environ.get("DATABASE_URL", "")
    if not url:
        logger.error("DATABASE_URL not set — database unavailable")
        return
    try:
        # TCP keepalives: prevent the OS from silently dropping idle connections
        # after PostgreSQL's idle-session timeout. Without this, connections that
        # sit in the pool for >~30 min get closed server-side and the next caller
        # gets a "connection already closed" error.
        _pool = ThreadedConnectionPool(
            min_conn, max_conn, url,
            keepalives=1,
            keepalives_idle=30,     # start probing after 30 s idle
            keepalives_interval=10, # retry probe every 10 s
            keepalives_count=5,     # drop after 5 failed probes
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
    try:
        yield conn
        conn.commit()
    except Exception:
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
    with get_conn() as conn:
        cursor_factory = psycopg2.extras.RealDictCursor if dict_cursor else None
        cur = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cur
        finally:
            cur.close()


def execute(sql: str, params: tuple = None) -> None:
    """Execute a single statement (INSERT, UPDATE, DELETE)."""
    with get_cursor() as cur:
        cur.execute(sql, params)


def fetch_one(sql: str, params: tuple = None) -> Optional[dict]:
    """Fetch a single row as a dict."""
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def fetch_all(sql: str, params: tuple = None) -> list[dict]:
    """Fetch all rows as a list of dicts."""
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


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
