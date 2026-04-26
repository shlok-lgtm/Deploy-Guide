"""
Backfill Base — shared utilities for all backfill scripts.
"""

import argparse
import asyncio
import logging
import os
import sys
import uuid
from datetime import datetime, timezone, timedelta

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

logger = logging.getLogger(__name__)


def parse_args():
    """Parse common CLI arguments for backfill scripts."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="Limit to N entities (0=all)")
    parser.add_argument("--days-back", type=int, default=365, help="Days of history")
    return parser.parse_args()


def init_db():
    """Initialize the database connection pool."""
    from app.database import init_pool
    init_pool()


def log_run_start(index_name: str, entity_slug: str, source: str) -> str:
    """Log a backfill run start. Returns run_id."""
    from app.database import fetch_one
    run_id = str(uuid.uuid4())
    try:
        fetch_one(
            """INSERT INTO backfill_runs (run_id, index_name, entity_slug, source_used)
               VALUES (%s, %s, %s, %s) RETURNING run_id""",
            (run_id, index_name, entity_slug, source),
        )
    except Exception as e:
        logger.warning(f"Failed to log backfill start: {e}")
    return run_id


def log_run_complete(run_id: str, rows_written: int, rows_failed: int, error: str = None):
    """Log a backfill run completion."""
    from app.database import execute
    try:
        execute(
            """UPDATE backfill_runs SET completed_at = NOW(),
               rows_written = %s, rows_failed = %s, error = %s
               WHERE run_id = %s""",
            (rows_written, rows_failed, error, run_id),
        )
    except Exception as e:
        logger.warning(f"Failed to log backfill completion: {e}")


def check_resume(index_name: str, entity_slug: str) -> str | None:
    """Check for an incomplete run to resume. Returns run_id or None."""
    from app.database import fetch_one
    try:
        row = fetch_one(
            """SELECT run_id FROM backfill_runs
               WHERE index_name = %s AND entity_slug = %s AND completed_at IS NULL
               ORDER BY started_at DESC LIMIT 1""",
            (index_name, entity_slug),
        )
        return row["run_id"] if row else None
    except Exception:
        return None


def date_range(start_date: datetime, end_date: datetime, step_days: int = 1):
    """Generate date range from start to end."""
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=step_days)
