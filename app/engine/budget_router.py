"""
Component 2c: Budget observability endpoint.

GET /api/engine/budget — admin-only. Returns cost_tracker.get_budget_status()
verbatim. Useful for ops dashboards and when an analysis returns
SHAPE_API_UNAVAILABLE — the operator can confirm whether the cause was
the daily ceiling, the monthly cap, or something else.

Auth: inline _check_admin_key, matching the pattern in analyze_router.py
and the rest of the codebase. No shared dependency module.
"""

from __future__ import annotations

import hmac
import logging
import os

from fastapi import APIRouter, HTTPException, Request

from app.engine import cost_tracker

logger = logging.getLogger(__name__)

router = APIRouter()


def _check_admin_key(request: Request) -> None:
    admin_key = os.environ.get("ADMIN_KEY", "")
    provided = (
        request.query_params.get("key", "")
        or request.headers.get("x-admin-key", "")
    )
    if not admin_key or not provided or not hmac.compare_digest(provided, admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized")


@router.get("/api/engine/budget")
def get_budget(request: Request) -> dict:
    """Return current LLM budget state — daily call count vs ceiling and
    monthly token spend vs cap. Read directly from
    engine_interpretation_cache (no separate counters table)."""
    _check_admin_key(request)
    return cost_tracker.get_budget_status()
