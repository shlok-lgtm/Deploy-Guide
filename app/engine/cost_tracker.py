"""
Component 2c: Cost tracker for LLM interpretation calls.

Two layers of guardrails:
  - Daily call ceiling (DAILY_CALL_HARD_CEILING = 50). Counts cache misses
    that resulted in API calls (cache hits don't count — those didn't
    spend tokens). Implemented by counting fresh INSERTs into
    engine_interpretation_cache for the current UTC day.
  - Monthly cost cap (MONTHLY_BUDGET_USD = 200). Aggregates token totals
    from the cache table since the start of the current UTC month, prices
    them at Sonnet 4.6 rates ($3 input / $15 output per 1M tokens), and
    blocks new calls when projected spend ≥ the cap.

When either guardrail trips, get_or_call_interpretation falls back to the
SHAPE_API_UNAVAILABLE template (not cached) so analyses still complete with
a degraded Interpretation; operator can re-run via force_new=true once the
budget rolls over or after raising the cap via env var.

Both thresholds are env-overridable so the operator can adjust without a
deploy:
  - BASIS_ENGINE_LLM_MONTHLY_BUDGET_USD
  - BASIS_ENGINE_LLM_DAILY_CALL_CEILING

This module is sync (matches the rest of the engine pipeline). DB queries
use the psycopg2 helpers in app.database.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone
from typing import Optional

from app.database import fetch_one

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Pricing — Sonnet 4.6, April 2026
# ─────────────────────────────────────────────────────────────────
INPUT_PRICE_PER_M_USD = 3.0
OUTPUT_PRICE_PER_M_USD = 15.0


# ─────────────────────────────────────────────────────────────────
# Budget thresholds (env-overridable)
# ─────────────────────────────────────────────────────────────────

def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning(
            "cost_tracker: invalid float in %s=%r; using default %s",
            name, raw, default,
        )
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            "cost_tracker: invalid int in %s=%r; using default %s",
            name, raw, default,
        )
        return default


def get_monthly_budget_usd() -> float:
    return _env_float("BASIS_ENGINE_LLM_MONTHLY_BUDGET_USD", 200.0)


def get_daily_call_ceiling() -> int:
    return _env_int("BASIS_ENGINE_LLM_DAILY_CALL_CEILING", 50)


# ─────────────────────────────────────────────────────────────────
# Internals
# ─────────────────────────────────────────────────────────────────

def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _month_start_utc() -> date:
    return _today_utc().replace(day=1)


def _compute_cost_usd(input_tokens: int, output_tokens: int) -> float:
    return (
        (input_tokens / 1_000_000) * INPUT_PRICE_PER_M_USD
        + (output_tokens / 1_000_000) * OUTPUT_PRICE_PER_M_USD
    )


# ─────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────

def can_make_call() -> tuple[bool, Optional[str]]:
    """Return (allowed, reason).

    reason is None when allowed; a short string otherwise — used directly
    in the SHAPE_API_UNAVAILABLE confidence_reasoning.
    """
    today = _today_utc()
    month_start = _month_start_utc()

    daily_ceiling = get_daily_call_ceiling()
    monthly_budget = get_monthly_budget_usd()

    # Daily ceiling check — count today's cache writes (one per API call)
    today_row = fetch_one(
        """
        SELECT COUNT(*) AS n
        FROM engine_interpretation_cache
        WHERE created_at::date = %s
        """,
        (today,),
    )
    today_count = (today_row or {}).get("n", 0) or 0
    if today_count >= daily_ceiling:
        reason = (
            f"daily LLM call ceiling reached "
            f"({today_count}/{daily_ceiling} calls today)"
        )
        logger.warning("cost_tracker: %s", reason)
        return False, reason

    # Monthly cost check
    month_row = fetch_one(
        """
        SELECT
          COALESCE(SUM(token_input_count), 0)  AS in_tokens,
          COALESCE(SUM(token_output_count), 0) AS out_tokens
        FROM engine_interpretation_cache
        WHERE created_at >= %s
        """,
        (month_start,),
    ) or {}
    in_tokens = int(month_row.get("in_tokens") or 0)
    out_tokens = int(month_row.get("out_tokens") or 0)
    cost = _compute_cost_usd(in_tokens, out_tokens)

    if cost >= monthly_budget:
        reason = (
            f"monthly LLM budget reached "
            f"(${cost:.2f}/${monthly_budget:.2f} this month)"
        )
        logger.warning("cost_tracker: %s", reason)
        return False, reason

    return True, None


def record_call(input_tokens: int, output_tokens: int) -> None:
    """No-op hook for forward compatibility. The cache write in
    interpretation.py already records token counts on the cache row;
    this function exists so future expansion (separate cost log,
    Datadog metric, etc.) has a single attachment point."""
    pass


def get_budget_status() -> dict:
    """Snapshot of the current budget state. Used by the
    GET /api/engine/budget endpoint and surfaced in operator runbooks."""
    today = _today_utc()
    month_start = _month_start_utc()
    daily_ceiling = get_daily_call_ceiling()
    monthly_budget = get_monthly_budget_usd()

    today_row = fetch_one(
        """
        SELECT COUNT(*) AS n
        FROM engine_interpretation_cache
        WHERE created_at::date = %s
        """,
        (today,),
    ) or {}
    today_count = int(today_row.get("n") or 0)

    month_row = fetch_one(
        """
        SELECT
          COUNT(*)                            AS calls,
          COALESCE(SUM(token_input_count), 0) AS in_tokens,
          COALESCE(SUM(token_output_count), 0) AS out_tokens
        FROM engine_interpretation_cache
        WHERE created_at >= %s
        """,
        (month_start,),
    ) or {}
    month_calls = int(month_row.get("calls") or 0)
    in_tokens = int(month_row.get("in_tokens") or 0)
    out_tokens = int(month_row.get("out_tokens") or 0)
    cost = _compute_cost_usd(in_tokens, out_tokens)

    return {
        "today_utc": today.isoformat(),
        "month_start_utc": month_start.isoformat(),
        "today_calls": today_count,
        "today_calls_remaining": max(0, daily_ceiling - today_count),
        "today_calls_ceiling": daily_ceiling,
        "month_calls": month_calls,
        "month_input_tokens": in_tokens,
        "month_output_tokens": out_tokens,
        "month_cost_usd": round(cost, 4),
        "month_budget_usd": monthly_budget,
        "month_budget_remaining_usd": round(max(0.0, monthly_budget - cost), 4),
        "input_price_per_m_usd": INPUT_PRICE_PER_M_USD,
        "output_price_per_m_usd": OUTPUT_PRICE_PER_M_USD,
    }
