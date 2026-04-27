"""
Component 4: Watchlist threshold evaluator.

Operators add rows to engine_watchlist describing a threshold to monitor:

  - threshold_type ∈ {score_below, score_above, tvl_drop_pct, score_drop_abs}
  - threshold_value: numeric trigger
  - measure_name: which measure to read (e.g., "security" inside PSI
    category_scores). Optional for tvl_drop_pct (implicit "tvl").
  - last_triggered_at: cooldown tracking — same row won't re-trigger
    inside 24h to prevent storms when a measure oscillates near a
    threshold.

evaluate_watchlist() runs every 15 min via APScheduler. For each active
row:

  1. Fetch current measure value from production (latest scored_date)
  2. Fetch a "previous" value (1 day ago) for crossing semantics
  3. If threshold crossed AND not in 24h cooldown:
     - INSERT a new engine_events row (source='watchlist', event_type=
       'threshold_crossed') with raw_event_data describing the crossing
     - Update the watchlist row's last_triggered_at
     - Spawn process_event in the background to trigger an analysis

Crossing semantics — designed to fire ON the crossing, not on every
sample below:

  score_below     : previous ≥ threshold AND current < threshold
  score_above     : previous ≤ threshold AND current > threshold
  tvl_drop_pct    : (previous - current) / previous * 100 ≥ threshold
                    over the last 24h, taken once per crossing window
  score_drop_abs  : (previous - current) ≥ threshold

The cooldown collapses the "fires once per crossing window" guarantee
even if a measure dithers — between a fire and the cooldown expiring,
the row simply won't re-fire.

Required schema columns added by migration 100:
    last_triggered_at TIMESTAMPTZ
    measure_name      TEXT
    notes             TEXT
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal, Optional
from uuid import UUID

import psycopg2.extras

from app.database import fetch_all, fetch_one, get_cursor
from app.engine.event_pipeline import process_event

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Constants + types
# ─────────────────────────────────────────────────────────────────

# Local literal — the schemas.py ThresholdType is a different (S0-era)
# set; C4 types live here so schemas.py stays untouched.
ThresholdType = Literal[
    "score_below", "score_above", "tvl_drop_pct", "score_drop_abs",
]

VALID_THRESHOLD_TYPES: set[str] = {
    "score_below", "score_above", "tvl_drop_pct", "score_drop_abs",
}

COOLDOWN_SECONDS = 24 * 60 * 60  # 24 hours


# ─────────────────────────────────────────────────────────────────
# Watchlist row read/write
# ─────────────────────────────────────────────────────────────────

_WATCHLIST_COLUMNS = """
    id, entity_slug, index_id, threshold_type, threshold_value,
    measure_name, notes, active, last_triggered_at, created_at
"""


def _row_to_watchlist_dict(row: dict) -> dict:
    """Pass-through; dict is the canonical shape used by the rest of
    this module + events_router. We don't bind to schemas.py
    WatchlistEntry because the C4 fields (measure_name, notes,
    last_triggered_at) aren't on it."""
    return dict(row)


def _list_active_watchlist_sync() -> list[dict]:
    rows = fetch_all(
        f"""
        SELECT {_WATCHLIST_COLUMNS}
        FROM engine_watchlist
        WHERE active = TRUE
        ORDER BY created_at DESC
        """
    )
    return [_row_to_watchlist_dict(r) for r in rows]


async def list_active_watchlist() -> list[dict]:
    return await asyncio.to_thread(_list_active_watchlist_sync)


def _update_last_triggered_sync(watchlist_id: UUID) -> None:
    with get_cursor() as cur:
        cur.execute(
            "UPDATE engine_watchlist SET last_triggered_at = NOW() WHERE id = %s",
            (str(watchlist_id),),
        )


async def update_last_triggered(watchlist_id: UUID) -> None:
    await asyncio.to_thread(_update_last_triggered_sync, watchlist_id)


# ─────────────────────────────────────────────────────────────────
# Measure value lookups
#
# Reads from generic_index_scores (overall_score column or category /
# component / raw_values JSONB) for non-SII indexes, scores +
# score_history for SII, historical_protocol_data for PSI fundamentals
# (tvl, fees, etc).
# ─────────────────────────────────────────────────────────────────

def _coerce_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f


def _fetch_value_at_or_before_sync(
    *,
    entity_slug: str,
    index_id: str,
    measure_name: Optional[str],
    target_date: date,
) -> Optional[float]:
    """Look up the measure value at or before target_date. Most recent
    record on or before that day. Returns None if no record found.

    Dispatch:
      - SII (index_id='sii'): score_history columns matching measure_name,
        else 'overall_score'.
      - PSI fundamentals (index_id='psi' AND measure_name in tvl/fees/etc):
        historical_protocol_data columns.
      - Otherwise (generic_index_scores): overall_score column or one of
        category_scores/component_scores/raw_values JSONB lookups.
    """
    # SII path
    if index_id == "sii":
        col = measure_name if measure_name in {
            "overall_score", "peg_score", "liquidity_score", "mint_burn_score",
            "distribution_score", "structural_score", "reserves_score",
            "contract_score", "oracle_score", "governance_score", "network_score",
        } else "overall_score"
        row = fetch_one(
            f"""
            SELECT {col} AS v
            FROM score_history
            WHERE stablecoin = %s AND score_date <= %s
            ORDER BY score_date DESC
            LIMIT 1
            """,
            (entity_slug, target_date),
        )
        return _coerce_float(row.get("v") if row else None)

    # PSI fundamentals via historical_protocol_data
    if index_id == "psi" and measure_name in {
        "tvl", "fees_24h", "revenue_24h", "token_price",
        "token_mcap", "token_volume", "chain_count",
    }:
        row = fetch_one(
            f"""
            SELECT {measure_name} AS v
            FROM historical_protocol_data
            WHERE protocol_slug = %s AND record_date <= %s
            ORDER BY record_date DESC
            LIMIT 1
            """,
            (entity_slug, target_date),
        )
        return _coerce_float(row.get("v") if row else None)

    # Generic path — overall_score column when measure_name is empty/None
    # or matches "overall_score"; otherwise look inside the JSONB columns.
    if not measure_name or measure_name == "overall_score":
        row = fetch_one(
            """
            SELECT overall_score AS v
            FROM generic_index_scores
            WHERE index_id = %s AND entity_slug = %s AND scored_date <= %s
            ORDER BY scored_date DESC
            LIMIT 1
            """,
            (index_id, entity_slug, target_date),
        )
        return _coerce_float(row.get("v") if row else None)

    row = fetch_one(
        """
        SELECT category_scores, component_scores, raw_values
        FROM generic_index_scores
        WHERE index_id = %s AND entity_slug = %s AND scored_date <= %s
        ORDER BY scored_date DESC
        LIMIT 1
        """,
        (index_id, entity_slug, target_date),
    )
    if row is None:
        return None
    for col in ("category_scores", "component_scores", "raw_values"):
        payload = row.get(col)
        if isinstance(payload, dict) and measure_name in payload:
            return _coerce_float(payload[measure_name])
    return None


async def fetch_value_at_or_before(
    *,
    entity_slug: str,
    index_id: str,
    measure_name: Optional[str],
    target_date: date,
) -> Optional[float]:
    return await asyncio.to_thread(
        _fetch_value_at_or_before_sync,
        entity_slug=entity_slug,
        index_id=index_id,
        measure_name=measure_name,
        target_date=target_date,
    )


# ─────────────────────────────────────────────────────────────────
# Threshold-crossing logic — pure functions for testability
# ─────────────────────────────────────────────────────────────────

def crosses_below(
    previous: Optional[float], current: Optional[float], threshold: float,
) -> bool:
    """previous ≥ threshold AND current < threshold."""
    if previous is None or current is None:
        return False
    return previous >= threshold and current < threshold


def crosses_above(
    previous: Optional[float], current: Optional[float], threshold: float,
) -> bool:
    """previous ≤ threshold AND current > threshold."""
    if previous is None or current is None:
        return False
    return previous <= threshold and current > threshold


def tvl_drop_exceeds_pct(
    previous: Optional[float], current: Optional[float], threshold_pct: float,
) -> bool:
    """(previous - current) / previous * 100 ≥ threshold_pct.
    Returns False on missing values or zero/negative previous."""
    if previous is None or current is None or previous <= 0:
        return False
    pct_drop = (previous - current) / previous * 100
    return pct_drop >= threshold_pct


def score_drops_by(
    previous: Optional[float], current: Optional[float], threshold_abs: float,
) -> bool:
    """(previous - current) ≥ threshold_abs."""
    if previous is None or current is None:
        return False
    return (previous - current) >= threshold_abs


# ─────────────────────────────────────────────────────────────────
# Cooldown
# ─────────────────────────────────────────────────────────────────

def is_in_cooldown(
    last_triggered_at: Optional[datetime],
    *,
    now: Optional[datetime] = None,
    cooldown_seconds: int = COOLDOWN_SECONDS,
) -> bool:
    if last_triggered_at is None:
        return False
    now = now or datetime.now(timezone.utc)
    # Defensive: if the stored timestamp is naive, assume UTC
    if last_triggered_at.tzinfo is None:
        last_triggered_at = last_triggered_at.replace(tzinfo=timezone.utc)
    age = (now - last_triggered_at).total_seconds()
    return age < cooldown_seconds


# ─────────────────────────────────────────────────────────────────
# Single-row evaluation (testable)
# ─────────────────────────────────────────────────────────────────

async def evaluate_one_row(row: dict, *, now: Optional[datetime] = None) -> Optional[dict]:
    """Returns a crossing-detail dict if the threshold fired and the
    row isn't in cooldown; None otherwise. Caller is responsible for
    persisting the resulting event + analysis trigger.
    """
    now = now or datetime.now(timezone.utc)

    if is_in_cooldown(row.get("last_triggered_at"), now=now):
        return None

    threshold_type = row["threshold_type"]
    if threshold_type not in VALID_THRESHOLD_TYPES:
        # Belt-and-suspenders — schema is TEXT and an old/wrong row could exist
        logger.warning(
            "watchlist: row %s has unsupported threshold_type=%r; skipping",
            row["id"], threshold_type,
        )
        return None

    entity_slug = row["entity_slug"]
    index_id = row["index_id"]
    measure_name = row.get("measure_name")
    threshold_value = float(row["threshold_value"])

    # Decide which (index_id, measure_name) tuple to read for tvl_drop_pct.
    # Watchlist row may leave measure_name NULL for tvl_drop_pct; fill in
    # the implicit defaults here so the value lookup works.
    if threshold_type == "tvl_drop_pct":
        eff_index_id = index_id or "psi"
        eff_measure = measure_name or "tvl"
    else:
        eff_index_id = index_id or ""
        eff_measure = measure_name

    today = now.date()
    yesterday = today - timedelta(days=1)

    current = await fetch_value_at_or_before(
        entity_slug=entity_slug,
        index_id=eff_index_id,
        measure_name=eff_measure,
        target_date=today,
    )
    previous = await fetch_value_at_or_before(
        entity_slug=entity_slug,
        index_id=eff_index_id,
        measure_name=eff_measure,
        target_date=yesterday,
    )

    crossed = False
    if threshold_type == "score_below":
        crossed = crosses_below(previous, current, threshold_value)
    elif threshold_type == "score_above":
        crossed = crosses_above(previous, current, threshold_value)
    elif threshold_type == "tvl_drop_pct":
        crossed = tvl_drop_exceeds_pct(previous, current, threshold_value)
    elif threshold_type == "score_drop_abs":
        crossed = score_drops_by(previous, current, threshold_value)

    if not crossed:
        return None

    return {
        "watchlist_id": row["id"],
        "entity_slug": entity_slug,
        "index_id": eff_index_id,
        "measure_name": eff_measure,
        "threshold_type": threshold_type,
        "threshold_value": threshold_value,
        "previous_value": previous,
        "current_value": current,
        "evaluated_at": now.isoformat(),
    }


# ─────────────────────────────────────────────────────────────────
# Persist a watchlist-fired event + spawn analysis trigger
# ─────────────────────────────────────────────────────────────────

def _insert_watchlist_event_sync(crossing: dict) -> Optional[UUID]:
    raw_json = psycopg2.extras.Json({
        "watchlist_crossing": crossing,
    })
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(
            """
            INSERT INTO engine_events (
                source, event_type, entity, event_date, severity,
                raw_event_data, status
            ) VALUES (
                'watchlist',
                'threshold_crossed',
                %s,
                CURRENT_DATE,
                %s,
                %s,
                'new'
            )
            ON CONFLICT (source, entity, event_date, event_type) DO NOTHING
            RETURNING id
            """,
            (
                crossing["entity_slug"],
                _severity_for_crossing(crossing),
                raw_json,
            ),
        )
        row = cur.fetchone()
        return row["id"] if row else None


def _severity_for_crossing(crossing: dict) -> str:
    """Heuristic severity for a watchlist crossing. score_below/above
    use the magnitude of the crossing relative to the threshold;
    tvl_drop_pct + score_drop_abs scale by the drop size."""
    t = crossing["threshold_type"]
    if t == "tvl_drop_pct":
        prev = crossing.get("previous_value") or 0
        cur = crossing.get("current_value") or 0
        if prev > 0:
            pct = (prev - cur) / prev * 100
            if pct >= 50:
                return "critical"
            if pct >= 25:
                return "high"
            return "medium"
        return "medium"
    if t == "score_drop_abs":
        prev = crossing.get("previous_value") or 0
        cur = crossing.get("current_value") or 0
        drop = prev - cur
        if drop >= 30:
            return "high"
        if drop >= 15:
            return "medium"
        return "low"
    # score_below / score_above: severity = how far past the threshold
    return "medium"


async def insert_watchlist_event(crossing: dict) -> Optional[UUID]:
    return await asyncio.to_thread(_insert_watchlist_event_sync, crossing)


# ─────────────────────────────────────────────────────────────────
# Public entry — scheduler calls this every 15 min
# ─────────────────────────────────────────────────────────────────

async def evaluate_watchlist() -> dict:
    """Evaluate every active watchlist row. For each crossing:
      - INSERT engine_events row (deduped by unique constraint)
      - UPDATE watchlist.last_triggered_at
      - asyncio.create_task(process_event(event_id)) to fire the analysis

    Returns a summary dict for logging.
    """
    summary = {
        "rows_evaluated": 0,
        "in_cooldown": 0,
        "crossings": 0,
        "events_inserted": 0,
        "duplicates": 0,
        "errors": 0,
    }

    rows = await list_active_watchlist()
    summary["rows_evaluated"] = len(rows)
    logger.info("evaluate_watchlist: %d active rows", len(rows))

    now = datetime.now(timezone.utc)

    for row in rows:
        try:
            if is_in_cooldown(row.get("last_triggered_at"), now=now):
                summary["in_cooldown"] += 1
                continue

            crossing = await evaluate_one_row(row, now=now)
            if crossing is None:
                continue
            summary["crossings"] += 1

            event_id = await insert_watchlist_event(crossing)
            if event_id is None:
                summary["duplicates"] += 1
                # Still update last_triggered_at — the crossing happened,
                # we just deduped at the events layer. Avoids tight-loop
                # re-firing on every poll.
                await update_last_triggered(row["id"])
                continue

            summary["events_inserted"] += 1
            await update_last_triggered(row["id"])

            logger.info(
                "evaluate_watchlist: fired event_id=%s entity=%s "
                "threshold=%s value=%.4f→%.4f",
                event_id, row["entity_slug"], row["threshold_type"],
                crossing.get("previous_value") or 0.0,
                crossing.get("current_value") or 0.0,
            )

            # Fire-and-forget analysis trigger
            try:
                asyncio.create_task(process_event(event_id))
            except Exception:
                summary["errors"] += 1
                logger.exception(
                    "evaluate_watchlist: failed to spawn process_event for %s",
                    event_id,
                )
        except Exception:
            summary["errors"] += 1
            logger.exception(
                "evaluate_watchlist: failed to process row %s",
                row.get("id"),
            )

    logger.info("evaluate_watchlist: done %s", summary)
    return summary
