"""
Component Replay Engine
========================
When a new component is added to any index, replay historical periods
using the newly available data. Tag replayed scores with the methodology
version.

Extends temporal_engine.py to support arbitrary component backfill.

Schedule: On-demand (triggered when new data types go live)
"""

import json
import logging
from datetime import datetime, timezone, timedelta

from app.database import fetch_all, fetch_one, get_cursor

logger = logging.getLogger(__name__)


def replay_sii_with_component(
    component_id: str,
    data_source_table: str,
    entity_col: str,
    value_col: str,
    time_col: str,
    category: str,
    days_back: int = 30,
) -> dict:
    """
    Replay SII scores for all stablecoins using a newly available component.

    1. For each day in the replay window:
       a. Get historical component readings (existing)
       b. Add the new component from the data source table
       c. Recompute SII score
       d. Store as replayed score with methodology tag

    Args:
        component_id: New component ID
        data_source_table: Table with historical data for the new component
        entity_col: Column that maps to stablecoin_id
        value_col: Column with the component value (0-100 normalized)
        time_col: Timestamp column
        category: SII category this component belongs to
        days_back: How far back to replay

    Returns:
        Summary of replay results.
    """
    from app.scoring import (
        calculate_sii, aggregate_legacy_to_v1, score_to_grade,
    )

    # Whitelist check for table/column names
    allowed_tables = {
        "liquidity_depth", "yield_snapshots", "peg_snapshots_5m",
        "volatility_surfaces", "exchange_snapshots", "governance_proposals",
        "component_readings",
    }
    if data_source_table not in allowed_tables:
        return {"error": f"Table {data_source_table} not in whitelist"}

    stablecoins = fetch_all(
        "SELECT id FROM stablecoins WHERE scoring_enabled = TRUE"
    )
    if not stablecoins:
        return {"error": "no stablecoins to replay"}

    replayed = 0
    errors = 0

    for day_offset in range(days_back):
        target_date = datetime.now(timezone.utc).date() - timedelta(days=day_offset)

        for sc in stablecoins:
            sid = sc["id"]
            try:
                # Get existing components for that day
                existing = fetch_all(
                    """SELECT component_id, category, normalized_score
                       FROM component_readings
                       WHERE stablecoin_id = %s
                         AND immutable_date(collected_at) = %s""",
                    (sid, target_date),
                )

                if not existing:
                    continue

                # Check if new component data exists for that day
                new_data = fetch_one(
                    f"""SELECT {value_col} as value
                        FROM {data_source_table}
                        WHERE {entity_col} = %s
                          AND DATE({time_col}) = %s
                        ORDER BY {time_col} DESC LIMIT 1""",
                    (sid, target_date),
                )

                if not new_data or new_data.get("value") is None:
                    continue

                # Merge: existing components + new component
                components = list(existing)
                components.append({
                    "component_id": component_id,
                    "category": category,
                    "normalized_score": float(new_data["value"]),
                })

                # Recompute SII
                from collections import defaultdict
                category_scores = defaultdict(list)
                for comp in components:
                    cat = comp.get("category", "unknown")
                    score = comp.get("normalized_score")
                    if score is not None:
                        category_scores[cat].append(float(score))

                cat_avgs = {
                    cat: sum(s) / len(s)
                    for cat, s in category_scores.items()
                }
                v1_scores = aggregate_legacy_to_v1(cat_avgs)
                overall = calculate_sii(v1_scores)

                if overall is None:
                    continue

                # Store replayed score
                with get_cursor() as cur:
                    cur.execute(
                        """INSERT INTO score_history
                           (stablecoin, score_date, overall_score, grade,
                            component_count, formula_version, created_at)
                           VALUES (%s, %s, %s, %s, %s, %s, NOW())
                           ON CONFLICT (stablecoin, score_date) DO NOTHING""",
                        (
                            sid, target_date,
                            round(overall, 2), score_to_grade(round(overall, 2)),
                            len(components),
                            f"replay_with_{component_id}",
                        ),
                    )

                replayed += 1

            except Exception as e:
                errors += 1
                logger.debug(
                    f"Replay failed for {sid} on {target_date}: {e}"
                )

    logger.info(
        f"Component replay complete: {replayed} scores replayed, "
        f"{errors} errors, component={component_id}"
    )

    return {
        "component_id": component_id,
        "days_replayed": days_back,
        "scores_replayed": replayed,
        "errors": errors,
    }


def get_replay_candidates() -> list[dict]:
    """
    List data types that have historical data but aren't yet used
    in SII scoring. These are candidates for component replay.
    """
    candidates = []

    checks = [
        {
            "name": "liquidity_depth_score",
            "table": "liquidity_depth",
            "check_query": """SELECT COUNT(*) as cnt FROM liquidity_depth
                              WHERE snapshot_at >= NOW() - INTERVAL '30 days'""",
            "category": "liquidity",
            "description": "Per-venue liquidity depth data available for replay",
        },
        {
            "name": "yield_sustainability",
            "table": "yield_snapshots",
            "check_query": """SELECT COUNT(*) as cnt FROM yield_snapshots
                              WHERE snapshot_at >= NOW() - INTERVAL '30 days'""",
            "category": "structural",
            "description": "Pool yield data available for structural risk replay",
        },
        {
            "name": "governance_velocity",
            "table": "governance_proposals",
            "check_query": """SELECT COUNT(*) as cnt FROM governance_proposals
                              WHERE collected_at >= NOW() - INTERVAL '30 days'""",
            "category": "governance",
            "description": "Governance proposal data available for replay",
        },
        {
            "name": "peg_volatility",
            "table": "volatility_surfaces",
            "check_query": """SELECT COUNT(*) as cnt FROM volatility_surfaces
                              WHERE computed_at >= NOW() - INTERVAL '30 days'""",
            "category": "peg_stability",
            "description": "5-minute volatility data available for peg stability replay",
        },
    ]

    for check in checks:
        try:
            row = fetch_one(check["check_query"])
            data_points = row["cnt"] if row else 0
            if data_points > 0:
                candidates.append({
                    "component": check["name"],
                    "table": check["table"],
                    "category": check["category"],
                    "description": check["description"],
                    "data_points_30d": data_points,
                })
        except Exception as e:
            logger.warning(f"[component_replay] check failed for {check.get('name')}: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="data_layer_get_replay_candidates_check_failure",
                    error_message=str(e)[:500],
                    cycle_phase="component_replay",
                )
            except Exception:
                pass

    return candidates
