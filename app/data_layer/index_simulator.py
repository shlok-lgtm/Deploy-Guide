"""
Index Creator Sandbox
=====================
Allows third-party index creators to simulate an index definition
against the last 30 days of data, without committing.

Endpoints:
- POST /api/indices/simulate: Run a JSON index definition
- GET /api/indices/coverage-matrix: Component-level data availability

Usage:
    POST /api/indices/simulate
    {
        "name": "My Custom Risk Index",
        "components": [
            {"id": "peg_stability", "weight": 0.30, "source": "peg_snapshots_5m"},
            {"id": "liquidity_depth", "weight": 0.25, "source": "liquidity_depth"},
            {"id": "yield_sustainability", "weight": 0.20, "source": "yield_snapshots"},
            {"id": "governance_quality", "weight": 0.25, "source": "governance_proposals"}
        ],
        "entity_type": "stablecoin"
    }
"""

import json
import logging
from datetime import datetime, timezone

from app.database import fetch_all, fetch_one

logger = logging.getLogger(__name__)

# Maps data source names to their tables and available metrics
DATA_SOURCE_REGISTRY = {
    "peg_snapshots_5m": {
        "table": "peg_snapshots_5m",
        "entity_col": "stablecoin_id",
        "time_col": "timestamp",
        "metrics": ["price", "deviation_bps"],
        "entity_type": "stablecoin",
    },
    "liquidity_depth": {
        "table": "liquidity_depth",
        "entity_col": "asset_id",
        "time_col": "snapshot_at",
        "metrics": ["bid_depth_1pct", "ask_depth_1pct", "spread_bps", "volume_24h"],
        "entity_type": "stablecoin",
    },
    "yield_snapshots": {
        "table": "yield_snapshots",
        "entity_col": "protocol",
        "time_col": "snapshot_at",
        "metrics": ["apy", "apy_base", "tvl_usd", "utilization"],
        "entity_type": "protocol",
    },
    "governance_proposals": {
        "table": "governance_proposals",
        "entity_col": "protocol",
        "time_col": "collected_at",
        "metrics": ["votes_for", "votes_against", "voter_count", "quorum_reached"],
        "entity_type": "protocol",
    },
    "bridge_flows": {
        "table": "bridge_flows",
        "entity_col": "bridge_id",
        "time_col": "snapshot_at",
        "metrics": ["volume_usd", "tvl_usd"],
        "entity_type": "bridge",
    },
    "exchange_snapshots": {
        "table": "exchange_snapshots",
        "entity_col": "exchange_id",
        "time_col": "snapshot_at",
        "metrics": ["trust_score", "trade_volume_24h_usd"],
        "entity_type": "exchange",
    },
    "volatility_surfaces": {
        "table": "volatility_surfaces",
        "entity_col": "asset_id",
        "time_col": "computed_at",
        "metrics": ["realized_vol_1d", "max_drawdown_7d"],
        "entity_type": "stablecoin",
    },
    "correlation_matrices": {
        "table": "correlation_matrices",
        "entity_col": None,
        "time_col": "computed_at",
        "metrics": ["matrix_data"],
        "entity_type": "global",
    },
    "scores": {
        "table": "scores",
        "entity_col": "stablecoin_id",
        "time_col": "computed_at",
        "metrics": ["overall_score", "peg_score", "liquidity_score", "structural_score"],
        "entity_type": "stablecoin",
    },
    "psi_scores": {
        "table": "psi_scores",
        "entity_col": "protocol_slug",
        "time_col": "scored_at",
        "metrics": ["overall_score"],
        "entity_type": "protocol",
    },
    "component_readings": {
        "table": "component_readings",
        "entity_col": "stablecoin_id",
        "time_col": "collected_at",
        "metrics": ["raw_value", "normalized_score"],
        "entity_type": "stablecoin",
    },
}


def simulate_index(definition: dict) -> dict:
    """
    Simulate a custom index definition against the last 30 days of data.

    Args:
        definition: {
            "name": str,
            "components": [{"id": str, "weight": float, "source": str, "metric": str}],
            "entity_type": str  # stablecoin, protocol, bridge, exchange
        }

    Returns:
        Simulated scores for all qualifying entities + data coverage report.
    """
    components = definition.get("components", [])
    entity_type = definition.get("entity_type", "stablecoin")

    if not components:
        return {"error": "No components defined"}

    # Validate weights sum to ~1.0
    total_weight = sum(c.get("weight", 0) for c in components)
    if abs(total_weight - 1.0) > 0.01:
        return {"error": f"Weights sum to {total_weight}, expected 1.0"}

    # Check data availability for each component
    coverage = []
    available_components = []
    for comp in components:
        source = comp.get("source", "")
        source_info = DATA_SOURCE_REGISTRY.get(source)

        if not source_info:
            coverage.append({
                "component": comp["id"],
                "source": source,
                "status": "no_data_source",
                "available": False,
            })
            continue

        # Check if table has data
        try:
            count_row = fetch_one(
                f"SELECT COUNT(*) as cnt FROM {source_info['table']} "
                f"WHERE {source_info['time_col']} >= NOW() - INTERVAL '30 days'"
            )
            row_count = count_row["cnt"] if count_row else 0

            if row_count > 0:
                coverage.append({
                    "component": comp["id"],
                    "source": source,
                    "status": "live",
                    "available": True,
                    "data_points_30d": row_count,
                })
                available_components.append(comp)
            else:
                coverage.append({
                    "component": comp["id"],
                    "source": source,
                    "status": "no_recent_data",
                    "available": False,
                    "data_points_30d": 0,
                })
        except Exception as e:
            coverage.append({
                "component": comp["id"],
                "source": source,
                "status": "error",
                "available": False,
                "error": str(e),
            })

    # Compute simulated scores
    simulated_scores = []
    if available_components:
        # Get entities
        entities = _get_entities(entity_type)

        for entity_id in entities:
            entity_score = 0
            component_scores = {}
            data_coverage = 0

            for comp in available_components:
                source_info = DATA_SOURCE_REGISTRY.get(comp["source"])
                if not source_info or not source_info.get("entity_col"):
                    continue

                metric = comp.get("metric", source_info["metrics"][0])
                try:
                    value = _get_latest_metric(
                        source_info["table"],
                        source_info["entity_col"],
                        entity_id,
                        metric,
                        source_info["time_col"],
                    )
                    if value is not None:
                        # Normalize to 0-100
                        normalized = min(100, max(0, float(value)))
                        weighted = normalized * comp.get("weight", 0)
                        entity_score += weighted
                        component_scores[comp["id"]] = round(normalized, 2)
                        data_coverage += 1
                except Exception as e:
                    logger.warning(f"[index_simulator] component score failed for {comp.get('id')}: {e}")
                    try:
                        from app.worker import _record_cycle_error
                        _record_cycle_error(
                            error_type="data_layer_simulate_index_component_failure",
                            error_message=str(e)[:500],
                            cycle_phase="index_simulator",
                        )
                    except Exception:
                        pass

            if data_coverage > 0:
                simulated_scores.append({
                    "entity_id": entity_id,
                    "simulated_score": round(entity_score, 2),
                    "components": component_scores,
                    "data_coverage_pct": round(
                        data_coverage / len(available_components) * 100, 1
                    ),
                })

    # Sort by score
    simulated_scores.sort(key=lambda x: x["simulated_score"], reverse=True)

    return {
        "name": definition.get("name", "Unnamed Index"),
        "entity_type": entity_type,
        "simulated_scores": simulated_scores[:50],
        "entities_scored": len(simulated_scores),
        "coverage_report": coverage,
        "available_components": len(available_components),
        "total_components": len(components),
        "data_coverage_pct": round(
            len(available_components) / len(components) * 100, 1
        ) if components else 0,
    }


def get_coverage_matrix() -> dict:
    """
    For each existing + proposed index, show component-level data availability.
    """
    indices = []

    # Existing indices
    existing = [
        {
            "name": "SII (Stablecoin Integrity Index)",
            "version": "v1.0.0",
            "components": [
                {"id": "peg_stability", "source": "component_readings"},
                {"id": "liquidity_depth", "source": "liquidity_depth"},
                {"id": "mint_burn_dynamics", "source": "component_readings"},
                {"id": "holder_distribution", "source": "component_readings"},
                {"id": "structural_risk", "source": "component_readings"},
            ],
        },
        {
            "name": "PSI (Protocol Safety Index)",
            "version": "v0.2.0",
            "components": [
                {"id": "collateral_quality", "source": "psi_scores"},
                {"id": "governance", "source": "governance_proposals"},
                {"id": "revenue", "source": "yield_snapshots"},
            ],
        },
    ]

    for idx in existing:
        components_status = []
        for comp in idx["components"]:
            source = comp["source"]
            source_info = DATA_SOURCE_REGISTRY.get(source)

            if not source_info:
                components_status.append({
                    "component": comp["id"],
                    "status": "no_source",
                    "has_live_data": False,
                })
                continue

            try:
                count_row = fetch_one(
                    f"SELECT COUNT(*) as cnt FROM {source_info['table']} "
                    f"WHERE {source_info['time_col']} >= NOW() - INTERVAL '24 hours'"
                )
                has_data = (count_row["cnt"] or 0) > 0 if count_row else False
                components_status.append({
                    "component": comp["id"],
                    "status": "live" if has_data else "stale",
                    "has_live_data": has_data,
                })
            except Exception:
                components_status.append({
                    "component": comp["id"],
                    "status": "error",
                    "has_live_data": False,
                })

        live_count = sum(1 for c in components_status if c["has_live_data"])
        indices.append({
            "name": idx["name"],
            "version": idx.get("version"),
            "components": components_status,
            "live_coverage_pct": round(
                live_count / len(components_status) * 100, 1
            ) if components_status else 0,
        })

    return {"indices": indices}


def _get_entities(entity_type: str) -> list[str]:
    """Get entity IDs for a given type."""
    if entity_type == "stablecoin":
        rows = fetch_all("SELECT id FROM stablecoins WHERE scoring_enabled = TRUE")
        return [r["id"] for r in rows] if rows else []
    elif entity_type == "protocol":
        rows = fetch_all("SELECT DISTINCT protocol_slug FROM psi_scores ORDER BY protocol_slug")
        return [r["protocol_slug"] for r in rows] if rows else []
    elif entity_type == "bridge":
        rows = fetch_all("SELECT DISTINCT bridge_id FROM bridge_flows ORDER BY bridge_id")
        return [r["bridge_id"] for r in rows] if rows else []
    elif entity_type == "exchange":
        rows = fetch_all("SELECT DISTINCT exchange_id FROM exchange_snapshots ORDER BY exchange_id")
        return [r["exchange_id"] for r in rows] if rows else []
    return []


def _get_latest_metric(
    table: str, entity_col: str, entity_id: str, metric: str, time_col: str
) -> float:
    """Get the latest value of a metric for an entity."""
    # Whitelist check to prevent SQL injection
    allowed_tables = set(s["table"] for s in DATA_SOURCE_REGISTRY.values())
    allowed_cols = {"stablecoin_id", "asset_id", "protocol", "bridge_id", "exchange_id", "protocol_slug"}
    allowed_metrics = set()
    for s in DATA_SOURCE_REGISTRY.values():
        allowed_metrics.update(s["metrics"])
    allowed_time_cols = {"timestamp", "snapshot_at", "collected_at", "computed_at", "computed_at", "scored_at"}

    if table not in allowed_tables:
        return None
    if entity_col not in allowed_cols:
        return None
    if metric not in allowed_metrics:
        return None
    if time_col not in allowed_time_cols:
        return None

    row = fetch_one(
        f"SELECT {metric} FROM {table} WHERE {entity_col} = %s ORDER BY {time_col} DESC LIMIT 1",
        (entity_id,),
    )
    if row and row.get(metric) is not None:
        return float(row[metric])
    return None
