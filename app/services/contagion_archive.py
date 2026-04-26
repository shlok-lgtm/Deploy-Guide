"""
Contagion Event Archive (Pipeline 16)
=======================================
Permanently archives every contagion propagation event with full graph state
at detection time.  Integrates into the divergence signal emission pipeline.

Never raises — all errors logged and skipped so the main pipeline is not blocked.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

from app.database import fetch_all, fetch_one, execute

logger = logging.getLogger(__name__)


def _serialize(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)


def _run_contagion_traversal(
    source_entity_type: str,
    source_entity_id: str,
) -> dict:
    """
    Run a depth-2 contagion traversal from a source entity.
    Uses the same CTE pattern as the /api/cqi contagion endpoint.
    Returns {edges_traversed, affected_entities, depth_distribution,
             total_exposure_usd, hhi_concentration, top_10_wallets}.
    """
    MAX_NODES = 500

    # Determine seed wallet addresses based on entity type
    seed_addrs = []
    if source_entity_type == "stablecoin":
        rows = fetch_all(
            """SELECT wallet_address FROM (
                   SELECT DISTINCT ON (wallet_address) wallet_address, value_usd
                   FROM wallet_graph.wallet_holdings
                   WHERE UPPER(symbol) = UPPER(%s)
                   ORDER BY wallet_address, value_usd DESC NULLS LAST
               ) sub ORDER BY value_usd DESC NULLS LAST LIMIT 50""",
            (source_entity_id,),
        )
        seed_addrs = [r["wallet_address"] for r in (rows or [])]
    elif source_entity_type == "protocol":
        rows = fetch_all(
            """SELECT wallet_address FROM protocol_pool_wallets
               WHERE protocol_slug = %s
               ORDER BY balance DESC NULLS LAST LIMIT 50""",
            (source_entity_id,),
        )
        seed_addrs = [r["wallet_address"] for r in (rows or [])]
    elif source_entity_type == "wallet":
        seed_addrs = [source_entity_id]

    if not seed_addrs:
        return {
            "edges_traversed": 0,
            "affected_entities": [],
            "depth_distribution": {},
            "total_exposure_usd": 0,
            "hhi_concentration": 0,
            "top_10_wallets": [],
        }

    # Depth-2 recursive CTE traversal
    try:
        contagion_rows = fetch_all("""
            WITH RECURSIVE contagion_path AS (
                SELECT
                    CASE WHEN e.from_address = ANY(%s) THEN e.to_address ELSE e.from_address END AS node,
                    e.weight,
                    e.total_value_usd,
                    1 AS depth,
                    ARRAY[e.from_address, e.to_address]::varchar[] AS path
                FROM wallet_graph.wallet_edges e
                WHERE (e.from_address = ANY(%s) OR e.to_address = ANY(%s))
                  AND e.weight > 0.05
                  AND NOT (e.from_address = ANY(%s) AND e.to_address = ANY(%s))

                UNION ALL

                SELECT
                    CASE WHEN e.from_address = cp.node THEN e.to_address ELSE e.from_address END,
                    e.weight,
                    e.total_value_usd,
                    cp.depth + 1,
                    cp.path || CASE WHEN e.from_address = cp.node THEN e.to_address ELSE e.from_address END
                FROM wallet_graph.wallet_edges e
                JOIN contagion_path cp ON (e.from_address = cp.node OR e.to_address = cp.node)
                WHERE cp.depth < 2
                  AND NOT (CASE WHEN e.from_address = cp.node THEN e.to_address ELSE e.from_address END) = ANY(cp.path)
                  AND e.weight > 0.05
            )
            SELECT DISTINCT ON (node) node AS address, depth, weight, total_value_usd
            FROM contagion_path
            WHERE NOT node = ANY(%s)
            ORDER BY node, depth ASC, weight DESC
            LIMIT %s
        """, (seed_addrs, seed_addrs, seed_addrs, seed_addrs, seed_addrs,
              seed_addrs, MAX_NODES))
    except Exception as e:
        logger.debug(f"Contagion traversal query failed: {e}")
        contagion_rows = []

    if not contagion_rows:
        return {
            "edges_traversed": 0,
            "affected_entities": [],
            "depth_distribution": {},
            "total_exposure_usd": 0,
            "hhi_concentration": 0,
            "top_10_wallets": [],
        }

    # Compute graph state
    depth_dist = {}
    total_exposure = 0.0
    balances = []
    for r in contagion_rows:
        d = r["depth"]
        depth_dist[d] = depth_dist.get(d, 0) + 1
        val = float(r["total_value_usd"]) if r.get("total_value_usd") else 0
        total_exposure += val
        if val > 0:
            balances.append(val)

    # HHI concentration
    hhi = 0.0
    if balances:
        total = sum(balances)
        if total > 0:
            shares = [b / total for b in balances]
            hhi = round(sum(s * s for s in shares) * 10000, 1)

    # Top 10 by value
    sorted_rows = sorted(contagion_rows, key=lambda r: float(r.get("total_value_usd") or 0), reverse=True)
    top_10 = [
        {"address": r["address"], "depth": r["depth"], "value_usd": float(r.get("total_value_usd") or 0)}
        for r in sorted_rows[:10]
    ]

    # Build affected entities list
    connected_addrs = [r["address"] for r in contagion_rows]
    risk_map = {}
    try:
        if connected_addrs:
            risk_rows = fetch_all("""
                SELECT DISTINCT ON (wallet_address)
                    wallet_address, risk_score, risk_grade
                FROM wallet_graph.wallet_risk_scores
                WHERE wallet_address = ANY(%s)
                ORDER BY wallet_address, computed_at DESC
            """, (connected_addrs,))
            risk_map = {r["wallet_address"]: r for r in (risk_rows or [])}
    except Exception:
        pass

    affected = []
    for r in contagion_rows:
        risk = risk_map.get(r["address"], {})
        affected.append({
            "entity_type": "wallet",
            "entity_id": r["address"],
            "graph_depth": r["depth"],
            "exposure_amount": float(r.get("total_value_usd") or 0),
            "risk_score": float(risk["risk_score"]) if risk.get("risk_score") else None,
            "risk_grade": risk.get("risk_grade"),
        })

    return {
        "edges_traversed": len(contagion_rows),
        "affected_entities": affected,
        "depth_distribution": depth_dist,
        "total_exposure_usd": round(total_exposure, 2),
        "hhi_concentration": hhi,
        "top_10_wallets": top_10,
    }


def archive_contagion_event(
    event_type: str,
    source_entity_type: str,
    source_entity_id: int | None,
    source_entity_symbol: str,
    trigger_metric: str,
    trigger_before: float,
    trigger_after: float,
    severity: str,
):
    """
    Archive a contagion propagation event with full graph state.
    Called after divergence signals are stored.
    Never raises — logs and returns.
    """
    try:
        now = datetime.now(timezone.utc)

        # Run contagion traversal
        entity_key = source_entity_symbol or str(source_entity_id or "")
        traversal = _run_contagion_traversal(source_entity_type, entity_key)

        # Build propagation summary
        depth_dist = traversal.get("depth_distribution", {})
        propagation_summary = {
            "depth_1_count": depth_dist.get(1, 0),
            "depth_2_count": depth_dist.get(2, 0),
            "depth_3_count": depth_dist.get(3, 0),
            "total_affected_wallets": traversal.get("edges_traversed", 0),
            "estimated_exposure_usd": traversal.get("total_exposure_usd", 0),
        }

        # Build graph state snapshot
        graph_state = {
            "edges_traversed": traversal.get("edges_traversed", 0),
            "max_depth": max(depth_dist.keys()) if depth_dist else 0,
            "total_exposure_usd": traversal.get("total_exposure_usd", 0),
            "hhi_concentration": traversal.get("hhi_concentration", 0),
            "top_10_wallets": traversal.get("top_10_wallets", []),
        }

        # Compute content hash
        content_data = (
            f"{event_type}{source_entity_id or ''}{trigger_metric}"
            f"{trigger_after}{now.isoformat()}"
        )
        content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

        # Store event
        execute(
            """INSERT INTO contagion_events
                (event_type, source_entity_type, source_entity_id, source_entity_symbol,
                 trigger_metric, trigger_value_before, trigger_value_after,
                 severity, affected_entities, graph_state_snapshot,
                 propagation_summary, detected_at, content_hash, attested_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            (
                event_type,
                source_entity_type,
                source_entity_id,
                source_entity_symbol,
                trigger_metric,
                trigger_before,
                trigger_after,
                severity,
                json.dumps(traversal.get("affected_entities", []), default=_serialize),
                json.dumps(graph_state, default=_serialize),
                json.dumps(propagation_summary, default=_serialize),
                now,
                content_hash,
            ),
        )

        # Attest
        try:
            from app.state_attestation import attest_state
            attest_state("contagion_events", [{
                "event_type": event_type,
                "source_entity_id": source_entity_id,
                "trigger_metric": trigger_metric,
                "trigger_after": trigger_after,
                "detected_at": now.isoformat(),
            }], str(source_entity_id))
        except Exception as ae:
            logger.debug(f"Contagion event attestation failed: {ae}")

        depth2_wallets = propagation_summary.get("total_affected_wallets", 0)
        exposure = propagation_summary.get("estimated_exposure_usd", 0)
        logger.info(
            f"CONTAGION EVENT ARCHIVED: {event_type} {source_entity_symbol} "
            f"depth2_wallets={depth2_wallets} exposure=${exposure}"
        )

    except Exception as e:
        logger.warning(f"Contagion event archive failed: {e}")


def archive_divergence_signals(signals: list):
    """
    Process a batch of divergence signals and archive contagion events
    for any that are alert or critical severity.
    Called from detect_all_divergences() after signals are stored.
    """
    ARCHIVABLE_SEVERITIES = {"alert", "critical"}

    EVENT_TYPE_MAP = {
        "asset_quality": "score_drop",
        "wallet_concentration": "wallet_concentration",
        "quality_flow": "quality_flow_divergence",
        "protocol_solvency": "score_drop",
        "cross_index": "quality_flow_divergence",
        "actor_flow_divergence": "quality_flow_divergence",
    }

    ENTITY_TYPE_MAP = {
        "asset_quality": "stablecoin",
        "wallet_concentration": "wallet",
        "quality_flow": "stablecoin",
        "protocol_solvency": "protocol",
        "cross_index": "stablecoin",
        "actor_flow_divergence": "stablecoin",
    }

    for signal in signals:
        try:
            severity = signal.get("severity", "silent")
            if severity not in ARCHIVABLE_SEVERITIES:
                continue

            sig_type = signal.get("type", "unknown")
            event_type = EVENT_TYPE_MAP.get(sig_type, sig_type)
            entity_type = ENTITY_TYPE_MAP.get(sig_type, "unknown")
            entity_symbol = (
                signal.get("symbol")
                or signal.get("protocol")
                or signal.get("stablecoin")
                or signal.get("wallet_address")
                or ""
            )

            trigger_metric = sig_type
            trigger_before = float(signal.get("score_before", 0) or signal.get("hhi_before", 0) or 0)
            trigger_after = float(
                signal.get("score_after", 0)
                or signal.get("magnitude", 0)
                or signal.get("hhi_change", 0)
                or 0
            )

            archive_contagion_event(
                event_type=event_type,
                source_entity_type=entity_type,
                source_entity_id=None,
                source_entity_symbol=entity_symbol,
                trigger_metric=trigger_metric,
                trigger_before=trigger_before,
                trigger_after=trigger_after,
                severity=severity,
            )
        except Exception as e:
            logger.debug(f"Failed to archive contagion for signal: {e}")
