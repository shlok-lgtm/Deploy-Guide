"""
Behavioral Wallet Classification
==================================
Tag every wallet by behavioral pattern derived from balance history:
accumulator, distributor, rotator, bridge_user, protocol_depositor,
whale, dormant, freshly_active.

Computed from balance history — no new API calls.

Schedule: Daily
"""

import json
import logging
import math
from datetime import datetime, timezone

from app.database import fetch_all, fetch_one, get_cursor

logger = logging.getLogger(__name__)

# Behavioral classification rules
# Each rule checks metrics computed from wallet data
BEHAVIOR_RULES = [
    {
        "type": "whale",
        "description": "Holds >$1M in stablecoins",
        "check": lambda m: m.get("total_value", 0) >= 1_000_000,
        "confidence_fn": lambda m: min(1.0, m.get("total_value", 0) / 10_000_000),
    },
    {
        "type": "accumulator",
        "description": "Value has increased over last 30 days with few outflows",
        "check": lambda m: (
            m.get("value_change_30d", 0) > 0
            and m.get("outflow_ratio", 1) < 0.3
        ),
        "confidence_fn": lambda m: min(1.0, max(0, m.get("value_change_30d", 0)) / max(m.get("total_value", 1), 1)),
    },
    {
        "type": "distributor",
        "description": "Net outflows over last 30 days",
        "check": lambda m: (
            m.get("value_change_30d", 0) < -0.1 * max(m.get("total_value", 1), 1)
            and m.get("outflow_ratio", 0) > 0.5
        ),
        "confidence_fn": lambda m: min(1.0, m.get("outflow_ratio", 0)),
    },
    {
        "type": "rotator",
        "description": "Frequently changes stablecoin composition",
        "check": lambda m: m.get("composition_changes_30d", 0) >= 3,
        "confidence_fn": lambda m: min(1.0, m.get("composition_changes_30d", 0) / 10),
    },
    {
        "type": "bridge_user",
        "description": "Active on multiple chains",
        "check": lambda m: m.get("chain_count", 0) >= 2,
        "confidence_fn": lambda m: min(1.0, m.get("chain_count", 0) / 3),
    },
    {
        "type": "protocol_depositor",
        "description": "Majority of holdings in DeFi protocol receipt tokens",
        "check": lambda m: m.get("defi_share", 0) > 0.5,
        "confidence_fn": lambda m: m.get("defi_share", 0),
    },
    {
        "type": "dormant",
        "description": "No activity for 30+ days",
        "check": lambda m: m.get("days_since_activity", 0) >= 30,
        "confidence_fn": lambda m: min(1.0, m.get("days_since_activity", 0) / 90),
    },
    {
        "type": "freshly_active",
        "description": "First seen within last 7 days",
        "check": lambda m: m.get("days_since_first_seen", 999) <= 7,
        "confidence_fn": lambda m: max(0, 1.0 - m.get("days_since_first_seen", 7) / 7),
    },
]


def _compute_wallet_metrics(address: str) -> dict:
    """
    Compute behavioral metrics for a wallet from existing data.
    No new API calls — uses wallet_holdings, wallet_risk_scores, wallet_edges.
    """
    metrics = {"address": address}

    # Current holdings value
    holdings = fetch_all(
        """SELECT token_symbol, balance_usd, chain
           FROM wallet_graph.wallet_holdings
           WHERE wallet_address = %s AND balance_usd > 0""",
        (address,),
    )

    if not holdings:
        metrics["total_value"] = 0
        return metrics

    total_value = sum(float(h.get("balance_usd") or 0) for h in holdings)
    metrics["total_value"] = total_value

    # Chain diversity
    chains = set(h.get("chain", "ethereum") for h in holdings if h.get("chain"))
    metrics["chain_count"] = len(chains)

    # DeFi share (receipt tokens like aUSDC, cUSDC, etc.)
    defi_prefixes = {"a", "c", "s", "w", "st"}
    defi_value = 0
    for h in holdings:
        symbol = (h.get("token_symbol") or "").lower()
        if any(symbol.startswith(p) and len(symbol) > len(p) + 2 for p in defi_prefixes):
            defi_value += float(h.get("balance_usd") or 0)
    metrics["defi_share"] = defi_value / total_value if total_value > 0 else 0

    # Risk score data
    risk = fetch_one(
        """SELECT risk_score, concentration_hhi, last_indexed_at, created_at
           FROM wallet_graph.wallet_risk_scores
           WHERE wallet_address = %s""",
        (address,),
    )

    if risk:
        metrics["risk_score"] = float(risk.get("risk_score") or 50)
        metrics["concentration_hhi"] = float(risk.get("concentration_hhi") or 0)

        if risk.get("last_indexed_at"):
            last_indexed = risk["last_indexed_at"]
            if hasattr(last_indexed, 'tzinfo') and last_indexed.tzinfo is None:
                last_indexed = last_indexed.replace(tzinfo=timezone.utc)
            days_since = (datetime.now(timezone.utc) - last_indexed).days
            metrics["days_since_activity"] = days_since
        else:
            metrics["days_since_activity"] = 999

        if risk.get("created_at"):
            created = risk["created_at"]
            if hasattr(created, 'tzinfo') and created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            metrics["days_since_first_seen"] = (datetime.now(timezone.utc) - created).days
        else:
            metrics["days_since_first_seen"] = 999
    else:
        metrics["days_since_activity"] = 999
        metrics["days_since_first_seen"] = 999

    # Edge count (counterparties)
    edge_count = fetch_one(
        """SELECT COUNT(*) as cnt FROM wallet_graph.wallet_edges
           WHERE source_address = %s OR target_address = %s""",
        (address, address),
    )
    metrics["edge_count"] = edge_count["cnt"] if edge_count else 0

    # Inflow/outflow ratio from edges
    inflow = fetch_one(
        """SELECT COALESCE(SUM(transfer_value_usd), 0) as total
           FROM wallet_graph.wallet_edges WHERE target_address = %s""",
        (address,),
    )
    outflow = fetch_one(
        """SELECT COALESCE(SUM(transfer_value_usd), 0) as total
           FROM wallet_graph.wallet_edges WHERE source_address = %s""",
        (address,),
    )

    inflow_val = float(inflow["total"]) if inflow else 0
    outflow_val = float(outflow["total"]) if outflow else 0
    total_flow = inflow_val + outflow_val

    metrics["outflow_ratio"] = outflow_val / total_flow if total_flow > 0 else 0.5
    metrics["value_change_30d"] = inflow_val - outflow_val  # Simplified

    # Composition changes (unique token types held)
    metrics["composition_changes_30d"] = len(set(
        (h.get("token_symbol") or "") for h in holdings
    ))

    return metrics


def _classify_wallet(metrics: dict) -> list[dict]:
    """Apply classification rules to wallet metrics. Returns matching behaviors."""
    tags = []
    for rule in BEHAVIOR_RULES:
        try:
            if rule["check"](metrics):
                confidence = rule["confidence_fn"](metrics)
                tags.append({
                    "behavior_type": rule["type"],
                    "confidence": round(confidence, 3),
                    "metrics": {
                        k: v for k, v in metrics.items()
                        if k != "address" and isinstance(v, (int, float))
                    },
                })
        except Exception:
            pass
    return tags


def _sanitize_float(val):
    """Return None if val is NaN or Infinity, else return val."""
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _store_behavior_tags(address: str, tags: list[dict]):
    """Store wallet behavior tags to database (per-row transactions)."""
    if not tags:
        return

    stored = 0
    errors = 0

    for tag in tags:
        try:
            # Sanitize numeric fields in metrics
            sanitized_metrics = {
                k: _sanitize_float(v) if isinstance(v, float) else v
                for k, v in tag.get("metrics", {}).items()
            }
            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO wallet_behavior_tags
                       (wallet_address, behavior_type, confidence, metrics, computed_at)
                       VALUES (%s, %s, %s, %s, NOW())
                       ON CONFLICT (wallet_address, behavior_type, computed_at)
                       DO UPDATE SET confidence = EXCLUDED.confidence,
                                     metrics = EXCLUDED.metrics""",
                    (
                        address,
                        tag["behavior_type"],
                        _sanitize_float(tag["confidence"]),
                        json.dumps(sanitized_metrics),
                    ),
                )
            stored += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.error(
                    "Failed to store behavior tag %s for %s: %s",
                    tag.get("behavior_type"), address, e,
                )

    if errors:
        logger.error(
            "Behavior tags store complete: %d stored, %d errors for %s",
            stored, errors, address,
        )


def run_behavioral_classification(batch_size: int = 2000) -> dict:
    """
    Classify wallets by behavioral pattern.
    Processes batch_size wallets per cycle, prioritizing by value.

    No new API calls — computed from existing data.
    """
    # Get wallets to classify, prioritized by value
    rows = fetch_all(
        """SELECT w.address
           FROM wallet_graph.wallets w
           LEFT JOIN wallet_graph.wallet_risk_scores r ON w.address = r.wallet_address
           WHERE r.total_stablecoin_value > 10000
           ORDER BY r.total_stablecoin_value DESC NULLS LAST
           LIMIT %s""",
        (batch_size,),
    )

    if not rows:
        return {"wallets_classified": 0}

    classified = 0
    total_tags = 0

    for row in rows:
        address = row["address"]
        try:
            metrics = _compute_wallet_metrics(address)
            tags = _classify_wallet(metrics)

            if tags:
                _store_behavior_tags(address, tags)
                total_tags += len(tags)

            classified += 1
        except Exception as e:
            logger.debug(f"Behavioral classification failed for {address}: {e}")

    logger.info(
        f"Behavioral classification complete: {classified} wallets, "
        f"{total_tags} tags assigned"
    )

    return {
        "wallets_classified": classified,
        "total_tags": total_tags,
    }
