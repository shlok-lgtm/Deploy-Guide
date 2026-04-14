"""
Rated Network Validator Performance Collector (Pipeline 17)
=============================================================
Captures Ethereum validator performance data from the Rated Network API
(free, no key required) and stores as permanent attested state for LSTI scoring.

Runs daily in the slow cycle.  Never raises — all errors logged and skipped.
"""

import hashlib
import logging
import time
from datetime import date, datetime, timezone

import httpx

from app.database import fetch_one, execute

logger = logging.getLogger(__name__)

RATED_BASE_URL = "https://api.rated.network/v0"

# LSTI entity → Rated operator name mapping
LSTI_TO_RATED = {
    "lido": ["Lido", "Lido Finance"],
    "rocket-pool": ["Rocket Pool"],
    "frax-ether": ["Frax Finance"],
    "stakewise": ["StakeWise"],
    "stader": ["Stader Labs"],
    "coinbase-wrapped-staked-eth": ["Coinbase"],
    "mantle-staked-ether": ["Mantle"],
}


def _fetch_operators() -> list[dict]:
    """Fetch operator list from Rated Network API."""
    try:
        resp = httpx.get(
            f"{RATED_BASE_URL}/eth/operators",
            params={"size": 200},
            headers={"Accept": "application/json"},
            timeout=30,
        )
        if resp.status_code == 429:
            logger.warning("Rated API rate limited, backing off 60s")
            time.sleep(60)
            resp = httpx.get(
                f"{RATED_BASE_URL}/eth/operators",
                params={"size": 200},
                headers={"Accept": "application/json"},
                timeout=30,
            )
        if resp.status_code != 200:
            logger.warning(f"Rated operators endpoint returned {resp.status_code}")
            return []
        data = resp.json()
        # Rated returns {"data": [...], ...} or a list directly
        if isinstance(data, dict):
            return data.get("data", data.get("results", []))
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning(f"Failed to fetch Rated operators: {e}")
        return []


def _fetch_operator_effectiveness(operator_id: str) -> dict | None:
    """Fetch effectiveness data for a specific operator."""
    try:
        resp = httpx.get(
            f"{RATED_BASE_URL}/eth/operators/{operator_id}/effectiveness",
            headers={"Accept": "application/json"},
            timeout=20,
        )
        if resp.status_code == 429:
            logger.warning(f"Rated API rate limited for operator {operator_id}, backing off 60s")
            time.sleep(60)
            resp = httpx.get(
                f"{RATED_BASE_URL}/eth/operators/{operator_id}/effectiveness",
                headers={"Accept": "application/json"},
                timeout=20,
            )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if isinstance(data, dict):
            return data.get("data", data) if "data" in data else data
        return data[0] if isinstance(data, list) and data else None
    except Exception as e:
        logger.debug(f"Failed to fetch effectiveness for {operator_id}: {e}")
        return None


def collect_validator_performance() -> dict:
    """
    Main collector: fetch operator data from Rated, match to LSTI entities,
    store daily snapshots.

    Returns summary: {operators_checked, snapshots_stored, skipped_existing}.
    """
    today = date.today()

    # DB gate: check if we already ran today
    existing = fetch_one(
        "SELECT COUNT(*) AS cnt FROM validator_performance_snapshots WHERE snapshot_date = %s",
        (today,),
    )
    if existing and existing.get("cnt", 0) > 0:
        logger.info(f"Validator performance: already captured {existing['cnt']} snapshots today, skipping")
        return {"operators_checked": 0, "snapshots_stored": 0, "skipped_existing": existing["cnt"]}

    operators = _fetch_operators()
    if not operators:
        logger.info("Validator performance: no operators returned from Rated")
        return {"operators_checked": 0, "snapshots_stored": 0, "skipped_existing": 0}

    # Build reverse map: operator name → LSTI entity slug
    name_to_lsti = {}
    for slug, names in LSTI_TO_RATED.items():
        for name in names:
            name_to_lsti[name.lower()] = slug

    operators_checked = 0
    snapshots_stored = 0

    for operator in operators:
        try:
            op_name = operator.get("displayName") or operator.get("id") or ""
            op_id = str(operator.get("id", op_name))

            # Match to LSTI entity
            lsti_slug = None
            for known_name, slug in name_to_lsti.items():
                if known_name in op_name.lower():
                    lsti_slug = slug
                    break

            # Only store operators that map to our LSTI entities
            if not lsti_slug:
                continue

            operators_checked += 1

            # Fetch detailed effectiveness
            effectiveness = _fetch_operator_effectiveness(op_id)
            time.sleep(1)  # Rate limit: 1 req/s

            eff_score = None
            att_eff = None
            proposal_luck = None

            if effectiveness:
                if isinstance(effectiveness, list):
                    effectiveness = effectiveness[0] if effectiveness else {}
                eff_score = effectiveness.get("avgValidatorEffectiveness") or effectiveness.get("effectiveness")
                att_eff = effectiveness.get("avgAttestationEfficiency") or effectiveness.get("attestationEffectiveness")
                proposal_luck = effectiveness.get("proposalLuck")

            validators_count = operator.get("validatorCount") or operator.get("activeValidators")
            network_penetration = operator.get("networkPenetration") or operator.get("networkShare")
            entity_type = operator.get("nodeOperatorType") or operator.get("type") or "unknown"

            # Compute content hash
            content_data = f"{op_id}{today.isoformat()}{eff_score or 0}"
            content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

            # Store snapshot
            execute(
                """INSERT INTO validator_performance_snapshots
                    (snapshot_date, operator_name, operator_id, entity_type,
                     validators_count, effectiveness_score, attestation_effectiveness,
                     proposal_luck, network_penetration, lsti_entity_slug,
                     content_hash, attested_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                   ON CONFLICT (snapshot_date, operator_id) DO NOTHING""",
                (
                    today, op_name, op_id, entity_type,
                    validators_count, eff_score, att_eff,
                    proposal_luck, network_penetration, lsti_slug,
                    content_hash,
                ),
            )
            snapshots_stored += 1

            # Check for effectiveness drop vs previous day
            if eff_score is not None:
                prev = fetch_one(
                    """SELECT effectiveness_score FROM validator_performance_snapshots
                       WHERE operator_id = %s AND snapshot_date < %s
                       ORDER BY snapshot_date DESC LIMIT 1""",
                    (op_id, today),
                )
                if prev and prev.get("effectiveness_score"):
                    prev_eff = float(prev["effectiveness_score"])
                    current_eff = float(eff_score)
                    if prev_eff > 0 and (prev_eff - current_eff) / prev_eff > 0.05:
                        logger.warning(
                            f"VALIDATOR EFFECTIVENESS DROP: {op_name} ({lsti_slug}) "
                            f"dropped from {prev_eff:.4f} to {current_eff:.4f}"
                        )

        except Exception as e:
            logger.debug(f"Failed to process operator {operator.get('id')}: {e}")

    # Attest batch
    if snapshots_stored > 0:
        try:
            from app.state_attestation import attest_state
            attest_state("validator_performance", [{
                "snapshot_date": today.isoformat(),
                "operators_stored": snapshots_stored,
            }])
        except Exception as ae:
            logger.debug(f"Validator performance attestation failed: {ae}")

    summary = {
        "operators_checked": operators_checked,
        "snapshots_stored": snapshots_stored,
        "skipped_existing": 0,
    }
    logger.info(
        f"Validator performance: checked={operators_checked} stored={snapshots_stored}"
    )
    return summary
