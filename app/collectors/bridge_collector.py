"""
Bridge Integrity Index Collector
==================================
Collects data for cross-chain bridge risk scoring.

Data sources:
- DeFiLlama /bridges endpoint: bridge TVL, volume, chain support
- Static config: security architecture, operational history, audit data
- CoinGecko: bridge token market data (where applicable)

Pattern follows psi_collector.py — collect → score_entity → store → attest.
"""

import json
import hashlib
import logging
import os
import time
from datetime import datetime, timezone

import requests

from app.database import execute, fetch_all, fetch_one
from app.index_definitions.bri_v01 import BRI_V01_DEFINITION, BRIDGE_ENTITIES
from app.scoring_engine import score_entity

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://api.llama.fi"

# =============================================================================
# Static config — security architecture, operational history, etc.
# =============================================================================

BRIDGE_STATIC_CONFIG = {
    "wormhole": {
        "verification_mechanism": 60,  # Multisig guardian set (19 guardians)
        "guardian_count": 19, "guardian_diversity": 0.85,
        "bridge_upgrade_mechanism": 65, "bridge_timelock": 24,
        "bridge_audit_count": 5,
        "uptime_pct": 99.5, "message_success_rate": 99.8,
        "incident_history": 20,  # $320M exploit Feb 2022
        "time_since_incident_days": 1500,
        "token_coverage": 50,
        "bridge_formal_verification": 40, "bug_bounty_size": 2500000,
        "contract_age_days": 1200, "bridge_dependency_risk": 60, "code_complexity": 50,
        "operator_geographic_diversity": 70, "validator_rotation": 50,
        "bridge_governance_mechanism": 55, "token_holder_concentration": 40,
        "cost_to_attack": 1000000000, "slashing_mechanism": 40,
        "bridge_insurance": 30, "restaking_security": 20,
    },
    "layerzero": {
        "verification_mechanism": 70,  # Ultra Light Nodes + DVNs
        "guardian_count": 10, "guardian_diversity": 0.75,
        "bridge_upgrade_mechanism": 60, "bridge_timelock": 12,
        "bridge_audit_count": 6,
        "uptime_pct": 99.8, "message_success_rate": 99.9,
        "incident_history": 90,  # No major exploits
        "time_since_incident_days": 1000,
        "token_coverage": 80,
        "bridge_formal_verification": 50, "bug_bounty_size": 15000000,
        "contract_age_days": 900, "bridge_dependency_risk": 55, "code_complexity": 60,
        "operator_geographic_diversity": 65, "validator_rotation": 60,
        "bridge_governance_mechanism": 50, "token_holder_concentration": 45,
        "cost_to_attack": 500000000, "slashing_mechanism": 50,
        "bridge_insurance": 30, "restaking_security": 30,
    },
    "axelar": {
        "verification_mechanism": 75,  # PoS validator set (75 validators)
        "guardian_count": 75, "guardian_diversity": 0.90,
        "bridge_upgrade_mechanism": 70, "bridge_timelock": 48,
        "bridge_audit_count": 5,
        "uptime_pct": 99.6, "message_success_rate": 99.7,
        "incident_history": 85,
        "time_since_incident_days": 800,
        "token_coverage": 40,
        "bridge_formal_verification": 45, "bug_bounty_size": 1000000,
        "contract_age_days": 800, "bridge_dependency_risk": 60, "code_complexity": 55,
        "operator_geographic_diversity": 75, "validator_rotation": 70,
        "bridge_governance_mechanism": 65, "token_holder_concentration": 35,
        "cost_to_attack": 300000000, "slashing_mechanism": 70,
        "bridge_insurance": 25, "restaking_security": 20,
    },
    "circle-cctp": {
        "verification_mechanism": 85,  # Native mint/burn by Circle
        "guardian_count": 1, "guardian_diversity": 0.0,  # Centralized but trusted
        "bridge_upgrade_mechanism": 75, "bridge_timelock": 0,
        "bridge_audit_count": 4,
        "uptime_pct": 99.9, "message_success_rate": 99.99,
        "incident_history": 100,  # No exploits
        "time_since_incident_days": 1000,
        "token_coverage": 1,  # USDC only
        "bridge_formal_verification": 60, "bug_bounty_size": 250000,
        "contract_age_days": 700, "bridge_dependency_risk": 80, "code_complexity": 70,
        "operator_geographic_diversity": 30, "validator_rotation": 10,
        "bridge_governance_mechanism": 30, "token_holder_concentration": 90,
        "cost_to_attack": 5000000000, "slashing_mechanism": 10,
        "bridge_insurance": 60, "restaking_security": 10,
    },
    "across": {
        "verification_mechanism": 70,  # Optimistic with UMA oracle
        "guardian_count": 5, "guardian_diversity": 0.70,
        "bridge_upgrade_mechanism": 60, "bridge_timelock": 24,
        "bridge_audit_count": 4,
        "uptime_pct": 99.7, "message_success_rate": 99.8,
        "incident_history": 90,
        "time_since_incident_days": 900,
        "token_coverage": 20,
        "bridge_formal_verification": 40, "bug_bounty_size": 1000000,
        "contract_age_days": 800, "bridge_dependency_risk": 50, "code_complexity": 55,
        "operator_geographic_diversity": 55, "validator_rotation": 40,
        "bridge_governance_mechanism": 55, "token_holder_concentration": 50,
        "cost_to_attack": 200000000, "slashing_mechanism": 50,
        "bridge_insurance": 20, "restaking_security": 30,
    },
    "stargate": {
        "verification_mechanism": 65,  # LayerZero-based
        "guardian_count": 10, "guardian_diversity": 0.70,
        "bridge_upgrade_mechanism": 55, "bridge_timelock": 12,
        "bridge_audit_count": 4,
        "uptime_pct": 99.5, "message_success_rate": 99.7,
        "incident_history": 85,
        "time_since_incident_days": 700,
        "token_coverage": 30,
        "bridge_formal_verification": 40, "bug_bounty_size": 500000,
        "contract_age_days": 900, "bridge_dependency_risk": 45, "code_complexity": 50,
        "operator_geographic_diversity": 55, "validator_rotation": 50,
        "bridge_governance_mechanism": 50, "token_holder_concentration": 45,
        "cost_to_attack": 300000000, "slashing_mechanism": 40,
        "bridge_insurance": 20, "restaking_security": 20,
    },
    "synapse": {
        "verification_mechanism": 60,
        "guardian_count": 5, "guardian_diversity": 0.65,
        "bridge_upgrade_mechanism": 50, "bridge_timelock": 6,
        "bridge_audit_count": 3,
        "uptime_pct": 99.0, "message_success_rate": 99.5,
        "incident_history": 70,
        "time_since_incident_days": 600,
        "token_coverage": 25,
        "bridge_formal_verification": 30, "bug_bounty_size": 250000,
        "contract_age_days": 1000, "bridge_dependency_risk": 45, "code_complexity": 50,
        "operator_geographic_diversity": 45, "validator_rotation": 40,
        "bridge_governance_mechanism": 45, "token_holder_concentration": 55,
        "cost_to_attack": 100000000, "slashing_mechanism": 30,
        "bridge_insurance": 15, "restaking_security": 20,
    },
    "debridge": {
        "verification_mechanism": 65,
        "guardian_count": 12, "guardian_diversity": 0.75,
        "bridge_upgrade_mechanism": 60, "bridge_timelock": 24,
        "bridge_audit_count": 4,
        "uptime_pct": 99.6, "message_success_rate": 99.8,
        "incident_history": 90,
        "time_since_incident_days": 700,
        "token_coverage": 30,
        "bridge_formal_verification": 40, "bug_bounty_size": 500000,
        "contract_age_days": 700, "bridge_dependency_risk": 55, "code_complexity": 50,
        "operator_geographic_diversity": 60, "validator_rotation": 50,
        "bridge_governance_mechanism": 50, "token_holder_concentration": 50,
        "cost_to_attack": 150000000, "slashing_mechanism": 45,
        "bridge_insurance": 20, "restaking_security": 25,
    },
    "celer-cbridge": {
        "verification_mechanism": 60,
        "guardian_count": 21, "guardian_diversity": 0.80,
        "bridge_upgrade_mechanism": 55, "bridge_timelock": 12,
        "bridge_audit_count": 3,
        "uptime_pct": 99.2, "message_success_rate": 99.5,
        "incident_history": 50,  # Had a DNS attack (~$240K)
        "time_since_incident_days": 900,
        "token_coverage": 35,
        "bridge_formal_verification": 30, "bug_bounty_size": 200000,
        "contract_age_days": 1000, "bridge_dependency_risk": 50, "code_complexity": 50,
        "operator_geographic_diversity": 60, "validator_rotation": 55,
        "bridge_governance_mechanism": 50, "token_holder_concentration": 50,
        "cost_to_attack": 100000000, "slashing_mechanism": 50,
        "bridge_insurance": 15, "restaking_security": 20,
    },
}


# =============================================================================
# DeFiLlama bridge data
# =============================================================================

def fetch_bridge_data() -> list[dict]:
    """Fetch all bridge data from DeFiLlama /bridges endpoint."""
    try:
        resp = requests.get(f"https://bridges.llama.fi/bridges?includeChains=true", timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("bridges", [])
    except Exception as e:
        logger.warning(f"DeFiLlama bridges fetch failed: {e}")
    return []


def fetch_bridge_volume(bridge_id: int) -> dict | None:
    """Fetch volume data for a specific bridge."""
    try:
        resp = requests.get(
            f"https://bridges.llama.fi/bridge/{bridge_id}",
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.debug(f"Bridge volume fetch failed for ID {bridge_id}: {e}")
    return None


def extract_bridge_raw_values(entity: dict, bridges_data: list[dict]) -> dict:
    """Extract raw component values from DeFiLlama bridge data + static config."""
    slug = entity["slug"]
    defillama_id = entity.get("defillama_id", "")
    raw = {}

    # Match DeFiLlama bridge by name
    matched = None
    for b in bridges_data:
        b_name = (b.get("displayName") or b.get("name") or "").lower()
        if defillama_id.lower() in b_name or slug.replace("-", " ") in b_name:
            matched = b
            break

    if matched:
        # TVL
        current_tvl = matched.get("currentDailyVolume")
        last_hourly = matched.get("lastHourlyVolume")
        chains = matched.get("chains", [])

        # Use the bridge's total TVL from lastDailyVolume
        daily_vol = matched.get("lastDailyVolume")
        if daily_vol:
            raw["daily_volume"] = daily_vol

        # Supported chains
        if chains:
            raw["supported_chains"] = len(chains)

        # Fetch detailed volume data
        bridge_id = matched.get("id")
        if bridge_id:
            vol_data = fetch_bridge_volume(bridge_id)
            if vol_data:
                # Total historical volume
                chain_data = vol_data.get("chainBreakdown", {})
                total_vol = 0
                for chain, data in chain_data.items():
                    if isinstance(data, dict):
                        deposits = data.get("deposit", {})
                        withdrawals = data.get("withdrawal", {})
                        for entry in (deposits.get("txs", []) + withdrawals.get("txs", [])):
                            if isinstance(entry, dict):
                                total_vol += entry.get("usdValue", 0)
                if total_vol > 0:
                    raw["total_value_transferred"] = total_vol

            time.sleep(0.5)

    # Static config components
    static = BRIDGE_STATIC_CONFIG.get(slug, {})
    raw.update(static)

    # Calculated: volume/TVL ratio
    if raw.get("daily_volume") and raw.get("bridge_tvl") and raw["bridge_tvl"] > 0:
        raw["volume_tvl_ratio"] = raw["daily_volume"] / raw["bridge_tvl"]

    return raw


# =============================================================================
# Score and store
# =============================================================================

def score_bridge(entity: dict, bridges_data: list[dict]) -> dict | None:
    """Score a single bridge entity."""
    slug = entity["slug"]
    logger.info(f"Scoring bridge: {slug}")

    raw_values = extract_bridge_raw_values(entity, bridges_data)
    if not raw_values:
        logger.warning(f"No data collected for bridge {slug}")
        return None

    result = score_entity(BRI_V01_DEFINITION, raw_values)
    result["entity_slug"] = slug
    result["entity_name"] = entity["name"]
    result["raw_values"] = raw_values

    return result


def store_bridge_score(result: dict) -> None:
    """Store a bridge score in the generic_index_scores table."""
    slug = result["entity_slug"]
    raw_for_storage = {k: v for k, v in result["raw_values"].items() if not k.startswith("_")}
    raw_canonical = json.dumps(raw_for_storage, sort_keys=True, default=str)
    inputs_hash = "0x" + hashlib.sha256(raw_canonical.encode()).hexdigest()

    execute("""
        INSERT INTO generic_index_scores
            (index_id, entity_slug, entity_name, overall_score,
             category_scores, component_scores, raw_values,
             formula_version, inputs_hash, confidence, confidence_tag)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (index_id, entity_slug, scored_date)
        DO UPDATE SET
            entity_name = EXCLUDED.entity_name,
            overall_score = EXCLUDED.overall_score,
            category_scores = EXCLUDED.category_scores,
            component_scores = EXCLUDED.component_scores,
            raw_values = EXCLUDED.raw_values,
            inputs_hash = EXCLUDED.inputs_hash,
            confidence = EXCLUDED.confidence,
            confidence_tag = EXCLUDED.confidence_tag,
            computed_at = NOW()
    """, (
        "bri", slug, result["entity_name"], result["overall_score"],
        json.dumps(result["category_scores"]),
        json.dumps(result["component_scores"]),
        json.dumps(raw_for_storage, default=str),
        result["version"], inputs_hash,
        result.get("confidence", "limited"),
        result.get("confidence_tag"),
    ))


def run_bri_scoring() -> list[dict]:
    """Score all bridge entities. Called from worker."""
    bridges_data = fetch_bridge_data()
    time.sleep(1)

    results = []
    for entity in BRIDGE_ENTITIES:
        try:
            result = score_bridge(entity, bridges_data)
            if result:
                store_bridge_score(result)
                results.append(result)
                logger.info(
                    f"  {result['entity_name']}: {result['overall_score']} "
                    f"({result['components_available']}/{result['components_total']} components)"
                )
        except Exception as e:
            logger.warning(f"BRI scoring failed for {entity['slug']}: {e}")

    # Attest BRI scores
    try:
        from app.state_attestation import attest_state
        if results:
            attest_state("bri_components", [
                {"slug": r["entity_slug"], "score": r["overall_score"]}
                for r in results
            ])
    except Exception:
        pass

    return results
