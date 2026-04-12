"""
Vault/Yield Strategy Risk Index Collector
===========================================
Collects data for vault and yield strategy risk scoring.

Data sources:
- DeFiLlama /yields/pools: APY, TVL, pool metadata (already integrated)
- DeFiLlama /yields/chart/{pool}: historical APY
- Existing CQI scores for underlying asset quality
- Static config: audit status, strategy transparency, operational risk

Key insight: "Underlying Asset Quality" category reads from existing
SII/PSI scores via CQI lookup — not re-derived.
"""

import json
import hashlib
import logging
import time
from datetime import datetime, timezone

import requests

from app.database import execute, fetch_all, fetch_one
from app.index_definitions.vsri_v01 import VSRI_V01_DEFINITION, VAULT_ENTITIES
from app.scoring_engine import score_entity

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://api.llama.fi"

# =============================================================================
# Static config
# =============================================================================

VAULT_STATIC_CONFIG = {
    "yearn-usdc": {
        "strategy_description_avail": 80, "strategy_code_public": 90,
        "parameter_visibility": 75, "rebalance_logic_documented": 70,
        "risk_disclosure": 65, "il_exposure": 90,
        "withdrawal_delay": 90, "position_liquidity": 80,
        "vault_audit_status": 5, "vault_contract_age_days": 1200,
        "vault_upgrade_mechanism": 65, "dependency_chain_depth": 2,
        "composability_risk": 60, "collateral_diversity": 50,
        "correlation_risk": 70, "curator_track_record": 80,
        "rebalance_frequency": 70, "strategy_change_history": 3,
        "vault_incident_history": 75, "fee_transparency": 80,
    },
    "yearn-dai": {
        "strategy_description_avail": 80, "strategy_code_public": 90,
        "parameter_visibility": 75, "rebalance_logic_documented": 70,
        "risk_disclosure": 65, "il_exposure": 90,
        "withdrawal_delay": 90, "position_liquidity": 80,
        "vault_audit_status": 5, "vault_contract_age_days": 1200,
        "vault_upgrade_mechanism": 65, "dependency_chain_depth": 2,
        "composability_risk": 60, "collateral_diversity": 50,
        "correlation_risk": 70, "curator_track_record": 80,
        "rebalance_frequency": 70, "strategy_change_history": 3,
        "vault_incident_history": 75, "fee_transparency": 80,
    },
    "yearn-eth": {
        "strategy_description_avail": 80, "strategy_code_public": 90,
        "parameter_visibility": 75, "rebalance_logic_documented": 70,
        "risk_disclosure": 65, "il_exposure": 85,
        "withdrawal_delay": 85, "position_liquidity": 75,
        "vault_audit_status": 5, "vault_contract_age_days": 1000,
        "vault_upgrade_mechanism": 65, "dependency_chain_depth": 2,
        "composability_risk": 55, "collateral_diversity": 45,
        "correlation_risk": 65, "curator_track_record": 80,
        "rebalance_frequency": 70, "strategy_change_history": 4,
        "vault_incident_history": 75, "fee_transparency": 80,
    },
    "morpho-usdc-aave": {
        "strategy_description_avail": 75, "strategy_code_public": 85,
        "parameter_visibility": 70, "rebalance_logic_documented": 65,
        "risk_disclosure": 60, "il_exposure": 95,
        "withdrawal_delay": 85, "position_liquidity": 80,
        "vault_audit_status": 4, "vault_contract_age_days": 500,
        "vault_upgrade_mechanism": 60, "dependency_chain_depth": 3,
        "composability_risk": 50, "collateral_diversity": 40,
        "correlation_risk": 75, "curator_track_record": 70,
        "rebalance_frequency": 65, "strategy_change_history": 1,
        "vault_incident_history": 90, "fee_transparency": 75,
    },
    "morpho-eth-aave": {
        "strategy_description_avail": 75, "strategy_code_public": 85,
        "parameter_visibility": 70, "rebalance_logic_documented": 65,
        "risk_disclosure": 60, "il_exposure": 90,
        "withdrawal_delay": 85, "position_liquidity": 75,
        "vault_audit_status": 4, "vault_contract_age_days": 500,
        "vault_upgrade_mechanism": 60, "dependency_chain_depth": 3,
        "composability_risk": 50, "collateral_diversity": 40,
        "correlation_risk": 70, "curator_track_record": 70,
        "rebalance_frequency": 65, "strategy_change_history": 1,
        "vault_incident_history": 90, "fee_transparency": 75,
    },
    "beefy-usdc-eth": {
        "strategy_description_avail": 70, "strategy_code_public": 80,
        "parameter_visibility": 65, "rebalance_logic_documented": 60,
        "risk_disclosure": 55, "il_exposure": 50,
        "withdrawal_delay": 80, "position_liquidity": 65,
        "vault_audit_status": 3, "vault_contract_age_days": 800,
        "vault_upgrade_mechanism": 55, "dependency_chain_depth": 3,
        "composability_risk": 45, "collateral_diversity": 50,
        "correlation_risk": 55, "curator_track_record": 65,
        "rebalance_frequency": 75, "strategy_change_history": 5,
        "vault_incident_history": 70, "fee_transparency": 70,
    },
    "beefy-usdt-usdc": {
        "strategy_description_avail": 70, "strategy_code_public": 80,
        "parameter_visibility": 65, "rebalance_logic_documented": 60,
        "risk_disclosure": 55, "il_exposure": 95,
        "withdrawal_delay": 80, "position_liquidity": 75,
        "vault_audit_status": 3, "vault_contract_age_days": 800,
        "vault_upgrade_mechanism": 55, "dependency_chain_depth": 3,
        "composability_risk": 45, "collateral_diversity": 40,
        "correlation_risk": 80, "curator_track_record": 65,
        "rebalance_frequency": 75, "strategy_change_history": 3,
        "vault_incident_history": 70, "fee_transparency": 70,
    },
}


# =============================================================================
# DeFiLlama yield pool data
# =============================================================================

def fetch_yield_pools() -> list[dict]:
    """Fetch all yield pools from DeFiLlama."""
    try:
        resp = requests.get(f"https://yields.llama.fi/pools", timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("data", [])
    except Exception as e:
        logger.warning(f"DeFiLlama yields fetch failed: {e}")
    return []


def match_vault_pools(entity: dict, all_pools: list[dict]) -> list[dict]:
    """Match a vault entity to DeFiLlama yield pools."""
    protocol = entity.get("protocol", "").lower()
    slug = entity["slug"]

    matched = []
    for pool in all_pools:
        pool_project = (pool.get("project") or "").lower()
        pool_symbol = (pool.get("symbol") or "").lower()

        if protocol in pool_project:
            # Match by token symbol hints in the slug
            if "usdc" in slug and "usdc" in pool_symbol:
                matched.append(pool)
            elif "dai" in slug and "dai" in pool_symbol:
                matched.append(pool)
            elif "eth" in slug and "eth" in pool_symbol and "steth" not in slug:
                matched.append(pool)
            elif "usdt" in slug and "usdt" in pool_symbol:
                matched.append(pool)
            elif "steth" in slug and "steth" in pool_symbol:
                matched.append(pool)
            elif "eeth" in slug and "eeth" in pool_symbol:
                matched.append(pool)

    # Sort by TVL and take top match
    matched.sort(key=lambda p: p.get("tvlUsd", 0), reverse=True)
    return matched[:3]


def extract_vault_raw_values(entity: dict, all_pools: list[dict]) -> dict:
    """Extract raw component values from DeFiLlama yield data + static config."""
    raw = {}

    # Match pools
    matched_pools = match_vault_pools(entity, all_pools)
    if matched_pools:
        best = matched_pools[0]
        raw["vault_tvl"] = best.get("tvlUsd", 0)
        raw["apy_7d"] = best.get("apyBase7d") or best.get("apy", 0)
        raw["apy_30d"] = best.get("apyBase30d") or best.get("apyMean30d") or best.get("apy", 0)

        # APY volatility from standard deviation
        apy_std = best.get("apyBaseInception") or 0
        if apy_std and isinstance(apy_std, (int, float)):
            raw["apy_volatility"] = abs(apy_std)

    # CQI lookup for underlying asset quality
    try:
        # Look up SII score for the underlying stablecoin
        if "usdc" in entity["slug"]:
            sii_row = fetch_one("SELECT overall_score FROM scores WHERE stablecoin_id = 'usdc'")
            if sii_row:
                raw["underlying_sii_score"] = float(sii_row["overall_score"])
        elif "dai" in entity["slug"]:
            sii_row = fetch_one("SELECT overall_score FROM scores WHERE stablecoin_id = 'dai'")
            if sii_row:
                raw["underlying_sii_score"] = float(sii_row["overall_score"])

        # Look up PSI score for the underlying protocol
        protocol_slug = entity.get("protocol")
        if protocol_slug:
            psi_row = fetch_one(
                "SELECT overall_score FROM psi_scores WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1",
                (protocol_slug,),
            )
            if psi_row:
                raw["underlying_psi_score"] = float(psi_row["overall_score"])
    except Exception as e:
        logger.debug(f"CQI lookup failed for {entity['slug']}: {e}")

    # Static config
    static = VAULT_STATIC_CONFIG.get(entity["slug"], {})
    raw.update(static)

    return raw


# =============================================================================
# Score and store
# =============================================================================

def score_vault(entity: dict, all_pools: list[dict]) -> dict | None:
    """Score a single vault entity."""
    slug = entity["slug"]
    logger.info(f"Scoring vault: {slug}")

    raw_values = extract_vault_raw_values(entity, all_pools)
    if not raw_values:
        logger.warning(f"No data collected for vault {slug}")
        return None

    result = score_entity(VSRI_V01_DEFINITION, raw_values)
    result["entity_slug"] = slug
    result["entity_name"] = entity["name"]
    result["raw_values"] = raw_values

    return result


def store_vault_score(result: dict) -> None:
    """Store a vault score in the generic_index_scores table."""
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
        "vsri", slug, result["entity_name"], result["overall_score"],
        json.dumps(result["category_scores"]),
        json.dumps(result["component_scores"]),
        json.dumps(raw_for_storage, default=str),
        result["version"], inputs_hash,
        result.get("confidence", "limited"),
        result.get("confidence_tag"),
    ))


def run_vsri_scoring() -> list[dict]:
    """Score all vault entities. Called from worker."""
    all_pools = fetch_yield_pools()
    time.sleep(1)

    results = []
    for entity in VAULT_ENTITIES:
        try:
            result = score_vault(entity, all_pools)
            if result:
                store_vault_score(result)
                results.append(result)
                logger.info(
                    f"  {result['entity_name']}: {result['overall_score']} "
                    f"({result['components_available']}/{result['components_total']} components)"
                )
        except Exception as e:
            logger.warning(f"VSRI scoring failed for {entity['slug']}: {e}")

    # Attest VSRI scores
    try:
        from app.state_attestation import attest_state
        if results:
            attest_state("vsri_components", [
                {"slug": r["entity_slug"], "score": r["overall_score"]}
                for r in results
            ])
    except Exception:
        pass

    return results
