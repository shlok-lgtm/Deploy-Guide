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
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://api.llama.fi"
ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"

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


# =============================================================================
# Phase 1: Live data automation for static components
# =============================================================================

def _automate_bridge_contract_age(entity: dict, static: dict) -> dict:
    """Fetch contract age via Etherscan V2 first transaction lookup."""
    automated = {}
    contract = entity.get("primary_contract")
    if not contract:
        return automated

    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        return automated

    try:
        time.sleep(0.15)
        resp = requests.get(ETHERSCAN_V2_BASE, params={
            "chainid": 1,
            "module": "account",
            "action": "txlist",
            "address": contract,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": 1,
            "sort": "asc",
            "apikey": api_key,
        }, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            txs = data.get("result", [])
            if isinstance(txs, list) and txs:
                first_ts = int(txs[0].get("timeStamp", 0))
                if first_ts > 0:
                    first_date = datetime.fromtimestamp(first_ts, tz=timezone.utc)
                    age_days = (datetime.now(timezone.utc) - first_date).days
                    static_age = static.get("contract_age_days", 0)
                    automated["contract_age_days"] = max(age_days, static_age)
                    logger.info(f"BRI contract age {entity['slug']}: {age_days} days")
    except Exception as e:
        logger.debug(f"BRI contract age failed for {entity['slug']}: {e}")

    return automated


def _automate_bridge_hacks(entity: dict, static: dict, hacks_cache: list = None) -> dict:
    """Automate incident_history and time_since_incident_days from DeFiLlama hacks."""
    automated = {}

    try:
        from app.collectors.defillama import (
            fetch_defillama_hacks, filter_hacks_by_name, score_exploit_history_from_hacks,
        )
        hacks = hacks_cache if hacks_cache is not None else fetch_defillama_hacks()
        name = entity.get("name", "")
        matched = filter_hacks_by_name(hacks, name)
        if not matched:
            matched = filter_hacks_by_name(hacks, entity["slug"].split("-")[0])

        # incident_history: 0-100 score (direct normalization)
        live_score = score_exploit_history_from_hacks(matched)
        static_score = static.get("incident_history", 100)
        automated["incident_history"] = min(live_score, static_score)

        # time_since_incident_days: days since most recent hack
        if matched:
            now = datetime.now(timezone.utc)
            most_recent_days = None
            for h in matched:
                date_str = h.get("date")
                if not date_str:
                    continue
                try:
                    if isinstance(date_str, (int, float)):
                        hack_date = datetime.fromtimestamp(date_str, tz=timezone.utc)
                    else:
                        ds = str(date_str).replace("Z", "+00:00")
                        hack_date = datetime.fromisoformat(ds)
                        if hack_date.tzinfo is None:
                            hack_date = hack_date.replace(tzinfo=timezone.utc)
                    days = (now - hack_date).days
                    if most_recent_days is None or days < most_recent_days:
                        most_recent_days = days
                except (ValueError, TypeError):
                    pass
            if most_recent_days is not None:
                automated["time_since_incident_days"] = most_recent_days
        else:
            # No incidents found — use contract age if available, else static
            automated["time_since_incident_days"] = static.get("time_since_incident_days", 1000)
    except Exception as e:
        logger.warning(f"BRI hacks automation failed for {entity['slug']}: {e}")

    return automated


def _automate_bridge_bounty(entity: dict, static: dict) -> dict:
    """Fetch bug bounty size from Immunefi."""
    automated = {}
    slug = entity["slug"]

    try:
        from app.collectors.smart_contract import fetch_immunefi_bounty
        # Try slug and also common bounty names
        bounty = fetch_immunefi_bounty(slug)
        if not bounty.get("active"):
            # Try with just first part of slug (e.g., "wormhole" from "wormhole")
            bounty = fetch_immunefi_bounty(slug.split("-")[0])

        if bounty.get("active") and bounty.get("max_bounty", 0) > 0:
            automated["bug_bounty_size"] = bounty["max_bounty"]
        else:
            # Keep static value if Immunefi didn't find it
            automated["bug_bounty_size"] = static.get("bug_bounty_size", 0)
    except Exception as e:
        logger.debug(f"BRI Immunefi fetch failed for {slug}: {e}")

    return automated


def _automate_bridge_from_defillama(entity: dict, static: dict, bridges_data: list[dict]) -> dict:
    """Automate uptime_pct, token_coverage, message_success_rate from DeFiLlama bridge data."""
    automated = {}
    slug = entity["slug"]
    defillama_id = entity.get("defillama_id", "")

    matched = None
    for b in bridges_data:
        b_name = (b.get("displayName") or b.get("name") or "").lower()
        if defillama_id.lower() in b_name or slug.replace("-", " ") in b_name:
            matched = b
            break

    if not matched:
        return automated

    # token_coverage: count unique tokens from bridge chains/tokens data
    chains = matched.get("chains", [])
    if chains:
        # Number of supported chains as a proxy for token coverage breadth
        automated["token_coverage"] = len(chains)

    # uptime_pct: derive from volume consistency
    # If the bridge has current volume > 0 and positive last hourly/daily data, it's "up"
    daily_vol = matched.get("lastDailyVolume") or matched.get("currentDailyVolume", 0)
    hourly_vol = matched.get("lastHourlyVolume", 0)

    if daily_vol and daily_vol > 0:
        # Active bridge — estimate high uptime
        # We can't truly measure uptime from this API, but active volume = operational
        static_uptime = static.get("uptime_pct", 99.0)
        automated["uptime_pct"] = max(99.0, static_uptime)
    elif hourly_vol == 0 and daily_vol == 0:
        # No recent volume — potential downtime
        automated["uptime_pct"] = min(95.0, static.get("uptime_pct", 95.0))

    # message_success_rate: volume consistency as proxy
    # If daily volume is consistent (>0 for both hourly and daily), high success rate
    if daily_vol and daily_vol > 0 and hourly_vol and hourly_vol > 0:
        static_msr = static.get("message_success_rate", 99.0)
        automated["message_success_rate"] = max(99.0, static_msr)

    # guardian_count: for Axelar (PoS validators), we can approximate from chain data
    # But for most bridges this requires contract reads — keep static for now
    # unless we find it in the bridge API response
    validators = matched.get("validatorCount") or matched.get("validators")
    if validators and isinstance(validators, (int, float)) and validators > 0:
        automated["guardian_count"] = int(validators)

    return automated


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

def score_bridge(entity: dict, bridges_data: list[dict], holder_cache: dict = None,
                 hacks_cache: list = None) -> dict | None:
    """Score a single bridge entity."""
    slug = entity["slug"]
    logger.info(f"Scoring bridge: {slug}")

    raw_values = extract_bridge_raw_values(entity, bridges_data)
    if not raw_values:
        logger.warning(f"No data collected for bridge {slug}")
        return None

    static = BRIDGE_STATIC_CONFIG.get(slug, {})

    # --- Phase 1 automation: replace static with live data ---
    # Contract age from Etherscan
    age_automated = _automate_bridge_contract_age(entity, static)
    raw_values.update(age_automated)

    # Incident history + time since incident from DeFiLlama hacks
    hacks_automated = _automate_bridge_hacks(entity, static, hacks_cache)
    raw_values.update(hacks_automated)

    # Bug bounty from Immunefi
    bounty_automated = _automate_bridge_bounty(entity, static)
    raw_values.update(bounty_automated)

    # Uptime, token coverage, message success rate from DeFiLlama bridge data
    dl_automated = _automate_bridge_from_defillama(entity, static, bridges_data)
    raw_values.update(dl_automated)

    # Holder analysis for governance token (if available)
    token_contract = entity.get("token_contract")
    if token_contract and holder_cache:
        hdata = holder_cache.get(token_contract.lower())
        if hdata:
            raw_values["token_holder_concentration"] = hdata["top_10_pct"]

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
             formula_version, inputs_hash, confidence, confidence_tag,
             component_coverage, components_populated, components_total, missing_categories)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            component_coverage = EXCLUDED.component_coverage,
            components_populated = EXCLUDED.components_populated,
            components_total = EXCLUDED.components_total,
            missing_categories = EXCLUDED.missing_categories,
            computed_at = NOW()
    """, (
        "bri", slug, result["entity_name"], result["overall_score"],
        json.dumps(result["category_scores"]),
        json.dumps(result["component_scores"]),
        json.dumps(raw_for_storage, default=str),
        result["version"], inputs_hash,
        result.get("confidence", "limited"),
        result.get("confidence_tag"),
        result.get("component_coverage"),
        result.get("components_populated"),
        result.get("components_total"),
        json.dumps(result.get("missing_categories") or []),
    ))


def run_bri_scoring() -> list[dict]:
    """Score all bridge entities. Called from worker."""
    bridges_data = fetch_bridge_data()
    time.sleep(1)

    # Pre-fetch DeFiLlama hacks data (cached 24h, shared across all entities)
    hacks_cache = []
    try:
        from app.collectors.defillama import fetch_defillama_hacks
        hacks_cache = fetch_defillama_hacks()
    except Exception as e:
        logger.warning(f"BRI hacks pre-fetch failed: {e}")

    # Pre-fetch holder data for bridges with governance tokens (cached 24h)
    holder_cache = {}
    try:
        from app.collectors.holder_analysis import analyze_holders_sync, get_cached_holders
        for entity in BRIDGE_ENTITIES:
            tc = entity.get("token_contract")
            if tc:
                cached = get_cached_holders(tc)
                if cached:
                    holder_cache[tc.lower()] = cached
                else:
                    hdata = analyze_holders_sync(tc, decimals=18)
                    if hdata.get("balances_found", 0) > 0:
                        holder_cache[tc.lower()] = hdata
    except Exception as e:
        logger.warning(f"BRI holder analysis pre-fetch failed: {e}")

    results = []
    for entity in BRIDGE_ENTITIES:
        try:
            result = score_bridge(entity, bridges_data, holder_cache=holder_cache,
                                  hacks_cache=hacks_cache)
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
    except Exception as e:
        logger.warning(f"BRI attestation failed: {e}")

    return results
