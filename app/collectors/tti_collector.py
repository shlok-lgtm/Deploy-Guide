"""
Tokenized Treasury Index Collector
====================================
Collects data for tokenized treasury / RWA product risk scoring.

76 components across 9 categories. 53% off-chain component ratio —
relies heavily on issuer disclosure ingestion (CDA pattern).

Data sources:
- CoinGecko: price, market cap, volume (existing integration)
- DeFiLlama: TVL for RWA protocols
- Etherscan: contract analysis, holder data (existing integration)
- Issuer disclosure pages: NAV reports, attestations (CDA pattern)
- Static config: regulatory, issuer, and compliance assessments

Composition: TTI x PSI = CQI pattern for "is this RWA safe in this protocol?"
"""

import json
import hashlib
import logging
import os
import time
from datetime import datetime, timezone

import requests

from app.database import execute, fetch_all, fetch_one
from app.index_definitions.tti_v01 import TTI_V01_DEFINITION, TTI_ENTITIES
from app.scoring_engine import score_entity
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
CG_BASE = "https://pro-api.coingecko.com/api/v3" if COINGECKO_API_KEY else "https://api.coingecko.com/api/v3"
DEFILLAMA_BASE = "https://api.llama.fi"


# =============================================================================
# Static config — off-chain assessments
# =============================================================================

TTI_STATIC_CONFIG = {
    "ondo-ousg": {
        "credit_quality": 90, "sovereign_risk": 95, "liquidity_of_underlying": 90,
        "currency_risk": 95, "reinvestment_risk": 80,
        "attestation_frequency": 80, "auditor_quality": 80, "reserve_coverage_ratio": 100,
        "collateral_segregation": 85, "custodian_quality": 85,
        "bankruptcy_remoteness": 80, "rehypothecation_risk": 85,
        "nav_update_frequency": 80, "pricing_methodology": 80, "oracle_integration": 70,
        "mark_to_market_accuracy": 80, "pricing_source_diversity": 70, "accrual_mechanism": 80,
        "redemption_window": 60, "settlement_time_hours": 24, "min_redemption_amount": 50,
        "gate_mechanism": 60, "redemption_fee": 75, "instant_liquidity_pct": 20,
        "tti_contract_audit": 3, "tti_upgradeability": 60, "tti_admin_key_risk": 60,
        "access_control": 70, "compliance_module": 80, "oracle_dependency": 65,
        "chain_infrastructure": 75, "tti_bug_bounty": 40, "minting_mechanism": 70,
        "emergency_mechanism": 60, "dependency_risk": 65,
        "issuer_regulatory_status": 80, "issuer_track_record": 70,
        "issuer_aum": 1000000000, "counterparty_count": 3, "bank_partner_quality": 75,
        "insurance_coverage_tti": 50, "conflict_of_interest": 70,
        "operational_continuity_tti": 70, "key_person_risk": 60,
        "securities_registration": 75, "investor_accreditation": 60,
        "kyc_aml_compliance": 80, "transfer_restrictions": 65,
        "tax_reporting": 60, "prospectus_availability": 70,
        "jurisdiction_risk_tti": 80, "regulatory_change_risk": 60,
        "institutional_holder_pct": 60, "geographic_distribution": 50,
    },
    "ondo-usdy": {
        "credit_quality": 85, "sovereign_risk": 90, "liquidity_of_underlying": 85,
        "currency_risk": 90, "reinvestment_risk": 75,
        "attestation_frequency": 75, "auditor_quality": 75, "reserve_coverage_ratio": 100,
        "collateral_segregation": 80, "custodian_quality": 80,
        "bankruptcy_remoteness": 75, "rehypothecation_risk": 80,
        "nav_update_frequency": 75, "pricing_methodology": 75, "oracle_integration": 70,
        "mark_to_market_accuracy": 75, "pricing_source_diversity": 65, "accrual_mechanism": 80,
        "redemption_window": 65, "settlement_time_hours": 24, "min_redemption_amount": 60,
        "gate_mechanism": 55, "redemption_fee": 70, "instant_liquidity_pct": 25,
        "tti_contract_audit": 3, "tti_upgradeability": 60, "tti_admin_key_risk": 60,
        "access_control": 70, "compliance_module": 75, "oracle_dependency": 65,
        "chain_infrastructure": 75, "tti_bug_bounty": 35, "minting_mechanism": 70,
        "emergency_mechanism": 55, "dependency_risk": 65,
        "issuer_regulatory_status": 75, "issuer_track_record": 65,
        "issuer_aum": 800000000, "counterparty_count": 3, "bank_partner_quality": 70,
        "insurance_coverage_tti": 45, "conflict_of_interest": 65,
        "operational_continuity_tti": 65, "key_person_risk": 55,
        "securities_registration": 70, "investor_accreditation": 50,
        "kyc_aml_compliance": 75, "transfer_restrictions": 60,
        "tax_reporting": 55, "prospectus_availability": 65,
        "jurisdiction_risk_tti": 75, "regulatory_change_risk": 55,
        "institutional_holder_pct": 50, "geographic_distribution": 50,
    },
    "blackrock-buidl": {
        "credit_quality": 95, "sovereign_risk": 95, "liquidity_of_underlying": 95,
        "currency_risk": 95, "reinvestment_risk": 85,
        "attestation_frequency": 90, "auditor_quality": 95, "reserve_coverage_ratio": 100,
        "collateral_segregation": 95, "custodian_quality": 95,
        "bankruptcy_remoteness": 90, "rehypothecation_risk": 95,
        "nav_update_frequency": 90, "pricing_methodology": 90, "oracle_integration": 75,
        "mark_to_market_accuracy": 90, "pricing_source_diversity": 80, "accrual_mechanism": 85,
        "redemption_window": 70, "settlement_time_hours": 24, "min_redemption_amount": 30,
        "gate_mechanism": 70, "redemption_fee": 80, "instant_liquidity_pct": 30,
        "tti_contract_audit": 4, "tti_upgradeability": 70, "tti_admin_key_risk": 70,
        "access_control": 85, "compliance_module": 90, "oracle_dependency": 70,
        "chain_infrastructure": 80, "tti_bug_bounty": 50, "minting_mechanism": 80,
        "emergency_mechanism": 70, "dependency_risk": 75,
        "issuer_regulatory_status": 95, "issuer_track_record": 95,
        "issuer_aum": 100000000000, "counterparty_count": 5, "bank_partner_quality": 95,
        "insurance_coverage_tti": 70, "conflict_of_interest": 80,
        "operational_continuity_tti": 90, "key_person_risk": 80,
        "securities_registration": 90, "investor_accreditation": 70,
        "kyc_aml_compliance": 95, "transfer_restrictions": 80,
        "tax_reporting": 85, "prospectus_availability": 90,
        "jurisdiction_risk_tti": 90, "regulatory_change_risk": 70,
        "institutional_holder_pct": 80, "geographic_distribution": 65,
    },
    "franklin-benji": {
        "credit_quality": 90, "sovereign_risk": 95, "liquidity_of_underlying": 90,
        "currency_risk": 95, "reinvestment_risk": 80,
        "attestation_frequency": 85, "auditor_quality": 90, "reserve_coverage_ratio": 100,
        "collateral_segregation": 90, "custodian_quality": 90,
        "bankruptcy_remoteness": 85, "rehypothecation_risk": 90,
        "nav_update_frequency": 85, "pricing_methodology": 85, "oracle_integration": 65,
        "mark_to_market_accuracy": 85, "pricing_source_diversity": 75, "accrual_mechanism": 80,
        "redemption_window": 65, "settlement_time_hours": 48, "min_redemption_amount": 40,
        "gate_mechanism": 65, "redemption_fee": 75, "instant_liquidity_pct": 20,
        "tti_contract_audit": 3, "tti_upgradeability": 65, "tti_admin_key_risk": 65,
        "access_control": 80, "compliance_module": 85, "oracle_dependency": 60,
        "chain_infrastructure": 75, "tti_bug_bounty": 40, "minting_mechanism": 75,
        "emergency_mechanism": 65, "dependency_risk": 70,
        "issuer_regulatory_status": 90, "issuer_track_record": 90,
        "issuer_aum": 50000000000, "counterparty_count": 4, "bank_partner_quality": 90,
        "insurance_coverage_tti": 65, "conflict_of_interest": 75,
        "operational_continuity_tti": 85, "key_person_risk": 75,
        "securities_registration": 85, "investor_accreditation": 65,
        "kyc_aml_compliance": 90, "transfer_restrictions": 75,
        "tax_reporting": 80, "prospectus_availability": 85,
        "jurisdiction_risk_tti": 85, "regulatory_change_risk": 65,
        "institutional_holder_pct": 70, "geographic_distribution": 55,
    },
    "mountain-usdm": {
        "credit_quality": 80, "sovereign_risk": 85, "liquidity_of_underlying": 80,
        "currency_risk": 85, "reinvestment_risk": 70,
        "attestation_frequency": 65, "auditor_quality": 60, "reserve_coverage_ratio": 100,
        "collateral_segregation": 70, "custodian_quality": 65,
        "bankruptcy_remoteness": 60, "rehypothecation_risk": 70,
        "nav_update_frequency": 70, "pricing_methodology": 65, "oracle_integration": 60,
        "mark_to_market_accuracy": 65, "pricing_source_diversity": 55, "accrual_mechanism": 70,
        "redemption_window": 55, "settlement_time_hours": 48, "min_redemption_amount": 50,
        "gate_mechanism": 50, "redemption_fee": 65, "instant_liquidity_pct": 15,
        "tti_contract_audit": 2, "tti_upgradeability": 55, "tti_admin_key_risk": 55,
        "access_control": 60, "compliance_module": 65, "oracle_dependency": 55,
        "chain_infrastructure": 70, "tti_bug_bounty": 30, "minting_mechanism": 60,
        "emergency_mechanism": 50, "dependency_risk": 55,
        "issuer_regulatory_status": 60, "issuer_track_record": 50,
        "issuer_aum": 100000000, "counterparty_count": 2, "bank_partner_quality": 55,
        "insurance_coverage_tti": 30, "conflict_of_interest": 55,
        "operational_continuity_tti": 50, "key_person_risk": 45,
        "securities_registration": 55, "investor_accreditation": 40,
        "kyc_aml_compliance": 65, "transfer_restrictions": 50,
        "tax_reporting": 45, "prospectus_availability": 50,
        "jurisdiction_risk_tti": 55, "regulatory_change_risk": 50,
        "institutional_holder_pct": 35, "geographic_distribution": 40,
    },
}


# =============================================================================
# CoinGecko market data
# =============================================================================

def _cg_headers() -> dict:
    h = {"Accept": "application/json"}
    if COINGECKO_API_KEY:
        h["x-cg-pro-api-key"] = COINGECKO_API_KEY
    return h


def fetch_tti_market_data(coingecko_id: str) -> dict | None:
    """Fetch market data for a tokenized treasury product."""
    if not coingecko_id:
        return None
    time.sleep(1.5)
    try:
        resp = requests.get(
            f"{CG_BASE}/coins/{coingecko_id}",
            params={"localization": "false", "tickers": "true",
                    "market_data": "true", "community_data": "false"},
            headers=_cg_headers(),
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.debug(f"CoinGecko TTI fetch failed for {coingecko_id}: {e}")
    return None


# =============================================================================
# Phase 1: Live data automation for static components
# =============================================================================

# Sovereign risk lookup: issuing jurisdiction → risk score (0-100, higher = safer)
SOVEREIGN_RISK_MAP = {
    "us": 95,       # US treasuries
    "uk": 90,       # UK gilts
    "germany": 90,  # Bunds
    "france": 85,
    "japan": 85,
    "canada": 90,
    "australia": 88,
    "switzerland": 92,
    "singapore": 90,
    "hong kong": 85,
    "cayman": 70,
    "bvi": 60,
    "default_g7": 85,
    "default_other": 70,
}

# Chain infrastructure quality mapping (chain → score)
CHAIN_INFRA_SCORES = {
    "ethereum": 100,
    "base": 80,
    "arbitrum": 80,
    "optimism": 78,
    "polygon": 75,
    "solana": 75,
    "avalanche": 72,
    "bnb chain": 65,
    "fantom": 55,
    "default": 60,
}

# Issuer → primary jurisdiction
ISSUER_JURISDICTION = {
    "ondo finance": "us",
    "blackrock": "us",
    "franklin templeton": "us",
    "backed finance": "switzerland",
    "maple finance": "cayman",
    "centrifuge": "us",
    "superstate": "us",
    "mountain protocol": "bvi",
    "openeden": "singapore",
}

# Issuer → deployment chain
ISSUER_CHAIN = {
    "ondo finance": "ethereum",
    "blackrock": "ethereum",
    "franklin templeton": "ethereum",
    "backed finance": "ethereum",
    "maple finance": "ethereum",
    "centrifuge": "ethereum",
    "superstate": "ethereum",
    "mountain protocol": "ethereum",
    "openeden": "ethereum",
}

# Immunefi bounty name mapping for TTI issuers
TTI_IMMUNEFI_SLUGS = {
    "ondo-ousg": "ondo-finance",
    "ondo-usdy": "ondo-finance",
    "blackrock-buidl": "blackrock",
    "franklin-benji": "franklin-templeton",
    "backed-bib01": "backed",
    "maple-cash": "maple",
    "centrifuge-pools": "centrifuge",
    "superstate-ustb": "superstate",
    "mountain-usdm": "mountain-protocol",
    "openeden-tbill": "openeden",
}


def _automate_tti_sovereign_risk(entity: dict, static: dict) -> dict:
    """Map issuer jurisdiction to sovereign risk score (deterministic lookup)."""
    automated = {}
    issuer = (entity.get("issuer") or "").lower()
    jurisdiction = ISSUER_JURISDICTION.get(issuer, "default_other")
    live_score = SOVEREIGN_RISK_MAP.get(jurisdiction, SOVEREIGN_RISK_MAP["default_other"])

    static_score = static.get("sovereign_risk", 70)
    automated["sovereign_risk"] = max(live_score, static_score)
    return automated


def _automate_tti_chain_infrastructure(entity: dict, static: dict) -> dict:
    """Map deployment chain to infrastructure quality score."""
    automated = {}
    issuer = (entity.get("issuer") or "").lower()
    chain = ISSUER_CHAIN.get(issuer, "default")
    live_score = CHAIN_INFRA_SCORES.get(chain, CHAIN_INFRA_SCORES["default"])

    static_score = static.get("chain_infrastructure", 60)
    automated["chain_infrastructure"] = max(live_score, static_score)
    return automated


def _automate_tti_bug_bounty(entity: dict, static: dict) -> dict:
    """Fetch bug bounty from Immunefi for TTI issuer."""
    automated = {}
    slug = entity["slug"]
    immunefi_slug = TTI_IMMUNEFI_SLUGS.get(slug, slug)

    try:
        from app.collectors.smart_contract import fetch_immunefi_bounty
        bounty = fetch_immunefi_bounty(immunefi_slug)

        if bounty.get("active") and bounty.get("max_bounty", 0) > 0:
            # tti_bug_bounty is a direct 0-100 score
            max_b = bounty["max_bounty"]
            if max_b >= 10_000_000:
                automated["tti_bug_bounty"] = 90
            elif max_b >= 1_000_000:
                automated["tti_bug_bounty"] = 70
            elif max_b >= 250_000:
                automated["tti_bug_bounty"] = 50
            elif max_b >= 50_000:
                automated["tti_bug_bounty"] = 35
            else:
                automated["tti_bug_bounty"] = 25
        else:
            automated["tti_bug_bounty"] = static.get("tti_bug_bounty", 20)
    except Exception as e:
        logger.debug(f"TTI Immunefi fetch failed for {slug}: {e}")

    return automated


def _automate_tti_smart_contract(entity: dict, static: dict) -> dict:
    """Automate tti_contract_audit, tti_upgradeability, tti_admin_key_risk from live analysis.

    Only runs if entity has a contract address.
    """
    automated = {}
    contract = entity.get("contract")
    if not contract:
        return automated

    try:
        from app.collectors.smart_contract import analyze_contract_for_index_sync
        analysis = analyze_contract_for_index_sync(contract)

        # tti_contract_audit: log normalization {1:30, 2:50, 3:70, 5:85, 10:100}
        static_audit = static.get("tti_contract_audit", 1)
        if analysis.get("audit_verified"):
            live_audit = 3
            if analysis.get("is_proxy") and analysis.get("implementation_verified"):
                live_audit = 5
            automated["tti_contract_audit"] = max(live_audit, static_audit)

        # tti_upgradeability: 0-100 direct
        live_upgrade = analysis.get("upgradeability_risk", 50)
        static_upgrade = static.get("tti_upgradeability", 50)
        automated["tti_upgradeability"] = max(live_upgrade, static_upgrade)

        # tti_admin_key_risk: 0-100 direct
        live_admin = analysis.get("admin_key_risk", 50)
        static_admin = static.get("tti_admin_key_risk", 50)
        automated["tti_admin_key_risk"] = max(live_admin, static_admin)
    except Exception as e:
        logger.warning(f"TTI smart contract automation failed for {entity['slug']}: {e}")

    return automated


def _automate_tti_nav_update_frequency(entity: dict, static: dict, cg_data: dict | None) -> dict:
    """Derive nav_update_frequency from CoinGecko price update patterns.

    If price updates frequently (many tickers), NAV is effectively real-time.
    """
    automated = {}
    if not cg_data:
        return automated

    tickers = cg_data.get("tickers", [])
    if not tickers:
        return automated

    # More exchange listings and tickers = more frequent price updates = higher score
    ticker_count = len(tickers)
    if ticker_count >= 20:
        live_score = 90  # very liquid, real-time NAV
    elif ticker_count >= 10:
        live_score = 80
    elif ticker_count >= 5:
        live_score = 70
    elif ticker_count >= 2:
        live_score = 60
    else:
        live_score = 50  # single exchange, limited updates

    static_score = static.get("nav_update_frequency", 50)
    automated["nav_update_frequency"] = max(live_score, static_score)
    return automated


def _automate_tti_oracle_dependency(entity: dict, static: dict) -> dict:
    """Check if TTI contract integrates Chainlink or other oracle feeds.

    Only runs if entity has a contract address.
    """
    automated = {}
    contract = entity.get("contract")
    if not contract:
        return automated

    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        return automated

    try:
        import time as _time
        _time.sleep(0.15)
        import httpx
        resp = httpx.get("https://api.etherscan.io/v2/api", params={
            "chainid": 1,
            "module": "contract",
            "action": "getabi",
            "address": contract,
            "apikey": api_key,
        }, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "1":
                import json
                abi_str = data.get("result", "")
                if abi_str and abi_str.startswith("["):
                    abi_text = abi_str.lower()
                    # Look for oracle-related function names
                    oracle_keywords = ["latestanswer", "latestrounddata", "pricefeed",
                                       "oracle", "getprice", "getlatestprice",
                                       "chainlink", "aggregator"]
                    oracle_count = sum(1 for kw in oracle_keywords if kw in abi_text)

                    if oracle_count >= 3:
                        automated["oracle_dependency"] = 85  # strong oracle integration
                    elif oracle_count >= 1:
                        automated["oracle_dependency"] = 70
                    else:
                        automated["oracle_dependency"] = static.get("oracle_dependency", 50)
    except Exception as e:
        logger.debug(f"TTI oracle dependency check failed for {entity['slug']}: {e}")

    return automated


def extract_tti_raw_values(entity: dict, holder_data: dict = None) -> dict:
    """Extract raw values from all sources."""
    slug = entity["slug"]
    raw = {}

    # CoinGecko market data
    cg_data = None
    cg_id = entity.get("coingecko_id")
    if cg_id:
        cg_data = fetch_tti_market_data(cg_id)
        if cg_data:
            market = cg_data.get("market_data", {})
            mcap = market.get("market_cap", {}).get("usd")
            if mcap:
                raw["tti_market_cap"] = mcap
                raw["tti_tvl"] = mcap  # for tokenized treasuries, market cap ≈ TVL

            vol = market.get("total_volume", {}).get("usd")
            if vol:
                raw["tti_volume_24h"] = vol

            # NAV deviation from price
            price = market.get("current_price", {}).get("usd")
            if price:
                # Most tokenized treasuries target $1.00 or accrue value
                raw["nav_deviation"] = abs(price - 1.0) * 100 if price < 10 else 0

            # Holder count from tickers
            tickers = cg_data.get("tickers", [])
            if tickers:
                raw["exchange_listing_count"] = len(set(t.get("market", {}).get("identifier", "") for t in tickers))

    # DeFiLlama TVL (if different from market cap)
    try:
        # Some TTI entities are listed on DeFiLlama as protocols
        issuer_slug = entity.get("issuer", "").lower().replace(" ", "-")
        if issuer_slug:
            time.sleep(1)
            resp = requests.get(f"{DEFILLAMA_BASE}/protocol/{issuer_slug}", timeout=15)
            if resp.status_code == 200:
                protocol_data = resp.json()
                tvl = protocol_data.get("tvl")
                if isinstance(tvl, list) and tvl:
                    last = tvl[-1]
                    if isinstance(last, dict):
                        raw["tti_tvl"] = raw.get("tti_tvl") or last.get("totalLiquidityUSD", 0)
    except Exception as e:
        logger.debug(f"DeFiLlama TTI fetch failed for {slug}: {e}")

    # Etherscan holder data (if contract and holder_cache provided)
    if holder_data:
        if holder_data.get("holder_count"):
            raw["tti_holder_count"] = holder_data["holder_count"]
        raw["tti_top10_concentration"] = holder_data.get("top_10_pct", 0)
        raw["defi_integration_count"] = holder_data.get("defi_protocol_count", 0)

    # Static config (off-chain components — bulk of TTI data, applied first)
    static = TTI_STATIC_CONFIG.get(slug, {})
    raw.update(static)

    # --- Phase 1 automation: replace static with live data ---
    # Sovereign risk from jurisdiction mapping (deterministic)
    raw.update(_automate_tti_sovereign_risk(entity, static))

    # Chain infrastructure from chain mapping (deterministic)
    raw.update(_automate_tti_chain_infrastructure(entity, static))

    # Bug bounty from Immunefi
    raw.update(_automate_tti_bug_bounty(entity, static))

    # Smart contract checks (audit, upgradeability, admin key) — only if contract exists
    raw.update(_automate_tti_smart_contract(entity, static))

    # NAV update frequency from CoinGecko ticker data
    raw.update(_automate_tti_nav_update_frequency(entity, static, cg_data))

    # Oracle dependency from contract ABI — only if contract exists
    raw.update(_automate_tti_oracle_dependency(entity, static))

    # --- Phase 3A: TTI issuer disclosure parsing ---
    try:
        from app.services.tti_disclosure_collector import (
            collect_entity_disclosures, map_disclosure_to_components,
        )
        disclosure_data = collect_entity_disclosures(slug, entity.get("name", slug))
        if disclosure_data:
            disclosure_components = map_disclosure_to_components(disclosure_data, static)
            raw.update(disclosure_components)
    except Exception as e:
        logger.debug(f"TTI disclosure automation failed for {slug}: {e}")

    return raw


# =============================================================================
# Score and store
# =============================================================================

def score_tti(entity: dict, holder_cache: dict = None) -> dict | None:
    """Score a single TTI entity."""
    slug = entity["slug"]
    logger.info(f"Scoring TTI: {slug}")

    # Look up holder data from cache if contract known
    holder_data = None
    contract = entity.get("contract")
    if contract and holder_cache:
        holder_data = holder_cache.get(contract.lower())

    raw_values = extract_tti_raw_values(entity, holder_data=holder_data)
    if not raw_values:
        logger.warning(f"No data collected for TTI {slug}")
        return None

    result = score_entity(TTI_V01_DEFINITION, raw_values)
    result["entity_slug"] = slug
    result["entity_name"] = entity["name"]
    result["raw_values"] = raw_values

    return result


def store_tti_score(result: dict) -> None:
    """Store a TTI score in the generic_index_scores table."""
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
        "tti", slug, result["entity_name"], result["overall_score"],
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


def run_tti_scoring() -> list[dict]:
    """Score all TTI entities. Called from worker."""
    # Pre-fetch holder data for entities with contracts (cached 24h)
    holder_cache = {}
    try:
        from app.collectors.holder_analysis import analyze_holders_sync, get_cached_holders
        for entity in TTI_ENTITIES:
            contract = entity.get("contract")
            if contract:
                cached = get_cached_holders(contract)
                if cached:
                    holder_cache[contract.lower()] = cached
                else:
                    hdata = analyze_holders_sync(contract, decimals=18)
                    if hdata.get("balances_found", 0) > 0:
                        holder_cache[contract.lower()] = hdata
    except Exception as e:
        logger.warning(f"TTI holder analysis pre-fetch failed: {e}")

    results = []
    for entity in TTI_ENTITIES:
        try:
            result = score_tti(entity, holder_cache=holder_cache)
            if result:
                store_tti_score(result)
                results.append(result)
                logger.info(
                    f"  {result['entity_name']}: {result['overall_score']} "
                    f"({result['components_available']}/{result['components_total']} components)"
                )
        except Exception as e:
            logger.warning(f"TTI scoring failed for {entity['slug']}: {e}")

    # Attest TTI scores
    try:
        from app.state_attestation import attest_state
        if results:
            attest_state("tti_components", [
                {"slug": r["entity_slug"], "score": r["overall_score"]}
                for r in results
            ])
    except Exception as e:
        logger.warning(f"TTI attestation failed: {e}")

    return results
