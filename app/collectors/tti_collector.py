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


def extract_tti_raw_values(entity: dict) -> dict:
    """Extract raw values from all sources."""
    slug = entity["slug"]
    raw = {}

    # CoinGecko market data
    cg_id = entity.get("coingecko_id")
    if cg_id:
        data = fetch_tti_market_data(cg_id)
        if data:
            market = data.get("market_data", {})
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
            tickers = data.get("tickers", [])
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

    # Etherscan holder data (if contract exists — skipped for now; uses config)
    # Static config (off-chain components — bulk of TTI data)
    static = TTI_STATIC_CONFIG.get(slug, {})
    raw.update(static)

    return raw


# =============================================================================
# Score and store
# =============================================================================

def score_tti(entity: dict) -> dict | None:
    """Score a single TTI entity."""
    slug = entity["slug"]
    logger.info(f"Scoring TTI: {slug}")

    raw_values = extract_tti_raw_values(entity)
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
        "tti", slug, result["entity_name"], result["overall_score"],
        json.dumps(result["category_scores"]),
        json.dumps(result["component_scores"]),
        json.dumps(raw_for_storage, default=str),
        result["version"], inputs_hash,
        result.get("confidence", "limited"),
        result.get("confidence_tag"),
    ))


def run_tti_scoring() -> list[dict]:
    """Score all TTI entities. Called from worker."""
    results = []
    for entity in TTI_ENTITIES:
        try:
            result = score_tti(entity)
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
    except Exception:
        pass

    return results
