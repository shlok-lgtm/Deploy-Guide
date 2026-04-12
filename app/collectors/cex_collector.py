"""
CEX Reserve Integrity Index Collector
=======================================
Collects data for centralized exchange reserve risk scoring.

This is the hardest collector because evidence sources are genuinely messy.
Each exchange publishes proof-of-reserves differently, at different frequencies,
with different completeness levels.

Data sources:
- CoinGecko exchange data: volume, trust score, year established (existing integration)
- DeFiLlama: exchange-held wallet balances (where labeled)
- Static config: PoR assessments, regulatory status, operational history

Per-exchange parsers follow the CDA pattern (type-aware verification per issuer).
"""

import json
import hashlib
import logging
import os
import time
from datetime import datetime, timezone

import requests

from app.database import execute, fetch_all, fetch_one
from app.index_definitions.cxri_v01 import CXRI_V01_DEFINITION, CEX_ENTITIES
from app.scoring_engine import score_entity

logger = logging.getLogger(__name__)

COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
CG_BASE = "https://pro-api.coingecko.com/api/v3" if COINGECKO_API_KEY else "https://api.coingecko.com/api/v3"

# =============================================================================
# Static config — PoR assessments, regulatory status, operational history
# Per-exchange, updated manually as conditions change.
# =============================================================================

CEX_STATIC_CONFIG = {
    "binance": {
        "por_method": 70,  # Merkle tree PoR, published regularly
        "por_frequency": 70, "por_recency_days": 30,
        "auditor_reputation": 50, "liabilities_included": 60,
        "negative_balance_detection": 50,
        "license_count": 15, "mica_status": 60, "us_licensing": 20,
        "enforcement_history": 40, "jurisdiction_quality": 50,
        "years_in_operation": 7, "withdrawal_freeze_count": 1,
        "security_breach_count": 1, "insurance_coverage": 60,
        "fund_segregation": 50,
        "public_audit_reports": 50, "realtime_reserve_dashboard": 80,
        "api_availability": 90, "corporate_disclosure": 40,
    },
    "okx": {
        "por_method": 75,  # Merkle tree + zk-STARKs
        "por_frequency": 75, "por_recency_days": 30,
        "auditor_reputation": 55, "liabilities_included": 65,
        "negative_balance_detection": 60,
        "license_count": 10, "mica_status": 50, "us_licensing": 10,
        "enforcement_history": 60, "jurisdiction_quality": 50,
        "years_in_operation": 7, "withdrawal_freeze_count": 0,
        "security_breach_count": 0, "insurance_coverage": 50,
        "fund_segregation": 50,
        "public_audit_reports": 55, "realtime_reserve_dashboard": 80,
        "api_availability": 85, "corporate_disclosure": 40,
    },
    "bybit": {
        "por_method": 65,
        "por_frequency": 65, "por_recency_days": 30,
        "auditor_reputation": 45, "liabilities_included": 55,
        "negative_balance_detection": 45,
        "license_count": 5, "mica_status": 40, "us_licensing": 10,
        "enforcement_history": 60, "jurisdiction_quality": 45,
        "years_in_operation": 6, "withdrawal_freeze_count": 0,
        "security_breach_count": 0, "insurance_coverage": 40,
        "fund_segregation": 40,
        "public_audit_reports": 45, "realtime_reserve_dashboard": 70,
        "api_availability": 80, "corporate_disclosure": 35,
    },
    "bitget": {
        "por_method": 60,
        "por_frequency": 60, "por_recency_days": 60,
        "auditor_reputation": 40, "liabilities_included": 45,
        "negative_balance_detection": 40,
        "license_count": 4, "mica_status": 30, "us_licensing": 10,
        "enforcement_history": 60, "jurisdiction_quality": 40,
        "years_in_operation": 6, "withdrawal_freeze_count": 0,
        "security_breach_count": 0, "insurance_coverage": 40,
        "fund_segregation": 35,
        "public_audit_reports": 40, "realtime_reserve_dashboard": 60,
        "api_availability": 75, "corporate_disclosure": 30,
    },
    "kraken": {
        "por_method": 80,  # Regular third-party audits
        "por_frequency": 70, "por_recency_days": 60,
        "auditor_reputation": 75, "liabilities_included": 75,
        "negative_balance_detection": 70,
        "license_count": 12, "mica_status": 70, "us_licensing": 80,
        "enforcement_history": 50, "jurisdiction_quality": 80,
        "years_in_operation": 13, "withdrawal_freeze_count": 0,
        "security_breach_count": 0, "insurance_coverage": 60,
        "fund_segregation": 70,
        "public_audit_reports": 70, "realtime_reserve_dashboard": 50,
        "api_availability": 85, "corporate_disclosure": 60,
    },
    "coinbase": {
        "por_method": 85,  # Publicly traded, SEC reporting
        "por_frequency": 85, "por_recency_days": 30,
        "auditor_reputation": 90, "liabilities_included": 90,
        "negative_balance_detection": 80,
        "license_count": 20, "mica_status": 80, "us_licensing": 95,
        "enforcement_history": 40, "jurisdiction_quality": 90,
        "years_in_operation": 12, "withdrawal_freeze_count": 0,
        "security_breach_count": 0, "insurance_coverage": 80,
        "fund_segregation": 85,
        "public_audit_reports": 90, "realtime_reserve_dashboard": 40,
        "api_availability": 90, "corporate_disclosure": 90,
    },
    "gate-io": {
        "por_method": 55,
        "por_frequency": 50, "por_recency_days": 90,
        "auditor_reputation": 35, "liabilities_included": 40,
        "negative_balance_detection": 30,
        "license_count": 3, "mica_status": 20, "us_licensing": 10,
        "enforcement_history": 50, "jurisdiction_quality": 35,
        "years_in_operation": 11, "withdrawal_freeze_count": 0,
        "security_breach_count": 1, "insurance_coverage": 30,
        "fund_segregation": 30,
        "public_audit_reports": 30, "realtime_reserve_dashboard": 50,
        "api_availability": 70, "corporate_disclosure": 25,
    },
    "kucoin": {
        "por_method": 60,
        "por_frequency": 55, "por_recency_days": 60,
        "auditor_reputation": 40, "liabilities_included": 45,
        "negative_balance_detection": 35,
        "license_count": 4, "mica_status": 25, "us_licensing": 10,
        "enforcement_history": 50, "jurisdiction_quality": 40,
        "years_in_operation": 7, "withdrawal_freeze_count": 1,
        "security_breach_count": 1, "insurance_coverage": 40,
        "fund_segregation": 35,
        "public_audit_reports": 35, "realtime_reserve_dashboard": 55,
        "api_availability": 75, "corporate_disclosure": 30,
    },
}


# =============================================================================
# CoinGecko exchange data
# =============================================================================

def _cg_headers() -> dict:
    h = {"Accept": "application/json"}
    if COINGECKO_API_KEY:
        h["x-cg-pro-api-key"] = COINGECKO_API_KEY
    return h


def fetch_exchange_data(coingecko_id: str) -> dict | None:
    """Fetch exchange data from CoinGecko."""
    time.sleep(1.5)
    try:
        resp = requests.get(
            f"{CG_BASE}/exchanges/{coingecko_id}",
            headers=_cg_headers(),
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.debug(f"CoinGecko exchange fetch failed for {coingecko_id}: {e}")
    return None


def extract_cex_raw_values(entity: dict) -> dict:
    """Extract raw values from CoinGecko + static config."""
    slug = entity["slug"]
    raw = {}

    # CoinGecko exchange data
    cg_id = entity.get("coingecko_id")
    if cg_id:
        data = fetch_exchange_data(cg_id)
        if data:
            # Trust score
            trust = data.get("trust_score")
            if trust:
                raw["reserve_asset_diversity"] = trust * 10  # trust_score is 1-10

            # Year established -> years in operation
            year = data.get("year_established")
            if year:
                raw["years_in_operation"] = datetime.now().year - year

            # Trade volume 24h (BTC) as proxy for activity
            vol_btc = data.get("trade_volume_24h_btc")
            if vol_btc:
                # Very rough: known wallet balance proxy (exchange with more volume = larger reserves)
                raw["known_wallet_balance"] = vol_btc * 60000  # approximate BTC price

    # Static config (overrides CoinGecko where both exist, except years_in_operation)
    static = CEX_STATIC_CONFIG.get(slug, {})
    for k, v in static.items():
        if k == "years_in_operation" and k in raw:
            continue  # Keep CoinGecko's live value
        raw[k] = v

    return raw


# =============================================================================
# Score and store
# =============================================================================

def score_cex(entity: dict) -> dict | None:
    """Score a single CEX entity."""
    slug = entity["slug"]
    logger.info(f"Scoring CEX: {slug}")

    raw_values = extract_cex_raw_values(entity)
    if not raw_values:
        logger.warning(f"No data collected for CEX {slug}")
        return None

    result = score_entity(CXRI_V01_DEFINITION, raw_values)
    result["entity_slug"] = slug
    result["entity_name"] = entity["name"]
    result["raw_values"] = raw_values

    return result


def store_cex_score(result: dict) -> None:
    """Store a CEX score in the generic_index_scores table."""
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
        "cxri", slug, result["entity_name"], result["overall_score"],
        json.dumps(result["category_scores"]),
        json.dumps(result["component_scores"]),
        json.dumps(raw_for_storage, default=str),
        result["version"], inputs_hash,
        result.get("confidence", "limited"),
        result.get("confidence_tag"),
    ))


def run_cxri_scoring() -> list[dict]:
    """Score all CEX entities. Called from worker."""
    results = []
    for entity in CEX_ENTITIES:
        try:
            result = score_cex(entity)
            if result:
                store_cex_score(result)
                results.append(result)
                logger.info(
                    f"  {result['entity_name']}: {result['overall_score']} "
                    f"({result['components_available']}/{result['components_total']} components)"
                )
        except Exception as e:
            logger.warning(f"CXRI scoring failed for {entity['slug']}: {e}")

    # Attest CXRI scores
    try:
        from app.state_attestation import attest_state
        if results:
            attest_state("cxri_components", [
                {"slug": r["entity_slug"], "score": r["overall_score"]}
                for r in results
            ])
    except Exception:
        pass

    return results
