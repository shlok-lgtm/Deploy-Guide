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


# =============================================================================
# Phase 1: Live data automation for static components
# =============================================================================

# Exchange API endpoints for availability checks (public, no auth needed)
CEX_API_ENDPOINTS = {
    "binance": "https://api.binance.com/api/v3/ping",
    "okx": "https://www.okx.com/api/v5/public/time",
    "bybit": "https://api.bybit.com/v5/market/time",
    "bitget": "https://api.bitget.com/api/v2/spot/public/time",
    "kraken": "https://api.kraken.com/0/public/Time",
    "coinbase": "https://api.coinbase.com/v2/time",
    "gate-io": "https://api.gateio.ws/api/v4/spot/time",
    "kucoin": "https://api.kucoin.com/api/v1/timestamp",
}

# Reserve dashboard URLs (checked for 200 response)
CEX_RESERVE_URLS = {
    "binance": "https://www.binance.com/en/proof-of-reserves",
    "okx": "https://www.okx.com/proof-of-reserves",
    "bybit": "https://www.bybit.com/app/proof-of-reserves",
    "bitget": "https://www.bitget.com/proof-of-reserves",
    "kraken": "https://www.kraken.com/proof-of-reserves",
    "gate-io": "https://www.gate.io/proof-of-reserves",
    "kucoin": "https://www.kucoin.com/proof-of-reserves",
}


def _automate_cex_years(entity: dict, cg_data: dict | None) -> dict:
    """Automate years_in_operation from CoinGecko year_established."""
    automated = {}
    if cg_data:
        year = cg_data.get("year_established")
        if year and isinstance(year, int) and year > 2000:
            automated["years_in_operation"] = datetime.now().year - year
    return automated


def _automate_cex_hacks(entity: dict, static: dict, hacks_cache: list = None) -> dict:
    """Automate security_breach_count and withdrawal_freeze_count from DeFiLlama hacks."""
    automated = {}
    slug = entity["slug"]

    try:
        from app.collectors.defillama import fetch_defillama_hacks, filter_hacks_by_name
        hacks = hacks_cache if hacks_cache is not None else fetch_defillama_hacks()
        name = entity.get("name", "")
        matched = filter_hacks_by_name(hacks, name)
        if not matched:
            matched = filter_hacks_by_name(hacks, slug.split("-")[0])

        # security_breach_count: count of confirmed hacks/exploits
        live_count = len(matched)
        static_count = static.get("security_breach_count", 0)
        automated["security_breach_count"] = max(live_count, static_count)

        # withdrawal_freeze_count: major withdrawal issues often appear as incidents
        # This is an imperfect proxy — supplement with static for known events
        freeze_count = sum(
            1 for h in matched
            if any(kw in (h.get("name") or "").lower()
                   for kw in ["freeze", "withdrawal", "halt", "suspend"])
        )
        static_freeze = static.get("withdrawal_freeze_count", 0)
        automated["withdrawal_freeze_count"] = max(freeze_count, static_freeze)
    except Exception as e:
        logger.warning(f"CXRI hacks automation failed for {slug}: {e}")

    return automated


def _automate_cex_api_availability(entity: dict, static: dict) -> dict:
    """Check exchange API availability with a simple health check."""
    automated = {}
    slug = entity["slug"]
    endpoint = CEX_API_ENDPOINTS.get(slug)
    if not endpoint:
        return automated

    try:
        resp = requests.get(endpoint, timeout=10)
        if resp.status_code == 200:
            automated["api_availability"] = 95  # API responding
        else:
            automated["api_availability"] = 60  # API returned non-200
    except requests.exceptions.Timeout:
        automated["api_availability"] = 40  # timeout
    except Exception:
        automated["api_availability"] = min(50, static.get("api_availability", 50))

    return automated


def _automate_cex_reserve_dashboard(entity: dict, static: dict) -> dict:
    """Check if exchange has a live reserve dashboard (HEAD request)."""
    automated = {}
    slug = entity["slug"]
    url = CEX_RESERVE_URLS.get(slug)
    if not url:
        return automated

    try:
        resp = requests.head(url, timeout=10, allow_redirects=True)
        if resp.status_code == 200:
            automated["realtime_reserve_dashboard"] = 80  # dashboard exists
        else:
            automated["realtime_reserve_dashboard"] = 20
    except Exception:
        # Keep static value on failure
        pass

    return automated


def extract_cex_raw_values(entity: dict, hacks_cache: list = None) -> dict:
    """Extract raw values from CoinGecko + static config + live automation."""
    slug = entity["slug"]
    raw = {}

    # CoinGecko exchange data
    cg_id = entity.get("coingecko_id")
    cg_data = None
    if cg_id:
        cg_data = fetch_exchange_data(cg_id)
        if cg_data:
            # Trust score
            trust = cg_data.get("trust_score")
            if trust:
                raw["reserve_asset_diversity"] = trust * 10  # trust_score is 1-10

            # Trade volume 24h (BTC) as proxy for activity
            vol_btc = cg_data.get("trade_volume_24h_btc")
            if vol_btc:
                # Very rough: known wallet balance proxy (exchange with more volume = larger reserves)
                raw["known_wallet_balance"] = vol_btc * 60000  # approximate BTC price

    # Static config (applied first)
    static = CEX_STATIC_CONFIG.get(slug, {})
    raw.update(static)

    # --- Phase 1 automation: override static with live data ---
    # years_in_operation from CoinGecko (live > static)
    years_automated = _automate_cex_years(entity, cg_data)
    raw.update(years_automated)

    # security_breach_count and withdrawal_freeze_count from DeFiLlama hacks
    hacks_automated = _automate_cex_hacks(entity, static, hacks_cache)
    raw.update(hacks_automated)

    # API availability from direct health check
    api_automated = _automate_cex_api_availability(entity, static)
    raw.update(api_automated)

    # Reserve dashboard from URL check
    dashboard_automated = _automate_cex_reserve_dashboard(entity, static)
    raw.update(dashboard_automated)

    # --- Phase 3B: Regulatory registry checks ---
    try:
        from app.collectors.regulatory_scraper import check_exchange_regulatory
        reg_scores = check_exchange_regulatory(slug)
        if reg_scores:
            for comp_id, live_score in reg_scores.items():
                static_score = static.get(comp_id, 0)
                raw[comp_id] = max(live_score, static_score)
    except Exception as e:
        logger.debug(f"CXRI regulatory check failed for {slug}: {e}")

    return raw


# =============================================================================
# Score and store
# =============================================================================

def score_cex(entity: dict, hacks_cache: list = None) -> dict | None:
    """Score a single CEX entity."""
    slug = entity["slug"]
    logger.info(f"Scoring CEX: {slug}")

    raw_values = extract_cex_raw_values(entity, hacks_cache=hacks_cache)
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
        "cxri", slug, result["entity_name"], result["overall_score"],
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


def run_cxri_scoring() -> list[dict]:
    """Score all CEX entities. Called from worker."""
    # Pre-fetch DeFiLlama hacks data (cached 24h, shared across all entities)
    hacks_cache = []
    try:
        from app.collectors.defillama import fetch_defillama_hacks
        hacks_cache = fetch_defillama_hacks()
    except Exception as e:
        logger.warning(f"CXRI hacks pre-fetch failed: {e}")

    results = []
    for entity in CEX_ENTITIES:
        try:
            result = score_cex(entity, hacks_cache=hacks_cache)
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
    except Exception as e:
        logger.warning(f"CXRI attestation failed: {e}")

    return results
