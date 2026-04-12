"""
LST Integrity Index Collector
==============================
Collects data for liquid staking token risk scoring.
LSTs are pegged to ETH rather than USD — peg deviation is measured
against ETH price, not $1.00.

Data sources:
- CoinGecko: price, market cap, volume (existing integration)
- DeFiLlama: TVL, pool data via /yields/pools (existing integration)
- Rated.network API: validator performance metrics (free tier)
- beaconcha.in: validator counts, slashing events (free tier)
- Static config: audit status, admin risk, exploit history

Pattern follows flows.py / psi_collector.py — collect → normalize → store → attest.
"""

import json
import hashlib
import logging
import os
import time
from datetime import datetime, timezone

import requests

from app.database import execute, fetch_all, fetch_one
from app.index_definitions.lsti_v01 import LSTI_V01_DEFINITION, LST_ENTITIES
from app.scoring_engine import score_entity

logger = logging.getLogger(__name__)

COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
CG_BASE = "https://pro-api.coingecko.com/api/v3" if COINGECKO_API_KEY else "https://api.coingecko.com/api/v3"
DEFILLAMA_BASE = "https://api.llama.fi"
RATED_BASE = "https://api.rated.network/v0"

# =============================================================================
# Static config for components that require manual assessment
# =============================================================================

LST_STATIC_CONFIG = {
    "lido-steth": {
        "audit_status": 8, "upgradeability_risk": 70, "admin_key_risk": 80,
        "withdrawal_queue_impl": 90, "exploit_history_lst": 100,
        "slashing_insurance": 80, "beacon_chain_dependency": 70, "mev_exposure": 60,
    },
    "lido-wsteth": {
        "audit_status": 8, "upgradeability_risk": 70, "admin_key_risk": 80,
        "withdrawal_queue_impl": 90, "exploit_history_lst": 100,
        "slashing_insurance": 80, "beacon_chain_dependency": 70, "mev_exposure": 60,
    },
    "rocket-pool-reth": {
        "audit_status": 6, "upgradeability_risk": 75, "admin_key_risk": 85,
        "withdrawal_queue_impl": 80, "exploit_history_lst": 100,
        "slashing_insurance": 90, "beacon_chain_dependency": 65, "mev_exposure": 70,
    },
    "coinbase-cbeth": {
        "audit_status": 4, "upgradeability_risk": 60, "admin_key_risk": 70,
        "withdrawal_queue_impl": 75, "exploit_history_lst": 100,
        "slashing_insurance": 60, "beacon_chain_dependency": 70, "mev_exposure": 50,
    },
    "frax-sfrxeth": {
        "audit_status": 5, "upgradeability_risk": 65, "admin_key_risk": 75,
        "withdrawal_queue_impl": 70, "exploit_history_lst": 100,
        "slashing_insurance": 50, "beacon_chain_dependency": 65, "mev_exposure": 55,
    },
    "mantle-meth": {
        "audit_status": 3, "upgradeability_risk": 55, "admin_key_risk": 60,
        "withdrawal_queue_impl": 65, "exploit_history_lst": 100,
        "slashing_insurance": 40, "beacon_chain_dependency": 60, "mev_exposure": 50,
    },
    "swell-sweth": {
        "audit_status": 3, "upgradeability_risk": 55, "admin_key_risk": 60,
        "withdrawal_queue_impl": 60, "exploit_history_lst": 100,
        "slashing_insurance": 40, "beacon_chain_dependency": 60, "mev_exposure": 50,
    },
    "etherfi-eeth": {
        "audit_status": 4, "upgradeability_risk": 60, "admin_key_risk": 65,
        "withdrawal_queue_impl": 70, "exploit_history_lst": 100,
        "slashing_insurance": 50, "beacon_chain_dependency": 55, "mev_exposure": 55,
    },
    "etherfi-weeth": {
        "audit_status": 4, "upgradeability_risk": 60, "admin_key_risk": 65,
        "withdrawal_queue_impl": 70, "exploit_history_lst": 100,
        "slashing_insurance": 50, "beacon_chain_dependency": 55, "mev_exposure": 55,
    },
    "kelp-rseth": {
        "audit_status": 3, "upgradeability_risk": 50, "admin_key_risk": 55,
        "withdrawal_queue_impl": 60, "exploit_history_lst": 100,
        "slashing_insurance": 40, "beacon_chain_dependency": 50, "mev_exposure": 60,
    },
}


def _cg_headers() -> dict:
    h = {"Accept": "application/json"}
    if COINGECKO_API_KEY:
        h["x-cg-pro-api-key"] = COINGECKO_API_KEY
    return h


# =============================================================================
# CoinGecko data — price, market cap, volume, peg deviation
# =============================================================================

def fetch_lst_market_data(coingecko_id: str) -> dict | None:
    """Fetch market data for an LST from CoinGecko."""
    time.sleep(1.5)
    try:
        resp = requests.get(
            f"{CG_BASE}/coins/{coingecko_id}",
            params={"localization": "false", "tickers": "true", "market_data": "true",
                    "community_data": "false", "developer_data": "false"},
            headers=_cg_headers(),
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.debug(f"CoinGecko LST fetch failed for {coingecko_id}: {e}")
    return None


def fetch_eth_price() -> float | None:
    """Fetch current ETH price in USD from CoinGecko."""
    try:
        resp = requests.get(
            f"{CG_BASE}/simple/price",
            params={"ids": "ethereum", "vs_currencies": "usd"},
            headers=_cg_headers(),
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("ethereum", {}).get("usd")
    except Exception as e:
        logger.debug(f"ETH price fetch failed: {e}")
    return None


def extract_peg_and_liquidity(data: dict, eth_price: float) -> dict:
    """Extract peg stability and liquidity components from CoinGecko data."""
    raw = {}
    market = data.get("market_data", {})

    # LST price in USD
    price_usd = market.get("current_price", {}).get("usd")
    # ETH peg deviation: how far the LST/ETH ratio is from expected
    price_eth = market.get("current_price", {}).get("eth")
    if price_eth is not None:
        # For most LSTs, the ETH price should be >= 1.0 (accruing value)
        # Deviation = distance from 1.0 for rebasing tokens, or from fair value
        # stETH should be ~1.0 ETH, rETH should be > 1.0 ETH
        raw["eth_peg_deviation"] = abs(price_eth - 1.0) * 100  # as percentage

    # Volatility as proxy for peg volatility
    vol_7d = market.get("price_change_percentage_7d")
    vol_30d = market.get("price_change_percentage_30d")
    if vol_7d is not None:
        raw["peg_volatility_7d"] = abs(vol_7d)
    if vol_30d is not None:
        raw["peg_volatility_30d"] = abs(vol_30d)

    # Market cap
    mcap = market.get("market_cap", {}).get("usd")
    if mcap:
        raw["market_cap"] = mcap

    # Volume
    vol_24h = market.get("total_volume", {}).get("usd")
    if vol_24h and mcap and mcap > 0:
        raw["volume_cap_ratio"] = vol_24h / mcap

    # Exchange price variance from tickers
    tickers = data.get("tickers", [])
    if tickers and len(tickers) >= 2:
        prices = []
        for t in tickers[:20]:
            last = t.get("converted_last", {}).get("usd")
            if last and last > 0:
                prices.append(last)
        if len(prices) >= 2:
            avg = sum(prices) / len(prices)
            if avg > 0:
                variance = sum((p - avg) ** 2 for p in prices) / len(prices)
                raw["exchange_price_variance"] = (variance ** 0.5 / avg) * 100

        # DEX vs CEX spread
        dex_prices = []
        cex_prices = []
        for t in tickers[:20]:
            last = t.get("converted_last", {}).get("usd")
            if not last:
                continue
            if t.get("market", {}).get("identifier", "").endswith("_dex"):
                dex_prices.append(last)
            else:
                cex_prices.append(last)
        if dex_prices and cex_prices:
            dex_avg = sum(dex_prices) / len(dex_prices)
            cex_avg = sum(cex_prices) / len(cex_prices)
            if cex_avg > 0:
                raw["dex_cex_spread"] = abs(dex_avg - cex_avg) / cex_avg * 100

    return raw


# =============================================================================
# DeFiLlama — pool depth, cross-chain liquidity
# =============================================================================

def fetch_defillama_pool_data(symbol: str) -> dict:
    """Fetch pool data from DeFiLlama yields API for an LST."""
    raw = {}
    try:
        resp = requests.get(f"{DEFILLAMA_BASE}/../yields/pools", timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            pools = data.get("data", [])
            sym_upper = symbol.upper()

            matching = [p for p in pools if sym_upper in (p.get("symbol") or "").upper()]
            if matching:
                total_tvl = sum(p.get("tvlUsd", 0) for p in matching)
                raw["dex_pool_depth"] = total_tvl

                chains = set(p.get("chain", "") for p in matching if p.get("chain"))
                raw["cross_chain_liquidity"] = len(chains)
    except Exception as e:
        logger.debug(f"DeFiLlama pool data failed for {symbol}: {e}")
    return raw


# =============================================================================
# Rated.network — validator performance
# =============================================================================

def fetch_rated_data(protocol_slug: str) -> dict:
    """Fetch validator performance from Rated.network free API."""
    raw = {}
    # Map our slugs to Rated operator names
    rated_operator_map = {
        "lido-steth": "Lido",
        "lido-wsteth": "Lido",
        "rocket-pool-reth": "Rocket Pool",
        "coinbase-cbeth": "Coinbase",
    }
    operator = rated_operator_map.get(protocol_slug)
    if not operator:
        return raw

    try:
        resp = requests.get(
            f"{RATED_BASE}/eth/operators/{operator}/summary",
            headers={"Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            raw["validator_count"] = data.get("validatorCount", 0)
            eff = data.get("avgCorrectness")
            if eff is not None:
                raw["attestation_rate"] = eff * 100  # convert to percentage

            # Operator diversity (approximate from market share)
            # Lower self-share = higher diversity
            share = data.get("networkPenetration")
            if share is not None:
                # 1 - HHI approximation: if single operator has share s, diversity ≈ 1 - s²
                raw["operator_diversity_hhi"] = 1.0 - (share ** 2)
    except Exception as e:
        logger.debug(f"Rated.network fetch failed for {protocol_slug}: {e}")
    return raw


# =============================================================================
# Score and store
# =============================================================================

def score_lst(entity: dict) -> dict | None:
    """Score a single LST entity. Returns scoring result dict."""
    slug = entity["slug"]
    cg_id = entity["coingecko_id"]
    symbol = entity["symbol"]

    logger.info(f"Scoring LST: {slug}")

    # Fetch ETH price
    eth_price = fetch_eth_price() or 3000.0

    # CoinGecko data
    cg_data = fetch_lst_market_data(cg_id)
    raw_values = {}
    if cg_data:
        raw_values.update(extract_peg_and_liquidity(cg_data, eth_price))

    # DeFiLlama pool data
    pool_data = fetch_defillama_pool_data(symbol)
    raw_values.update(pool_data)

    # Rated.network validator data
    rated_data = fetch_rated_data(slug)
    raw_values.update(rated_data)

    # Static config components
    static = LST_STATIC_CONFIG.get(slug, {})
    raw_values.update(static)

    if not raw_values:
        logger.warning(f"No data collected for LST {slug}")
        return None

    # Score using generic engine
    result = score_entity(LSTI_V01_DEFINITION, raw_values)
    result["entity_slug"] = slug
    result["entity_name"] = entity["name"]
    result["symbol"] = symbol
    result["raw_values"] = raw_values

    return result


def store_lst_score(result: dict) -> None:
    """Store an LST score in the generic_index_scores table."""
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
        "lsti",
        slug,
        result["entity_name"],
        result["overall_score"],
        json.dumps(result["category_scores"]),
        json.dumps(result["component_scores"]),
        json.dumps(raw_for_storage, default=str),
        result["version"],
        inputs_hash,
        result.get("confidence", "limited"),
        result.get("confidence_tag"),
    ))


def run_lsti_scoring() -> list[dict]:
    """Score all LST entities. Called from worker."""
    results = []
    for entity in LST_ENTITIES:
        try:
            result = score_lst(entity)
            if result:
                store_lst_score(result)
                results.append(result)
                logger.info(
                    f"  {result['entity_name']}: {result['overall_score']} "
                    f"({result['components_available']}/{result['components_total']} components, "
                    f"confidence={result.get('confidence', '?')})"
                )
        except Exception as e:
            logger.warning(f"LSTI scoring failed for {entity['slug']}: {e}")

    # Attest LSTI scores
    try:
        from app.state_attestation import attest_state
        if results:
            attest_state("lsti_components", [
                {"slug": r["entity_slug"], "score": r["overall_score"]}
                for r in results
            ])
    except Exception:
        pass

    return results
