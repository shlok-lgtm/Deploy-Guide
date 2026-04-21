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
        # exploit_history_lst lowered 100 -> 10 per audits/lsti_rseth_audit_2026-04-20.md:
        # 2026-04-18 LayerZero-bridge mint of 116,500 unbacked rsETH (~$292M).
        # Severity band ($100M+) = 10, recency <90d = full weight. Held as static
        # floor until DeFiLlama hacks ingestion confirms (min(live, static) keeps it low).
        "withdrawal_queue_impl": 60, "exploit_history_lst": 10,
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

def fetch_defillama_all_pools() -> list:
    """Fetch all pools from DeFiLlama yields API (single bulk call)."""
    try:
        resp = requests.get("https://yields.llama.fi/pools", timeout=30)
        if resp.status_code == 200:
            return resp.json().get("data", [])
    except Exception as e:
        logger.debug(f"DeFiLlama bulk pool fetch failed: {e}")
    return []


def _extract_pool_data_from_cache(symbol: str, all_pools: list) -> dict:
    """Extract pool data for a symbol from pre-fetched pool cache."""
    raw = {}
    sym_upper = symbol.upper()
    matching = [p for p in all_pools if sym_upper in (p.get("symbol") or "").upper()]
    if matching:
        total_tvl = sum(p.get("tvlUsd", 0) for p in matching)
        raw["dex_pool_depth"] = total_tvl
        chains = set(p.get("chain", "") for p in matching if p.get("chain"))
        raw["cross_chain_liquidity"] = len(chains)
    return raw


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

def _automate_lst_smart_contract(entity: dict, static: dict) -> dict:
    """Automate smart contract components using live Etherscan analysis.

    Replaces static values for: audit_status, admin_key_risk, upgradeability_risk.
    Falls back to static values on failure.
    """
    automated = {}
    contract = entity.get("contract")
    if not contract:
        return automated

    try:
        from app.collectors.smart_contract import analyze_contract_for_index_sync
        analysis = analyze_contract_for_index_sync(contract)

        # audit_status: Etherscan verification check
        # LSTI uses log normalization with thresholds {1:30, 2:50, 3:70, 5:85, 10:100}
        # The raw_value is a count of audits. We use verification as a base signal.
        static_audit = static.get("audit_status", 1)
        if analysis.get("audit_verified"):
            # Verified contract: base score of 5 (proxy detected + impl verified = 7)
            live_audit = 5
            if analysis.get("is_proxy") and analysis.get("implementation_verified"):
                live_audit = 7
            # Use max of live check and static (static may reflect manual audit count)
            automated["audit_status"] = max(live_audit, static_audit)
        else:
            automated["audit_status"] = static_audit

        # admin_key_risk: from live contract analysis (0-100 scale, direct normalization)
        live_admin = analysis.get("admin_key_risk", 50)
        static_admin = static.get("admin_key_risk", 50)
        automated["admin_key_risk"] = max(live_admin, static_admin)

        # upgradeability_risk: from proxy pattern detection (0-100 scale, direct normalization)
        live_upgrade = analysis.get("upgradeability_risk", 50)
        static_upgrade = static.get("upgradeability_risk", 50)
        automated["upgradeability_risk"] = max(live_upgrade, static_upgrade)

        logger.info(
            f"LST smart contract automation {entity['slug']}: "
            f"audit={automated.get('audit_status')} "
            f"admin={automated.get('admin_key_risk')} "
            f"upgrade={automated.get('upgradeability_risk')}"
        )
    except Exception as e:
        logger.warning(f"LST smart contract automation failed for {entity['slug']}: {e}")

    return automated


def _automate_lst_exploit_history(entity: dict, static: dict, hacks_cache: list = None) -> dict:
    """Automate exploit_history_lst from DeFiLlama hacks data.

    Falls back to static value on failure.
    """
    automated = {}
    protocol_name = entity.get("name", "")
    slug = entity["slug"]

    try:
        from app.collectors.defillama import (
            fetch_defillama_hacks, filter_hacks_by_name, score_exploit_history_from_hacks,
        )
        hacks = hacks_cache if hacks_cache is not None else fetch_defillama_hacks()
        # Match by protocol name and slug
        matched = filter_hacks_by_name(hacks, protocol_name)
        if not matched:
            matched = filter_hacks_by_name(hacks, slug.split("-")[0])  # try first part e.g. "lido"

        live_score = score_exploit_history_from_hacks(matched)
        static_score = static.get("exploit_history_lst", 100)
        # Use the lower score (more conservative — if either source found an exploit, reflect it)
        automated["exploit_history_lst"] = min(live_score, static_score)
    except Exception as e:
        logger.warning(f"LST exploit history automation failed for {slug}: {e}")

    return automated


def _automate_lst_withdrawal_queue(entity: dict, static: dict) -> dict:
    """Automate withdrawal_queue_impl using DeFiLlama protocol detail.

    For Lido: checks currentChainTvls to see if withdrawal is active.
    Uses protocol TVL trend + chain coverage as operational maturity signal.
    Falls back to static value as floor.
    """
    automated = {}
    slug = entity["slug"]

    # Only attempt for protocols with DeFiLlama protocol listings
    protocol_map = {
        "lido-steth": "lido", "lido-wsteth": "lido",
        "rocket-pool-reth": "rocket-pool",
        "coinbase-cbeth": "coinbase-wrapped-staked-eth",
        "frax-sfrxeth": "frax-ether",
        "mantle-meth": "mantle-staked-ether",
        "swell-sweth": "swell",
        "etherfi-eeth": "ether.fi-stake",
        "etherfi-weeth": "ether.fi-stake",
        "kelp-rseth": "kelp-dao",
    }
    defillama_slug = protocol_map.get(slug)
    if not defillama_slug:
        return automated

    try:
        from app.collectors.defillama import fetch_defillama_protocol_detail
        data = fetch_defillama_protocol_detail(defillama_slug)
        if not data:
            return automated

        # Check chain TVLs — more chains with active TVL = more operational maturity
        chain_tvls = data.get("currentChainTvls", {})
        active_chains = sum(1 for v in chain_tvls.values() if isinstance(v, (int, float)) and v > 0)

        # TVL history — check if TVL has been stable/growing (operational maturity)
        tvl_history = data.get("tvl", [])
        if isinstance(tvl_history, list) and len(tvl_history) >= 30:
            recent = tvl_history[-1].get("totalLiquidityUSD", 0) if isinstance(tvl_history[-1], dict) else 0
            month_ago = tvl_history[-30].get("totalLiquidityUSD", 0) if isinstance(tvl_history[-30], dict) else 0
            if recent > 0 and month_ago > 0:
                growth = (recent - month_ago) / month_ago
                # Stable or growing TVL + multiple chains = good withdrawal implementation
                if active_chains >= 3 and growth >= -0.1:
                    live_score = 85
                elif active_chains >= 2 and growth >= -0.2:
                    live_score = 75
                else:
                    live_score = 60

                static_floor = static.get("withdrawal_queue_impl", 50)
                automated["withdrawal_queue_impl"] = max(live_score, static_floor)
    except Exception as e:
        logger.warning(f"LST withdrawal queue automation failed for {slug}: {e}")

    return automated


def score_lst(entity: dict, eth_price: float = None, pool_cache: list = None,
              holder_cache: dict = None, hacks_cache: list = None) -> dict | None:
    """Score a single LST entity. Returns scoring result dict.

    Args:
        entity: LST entity config dict
        eth_price: Pre-fetched ETH price (avoids per-entity API call)
        pool_cache: Pre-fetched DeFiLlama pools (avoids per-entity API call)
        holder_cache: Pre-fetched holder analysis {contract_lower: result}
        hacks_cache: Pre-fetched DeFiLlama hacks data (avoids per-entity API call)
    """
    slug = entity["slug"]
    cg_id = entity["coingecko_id"]
    symbol = entity["symbol"]

    logger.info(f"Scoring LST: {slug}")

    if eth_price is None:
        eth_price = fetch_eth_price() or 3000.0

    # CoinGecko data
    cg_data = fetch_lst_market_data(cg_id)
    raw_values = {}
    if cg_data:
        raw_values.update(extract_peg_and_liquidity(cg_data, eth_price))

    # DeFiLlama pool data (use cache if provided)
    if pool_cache is not None:
        pool_data = _extract_pool_data_from_cache(symbol, pool_cache)
    else:
        pool_data = fetch_defillama_pool_data(symbol)

    # Rated.network validator data
    rated_data = fetch_rated_data(slug)
    raw_values.update(rated_data)

    # Static config components (applied first, then overridden by live data)
    static = LST_STATIC_CONFIG.get(slug, {})
    raw_values.update(static)

    # --- Phase 1 automation: replace static with live data ---
    # Smart contract analysis (audit_status, admin_key_risk, upgradeability_risk)
    sc_automated = _automate_lst_smart_contract(entity, static)
    raw_values.update(sc_automated)

    # Exploit history from DeFiLlama hacks
    exploit_automated = _automate_lst_exploit_history(entity, static, hacks_cache)
    raw_values.update(exploit_automated)

    # Withdrawal queue implementation from DeFiLlama protocol detail
    wq_automated = _automate_lst_withdrawal_queue(entity, static)
    raw_values.update(wq_automated)

    # Holder distribution (Etherscan — daily-gated via 24h cache)
    contract = entity.get("contract")
    if contract and holder_cache is not None:
        holder_data = holder_cache.get(contract.lower())
        if holder_data:
            raw_values["top_holder_concentration"] = holder_data["top_10_pct"]
            raw_values["holder_gini"] = holder_data["gini"]
            raw_values["defi_protocol_share"] = holder_data["defi_pct"]
            raw_values["exchange_concentration"] = holder_data["exchange_pct"]

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
        result.get("component_coverage"),
        result.get("components_populated"),
        result.get("components_total"),
        json.dumps(result.get("missing_categories") or []),
    ))


def run_lsti_scoring() -> list[dict]:
    """Score all LST entities. Called from worker."""
    # Pre-fetch shared data once (saves ~20 redundant API calls)
    eth_price = fetch_eth_price() or 3000.0
    pool_cache = fetch_defillama_all_pools()

    # Pre-fetch DeFiLlama hacks data (cached 24h, shared across all entities)
    hacks_cache = []
    try:
        from app.collectors.defillama import fetch_defillama_hacks
        hacks_cache = fetch_defillama_hacks()
    except Exception as e:
        logger.warning(f"LSTI hacks pre-fetch failed: {e}")

    # Pre-fetch holder data for all entities (cached 24h — only hits Etherscan once/day)
    holder_cache = {}
    try:
        from app.collectors.holder_analysis import analyze_holders_sync, get_cached_holders
        for entity in LST_ENTITIES:
            contract = entity.get("contract")
            if contract:
                cached = get_cached_holders(contract)
                if cached:
                    holder_cache[contract.lower()] = cached
                else:
                    # Fetch fresh — only runs once per 24h due to cache
                    hdata = analyze_holders_sync(contract, decimals=18, market_cap=None)
                    if hdata.get("balances_found", 0) > 0:
                        holder_cache[contract.lower()] = hdata
    except Exception as e:
        logger.warning(f"LSTI holder analysis pre-fetch failed: {e}")

    results = []
    for entity in LST_ENTITIES:
        try:
            result = score_lst(entity, eth_price=eth_price, pool_cache=pool_cache,
                               holder_cache=holder_cache, hacks_cache=hacks_cache)
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
    except Exception as e:
        logger.warning(f"LSTI attestation failed: {e}")

    return results
