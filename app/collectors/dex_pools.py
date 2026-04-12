"""
DEX Pool Collector (CoinGecko On-chain / GeckoTerminal)
=========================================================
Uses CoinGecko's on-chain DEX API to get pool-level data for
PSI protocol scoring: TVL, volume, OHLCV trends, token diversity.

Components produced:
  - position_liquidity:     TVL + volume stability composite
  - collateral_diversity:   Token diversity across pools

Data source: CoinGecko Pro on-chain API (GeckoTerminal)
"""

import os
import json
import hashlib
import logging
import time
from datetime import datetime, timezone

import requests

from app.database import execute, fetch_all, fetch_one
from app.index_definitions.psi_v01 import TARGET_PROTOCOLS

logger = logging.getLogger(__name__)

COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
ONCHAIN_BASE = "https://pro-api.coingecko.com/api/v3/onchain" if COINGECKO_API_KEY else ""

RATE_LIMIT_DELAY = 0.5  # Conservative: 500ms between calls


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if COINGECKO_API_KEY:
        h["x-cg-pro-api-key"] = COINGECKO_API_KEY
    return h


# Protocol slug -> GeckoTerminal DEX/network mapping
PROTOCOL_DEX_MAP = {
    "aave": {"network": "eth", "search_name": "aave"},
    "lido": {"network": "eth", "search_name": "lido stETH"},
    "eigenlayer": {"network": "eth", "search_name": "eigenlayer"},
    "sky": {"network": "eth", "search_name": "maker DAI"},
    "compound-finance": {"network": "eth", "search_name": "compound"},
    "uniswap": {"network": "eth", "search_name": "uniswap v3"},
    "curve-finance": {"network": "eth", "search_name": "curve"},
    "morpho": {"network": "eth", "search_name": "morpho"},
    "spark": {"network": "eth", "search_name": "spark"},
    "convex-finance": {"network": "eth", "search_name": "convex"},
    "drift": {"network": "solana", "search_name": "drift"},
    "jupiter-perpetual-exchange": {"network": "solana", "search_name": "jupiter"},
    "raydium": {"network": "solana", "search_name": "raydium"},
}


# =============================================================================
# API functions
# =============================================================================

def get_protocol_pools(protocol_slug: str, network: str = "eth") -> list:
    """
    Search for protocol pools via GeckoTerminal search endpoint.
    Returns list of pool objects with address, token pair, TVL, volume.
    """
    if not ONCHAIN_BASE:
        return []
    try:
        search_name = PROTOCOL_DEX_MAP.get(protocol_slug, {}).get("search_name", protocol_slug)
        resp = requests.get(
            f"{ONCHAIN_BASE}/search/pools",
            params={"query": search_name, "network": network},
            headers=_headers(),
            timeout=15,
        )
        time.sleep(RATE_LIMIT_DELAY)
        if resp.status_code != 200:
            logger.debug(f"Pool search failed for {protocol_slug}: HTTP {resp.status_code}")
            return []

        data = resp.json()
        pools = []
        for pool in data.get("data", [])[:10]:  # Top 10 pools
            attrs = pool.get("attributes", {})
            pools.append({
                "address": attrs.get("address", ""),
                "name": attrs.get("name", ""),
                "reserve_in_usd": float(attrs.get("reserve_in_usd", 0) or 0),
                "volume_24h": float(attrs.get("volume_usd", {}).get("h24", 0) or 0),
                "fee_tier": attrs.get("pool_fee", None),
                "network": network,
            })
        return pools
    except Exception as e:
        logger.debug(f"get_protocol_pools failed for {protocol_slug}: {e}")
        return []


def get_pool_ohlcv(network: str, pool_address: str, timeframe: str = "day", days: int = 30) -> list:
    """
    Get OHLCV history for a pool.
    Returns list of [timestamp, open, high, low, close, volume] arrays.
    """
    if not ONCHAIN_BASE or not pool_address:
        return []
    try:
        resp = requests.get(
            f"{ONCHAIN_BASE}/networks/{network}/pools/{pool_address}/ohlcv/{timeframe}",
            params={"aggregate": 1, "limit": days},
            headers=_headers(),
            timeout=15,
        )
        time.sleep(RATE_LIMIT_DELAY)
        if resp.status_code != 200:
            return []

        data = resp.json()
        return data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
    except Exception as e:
        logger.debug(f"get_pool_ohlcv failed for {pool_address}: {e}")
        return []


def get_pool_tokens(network: str, pool_address: str) -> dict:
    """
    Get pool token composition (base_token, quote_token, reserves).
    """
    if not ONCHAIN_BASE or not pool_address:
        return {}
    try:
        resp = requests.get(
            f"{ONCHAIN_BASE}/networks/{network}/pools/{pool_address}",
            headers=_headers(),
            timeout=15,
        )
        time.sleep(RATE_LIMIT_DELAY)
        if resp.status_code != 200:
            return {}

        data = resp.json()
        attrs = data.get("data", {}).get("attributes", {})
        relationships = data.get("data", {}).get("relationships", {})

        return {
            "base_token": relationships.get("base_token", {}).get("data", {}).get("id", ""),
            "quote_token": relationships.get("quote_token", {}).get("data", {}).get("id", ""),
            "reserve_in_usd": float(attrs.get("reserve_in_usd", 0) or 0),
        }
    except Exception as e:
        logger.debug(f"get_pool_tokens failed for {pool_address}: {e}")
        return {}


# =============================================================================
# Composite computation functions
# =============================================================================

def compute_position_liquidity(protocol_slug: str) -> dict:
    """
    Compute position liquidity composite for a protocol.
    Returns {"current_tvl": X, "volume_24h": Y, "tvl_30d_trend": Z, "volume_stability": W, "score": S}.
    """
    try:
        config = PROTOCOL_DEX_MAP.get(protocol_slug)
        if not config:
            return {}

        network = config["network"]
        pools = get_protocol_pools(protocol_slug, network)
        if not pools:
            return {}

        total_tvl = sum(p.get("reserve_in_usd", 0) for p in pools)
        total_volume = sum(p.get("volume_24h", 0) for p in pools)

        # Get 30-day OHLCV for the largest pool to measure trend
        tvl_trend = 0.0
        volume_stability = 0.5
        if pools:
            largest_pool = max(pools, key=lambda p: p.get("reserve_in_usd", 0))
            if largest_pool.get("address"):
                ohlcv = get_pool_ohlcv(network, largest_pool["address"], "day", 30)
                if ohlcv and len(ohlcv) >= 7:
                    # TVL trend: compare last 7d avg to first 7d avg
                    recent_volumes = [float(c[5]) for c in ohlcv[:7] if len(c) > 5]
                    older_volumes = [float(c[5]) for c in ohlcv[-7:] if len(c) > 5]
                    if recent_volumes and older_volumes:
                        recent_avg = sum(recent_volumes) / len(recent_volumes)
                        older_avg = sum(older_volumes) / len(older_volumes)
                        if older_avg > 0:
                            tvl_trend = (recent_avg - older_avg) / older_avg

                    # Volume stability: coefficient of variation (lower = more stable)
                    all_volumes = [float(c[5]) for c in ohlcv if len(c) > 5 and float(c[5]) > 0]
                    if len(all_volumes) >= 5:
                        avg_vol = sum(all_volumes) / len(all_volumes)
                        if avg_vol > 0:
                            std_vol = (sum((v - avg_vol) ** 2 for v in all_volumes) / len(all_volumes)) ** 0.5
                            cv = std_vol / avg_vol
                            volume_stability = max(0, 1 - cv)  # 0 to 1, higher = more stable

        # Normalize: high TVL + stable volume = higher score
        tvl_score = min(100, max(0, 20 + 80 * min(1, total_tvl / 1e9)))  # $1B = 100
        vol_score = min(100, max(0, 20 + 80 * min(1, total_volume / 1e8)))  # $100M = 100
        stability_score = volume_stability * 100
        score = 0.4 * tvl_score + 0.3 * vol_score + 0.3 * stability_score

        return {
            "current_tvl": total_tvl,
            "volume_24h": total_volume,
            "tvl_30d_trend": round(tvl_trend, 4),
            "volume_stability": round(volume_stability, 4),
            "score": round(score, 2),
        }
    except Exception as e:
        logger.debug(f"compute_position_liquidity failed for {protocol_slug}: {e}")
        return {}


def compute_collateral_diversity(protocol_slug: str) -> dict:
    """
    Compute collateral diversity for a protocol.
    Returns {"unique_tokens": N, "concentration_top3": pct, "has_stablecoin_exposure": bool, "score": S}.
    """
    try:
        config = PROTOCOL_DEX_MAP.get(protocol_slug)
        if not config:
            return {}

        network = config["network"]
        pools = get_protocol_pools(protocol_slug, network)
        if not pools:
            return {}

        # Collect unique tokens from all pools
        token_tvl = {}
        for pool in pools[:5]:  # Limit to top 5 pools to conserve API calls
            if pool.get("address"):
                tokens = get_pool_tokens(network, pool["address"])
                if tokens:
                    for key in ["base_token", "quote_token"]:
                        token_id = tokens.get(key, "")
                        if token_id:
                            token_tvl[token_id] = token_tvl.get(token_id, 0) + tokens.get("reserve_in_usd", 0) / 2

        unique_count = len(token_tvl)
        if unique_count == 0:
            return {"unique_tokens": 0, "concentration_top3": 100, "has_stablecoin_exposure": False, "score": 10.0}

        # Top 3 concentration
        sorted_tvl = sorted(token_tvl.values(), reverse=True)
        total_tvl = sum(sorted_tvl) or 1
        top3_tvl = sum(sorted_tvl[:3])
        concentration_top3 = (top3_tvl / total_tvl) * 100

        # Check stablecoin exposure from existing DB data
        has_stablecoin = False
        try:
            row = fetch_one(
                "SELECT COUNT(*) as cnt FROM protocol_collateral_exposure WHERE protocol_slug = %s AND stablecoin_id IS NOT NULL",
                (protocol_slug,),
            )
            has_stablecoin = row and row.get("cnt", 0) > 0
        except Exception:
            pass

        # Normalize: more diverse + lower concentration = higher score
        diversity_score = min(100, unique_count * 15)  # 7+ tokens = 100
        concentration_score = max(0, 100 - concentration_top3)  # Lower concentration = higher
        score = 0.5 * diversity_score + 0.5 * concentration_score

        return {
            "unique_tokens": unique_count,
            "concentration_top3": round(concentration_top3, 2),
            "has_stablecoin_exposure": has_stablecoin,
            "score": round(score, 2),
        }
    except Exception as e:
        logger.debug(f"compute_collateral_diversity failed for {protocol_slug}: {e}")
        return {}


# =============================================================================
# Store helper
# =============================================================================

def _store_dex_component(entity_slug: str, component_id: str, category: str,
                         raw_value: float, normalized_score: float, raw_data: dict):
    """Store a DEX pool component reading."""
    try:
        execute(
            """
            INSERT INTO generic_index_scores (index_id, entity_slug, entity_name,
                overall_score, category_scores, component_scores, raw_values,
                formula_version, confidence, scored_date)
            VALUES ('dex_pool_data', %s, %s, %s, %s, %s, %s, 'v1.0.0', 'standard', CURRENT_DATE)
            ON CONFLICT (index_id, entity_slug, scored_date)
            DO UPDATE SET
                overall_score = EXCLUDED.overall_score,
                component_scores = EXCLUDED.component_scores,
                raw_values = EXCLUDED.raw_values,
                computed_at = NOW()
            """,
            (
                entity_slug,
                entity_slug,
                normalized_score,
                json.dumps({category: normalized_score}),
                json.dumps({component_id: normalized_score}),
                json.dumps(raw_data),
            ),
        )
    except Exception as e:
        logger.warning(f"Failed to store DEX component {component_id} for {entity_slug}: {e}")


# =============================================================================
# Main runner
# =============================================================================

def run_dex_pool_collection() -> list[dict]:
    """
    Run DEX pool data collection for all PSI-scored protocols.
    Called from worker slow cycle (every 3 hours).
    Returns list of result dicts.
    """
    if not COINGECKO_API_KEY:
        logger.warning("COINGECKO_API_KEY not set, skipping DEX pool collection")
        return []

    results = []
    for slug in TARGET_PROTOCOLS:
        try:
            # Position liquidity
            pl_data = compute_position_liquidity(slug)
            if pl_data and pl_data.get("score") is not None:
                _store_dex_component(
                    slug, "position_liquidity", "liquidity",
                    pl_data["score"], pl_data["score"], pl_data,
                )
                results.append({
                    "protocol_slug": slug,
                    "component": "position_liquidity",
                    "score": pl_data["score"],
                })

            # Collateral diversity
            cd_data = compute_collateral_diversity(slug)
            if cd_data and cd_data.get("score") is not None:
                _store_dex_component(
                    slug, "collateral_diversity", "liquidity",
                    cd_data["score"], cd_data["score"], cd_data,
                )
                results.append({
                    "protocol_slug": slug,
                    "component": "collateral_diversity",
                    "score": cd_data["score"],
                })

            time.sleep(RATE_LIMIT_DELAY)  # Between protocols
        except Exception as e:
            logger.warning(f"DEX pool collection failed for {slug}: {e}")
            results.append({"protocol_slug": slug, "error": str(e)})

    # Attest
    try:
        from app.state_attestation import attest_state
        if results:
            attest_state("dex_pool_data", [
                {"slug": r.get("protocol_slug"), "component": r.get("component"), "score": r.get("score")}
                for r in results if "score" in r
            ])
    except Exception:
        pass

    return results
