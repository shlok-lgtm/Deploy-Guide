"""
DeFiLlama Collector
====================
Collects TVL, chain distribution, and lending yield data.
Produces liquidity components.
"""

import logging
from typing import Any

import httpx

from app.scoring import normalize_log

logger = logging.getLogger(__name__)

STABLECOINS_URL = "https://stablecoins.llama.fi"
YIELDS_URL = "https://yields.llama.fi"


async def fetch_stablecoin_data(client: httpx.AsyncClient, coingecko_id: str) -> dict:
    """Get stablecoin data including chain breakdown."""
    try:
        resp = await client.get(f"{STABLECOINS_URL}/stablecoins", timeout=15)
        resp.raise_for_status()
        data = resp.json()
        for stable in data.get("peggedAssets", []):
            if stable.get("gecko_id") == coingecko_id:
                return stable
        return {}
    except Exception as e:
        logger.error(f"DeFiLlama stablecoin error for {coingecko_id}: {e}")
        return {}


async def fetch_lending_yields(client: httpx.AsyncClient, symbol: str) -> dict:
    """Get lending yields from major protocols."""
    try:
        resp = await client.get(f"{YIELDS_URL}/pools", timeout=15)
        resp.raise_for_status()
        pools = resp.json().get("data", [])
        
        major_protocols = ["aave", "compound", "morpho", "spark", "maker", "venus", "benqi", "radiant", "fluid"]
        symbol_upper = symbol.upper()
        
        matching = []
        for pool in pools:
            project = pool.get("project", "").lower()
            if (pool.get("symbol", "").upper() == symbol_upper
                    and any(p in project for p in major_protocols)
                    and pool.get("tvlUsd", 0) > 1_000_000):
                matching.append({
                    "protocol": pool.get("project"),
                    "chain": pool.get("chain"),
                    "tvl": pool.get("tvlUsd", 0),
                    "apy": pool.get("apy", 0),
                })
        
        return {
            "pool_count": len(matching),
            "total_tvl": sum(p["tvl"] for p in matching),
            "avg_apy": sum(p["apy"] for p in matching) / len(matching) if matching else 0,
            "protocols": list(set(p["protocol"] for p in matching)),
        }
    except Exception as e:
        logger.error(f"DeFiLlama yields error for {symbol}: {e}")
        return {"pool_count": 0, "total_tvl": 0, "avg_apy": 0, "protocols": []}


async def collect_defillama_components(
    client: httpx.AsyncClient, coingecko_id: str, stablecoin_id: str
) -> list[dict]:
    """Collect DeFiLlama-sourced liquidity components."""
    components = []
    
    stable_data = await fetch_stablecoin_data(client, coingecko_id)
    
    # Chain Count
    chains = stable_data.get("chains", [])
    chain_count = len(chains) if chains else 1
    components.append({
        "component_id": "chain_count",
        "category": "liquidity",
        "raw_value": chain_count,
        "normalized_score": round(normalize_log(chain_count, {
            1: 20, 3: 40, 5: 60, 10: 80, 20: 100
        }), 2),
        "data_source": "defillama",
    })
    
    # Cross-Chain Liquidity
    chain_tvls = stable_data.get("chainCirculating", {})
    if chain_tvls and isinstance(chain_tvls, dict):
        chain_values = []
        for chain, value in chain_tvls.items():
            if isinstance(value, dict):
                numeric = value.get("current", value.get("circulating", 0))
            else:
                numeric = value
            if isinstance(numeric, (int, float)) and numeric > 0:
                chain_values.append((chain, numeric))
        
        if len(chain_values) > 1:
            total = sum(v for _, v in chain_values)
            eth_val = next((v for c, v in chain_values if c.lower() == "ethereum"), 0)
            non_eth = total - eth_val
            
            components.append({
                "component_id": "cross_chain_liquidity",
                "category": "liquidity",
                "raw_value": non_eth,
                "normalized_score": round(normalize_log(non_eth, {
                    1e6: 20, 1e7: 40, 1e8: 60, 1e9: 80, 1e10: 100
                }), 2),
                "data_source": "defillama",
            })
    
    # Lending TVL & Protocol Integrations
    yields = await fetch_lending_yields(client, stablecoin_id)
    if yields["pool_count"] > 0:
        components.append({
            "component_id": "lending_tvl",
            "category": "liquidity",
            "raw_value": round(yields["total_tvl"], 2),
            "normalized_score": round(normalize_log(yields["total_tvl"], {
                1e6: 20, 1e7: 40, 1e8: 60, 5e8: 80, 1e9: 100
            }), 2),
            "data_source": "defillama",
        })
        components.append({
            "component_id": "defi_protocol_count",
            "category": "liquidity",
            "raw_value": len(yields["protocols"]),
            "normalized_score": round(normalize_log(len(yields["protocols"]), {
                1: 30, 2: 50, 3: 70, 5: 85, 7: 100
            }), 2),
            "data_source": "defillama",
        })
        if yields["avg_apy"] > 0:
            components.append({
                "component_id": "lending_apy",
                "category": "liquidity",
                "raw_value": round(yields["avg_apy"], 2),
                "normalized_score": round(min(100, yields["avg_apy"] * 10), 2),
                "data_source": "defillama",
            })
    
    return components
