"""
Curve Finance Collector
========================
Collects 3pool balance data for DAI, USDC, USDT liquidity scoring.
"""

import logging

import httpx

from app.scoring import normalize_log
from app.data_source_registry import register_data_source

logger = logging.getLogger(__name__)

CURVE_URL = "https://api.curve.finance/v1"
THREEPOOL_ADDRESS = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"


async def fetch_3pool_data(client: httpx.AsyncClient) -> dict:
    """Get 3pool balances and imbalance metrics."""
    register_data_source("api.curve.finance", "/v1/getPools/ethereum/main", "sii_collector",
                         description="3pool balances for SII liquidity scoring")
    try:
        resp = await client.get(f"{CURVE_URL}/getPools/ethereum/main", timeout=15)
        resp.raise_for_status()
        pools = resp.json().get("data", {}).get("poolData", [])
        
        threepool = None
        for pool in pools:
            if pool.get("address", "").lower() == THREEPOOL_ADDRESS.lower():
                threepool = pool
                break
        
        if not threepool:
            return {}
        
        total_usd = float(threepool.get("usdTotal", 0))
        coins = threepool.get("coins", [])
        balances = {}
        
        for coin in coins:
            symbol = coin.get("symbol", "").upper()
            if symbol in ("DAI", "USDC", "USDT"):
                raw = float(coin.get("poolBalance", 0))
                decimals = int(coin.get("decimals", 18))
                token_amount = raw / (10 ** decimals)
                usd_price = float(coin.get("usdPrice", 1.0))
                balances[symbol] = token_amount * usd_price
        
        if not balances:
            return {}
        
        total = total_usd if total_usd > 0 else sum(balances.values())
        ideal = 100 / 3
        
        shares = {}
        deviations = {}
        for sym, bal in balances.items():
            share = (bal / total) * 100 if total > 0 else ideal
            shares[sym] = round(share, 2)
            deviations[sym] = round(share - ideal, 2)
        
        max_dev = max(abs(d) for d in deviations.values()) if deviations else 0
        imbalance_score = sum(abs(d) for d in deviations.values()) / 3
        
        most_overweight = max(deviations, key=lambda k: deviations[k]) if deviations else None
        
        return {
            "balances": balances,
            "total_usd": total,
            "shares": shares,
            "deviations": deviations,
            "imbalance_score": round(imbalance_score, 2),
            "most_overweight": most_overweight,
        }
    except Exception as e:
        logger.error(f"Curve 3pool error: {e}")
        return {}


async def collect_curve_components(
    client: httpx.AsyncClient, stablecoin_id: str
) -> list[dict]:
    """Collect Curve 3pool components for DAI, USDC, USDT."""
    if stablecoin_id.upper() not in ("USDC", "USDT", "DAI"):
        return []
    
    components = []
    data = await fetch_3pool_data(client)
    if not data or not data.get("shares"):
        return components
    
    # 3Pool TVL
    total = data.get("total_usd", 0)
    if total > 0:
        components.append({
            "component_id": "curve_3pool_tvl",
            "category": "liquidity",
            "raw_value": total,
            "normalized_score": round(normalize_log(total, {
                1e6: 20, 1e7: 40, 1e8: 60, 5e8: 80, 1e9: 100
            }), 2),
            "data_source": "curve",
        })
    
    # This coin's share (ideal = 33.33%)
    coin_share = data["shares"].get(stablecoin_id.upper(), 33.33)
    dev_from_ideal = abs(coin_share - 33.33)
    if dev_from_ideal <= 5:
        balance_score = 100
    else:
        balance_score = max(40, 100 - (dev_from_ideal - 5) * 2)
    
    components.append({
        "component_id": "curve_3pool_share",
        "category": "liquidity",
        "raw_value": round(coin_share, 2),
        "normalized_score": round(balance_score, 2),
        "data_source": "curve",
    })
    
    # Pool Health (overall imbalance)
    imb = data.get("imbalance_score", 0)
    if imb <= 5:
        health = 100
    elif imb <= 10:
        health = 90 - ((imb - 5) * 2)
    elif imb <= 20:
        health = 80 - ((imb - 10) * 2)
    else:
        health = max(40, 60 - (imb - 20))
    
    components.append({
        "component_id": "curve_3pool_health",
        "category": "liquidity",
        "raw_value": round(imb, 2),
        "normalized_score": round(health, 2),
        "data_source": "curve",
    })
    
    return components
