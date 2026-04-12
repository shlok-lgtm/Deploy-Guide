"""
CoinGecko Collector
====================
Collects price, volume, market cap, and ticker data.
Produces peg_stability, liquidity, and market_activity components.
"""

import os
import statistics
import logging
from typing import Any

import httpx

from app.scoring import (
    normalize_inverse_linear, normalize_linear, normalize_log,
)
from app.data_source_registry import register_data_source

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("COINGECKO_API_KEY", "")
BASE_URL = "https://pro-api.coingecko.com/api/v3" if API_KEY else "https://api.coingecko.com/api/v3"


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if API_KEY:
        h["x-cg-pro-api-key"] = API_KEY
    return h


async def fetch_current(client: httpx.AsyncClient, coingecko_id: str) -> dict:
    """Get current coin data including tickers."""
    url = f"{BASE_URL}/coins/{coingecko_id}"
    params = {
        "localization": "false",
        "tickers": "true",
        "market_data": "true",
        "community_data": "false",
        "developer_data": "false",
    }
    register_data_source("pro-api.coingecko.com", f"/api/v3/coins/{coingecko_id}",
                         "sii_collector", description="Current coin data for SII scoring",
                         params_template={"localization": "false", "tickers": "true", "market_data": "true"})
    try:
        resp = await client.get(url, params=params, headers=_headers(), timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"CoinGecko current data error for {coingecko_id}: {e}")
        return {}


async def fetch_price_history(client: httpx.AsyncClient, coingecko_id: str, days: int = 7) -> list[float]:
    """Get historical USD prices for peg analysis."""
    url = f"{BASE_URL}/coins/{coingecko_id}/market_chart"
    params = {"vs_currency": "usd", "days": days}
    register_data_source("pro-api.coingecko.com", f"/api/v3/coins/{coingecko_id}/market_chart",
                         "sii_collector", description="Price history for peg analysis",
                         params_template={"vs_currency": "usd", "days": str(days)})
    try:
        resp = await client.get(url, params=params, headers=_headers(), timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return [p[1] for p in data.get("prices", [])]
    except Exception as e:
        logger.error(f"CoinGecko history error for {coingecko_id} ({days}d): {e}")
        return []


# =============================================================================
# Component extraction — peg stability
# =============================================================================

async def collect_peg_components(
    client: httpx.AsyncClient, coingecko_id: str, stablecoin_id: str
) -> list[dict]:
    """Collect all peg stability components. Returns list of component readings."""
    components = []
    
    current = await fetch_current(client, coingecko_id)
    if not current:
        return components
    
    prices_24h = await fetch_price_history(client, coingecko_id, days=1)
    prices_7d = await fetch_price_history(client, coingecko_id, days=7)
    prices_30d = await fetch_price_history(client, coingecko_id, days=30)
    
    market_data = current.get("market_data", {})
    current_price = market_data.get("current_price", {}).get("usd", 1.0)
    
    # 1. Current Peg Deviation
    deviation = abs(1.0 - current_price) * 100
    components.append({
        "component_id": "peg_current_deviation",
        "category": "peg_stability",
        "raw_value": round(deviation, 4),
        "normalized_score": round(normalize_inverse_linear(deviation, 0, 5), 2),
        "data_source": "coingecko",
    })
    
    # 2. 24h Max Deviation
    if prices_24h:
        max_dev = max(abs(1.0 - p) for p in prices_24h) * 100
        components.append({
            "component_id": "peg_24h_max_deviation",
            "category": "peg_stability",
            "raw_value": round(max_dev, 4),
            "normalized_score": round(normalize_inverse_linear(max_dev, 0, 10), 2),
            "data_source": "coingecko",
        })
    
    # 3. 7-Day Standard Deviation
    if len(prices_7d) > 1:
        stddev = statistics.stdev(prices_7d)
        components.append({
            "component_id": "peg_7d_stddev",
            "category": "peg_stability",
            "raw_value": round(stddev, 6),
            "normalized_score": round(normalize_inverse_linear(stddev, 0, 0.02), 2),
            "data_source": "coingecko",
        })
    
    # 4. 7-Day Min/Max/Range
    if prices_7d:
        peg_min = min(prices_7d)
        peg_max = max(prices_7d)
        min_dev = abs(1.0 - peg_min) * 100
        max_dev_7d = abs(1.0 - peg_max) * 100
        range_pct = (peg_max - peg_min) * 100
        
        components.append({
            "component_id": "peg_7d_min",
            "category": "peg_stability",
            "raw_value": round(peg_min, 6),
            "normalized_score": round(normalize_inverse_linear(min_dev, 0, 5), 2),
            "data_source": "coingecko",
        })
        components.append({
            "component_id": "peg_7d_max",
            "category": "peg_stability",
            "raw_value": round(peg_max, 6),
            "normalized_score": round(normalize_inverse_linear(max_dev_7d, 0, 5), 2),
            "data_source": "coingecko",
        })
        components.append({
            "component_id": "peg_range_7d",
            "category": "peg_stability",
            "raw_value": round(peg_max - peg_min, 6),
            "normalized_score": round(normalize_inverse_linear(range_pct, 0, 2), 2),
            "data_source": "coingecko",
        })
    
    # 5. 30-Day Stability Score
    if prices_30d:
        breaches = sum(1 for p in prices_30d if abs(1.0 - p) > 0.005)
        stability = 1 - (breaches / len(prices_30d))
        components.append({
            "component_id": "peg_30d_stability",
            "category": "peg_stability",
            "raw_value": round(stability, 4),
            "normalized_score": round(stability * 100, 2),
            "data_source": "coingecko",
        })
    
    # 6. Cross-Exchange Variance
    tickers = current.get("tickers", [])
    ticker_prices = [
        t.get("converted_last", {}).get("usd", 1.0)
        for t in tickers[:20]
        if t.get("trust_score") in ("green", "yellow")
    ]
    if len(ticker_prices) >= 3:
        variance = statistics.stdev(ticker_prices)
        components.append({
            "component_id": "cross_exchange_variance",
            "category": "peg_stability",
            "raw_value": round(variance, 6),
            "normalized_score": round(normalize_inverse_linear(variance, 0, 0.01), 2),
            "data_source": "coingecko",
        })
    
    # 7. Depeg Events (30d)
    if prices_30d:
        depeg_events = 0
        in_depeg = False
        for p in prices_30d:
            if abs(1.0 - p) > 0.02:
                if not in_depeg:
                    depeg_events += 1
                    in_depeg = True
            else:
                in_depeg = False
        components.append({
            "component_id": "depeg_events_30d",
            "category": "peg_stability",
            "raw_value": depeg_events,
            "normalized_score": round(normalize_inverse_linear(depeg_events, 0, 5), 2),
            "data_source": "coingecko",
        })
    
    # 8. Max Drawdown (30d)
    if prices_30d:
        max_p = max(prices_30d)
        min_p = min(prices_30d)
        if max_p > 0:
            drawdown = ((max_p - min_p) / max_p) * 100
            components.append({
                "component_id": "max_drawdown_30d",
                "category": "peg_stability",
                "raw_value": round(drawdown, 4),
                "normalized_score": round(normalize_inverse_linear(drawdown, 0, 10), 2),
                "data_source": "coingecko",
            })
    
    # 9. Stress Performance
    if len(prices_7d) > 24:
        rolling_vols = []
        for i in range(len(prices_7d) - 24):
            window = prices_7d[i:i + 24]
            if len(window) > 1:
                rolling_vols.append(statistics.stdev(window))
        if rolling_vols:
            max_vol = max(rolling_vols)
            avg_vol = statistics.mean(rolling_vols)
            stress_ratio = max_vol / avg_vol if avg_vol > 0 else 1
            components.append({
                "component_id": "stress_performance",
                "category": "peg_stability",
                "raw_value": round(stress_ratio, 4),
                "normalized_score": round(max(0, 100 - (stress_ratio - 1) * 20), 2),
                "data_source": "coingecko",
            })
    
    # 10. Arbitrage Efficiency
    if len(ticker_prices) >= 5:
        price_range = max(ticker_prices) - min(ticker_prices)
        arb_eff = max(0, 100 - (price_range * 10000))
        components.append({
            "component_id": "arbitrage_efficiency",
            "category": "peg_stability",
            "raw_value": round(price_range, 6),
            "normalized_score": round(arb_eff, 2),
            "data_source": "coingecko",
        })
    
    return components


# =============================================================================
# Liquidity components (from CoinGecko data)
# =============================================================================

async def collect_liquidity_components(
    client: httpx.AsyncClient, coingecko_id: str, stablecoin_id: str
) -> list[dict]:
    """Collect liquidity components from CoinGecko (market cap, volume, tickers)."""
    components = []
    
    current = await fetch_current(client, coingecko_id)
    if not current:
        return components
    
    md = current.get("market_data", {})
    
    # Market Cap
    market_cap = md.get("market_cap", {}).get("usd", 0)
    components.append({
        "component_id": "market_cap",
        "category": "liquidity",
        "raw_value": market_cap,
        "normalized_score": round(normalize_log(market_cap, {
            1e6: 10, 1e8: 40, 1e9: 60, 1e10: 80, 1e11: 100
        }), 2),
        "data_source": "coingecko",
    })
    
    # 24h Volume
    volume = md.get("total_volume", {}).get("usd", 0)
    components.append({
        "component_id": "volume_24h",
        "category": "liquidity",
        "raw_value": volume,
        "normalized_score": round(normalize_log(volume, {
            1e6: 20, 1e7: 40, 1e8: 60, 1e9: 80, 1e10: 100
        }), 2),
        "data_source": "coingecko",
    })
    
    # Volume/MCap Ratio
    if market_cap > 0:
        ratio = volume / market_cap
        components.append({
            "component_id": "volume_mcap_ratio",
            "category": "liquidity",
            "raw_value": round(ratio, 4),
            "normalized_score": round(min(100, normalize_linear(ratio, 0.01, 0.15)), 2),
            "data_source": "coingecko",
        })
    
    # Circulating/Total Ratio
    circ = md.get("circulating_supply", 0)
    total = md.get("total_supply", 0)
    if total > 0:
        circ_ratio = circ / total
        components.append({
            "component_id": "circulating_ratio",
            "category": "liquidity",
            "raw_value": round(circ_ratio, 4),
            "normalized_score": round(circ_ratio * 100, 2),
            "data_source": "coingecko",
        })
    
    # Ticker-derived metrics
    tickers = current.get("tickers", [])
    if tickers:
        # CEX/DEX listing counts
        cex_set, dex_set = set(), set()
        dex_markers = ["uniswap", "curve", "sushiswap", "balancer", "pancake", "1inch", "dex", "swap"]
        for t in tickers:
            mid = t.get("market", {}).get("identifier", "")
            if any(m in mid.lower() for m in dex_markers):
                dex_set.add(mid)
            else:
                cex_set.add(mid)
        
        components.append({
            "component_id": "cex_listing_count",
            "category": "liquidity",
            "raw_value": len(cex_set),
            "normalized_score": round(normalize_log(len(cex_set), {
                5: 30, 20: 50, 50: 70, 100: 85, 200: 100
            }), 2),
            "data_source": "coingecko",
        })
        components.append({
            "component_id": "dex_pool_count",
            "category": "liquidity",
            "raw_value": len(dex_set),
            "normalized_score": round(normalize_log(len(dex_set) + 1, {
                1: 20, 3: 40, 5: 60, 10: 80, 20: 100
            }), 2),
            "data_source": "coingecko",
        })
        
        # Bid-Ask Spread
        spreads = [t.get("bid_ask_spread_percentage", 0) for t in tickers[:20]
                   if 0 < (t.get("bid_ask_spread_percentage") or 0) < 5]
        if spreads:
            avg_spread = statistics.mean(spreads)
            components.append({
                "component_id": "avg_bid_ask_spread",
                "category": "liquidity",
                "raw_value": round(avg_spread, 4),
                "normalized_score": round(normalize_inverse_linear(avg_spread, 0, 1), 2),
                "data_source": "coingecko",
            })
        
        # Volume Concentration
        volumes = sorted(
            [t.get("converted_volume", {}).get("usd", 0) for t in tickers if t.get("converted_volume", {}).get("usd", 0) > 0],
            reverse=True
        )
        if len(volumes) >= 3:
            total_vol = sum(volumes)
            if total_vol > 0:
                concentration = sum(volumes[:3]) / total_vol
                components.append({
                    "component_id": "volume_concentration",
                    "category": "liquidity",
                    "raw_value": round(concentration, 4),
                    "normalized_score": round(normalize_inverse_linear(concentration, 0.3, 0.9), 2),
                    "data_source": "coingecko",
                })
    
    return components


# =============================================================================
# Market activity (mint/burn proxy) components
# =============================================================================

async def collect_market_activity_components(
    client: httpx.AsyncClient, coingecko_id: str, stablecoin_id: str
) -> list[dict]:
    """Collect market activity / mint-burn proxy components."""
    components = []
    
    current = await fetch_current(client, coingecko_id)
    if not current:
        return components
    
    md = current.get("market_data", {})
    
    # 24h Price Stability
    change_24h = abs(md.get("price_change_percentage_24h", 0) or 0)
    components.append({
        "component_id": "price_stability_24h",
        "category": "market_activity",
        "raw_value": round(change_24h, 4),
        "normalized_score": round(normalize_inverse_linear(change_24h, 0, 5), 2),
        "data_source": "coingecko",
    })
    
    # 7d Price Stability
    change_7d = abs(md.get("price_change_percentage_7d", 0) or 0)
    components.append({
        "component_id": "price_stability_7d",
        "category": "market_activity",
        "raw_value": round(change_7d, 4),
        "normalized_score": round(normalize_inverse_linear(change_7d, 0, 10), 2),
        "data_source": "coingecko",
    })
    
    # Exchange coverage
    tickers = current.get("tickers", [])
    exchange_count = len(set(t.get("market", {}).get("identifier", "") for t in tickers))
    components.append({
        "component_id": "exchange_count",
        "category": "market_activity",
        "raw_value": exchange_count,
        "normalized_score": round(normalize_log(exchange_count, {
            5: 30, 20: 50, 50: 70, 100: 85, 200: 100
        }), 2),
        "data_source": "coingecko",
    })
    
    # Trust Score Ratio
    if tickers:
        green = sum(1 for t in tickers if t.get("trust_score") == "green")
        trust_ratio = green / len(tickers)
        components.append({
            "component_id": "exchange_trust_ratio",
            "category": "market_activity",
            "raw_value": round(trust_ratio, 4),
            "normalized_score": round(trust_ratio * 100, 2),
            "data_source": "coingecko",
        })
    
    # Stablecoin Market Share
    market_cap = md.get("market_cap", {}).get("usd", 0)
    if market_cap > 0:
        share = (market_cap / 150_000_000_000) * 100  # ~$150B total stable market
        components.append({
            "component_id": "stablecoin_market_share",
            "category": "market_activity",
            "raw_value": round(share, 4),
            "normalized_score": round(normalize_log(share, {
                0.1: 20, 1: 40, 5: 60, 20: 80, 50: 100
            }), 2),
            "data_source": "coingecko",
        })
    
    # Daily Turnover Ratio
    volume = md.get("total_volume", {}).get("usd", 0)
    if market_cap > 0 and volume > 0:
        turnover = (volume / market_cap) * 100
        if 5 <= turnover <= 20:
            score = 100
        elif turnover < 5:
            score = turnover * 20
        else:
            score = max(50, 100 - (turnover - 20) * 2)
        components.append({
            "component_id": "daily_turnover_ratio",
            "category": "market_activity",
            "raw_value": round(turnover, 4),
            "normalized_score": round(score, 2),
            "data_source": "coingecko",
        })
    
    # MCap 24h Change
    mcap_change = md.get("market_cap_change_percentage_24h", 0) or 0
    if mcap_change >= 0:
        mcap_score = min(100, 70 + mcap_change * 3)
    else:
        mcap_score = max(30, 70 + mcap_change * 2)
    components.append({
        "component_id": "mcap_change_24h",
        "category": "market_activity",
        "raw_value": round(mcap_change, 2),
        "normalized_score": round(mcap_score, 2),
        "data_source": "coingecko",
    })
    
    # Trading Pair Diversity
    if tickers:
        quotes = set(t.get("target", "").upper() for t in tickers if t.get("target"))
        components.append({
            "component_id": "trading_pair_diversity",
            "category": "market_activity",
            "raw_value": len(quotes),
            "normalized_score": round(normalize_log(len(quotes), {
                2: 20, 5: 40, 10: 60, 20: 80, 50: 100
            }), 2),
            "data_source": "coingecko",
        })
    
    return components


def extract_price_context(current_data: dict) -> dict:
    """Extract current price, market_cap, volume for the scores table."""
    md = current_data.get("market_data", {})
    return {
        "current_price": md.get("current_price", {}).get("usd"),
        "market_cap": int(md.get("market_cap", {}).get("usd", 0)) or None,
        "volume_24h": int(md.get("total_volume", {}).get("usd", 0)) or None,
    }
