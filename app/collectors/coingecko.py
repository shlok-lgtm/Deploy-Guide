"""
CoinGecko Collector
====================
Collects price, volume, market cap, and ticker data.
Produces peg_stability, liquidity, and market_activity components.
"""

import asyncio

import os
import statistics
import logging
import time as _time
from typing import Any

import httpx

from app.scoring import (
    normalize_inverse_linear, normalize_linear, normalize_log,
)
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("COINGECKO_API_KEY", "")
BASE_URL = "https://pro-api.coingecko.com/api/v3" if API_KEY else "https://api.coingecko.com/api/v3"


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if API_KEY:
        h["x-cg-pro-api-key"] = API_KEY
    return h


def _has_trust_signal(ticker: dict) -> bool:
    """Return True iff CoinGecko provided ANY usable trust signal on this ticker.

    Distinguishes "field absent" (signal=None, both keys missing or null) from
    "low trust" (signal=red, or numeric rank>10). The former should NOT count
    toward the denominator of exchange_trust_ratio — we have no information,
    not bad information.

    Signals we consider present:
      - trust_score is a known string value ("green", "yellow", "red")
      - trust_score_rank is a finite numeric value
    """
    ts = ticker.get("trust_score")
    if ts in ("green", "yellow", "red"):
        return True
    rank = ticker.get("trust_score_rank")
    if isinstance(rank, (int, float)):
        return True
    return False


def _is_high_trust(ticker: dict) -> bool:
    """Check if a ticker is from a high-trust exchange.

    CoinGecko historically returned trust_score as a string ("green", "yellow",
    "red").  Modern responses may return ``null`` for that field and instead
    provide a numeric ``trust_score_rank``.  We accept either signal.
    """
    if ticker.get("trust_score") == "green":
        return True
    rank = ticker.get("trust_score_rank")
    if isinstance(rank, (int, float)) and rank <= 10:
        return True
    return False


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
    _t0 = _time.monotonic()
    _status = None
    try:
        resp = await client.get(url, params=params, headers=_headers(), timeout=15)
        _status = resp.status_code
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        if _status is None:
            _status = 0
        logger.error(f"CoinGecko current data error for {coingecko_id}: {e}")
        return {}
    finally:
        try:
            track_api_call(
                provider="coingecko",
                endpoint=f"/coins/{coingecko_id}",
                caller="collectors.coingecko",
                status=_status,
                latency_ms=int((_time.monotonic() - _t0) * 1000),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"fetch current failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="collectors_fetch_current_failure",
                    error_message=str(e)[:500],
                    cycle_phase="coingecko",
                )
            except Exception:
                pass


async def fetch_price_history(client: httpx.AsyncClient, coingecko_id: str, days: int = 7) -> list[float]:
    """Get historical USD prices for peg analysis."""
    url = f"{BASE_URL}/coins/{coingecko_id}/market_chart"
    params = {"vs_currency": "usd", "days": days}
    _t0 = _time.monotonic()
    _status = None
    try:
        resp = await client.get(url, params=params, headers=_headers(), timeout=15)
        _status = resp.status_code
        resp.raise_for_status()
        data = resp.json()
        return [p[1] for p in data.get("prices", [])]
    except Exception as e:
        if _status is None:
            _status = 0
        logger.error(f"CoinGecko history error for {coingecko_id} ({days}d): {e}")
        return []
    finally:
        try:
            track_api_call(
                provider="coingecko",
                endpoint=f"/coins/{coingecko_id}/market_chart",
                caller="collectors.coingecko",
                status=_status,
                latency_ms=int((_time.monotonic() - _t0) * 1000),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"fetch price history failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="collectors_fetch_price_history_failure",
                    error_message=str(e)[:500],
                    cycle_phase="coingecko",
                )
            except Exception:
                pass


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
        or _is_high_trust(t)
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
    
    # Coerce None to 0 for illiquid assets (e.g. stkgho returns null fields)
    market_cap = md.get("market_cap", {}).get("usd") or 0
    volume = md.get("total_volume", {}).get("usd") or 0
    circ = md.get("circulating_supply") or 0
    total = md.get("total_supply") or 0

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
    if market_cap and market_cap > 0 and volume is not None:
        ratio = volume / market_cap
        components.append({
            "component_id": "volume_mcap_ratio",
            "category": "liquidity",
            "raw_value": round(ratio, 4),
            "normalized_score": round(min(100, normalize_linear(ratio, 0.01, 0.15)), 2),
            "data_source": "coingecko",
        })

    # Circulating/Total Ratio
    if total and total > 0 and circ is not None:
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
        
        # Volume Concentration (guard against None values)
        volumes = sorted(
            [
                (t.get("converted_volume") or {}).get("usd") or 0
                for t in tickers
                if ((t.get("converted_volume") or {}).get("usd") or 0) > 0
            ],
            reverse=True,
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
    #
    # CoinGecko's response shape for tickers has drifted: for some assets
    # (notably USDC on 2026-05-11) both `trust_score` and `trust_score_rank`
    # are null on every ticker. The previous implementation divided
    # high_trust by len(tickers) regardless, producing raw=0/normalized=0 —
    # which falsely scored the asset as "no exchange has high trust" when the
    # truth is "CoinGecko didn't tell us." Distinguish those two cases:
    # divide only by tickers that have a usable trust signal, and if NONE do,
    # mark the reading stale instead of emitting a misleading zero.
    if tickers:
        rated = [t for t in tickers if _has_trust_signal(t)]
        if rated:
            high_trust = sum(1 for t in rated if _is_high_trust(t))
            trust_ratio = high_trust / len(rated)
            components.append({
                "component_id": "exchange_trust_ratio",
                "category": "market_activity",
                "raw_value": round(trust_ratio, 4),
                "normalized_score": round(trust_ratio * 100, 2),
                "data_source": "coingecko",
            })
        else:
            components.append({
                "component_id": "exchange_trust_ratio",
                "category": "market_activity",
                "raw_value": None,
                "normalized_score": None,
                "data_source": "coingecko",
                "is_stale": True,
                "error_message": (
                    f"CoinGecko returned {len(tickers)} tickers but none "
                    f"carried a trust_score or trust_score_rank — field "
                    f"absent, not zero."
                ),
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
