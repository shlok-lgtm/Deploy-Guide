"""
DeFiLlama Collector
====================
Collects TVL, chain distribution, and lending yield data.
Produces liquidity components.

Also provides shared utility functions used by Circle 7 collectors:
- fetch_defillama_hacks(): exploit/hack history (24h cache)
- fetch_defillama_treasury(): protocol treasury value
- fetch_defillama_fees(): protocol fee/revenue data
- fetch_defillama_protocol_detail(): full protocol detail
"""

import asyncio

import logging
import time
from typing import Any

import httpx

from app.scoring import normalize_log
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

STABLECOINS_URL = "https://stablecoins.llama.fi"
YIELDS_URL = "https://yields.llama.fi"
LLAMA_BASE = "https://api.llama.fi"

# =============================================================================
# In-memory caches for shared utilities
# =============================================================================

_hacks_cache: dict[str, tuple[float, list]] = {}  # {"hacks": (timestamp, data)}
_treasury_cache: dict[str, tuple[float, dict]] = {}
_fees_cache: dict[str, tuple[float, dict]] = {}
_protocol_cache: dict[str, tuple[float, dict]] = {}

_HACKS_TTL = 86400       # 24 hours
_TREASURY_TTL = 86400    # 24 hours
_FEES_TTL = 86400        # 24 hours
_PROTOCOL_TTL = 86400    # 24 hours


async def fetch_stablecoin_data(client: httpx.AsyncClient, coingecko_id: str) -> dict:
    """Get stablecoin data including chain breakdown."""
    _t0 = time.monotonic()
    _status = None
    try:
        resp = await client.get(f"{STABLECOINS_URL}/stablecoins", timeout=15)
        _status = resp.status_code
        resp.raise_for_status()
        data = resp.json()
        for stable in data.get("peggedAssets", []):
            if stable.get("gecko_id") == coingecko_id:
                return stable
        return {}
    except Exception as e:
        if _status is None:
            _status = 0
        logger.error(f"DeFiLlama stablecoin error for {coingecko_id}: {e}")
        return {}
    finally:
        try:
            track_api_call(
                provider="defillama",
                endpoint="/stablecoins",
                caller="collectors.defillama",
                status=_status,
                latency_ms=int((time.monotonic() - _t0) * 1000),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"fetch stablecoin data failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="collectors_fetch_stablecoin_data_failure",
                    error_message=str(e)[:500],
                    cycle_phase="defillama",
                )
            except Exception:
                pass


async def fetch_lending_yields(client: httpx.AsyncClient, symbol: str) -> dict:
    """Get lending yields from major protocols."""
    _t0 = time.monotonic()
    _status = None
    try:
        resp = await client.get(f"{YIELDS_URL}/pools", timeout=15)
        _status = resp.status_code
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
        if _status is None:
            _status = 0
        logger.error(f"DeFiLlama yields error for {symbol}: {e}")
        return {"pool_count": 0, "total_tvl": 0, "avg_apy": 0, "protocols": []}
    finally:
        try:
            track_api_call(
                provider="defillama",
                endpoint="/pools",
                caller="collectors.defillama",
                status=_status,
                latency_ms=int((time.monotonic() - _t0) * 1000),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"fetch lending yields failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="collectors_fetch_lending_yields_failure",
                    error_message=str(e)[:500],
                    cycle_phase="defillama",
                )
            except Exception:
                pass


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


# =============================================================================
# Shared utilities for Circle 7 collectors
# =============================================================================

def fetch_defillama_hacks() -> list[dict]:
    """Fetch all known exploits/hacks from DeFiLlama.

    Returns list of dicts with keys: name, date, amount, chains, etc.
    Cached for 24h. Used by LSTI, BRI, VSRI, CXRI collectors.
    """
    cached = _hacks_cache.get("hacks")
    if cached and (time.time() - cached[0]) < _HACKS_TTL:
        return cached[1]

    _t0 = time.monotonic()
    _status = None
    try:
        resp = httpx.get(f"{LLAMA_BASE}/hacks", timeout=30)
        _status = resp.status_code
        if resp.status_code == 402:
            # DeFiLlama hacks API paywalled as of April 2026.
            # Circle 7 indices fall back to empty exploit history (score = 100).
            logger.debug("DeFiLlama /hacks returned 402 (paywalled) — using cached or empty")
            if cached:
                return cached[1]
            return []
        resp.raise_for_status()
        data = resp.json()
        hacks = data if isinstance(data, list) else []
        _hacks_cache["hacks"] = (time.time(), hacks)
        logger.info(f"DeFiLlama hacks: fetched {len(hacks)} records")
        return hacks
    except Exception as e:
        if _status is None:
            _status = 0
        logger.warning(f"DeFiLlama hacks fetch failed: {e}")
        if cached:
            return cached[1]
        return []
    finally:
        try:
            track_api_call(
                provider="defillama",
                endpoint="/hacks",
                caller="collectors.defillama",
                status=_status,
                latency_ms=int((time.monotonic() - _t0) * 1000),
            )
        except Exception as e:
            logger.warning(f"fetch defillama hacks failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="collectors_fetch_defillama_hacks_failure",
                    error_message=str(e)[:500],
                    cycle_phase="defillama",
                )
            except Exception:
                pass


def filter_hacks_by_name(hacks: list[dict], name: str) -> list[dict]:
    """Filter hack records matching a protocol/bridge/exchange name.

    Matches case-insensitively against the 'name' and 'project' fields.
    """
    name_lower = name.lower()
    # Also try common variations
    variations = {name_lower, name_lower.replace("-", " "), name_lower.replace(" ", "")}
    matched = []
    for h in hacks:
        h_name = (h.get("name") or "").lower()
        h_project = (h.get("project") or "").lower()
        for v in variations:
            if v in h_name or v in h_project:
                matched.append(h)
                break
    return matched


def score_exploit_history_from_hacks(hacks: list[dict]) -> float:
    """Score exploit history from DeFiLlama hacks data.

    Returns 0-100 score: 100 = no exploits, lower = worse.
    Factors in severity (amount lost) and recency.
    """
    if not hacks:
        return 100.0

    import math
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    worst_score = 100.0

    for h in hacks:
        amount = h.get("amount", 0) or 0
        # Parse date
        date_str = h.get("date")
        if not date_str:
            continue
        try:
            if isinstance(date_str, (int, float)):
                hack_date = datetime.fromtimestamp(date_str, tz=timezone.utc)
            else:
                ds = str(date_str).replace("Z", "+00:00")
                hack_date = datetime.fromisoformat(ds)
                if hack_date.tzinfo is None:
                    hack_date = hack_date.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            hack_date = now  # assume recent if unparseable

        days_ago = max(1, (now - hack_date).days)

        # Base severity from amount: $0 = minor (80), $1M = 50, $10M = 30, $100M+ = 10
        if amount <= 0:
            severity = 80.0
        elif amount < 1_000_000:
            severity = 70.0
        elif amount < 10_000_000:
            severity = 50.0
        elif amount < 100_000_000:
            severity = 30.0
        else:
            severity = 10.0

        # Recency factor: recent exploits are worse
        # <90 days: full penalty, 90-365: partial, >365: reduced, >730: heavily reduced
        if days_ago < 90:
            recency_factor = 1.0
        elif days_ago < 365:
            recency_factor = 0.7
        elif days_ago < 730:
            recency_factor = 0.4
        else:
            recency_factor = 0.2

        # Effective score for this exploit
        effective = severity + (100 - severity) * (1 - recency_factor)
        worst_score = min(worst_score, effective)

    return round(max(0.0, worst_score), 1)


def fetch_defillama_treasury(protocol: str) -> dict:
    """Fetch treasury data for a protocol from DeFiLlama.

    Returns dict with keys: total_usd, stablecoin_usd, native_token_usd,
    token_breakdown (dict of token->usd_value).
    Cached for 24h.
    """
    cache_key = protocol.lower()
    cached = _treasury_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < _TREASURY_TTL:
        return cached[1]

    result = {"total_usd": 0, "stablecoin_usd": 0, "native_token_usd": 0, "token_breakdown": {}}

    _t0 = time.monotonic()
    _status = None
    try:
        time.sleep(1)  # rate limit
        resp = httpx.get(f"{LLAMA_BASE}/treasury/{protocol}", timeout=15)
        _status = resp.status_code
        if resp.status_code != 200:
            _treasury_cache[cache_key] = (time.time(), result)
            return result

        data = resp.json()
        chain_tvls = data.get("chainTvls", {})
        total = 0
        stablecoin_total = 0
        native_total = 0
        token_breakdown = {}

        for chain_name, chain_data in chain_tvls.items():
            if not isinstance(chain_data, dict):
                continue
            tvl_list = chain_data.get("tvl", [])
            if tvl_list:
                last = tvl_list[-1]
                if isinstance(last, dict):
                    total += last.get("totalLiquidityUSD", 0)

            tokens_list = chain_data.get("tokens", [])
            if tokens_list:
                latest_tokens = tokens_list[-1].get("tokens", {}) if tokens_list else {}
                for token_name, usd_value in latest_tokens.items():
                    if isinstance(usd_value, (int, float)):
                        token_breakdown[token_name] = token_breakdown.get(token_name, 0) + usd_value
                        sym = token_name.upper()
                        if any(s in sym for s in ["USDC", "USDT", "DAI", "FRAX", "USD", "LUSD", "BUSD"]):
                            stablecoin_total += usd_value

        result = {
            "total_usd": total,
            "stablecoin_usd": stablecoin_total,
            "native_token_usd": native_total,
            "token_breakdown": token_breakdown,
        }
        _treasury_cache[cache_key] = (time.time(), result)
        logger.info(f"DeFiLlama treasury {protocol}: ${total:,.0f}")
    except Exception as e:
        if _status is None:
            _status = 0
        logger.warning(f"DeFiLlama treasury fetch failed for {protocol}: {e}")
    finally:
        try:
            track_api_call(
                provider="defillama",
                endpoint=f"/treasury/{protocol}",
                caller="collectors.defillama",
                status=_status,
                latency_ms=int((time.monotonic() - _t0) * 1000),
            )
        except Exception as e:
            logger.warning(f"fetch defillama treasury failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="collectors_fetch_defillama_treasury_failure",
                    error_message=str(e)[:500],
                    cycle_phase="defillama",
                )
            except Exception:
                pass

    return result


def fetch_defillama_fees(protocol: str) -> dict:
    """Fetch fee/revenue data for a protocol from DeFiLlama.

    Returns dict with keys: total_daily_fees, total_daily_revenue,
    total_30d_fees, total_30d_revenue.
    Cached for 24h.
    """
    cache_key = protocol.lower()
    cached = _fees_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < _FEES_TTL:
        return cached[1]

    result = {"total_daily_fees": 0, "total_daily_revenue": 0,
              "total_30d_fees": 0, "total_30d_revenue": 0}

    _t0 = time.monotonic()
    _status = None
    try:
        time.sleep(1)  # rate limit
        resp = httpx.get(f"{LLAMA_BASE}/summary/fees/{protocol}", timeout=15)
        _status = resp.status_code
        if resp.status_code != 200:
            _fees_cache[cache_key] = (time.time(), result)
            return result

        data = resp.json()
        result["total_daily_fees"] = data.get("total24h", 0) or 0
        result["total_daily_revenue"] = data.get("totalRevenue24h") or data.get("revenue24h") or 0
        result["total_30d_fees"] = data.get("total30d", 0) or 0
        result["total_30d_revenue"] = data.get("totalRevenue30d") or data.get("revenue30d") or 0

        _fees_cache[cache_key] = (time.time(), result)
        logger.info(f"DeFiLlama fees {protocol}: daily=${result['total_daily_fees']:,.0f}")
    except Exception as e:
        if _status is None:
            _status = 0
        logger.warning(f"DeFiLlama fees fetch failed for {protocol}: {e}")
    finally:
        try:
            track_api_call(
                provider="defillama",
                endpoint=f"/summary/fees/{protocol}",
                caller="collectors.defillama",
                status=_status,
                latency_ms=int((time.monotonic() - _t0) * 1000),
            )
        except Exception as e:
            logger.warning(f"fetch defillama fees failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="collectors_fetch_defillama_fees_failure",
                    error_message=str(e)[:500],
                    cycle_phase="defillama",
                )
            except Exception:
                pass

    return result


def fetch_defillama_protocol_detail(protocol: str) -> dict:
    """Fetch full protocol detail from DeFiLlama.

    Returns the full protocol response dict including currentChainTvls,
    tvl history, chains, etc. Cached for 24h.
    """
    cache_key = protocol.lower()
    cached = _protocol_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < _PROTOCOL_TTL:
        return cached[1]

    _t0 = time.monotonic()
    _status = None
    try:
        time.sleep(1)  # rate limit
        resp = httpx.get(f"{LLAMA_BASE}/protocol/{protocol}", timeout=15)
        _status = resp.status_code
        if resp.status_code != 200:
            _protocol_cache[cache_key] = (time.time(), {})
            return {}

        data = resp.json()
        _protocol_cache[cache_key] = (time.time(), data)
        logger.info(f"DeFiLlama protocol detail {protocol}: fetched")
        return data
    except Exception as e:
        if _status is None:
            _status = 0
        logger.warning(f"DeFiLlama protocol detail failed for {protocol}: {e}")
        return {}
    finally:
        try:
            track_api_call(
                provider="defillama",
                endpoint=f"/protocol/{protocol}",
                caller="collectors.defillama",
                status=_status,
                latency_ms=int((time.monotonic() - _t0) * 1000),
            )
        except Exception as e:
            logger.warning(f"fetch defillama protocol detail failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="collectors_fetch_defillama_protocol_detail_failure",
                    error_message=str(e)[:500],
                    cycle_phase="defillama",
                )
            except Exception:
                pass
