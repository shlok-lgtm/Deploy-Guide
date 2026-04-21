"""
GeckoTerminal OHLCV Collector
==============================
Pool-level candlestick data for market microstructure analysis.

Tiered resolution:
- Top 10 pools by TVL: 15-minute candles (micro-depeg detection)
- All other pools: hourly candles

CoinGecko Pro endpoint:
  GET /onchain/networks/{network}/pools/{pool}/ohlcv/{timeframe}

Estimated calls/day:
  Top 10 pools × 8 cycles × 15min = ~960/day
  Remaining pools × 8 cycles × hourly = ~1,600/day
  Total: ~2,560/day

Schedule: Every slow cycle (3h)
"""

import json
import logging
import math
import os
import time
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("COINGECKO_API_KEY", "")
GT_BASE = "https://pro-api.coingecko.com/api/v3/onchain" if API_KEY else "https://api.coingecko.com/api/v3/onchain"

CHAIN_MAP = {
    "ethereum": "eth",
    "base": "base",
    "arbitrum": "arbitrum-one",
}

# Top stablecoin DEX pools by TVL — get 15-minute resolution
# These are the pools where micro-depegs and liquidity shifts show up first
TOP_POOL_KEYWORDS = {
    "uniswap", "curve", "aerodrome", "velodrome", "pancakeswap",
}
TOP_POOL_ASSETS = {"usdc", "usdt"}
TOP_POOL_COUNT = 10


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if API_KEY:
        h["x-cg-pro-api-key"] = API_KEY
    return h


async def _fetch_pool_ohlcv(
    client: httpx.AsyncClient,
    network: str,
    pool_address: str,
    timeframe: str = "hour",
    limit: int = 24,
) -> list[dict]:
    """Fetch OHLCV data for a specific pool."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("coingecko")

    url = f"{GT_BASE}/networks/{network}/pools/{pool_address}/ohlcv/{timeframe}"
    params = {"limit": limit, "currency": "usd"}

    start = time.time()
    try:
        resp = await client.get(url, params=params, headers=_headers(), timeout=15)
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/onchain/pools/ohlcv/{timeframe}",
                       caller="ohlcv_collector", status=resp.status_code, latency_ms=latency)

        if resp.status_code == 429:
            rate_limiter.report_429("coingecko")
            return []

        resp.raise_for_status()
        rate_limiter.report_success("coingecko")
        data = resp.json()

        attrs = data.get("data", {}).get("attributes", {})
        ohlcv_list = attrs.get("ohlcv_list", [])
        return ohlcv_list
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/onchain/pools/ohlcv/{timeframe}",
                       caller="ohlcv_collector", status=500, latency_ms=latency)
        logger.debug(f"OHLCV fetch failed for {pool_address[:10]}… on {network}: {e}")
        return []


def _safe_float(val):
    """Return None if val is NaN or Infinity, otherwise float."""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _store_ohlcv_records(records: list[dict]):
    """Store OHLCV records to database (per-row transactions)."""
    if not records:
        return

    from app.database import get_cursor

    stored = 0
    errors = 0

    for rec in records:
        try:
            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO dex_pool_ohlcv
                       (pool_address, chain, dex, asset_id, timestamp,
                        open, high, low, close, volume, trades_count)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (pool_address, chain, timestamp) DO UPDATE SET
                           volume = EXCLUDED.volume,
                           close = EXCLUDED.close""",
                    (
                        rec["pool_address"], rec["chain"], rec.get("dex"),
                        rec.get("asset_id"), rec["timestamp"],
                        _safe_float(rec.get("open")), _safe_float(rec.get("high")),
                        _safe_float(rec.get("low")), _safe_float(rec.get("close")),
                        _safe_float(rec.get("volume")), rec.get("trades_count"),
                    ),
                )
            stored += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.error(f"Failed to store ohlcv record pool={rec.get('pool_address')}: {e}")

    if errors:
        logger.error(f"dex_pool_ohlcv: stored={stored}, errors={errors} out of {len(records)}")
    else:
        logger.info(f"Stored {stored} OHLCV records")


def _get_tracked_pools_tiered() -> tuple[list[dict], list[dict]]:
    """
    Get tracked pools split into two tiers:
    - top_pools: highest TVL stablecoin pairs → 15-min resolution
    - other_pools: everything else → hourly resolution
    """
    from app.database import fetch_all

    rows = fetch_all(
        """SELECT DISTINCT ON (pool_address)
                  asset_id, venue, chain, pool_address, volume_24h
           FROM liquidity_depth
           WHERE venue_type = 'dex'
             AND pool_address IS NOT NULL
             AND pool_address != ''
             AND snapshot_at > NOW() - INTERVAL '24 hours'
           ORDER BY pool_address, snapshot_at DESC"""
    )
    if not rows:
        return [], []

    pools = [dict(r) for r in rows]

    # Classify: top pools are USDC/USDT on major DEXes
    top_candidates = []
    other = []
    for pool in pools:
        asset = (pool.get("asset_id") or "").lower()
        venue = (pool.get("venue") or "").lower()
        is_top_asset = any(a in asset for a in TOP_POOL_ASSETS)
        is_top_venue = any(v in venue for v in TOP_POOL_KEYWORDS)
        if is_top_asset and is_top_venue:
            top_candidates.append(pool)
        else:
            other.append(pool)

    # Sort top candidates by volume, take top N
    top_candidates.sort(key=lambda p: float(p.get("volume_24h") or 0), reverse=True)
    top_pools = top_candidates[:TOP_POOL_COUNT]
    other.extend(top_candidates[TOP_POOL_COUNT:])

    return top_pools, other


async def run_ohlcv_collection() -> dict:
    """
    Fetch OHLCV data for all tracked DEX pools with tiered resolution:
    - Top 10 by TVL: 15-minute candles (96 per day)
    - All others: hourly candles (24 per day)
    """
    top_pools, other_pools = _get_tracked_pools_tiered()
    all_pools = len(top_pools) + len(other_pools)

    logger.error(
        f"[dex_pool_ohlcv] starting: top_pools={len(top_pools)}, other_pools={len(other_pools)}, "
        f"total={all_pools} (sourced from liquidity_depth WHERE venue_type='dex')"
    )

    if all_pools == 0:
        logger.error("[dex_pool_ohlcv] ZERO pools found — liquidity_depth has no DEX rows. OHLCV depends on liquidity collector producing pool data first.")
        return {"pools_found": 0, "records_stored": 0}

    total_records = 0
    pools_processed = 0
    top_processed = 0

    async with httpx.AsyncClient(timeout=30) as client:
        # Top pools: 15-minute resolution
        for pool in top_pools:
            chain = pool.get("chain", "ethereum")
            network = CHAIN_MAP.get(chain)
            pool_address = pool.get("pool_address")
            if not network or not pool_address:
                continue

            try:
                ohlcv_list = await _fetch_pool_ohlcv(
                    client, network, pool_address, timeframe="minute", limit=96
                )
                if ohlcv_list:
                    records = _parse_ohlcv(ohlcv_list, pool)
                    if records:
                        _store_ohlcv_records(records)
                        total_records += len(records)
                        pools_processed += 1
                        top_processed += 1
            except Exception as e:
                logger.debug(f"15min OHLCV failed for {pool_address[:10]}…: {e}")

        # Other pools: hourly resolution
        for pool in other_pools:
            chain = pool.get("chain", "ethereum")
            network = CHAIN_MAP.get(chain)
            pool_address = pool.get("pool_address")
            if not network or not pool_address:
                continue

            try:
                ohlcv_list = await _fetch_pool_ohlcv(
                    client, network, pool_address, timeframe="hour", limit=24
                )
                if ohlcv_list:
                    records = _parse_ohlcv(ohlcv_list, pool)
                    if records:
                        _store_ohlcv_records(records)
                        total_records += len(records)
                        pools_processed += 1
            except Exception as e:
                logger.debug(f"Hourly OHLCV failed for {pool_address[:10]}…: {e}")

    # Provenance
    try:
        from app.data_layer.provenance_scaling import attest_data_batch, link_batch_to_proof
        if total_records > 0:
            attest_data_batch("dex_pool_ohlcv", [{"records": total_records, "pools": pools_processed}])
            link_batch_to_proof("dex_pool_ohlcv", "liquidity_depth")
    except Exception:
        pass

    logger.error(
        f"[dex_pool_ohlcv] SUMMARY: pools_queried={pools_processed}, bars_received={total_records}, "
        f"top_pools_processed={top_processed}"
    )
    logger.info(
        f"OHLCV collection complete: {total_records} candles from "
        f"{pools_processed}/{all_pools} pools ({top_processed} at 15-min resolution)"
    )

    return {
        "pools_found": all_pools,
        "pools_processed": pools_processed,
        "top_pools_15min": top_processed,
        "other_pools_hourly": pools_processed - top_processed,
        "records_stored": total_records,
    }


def _parse_ohlcv(ohlcv_list: list, pool: dict) -> list[dict]:
    """Parse OHLCV list into storage records."""
    records = []
    for candle in ohlcv_list:
        if not isinstance(candle, list) or len(candle) < 6:
            continue
        ts = datetime.fromtimestamp(candle[0], tz=timezone.utc)
        records.append({
            "pool_address": pool.get("pool_address", "").lower(),
            "chain": pool.get("chain", "ethereum"),
            "dex": pool.get("venue"),
            "asset_id": pool.get("asset_id"),
            "timestamp": ts,
            "open": candle[1],
            "high": candle[2],
            "low": candle[3],
            "close": candle[4],
            "volume": candle[5],
            "trades_count": candle[6] if len(candle) > 6 else None,
        })
    return records
