"""
Tier 1: Per-Asset Liquidity Depth Collector
============================================
Collects per-venue liquidity data from GeckoTerminal (DEX) and CoinGecko tickers (CEX).
Stores normalized liquidity profiles per asset per venue.

Sources:
- GeckoTerminal: pool-level OHLCV, liquidity, trade count, buy/sell ratio
- CoinGecko /coins/{id}/tickers: per-exchange price, volume, spread, trust score

Schedule: DEX pools every 15 minutes (high-value pools), hourly (all)
          CEX tickers hourly
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("COINGECKO_API_KEY", "")
CG_BASE = "https://pro-api.coingecko.com/api/v3" if API_KEY else "https://api.coingecko.com/api/v3"

# GeckoTerminal (on-chain DEX data) uses the same API key
GT_BASE = "https://pro-api.coingecko.com/api/v3/onchain" if API_KEY else "https://api.coingecko.com/api/v3/onchain"

# Chains we track for DEX pools
DEX_CHAINS = {
    "ethereum": "eth",
    "base": "base",
    "arbitrum": "arbitrum-one",
}

# Known stablecoin contracts per chain for DEX pool lookups
STABLECOIN_CONTRACTS_BY_CHAIN = {
    "ethereum": {
        "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "DAI":  "0x6b175474e89094c44da98b954eedeac495271d0f",
        "FRAX": "0x853d955acef822db058eb8505911ed77f175b99e",
        "PYUSD": "0x6c3ea9036406852006290770bedfcaba0e23a0e8",
        "USDe": "0x4c9edd5852cd905f086c759e8383e09bff1e68b3",
        "FDUSD": "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409",
        "USD1": "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",
    },
    "base": {
        "USDC": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        "USDT": "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2",
        "DAI":  "0x50c5725949a6f0c72e6c4a641f24049a917db0cb",
    },
    "arbitrum": {
        "USDC": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "USDT": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
        "DAI":  "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
        "FRAX": "0x17fc002b466eec40dae837fc4be5c67993ddbd6f",
    },
}


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if API_KEY:
        h["x-cg-pro-api-key"] = API_KEY
    return h


async def collect_cex_tickers(
    client: httpx.AsyncClient,
    coingecko_id: str,
    asset_id: str,
) -> list[dict]:
    """
    Collect CEX ticker data for a stablecoin from CoinGecko.
    Returns normalized liquidity records per exchange.
    """
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("coingecko")

    url = f"{CG_BASE}/coins/{coingecko_id}/tickers"
    params = {"include_exchange_logo": "false", "depth": "true"}

    start = time.time()
    try:
        resp = await client.get(url, params=params, headers=_headers(), timeout=15)
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/coins/{coingecko_id}/tickers",
                       caller="liquidity_collector", status=resp.status_code, latency_ms=latency)

        if resp.status_code == 429:
            rate_limiter.report_429("coingecko")
            return []

        resp.raise_for_status()
        rate_limiter.report_success("coingecko")
        data = resp.json()
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/coins/{coingecko_id}/tickers",
                       caller="liquidity_collector", status=500, latency_ms=latency)
        logger.warning(f"CEX ticker fetch failed for {coingecko_id}: {e}")
        return []

    records = []
    for ticker in data.get("tickers", []):
        # Only USD/USDT/USDC pairs
        target = (ticker.get("target") or "").upper()
        if target not in ("USD", "USDT", "USDC", "BUSD", "DAI"):
            continue

        exchange = ticker.get("market", {})
        exchange_id = exchange.get("identifier", "unknown")

        # Extract depth data if available
        cost_to_move = ticker.get("cost_to_move_up_usd") or 0
        cost_to_move_down = ticker.get("cost_to_move_down_usd") or 0

        record = {
            "asset_id": asset_id,
            "venue": exchange_id,
            "venue_type": "cex",
            "chain": None,
            "pool_address": None,
            "bid_depth_1pct": cost_to_move_down if cost_to_move_down else None,
            "ask_depth_1pct": cost_to_move if cost_to_move else None,
            "bid_depth_2pct": None,
            "ask_depth_2pct": None,
            "spread_bps": (
                round(ticker["bid_ask_spread_percentage"] * 100, 2)
                if ticker.get("bid_ask_spread_percentage") else None
            ),
            "volume_24h": ticker.get("converted_volume", {}).get("usd"),
            "trade_count_24h": None,
            "buy_sell_ratio": None,
            "trust_score": ticker.get("trust_score"),
            "liquidity_score": None,  # Computed after aggregation
            "raw_data": {
                "base": ticker.get("base"),
                "target": target,
                "last": ticker.get("last"),
                "is_anomaly": ticker.get("is_anomaly"),
                "is_stale": ticker.get("is_stale"),
                "trade_url": ticker.get("trade_url"),
            },
        }
        records.append(record)

    return records


async def collect_dex_pools(
    client: httpx.AsyncClient,
    chain: str,
    asset_id: str,
    token_address: Optional[str] = None,
) -> list[dict]:
    """
    Collect DEX pool data for a token on a specific chain via GeckoTerminal.
    Returns normalized liquidity records per pool.
    """
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    if not token_address:
        return []

    gt_chain = DEX_CHAINS.get(chain)
    if not gt_chain:
        return []

    await rate_limiter.acquire("coingecko")

    # Search for pools containing this token
    url = f"{GT_BASE}/networks/{gt_chain}/tokens/{token_address}/pools"
    params = {"page": 1}

    start = time.time()
    try:
        resp = await client.get(url, params=params, headers=_headers(), timeout=15)
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/onchain/tokens/{token_address}/pools",
                       caller="liquidity_collector", status=resp.status_code, latency_ms=latency)

        if resp.status_code == 429:
            rate_limiter.report_429("coingecko")
            return []

        resp.raise_for_status()
        rate_limiter.report_success("coingecko")
        data = resp.json()
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/onchain/tokens/{token_address}/pools",
                       caller="liquidity_collector", status=500, latency_ms=latency)
        logger.warning(f"DEX pool fetch failed for {token_address} on {chain}: {e}")
        return []

    records = []
    for pool in data.get("data", []):
        attrs = pool.get("attributes", {})
        pool_address = attrs.get("address", "")
        pool_name = attrs.get("name", "")

        # Extract DEX name from relationships
        dex_id = "unknown_dex"
        rels = pool.get("relationships", {})
        dex_data = rels.get("dex", {}).get("data", {})
        if dex_data:
            dex_id = dex_data.get("id", "unknown_dex")

        volume_24h = None
        vol_data = attrs.get("volume_usd", {})
        if vol_data:
            volume_24h = float(vol_data.get("h24", 0) or 0)

        reserve_usd = float(attrs.get("reserve_in_usd", 0) or 0)

        # Buy/sell ratio from transactions
        txns = attrs.get("transactions", {})
        txns_24h = txns.get("h24", {})
        buys = txns_24h.get("buys", 0) or 0
        sells = txns_24h.get("sells", 0) or 0
        buy_sell_ratio = round(buys / sells, 3) if sells > 0 else None
        trade_count = buys + sells

        record = {
            "asset_id": asset_id,
            "venue": dex_id,
            "venue_type": "dex",
            "chain": chain,
            "pool_address": pool_address,
            "bid_depth_1pct": reserve_usd / 2 if reserve_usd else None,  # Rough estimate: half of reserves
            "ask_depth_1pct": reserve_usd / 2 if reserve_usd else None,
            "bid_depth_2pct": None,
            "ask_depth_2pct": None,
            "spread_bps": None,  # Not directly available from GeckoTerminal
            "volume_24h": volume_24h,
            "trade_count_24h": trade_count if trade_count > 0 else None,
            "buy_sell_ratio": buy_sell_ratio,
            "trust_score": None,
            "liquidity_score": None,
            "raw_data": {
                "pool_name": pool_name,
                "reserve_usd": reserve_usd,
                "price_change_24h": attrs.get("price_change_percentage", {}).get("h24"),
                "pool_created_at": attrs.get("pool_created_at"),
            },
        }
        records.append(record)

    return records


def _store_liquidity_records(records: list[dict]):
    """Store liquidity depth records. Per-row error handling."""
    if not records:
        return

    from app.database import get_cursor

    stored = 0
    errors = 0
    for rec in records:
        try:
            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO liquidity_depth
                       (asset_id, venue, venue_type, chain, pool_address,
                        bid_depth_1pct, ask_depth_1pct, bid_depth_2pct, ask_depth_2pct,
                        spread_bps, volume_24h, trade_count_24h, buy_sell_ratio,
                        trust_score, liquidity_score, raw_data, snapshot_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                       ON CONFLICT (asset_id, venue, chain, snapshot_at) DO UPDATE SET
                           volume_24h = EXCLUDED.volume_24h,
                           bid_depth_1pct = EXCLUDED.bid_depth_1pct,
                           ask_depth_1pct = EXCLUDED.ask_depth_1pct,
                           spread_bps = EXCLUDED.spread_bps,
                           raw_data = EXCLUDED.raw_data""",
                    (
                        rec.get("asset_id", ""), rec.get("venue", ""), rec.get("venue_type", ""),
                        rec.get("chain"), rec.get("pool_address"),
                        rec.get("bid_depth_1pct"), rec.get("ask_depth_1pct"),
                        rec.get("bid_depth_2pct"), rec.get("ask_depth_2pct"),
                        rec.get("spread_bps"), rec.get("volume_24h"),
                        rec.get("trade_count_24h"), rec.get("buy_sell_ratio"),
                        rec.get("trust_score"), rec.get("liquidity_score"),
                        json.dumps(rec.get("raw_data")) if rec.get("raw_data") else None,
                    ),
                )
            stored += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.error(f"liquidity_depth row FAILED: {rec.get('asset_id')}:{rec.get('venue')}: {type(e).__name__}: {e}")

    if stored > 0 or errors > 0:
        logger.error(f"liquidity_depth: {stored} stored, {errors} errors out of {len(records)}")


async def run_liquidity_collection() -> dict:
    """
    Full liquidity collection cycle:
    1. Get all scored stablecoins from DB
    2. For each: collect CEX tickers + DEX pools on all chains
    3. Validate and store

    Returns summary of collection.
    """
    from app.database import fetch_all

    # Get stablecoins to collect for
    rows = fetch_all(
        """SELECT id, symbol, coingecko_id, contract
           FROM stablecoins WHERE scoring_enabled = TRUE"""
    )
    if not rows:
        logger.error("[liquidity_depth] no stablecoins found")
        return {"error": "no stablecoins found"}

    logger.error(f"[liquidity_depth] starting: {len(rows)} stablecoins to collect")
    total_records = 0
    total_cex = 0
    total_dex = 0
    stablecoins_processed = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for row in rows:
            cg_id = row.get("coingecko_id")
            asset_id = row["id"]
            symbol = row.get("symbol", "").upper()
            contract = row.get("contract")

            if not cg_id:
                continue

            # CEX tickers
            try:
                cex_records = await collect_cex_tickers(client, cg_id, asset_id)
                if cex_records:
                    _store_liquidity_records(cex_records)
                    total_cex += len(cex_records)
                    total_records += len(cex_records)
            except Exception as e:
                logger.error(f"[liquidity_depth] CEX tickers failed for {asset_id}: {e}")

            # DEX pools on each chain — use per-chain contract addresses
            for chain, _ in DEX_CHAINS.items():
                chain_contracts = STABLECOIN_CONTRACTS_BY_CHAIN.get(chain, {})
                token_addr = chain_contracts.get(symbol)
                if not token_addr:
                    # Fall back to main contract for ethereum
                    if chain == "ethereum" and contract:
                        token_addr = contract
                    else:
                        continue

                try:
                    dex_records = await collect_dex_pools(client, chain, asset_id, token_addr)
                    if dex_records:
                        _store_liquidity_records(dex_records)
                        total_dex += len(dex_records)
                        total_records += len(dex_records)
                except Exception as e:
                    logger.error(f"[liquidity_depth] DEX pools failed for {asset_id} on {chain}: {e}")

            stablecoins_processed += 1

    # Provenance: attest and link
    try:
        from app.data_layer.provenance_scaling import attest_data_batch, link_batch_to_proof
        if total_records > 0:
            attest_data_batch("liquidity_depth", [{"records": total_records, "cex": total_cex, "dex": total_dex}])
            link_batch_to_proof("liquidity_depth", "liquidity_depth")
    except Exception as e:
        logger.debug(f"Liquidity provenance failed: {e}")

    logger.error(
        f"[liquidity_depth] SUMMARY: stablecoins={stablecoins_processed}, "
        f"records={total_records} (cex={total_cex}, dex={total_dex})"
    )
    logger.info(
        f"Liquidity collection complete: {total_records} records "
        f"({total_cex} CEX, {total_dex} DEX) across {stablecoins_processed} stablecoins"
    )

    return {
        "stablecoins_processed": stablecoins_processed,
        "total_records": total_records,
        "cex_records": total_cex,
        "dex_records": total_dex,
    }


def get_liquidity_profile(asset_id: str) -> dict:
    """
    Get the current liquidity profile for an asset.
    Aggregates across all venues.
    """
    from app.database import fetch_all

    rows = fetch_all(
        """SELECT venue, venue_type, chain, volume_24h, bid_depth_1pct,
                  ask_depth_1pct, spread_bps, trust_score, trade_count_24h,
                  buy_sell_ratio, snapshot_at
           FROM liquidity_depth
           WHERE asset_id = %s
             AND snapshot_at > NOW() - INTERVAL '2 hours'
           ORDER BY volume_24h DESC NULLS LAST""",
        (asset_id,),
    )

    if not rows:
        return {"asset_id": asset_id, "venues": [], "summary": {}}

    venues = [dict(r) for r in rows]

    # Compute summary
    total_volume = sum(float(v.get("volume_24h") or 0) for v in venues)
    total_bid_depth = sum(float(v.get("bid_depth_1pct") or 0) for v in venues)
    total_ask_depth = sum(float(v.get("ask_depth_1pct") or 0) for v in venues)
    cex_count = sum(1 for v in venues if v.get("venue_type") == "cex")
    dex_count = sum(1 for v in venues if v.get("venue_type") == "dex")

    return {
        "asset_id": asset_id,
        "venues": venues,
        "summary": {
            "total_volume_24h": total_volume,
            "total_bid_depth_1pct": total_bid_depth,
            "total_ask_depth_1pct": total_ask_depth,
            "venue_count": len(venues),
            "cex_count": cex_count,
            "dex_count": dex_count,
            "concentration_hhi": _compute_volume_hhi(venues),
        },
    }


def _compute_volume_hhi(venues: list[dict]) -> Optional[float]:
    """Compute Herfindahl-Hirschman Index of volume concentration across venues."""
    volumes = [float(v.get("volume_24h") or 0) for v in venues]
    total = sum(volumes)
    if total <= 0:
        return None

    shares = [v / total for v in volumes if v > 0]
    hhi = sum(s ** 2 for s in shares)
    return round(hhi, 4)
