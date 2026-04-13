"""
Bulk Markets Collector + Extended 5-min Pulls
==============================================
Two high-density CoinGecko calls:

1. /coins/markets bulk query: one call returns market data for up to 250 coins.
   Call 4x/day with all scored entities batched. Returns price change %,
   ATH, ATH date, sparkline — data not available from individual /coins/{id}.

2. Extended 5-min market_chart daily pull: covers not just 36 SII stablecoins
   but all Circle 7 entities with CoinGecko IDs (~50 more entities).
   5-min resolution catches micro-depegs on LST tokens, bridge tokens, etc.

Schedule:
  - Bulk markets: 4x/day (every 6h) — 4 calls/day
  - 5-min pulls: daily — ~86 calls/day (36 stablecoins + ~50 Circle 7)
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
CG_BASE = "https://pro-api.coingecko.com/api/v3" if API_KEY else "https://api.coingecko.com/api/v3"

# Circle 7 entities with CoinGecko IDs
CIRCLE7_COINGECKO_IDS = [
    # LSTs
    "staked-ether", "rocket-pool-eth", "coinbase-wrapped-staked-eth",
    "frax-ether", "mantle-staked-ether", "sweth",
    # Bridge tokens
    "wormhole", "axelar", "layerzero", "stargate-finance", "across-protocol",
    # Exchange tokens
    "binancecoin", "okb", "kucoin-shares", "crypto-com-chain", "mx-token",
    # Vault/yield tokens
    "yearn-finance",
    # TTI tokens
    "ondo-finance", "hashnote-usyc", "mountain-protocol",
    # Protocol governance tokens (PSI)
    "aave", "compound-governance-token", "morpho", "maker",
    "lido-dao", "uniswap", "curve-dao-token", "convex-finance",
    "eigenlayer", "pendle", "ethena", "drift-protocol",
    "jupiter-exchange-solana", "raydium", "spark",
]


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if API_KEY:
        h["x-cg-pro-api-key"] = API_KEY
    return h


async def _fetch_markets_bulk(
    client: httpx.AsyncClient,
    coin_ids: list[str],
) -> list[dict]:
    """
    Fetch /coins/markets for up to 250 coins in one call.
    Returns: price, market_cap, volume, price_change_%, ATH, ATH_date, sparkline.
    """
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("coingecko")

    ids_str = ",".join(coin_ids[:250])
    url = f"{CG_BASE}/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ids_str,
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "true",
        "price_change_percentage": "1h,24h,7d,30d",
    }

    start = time.time()
    try:
        resp = await client.get(url, params=params, headers=_headers(), timeout=20)
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", "/coins/markets",
                       caller="markets_collector", status=resp.status_code, latency_ms=latency)

        if resp.status_code == 429:
            rate_limiter.report_429("coingecko")
            return []

        resp.raise_for_status()
        rate_limiter.report_success("coingecko")
        return resp.json()
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", "/coins/markets",
                       caller="markets_collector", status=500, latency_ms=latency)
        logger.warning(f"Markets bulk fetch failed: {e}")
        return []


def _sanitize_float(val):
    """Return None if val is NaN or Infinity, else return val."""
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _store_markets_data(markets: list[dict]):
    """Store bulk markets data into entity_snapshots_hourly (per-row transactions)."""
    if not markets:
        return

    from app.database import get_cursor

    stored = 0
    errors = 0

    for m in markets:
        try:
            coin_id = m.get("id", "")
            raw_data = {
                "price_change_1h": _sanitize_float(m.get("price_change_percentage_1h_in_currency")),
                "price_change_24h": _sanitize_float(m.get("price_change_percentage_24h_in_currency")),
                "price_change_7d": _sanitize_float(m.get("price_change_percentage_7d_in_currency")),
                "price_change_30d": _sanitize_float(m.get("price_change_percentage_30d_in_currency")),
                "ath": _sanitize_float(m.get("ath")),
                "ath_date": m.get("ath_date"),
                "ath_change_pct": _sanitize_float(m.get("ath_change_percentage")),
                "atl": _sanitize_float(m.get("atl")),
                "atl_date": m.get("atl_date"),
                "high_24h": _sanitize_float(m.get("high_24h")),
                "low_24h": _sanitize_float(m.get("low_24h")),
                "fully_diluted_valuation": _sanitize_float(m.get("fully_diluted_valuation")),
                "sparkline_7d": m.get("sparkline_in_7d", {}).get("price", [])[-24:] if m.get("sparkline_in_7d") else None,
            }

            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO entity_snapshots_hourly
                       (entity_id, entity_type, market_cap, total_volume,
                        price_usd, price_change_24h, circulating_supply,
                        total_supply, raw_data, snapshot_at)
                       VALUES (%s, 'markets_bulk', %s, %s, %s, %s, %s, %s, %s, NOW())
                       ON CONFLICT (entity_id, entity_type, snapshot_at) DO UPDATE SET
                           market_cap = EXCLUDED.market_cap,
                           total_volume = EXCLUDED.total_volume,
                           price_usd = EXCLUDED.price_usd,
                           raw_data = EXCLUDED.raw_data""",
                    (
                        coin_id,
                        _sanitize_float(m.get("market_cap")),
                        _sanitize_float(m.get("total_volume")),
                        _sanitize_float(m.get("current_price")),
                        _sanitize_float(m.get("price_change_percentage_24h")),
                        _sanitize_float(m.get("circulating_supply")),
                        _sanitize_float(m.get("total_supply")),
                        json.dumps(raw_data),
                    ),
                )
            stored += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.error(
                    "Failed to store markets data for %s: %s",
                    m.get("id", "unknown"), e,
                )

    if errors:
        logger.error(
            "Markets data store: %d stored, %d errors", stored, errors,
        )


async def run_bulk_markets() -> dict:
    """
    Fetch /coins/markets for all scored entities in one bulk call.
    Captures price changes, ATH, sparklines — data not in individual endpoints.
    """
    from app.database import fetch_all

    # Collect all CoinGecko IDs
    all_ids = []

    # Stablecoins
    stablecoins = fetch_all(
        "SELECT coingecko_id FROM stablecoins WHERE scoring_enabled = TRUE AND coingecko_id IS NOT NULL"
    )
    if stablecoins:
        all_ids.extend(r["coingecko_id"] for r in stablecoins)

    # Circle 7 entities
    all_ids.extend(CIRCLE7_COINGECKO_IDS)

    # Deduplicate
    all_ids = list(dict.fromkeys(all_ids))

    if not all_ids:
        return {"error": "no entities to query"}

    async with httpx.AsyncClient(timeout=30) as client:
        markets = await _fetch_markets_bulk(client, all_ids)

    if markets:
        _store_markets_data(markets)

    logger.info(f"Bulk markets: {len(markets)} entities from 1 API call ({len(all_ids)} requested)")

    return {
        "entities_requested": len(all_ids),
        "entities_returned": len(markets),
        "api_calls": 1,
    }


async def run_extended_5min_pulls() -> dict:
    """
    Pull 5-min resolution market_chart for all Circle 7 entities (not just stablecoins).
    5-min data catches micro-depegs on LST tokens, bridge tokens, etc.
    """
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call
    from app.data_layer.market_chart_backfill import _store_market_chart_records

    total_records = 0
    entities_processed = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for cg_id in CIRCLE7_COINGECKO_IDS:
            await rate_limiter.acquire("coingecko")

            url = f"{CG_BASE}/coins/{cg_id}/market_chart"
            params = {"vs_currency": "usd", "days": 1}  # 1 day = 5-min resolution

            start = time.time()
            try:
                resp = await client.get(url, params=params, headers=_headers(), timeout=15)
                latency = int((time.time() - start) * 1000)
                track_api_call("coingecko", f"/coins/{cg_id}/market_chart",
                               caller="extended_5min", status=resp.status_code, latency_ms=latency)

                if resp.status_code == 429:
                    rate_limiter.report_429("coingecko")
                    continue

                resp.raise_for_status()
                rate_limiter.report_success("coingecko")
                data = resp.json()
            except Exception as e:
                latency = int((time.time() - start) * 1000)
                track_api_call("coingecko", f"/coins/{cg_id}/market_chart",
                               caller="extended_5min", status=500, latency_ms=latency)
                logger.debug(f"5-min pull failed for {cg_id}: {e}")
                continue

            prices = data.get("prices", [])
            mcaps = data.get("market_caps", [])
            vols = data.get("total_volumes", [])

            if not prices:
                continue

            mcap_map = {int(m[0]): m[1] for m in mcaps} if mcaps else {}
            vol_map = {int(v[0]): v[1] for v in vols} if vols else {}

            records = []
            for p in prices:
                ts_ms = int(p[0])
                ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                records.append({
                    "coin_id": cg_id,
                    "stablecoin_id": None,
                    "timestamp": ts,
                    "price": p[1],
                    "market_cap": mcap_map.get(ts_ms),
                    "total_volume": vol_map.get(ts_ms),
                    "granularity": "5min",
                })

            if records:
                _store_market_chart_records(records)
                total_records += len(records)
                entities_processed += 1

    # Provenance
    try:
        from app.data_layer.provenance_scaling import attest_data_batch
        if total_records > 0:
            attest_data_batch("market_chart_history", [{"records": total_records, "entities": entities_processed, "type": "circle7_5min"}])
    except Exception:
        pass

    logger.info(
        f"Extended 5-min pulls: {total_records} records from "
        f"{entities_processed}/{len(CIRCLE7_COINGECKO_IDS)} Circle 7 entities"
    )

    return {
        "entities_processed": entities_processed,
        "records_stored": total_records,
    }
