"""
Historical Price Backfill Service
==================================
Populates historical_prices from CoinGecko /coins/{id}/market_chart/range.
Daily granularity, idempotent. Used by the temporal reconstruction engine.
"""

import os
import time
import logging
from datetime import datetime, timezone, timedelta

import httpx

from app.database import execute, fetch_one, fetch_all
from app.config import STABLECOIN_REGISTRY

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("COINGECKO_API_KEY", "")
BASE_URL = "https://pro-api.coingecko.com/api/v3" if API_KEY else "https://api.coingecko.com/api/v3"

# 90-day chunks to ensure daily granularity from CoinGecko
CHUNK_DAYS = 90


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if API_KEY:
        h["x-cg-pro-api-key"] = API_KEY
    return h


def _store_chunk(coingecko_id, data):
    """Parse and store a CoinGecko market_chart response chunk. Returns records inserted."""
    prices = data.get("prices", [])
    mcaps = data.get("market_caps", [])
    volumes = data.get("total_volumes", [])

    mcap_by_date = {}
    for ts_ms, val in mcaps:
        d = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date()
        mcap_by_date[d] = val

    vol_by_date = {}
    for ts_ms, val in volumes:
        d = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date()
        vol_by_date[d] = val

    inserted = 0
    for ts_ms, price in prices:
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        d = ts.date()
        mcap = mcap_by_date.get(d)
        vol = vol_by_date.get(d)

        try:
            execute(
                """
                INSERT INTO historical_prices
                    (coingecko_id, "timestamp", price, market_cap, volume_24h)
                SELECT %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM historical_prices
                    WHERE coingecko_id = %s
                      AND "timestamp"::date = %s::date
                )
                """,
                (coingecko_id, ts, price, mcap, vol, coingecko_id, ts),
            )
            inserted += 1
        except Exception:
            pass
    return inserted


def backfill_coin_sync(
    coingecko_id: str,
    from_date: str = "2020-01-01",
    to_date: str = None,
) -> int:
    """Backfill historical prices for one coin. Synchronous — safe for background tasks."""
    if not API_KEY:
        logger.warning("COINGECKO_API_KEY not set — cannot backfill")
        return 0

    if to_date is None:
        to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    start = datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(to_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    if start >= end:
        return 0

    total_inserted = 0

    with httpx.Client() as client:
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), end)

            from_ts = int(chunk_start.timestamp())
            to_ts = int(chunk_end.timestamp())

            try:
                resp = client.get(
                    f"{BASE_URL}/coins/{coingecko_id}/market_chart/range",
                    params={"vs_currency": "usd", "from": from_ts, "to": to_ts},
                    headers=_headers(),
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(
                    f"CoinGecko market_chart/range failed for {coingecko_id} "
                    f"({chunk_start.date()} to {chunk_end.date()}): {e}"
                )
                chunk_start = chunk_end
                time.sleep(2)
                continue

            chunk_inserted = _store_chunk(coingecko_id, data)
            total_inserted += chunk_inserted
            logger.info(
                f"  {coingecko_id}: {chunk_start.date()} to {chunk_end.date()} "
                f"— {chunk_inserted} records"
            )

            chunk_start = chunk_end
            time.sleep(2)

    logger.info(f"Backfilled {coingecko_id}: {from_date} to {to_date}, {total_inserted} records")
    return total_inserted


def backfill_all_sync(from_date: str = "2020-01-01", to_date: str = None) -> dict:
    """Backfill all scored stablecoins. Synchronous — safe for background tasks."""
    results = {}
    total = 0

    for sid, cfg in STABLECOIN_REGISTRY.items():
        gecko_id = cfg.get("coingecko_id")
        if not gecko_id:
            continue

        logger.info(f"Backfilling {cfg['symbol']} ({gecko_id})...")
        count = backfill_coin_sync(gecko_id, from_date, to_date)
        results[gecko_id] = count
        total += count

    try:
        promoted = fetch_all(
            "SELECT coingecko_id FROM stablecoins WHERE scoring_enabled = TRUE AND coingecko_id IS NOT NULL"
        )
        for row in promoted:
            gecko_id = row["coingecko_id"]
            if gecko_id not in results:
                logger.info(f"Backfilling promoted coin {gecko_id}...")
                count = backfill_coin_sync(gecko_id, from_date, to_date)
                results[gecko_id] = count
                total += count
    except Exception as e:
        logger.debug(f"Could not fetch promoted stablecoins: {e}")

    logger.info(f"Backfill complete: {len(results)} coins, {total} total records")
    return {"coins": results, "total": total}


# Async wrappers for backward compatibility
async def backfill_coin(coingecko_id: str, from_date: str = "2020-01-01", to_date: str = None) -> int:
    return backfill_coin_sync(coingecko_id, from_date, to_date)

async def backfill_all(from_date: str = "2020-01-01", to_date: str = None) -> dict:
    return backfill_all_sync(from_date, to_date)
