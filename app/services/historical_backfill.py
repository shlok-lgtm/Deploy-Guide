"""
Historical Price Backfill Service
==================================
Populates historical_prices from CoinGecko /coins/{id}/market_chart/range.
Daily granularity, idempotent. Used by the temporal reconstruction engine.
"""

import json as _json
import os
import time
import logging
from datetime import datetime, timezone, timedelta

import httpx

from app.database import execute, fetch_one, fetch_all, get_conn
from app.config import STABLECOIN_REGISTRY
from app.api_usage_tracker import track_api_call

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

    mcap_by_ts = {}
    for ts_ms, val in mcaps:
        mcap_by_ts[ts_ms // 3600000 * 3600000] = val  # round to hour

    vol_by_ts = {}
    for ts_ms, val in volumes:
        vol_by_ts[ts_ms // 3600000 * 3600000] = val

    rows = []
    seen_dates = set()
    for ts_ms, price in prices:
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        d = ts.date()
        if d in seen_dates:
            continue  # one per day
        seen_dates.add(d)

        hour_key = ts_ms // 3600000 * 3600000
        mcap = mcap_by_ts.get(hour_key)
        vol = vol_by_ts.get(hour_key)
        rows.append((coingecko_id, ts, price, mcap, vol))

    if not rows:
        return 0

    inserted = 0
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TEMP TABLE _hp_stage (
                        coingecko_id VARCHAR(50),
                        ts TIMESTAMPTZ,
                        price DOUBLE PRECISION,
                        market_cap DOUBLE PRECISION,
                        volume_24h DOUBLE PRECISION
                    ) ON COMMIT DROP
                """)
                from psycopg2.extras import execute_values
                execute_values(
                    cur,
                    'INSERT INTO _hp_stage VALUES %s',
                    rows,
                    template='(%s, %s, %s, %s, %s)',
                    page_size=500
                )
                cur.execute("""
                    INSERT INTO historical_prices (coingecko_id, "timestamp", price, market_cap, volume_24h)
                    SELECT s.coingecko_id, s.ts, s.price, s.market_cap, s.volume_24h
                    FROM _hp_stage s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM historical_prices h
                        WHERE h.coingecko_id = s.coingecko_id
                          AND h."timestamp"::date = s.ts::date
                    )
                """)
                inserted = cur.rowcount
                conn.commit()
    except Exception as e:
        logger.error(f"Batch insert failed for {coingecko_id}: {e}")

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

            data = None
            for attempt in range(2):
                try:
                    _t0 = time.monotonic()
                    _status = None
                    try:
                        resp = client.get(
                            f"{BASE_URL}/coins/{coingecko_id}/market_chart/range",
                            params={"vs_currency": "usd", "from": from_ts, "to": to_ts},
                            headers=_headers(),
                            timeout=30,
                        )
                        _status = resp.status_code
                    except Exception:
                        _status = 0
                        raise
                    finally:
                        try:
                            track_api_call(provider="coingecko", endpoint=f"/coins/{coingecko_id}/market_chart/range", caller="services.historical_backfill", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                        except Exception:
                            pass
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except Exception as e:
                    if attempt == 0:
                        logger.warning(
                            f"Retry {coingecko_id} chunk "
                            f"({chunk_start.date()} to {chunk_end.date()}) after 30s: {e}"
                        )
                        time.sleep(30)
                    else:
                        logger.error(
                            f"Failed twice for {coingecko_id} "
                            f"({chunk_start.date()} to {chunk_end.date()}): {e}"
                        )

            if data is None:
                chunk_start = chunk_end
                time.sleep(5)
                continue

            chunk_inserted = _store_chunk(coingecko_id, data)
            total_inserted += chunk_inserted
            logger.info(
                f"  {coingecko_id}: {chunk_start.date()} to {chunk_end.date()} "
                f"— {chunk_inserted} records"
            )

            chunk_start = chunk_end
            time.sleep(5)

    logger.info(f"Backfilled {coingecko_id}: {from_date} to {to_date}, {total_inserted} records")
    return total_inserted


def backfill_all_sync(from_date: str = "2020-01-01", to_date: str = None) -> dict:
    """Backfill all scored stablecoins. Synchronous — safe for background tasks."""
    # Create status row
    try:
        execute(
            "INSERT INTO backfill_status (coins_total, status) VALUES (%s, 'running')",
            (len(STABLECOIN_REGISTRY),)
        )
    except Exception:
        pass

    results = {}
    total = 0
    completed = 0

    for sid, cfg in STABLECOIN_REGISTRY.items():
        gecko_id = cfg.get("coingecko_id")
        if not gecko_id:
            continue

        # Update current coin in status
        try:
            execute(
                """UPDATE backfill_status SET current_coin = %s, coins_completed = %s, records_total = %s
                   WHERE id = (SELECT MAX(id) FROM backfill_status)""",
                (gecko_id, completed, total)
            )
        except Exception:
            pass

        logger.info(f"Backfilling {cfg['symbol']} ({gecko_id})...")
        count = backfill_coin_sync(gecko_id, from_date, to_date)
        results[gecko_id] = count
        total += count
        completed += 1

    # Also backfill promoted coins
    try:
        promoted = fetch_all(
            "SELECT coingecko_id FROM stablecoins WHERE scoring_enabled = TRUE AND coingecko_id IS NOT NULL"
        )
        for row in promoted:
            gecko_id = row["coingecko_id"]
            if gecko_id not in results:
                try:
                    execute(
                        """UPDATE backfill_status SET current_coin = %s
                           WHERE id = (SELECT MAX(id) FROM backfill_status)""",
                        (gecko_id,)
                    )
                except Exception:
                    pass
                logger.info(f"Backfilling promoted coin {gecko_id}...")
                count = backfill_coin_sync(gecko_id, from_date, to_date)
                results[gecko_id] = count
                total += count
                completed += 1
    except Exception as e:
        logger.debug(f"Could not fetch promoted stablecoins: {e}")

    # Mark complete
    try:
        execute(
            """UPDATE backfill_status SET status = 'completed', finished_at = NOW(),
                      coins_completed = %s, records_total = %s, details = %s
               WHERE id = (SELECT MAX(id) FROM backfill_status)""",
            (completed, total, _json.dumps(results))
        )
    except Exception:
        pass

    logger.info(f"Backfill complete: {len(results)} coins, {total} total records")
    return {"coins": results, "total": total}


# Async wrappers for backward compatibility
async def backfill_coin(coingecko_id: str, from_date: str = "2020-01-01", to_date: str = None) -> int:
    return backfill_coin_sync(coingecko_id, from_date, to_date)

async def backfill_all(from_date: str = "2020-01-01", to_date: str = None) -> dict:
    return backfill_all_sync(from_date, to_date)
