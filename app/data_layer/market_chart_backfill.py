"""
Market Chart Historical Backfill
=================================
CoinGecko /coins/{id}/market_chart/range for custom date ranges.
Fills the gap between current hourly snapshots and historical daily data.

Modes:
- Backfill: 90-day hourly data for all 36 stablecoins (one-time, 36 calls)
- Daily: 5-minute resolution for last 24h (36 calls/day)

Feeds:
- Tier 7 volatility surfaces: realized vol, drawdown, recovery time
- 5-minute peg resolution: micro-depeg detection
- Temporal reconstruction: hourly component data

Schedule: Daily
"""

import json
import logging
import math
import os
import statistics
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("COINGECKO_API_KEY", "")
CG_BASE = "https://pro-api.coingecko.com/api/v3" if API_KEY else "https://api.coingecko.com/api/v3"


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if API_KEY:
        h["x-cg-pro-api-key"] = API_KEY
    return h


async def _fetch_market_chart_range(
    client: httpx.AsyncClient,
    coin_id: str,
    from_ts: int,
    to_ts: int,
) -> dict:
    """Fetch market chart with custom date range."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("coingecko")

    url = f"{CG_BASE}/coins/{coin_id}/market_chart/range"
    params = {"vs_currency": "usd", "from": from_ts, "to": to_ts}

    start = time.time()
    try:
        resp = await client.get(url, params=params, headers=_headers(), timeout=20)
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/coins/{coin_id}/market_chart/range",
                       caller="market_chart_backfill", status=resp.status_code, latency_ms=latency)

        if resp.status_code == 429:
            rate_limiter.report_429("coingecko")
            return {}

        resp.raise_for_status()
        rate_limiter.report_success("coingecko")
        return resp.json()
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/coins/{coin_id}/market_chart/range",
                       caller="market_chart_backfill", status=500, latency_ms=latency)
        logger.warning(f"Market chart range fetch failed for {coin_id}: {e}")
        return {}


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


def _store_market_chart_records(records: list[dict]):
    """Store market chart history records — batched into one transaction."""
    if not records:
        return

    import time as _t
    from app.database import get_cursor

    _start = _t.monotonic()

    # Build batch with sanitized values
    rows = []
    for rec in records:
        rows.append((
            rec["coin_id"], rec.get("stablecoin_id"),
            rec["timestamp"], _safe_float(rec.get("price")),
            _safe_float(rec.get("market_cap")),
            _safe_float(rec.get("total_volume")),
            rec["granularity"],
        ))

    stored = 0
    try:
        from psycopg2.extras import execute_values
        with get_cursor() as cur:
            execute_values(cur,
                """INSERT INTO market_chart_history
                   (coin_id, stablecoin_id, timestamp, price, market_cap,
                    total_volume, granularity)
                   VALUES %s
                   ON CONFLICT (coin_id, timestamp, granularity) DO UPDATE SET
                       price = EXCLUDED.price,
                       market_cap = EXCLUDED.market_cap,
                       total_volume = EXCLUDED.total_volume""",
                rows, page_size=500,
            )
        stored = len(rows)
    except Exception as batch_err:
        logger.error(f"market_chart batch insert failed, falling back to per-row: {batch_err}")
        for row in rows:
            try:
                with get_cursor() as cur:
                    cur.execute(
                        """INSERT INTO market_chart_history
                           (coin_id, stablecoin_id, timestamp, price, market_cap,
                            total_volume, granularity)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (coin_id, timestamp, granularity) DO UPDATE SET
                               price = EXCLUDED.price, market_cap = EXCLUDED.market_cap,
                               total_volume = EXCLUDED.total_volume""",
                        row,
                    )
                stored += 1
            except Exception:
                pass

    elapsed = _t.monotonic() - _start
    logger.error(f"market_chart_history: {stored}/{len(rows)} stored in {elapsed:.1f}s")


def _compute_volatility_from_prices(
    stablecoin_id: str, prices: list[float], intervals_per_day: int = 24,
) -> dict:
    """Compute rolling volatility metrics from price array."""
    if len(prices) < 10:
        return {}

    # Log returns
    returns = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0 and prices[i] > 0:
            returns.append(math.log(prices[i] / prices[i - 1]))

    if len(returns) < 5:
        return {}

    intervals_per_year = intervals_per_day * 365
    stdev = statistics.stdev(returns)

    # Rolling windows
    def _vol(rets, window):
        if len(rets) < window:
            return None
        subset = rets[-window:]
        return statistics.stdev(subset) * math.sqrt(intervals_per_year) if len(subset) > 1 else None

    # Max drawdown
    peak = prices[0]
    max_dd = 0
    dd_start_idx = 0
    dd_end_idx = 0
    for i, p in enumerate(prices):
        if p > peak:
            peak = p
        dd = (peak - p) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
            dd_end_idx = i

    # Recovery time: how many intervals from trough back to prior peak
    recovery_intervals = None
    if dd_end_idx < len(prices) - 1:
        trough_price = prices[dd_end_idx]
        for i in range(dd_end_idx + 1, len(prices)):
            if prices[i] >= trough_price / (1 - max_dd + 0.001):  # back to pre-drawdown
                recovery_intervals = i - dd_end_idx
                break

    recovery_hours = round(recovery_intervals / intervals_per_day * 24, 1) if recovery_intervals else None

    return {
        "realized_vol_1d": round(_vol(returns, intervals_per_day) or 0, 6),
        "realized_vol_7d": round(_vol(returns, intervals_per_day * 7) or 0, 6),
        "realized_vol_30d": round(_vol(returns, intervals_per_day * 30) or 0, 6),
        "realized_vol_90d": round(stdev * math.sqrt(intervals_per_year), 6),
        "max_drawdown_7d": round(max_dd if len(prices) <= intervals_per_day * 7 else 0, 6),
        "max_drawdown_30d": round(max_dd if len(prices) <= intervals_per_day * 30 else 0, 6),
        "max_drawdown_90d": round(max_dd, 6),
        "recovery_time_hours": recovery_hours,
    }


def _store_volatility_surface(stablecoin_id: str, vol_data: dict):
    """Store computed volatility surface (per-row transaction)."""
    from app.database import get_cursor

    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO volatility_surfaces
                   (asset_id, realized_vol_1d, realized_vol_7d, realized_vol_30d,
                    realized_vol_90d, max_drawdown_7d, max_drawdown_30d,
                    max_drawdown_90d, recovery_time_hours, computed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                   ON CONFLICT (asset_id, computed_at) DO UPDATE SET
                       realized_vol_1d = EXCLUDED.realized_vol_1d,
                       realized_vol_7d = EXCLUDED.realized_vol_7d,
                       realized_vol_30d = EXCLUDED.realized_vol_30d,
                       realized_vol_90d = EXCLUDED.realized_vol_90d""",
                (
                    stablecoin_id,
                    _safe_float(vol_data.get("realized_vol_1d")),
                    _safe_float(vol_data.get("realized_vol_7d")),
                    _safe_float(vol_data.get("realized_vol_30d")),
                    _safe_float(vol_data.get("realized_vol_90d")),
                    _safe_float(vol_data.get("max_drawdown_7d")),
                    _safe_float(vol_data.get("max_drawdown_30d")),
                    _safe_float(vol_data.get("max_drawdown_90d")),
                    _safe_float(vol_data.get("recovery_time_hours")),
                ),
            )
    except Exception as e:
        logger.error(f"Failed to store volatility surface for asset={stablecoin_id}: {e}")


async def run_market_chart_backfill(backfill_days: int = 90) -> dict:
    """
    Backfill market chart history for all stablecoins.
    - 90-day hourly data (one-time backfill, then incremental)
    - Compute volatility surfaces from the data
    """
    from app.database import fetch_all, fetch_one

    stablecoins = fetch_all(
        "SELECT id, coingecko_id FROM stablecoins WHERE scoring_enabled = TRUE"
    )
    if not stablecoins:
        return {"error": "no stablecoins found"}

    now = datetime.now(timezone.utc)
    to_ts = int(now.timestamp())
    from_ts = int((now - timedelta(days=backfill_days)).timestamp())

    total_records = 0
    vol_surfaces = 0
    coins_processed = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for sc in stablecoins:
            coin_id = sc.get("coingecko_id")
            stablecoin_id = sc["id"]
            if not coin_id:
                continue

            # Check if we already have recent data
            existing = fetch_one(
                """SELECT COUNT(*) as cnt FROM market_chart_history
                   WHERE coin_id = %s AND granularity = 'hourly'
                     AND timestamp > NOW() - INTERVAL '24 hours'""",
                (coin_id,),
            )
            if existing and existing["cnt"] > 20:
                # Already have recent hourly data — just do incremental
                from_ts_incr = int((now - timedelta(days=2)).timestamp())
                data = await _fetch_market_chart_range(client, coin_id, from_ts_incr, to_ts)
            else:
                # Full backfill
                data = await _fetch_market_chart_range(client, coin_id, from_ts, to_ts)

            if not data:
                continue

            prices_raw = data.get("prices", [])
            mcaps_raw = data.get("market_caps", [])
            vols_raw = data.get("total_volumes", [])

            if not prices_raw:
                continue

            # Determine granularity from data spacing
            if len(prices_raw) > 1:
                interval = (prices_raw[1][0] - prices_raw[0][0]) / 1000  # ms → s
                if interval < 600:
                    granularity = "5min"
                elif interval < 7200:
                    granularity = "hourly"
                else:
                    granularity = "daily"
            else:
                granularity = "hourly"

            # Build records
            records = []
            mcap_map = {int(m[0]): m[1] for m in mcaps_raw} if mcaps_raw else {}
            vol_map = {int(v[0]): v[1] for v in vols_raw} if vols_raw else {}

            for p in prices_raw:
                ts_ms = int(p[0])
                ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                records.append({
                    "coin_id": coin_id,
                    "stablecoin_id": stablecoin_id,
                    "timestamp": ts,
                    "price": p[1],
                    "market_cap": mcap_map.get(ts_ms),
                    "total_volume": vol_map.get(ts_ms),
                    "granularity": granularity,
                })

            _store_market_chart_records(records)
            total_records += len(records)
            coins_processed += 1

            # Compute volatility surface from prices
            prices = [p[1] for p in prices_raw]
            ipd = 24 if granularity == "hourly" else (288 if granularity == "5min" else 1)
            vol_data = _compute_volatility_from_prices(stablecoin_id, prices, intervals_per_day=ipd)
            if vol_data:
                _store_volatility_surface(stablecoin_id, vol_data)
                vol_surfaces += 1

    # Provenance
    try:
        from app.data_layer.provenance_scaling import attest_data_batch, link_batch_to_proof
        if total_records > 0:
            attest_data_batch("market_chart_history", [{"records": total_records, "coins": coins_processed}])
    except Exception:
        pass

    logger.info(
        f"Market chart backfill complete: {total_records} records from "
        f"{coins_processed} coins, {vol_surfaces} volatility surfaces computed"
    )

    return {
        "coins_processed": coins_processed,
        "records_stored": total_records,
        "volatility_surfaces": vol_surfaces,
        "backfill_days": backfill_days,
    }
