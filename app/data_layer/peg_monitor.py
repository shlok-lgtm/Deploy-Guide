"""
5-Minute Peg Resolution Monitor
================================
CoinGecko /coins/{id}/market_chart returns 5-minute intervals for the last
24 hours. 36 calls/day — nothing. Catches micro-depegs that last 30 minutes.

Also computes volatility surfaces from price history.

Sources:
- CoinGecko /coins/{id}/market_chart: 5-min resolution for last 24h

Schedule: Daily (pulls 24h of 5-min data)
"""

import logging
import math
import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


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

API_KEY = os.environ.get("COINGECKO_API_KEY", "")
CG_BASE = "https://pro-api.coingecko.com/api/v3" if API_KEY else "https://api.coingecko.com/api/v3"


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if API_KEY:
        h["x-cg-pro-api-key"] = API_KEY
    return h


async def _fetch_market_chart(
    client: httpx.AsyncClient, coingecko_id: str, days: int = 1
) -> dict:
    """Fetch market chart data from CoinGecko. days=1 gives 5-min resolution."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("coingecko")

    url = f"{CG_BASE}/coins/{coingecko_id}/market_chart"
    params = {"vs_currency": "usd", "days": days}

    start = time.time()
    try:
        resp = await client.get(url, params=params, headers=_headers(), timeout=15)
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/coins/{coingecko_id}/market_chart",
                       caller="peg_monitor", status=resp.status_code, latency_ms=latency)

        if resp.status_code == 429:
            rate_limiter.report_429("coingecko")
            return {}

        resp.raise_for_status()
        rate_limiter.report_success("coingecko")
        return resp.json()
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/coins/{coingecko_id}/market_chart",
                       caller="peg_monitor", status=500, latency_ms=latency)
        logger.warning(f"Market chart fetch failed for {coingecko_id}: {e}")
        return {}


def _store_peg_snapshots(stablecoin_id: str, price_points: list[tuple]):
    """Store 5-minute peg snapshots — batched into one transaction."""
    if not price_points:
        return

    import time as _t
    from app.database import get_cursor

    _start = _t.monotonic()

    # Build batch, filtering invalid values
    rows = []
    skipped = 0
    for ts_ms, price in price_points:
        safe_price = _safe_float(price)
        if safe_price is None:
            skipped += 1
            continue
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        deviation_bps = round(abs(safe_price - 1.0) * 10000, 2)
        rows.append((stablecoin_id, safe_price, ts, deviation_bps))

    if not rows:
        return

    # Batch insert — single round-trip via execute_values
    stored = 0
    try:
        from psycopg2.extras import execute_values
        with get_cursor() as cur:
            execute_values(cur,
                """INSERT INTO peg_snapshots_5m
                   (stablecoin_id, price, timestamp, deviation_bps)
                   VALUES %s
                   ON CONFLICT (stablecoin_id, timestamp) DO UPDATE SET
                       price = EXCLUDED.price,
                       deviation_bps = EXCLUDED.deviation_bps""",
                rows, page_size=500,
            )
        stored = len(rows)
    except Exception as batch_err:
        # Fall back to per-row on batch failure
        logger.error(f"peg batch insert failed for {stablecoin_id}, falling back to per-row: {batch_err}")
        for row in rows:
            try:
                with get_cursor() as cur:
                    cur.execute(
                        """INSERT INTO peg_snapshots_5m
                           (stablecoin_id, price, timestamp, deviation_bps)
                           VALUES (%s, %s, %s, %s)
                           ON CONFLICT (stablecoin_id, timestamp) DO UPDATE SET
                               price = EXCLUDED.price, deviation_bps = EXCLUDED.deviation_bps""",
                        row,
                    )
                stored += 1
            except Exception as e:
                logger.warning(f"[peg_monitor] snapshot insert failed for {stablecoin_id}: {e}")
                try:
                    from app.worker import _record_cycle_error
                    _record_cycle_error(
                        error_type="data_layer__store_peg_snapshots_insert_failure",
                        error_message=str(e)[:500],
                        cycle_phase="peg_monitor",
                    )
                except Exception:
                    pass

    elapsed = _t.monotonic() - _start
    logger.error(f"peg {stablecoin_id}: {stored} rows in {elapsed:.1f}s (skipped={skipped})")


def _store_market_chart(stablecoin_id: str, coingecko_id: str, price_points: list[tuple]):
    """Store same days=1 price points to market_chart_history (granularity='5min').

    Coupled-write per v9.13: the /market_chart fetch in run_peg_monitoring
    already produces this data; mirroring it to market_chart_history avoids
    a duplicate fetch against CoinGecko.
    """
    if not price_points:
        return 0

    from app.database import get_cursor
    rows = []
    for ts_ms, price in price_points:
        safe_price = _safe_float(price)
        if safe_price is None:
            continue
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        rows.append((coingecko_id, stablecoin_id, ts, safe_price, "5min"))
    if not rows:
        return 0

    try:
        from psycopg2.extras import execute_values
        with get_cursor() as cur:
            execute_values(cur,
                """INSERT INTO market_chart_history
                   (coin_id, stablecoin_id, timestamp, price, granularity)
                   VALUES %s ON CONFLICT DO NOTHING""",
                rows, page_size=500,
            )
        return len(rows)
    except Exception as e:
        logger.warning(f"[peg_monitor] mchart batch insert failed for {stablecoin_id}: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="data_layer__store_market_chart_insert_failure",
                error_message=str(e)[:500],
                cycle_phase="peg_monitor",
            )
        except Exception:
            pass
        return 0


def _compute_volatility_surface(
    prices: list[float],
    asset_id: str,
) -> Optional[dict]:
    """Compute realized volatility metrics from price array."""
    if len(prices) < 10:
        return None

    # Log returns
    returns = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0 and prices[i] > 0:
            returns.append(math.log(prices[i] / prices[i - 1]))

    if len(returns) < 5:
        return None

    # Realized volatility (annualized)
    import statistics
    std = statistics.stdev(returns) if len(returns) > 1 else 0

    # Determine interval for annualization
    # 5-min data: 288 points/day, 365 days
    intervals_per_year = 288 * 365  # for 5-min data
    if len(prices) < 100:
        intervals_per_year = 365  # daily data

    vol = std * math.sqrt(intervals_per_year)

    # Max drawdown
    peak = prices[0]
    max_dd = 0
    max_dd_start = 0
    max_dd_end = 0
    recovery_idx = 0

    for i, p in enumerate(prices):
        if p > peak:
            peak = p
            recovery_idx = i
        dd = (peak - p) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
            max_dd_end = i

    return {
        "asset_id": asset_id,
        "realized_vol": round(vol, 6),
        "max_drawdown": round(max_dd, 6),
        "n_observations": len(prices),
    }


def _store_volatility_surface(surface: dict):
    """Store volatility surface to database (per-row transaction)."""
    from app.database import get_cursor

    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO volatility_surfaces
                   (asset_id, realized_vol_1d, max_drawdown_7d,
                    raw_prices, computed_at)
                   VALUES (%s, %s, %s, %s, NOW())
                   ON CONFLICT (asset_id, computed_at) DO UPDATE SET
                       realized_vol_1d = EXCLUDED.realized_vol_1d,
                       max_drawdown_7d = EXCLUDED.max_drawdown_7d""",
                (
                    surface["asset_id"],
                    _safe_float(surface.get("realized_vol")),
                    _safe_float(surface.get("max_drawdown")),
                    None,  # Don't store raw prices to save space
                ),
            )
    except Exception as e:
        logger.error(f"Failed to store volatility surface for asset={surface.get('asset_id')}: {e}")


def _store_volatility_surface_90d_sync(stablecoin_id, vol_90d):
    """Sync helper: insert 90d volatility surface via to_thread."""
    from app.database import get_cursor
    with get_cursor() as cur:
        cur.execute(
            """INSERT INTO volatility_surfaces
               (asset_id, realized_vol_30d, realized_vol_90d,
                max_drawdown_30d, max_drawdown_90d, computed_at)
               VALUES (%s, %s, %s, %s, %s, NOW())
               ON CONFLICT (asset_id, computed_at) DO UPDATE SET
                   realized_vol_90d = EXCLUDED.realized_vol_90d,
                   max_drawdown_90d = EXCLUDED.max_drawdown_90d""",
            (stablecoin_id,
             vol_90d.get("realized_vol"),
             vol_90d.get("realized_vol"),
             vol_90d.get("max_drawdown"),
             vol_90d.get("max_drawdown")),
        )


def _emit_depeg_signals_sync(micro_depegs):
    """Sync helper: emit discovery signals for micro-depegs."""
    from app.database import execute as db_execute
    for depeg in micro_depegs:
        db_execute(
            """INSERT INTO discovery_signals
               (signal_type, domain, entity_id, severity, title, details, created_at)
               VALUES ('micro_depeg', 'sii', %s, %s, %s, %s, NOW())""",
            (
                depeg["stablecoin_id"],
                "alert" if depeg["max_deviation_bps"] > 100 else "notable",
                f"Micro-depeg detected: {depeg['stablecoin']}",
                f"{depeg['consecutive_5m_intervals']} consecutive 5-min intervals "
                f"with >{depeg['max_deviation_bps']:.0f}bps deviation "
                f"({depeg['duration_minutes']} min duration)",
            ),
        )


async def run_peg_monitoring() -> dict:
    """
    Full 5-minute peg monitoring cycle:
    1. Get all stablecoins from DB
    2. For each, fetch 24h of 5-minute price data
    3. Store peg snapshots and compute volatility

    Returns summary with any micro-depeg detections.
    """
    from app.database import fetch_all

    rows = await asyncio.to_thread(
        fetch_all,
        """SELECT id, symbol, coingecko_id
           FROM stablecoins WHERE scoring_enabled = TRUE""",
    )
    if not rows:
        return {"error": "no stablecoins found"}

    total_snapshots = 0
    mchart_rows = 0
    micro_depegs = []
    vol_surfaces = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for row in rows:
            cg_id = row.get("coingecko_id")
            stablecoin_id = row["id"]
            symbol = row.get("symbol", "").upper()

            if not cg_id:
                continue

            try:
                data = await _fetch_market_chart(client, cg_id, days=1)
                prices_raw = data.get("prices", [])

                if not prices_raw:
                    continue

                # Coupled-write per v9.13: same price points feed both
                # peg_snapshots_5m and market_chart_history. One fetch,
                # two domains.
                await asyncio.to_thread(_store_peg_snapshots, stablecoin_id, prices_raw)
                total_snapshots += len(prices_raw)
                mchart_rows += await asyncio.to_thread(
                    _store_market_chart, stablecoin_id, cg_id, prices_raw,
                )

                # Detect micro-depegs (>50bps deviation for 3+ consecutive points)
                prices = [p[1] for p in prices_raw]
                consecutive_depeg = 0
                max_deviation = 0

                for price in prices:
                    deviation_bps = abs(price - 1.0) * 10000
                    if deviation_bps > 50:  # >0.5% from peg
                        consecutive_depeg += 1
                        max_deviation = max(max_deviation, deviation_bps)
                    else:
                        if consecutive_depeg >= 3:
                            micro_depegs.append({
                                "stablecoin": symbol,
                                "stablecoin_id": stablecoin_id,
                                "consecutive_5m_intervals": consecutive_depeg,
                                "max_deviation_bps": round(max_deviation, 2),
                                "duration_minutes": consecutive_depeg * 5,
                            })
                        consecutive_depeg = 0
                        max_deviation = 0

                # Compute volatility surface from 1-day data
                vol = _compute_volatility_surface(prices, stablecoin_id)
                if vol:
                    await asyncio.to_thread(_store_volatility_surface, vol)
                    vol_surfaces += 1

                # Also fetch 90-day data for deep volatility surfaces
                try:
                    data_90d = await _fetch_market_chart(client, cg_id, days=90)
                    prices_90d = [p[1] for p in data_90d.get("prices", [])]
                    if len(prices_90d) > 50:
                        vol_90d = _compute_volatility_surface(prices_90d, stablecoin_id)
                        if vol_90d:
                            await asyncio.to_thread(
                                _store_volatility_surface_90d_sync, stablecoin_id, vol_90d
                            )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"90d vol surface failed for {stablecoin_id}: {e}")
                    try:
                        from app.worker import _record_cycle_error
                        _record_cycle_error(
                            error_type="data_layer_run_peg_monitoring_vol_90d_failure",
                            error_message=str(e)[:500],
                            cycle_phase="peg_monitor",
                        )
                    except Exception:
                        pass

            except Exception as e:
                logger.warning(f"Peg monitoring failed for {stablecoin_id}: {e}")

    # Provenance — coupled-write attestation per v9.13: this module owns
    # all three write tables (peg_snapshots_5m, market_chart_history,
    # volatility_surfaces) and must attest to one domain per table.
    # Always attest, even on zero-write cycles (CoinGecko 429, geofence,
    # all stablecoins skipped). link_batch_to_proof stays conditional
    # because there are no rows to correlate when counts are zero.
    try:
        from app.data_layer.provenance_scaling import attest_data_batch, link_batch_to_proof

        peg_payload: dict = {"snapshots": total_snapshots, "vol_surfaces": vol_surfaces}
        if total_snapshots == 0:
            peg_payload["status"] = "ran_no_snapshots"
        mchart_payload: dict = {"rows": mchart_rows, "granularity": "5min"}
        if mchart_rows == 0:
            mchart_payload["status"] = "ran_no_rows"
        vs_payload: dict = {"surfaces": vol_surfaces}
        if vol_surfaces == 0:
            vs_payload["status"] = "ran_no_surfaces"

        await asyncio.to_thread(attest_data_batch, "peg_snapshots_5m", [peg_payload])
        await asyncio.to_thread(attest_data_batch, "market_chart_history", [mchart_payload])
        await asyncio.to_thread(attest_data_batch, "volatility_surfaces", [vs_payload])

        if total_snapshots > 0:
            await link_batch_to_proof("peg_snapshots_5m", "peg_snapshots_5m")
        if mchart_rows > 0:
            await link_batch_to_proof("market_chart_history", "market_chart_history")
        if vol_surfaces > 0:
            await link_batch_to_proof("volatility_surfaces", "volatility_surfaces")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"Peg provenance failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="data_layer_run_peg_monitoring_provenance_failure",
                error_message=str(e)[:500],
                cycle_phase="peg_monitor",
            )
        except Exception:
            pass

    logger.info(
        f"Peg monitoring complete: {total_snapshots} snapshots, "
        f"{len(micro_depegs)} micro-depegs detected, "
        f"{vol_surfaces} volatility surfaces computed"
    )

    # Emit discovery signals for micro-depegs
    if micro_depegs:
        try:
            await asyncio.to_thread(_emit_depeg_signals_sync, micro_depegs)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Micro-depeg signal emission failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="data_layer_run_peg_monitoring_signal_emission_failure",
                    error_message=str(e)[:500],
                    cycle_phase="peg_monitor",
                )
            except Exception:
                pass

    return {
        "stablecoins_monitored": len(rows),
        "total_5m_snapshots": total_snapshots,
        "mchart_rows": mchart_rows,
        "micro_depegs_detected": len(micro_depegs),
        "micro_depegs": micro_depegs,
        "volatility_surfaces_computed": vol_surfaces,
    }


_PEG_FRESHNESS_MINUTES = 50


async def run_peg_monitoring_scheduled() -> dict:
    """Module-canonical entry per v9.13: freshness gate + work + 3-domain attestation.

    Mirrors the v9.12 ohlcv pattern (PR #179). Returns a status dict regardless
    of branch:
      - {"status": "skipped_fresh", "table_age_minutes": X}
      - {"status": "ran", ...run_peg_monitoring() result}
      - {"status": "error", "error": str}

    The state_attestations rows fire inside this function (skipped/error
    branches) or inside run_peg_monitoring() (work branch). Schedulers
    (worker.py, enrichment_worker.py, main.py) MUST NOT re-attest.
    """
    from app.database import fetch_one
    from app.data_layer.provenance_scaling import attest_data_batch

    table_age_minutes: float = float(_PEG_FRESHNESS_MINUTES)
    try:
        latest = await asyncio.to_thread(
            fetch_one, "SELECT MAX(timestamp) AS t FROM peg_snapshots_5m"
        )
        if latest and latest.get("t"):
            _t = latest["t"]
            if hasattr(_t, "tzinfo") and _t.tzinfo is None:
                _t = _t.replace(tzinfo=timezone.utc)
            if hasattr(_t, "timestamp"):
                table_age_minutes = (
                    datetime.now(timezone.utc) - _t
                ).total_seconds() / 60
    except Exception as e:
        logger.warning(f"[peg_monitor] freshness check failed: {e}")
        table_age_minutes = float(_PEG_FRESHNESS_MINUTES)

    if table_age_minutes < _PEG_FRESHNESS_MINUTES:
        skipped_payload = {
            "status": "skipped_fresh",
            "table_age_minutes": round(table_age_minutes, 2),
        }
        try:
            for domain in ("peg_snapshots_5m", "market_chart_history", "volatility_surfaces"):
                await asyncio.to_thread(attest_data_batch, domain, [skipped_payload])
        except Exception as e:
            logger.warning(f"[peg_monitor] skipped-fresh attest failed: {e}")
        return skipped_payload

    try:
        result = await run_peg_monitoring()
        return {
            "status": "ran",
            "table_age_minutes": round(table_age_minutes, 2),
            **result,
        }
    except Exception as e:
        logger.warning(f"[peg_monitor] scheduled run failed: {e}")
        error_payload = {
            "status": "error",
            "table_age_minutes": round(table_age_minutes, 2),
            "error": str(e)[:200],
        }
        try:
            for domain in ("peg_snapshots_5m", "market_chart_history", "volatility_surfaces"):
                await asyncio.to_thread(attest_data_batch, domain, [error_payload])
        except Exception:
            pass
        return {"status": "error", "error": str(e)[:500]}
