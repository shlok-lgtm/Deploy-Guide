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

# CoinGecko id remaps (mirrors the pre-v9.13 worker.py inline path; needed
# because a few stablecoins.coingecko_id values diverge from CG's canonical
# id for the /market_chart endpoint).
_CG_ID_REMAP = {"susd": "nusd", "spark": "spark-protocol"}


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


def _store_market_chart_history(coin_id: str, stablecoin_id: str, price_points: list[tuple]):
    """Store 5-min market_chart_history rows — batched into one transaction.

    Mirrors the pre-v9.13 worker.py inline path: same coin_id/stablecoin_id/
    timestamp/price/granularity columns, ON CONFLICT DO NOTHING semantics.
    """
    if not price_points:
        return 0

    from psycopg2.extras import execute_values
    from app.database import get_cursor

    rows = []
    for ts_ms, price in price_points:
        safe_price = _safe_float(price)
        if safe_price is None:
            continue
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        rows.append((coin_id, stablecoin_id, ts, safe_price, "5min"))

    if not rows:
        return 0

    try:
        with get_cursor() as cur:
            execute_values(cur,
                "INSERT INTO market_chart_history "
                "(coin_id, stablecoin_id, timestamp, price, granularity) "
                "VALUES %s ON CONFLICT DO NOTHING",
                rows, page_size=500,
            )
        return len(rows)
    except Exception as e:
        logger.warning(f"[peg_monitor] mchart insert failed for {stablecoin_id}: {e}")
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
    import json
    from app.database import execute as db_execute
    for depeg in micro_depegs:
        # Severity → novelty_score: 'alert' (>100bps) → 0.6, 'notable' (50-100bps) → 0.3
        novelty = 0.6 if depeg["max_deviation_bps"] > 100 else 0.3
        description = (
            f"{depeg['consecutive_5m_intervals']} consecutive 5-min intervals "
            f"with >{depeg['max_deviation_bps']:.0f}bps deviation "
            f"({depeg['duration_minutes']} min duration)"
        )
        db_execute(
            """INSERT INTO discovery_signals
               (signal_type, domain, title, description, entities,
                novelty_score, direction, magnitude, baseline,
                detail, methodology_version)
               VALUES ('micro_depeg', 'sii', %s, %s, %s,
                       %s, %s, %s, %s, %s, 'discovery-v0.1.0')""",
            (
                f"Micro-depeg detected: {depeg['stablecoin']}",
                description,
                json.dumps([depeg["stablecoin_id"]]),
                novelty,
                "decrease",  # depeg = price moving away from $1
                float(depeg["max_deviation_bps"]),
                50.0,  # 50bps trigger threshold
                json.dumps(depeg),
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
    total_mchart_rows = 0
    micro_depegs = []
    vol_surfaces = 0
    block_error: Optional[str] = None

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            for row in rows:
                cg_id_raw = row.get("coingecko_id")
                stablecoin_id = row["id"]
                symbol = row.get("symbol", "").upper()

                if not cg_id_raw:
                    continue

                cg_id = _CG_ID_REMAP.get(cg_id_raw, cg_id_raw)

                try:
                    data = await _fetch_market_chart(client, cg_id, days=1)
                    prices_raw = data.get("prices", [])

                    if not prices_raw:
                        continue

                    # Store 5-minute snapshots + market_chart_history (coupled
                    # write per v9.13: both derive from the same days=1 fetch).
                    await asyncio.to_thread(_store_peg_snapshots, stablecoin_id, prices_raw)
                    total_snapshots += len(prices_raw)
                    mchart_written = await asyncio.to_thread(
                        _store_market_chart_history, cg_id_raw, stablecoin_id, prices_raw,
                    )
                    total_mchart_rows += mchart_written

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
    except asyncio.CancelledError:
        raise
    except Exception as e:
        # Block-level failure (e.g. AsyncClient setup, DB connection
        # mid-loop). Per v9.13, every owned domain attests with the same
        # block_failed status so monitors flag all three together.
        block_error = f"{type(e).__name__}: {e}"[:200]
        logger.error(f"peg_monitor block-level failure: {block_error}")

    # Provenance — coupled-write per v9.13: this module owns three live
    # data-layer domains (peg_snapshots_5m, market_chart_history,
    # volatility_surfaces), all derived from the shared /market_chart fetch.
    # Attest each independently with its own status payload, so a partial
    # failure in one write doesn't silence the others.
    try:
        from app.data_layer.provenance_scaling import attest_data_batch, link_batch_to_proof

        def _status(rows_written: int) -> str:
            if block_error:
                return "block_failed"
            return "ok" if rows_written > 0 else "ran_no_inserts"

        peg_payload: dict = {"status": _status(total_snapshots), "rows": total_snapshots}
        mchart_payload: dict = {"status": _status(total_mchart_rows), "rows": total_mchart_rows}
        vs_payload: dict = {"status": _status(vol_surfaces), "rows": vol_surfaces}
        if block_error:
            peg_payload["error"] = block_error
            mchart_payload["error"] = block_error
            vs_payload["error"] = block_error

        await asyncio.to_thread(attest_data_batch, "peg_snapshots_5m", [peg_payload], None, "module.peg_monitor")
        await asyncio.to_thread(attest_data_batch, "market_chart_history", [mchart_payload], None, "module.peg_monitor")
        await asyncio.to_thread(attest_data_batch, "volatility_surfaces", [vs_payload], None, "module.peg_monitor")

        if total_snapshots > 0:
            await link_batch_to_proof("peg_snapshots_5m", "peg_snapshots_5m")
        if total_mchart_rows > 0:
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
        f"Peg monitoring complete: {total_snapshots} peg snapshots, "
        f"{total_mchart_rows} mchart rows, "
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
        "total_mchart_rows": total_mchart_rows,
        "micro_depegs_detected": len(micro_depegs),
        "micro_depegs": micro_depegs,
        "volatility_surfaces_computed": vol_surfaces,
    }


_PEG_FRESHNESS_MINUTES = 50


async def run_peg_monitoring_scheduled() -> dict:
    """Module-canonical scheduler entry per v9.13: freshness gate + work + attestation.

    Mirrors the v9.12 ohlcv pattern (PR #179). Returns a status dict
    regardless of branch:
      - {"status": "skipped_fresh", "table_age_minutes": X}
      - {"status": "ran", ...run_peg_monitoring() result}
      - {"status": "error", "error": str}

    The state_attestations rows for all three coupled-write domains
    (peg_snapshots_5m, market_chart_history, volatility_surfaces) fire
    inside this function (skipped/error branches) or inside
    run_peg_monitoring() (work branch). Schedulers MUST NOT re-attest.
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
                await asyncio.to_thread(attest_data_batch, domain, [skipped_payload], None, "module.peg_monitor")
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
                await asyncio.to_thread(attest_data_batch, domain, [error_payload], None, "module.peg_monitor")
        except Exception:
            pass
        return {"status": "error", "error": str(e)[:500]}
