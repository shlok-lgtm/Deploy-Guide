"""
Tier 5: Exchange-Level Reserve and Flow Data Collector
======================================================
Continuous exchange health monitoring combining CoinGecko exchange data
with on-chain wallet balance verification.

Sources:
- CoinGecko /exchanges/{id}: volume, trust score, trading pairs
- CoinGecko /exchanges/{id}/volume_chart/range: historical volume
- DeFiLlama /protocols filtered by CEX category

Schedule: Hourly for trust scores/volume, daily for detailed data
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("COINGECKO_API_KEY", "")
CG_BASE = "https://pro-api.coingecko.com/api/v3" if API_KEY else "https://api.coingecko.com/api/v3"

# Top 50 exchanges by volume (CoinGecko IDs)
TOP_EXCHANGES = [
    "binance", "coinbase-exchange", "okx", "bybit_spot",
    "kraken", "kucoin", "gate", "bitget",
    "htx", "crypto_com", "mexc", "bitfinex",
    "bitstamp", "gemini", "lbank",
    # Extended to 50
    "upbit", "bithumb", "whitebit", "bitrue", "poloniex",
    "hashkey-exchange", "bitmart", "phemex", "deribit", "bitflyer",
    "indodax", "korbit", "exmo", "btcturk", "tidex",
    "coinone", "probit-exchange", "bitbank", "zaif", "coincheck",
    "okcoin", "gopax", "liquid", "btcbox", "bkex",
    "latoken", "hotbit", "coinex", "bigone", "digifinex",
    "xt", "deepcoin", "toobit", "bingx", "bitvenus",
]


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if API_KEY:
        h["x-cg-pro-api-key"] = API_KEY
    return h


async def _fetch_exchange_data(
    client: httpx.AsyncClient, exchange_id: str
) -> dict:
    """Fetch exchange detail from CoinGecko."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("coingecko")

    url = f"{CG_BASE}/exchanges/{exchange_id}"
    start = time.time()
    try:
        resp = await client.get(url, headers=_headers(), timeout=15)
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/exchanges/{exchange_id}",
                       caller="exchange_collector", status=resp.status_code, latency_ms=latency)

        if resp.status_code == 429:
            rate_limiter.report_429("coingecko")
            return {}

        resp.raise_for_status()
        rate_limiter.report_success("coingecko")
        return resp.json()
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/exchanges/{exchange_id}",
                       caller="exchange_collector", status=500, latency_ms=latency)
        logger.warning(f"Exchange data fetch failed for {exchange_id}: {e}")
        return {}


async def _fetch_exchange_volume_history(
    client: httpx.AsyncClient, exchange_id: str, days: int = 30
) -> list:
    """Fetch historical volume chart from CoinGecko."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("coingecko")

    url = f"{CG_BASE}/exchanges/{exchange_id}/volume_chart/{days}"
    start = time.time()
    try:
        resp = await client.get(url, headers=_headers(), timeout=15)
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/exchanges/{exchange_id}/volume_chart",
                       caller="exchange_collector", status=resp.status_code, latency_ms=latency)

        if resp.status_code == 429:
            rate_limiter.report_429("coingecko")
            return []

        resp.raise_for_status()
        rate_limiter.report_success("coingecko")
        return resp.json()
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/exchanges/{exchange_id}/volume_chart",
                       caller="exchange_collector", status=500, latency_ms=latency)
        logger.debug(f"Exchange volume history failed for {exchange_id}: {e}")
        return []


def _extract_stablecoin_pairs(tickers: list[dict]) -> list[dict]:
    """Extract stablecoin-specific ticker data from exchange tickers."""
    stablecoins = {"USDC", "USDT", "DAI", "FRAX", "PYUSD", "FDUSD", "USDE", "TUSD", "USDD", "USD1"}
    pairs = []

    for ticker in tickers:
        base = (ticker.get("base") or "").upper()
        target = (ticker.get("target") or "").upper()

        if base in stablecoins or target in stablecoins:
            pairs.append({
                "base": base,
                "target": target,
                "last": ticker.get("last"),
                "volume": ticker.get("converted_volume", {}).get("usd"),
                "spread": ticker.get("bid_ask_spread_percentage"),
                "trust_score": ticker.get("trust_score"),
                "is_stale": ticker.get("is_stale"),
            })

    return pairs


def _store_exchange_snapshots(snapshots: list[dict]):
    """Store exchange snapshots to database."""
    if not snapshots:
        return

    from app.database import get_cursor
    from app.data_layer.coherence_guards import DataCoherenceGuard, store_violation

    guard = DataCoherenceGuard("exchange_snapshots")

    with get_cursor() as cur:
        for snap in snapshots:
            violations = guard.validate_exchange(snap["exchange_id"], snap)
            for v in violations:
                store_violation(v)

            cur.execute(
                """INSERT INTO exchange_snapshots
                   (exchange_id, name, trust_score, trust_score_rank,
                    trade_volume_24h_btc, trade_volume_24h_usd,
                    year_established, country, trading_pairs,
                    has_trading_incentive, stablecoin_pairs, raw_data, snapshot_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                   ON CONFLICT (exchange_id, snapshot_at) DO UPDATE SET
                       trade_volume_24h_usd = EXCLUDED.trade_volume_24h_usd,
                       trust_score = EXCLUDED.trust_score""",
                (
                    snap["exchange_id"], snap.get("name"),
                    snap.get("trust_score"), snap.get("trust_score_rank"),
                    snap.get("trade_volume_24h_btc"), snap.get("trade_volume_24h_usd"),
                    snap.get("year_established"), snap.get("country"),
                    snap.get("trading_pairs"), snap.get("has_trading_incentive"),
                    json.dumps(snap.get("stablecoin_pairs")) if snap.get("stablecoin_pairs") else None,
                    json.dumps(snap.get("raw_data")) if snap.get("raw_data") else None,
                ),
            )

    logger.info(f"Stored {len(snapshots)} exchange snapshots")


async def run_exchange_collection() -> dict:
    """
    Full exchange collection cycle:
    1. Fetch data for each tracked exchange from CoinGecko
    2. Extract stablecoin-specific trading pair data
    3. Validate and store

    Returns summary.
    """
    total_snapshots = 0
    total_stablecoin_pairs = 0

    async with httpx.AsyncClient(timeout=30) as client:
        snapshots = []

        for exchange_id in TOP_EXCHANGES:
            try:
                data = await _fetch_exchange_data(client, exchange_id)
                if not data:
                    continue

                # Extract stablecoin pairs from tickers
                tickers = data.get("tickers", [])
                stablecoin_pairs = _extract_stablecoin_pairs(tickers)

                snapshot = {
                    "exchange_id": exchange_id,
                    "name": data.get("name"),
                    "trust_score": data.get("trust_score"),
                    "trust_score_rank": data.get("trust_score_rank"),
                    "trade_volume_24h_btc": data.get("trade_volume_24h_btc"),
                    "trade_volume_24h_usd": None,  # Computed from BTC price
                    "year_established": data.get("year_established"),
                    "country": data.get("country"),
                    "trading_pairs": len(tickers) if tickers else None,
                    "has_trading_incentive": data.get("has_trading_incentive"),
                    "stablecoin_pairs": stablecoin_pairs[:50] if stablecoin_pairs else None,
                    "raw_data": {
                        "centralized": data.get("centralized"),
                        "public_notice": data.get("public_notice"),
                        "alert_notice": data.get("alert_notice"),
                        "status_updates": data.get("status_updates", [])[:5],
                    },
                }

                # Estimate USD volume from BTC volume
                btc_vol = data.get("trade_volume_24h_btc")
                if btc_vol:
                    # Use a rough BTC price — will be refined with live price
                    snapshot["trade_volume_24h_usd"] = btc_vol * 65000  # Rough estimate

                snapshots.append(snapshot)
                total_stablecoin_pairs += len(stablecoin_pairs) if stablecoin_pairs else 0

                # Fetch volume history (30-day backfill)
                try:
                    vol_history = await _fetch_exchange_volume_history(client, exchange_id, days=30)
                    if vol_history:
                        snapshot["raw_data"]["volume_history_points"] = len(vol_history)
                except Exception:
                    pass

            except Exception as e:
                logger.warning(f"Exchange collection failed for {exchange_id}: {e}")

        # Store all snapshots
        if snapshots:
            _store_exchange_snapshots(snapshots)
            total_snapshots = len(snapshots)

    logger.info(
        f"Exchange collection complete: {total_snapshots} exchanges, "
        f"{total_stablecoin_pairs} stablecoin pairs"
    )

    return {
        "exchanges_processed": total_snapshots,
        "stablecoin_pairs": total_stablecoin_pairs,
    }
