"""
Full Hourly Entity Snapshots
==============================
Pull full /coins/{id} from CoinGecko for every scored entity hourly.
~90 entities x 24 hours x 30 days = ~65K calls/month, ~13% of CoinGecko budget.

Hourly snapshots of market cap, volume, exchange tickers, developer
activity, community stats.

Sources:
- CoinGecko /coins/{id}: full coin data

Schedule: Hourly
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


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if API_KEY:
        h["x-cg-pro-api-key"] = API_KEY
    return h


async def _fetch_coin_data(
    client: httpx.AsyncClient, coingecko_id: str
) -> dict:
    """Fetch full coin data from CoinGecko."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("coingecko")

    url = f"{CG_BASE}/coins/{coingecko_id}"
    params = {
        "localization": "false",
        "tickers": "true",
        "market_data": "true",
        "community_data": "true",
        "developer_data": "true",
    }

    start = time.time()
    try:
        resp = await client.get(url, params=params, headers=_headers(), timeout=15)
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/coins/{coingecko_id}",
                       caller="entity_snapshots", status=resp.status_code, latency_ms=latency)

        if resp.status_code == 429:
            rate_limiter.report_429("coingecko")
            return {}

        resp.raise_for_status()
        rate_limiter.report_success("coingecko")
        return resp.json()
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("coingecko", f"/coins/{coingecko_id}",
                       caller="entity_snapshots", status=500, latency_ms=latency)
        logger.warning(f"Coin data fetch failed for {coingecko_id}: {e}")
        return {}


def _store_snapshots(snapshots: list[dict]):
    """Store entity snapshots to database."""
    if not snapshots:
        return

    from app.database import get_cursor

    with get_cursor() as cur:
        for snap in snapshots:
            cur.execute(
                """INSERT INTO entity_snapshots_hourly
                   (entity_id, entity_type, market_cap, total_volume,
                    price_usd, price_change_24h, circulating_supply,
                    total_supply, exchange_tickers_count,
                    developer_data, community_data, raw_data, snapshot_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                   ON CONFLICT (entity_id, entity_type, snapshot_at)
                   DO UPDATE SET
                       market_cap = EXCLUDED.market_cap,
                       total_volume = EXCLUDED.total_volume,
                       price_usd = EXCLUDED.price_usd""",
                (
                    snap["entity_id"], snap["entity_type"],
                    snap.get("market_cap"), snap.get("total_volume"),
                    snap.get("price_usd"), snap.get("price_change_24h"),
                    snap.get("circulating_supply"), snap.get("total_supply"),
                    snap.get("exchange_tickers_count"),
                    json.dumps(snap.get("developer_data")) if snap.get("developer_data") else None,
                    json.dumps(snap.get("community_data")) if snap.get("community_data") else None,
                    json.dumps(snap.get("raw_data")) if snap.get("raw_data") else None,
                ),
            )

    logger.info(f"Stored {len(snapshots)} entity snapshots")


async def run_entity_snapshots() -> dict:
    """
    Full hourly entity snapshot cycle.
    Fetch full CoinGecko data for all scored entities.
    """
    from app.database import fetch_all

    # Collect all entities to snapshot
    entities = []

    # SII stablecoins
    stablecoins = fetch_all(
        "SELECT id, coingecko_id FROM stablecoins WHERE scoring_enabled = TRUE"
    )
    if stablecoins:
        for sc in stablecoins:
            if sc.get("coingecko_id"):
                entities.append({
                    "entity_id": sc["id"],
                    "entity_type": "stablecoin",
                    "coingecko_id": sc["coingecko_id"],
                })

    # PSI protocol tokens
    PSI_CG_MAP = {
        "aave": "aave", "compound-finance": "compound-governance-token",
        "morpho": "morpho", "spark": "spark",
        "lido": "lido-dao", "rocket-pool": "rocket-pool",
        "uniswap": "uniswap", "curve-finance": "curve-dao-token",
        "convex-finance": "convex-finance", "eigenlayer": "eigenlayer",
        "pendle": "pendle", "ethena": "ethena",
        "sky": "maker", "drift": "drift-protocol",
        "jupiter-perpetual-exchange": "jupiter-exchange-solana",
        "raydium": "raydium",
    }
    try:
        psi_rows = fetch_all(
            "SELECT DISTINCT protocol_slug FROM psi_scores ORDER BY protocol_slug"
        )
        if psi_rows:
            for row in psi_rows:
                slug = row["protocol_slug"]
                cg_id = PSI_CG_MAP.get(slug)
                if cg_id:
                    entities.append({
                        "entity_id": slug,
                        "entity_type": "protocol_token",
                        "coingecko_id": cg_id,
                    })
    except Exception as e:
        logger.debug(f"PSI entity lookup failed: {e}")

    # Circle 7 entities — LSTs, bridges, vaults, exchanges, DAOs, TTIs
    CIRCLE7_CG_MAP = {
        # LSTs
        "lido-staked-ether": "staked-ether",
        "rocket-pool-eth": "rocket-pool-eth",
        "coinbase-wrapped-staked-eth": "coinbase-wrapped-staked-eth",
        "frax-ether": "frax-ether",
        "mantle-staked-ether": "mantle-staked-ether",
        "swell-staked-eth": "sweth",
        # Bridges (governance tokens)
        "wormhole": "wormhole", "axelar": "axelar",
        "layerzero": "layerzero", "stargate-finance": "stargate-finance",
        "across-protocol": "across-protocol",
        # Exchanges (governance tokens)
        "binancecoin": "binancecoin", "okb": "okb", "kucoin-shares": "kucoin-shares",
        "crypto-com-chain": "crypto-com-chain", "mx-token": "mx-token",
        # Vaults
        "yearn-finance": "yearn-finance",
        # TTIs
        "ondo-finance": "ondo-finance", "hashnote-usyc": "hashnote-usyc",
        "mountain-protocol": "mountain-protocol",
    }
    try:
        for entity_id, cg_id in CIRCLE7_CG_MAP.items():
            entities.append({
                "entity_id": entity_id,
                "entity_type": "circle7",
                "coingecko_id": cg_id,
            })
    except Exception as e:
        logger.debug(f"Circle 7 entity mapping failed: {e}")

    if not entities:
        return {"error": "no entities to snapshot"}

    total_snapshots = 0
    snapshots = []

    async with httpx.AsyncClient(timeout=30) as client:
        for entity in entities:
            try:
                data = await _fetch_coin_data(client, entity["coingecko_id"])
                if not data:
                    continue

                market = data.get("market_data", {})

                dev_data = data.get("developer_data")
                comm_data = data.get("community_data")

                snap = {
                    "entity_id": entity["entity_id"],
                    "entity_type": entity["entity_type"],
                    "market_cap": market.get("market_cap", {}).get("usd"),
                    "total_volume": market.get("total_volume", {}).get("usd"),
                    "price_usd": market.get("current_price", {}).get("usd"),
                    "price_change_24h": market.get("price_change_percentage_24h"),
                    "circulating_supply": market.get("circulating_supply"),
                    "total_supply": market.get("total_supply"),
                    "exchange_tickers_count": data.get("tickers_count"),
                    "developer_data": {
                        "forks": dev_data.get("forks"),
                        "stars": dev_data.get("stars"),
                        "subscribers": dev_data.get("subscribers"),
                        "total_issues": dev_data.get("total_issues"),
                        "closed_issues": dev_data.get("closed_issues"),
                        "pull_requests_merged": dev_data.get("pull_requests_merged"),
                        "commit_count_4_weeks": dev_data.get("commit_count_4_weeks"),
                    } if dev_data else None,
                    "community_data": {
                        "twitter_followers": comm_data.get("twitter_followers"),
                        "reddit_subscribers": comm_data.get("reddit_subscribers"),
                        "telegram_channel_user_count": comm_data.get("telegram_channel_user_count"),
                    } if comm_data else None,
                    "raw_data": {
                        "sentiment_votes_up_percentage": data.get("sentiment_votes_up_percentage"),
                        "sentiment_votes_down_percentage": data.get("sentiment_votes_down_percentage"),
                        "coingecko_rank": data.get("coingecko_rank"),
                        "coingecko_score": data.get("coingecko_score"),
                        "developer_score": data.get("developer_score"),
                        "community_score": data.get("community_score"),
                        "liquidity_score": data.get("liquidity_score"),
                    },
                }
                snapshots.append(snap)

            except Exception as e:
                logger.warning(f"Entity snapshot failed for {entity['entity_id']}: {e}")

    if snapshots:
        _store_snapshots(snapshots)
        total_snapshots = len(snapshots)

    logger.info(f"Entity snapshots complete: {total_snapshots}/{len(entities)} entities")

    return {
        "entities_targeted": len(entities),
        "snapshots_stored": total_snapshots,
    }
