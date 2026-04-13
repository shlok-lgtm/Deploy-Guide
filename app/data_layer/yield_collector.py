"""
Tier 2: Yield and Rate Data Collector
======================================
Collects pool-level yield, TVL, and utilization data from DeFiLlama.
Stores every snapshot — stops throwing away the yield data we already fetch.

Sources:
- DeFiLlama /pools: current APY, TVL, utilization for every pool
- DeFiLlama /chart/{pool}: historical yield per pool (daily backfill)

Schedule: Daily
"""

import json
import logging
import math
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

DEFILLAMA_YIELDS_URL = "https://yields.llama.fi"

# Filter pools by these protocols (scored entities + major DeFi)
# Start with scored protocols, expand to all above $1M TVL
PROTOCOL_ALLOWLIST = {
    "aave-v3", "aave-v2", "compound-v3", "compound-v2",
    "morpho", "morpho-blue", "spark",
    "maker", "sky",
    "curve-dex", "convex-finance",
    "lido", "rocket-pool",
    "uniswap-v3", "uniswap-v2",
    "yearn-finance", "pendle",
    "eigenlayer", "ethena",
    "frax-ether", "frax-finance",
}

# Minimum TVL to store (filter noise)
MIN_TVL_USD = 100_000  # $100K


async def _fetch_all_pools(client: httpx.AsyncClient) -> list[dict]:
    """Fetch all yield pools from DeFiLlama."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("defillama")

    url = f"{DEFILLAMA_YIELDS_URL}/pools"
    start = time.time()
    try:
        resp = await client.get(url, timeout=30)
        latency = int((time.time() - start) * 1000)
        track_api_call("defillama", "/pools", caller="yield_collector",
                       status=resp.status_code, latency_ms=latency)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("defillama", "/pools", caller="yield_collector",
                       status=500, latency_ms=latency)
        logger.warning(f"DeFiLlama pools fetch failed: {e}")
        return []


async def _fetch_pool_history(
    client: httpx.AsyncClient, pool_id: str
) -> list[dict]:
    """Fetch historical yield data for a specific pool."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("defillama")

    url = f"{DEFILLAMA_YIELDS_URL}/chart/{pool_id}"
    start = time.time()
    try:
        resp = await client.get(url, timeout=15)
        latency = int((time.time() - start) * 1000)
        track_api_call("defillama", f"/chart/{pool_id}", caller="yield_collector",
                       status=resp.status_code, latency_ms=latency)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("defillama", f"/chart/{pool_id}", caller="yield_collector",
                       status=500, latency_ms=latency)
        logger.debug(f"Pool history fetch failed for {pool_id}: {e}")
        return []


def _is_relevant_pool(pool: dict) -> bool:
    """Check if a pool is relevant for storage."""
    project = (pool.get("project") or "").lower()
    tvl = pool.get("tvlUsd") or 0

    # Include if protocol is in allowlist
    if project in PROTOCOL_ALLOWLIST:
        return tvl >= MIN_TVL_USD

    # Include any pool with stablecoin exposure above $1M
    symbol = (pool.get("symbol") or "").upper()
    stablecoin_keywords = {"USDC", "USDT", "DAI", "FRAX", "PYUSD", "FDUSD", "USDE", "TUSD", "USD1", "USDD"}
    has_stablecoin = any(s in symbol for s in stablecoin_keywords)
    if has_stablecoin and tvl >= 1_000_000:
        return True

    # Include any pool above $10M TVL (significant DeFi activity)
    if tvl >= 10_000_000:
        return True

    return False


def _store_yield_snapshots(snapshots: list[dict]):
    """Store yield snapshots to database. Per-row error handling — one bad row doesn't kill the batch."""
    if not snapshots:
        return

    from app.database import get_cursor
    from app.data_layer.coherence_guards import DataCoherenceGuard, store_violation

    guard = DataCoherenceGuard("yield_snapshots")

    def _safe_num(v):
        if v is None:
            return None
        try:
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except (TypeError, ValueError):
            return None

    stored = 0
    errors = 0
    for snap in snapshots:
        try:
            # Validate
            violations = guard.validate_yield(snap["pool_id"], snap)
            for v in violations:
                store_violation(v)

            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO yield_snapshots
                       (pool_id, protocol, chain, asset, apy, apy_base, apy_reward,
                        tvl_usd, utilization, il_risk, exposure, stable_pool,
                        pool_meta, snapshot_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                       ON CONFLICT (pool_id, snapshot_at) DO UPDATE SET
                           apy = EXCLUDED.apy,
                           tvl_usd = EXCLUDED.tvl_usd,
                           utilization = EXCLUDED.utilization""",
                    (
                        snap["pool_id"], snap["protocol"], snap["chain"],
                        snap["asset"], _safe_num(snap.get("apy")), _safe_num(snap.get("apy_base")),
                        _safe_num(snap.get("apy_reward")), _safe_num(snap.get("tvl_usd")),
                        _safe_num(snap.get("utilization")), snap.get("il_risk"),
                        snap.get("exposure"), snap.get("stable_pool"),
                        json.dumps(snap.get("pool_meta")) if snap.get("pool_meta") else None,
                    ),
                )
            stored += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.error(f"yield_snapshots row FAILED: pool_id={snap.get('pool_id')}: {type(e).__name__}: {e}")

    logger.error(f"yield_snapshots: {stored} stored, {errors} errors out of {len(snapshots)}")


async def run_yield_collection() -> dict:
    """
    Full yield collection cycle:
    1. Fetch all pools from DeFiLlama
    2. Filter to relevant pools (scored protocols, stablecoin exposure, high TVL)
    3. Validate and store snapshots
    4. Backfill history for pools we haven't seen before (max 20 per cycle)

    Returns summary.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        # 1. Fetch all pools
        all_pools = await _fetch_all_pools(client)
        if not all_pools:
            return {"error": "no pools returned from DeFiLlama"}

        # 2. Filter
        relevant = [p for p in all_pools if _is_relevant_pool(p)]

        # 3. Build snapshots
        snapshots = []
        for pool in relevant:
            snap = {
                "pool_id": pool.get("pool", ""),
                "protocol": pool.get("project", ""),
                "chain": pool.get("chain", ""),
                "asset": pool.get("symbol", ""),
                "apy": pool.get("apy"),
                "apy_base": pool.get("apyBase"),
                "apy_reward": pool.get("apyReward"),
                "tvl_usd": pool.get("tvlUsd"),
                "utilization": pool.get("apyBaseBorrow"),  # Utilization if lending pool
                "il_risk": pool.get("ilRisk"),
                "exposure": pool.get("exposure"),
                "stable_pool": pool.get("stablecoin", False),
                "pool_meta": {
                    "underlying_tokens": pool.get("underlyingTokens"),
                    "reward_tokens": pool.get("rewardTokens"),
                    "pool_meta": pool.get("poolMeta"),
                    "mu": pool.get("mu"),
                    "sigma": pool.get("sigma"),
                    "count": pool.get("count"),
                    "outlier": pool.get("outlier"),
                    "predictions": pool.get("predictions"),
                },
            }
            snapshots.append(snap)

        # 4. Store
        _store_yield_snapshots(snapshots)

        # 5. Backfill history for new pools (limited)
        new_pools_backfilled = 0
        try:
            from app.database import fetch_one
            for snap in snapshots[:20]:  # Max 20 history backfills per cycle
                pool_id = snap["pool_id"]
                existing = fetch_one(
                    "SELECT COUNT(*) as cnt FROM yield_snapshots WHERE pool_id = %s",
                    (pool_id,),
                )
                if existing and existing["cnt"] <= 1:
                    # New pool — backfill history
                    history = await _fetch_pool_history(client, pool_id)
                    if history:
                        historical_snaps = []
                        for h in history[-90:]:  # Last 90 days
                            historical_snaps.append({
                                "pool_id": pool_id,
                                "protocol": snap["protocol"],
                                "chain": snap["chain"],
                                "asset": snap["asset"],
                                "apy": h.get("apy"),
                                "apy_base": h.get("apyBase"),
                                "apy_reward": h.get("apyReward"),
                                "tvl_usd": h.get("tvlUsd"),
                                "utilization": None,
                                "il_risk": None,
                                "exposure": None,
                                "stable_pool": snap.get("stable_pool"),
                                "pool_meta": None,
                            })
                        if historical_snaps:
                            _store_yield_snapshots(historical_snaps)
                            new_pools_backfilled += 1
        except Exception as e:
            logger.warning(f"Yield history backfill failed: {e}")

    # Provenance: attest and link
    try:
        from app.data_layer.provenance_scaling import attest_data_batch, link_batch_to_proof
        if snapshots:
            attest_data_batch("yield_snapshots", [{"pools": len(snapshots)}])
            link_batch_to_proof("yield_snapshots", "yield_snapshots")
    except Exception as e:
        logger.debug(f"Yield provenance failed: {e}")

    logger.info(
        f"Yield collection complete: {len(snapshots)} snapshots from "
        f"{len(relevant)}/{len(all_pools)} relevant pools, "
        f"{new_pools_backfilled} pools backfilled"
    )

    return {
        "total_pools_fetched": len(all_pools),
        "relevant_pools": len(relevant),
        "snapshots_stored": len(snapshots),
        "pools_backfilled": new_pools_backfilled,
    }


def get_yield_summary(protocol: Optional[str] = None) -> dict:
    """Get yield summary for a protocol or all protocols."""
    from app.database import fetch_all

    if protocol:
        rows = fetch_all(
            """SELECT protocol, chain, asset, apy, tvl_usd, utilization, snapshot_at
               FROM yield_snapshots
               WHERE protocol = %s AND snapshot_at > NOW() - INTERVAL '2 hours'
               ORDER BY tvl_usd DESC NULLS LAST""",
            (protocol,),
        )
    else:
        rows = fetch_all(
            """SELECT protocol, COUNT(*) as pool_count,
                      AVG(apy) as avg_apy,
                      SUM(tvl_usd) as total_tvl,
                      MAX(snapshot_at) as latest_snapshot
               FROM yield_snapshots
               WHERE snapshot_at > NOW() - INTERVAL '2 hours'
               GROUP BY protocol
               ORDER BY total_tvl DESC NULLS LAST"""
        )

    return {"data": [dict(r) for r in (rows or [])]}
