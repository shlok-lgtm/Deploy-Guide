"""
Tier 4: Bridge Flow Volumes and Directionality Collector
=========================================================
Directional flow data: "$50M moved Ethereum → Arbitrum in 24h, $12M back."

Sources:
- DeFiLlama /bridges: bridge volumes, TVL, chain breakdown
- DeFiLlama /bridges/{id}: individual bridge detail with directional flows

Schedule: Daily
"""

import json
import logging
import math
import time
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://bridges.llama.fi"


async def _fetch_all_bridges(client: httpx.AsyncClient) -> list[dict]:
    """Fetch all bridges from DeFiLlama."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("defillama")

    url = f"{DEFILLAMA_BASE}/bridges"
    params = {"includeChains": "true"}

    start = time.time()
    try:
        resp = await client.get(url, params=params, timeout=30)
        latency = int((time.time() - start) * 1000)
        track_api_call("defillama", "/bridges", caller="bridge_flow_collector",
                       status=resp.status_code, latency_ms=latency)
        resp.raise_for_status()
        data = resp.json()
        return data.get("bridges", [])
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("defillama", "/bridges", caller="bridge_flow_collector",
                       status=500, latency_ms=latency)
        logger.warning(f"DeFiLlama bridges fetch failed: {e}")
        return []


async def _fetch_bridge_detail(
    client: httpx.AsyncClient, bridge_id: int
) -> dict:
    """Fetch per-chain volume breakdown for a specific bridge."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("defillama")

    url = f"{DEFILLAMA_BASE}/bridge/{bridge_id}"
    params = {"period": "1d"}

    start = time.time()
    try:
        resp = await client.get(url, params=params, timeout=15)
        latency = int((time.time() - start) * 1000)
        track_api_call("defillama", f"/bridge/{bridge_id}", caller="bridge_flow_collector",
                       status=resp.status_code, latency_ms=latency)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("defillama", f"/bridge/{bridge_id}", caller="bridge_flow_collector",
                       status=500, latency_ms=latency)
        logger.debug(f"Bridge detail fetch failed for {bridge_id}: {e}")
        return {}


def _store_bridge_flows(flows: list[dict]):
    """Store bridge flow records to database. Per-row error handling — one bad row doesn't kill the batch."""
    if not flows:
        return

    from app.database import get_cursor
    from app.data_layer.coherence_guards import DataCoherenceGuard, store_violation

    guard = DataCoherenceGuard("bridge_flows")

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
    for flow in flows:
        try:
            violations = guard.validate_bridge_flow(
                flow["bridge_id"], flow["source_chain"], flow["dest_chain"], flow,
            )
            for v in violations:
                store_violation(v)

            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO bridge_flows
                       (bridge_id, bridge_name, source_chain, dest_chain,
                        volume_usd, txn_count, tvl_usd, period, raw_data, snapshot_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                       ON CONFLICT (bridge_id, source_chain, dest_chain, period, snapshot_at)
                       DO UPDATE SET
                           volume_usd = EXCLUDED.volume_usd,
                           tvl_usd = EXCLUDED.tvl_usd""",
                    (
                        flow["bridge_id"], flow.get("bridge_name"),
                        flow["source_chain"], flow["dest_chain"],
                        _safe_num(flow.get("volume_usd")), flow.get("txn_count"),
                        _safe_num(flow.get("tvl_usd")), flow.get("period", "24h"),
                        json.dumps(flow.get("raw_data")) if flow.get("raw_data") else None,
                    ),
                )
            stored += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.error(f"bridge_flows row FAILED: bridge_id={flow.get('bridge_id')}: {type(e).__name__}: {e}")

    logger.error(f"bridge_flows: {stored} stored, {errors} errors out of {len(flows)}")


async def run_bridge_flow_collection() -> dict:
    """
    Full bridge flow collection cycle:
    1. Fetch all bridges from DeFiLlama
    2. For top bridges by volume, fetch per-chain directional flows
    3. Store flow records

    Returns summary.
    """
    total_flows = 0
    bridges_processed = 0

    async with httpx.AsyncClient(timeout=30) as client:
        # 1. Fetch all bridges
        all_bridges = await _fetch_all_bridges(client)
        if not all_bridges:
            return {"error": "no bridges returned from DeFiLlama"}

        # 2. Filter to significant bridges (>$50M volume or TVL)
        significant = [
            b for b in all_bridges
            if (b.get("currentDayVolume") or 0) > 0
               or (b.get("lastDayVolume") or 0) > 50_000_000
        ]

        # Sort by volume, take top 30
        significant.sort(key=lambda b: b.get("lastDayVolume") or 0, reverse=True)
        significant = significant[:30]

        for bridge in significant:
            bridge_id = bridge.get("id")
            bridge_name = bridge.get("displayName") or bridge.get("name", "")

            if not bridge_id:
                continue

            try:
                # Fetch per-chain breakdown
                detail = await _fetch_bridge_detail(client, bridge_id)

                # Extract chain volume data
                chain_volumes = detail.get("chainVolumeUsd", {})
                if not chain_volumes:
                    # Store aggregate flow record
                    flow = {
                        "bridge_id": str(bridge_id),
                        "bridge_name": bridge_name,
                        "source_chain": "all",
                        "dest_chain": "all",
                        "volume_usd": bridge.get("lastDayVolume"),
                        "txn_count": None,
                        "tvl_usd": bridge.get("currentDayTvl"),
                        "period": "24h",
                        "raw_data": {
                            "chains": bridge.get("chains", []),
                            "destination_chains": bridge.get("destinationChains", []),
                        },
                    }
                    _store_bridge_flows([flow])
                    total_flows += 1
                else:
                    # Store per-chain directional flows
                    flows = []
                    for chain_pair, volume_data in chain_volumes.items():
                        # chain_pair format varies — handle common patterns
                        parts = chain_pair.split("->") if "->" in chain_pair else [chain_pair, "unknown"]
                        source = parts[0].strip() if len(parts) > 0 else "unknown"
                        dest = parts[1].strip() if len(parts) > 1 else "unknown"

                        volume = volume_data if isinstance(volume_data, (int, float)) else 0

                        flows.append({
                            "bridge_id": str(bridge_id),
                            "bridge_name": bridge_name,
                            "source_chain": source.lower(),
                            "dest_chain": dest.lower(),
                            "volume_usd": volume,
                            "txn_count": None,
                            "tvl_usd": None,
                            "period": "24h",
                            "raw_data": None,
                        })

                    if flows:
                        _store_bridge_flows(flows)
                        total_flows += len(flows)

                bridges_processed += 1

            except Exception as e:
                logger.warning(f"Bridge flow collection failed for {bridge_name}: {e}")

    # Provenance
    try:
        from app.data_layer.provenance_scaling import attest_data_batch, link_batch_to_proof
        if total_flows > 0:
            attest_data_batch("bridge_flows", [{"flows": total_flows, "bridges": bridges_processed}])
            link_batch_to_proof("bridge_flows", "bridge_flows")
    except Exception as e:
        logger.debug(f"Bridge flow provenance failed: {e}")

    logger.info(
        f"Bridge flow collection complete: {total_flows} flow records "
        f"from {bridges_processed}/{len(significant)} bridges"
    )

    return {
        "total_bridges_found": len(all_bridges),
        "significant_bridges": len(significant),
        "bridges_processed": bridges_processed,
        "flow_records": total_flows,
    }
