"""
Autonomous Wallet Graph Expansion — Pipelined
===============================================
Producer-consumer pipeline: fetch at 4.9/s, parse + insert in parallel.
Zero dead time between API calls.

Strategy: crawl outward from edge wallets (fewest connections, highest value),
discover counterparties via Etherscan tokentx, auto-seed into graph.

Uses EtherscanPipeline for ~26% throughput increase over sequential.
"""

import logging
import os
import time
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


async def run_wallet_graph_expansion(
    target_new_wallets: int = 10_000,
    max_etherscan_calls: int = 250_000,
) -> dict:
    """
    Expand the wallet graph using producer-consumer pipeline.

    Producer: fetches tokentx from Etherscan at rate-limited speed.
    Consumer: extracts counterparty addresses, batches inserts.
    """
    from app.database import fetch_all, fetch_one, get_cursor
    from app.data_layer.async_pipeline import EtherscanPipeline

    ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
    if not ETHERSCAN_API_KEY:
        logger.error("[wallet_expansion] ETHERSCAN_API_KEY not set — cannot expand")
        return {"error": "ETHERSCAN_API_KEY not set"}

    # 1. Find edge wallets: high value, few connections
    logger.error("[wallet_expansion] querying edge wallets (value >= $100K, sorted by fewest edges)")
    edge_wallets = fetch_all(
        """SELECT w.address, r.total_stablecoin_value,
                  COALESCE(e.edge_count, 0) as edge_count
           FROM wallet_graph.wallets w
           JOIN wallet_graph.wallet_risk_scores r ON w.address = r.wallet_address
           LEFT JOIN (
               SELECT from_address as address, COUNT(*) as edge_count
               FROM wallet_graph.wallet_edges
               GROUP BY from_address
           ) e ON w.address = e.address
           WHERE r.total_stablecoin_value >= 100000
           ORDER BY COALESCE(e.edge_count, 0) ASC, r.total_stablecoin_value DESC
           LIMIT %s""",
        (target_new_wallets,),
    )

    if not edge_wallets:
        logger.error("[wallet_expansion] ZERO edge wallets found — no wallets have $100K+ value with risk scores")
        # Diagnostic: how many wallets have risk scores at all?
        _scored = fetch_one("SELECT COUNT(*) as cnt FROM wallet_graph.wallet_risk_scores WHERE total_stablecoin_value > 0")
        _total = fetch_one("SELECT COUNT(*) as cnt FROM wallet_graph.wallets")
        logger.error(
            f"[wallet_expansion] DEBUG: total_wallets={_total['cnt'] if _total else 0}, "
            f"scored_with_value={_scored['cnt'] if _scored else 0}"
        )
        # Try lower threshold
        edge_wallets = fetch_all(
            """SELECT w.address, COALESCE(r.total_stablecoin_value, 0) as total_stablecoin_value,
                      0 as edge_count
               FROM wallet_graph.wallets w
               LEFT JOIN wallet_graph.wallet_risk_scores r ON w.address = r.wallet_address
               ORDER BY r.total_stablecoin_value DESC NULLS LAST
               LIMIT %s""",
            (min(target_new_wallets, 500),),
        )
        logger.error(f"[wallet_expansion] fallback: found {len(edge_wallets)} wallets (any value)")
        if not edge_wallets:
            return {"error": "no wallets found even with fallback"}

    logger.error(
        f"[wallet_expansion] found {len(edge_wallets)} edge wallets "
        f"(top value: ${edge_wallets[0].get('total_stablecoin_value', 0):,.0f}, "
        f"edges: {edge_wallets[0].get('edge_count', 0)})"
    )

    # Pre-load existing addresses for dedup
    existing = fetch_all("SELECT address FROM wallet_graph.wallets")
    existing_set = set(r["address"].lower() for r in existing) if existing else set()
    logger.error(f"[wallet_expansion] existing wallets for dedup: {len(existing_set)}")
    discovered_addresses = set()
    _api_errors = 0
    _api_empty = 0
    _api_ok = 0

    # Early diagnostic: edge wallets are already in the graph (they must be — that's where we got them).
    # New wallets come from their COUNTERPARTIES in token transfers.
    # If all counterparties are already tracked, discovered=0 and the graph is closed.
    edge_addrs = {w["address"].lower() for w in edge_wallets}
    overlap = edge_addrs & existing_set
    logger.error(
        f"[wallet_expansion] DEDUP: edge_wallets={len(edge_wallets)}, "
        f"existing={len(existing_set)}, edge_in_existing={len(overlap)}/{len(edge_addrs)} "
        f"(new wallets come from counterparties, not edge wallets themselves)"
    )

    # 2. Define producer function (fetch tokentx)
    async def fetch_tokentx(client: httpx.AsyncClient, wallet: dict) -> dict:
        nonlocal _api_errors
        resp = await client.get(
            ETHERSCAN_V2_BASE,
            params={
                "chainid": 1,
                "module": "account",
                "action": "tokentx",
                "address": wallet["address"],
                "startblock": 0,
                "endblock": 99999999,
                "page": 1,
                "offset": 50,
                "sort": "desc",
                "apikey": ETHERSCAN_API_KEY,
            },
            timeout=15,
        )
        if resp.status_code == 429 or "Max rate limit" in resp.text:
            _api_errors += 1
            raise httpx.HTTPStatusError(
                "Rate limited", request=resp.request, response=resp
            )
        resp.raise_for_status()
        return resp.json()

    # 3. Define consumer function (parse + discover)
    async def process_transfers(data: dict, wallet: dict):
        nonlocal _api_empty, _api_ok
        if data.get("status") != "1":
            _api_empty += 1
            return

        transfers = data.get("result", [])
        if not transfers:
            _api_empty += 1
            return

        _api_ok += 1
        address_lower = wallet["address"].lower()

        for tx in transfers:
            for addr_field in ["from", "to"]:
                counterparty = (tx.get(addr_field) or "").lower()
                if (
                    counterparty
                    and counterparty != address_lower
                    and counterparty != ZERO_ADDRESS
                    and counterparty not in existing_set
                    and counterparty not in discovered_addresses
                    and counterparty.startswith("0x")
                    and len(counterparty) == 42
                ):
                    discovered_addresses.add(counterparty)

    # 4. Run the pipeline
    logger.error(f"[wallet_expansion] starting pipeline: {len(edge_wallets)} wallets, max_calls={max_etherscan_calls}")
    pipeline = EtherscanPipeline(
        provider="etherscan",
        caller="wallet_expansion",
        max_calls=max_etherscan_calls,
        queue_size=100,
        consumer_count=2,
    )

    stats = await pipeline.run(
        items=edge_wallets,
        fetch_fn=fetch_tokentx,
        process_fn=process_transfers,
    )

    logger.error(
        f"[wallet_expansion] pipeline done: processed={stats.items_processed}, "
        f"fetched={stats.items_fetched}, api_ok={_api_ok}, api_empty={_api_empty}, "
        f"api_errors={_api_errors}, discovered={len(discovered_addresses)}"
    )
    if discovered_addresses:
        sample = list(discovered_addresses)[:5]
        logger.error(f"[wallet_expansion] sample new addresses: {sample}")
    elif _api_ok > 0:
        logger.error(
            f"[wallet_expansion] ZERO new addresses despite {_api_ok} successful API calls — "
            f"all counterparties already in graph ({len(existing_set)} existing)"
        )

    # 5. Batch insert discovered wallets
    new_wallets_seeded = 0
    already_existed = 0
    insert_errors = 0
    if discovered_addresses:
        batch = list(discovered_addresses)[:target_new_wallets]
        logger.error(f"[wallet_expansion] inserting {len(batch)} discovered addresses")
        for addr in batch:
            try:
                with get_cursor() as cur:
                    cur.execute(
                        """INSERT INTO wallet_graph.wallets (address, source, created_at)
                           VALUES (%s, 'graph_expansion', NOW())
                           ON CONFLICT (address) DO NOTHING""",
                        (addr,),
                    )
                    if cur.rowcount > 0:
                        new_wallets_seeded += 1
                    else:
                        already_existed += 1
            except Exception as e:
                insert_errors += 1
                if insert_errors <= 3:
                    logger.error(f"[wallet_expansion] insert failed: {addr}: {e}")
        logger.error(
            f"[wallet_expansion] insert results: new={new_wallets_seeded}, "
            f"already_existed={already_existed}, errors={insert_errors} "
            f"(batch={len(batch)})"
        )
    else:
        logger.error("[wallet_expansion] ZERO new addresses discovered — all counterparties already in graph")

    # Stats
    try:
        total_wallets = fetch_one("SELECT COUNT(*) as cnt FROM wallet_graph.wallets")
        total_count = total_wallets["cnt"] if total_wallets else 0
    except Exception:
        total_count = "unknown"

    result = {
        "edge_wallets_processed": stats.items_processed,
        "new_wallets_discovered": len(discovered_addresses),
        "new_wallets_seeded": new_wallets_seeded,
        "insert_errors": insert_errors,
        "etherscan_calls_used": stats.items_fetched,
        "api_ok": _api_ok,
        "api_empty": _api_empty,
        "api_errors": _api_errors,
        "total_graph_size": total_count,
        "pipeline": stats.to_dict(),
    }

    logger.error(
        f"[wallet_expansion] SUMMARY: edge_wallets={len(edge_wallets)}, "
        f"discovered={len(discovered_addresses)}, inserted={new_wallets_seeded}, "
        f"insert_errors={insert_errors}, graph_size={total_count}"
    )

    return result
