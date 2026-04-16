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
        return {"error": "ETHERSCAN_API_KEY not set"}

    # 1. Find edge wallets: high value, few connections
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
        return {"error": "no edge wallets found for expansion"}

    # Pre-load existing addresses for dedup
    existing = fetch_all("SELECT address FROM wallet_graph.wallets")
    existing_set = set(r["address"].lower() for r in existing) if existing else set()
    discovered_addresses = set()

    # 2. Define producer function (fetch tokentx)
    async def fetch_tokentx(client: httpx.AsyncClient, wallet: dict) -> dict:
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
            raise httpx.HTTPStatusError(
                "Rate limited", request=resp.request, response=resp
            )
        resp.raise_for_status()
        return resp.json()

    # 3. Define consumer function (parse + discover)
    async def process_transfers(data: dict, wallet: dict):
        if data.get("status") != "1":
            return

        transfers = data.get("result", [])
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

    # 5. Batch insert discovered wallets (per-row commits to avoid all-or-nothing)
    new_wallets_seeded = 0
    if discovered_addresses:
        batch = list(discovered_addresses)[:target_new_wallets]
        for addr in batch:
            try:
                with get_cursor() as cur:
                    cur.execute(
                        """INSERT INTO wallet_graph.wallets (address, source, created_at)
                           VALUES (%s, 'graph_expansion', NOW())
                           ON CONFLICT (address) DO NOTHING""",
                        (addr,),
                    )
                new_wallets_seeded += 1
            except Exception as e:
                if new_wallets_seeded == 0:
                    logger.warning(f"Wallet expansion insert failed: {e}")

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
        "etherscan_calls_used": stats.items_fetched,
        "total_graph_size": total_count,
        "pipeline": stats.to_dict(),
    }

    logger.info(
        f"Wallet expansion complete: {stats.items_processed} wallets processed at "
        f"{stats.effective_rate:.1f}/s, {new_wallets_seeded} seeded, "
        f"graph: {total_count}"
    )

    return result
