"""
Wallet Indexer — Edge Builder
==============================
Derives wallet-to-wallet transfer edges from ERC-20 stablecoin token transfer
histories (tokentx). Stores edges in wallet_graph.wallet_edges with weight
signals (transfer count, total value, recency).

Uses the same Blockscout/Etherscan API as scanner.py. Admin-triggered only.
"""

import os
import math
import asyncio
import logging
import json
from datetime import datetime, timezone

import httpx

from app.database import fetch_all, fetch_one, execute
from app.indexer.config import (
    BLOCK_EXPLORER_PROVIDER,
    EXPLORER_RATE_LIMIT_DELAY,
    get_all_known_contracts,
)

logger = logging.getLogger(__name__)

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

if BLOCK_EXPLORER_PROVIDER == "etherscan":
    EXPLORER_BASE = "https://api.etherscan.io/v2/api"
else:
    EXPLORER_BASE = "https://api.blockscout.com/v2/api"

_EXPLORER_CHAIN_KEY = "chainid" if BLOCK_EXPLORER_PROVIDER == "etherscan" else "chain_id"


async def _fetch_tokentx_page(
    client: httpx.AsyncClient,
    wallet_address: str,
    api_key: str,
    page: int = 1,
    offset: int = 100,
) -> list[dict] | None:
    """Fetch one page of ERC-20 token transfer events for a wallet."""
    try:
        resp = await client.get(
            EXPLORER_BASE,
            params={
                _EXPLORER_CHAIN_KEY: 1,
                "module": "account",
                "action": "tokentx",
                "address": wallet_address,
                "page": page,
                "offset": offset,
                "sort": "desc",
                "apikey": api_key,
            },
            timeout=15.0,
        )
        data = resp.json()
        if data.get("status") == "1" and isinstance(data.get("result"), list):
            return data["result"]
        msg = data.get("result", "")
        if "Max rate limit" in str(msg):
            logger.warning("Explorer rate limit hit, backing off")
            await asyncio.sleep(2.0)
        return None
    except Exception as e:
        logger.debug(f"tokentx fetch error for {wallet_address[:10]}…: {e}")
        return None


def _compute_weight(total_value_usd: float, transfer_count: int, last_transfer_at: datetime) -> float:
    """Compute edge weight from value, frequency, and recency."""
    now = datetime.now(timezone.utc)
    if last_transfer_at.tzinfo is None:
        last_transfer_at = last_transfer_at.replace(tzinfo=timezone.utc)
    days_since = (now - last_transfer_at).days
    recency = max(0.1, 1.0 - (days_since / 365))
    return math.log10(1 + total_value_usd) * transfer_count * recency


async def build_edges_for_wallet(
    client: httpx.AsyncClient,
    wallet_address: str,
    api_key: str,
    max_pages: int = 10,
) -> dict:
    """
    Fetch token transfer history for a wallet and upsert stablecoin transfer
    edges into wallet_graph.wallet_edges.
    """
    scored_contracts, all_contracts = get_all_known_contracts()
    wallet_lower = wallet_address.lower()

    # Accumulate edges: (from, to) -> {count, total_value, first_ts, last_ts, tokens}
    edge_map: dict[tuple[str, str], dict] = {}
    total_transfers = 0
    pages_fetched = 0

    for page in range(1, max_pages + 1):
        transfers = await _fetch_tokentx_page(client, wallet_lower, api_key, page=page)
        await asyncio.sleep(EXPLORER_RATE_LIMIT_DELAY)
        pages_fetched += 1

        if not transfers:
            break

        for tx in transfers:
            contract_addr = (tx.get("contractAddress") or "").lower()
            if contract_addr not in scored_contracts:
                continue

            from_addr = (tx.get("from") or "").lower()
            to_addr = (tx.get("to") or "").lower()

            if from_addr == ZERO_ADDRESS or to_addr == ZERO_ADDRESS:
                continue
            if not from_addr or not to_addr:
                continue

            token_info = scored_contracts[contract_addr]
            decimals = token_info.get("decimals", 18)
            symbol = token_info.get("symbol", "???")

            try:
                raw_value = int(tx.get("value", "0"))
            except (ValueError, TypeError):
                continue
            value_usd = raw_value / (10 ** decimals)

            ts_raw = tx.get("timeStamp")
            try:
                ts = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc) if ts_raw else datetime.now(timezone.utc)
            except (ValueError, TypeError):
                ts = datetime.now(timezone.utc)

            edge_key = (from_addr, to_addr)
            if edge_key not in edge_map:
                edge_map[edge_key] = {
                    "count": 0,
                    "total_value": 0.0,
                    "first_ts": ts,
                    "last_ts": ts,
                    "tokens": {},
                }

            edge = edge_map[edge_key]
            edge["count"] += 1
            edge["total_value"] += value_usd
            edge["first_ts"] = min(edge["first_ts"], ts)
            edge["last_ts"] = max(edge["last_ts"], ts)

            if symbol not in edge["tokens"]:
                edge["tokens"][symbol] = {"count": 0, "value": 0.0}
            edge["tokens"][symbol]["count"] += 1
            edge["tokens"][symbol]["value"] += value_usd

            total_transfers += 1

        if len(transfers) < 100:
            break

    # Upsert edges
    edges_upserted = 0
    for (from_addr, to_addr), edge in edge_map.items():
        weight = _compute_weight(edge["total_value"], edge["count"], edge["last_ts"])
        tokens_json = json.dumps(edge["tokens"])

        execute(
            """
            INSERT INTO wallet_graph.wallet_edges
                (from_address, to_address, transfer_count, total_value_usd,
                 first_transfer_at, last_transfer_at, tokens_transferred, weight)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (from_address, to_address) DO UPDATE SET
                transfer_count = wallet_graph.wallet_edges.transfer_count + EXCLUDED.transfer_count,
                total_value_usd = wallet_graph.wallet_edges.total_value_usd + EXCLUDED.total_value_usd,
                first_transfer_at = LEAST(wallet_graph.wallet_edges.first_transfer_at, EXCLUDED.first_transfer_at),
                last_transfer_at = GREATEST(wallet_graph.wallet_edges.last_transfer_at, EXCLUDED.last_transfer_at),
                tokens_transferred = wallet_graph.wallet_edges.tokens_transferred || EXCLUDED.tokens_transferred,
                weight = EXCLUDED.weight,
                updated_at = NOW()
            """,
            (from_addr, to_addr, edge["count"], edge["total_value"],
             edge["first_ts"], edge["last_ts"], tokens_json, weight),
        )
        edges_upserted += 1

    # Update build status
    execute(
        """
        INSERT INTO wallet_graph.edge_build_status
            (wallet_address, last_built_at, transfers_processed, edges_created, pages_fetched, status)
        VALUES (%s, NOW(), %s, %s, %s, 'complete')
        ON CONFLICT (wallet_address) DO UPDATE SET
            last_built_at = NOW(),
            transfers_processed = EXCLUDED.transfers_processed,
            edges_created = EXCLUDED.edges_created,
            pages_fetched = EXCLUDED.pages_fetched,
            status = 'complete'
        """,
        (wallet_lower, total_transfers, edges_upserted, pages_fetched),
    )

    return {
        "transfers_processed": total_transfers,
        "edges_upserted": edges_upserted,
        "pages_fetched": pages_fetched,
    }


async def run_edge_builder(
    max_wallets: int = 100,
    max_pages_per_wallet: int = 10,
    priority: str = "value",
) -> dict:
    """
    Batch edge builder. Queries wallets needing edge building and processes them.
    """
    order_clause = "w.total_stablecoin_value DESC NULLS LAST"
    if priority == "unbuilt":
        order_clause = "w.created_at ASC"

    wallets = fetch_all(
        f"""
        SELECT w.address, w.total_stablecoin_value
        FROM wallet_graph.wallets w
        LEFT JOIN wallet_graph.edge_build_status e ON w.address = e.wallet_address
        WHERE e.wallet_address IS NULL OR e.status = 'pending'
        ORDER BY {order_clause}
        LIMIT %s
        """,
        (max_wallets,),
    )

    if not wallets:
        logger.info("No wallets need edge building")
        return {"wallets_processed": 0, "total_edges_created": 0, "total_transfers": 0}

    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    total_edges = 0
    total_transfers = 0
    wallets_processed = 0

    async with httpx.AsyncClient() as client:
        for i, w in enumerate(wallets):
            try:
                result = await build_edges_for_wallet(
                    client, w["address"], api_key,
                    max_pages=max_pages_per_wallet,
                )
                total_edges += result["edges_upserted"]
                total_transfers += result["transfers_processed"]
                wallets_processed += 1

                if (i + 1) % 10 == 0:
                    logger.info(
                        f"Edge builder progress: {i + 1}/{len(wallets)} wallets, "
                        f"{total_edges} edges, {total_transfers} transfers"
                    )
            except Exception as e:
                logger.error(f"Edge build failed for {w['address'][:10]}…: {e}")
                execute(
                    """
                    INSERT INTO wallet_graph.edge_build_status (wallet_address, status)
                    VALUES (%s, 'pending')
                    ON CONFLICT (wallet_address) DO UPDATE SET status = 'pending'
                    """,
                    (w["address"],),
                )

    logger.info(
        f"Edge builder complete: {wallets_processed} wallets, "
        f"{total_edges} edges, {total_transfers} transfers"
    )

    return {
        "wallets_processed": wallets_processed,
        "total_edges_created": total_edges,
        "total_transfers": total_transfers,
    }
