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
    get_chain_contracts,
    CHAIN_CONFIGS,
    SUPPORTED_CHAINS,
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
    explorer_base: str = None,
    chain_id: int = 1,
) -> list[dict] | None:
    """Fetch one page of ERC-20 token transfer events for a wallet."""
    base_url = explorer_base or EXPLORER_BASE
    try:
        resp = await client.get(
            base_url,
            params={
                "chain_id" if "blockscout" in base_url else "chainid": chain_id,
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
    chain: str = "ethereum",
) -> dict:
    """
    Fetch token transfer history for a wallet and upsert stablecoin transfer
    edges into wallet_graph.wallet_edges.
    """
    chain_cfg = CHAIN_CONFIGS.get(chain, CHAIN_CONFIGS["ethereum"])
    explorer_base = chain_cfg["explorer_base"]
    chain_id = chain_cfg.get("chain_id", 1)
    scored_contracts = get_chain_contracts(chain)
    wallet_lower = wallet_address.lower()

    # Accumulate edges: (from, to) -> {count, total_value, first_ts, last_ts, tokens}
    edge_map: dict[tuple[str, str], dict] = {}
    total_transfers = 0
    pages_fetched = 0

    for page in range(1, max_pages + 1):
        transfers = await _fetch_tokentx_page(
            client, wallet_lower, api_key, page=page,
            explorer_base=explorer_base, chain_id=chain_id,
        )
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
                (from_address, to_address, chain, transfer_count, total_value_usd,
                 first_transfer_at, last_transfer_at, tokens_transferred, weight)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (from_address, to_address, chain) DO UPDATE SET
                transfer_count = wallet_graph.wallet_edges.transfer_count + EXCLUDED.transfer_count,
                total_value_usd = wallet_graph.wallet_edges.total_value_usd + EXCLUDED.total_value_usd,
                first_transfer_at = LEAST(wallet_graph.wallet_edges.first_transfer_at, EXCLUDED.first_transfer_at),
                last_transfer_at = GREATEST(wallet_graph.wallet_edges.last_transfer_at, EXCLUDED.last_transfer_at),
                tokens_transferred = wallet_graph.wallet_edges.tokens_transferred || EXCLUDED.tokens_transferred,
                weight = EXCLUDED.weight,
                updated_at = NOW()
            """,
            (from_addr, to_addr, chain, edge["count"], edge["total_value"],
             edge["first_ts"], edge["last_ts"], tokens_json, weight),
        )
        edges_upserted += 1

    # Update build status
    execute(
        """
        INSERT INTO wallet_graph.edge_build_status
            (wallet_address, chain, last_built_at, transfers_processed, edges_created, pages_fetched, status)
        VALUES (%s, %s, NOW(), %s, %s, %s, 'complete')
        ON CONFLICT (wallet_address, chain) DO UPDATE SET
            last_built_at = NOW(),
            transfers_processed = EXCLUDED.transfers_processed,
            edges_created = EXCLUDED.edges_created,
            pages_fetched = EXCLUDED.pages_fetched,
            status = 'complete'
        """,
        (wallet_lower, chain, total_transfers, edges_upserted, pages_fetched),
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
    chain: str = "ethereum",
) -> dict:
    """
    Batch edge builder. Queries wallets needing edge building and processes them.
    Delegates to Solana adapter for chain='solana'.
    """
    # Solana uses a different data source (Helius, not Blockscout/Etherscan)
    if chain == "solana":
        from app.indexer.solana_edges import run_solana_edge_builder
        return await run_solana_edge_builder(
            max_wallets=max_wallets,
            max_pages_per_wallet=max_pages_per_wallet,
        )

    order_clause = "w.total_stablecoin_value DESC NULLS LAST"
    if priority == "unbuilt":
        order_clause = "w.created_at ASC"

    # Include wallets that haven't been built in 7+ days (re-scan for new transfers)
    wallets = fetch_all(
        f"""
        SELECT w.address, w.total_stablecoin_value
        FROM wallet_graph.wallets w
        LEFT JOIN wallet_graph.edge_build_status e
            ON w.address = e.wallet_address AND e.chain = %s
        WHERE e.wallet_address IS NULL
           OR e.status = 'pending'
           OR e.last_built_at < NOW() - INTERVAL '7 days'
        ORDER BY {order_clause}
        LIMIT %s
        """,
        (chain, max_wallets),
    )

    # Count how many are fresh vs stale
    unbuilt = fetch_one(
        "SELECT COUNT(*) as cnt FROM wallet_graph.wallets w "
        "LEFT JOIN wallet_graph.edge_build_status e ON w.address = e.wallet_address AND e.chain = %s "
        "WHERE e.wallet_address IS NULL", (chain,)
    )
    stale = fetch_one(
        "SELECT COUNT(*) as cnt FROM wallet_graph.edge_build_status "
        "WHERE chain = %s AND last_built_at < NOW() - INTERVAL '7 days'", (chain,)
    )
    logger.error(
        f"[edge_builder] {chain}: {len(wallets)} candidates "
        f"(unbuilt={unbuilt['cnt'] if unbuilt else 0}, stale_7d={stale['cnt'] if stale else 0})"
    )

    if not wallets:
        logger.error(f"[edge_builder] {chain}: no wallets need edge building")
        return {"chain": chain, "wallets_processed": 0, "total_edges_created": 0, "total_transfers": 0}

    api_key = os.environ.get("ETHERSCAN_API_KEY", "") if chain == "ethereum" else ""
    total_edges = 0
    total_transfers = 0
    wallets_processed = 0

    async with httpx.AsyncClient() as client:
        for i, w in enumerate(wallets):
            try:
                result = await build_edges_for_wallet(
                    client, w["address"], api_key,
                    max_pages=max_pages_per_wallet,
                    chain=chain,
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
                    INSERT INTO wallet_graph.edge_build_status (wallet_address, chain, status)
                    VALUES (%s, %s, 'pending')
                    ON CONFLICT (wallet_address, chain) DO UPDATE SET status = 'pending'
                    """,
                    (w["address"], chain),
                )

    logger.error(
        f"[edge_builder] {chain}: examined {wallets_processed} wallets, "
        f"new_edges={total_edges}, transfers={total_transfers}"
    )

    # Attest edges for this chain
    try:
        from app.state_attestation import attest_state
        if total_edges > 0:
            attest_state("edges", [{"chain": chain, "wallets": wallets_processed, "edges": total_edges}], entity_id=chain)
    except Exception as ae:
        logger.debug(f"Edge attestation skipped for {chain}: {ae}")

    return {
        "chain": chain,
        "wallets_processed": wallets_processed,
        "total_edges_created": total_edges,
        "total_transfers": total_transfers,
    }


# =============================================================================
# Edge Decay — recalculate weights with time-decay multiplier
# =============================================================================

def decay_edges() -> dict:
    """
    Recalculate edge weights using a time-decay multiplier.

    decay_factor = max(0.1, 1.0 - (days_since_last_transfer / 180))
    new_weight = log10(total_value_usd + 1) * ln(transfer_count + 1) * decay_factor

    Skips edges with last_transfer_at within the last day (fresh edges).
    """
    result = execute(
        """
        UPDATE wallet_graph.wallet_edges
        SET
            weight = GREATEST(0.01,
                log(total_value_usd + 1)
                * ln(transfer_count + 1)
                * GREATEST(0.1, 1.0 - (EXTRACT(EPOCH FROM (NOW() - last_transfer_at)) / 86400.0 / 180.0))
            ),
            updated_at = NOW()
        WHERE last_transfer_at < NOW() - INTERVAL '1 day'
          AND last_transfer_at IS NOT NULL
        """,
    )

    # Count how many were updated
    count_row = fetch_one(
        """
        SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges
        WHERE updated_at > NOW() - INTERVAL '10 seconds'
        """
    )
    updated = count_row["cnt"] if count_row else 0
    logger.info(f"Edge decay: {updated} edges recalculated")
    return {"edges_decayed": updated}


# =============================================================================
# Edge Pruning — archive edges older than 180 days
# =============================================================================

def prune_stale_edges() -> dict:
    """
    Move edges with last_transfer_at older than 180 days to archive table.
    Returns count of edges archived.
    """
    # Count before archiving
    count_row = fetch_one(
        """
        SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges
        WHERE last_transfer_at < NOW() - INTERVAL '180 days'
        """
    )
    to_archive = count_row["cnt"] if count_row else 0

    if to_archive == 0:
        logger.info("Edge pruning: no stale edges to archive")
        return {"edges_archived": 0, "edges_remaining": 0}

    # Archive: copy to archive table
    execute(
        """
        INSERT INTO wallet_graph.wallet_edges_archive
            (id, from_address, to_address, transfer_count, total_value_usd,
             first_transfer_at, last_transfer_at, weight, tokens_transferred,
             created_at, updated_at)
        SELECT id, from_address, to_address, transfer_count, total_value_usd,
               first_transfer_at, last_transfer_at, weight, tokens_transferred,
               created_at, updated_at
        FROM wallet_graph.wallet_edges
        WHERE last_transfer_at < NOW() - INTERVAL '180 days'
        ON CONFLICT DO NOTHING
        """,
    )

    # Delete from live table
    execute(
        """
        DELETE FROM wallet_graph.wallet_edges
        WHERE last_transfer_at < NOW() - INTERVAL '180 days'
        """,
    )

    remaining_row = fetch_one("SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges")
    remaining = remaining_row["cnt"] if remaining_row else 0

    logger.info(f"Edge pruning: {to_archive} edges archived, {remaining} remaining")
    return {"edges_archived": to_archive, "edges_remaining": remaining}


# =============================================================================
# Sprint 3 background loop — high-throughput edge building
# =============================================================================

EDGE_BUILDER_BATCH_SIZE = 2000
EDGE_BUILDER_ETHERSCAN_CAP = 120_000


def _get_etherscan_24h_usage() -> int:
    try:
        row = fetch_one("""
            SELECT SUM(total_calls) AS total FROM api_usage_hourly
            WHERE provider = 'etherscan' AND hour > NOW() - INTERVAL '24 hours'
        """)
        return int(row["total"]) if row and row.get("total") else 0
    except Exception:
        return 0


async def edge_builder_background_loop():
    """Independent background loop for Sprint 3 edge graph density."""
    logger.error("[edge_builder_bg] background loop started")
    await asyncio.sleep(240)  # stagger behind other Phase 2 loops

    while True:
        try:
            logger.error("[edge_builder_bg] loop tick")

            usage = _get_etherscan_24h_usage()
            if usage > EDGE_BUILDER_ETHERSCAN_CAP:
                logger.error(f"[edge_builder_bg] PAUSED: Etherscan 24h usage {usage:,}/{EDGE_BUILDER_ETHERSCAN_CAP:,}")
                await asyncio.sleep(3600)
                continue

            # Check how many wallets are scannable
            scannable = fetch_one("""
                SELECT COUNT(*) AS cnt FROM wallet_graph.wallets w
                LEFT JOIN wallet_graph.edge_build_status e
                    ON w.address = e.wallet_address AND e.chain = 'ethereum'
                WHERE e.wallet_address IS NULL
                   OR e.last_built_at < NOW() - INTERVAL '24 hours'
            """)
            scannable_count = int(scannable["cnt"]) if scannable else 0

            if scannable_count == 0:
                logger.error("[edge_builder_bg] no wallets need scanning, sleeping 1h")
                await asyncio.sleep(3600)
                continue

            batch = min(EDGE_BUILDER_BATCH_SIZE, scannable_count)
            logger.error(f"[edge_builder_bg] {scannable_count} wallets need scanning, running batch of {batch}")

            result = await run_edge_builder(
                max_wallets=batch,
                max_pages_per_wallet=10,
                priority="value",
                chain="ethereum",
            )

            logger.error(
                f"[edge_builder_bg] BATCH SUMMARY: "
                f"wallets={result.get('wallets_processed', 0)}, "
                f"new_edges={result.get('total_edges_created', 0)}, "
                f"transfers={result.get('total_transfers', 0)}"
            )

            # Short sleep before next batch — continuous while there are wallets to scan
            await asyncio.sleep(300)

        except Exception as e:
            logger.error(f"[edge_builder_bg] ERROR: {type(e).__name__}: {e}")
            await asyncio.sleep(600)
