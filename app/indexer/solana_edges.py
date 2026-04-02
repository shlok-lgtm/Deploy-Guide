"""
Solana Wallet Edge Builder
===========================
Builds wallet-to-wallet transfer edges from SPL token transfer history
via Helius parsed transactions API.

Mirrors the EVM edge builder (edges.py) — same storage tables, same weight
function, same edge schema. Only the data source differs.

Helius free tier: 1M credits/month, 10 RPS.
"""

import os
import math
import asyncio
import json
import logging
from datetime import datetime, timezone

import httpx

from app.database import execute, fetch_all

logger = logging.getLogger(__name__)

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
HELIUS_API_URL = "https://api.helius.xyz"
RATE_LIMIT_DELAY = 0.15

# Known stablecoin mints on Solana
SOLANA_STABLECOIN_MINTS = {
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {"symbol": "USDC", "decimals": 6},
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {"symbol": "USDT", "decimals": 6},
}

SYSTEM_ADDRESSES = {
    None, "",
    "11111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
}

DRIFT_PROGRAM_ID = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"


def _compute_weight(total_value_usd: float, transfer_count: int, last_transfer_at: datetime) -> float:
    """Same weight function as EVM edge builder (edges.py:81-88)."""
    now = datetime.now(timezone.utc)
    if last_transfer_at.tzinfo is None:
        last_transfer_at = last_transfer_at.replace(tzinfo=timezone.utc)
    days_since = (now - last_transfer_at).days
    recency = max(0.1, 1.0 - (days_since / 365))
    return math.log10(1 + total_value_usd) * transfer_count * recency


async def _fetch_solana_transfers(
    client: httpx.AsyncClient,
    wallet_address: str,
    before_signature: str = None,
    limit: int = 100,
) -> list[dict]:
    """Fetch parsed token transfers for a Solana wallet via Helius."""
    url = f"{HELIUS_API_URL}/v0/addresses/{wallet_address}/transactions"
    params = {
        "api-key": HELIUS_API_KEY,
        "type": "TRANSFER",
        "limit": limit,
    }
    if before_signature:
        params["before"] = before_signature

    try:
        resp = await client.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"Helius transfers {resp.status_code} for {wallet_address[:12]}...")
            return []
        return resp.json()
    except Exception as e:
        logger.debug(f"Helius transfers error for {wallet_address[:12]}...: {e}")
        return []


async def build_solana_edges_for_wallet(
    client: httpx.AsyncClient,
    wallet_address: str,
    max_pages: int = 5,
) -> dict:
    """
    Fetch SPL token transfer history for a Solana wallet and upsert
    stablecoin transfer edges into wallet_graph.wallet_edges.

    Same storage schema as EVM edges — chain='solana'.
    """
    edge_map: dict[tuple[str, str], dict] = {}
    total_transfers = 0
    pages_fetched = 0
    last_signature = None

    for _ in range(max_pages):
        transfers = await _fetch_solana_transfers(
            client, wallet_address, before_signature=last_signature
        )
        await asyncio.sleep(RATE_LIMIT_DELAY)
        pages_fetched += 1

        if not transfers:
            break

        for tx in transfers:
            ts_unix = tx.get("timestamp")
            try:
                ts = datetime.fromtimestamp(ts_unix, tz=timezone.utc) if ts_unix else datetime.now(timezone.utc)
            except (ValueError, TypeError):
                ts = datetime.now(timezone.utc)

            sig = tx.get("signature", "")

            for tt in tx.get("tokenTransfers", []):
                mint = tt.get("mint", "")
                if mint not in SOLANA_STABLECOIN_MINTS:
                    continue

                from_addr = tt.get("fromUserAccount", "")
                to_addr = tt.get("toUserAccount", "")

                if from_addr in SYSTEM_ADDRESSES or to_addr in SYSTEM_ADDRESSES:
                    continue
                if not from_addr or not to_addr:
                    continue

                symbol = SOLANA_STABLECOIN_MINTS[mint]["symbol"]

                amount = tt.get("tokenAmount", 0)
                if not amount or not isinstance(amount, (int, float)):
                    continue
                value_usd = amount  # stablecoin ~ USD

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

            last_signature = sig

        if len(transfers) < 100:
            break

    # Upsert edges — same table as EVM, chain='solana'
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
            (from_addr, to_addr, "solana", edge["count"], edge["total_value"],
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
        (wallet_address, "solana", total_transfers, edges_upserted, pages_fetched),
    )

    return {
        "transfers_processed": total_transfers,
        "edges_upserted": edges_upserted,
        "pages_fetched": pages_fetched,
    }


async def run_solana_edge_builder(
    wallet_addresses: list[str] = None,
    max_pages_per_wallet: int = 5,
    max_wallets: int = 50,
) -> dict:
    """
    Build Solana edges. If wallet_addresses provided, uses those.
    Otherwise queries wallet_graph.wallets for Solana wallets needing edges.
    """
    if not HELIUS_API_KEY:
        logger.warning("HELIUS_API_KEY not set — skipping Solana edge builder")
        return {"chain": "solana", "wallets_processed": 0, "total_edges_created": 0}

    if wallet_addresses is None:
        rows = fetch_all(
            """
            SELECT w.address
            FROM wallet_graph.wallets w
            LEFT JOIN wallet_graph.edge_build_status e
                ON w.address = e.wallet_address AND e.chain = 'solana'
            WHERE w.chain = 'solana'
              AND (e.wallet_address IS NULL OR e.status = 'pending')
            ORDER BY w.total_stablecoin_value DESC NULLS LAST
            LIMIT %s
            """,
            (max_wallets,),
        )
        wallet_addresses = [r["address"] for r in rows]

    if not wallet_addresses:
        logger.info("No Solana wallets need edge building")
        return {"chain": "solana", "wallets_processed": 0, "total_edges_created": 0}

    total_edges = 0
    total_transfers = 0
    wallets_processed = 0

    async with httpx.AsyncClient() as client:
        for i, addr in enumerate(wallet_addresses):
            try:
                result = await build_solana_edges_for_wallet(
                    client, addr, max_pages=max_pages_per_wallet
                )
                total_edges += result["edges_upserted"]
                total_transfers += result["transfers_processed"]
                wallets_processed += 1

                if (i + 1) % 10 == 0:
                    logger.info(
                        f"Solana edge builder: {i+1}/{len(wallet_addresses)} wallets, "
                        f"{total_edges} edges, {total_transfers} transfers"
                    )
            except Exception as e:
                logger.error(f"Solana edge build failed for {addr[:12]}...: {e}")

    logger.info(
        f"Solana edge builder complete: {wallets_processed} wallets, "
        f"{total_edges} edges, {total_transfers} transfers"
    )
    return {
        "chain": "solana",
        "wallets_processed": wallets_processed,
        "total_edges_created": total_edges,
        "total_transfers": total_transfers,
    }


async def discover_drift_depositors(client: httpx.AsyncClient, limit: int = 50) -> list[str]:
    """
    Discover high-value Solana wallet addresses by querying recent large
    stablecoin transfers (>$1000) on USDC and USDT SPL mints via Helius.

    Queries each mint for TRANSFER-type transactions, extracts unique
    sender/receiver addresses from transfers exceeding the value threshold.
    """
    if not HELIUS_API_KEY:
        return []

    min_value_usd = 1000
    mints = [
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",   # USDT
    ]

    wallets: set[str] = set()

    for mint in mints:
        url = f"{HELIUS_API_URL}/v0/addresses/{mint}/transactions"
        params = {"api-key": HELIUS_API_KEY, "type": "TRANSFER", "limit": 100}

        try:
            resp = await client.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"Helius discovery {resp.status_code} for mint {mint[:8]}...")
                continue
            txns = resp.json()
        except Exception as e:
            logger.debug(f"Helius discovery error for mint {mint[:8]}...: {e}")
            continue

        for tx in txns:
            for tt in tx.get("tokenTransfers", []):
                amount = tt.get("tokenAmount", 0)
                if not isinstance(amount, (int, float)) or amount < min_value_usd:
                    continue
                for field in ["fromUserAccount", "toUserAccount"]:
                    addr = tt.get(field, "")
                    if addr and addr not in SYSTEM_ADDRESSES:
                        wallets.add(addr)

        await asyncio.sleep(RATE_LIMIT_DELAY)

    logger.info(f"Solana discovery: {len(wallets)} unique wallets from {len(mints)} mint queries")
    return list(wallets)[:limit]
