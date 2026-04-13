"""
Pool Wallet Collector
======================
Discovers wallets supplying stablecoins into scored protocol pools by querying
top holders of receipt tokens (aTokens, cTokens, Comet contracts, vault shares).

Uses the protocol_adapters module for the full receipt token registry covering
all 13 PSI-scored protocols across Ethereum, Base, and Arbitrum.

Follows the same patterns as app/indexer/expander.py:
  - Uses fetch_top_holders() from scanner.py
  - Rate-limited, idempotent, paginates via page cursor
  - Runs in the worker slow cycle under a 24h gate
"""

import os
import asyncio
import logging

import httpx

from app.database import fetch_all, fetch_one, execute
from app.indexer.config import EXPLORER_RATE_LIMIT_DELAY
from app.indexer.scanner import fetch_top_holders

logger = logging.getLogger(__name__)

# Zero address — skip mints/burns
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Per-chain Blockscout native API bases — each has its own 100K/day budget
BLOCKSCOUT_CHAINS = {
    "ethereum": "https://eth.blockscout.com/api",
    "base": "https://base.blockscout.com/api",
    "arbitrum": "https://arbitrum.blockscout.com/api",
}

# Etherscan V2 chain IDs (fallback if Blockscout fails)
CHAIN_IDS = {"ethereum": 1, "base": 8453, "arbitrum": 42161}


async def _fetch_top_holders_multichain(
    client: httpx.AsyncClient,
    contract: str,
    api_key: str,
    chain: str = "ethereum",
    page: int = 1,
    offset: int = 100,
) -> list[str]:
    """
    Fetch top token holders — prefers Blockscout per-chain native API
    (saves Etherscan budget). Falls back to Etherscan V2 if Blockscout fails.

    Blockscout per-chain instances each have their own 100K/day budget,
    so using them for holder queries is essentially free relative to
    the shared Etherscan budget.
    """
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    blockscout_base = BLOCKSCOUT_CHAINS.get(chain)

    if blockscout_base:
        # Try Blockscout first
        try:
            await rate_limiter.acquire("blockscout")
            import time
            start = time.time()

            resp = await client.get(
                blockscout_base,
                params={
                    "module": "token",
                    "action": "getTokenHolders",
                    "contractaddress": contract,
                    "page": page,
                    "offset": offset,
                },
                timeout=15.0,
            )
            latency = int((time.time() - start) * 1000)
            track_api_call("blockscout", f"/getTokenHolders/{chain}",
                           caller="pool_wallet_collector", status=resp.status_code,
                           latency_ms=latency)

            data = resp.json()
            if data.get("status") == "1" and isinstance(data.get("result"), list):
                rate_limiter.report_success("blockscout")
                return [
                    (h.get("address") or h.get("TokenHolderAddress", "")).lower()
                    for h in data["result"]
                    if h.get("address") or h.get("TokenHolderAddress")
                ]

            # Status != 1 — try Etherscan fallback
        except Exception as e:
            logger.debug(f"Blockscout holder fetch failed for {contract[:10]}… on {chain}: {e}")

    # Fallback: Etherscan V2 (uses shared Etherscan budget)
    if chain == "ethereum":
        return await fetch_top_holders(client, contract, api_key, page=page, offset=offset)

    chain_id = CHAIN_IDS.get(chain, 1)
    try:
        from app.shared_rate_limiter import rate_limiter as _rl
        await _rl.acquire("etherscan")

        resp = await client.get(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": chain_id,
                "module": "token",
                "action": "tokenholderlist",
                "contractaddress": contract,
                "page": page,
                "offset": offset,
                "apikey": api_key,
            },
            timeout=15.0,
        )
        data = resp.json()
        if data.get("status") == "1" and isinstance(data.get("result"), list):
            return [
                (h.get("TokenHolderAddress") or h.get("address", "")).lower()
                for h in data["result"]
                if h.get("TokenHolderAddress") or h.get("address")
            ]
        return []
    except Exception as e:
        logger.debug(f"Etherscan holder fetch fallback failed for {contract[:10]}… on {chain}: {e}")
        return []


async def run_pool_wallet_collection(max_pages_per_pool: int = 30) -> dict:
    """
    Discover wallets holding receipt tokens for all protocol adapters.

    For each (protocol, stablecoin, chain) tuple in the full registry:
      1. Fetch top holders of the receipt token contract
      2. Upsert into protocol_pool_wallets
      3. Seed into wallet_graph.wallets for graph expansion

    Args:
        max_pages_per_pool: Max pages of holders to fetch per pool (100 per page).

    Returns:
        Summary dict with pools_processed, wallets_discovered, wallets_seeded.
    """
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        logger.warning("No ETHERSCAN_API_KEY — skipping pool wallet collection")
        return {"pools_processed": 0, "wallets_discovered": 0, "wallets_seeded": 0}

    # Load full receipt token registry from protocol adapters
    from app.collectors.protocol_adapters import get_all_receipt_tokens
    registry = get_all_receipt_tokens()

    if not registry:
        logger.warning("Empty receipt token registry")
        return {"pools_processed": 0, "wallets_discovered": 0, "wallets_seeded": 0}

    logger.info(f"Pool wallet collection: {len(registry)} receipt tokens across all protocols")

    # Pre-fetch existing wallet addresses to avoid redundant inserts
    existing_rows = fetch_all("SELECT address FROM wallet_graph.wallets")
    existing_addrs = {row["address"].lower() for row in existing_rows}

    total_discovered = 0
    total_seeded = 0
    pools_processed = 0
    by_protocol = {}

    async with httpx.AsyncClient() as client:
        for (protocol_slug, symbol, chain), receipt_token in registry.items():
            contract = receipt_token.contract
            label = receipt_token.label

            logger.info(f"Pool wallet discovery: {protocol_slug}/{symbol}/{chain} ({label})")

            pool_discovered = 0
            pool_seeded = 0

            for page in range(1, max_pages_per_pool + 1):
                holders = await _fetch_top_holders_multichain(
                    client, contract, api_key,
                    chain=chain, page=page, offset=100,
                )
                await asyncio.sleep(EXPLORER_RATE_LIMIT_DELAY)

                if not holders:
                    logger.info(f"  {label}: holder list exhausted at page {page}")
                    break

                for addr in holders:
                    if not addr or not addr.startswith("0x"):
                        continue
                    addr_lower = addr.lower()
                    if addr_lower == ZERO_ADDRESS:
                        continue

                    # Upsert into protocol_pool_wallets
                    try:
                        execute("""
                            INSERT INTO protocol_pool_wallets
                                (protocol_slug, stablecoin_symbol, chain,
                                 wallet_address, pool_contract_address,
                                 discovered_at, last_seen)
                            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                            ON CONFLICT (protocol_slug, stablecoin_symbol, chain, wallet_address)
                            DO UPDATE SET last_seen = NOW()
                        """, (
                            protocol_slug, symbol, chain,
                            addr_lower, contract.lower(),
                        ))
                        pool_discovered += 1
                    except Exception as e:
                        logger.debug(f"  Failed to upsert pool wallet {addr_lower[:10]}…: {e}")

                    # Seed into wallet_graph.wallets if new
                    if addr_lower not in existing_addrs:
                        try:
                            execute("""
                                INSERT INTO wallet_graph.wallets
                                    (address, source, label, created_at, updated_at)
                                VALUES (%s, 'pool_discovery', %s, NOW(), NOW())
                                ON CONFLICT (address) DO NOTHING
                            """, (
                                addr_lower,
                                f"pool:{protocol_slug}:{symbol}",
                            ))
                            existing_addrs.add(addr_lower)
                            pool_seeded += 1
                        except Exception as e:
                            logger.debug(f"  Failed to seed wallet {addr_lower[:10]}…: {e}")

            logger.info(
                f"  {label}: {pool_discovered} holders stored, "
                f"{pool_seeded} new wallets seeded"
            )
            total_discovered += pool_discovered
            total_seeded += pool_seeded
            pools_processed += 1
            by_protocol.setdefault(protocol_slug, {"pools": 0, "wallets": 0})
            by_protocol[protocol_slug]["pools"] += 1
            by_protocol[protocol_slug]["wallets"] += pool_discovered

    logger.info(
        f"Pool wallet collection complete: {pools_processed} pools across "
        f"{len(by_protocol)} protocols, {total_discovered} holders stored, "
        f"{total_seeded} new wallets seeded"
    )

    # Attest state
    try:
        from app.state_attestation import attest_state
        if total_discovered > 0:
            attest_state("pool_wallet_discovery", [{
                "pools": pools_processed,
                "discovered": total_discovered,
                "seeded": total_seeded,
                "protocols": list(by_protocol.keys()),
            }])
    except Exception as e:
        logger.debug(f"Pool wallet attestation skipped: {e}")

    return {
        "pools_processed": pools_processed,
        "wallets_discovered": total_discovered,
        "wallets_seeded": total_seeded,
        "by_protocol": by_protocol,
    }
