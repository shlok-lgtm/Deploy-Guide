"""
Pool Wallet Collector
======================
Discovers wallets supplying stablecoins into scored protocol pools by querying
top holders of receipt tokens (aTokens, cTokens, etc.).

For each protocol-stablecoin pair in the RECEIPT_TOKEN_REGISTRY, fetches holder
addresses and stores them in protocol_pool_wallets.  Also seeds new addresses
into wallet_graph.wallets so the edge builder and risk scorer pick them up on
next cycle.

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

# =============================================================================
# Receipt Token Registry — hardcoded, easy to extend
# =============================================================================
# Key: (protocol_slug, stablecoin_symbol, chain)
# Value: dict with contract address and label
#
# Addresses verified against bgd-labs/aave-address-book (2024-06-23)
# and on-chain UNDERLYING_ASSET_ADDRESS() calls.

RECEIPT_TOKEN_REGISTRY = {
    # Aave V3 Ethereum
    ("aave", "USDC", "ethereum"): {
        "contract": "0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c",
        "label": "Aave V3 aEthUSDC",
    },
    ("aave", "USDT", "ethereum"): {
        "contract": "0x23878914EFE38d27C4D67Ab83ed1b93A74D4086a",
        "label": "Aave V3 aEthUSDT",
    },
}

# Zero address — skip mints/burns
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


async def run_pool_wallet_collection(max_pages_per_pool: int = 5) -> dict:
    """
    Discover wallets holding receipt tokens for each registry entry.

    For each (protocol, stablecoin, chain) tuple:
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

    # Pre-fetch existing wallet addresses to avoid redundant inserts
    existing_rows = fetch_all("SELECT address FROM wallet_graph.wallets")
    existing_addrs = {row["address"].lower() for row in existing_rows}

    total_discovered = 0
    total_seeded = 0
    pools_processed = 0

    async with httpx.AsyncClient() as client:
        for (protocol_slug, symbol, chain), entry in RECEIPT_TOKEN_REGISTRY.items():
            contract = entry["contract"]
            label = entry["label"]

            logger.info(f"Pool wallet discovery: {protocol_slug}/{symbol}/{chain} ({label})")

            pool_discovered = 0
            pool_seeded = 0

            for page in range(1, max_pages_per_pool + 1):
                holders = await fetch_top_holders(
                    client, contract, api_key,
                    page=page, offset=100,
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

    logger.info(
        f"Pool wallet collection complete: {pools_processed} pools, "
        f"{total_discovered} holders stored, {total_seeded} new wallets seeded"
    )

    # Attest state
    try:
        from app.state_attestation import attest_state
        if total_discovered > 0:
            attest_state("pool_wallet_discovery", [{
                "pools": pools_processed,
                "discovered": total_discovered,
                "seeded": total_seeded,
            }])
    except Exception as e:
        logger.debug(f"Pool wallet attestation skipped: {e}")

    return {
        "pools_processed": pools_processed,
        "wallets_discovered": total_discovered,
        "wallets_seeded": total_seeded,
    }
