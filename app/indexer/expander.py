"""
Wallet Indexer — Expander
=========================
Uses surplus API budget to seed and index new wallets from top-holder lists
of under-covered stablecoins. This is how 44K wallets becomes 100K+ without
spending more money.

Strategy:
  1. Check which stablecoins have the fewest indexed wallets
  2. Fetch next batch of top holders for the least-covered stablecoin
  3. Store new wallet addresses into the graph
  4. Repeat until budget exhausted

Note: The actual token-balance scanning of these new wallets happens in the
next regular pipeline refresh cycle. This module only seeds addresses.
"""

import os
import asyncio
import logging

import httpx

from app.database import fetch_all, fetch_one, execute
from app.indexer.config import EXPLORER_RATE_LIMIT_DELAY
from app.indexer.scanner import fetch_top_holders

logger = logging.getLogger(__name__)


def _get_coverage_gaps() -> list[dict]:
    """
    Find stablecoins with the fewest indexed wallets.
    Returns list sorted by indexed_wallets ASC (least covered first).
    """
    rows = fetch_all("""
        SELECT
            s.id AS stablecoin_id,
            s.symbol,
            s.contract_address,
            COUNT(DISTINCT wh.wallet_address) AS indexed_wallets
        FROM stablecoins s
        LEFT JOIN wallet_graph.wallet_holdings wh
            ON LOWER(wh.token_address) = LOWER(s.contract_address)
        WHERE s.contract_address IS NOT NULL
          AND s.scoring_enabled = TRUE
        GROUP BY s.id, s.symbol, s.contract_address
        ORDER BY indexed_wallets ASC
    """)
    return rows


def _get_last_seeded_page(contract_address: str) -> int:
    """
    Track how many pages of top holders we've already fetched for this contract.
    Uses a simple counter in the budget metadata. Returns 0 if never seeded.
    """
    row = fetch_one(
        """
        SELECT COALESCE(
            (SELECT wallet_expansion_calls_used FROM ops.api_budget
             WHERE budget_date = CURRENT_DATE AND provider = 'etherscan'),
            0
        ) AS calls
        """
    )
    # We track pages via a lightweight approach: check how many wallets exist
    # that were sourced from expansion for this contract today.
    count_row = fetch_one(
        """
        SELECT COUNT(*) AS cnt FROM wallet_graph.wallets
        WHERE source = 'expansion'
          AND created_at >= CURRENT_DATE
        """
    )
    # Approximate: each page = 100 holders
    return (count_row["cnt"] if count_row else 0) // 100


async def run_wallet_expansion(max_etherscan_calls: int) -> dict:
    """
    Use surplus API budget to seed new wallets from under-covered stablecoins.

    Args:
        max_etherscan_calls: Maximum Etherscan API calls to use.

    Returns:
        Summary dict with calls used and wallets seeded.
    """
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        logger.warning("No ETHERSCAN_API_KEY — skipping wallet expansion")
        return {"etherscan_calls_used": 0, "new_wallets_seeded": 0}

    coverage = _get_coverage_gaps()
    if not coverage:
        logger.info("No stablecoins with contracts found for expansion")
        return {"etherscan_calls_used": 0, "new_wallets_seeded": 0}

    # Get existing wallet addresses to avoid duplicates
    existing_rows = fetch_all("SELECT address FROM wallet_graph.wallets")
    existing_addrs = {row["address"].lower() for row in existing_rows}

    calls_used = 0
    new_wallets = 0
    page_size = 100

    async with httpx.AsyncClient() as client:
        for coin in coverage:
            if calls_used >= max_etherscan_calls - 100:  # Reserve buffer
                logger.info(
                    f"Expansion budget nearly exhausted ({calls_used}/{max_etherscan_calls})"
                )
                break

            contract = coin["contract_address"]
            symbol = coin["symbol"]
            current_coverage = coin["indexed_wallets"]

            # Fetch multiple pages of top holders
            pages_to_fetch = min(
                5,  # Max 5 pages per coin per expansion run
                (max_etherscan_calls - calls_used) // 1,  # 1 call per page
            )

            coin_new = 0
            for page in range(1, pages_to_fetch + 1):
                if calls_used >= max_etherscan_calls - 100:
                    break

                holders = await fetch_top_holders(
                    client, contract, api_key,
                    page=page, offset=page_size,
                )
                calls_used += 1
                await asyncio.sleep(EXPLORER_RATE_LIMIT_DELAY)

                if not holders:
                    break  # No more pages

                for addr in holders:
                    if addr and addr.startswith("0x") and addr.lower() not in existing_addrs:
                        execute(
                            """
                            INSERT INTO wallet_graph.wallets
                                (address, source, label, created_at, updated_at)
                            VALUES (%s, 'expansion', %s, NOW(), NOW())
                            ON CONFLICT (address) DO NOTHING
                            """,
                            (addr, f"expansion:{symbol}"),
                        )
                        existing_addrs.add(addr.lower())
                        coin_new += 1

            if coin_new > 0:
                logger.info(
                    f"  {symbol}: seeded {coin_new} new wallets "
                    f"(was {current_coverage} indexed)"
                )
                new_wallets += coin_new

    logger.info(
        f"Wallet expansion complete: {new_wallets} new wallets seeded, "
        f"{calls_used} Etherscan calls used"
    )

    return {
        "etherscan_calls_used": calls_used,
        "new_wallets_seeded": new_wallets,
    }
