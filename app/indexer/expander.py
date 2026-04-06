"""
Wallet Indexer — Expander
=========================
Uses surplus API budget to seed and index new wallets from top-holder lists
of under-covered stablecoins. This is how 44K wallets becomes 100K+ without
spending more money.

Strategy:
  1. Check which stablecoins have the fewest indexed wallets
  2. Fetch the NEXT batch of top holders (continuing from last run's page)
  3. Store new wallet addresses into the graph
  4. Update the per-stablecoin page cursor so tomorrow picks up deeper

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
    Find stablecoins with the fewest indexed wallets that haven't been exhausted.
    Returns list sorted by indexed_wallets ASC (least covered first).
    Includes expansion_last_page so the caller knows where to resume.
    """
    rows = fetch_all("""
        SELECT
            s.id AS stablecoin_id,
            s.symbol,
            s.contract,
            s.expansion_last_page,
            s.expansion_exhausted,
            COUNT(DISTINCT wh.wallet_address) AS indexed_wallets
        FROM stablecoins s
        LEFT JOIN wallet_graph.wallet_holdings wh
            ON LOWER(wh.token_address) = LOWER(s.contract)
        WHERE s.contract IS NOT NULL
          AND s.scoring_enabled = TRUE
          AND (s.expansion_exhausted IS FALSE OR s.expansion_exhausted IS NULL)
        GROUP BY s.id, s.symbol, s.contract, s.expansion_last_page, s.expansion_exhausted
        ORDER BY indexed_wallets ASC
    """)
    return rows


async def run_wallet_expansion(max_etherscan_calls: int) -> dict:
    """
    Use surplus API budget to seed new wallets from under-covered stablecoins.
    Resumes from where the previous run left off using expansion_last_page.

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
        logger.info("No stablecoins with contracts found for expansion (all exhausted or none enabled)")
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

            contract = coin["contract"]
            symbol = coin["symbol"]
            stablecoin_id = coin["stablecoin_id"]
            current_coverage = coin["indexed_wallets"]
            start_page = (coin["expansion_last_page"] or 0) + 1

            # Fetch up to 5 pages per coin per run, continuing from last cursor
            pages_to_fetch = min(
                5,
                (max_etherscan_calls - calls_used),
            )

            coin_new = 0
            last_page_reached = start_page - 1
            exhausted = False

            for page in range(start_page, start_page + pages_to_fetch):
                if calls_used >= max_etherscan_calls - 100:
                    break

                holders = await fetch_top_holders(
                    client, contract, api_key,
                    page=page, offset=page_size,
                )
                calls_used += 1
                await asyncio.sleep(EXPLORER_RATE_LIMIT_DELAY)

                if not holders:
                    # Empty page — this stablecoin's holder list is exhausted
                    exhausted = True
                    logger.info(f"  {symbol}: holder list exhausted at page {page}")
                    break

                last_page_reached = page

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

            # Persist pagination cursor
            if exhausted:
                execute(
                    "UPDATE stablecoins SET expansion_exhausted = TRUE WHERE id = %s",
                    (stablecoin_id,),
                )
            elif last_page_reached >= start_page:
                execute(
                    "UPDATE stablecoins SET expansion_last_page = %s WHERE id = %s",
                    (last_page_reached, stablecoin_id),
                )

            if coin_new > 0:
                logger.info(
                    f"  {symbol}: seeded {coin_new} new wallets "
                    f"(pages {start_page}–{last_page_reached}, was {current_coverage} indexed)"
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
