"""
Wallet Indexer — Expander
=========================
Uses surplus API budget to seed and index new wallets from top-holder lists
of under-covered stablecoins. Supports dual-provider mode (Etherscan +
Blockscout concurrently on disjoint page ranges) for 2-3x throughput.

Strategy:
  1. Check which stablecoins have the fewest indexed wallets
  2. Fetch the NEXT batch of top holders (continuing from last run's page)
  3. Store new wallet addresses into the graph
  4. Update the per-stablecoin page cursor so tomorrow picks up deeper
"""

import os
import asyncio
import logging
import time

import httpx

from app.database import fetch_all, fetch_one, execute
from app.indexer.config import EXPLORER_RATE_LIMIT_DELAY
from app.indexer.scanner import fetch_top_holders, _PROVIDER_CONFIGS

logger = logging.getLogger(__name__)

EXPANDER_DUAL_PROVIDER = os.environ.get("EXPANDER_DUAL_PROVIDER", "false").lower() == "true"
EXPANDER_PAGES_PER_CYCLE = int(os.environ.get("EXPANDER_PAGES_PER_CYCLE", "10" if EXPANDER_DUAL_PROVIDER else "5"))


def _get_coverage_gaps() -> list[dict]:
    """Find stablecoins with the fewest indexed wallets that haven't been exhausted."""
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


def _persist_expansion_batch_sync(new_addrs: list[tuple], stablecoin_id: str, exhausted: bool, last_page: int | None) -> int:
    """Sync helper: insert addresses + update pagination cursor in one transaction."""
    from app.database import get_cursor
    seeded = 0
    with get_cursor() as cur:
        for addr, label in new_addrs:
            try:
                cur.execute(
                    """INSERT INTO wallet_graph.wallets
                       (address, source, label, created_at, updated_at)
                       VALUES (%s, 'expansion', %s, NOW(), NOW())
                       ON CONFLICT (address) DO NOTHING""",
                    (addr, label),
                )
                seeded += 1
            except Exception as e:
                logger.error(f"expansion insert failed for {addr}: {e}")

        if exhausted:
            cur.execute(
                "UPDATE stablecoins SET expansion_exhausted = TRUE WHERE id = %s",
                (stablecoin_id,),
            )
        elif last_page is not None:
            cur.execute(
                "UPDATE stablecoins SET expansion_last_page = %s WHERE id = %s",
                (last_page, stablecoin_id),
            )
    return seeded


def _store_cycle_stats_sync(stats: dict):
    """Persist one expander_cycle_stats row."""
    try:
        execute(
            """INSERT INTO expander_cycle_stats
               (stablecoin_id, etherscan_pages_fetched, blockscout_pages_fetched,
                etherscan_addresses_returned, blockscout_addresses_returned,
                new_wallets_persisted, duplicates_skipped, cursor_advanced_to,
                cycle_duration_ms)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                stats["stablecoin_id"],
                stats.get("etherscan_pages", 0),
                stats.get("blockscout_pages", 0),
                stats.get("etherscan_addrs", 0),
                stats.get("blockscout_addrs", 0),
                stats.get("new_wallets", 0),
                stats.get("duplicates", 0),
                stats.get("cursor_to"),
                stats.get("duration_ms"),
            ),
        )
    except Exception as e:
        logger.debug(f"Failed to store expander cycle stats: {e}")


async def _fetch_pages_for_provider(
    client: httpx.AsyncClient,
    contract: str,
    api_key: str,
    pages: list[int],
    provider: str,
    page_size: int = 100,
) -> tuple[list[str], int, bool]:
    """Fetch multiple pages from a single provider sequentially with its own rate limit.

    Returns (addresses, max_page_reached, saw_empty_page).
    """
    cfg = _PROVIDER_CONFIGS.get(provider, {})
    delay = cfg.get("rate_limit_delay", 0.22)

    all_addrs = []
    max_page = 0
    saw_empty = False

    for page in pages:
        holders = await fetch_top_holders(
            client, contract, api_key,
            page=page, offset=page_size,
            provider=provider,
        )
        await asyncio.sleep(delay)

        if not holders:
            saw_empty = True
            break

        max_page = page
        all_addrs.extend(holders)

    return all_addrs, max_page, saw_empty


async def _fetch_holders_dual(
    client: httpx.AsyncClient,
    contract: str,
    etherscan_key: str,
    blockscout_key: str,
    start_page: int,
    pages_to_fetch: int,
) -> dict:
    """Fetch top holders from BOTH providers concurrently on disjoint page ranges.

    Etherscan handles ODD pages, Blockscout handles EVEN pages.
    Both run concurrently via asyncio.gather, each with its own rate limit.
    Results are union'd and deduped on lowercased address.
    """
    # Partition pages: odd for etherscan, even for blockscout
    all_pages = list(range(start_page, start_page + pages_to_fetch))
    etherscan_pages = [p for p in all_pages if p % 2 == 1]
    blockscout_pages = [p for p in all_pages if p % 2 == 0]

    # If start_page is even, blockscout gets the first page; if odd, etherscan does.
    # Both lists are non-empty as long as pages_to_fetch >= 2.

    eth_result, bs_result = await asyncio.gather(
        _fetch_pages_for_provider(client, contract, etherscan_key, etherscan_pages, "etherscan"),
        _fetch_pages_for_provider(client, contract, blockscout_key, blockscout_pages, "blockscout"),
    )

    eth_addrs, eth_max_page, eth_empty = eth_result
    bs_addrs, bs_max_page, bs_empty = bs_result

    # Dedup across providers
    seen = set()
    deduped = []
    for addr in eth_addrs + bs_addrs:
        a = addr.lower()
        if a not in seen:
            seen.add(a)
            deduped.append(a)

    return {
        "addresses": deduped,
        "etherscan_addrs": len(eth_addrs),
        "blockscout_addrs": len(bs_addrs),
        "etherscan_pages": len(etherscan_pages) if not eth_empty else etherscan_pages.index(etherscan_pages[-1]) + 1 if etherscan_pages else 0,
        "blockscout_pages": len(blockscout_pages) if not bs_empty else blockscout_pages.index(blockscout_pages[-1]) + 1 if blockscout_pages else 0,
        "max_page": max(eth_max_page, bs_max_page, start_page - 1),
        # Exhausted only if BOTH providers returned empty
        "exhausted": eth_empty and bs_empty,
        "eth_pages_list": etherscan_pages,
        "bs_pages_list": blockscout_pages,
    }


async def run_wallet_expansion(max_etherscan_calls: int) -> dict:
    """
    Use surplus API budget to seed new wallets from under-covered stablecoins.
    Supports dual-provider mode (EXPANDER_DUAL_PROVIDER=true) for 2-3x throughput.
    """
    etherscan_key = os.environ.get("ETHERSCAN_API_KEY", "")
    blockscout_key = os.environ.get("BLOCKSCOUT_API_KEY", "")

    dual_mode = EXPANDER_DUAL_PROVIDER and etherscan_key and blockscout_key
    if EXPANDER_DUAL_PROVIDER and not dual_mode:
        missing = []
        if not etherscan_key:
            missing.append("ETHERSCAN_API_KEY")
        if not blockscout_key:
            missing.append("BLOCKSCOUT_API_KEY")
        logger.warning(f"EXPANDER_DUAL_PROVIDER=true but missing {', '.join(missing)} — falling back to single provider")

    api_key = etherscan_key or blockscout_key
    if not api_key:
        logger.warning("No API key available — skipping wallet expansion")
        return {"etherscan_calls_used": 0, "new_wallets_seeded": 0}

    coverage = await asyncio.to_thread(_get_coverage_gaps)
    if not coverage:
        logger.info("No stablecoins with contracts found for expansion (all exhausted or none enabled)")
        return {"etherscan_calls_used": 0, "new_wallets_seeded": 0}

    existing_rows = await asyncio.to_thread(fetch_all, "SELECT address FROM wallet_graph.wallets")
    existing_addrs = {row["address"].lower() for row in existing_rows}

    calls_used = 0
    new_wallets = 0
    page_size = 100
    pages_per_coin = EXPANDER_PAGES_PER_CYCLE

    async with httpx.AsyncClient() as client:
        for coin in coverage:
            if calls_used >= max_etherscan_calls - 100:
                logger.info(f"Expansion budget nearly exhausted ({calls_used}/{max_etherscan_calls})")
                break

            contract = coin["contract"]
            symbol = coin["symbol"]
            stablecoin_id = coin["stablecoin_id"]
            current_coverage = coin["indexed_wallets"]
            start_page = (coin["expansion_last_page"] or 0) + 1
            coin_start = time.monotonic()

            actual_pages = min(pages_per_coin, max_etherscan_calls - calls_used)

            if dual_mode and actual_pages >= 2:
                # Dual-provider: Etherscan odd pages, Blockscout even pages
                result = await _fetch_holders_dual(
                    client, contract, etherscan_key, blockscout_key,
                    start_page, actual_pages,
                )
                holders = result["addresses"]
                max_page = result["max_page"]
                exhausted = result["exhausted"]
                calls_used += result["etherscan_pages"] + result["blockscout_pages"]

                # Filter new addresses
                pending_addrs = []
                for addr in holders:
                    if addr and addr.startswith("0x") and addr not in existing_addrs:
                        pending_addrs.append((addr, f"expansion:{symbol}"))
                        existing_addrs.add(addr)

                coin_new = 0
                if pending_addrs:
                    coin_new = await asyncio.to_thread(
                        _persist_expansion_batch_sync,
                        pending_addrs, stablecoin_id, False, None,
                    )

                # Persist cursor
                cursor_to = max_page if (not exhausted and max_page >= start_page) else None
                if exhausted or cursor_to is not None:
                    await asyncio.to_thread(
                        _persist_expansion_batch_sync,
                        [], stablecoin_id, exhausted, cursor_to,
                    )

                duration_ms = int((time.monotonic() - coin_start) * 1000)
                await asyncio.to_thread(_store_cycle_stats_sync, {
                    "stablecoin_id": stablecoin_id,
                    "etherscan_pages": result["etherscan_pages"],
                    "blockscout_pages": result["blockscout_pages"],
                    "etherscan_addrs": result["etherscan_addrs"],
                    "blockscout_addrs": result["blockscout_addrs"],
                    "new_wallets": coin_new,
                    "duplicates": len(holders) - len(pending_addrs),
                    "cursor_to": cursor_to,
                    "duration_ms": duration_ms,
                })

                if coin_new > 0:
                    logger.info(
                        f"  {symbol}: +{coin_new} wallets (etherscan pages "
                        f"{','.join(str(p) for p in result['eth_pages_list'])}; "
                        f"blockscout pages {','.join(str(p) for p in result['bs_pages_list'])}; "
                        f"was {current_coverage} indexed)"
                    )
                new_wallets += coin_new

            else:
                # Single-provider mode (original behavior)
                coin_new = 0
                last_page_reached = start_page - 1
                exhausted = False

                for page in range(start_page, start_page + actual_pages):
                    if calls_used >= max_etherscan_calls - 100:
                        break

                    holders = await fetch_top_holders(
                        client, contract, api_key,
                        page=page, offset=page_size,
                    )
                    calls_used += 1
                    await asyncio.sleep(EXPLORER_RATE_LIMIT_DELAY)

                    if not holders:
                        exhausted = True
                        logger.info(f"  {symbol}: holder list exhausted at page {page}")
                        break

                    last_page_reached = page

                    pending_addrs = []
                    for addr in holders:
                        if addr and addr.startswith("0x") and addr.lower() not in existing_addrs:
                            pending_addrs.append((addr.lower(), f"expansion:{symbol}"))
                            existing_addrs.add(addr.lower())

                    if pending_addrs:
                        seeded = await asyncio.to_thread(
                            _persist_expansion_batch_sync,
                            pending_addrs, stablecoin_id, False, None,
                        )
                        coin_new += seeded

                last_page_val = last_page_reached if (not exhausted and last_page_reached >= start_page) else None
                if exhausted or last_page_val is not None:
                    await asyncio.to_thread(
                        _persist_expansion_batch_sync,
                        [], stablecoin_id, exhausted, last_page_val,
                    )

                if coin_new > 0:
                    logger.info(
                        f"  {symbol}: seeded {coin_new} new wallets "
                        f"(pages {start_page}–{last_page_reached}, was {current_coverage} indexed)"
                    )
                    new_wallets += coin_new

    mode_label = "dual-provider" if dual_mode else "single-provider"
    logger.info(
        f"Wallet expansion complete ({mode_label}): {new_wallets} new wallets seeded, "
        f"{calls_used} API calls used"
    )

    return {
        "etherscan_calls_used": calls_used,
        "new_wallets_seeded": new_wallets,
        "mode": mode_label,
    }
