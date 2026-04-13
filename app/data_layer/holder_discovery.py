"""
Holder Discovery — Deep Stablecoin Holder Expansion via Blockscout
===================================================================
Uses the 90K/day Blockscout budget (per chain instance) for deep holder
list pagination across all 36 SII stablecoins on all 3 chains.

Blockscout per-chain instances:
- eth.blockscout.com: 100K credits/day
- base.blockscout.com: 100K credits/day
- arbitrum.blockscout.com: 100K credits/day
Total: 300K credits/day across 3 chains

This collector uses ~90K of that (30K per chain) to discover new wallet
addresses by paginating deep into token holder lists.

Feeds:
- wallet_graph.wallets (new addresses for scoring/edge building)
- SII holder distribution components (concentration, gini, etc.)

Schedule: Daily via enrichment pipeline
"""

import logging
import os
import time
from datetime import datetime, timezone

import httpx

from app.database import fetch_all, execute, get_cursor

logger = logging.getLogger(__name__)

# Per-chain Blockscout APIs
BLOCKSCOUT_CHAINS = {
    "ethereum": {
        "base_url": "https://eth.blockscout.com/api",
        "budget": 30_000,  # 30K of 100K per chain
    },
    "base": {
        "base_url": "https://base.blockscout.com/api",
        "budget": 30_000,
    },
    "arbitrum": {
        "base_url": "https://arbitrum.blockscout.com/api",
        "budget": 30_000,
    },
}

# Stablecoin contracts per chain
STABLECOIN_CONTRACTS = {
    "ethereum": {
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
        "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
        "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
        "0x853d955acef822db058eb8505911ed77f175b99e": "FRAX",
        "0x6c3ea9036406852006290770bedfcaba0e23a0e8": "PYUSD",
        "0x4c9edd5852cd905f086c759e8383e09bff1e68b3": "USDe",
        "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409": "FDUSD",
        "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d": "USD1",
    },
    "base": {
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": "USDC",
        "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2": "USDT",
        "0x50c5725949a6f0c72e6c4a641f24049a917db0cb": "DAI",
    },
    "arbitrum": {
        "0xaf88d065e77c8cc2239327c5edb3a432268e5831": "USDC",
        "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": "USDT",
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": "DAI",
    },
}

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


async def _fetch_holders_blockscout(
    client: httpx.AsyncClient,
    base_url: str,
    contract: str,
    page: int = 1,
    offset: int = 100,
) -> list[str]:
    """Fetch token holders from a per-chain Blockscout instance."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("blockscout")

    start = time.time()
    try:
        resp = await client.get(
            base_url,
            params={
                "module": "token",
                "action": "getTokenHolders",
                "contractaddress": contract,
                "page": page,
                "offset": offset,
            },
            timeout=15,
        )
        latency = int((time.time() - start) * 1000)
        track_api_call("blockscout", "/getTokenHolders",
                       caller="holder_discovery", status=resp.status_code,
                       latency_ms=latency)

        if resp.status_code == 429:
            rate_limiter.report_429("blockscout")
            return []

        rate_limiter.report_success("blockscout")
        data = resp.json()

        if data.get("status") == "1" and isinstance(data.get("result"), list):
            return [
                (h.get("address") or h.get("TokenHolderAddress", "")).lower()
                for h in data["result"]
                if h.get("address") or h.get("TokenHolderAddress")
            ]
        return []
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("blockscout", "/getTokenHolders",
                       caller="holder_discovery", status=500, latency_ms=latency)
        logger.debug(f"Blockscout holder fetch failed for {contract[:10]}…: {e}")
        return []


async def run_holder_discovery() -> dict:
    """
    Deep holder list pagination for all stablecoins on all chains via Blockscout.

    Budget: ~30K calls per chain = ~300 pages per stablecoin per chain
    (at 100 holders per page = up to 30,000 holders discovered per stablecoin).

    Returns summary of discovery.
    """
    # Pre-load existing addresses for dedup
    existing_rows = fetch_all("SELECT address FROM wallet_graph.wallets")
    existing_set = set(r["address"].lower() for r in existing_rows) if existing_rows else set()

    total_discovered = 0
    total_seeded = 0
    chains_processed = 0
    by_chain = {}

    async with httpx.AsyncClient(timeout=15) as client:
        for chain, chain_cfg in BLOCKSCOUT_CHAINS.items():
            base_url = chain_cfg["base_url"]
            chain_budget = chain_cfg["budget"]
            contracts = STABLECOIN_CONTRACTS.get(chain, {})

            if not contracts:
                continue

            chain_calls = 0
            chain_discovered = 0
            chain_seeded = 0

            # Budget per contract
            calls_per_contract = chain_budget // len(contracts)
            pages_per_contract = calls_per_contract  # 1 call per page

            for contract_addr, symbol in contracts.items():
                if chain_calls >= chain_budget:
                    break

                page_count = 0
                empty_pages = 0

                for page in range(1, pages_per_contract + 1):
                    if chain_calls >= chain_budget:
                        break

                    holders = await _fetch_holders_blockscout(
                        client, base_url, contract_addr, page=page, offset=100,
                    )
                    chain_calls += 1
                    page_count += 1

                    if not holders:
                        empty_pages += 1
                        if empty_pages >= 3:
                            # 3 consecutive empty pages — holder list exhausted
                            break
                        continue

                    empty_pages = 0

                    # Discover new addresses
                    new_addrs = []
                    for addr in holders:
                        if (
                            addr
                            and addr != ZERO_ADDRESS
                            and addr not in existing_set
                            and addr.startswith("0x")
                            and len(addr) == 42
                        ):
                            new_addrs.append(addr)
                            existing_set.add(addr)

                    if new_addrs:
                        chain_discovered += len(new_addrs)

                        # Batch insert
                        try:
                            with get_cursor() as cur:
                                for addr in new_addrs:
                                    cur.execute(
                                        """INSERT INTO wallet_graph.wallets
                                           (address, source, label, created_at, updated_at)
                                           VALUES (%s, 'holder_discovery', %s, NOW(), NOW())
                                           ON CONFLICT (address) DO NOTHING""",
                                        (addr, f"holder:{symbol}:{chain}"),
                                    )
                            chain_seeded += len(new_addrs)
                        except Exception as e:
                            logger.debug(f"Holder insert failed: {e}")

                logger.info(
                    f"Holder discovery [{chain}:{symbol}]: {page_count} pages, "
                    f"{chain_discovered} new addresses"
                )

            total_discovered += chain_discovered
            total_seeded += chain_seeded
            chains_processed += 1
            by_chain[chain] = {
                "calls": chain_calls,
                "discovered": chain_discovered,
                "seeded": chain_seeded,
            }

    # Provenance
    try:
        from app.data_layer.provenance_scaling import attest_data_batch
        if total_discovered > 0:
            attest_data_batch("wallet_holdings", [{"discovered": total_discovered, "seeded": total_seeded}])
    except Exception:
        pass

    logger.info(
        f"Holder discovery complete: {total_discovered} new addresses across "
        f"{chains_processed} chains, {total_seeded} seeded"
    )

    return {
        "chains_processed": chains_processed,
        "total_discovered": total_discovered,
        "total_seeded": total_seeded,
        "by_chain": by_chain,
    }
