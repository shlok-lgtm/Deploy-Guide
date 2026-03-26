"""
Wallet Indexer — Scanner
========================
Etherscan API integration: fetch ERC-20 token balances for a wallet address,
filtered to known stablecoin contracts.
"""

import os
import asyncio
import logging
from typing import Optional

import httpx

from app.indexer.config import (
    SCORED_CONTRACTS,
    UNSCORED_CONTRACTS,
    ALL_KNOWN_CONTRACTS,
    ETHERSCAN_RATE_LIMIT_DELAY,
    get_all_known_contracts,
)

logger = logging.getLogger(__name__)

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"


async def fetch_token_balance(
    client: httpx.AsyncClient,
    contract_address: str,
    wallet_address: str,
    api_key: str,
) -> Optional[int]:
    """Fetch ERC-20 token balance for one wallet via Etherscan V2 API."""
    try:
        resp = await client.get(
            ETHERSCAN_V2_BASE,
            params={
                "chainid": 1,
                "module": "account",
                "action": "tokenbalance",
                "contractaddress": contract_address,
                "address": wallet_address,
                "tag": "latest",
                "apikey": api_key,
            },
            timeout=10.0,
        )
        data = resp.json()
        if data.get("status") == "1":
            return int(data["result"])
        msg = data.get("result", "")
        if "Max rate limit" in str(msg):
            logger.warning("Etherscan rate limit hit, backing off")
            await asyncio.sleep(1.0)
        return None
    except Exception as e:
        logger.debug(f"Balance fetch error {wallet_address[:10]}…: {e}")
        return None


async def fetch_token_list(
    client: httpx.AsyncClient,
    wallet_address: str,
    api_key: str,
) -> Optional[list[dict]]:
    """Fetch all ERC-20 token transfer events for a wallet to discover holdings.
    Uses tokentx action with a limited page size, then deduplicates contract addresses."""
    try:
        resp = await client.get(
            ETHERSCAN_V2_BASE,
            params={
                "chainid": 1,
                "module": "account",
                "action": "tokentx",
                "address": wallet_address,
                "page": 1,
                "offset": 100,
                "sort": "desc",
                "apikey": api_key,
            },
            timeout=15.0,
        )
        data = resp.json()
        if data.get("status") == "1" and isinstance(data.get("result"), list):
            return data["result"]
        return None
    except Exception as e:
        logger.debug(f"Token list fetch error {wallet_address[:10]}…: {e}")
        return None


async def scan_wallet_holdings(
    client: httpx.AsyncClient,
    wallet_address: str,
    api_key: str,
    sii_scores: dict,
) -> list[dict]:
    """
    Scan a wallet for stablecoin holdings.

    For each known stablecoin contract (scored + unscored), queries the balance.
    Contract list is built from the DB at runtime so promoted coins are included.
    Returns a list of holding dicts ready for storage.

    Args:
        client: httpx async client
        wallet_address: 0x-prefixed Ethereum address
        api_key: Etherscan API key
        sii_scores: dict of stablecoin_id → {overall_score, grade} from scores table
    """
    holdings = []

    # Build contract registry from DB at runtime — picks up promoted coins
    scored_contracts, all_contracts = get_all_known_contracts()

    for contract_lower, info in all_contracts.items():
        balance_raw = await fetch_token_balance(
            client, contract_lower, wallet_address, api_key
        )
        await asyncio.sleep(ETHERSCAN_RATE_LIMIT_DELAY)

        if balance_raw is None or balance_raw == 0:
            continue

        decimals = info.get("decimals", 18)
        balance = balance_raw / (10 ** decimals)

        # Check if this is a scored asset and get price + score data
        is_scored = contract_lower in scored_contracts
        sii_score = None
        sii_grade = None
        price = 1.0  # default for unscored stablecoins
        if is_scored:
            sid = scored_contracts[contract_lower]["stablecoin_id"]
            score_data = sii_scores.get(sid)
            if score_data:
                sii_score = score_data.get("overall_score")
                sii_grade = score_data.get("grade")
                if score_data.get("current_price") is not None:
                    price = score_data["current_price"]

        value_usd = balance * price

        holdings.append({
            "token_address": contract_lower,
            "symbol": info.get("symbol", "???"),
            "name": info.get("name", ""),
            "decimals": decimals,
            "balance": balance,
            "value_usd": value_usd,
            "is_scored": is_scored,
            "sii_score": sii_score,
            "sii_grade": sii_grade,
        })

    return holdings


TOKENBALANCEMULTI_BATCH_SIZE = 20  # Etherscan max addresses per tokenbalancemulti call


async def fetch_token_balance_multi(
    client: httpx.AsyncClient,
    contract_address: str,
    wallet_addresses: list[str],
    api_key: str,
) -> Optional[dict[str, int]]:
    """
    Fetch ERC-20 token balances for up to 20 wallet addresses in a single call.
    Uses Etherscan V2 tokenbalancemulti action.

    Returns:
        dict[wallet_address_lower → raw_balance_int] for non-zero balances on success
        {} (empty dict) if all wallets have zero balance (genuine zero, API success)
        None if the API call failed (status != "1", rate-limit, or network error)

    Callers should treat None as a batch failure and {} as a genuine zero-balance result.
    """
    if not wallet_addresses:
        return {}
    if len(wallet_addresses) > TOKENBALANCEMULTI_BATCH_SIZE:
        raise ValueError(f"tokenbalancemulti batch size capped at {TOKENBALANCEMULTI_BATCH_SIZE}")

    try:
        resp = await client.get(
            ETHERSCAN_V2_BASE,
            params={
                "chainid": 1,
                "module": "account",
                "action": "tokenbalancemulti",
                "contractaddress": contract_address,
                "address": ",".join(wallet_addresses),
                "tag": "latest",
                "apikey": api_key,
            },
            timeout=15.0,
        )
        data = resp.json()
        if data.get("status") == "1" and isinstance(data.get("result"), list):
            result = {}
            for entry in data["result"]:
                addr = entry.get("account", "").lower()
                try:
                    balance = int(entry.get("balance", "0"))
                except (ValueError, TypeError):
                    balance = 0
                if balance > 0:
                    result[addr] = balance
            return result

        msg = data.get("result", "")
        if "Max rate limit" in str(msg):
            logger.warning(f"Etherscan rate limit hit (tokenbalancemulti for {contract_address[:10]}…), backing off")
            await asyncio.sleep(1.0)
        else:
            logger.warning(f"tokenbalancemulti non-1 status for {contract_address[:10]}…: {data.get('message','')} — {msg}")
        return None
    except Exception as e:
        logger.warning(f"tokenbalancemulti error for {contract_address[:10]}…: {type(e).__name__}: {e}")
        return None


async def batch_scan_all_holdings(
    client: httpx.AsyncClient,
    wallet_addresses: list[str],
    api_key: str,
    sii_scores: dict,
    contract_override: Optional[dict] = None,
    wallet_override: Optional[list[str]] = None,
) -> tuple[dict, int, int]:
    """
    Contract-first batch scan: for each known stablecoin contract, fetch balances
    for all wallet addresses in batches of 20 via tokenbalancemulti.

    Returns a tuple of (holdings_by_wallet, batch_failures, calls_made):
      holdings_by_wallet: {wallet_address_lower: [holding_dict, ...]} — only wallets
        with at least one non-zero balance; wallets with zero holdings are omitted.
      batch_failures: number of API calls that returned None (rate-limit/network error)
      calls_made: exact number of tokenbalancemulti API calls executed

    Normal usage (Phase A — full scan):
      Builds contract list from DB at runtime via get_all_known_contracts() so promoted
      coins are included automatically. Scans all of wallet_addresses.

      Old: 24 contracts × 44k wallets ≈ 1.07M individual tokenbalance calls (~48h)
      New: 24 contracts × ⌈44k/20⌉ batches ≈ 53k tokenbalancemulti calls (~2-3h)

    Override usage (Phase 2 discovery tiered scan):
      contract_override: if provided, use this dict instead of calling
        get_all_known_contracts(). All contracts in the override are treated as
        unscored (is_scored=False, value_usd=balance as 1:1 placeholder — price
        unknown at discovery time; for stablecoins this is close to accurate,
        for non-stablecoins it is a rough proxy for demand signal ranking).
      wallet_override: if provided, use this list instead of wallet_addresses.
        Useful when you want to scan only a subset of wallets (e.g. 200 sampled
        whales) without touching the full wallet_addresses argument.

    Args:
        client: httpx async client
        wallet_addresses: list of 0x-prefixed Ethereum addresses to scan
        api_key: Etherscan API key
        sii_scores: dict of stablecoin_id → {overall_score, grade, current_price};
                    ignored when contract_override is set (all override contracts are unscored)
        contract_override: optional dict of contract_addr → {symbol, name, decimals, ...};
                           when set, replaces the DB-built contract registry entirely
        wallet_override: optional list of wallet addresses; when set, replaces
                         wallet_addresses as the effective scan target
    """
    # Build contract registry
    if contract_override is not None:
        scored_contracts: dict = {}   # all override contracts are unscored
        all_contracts = contract_override
    else:
        # Build from DB at runtime — picks up promoted coins
        scored_contracts, all_contracts = get_all_known_contracts()

    # Determine effective wallet list
    effective_wallets = wallet_override if wallet_override is not None else wallet_addresses

    # wallet_address (lowercased) → list of holding dicts
    holdings_by_wallet: dict[str, list[dict]] = {}
    wallet_list = [addr.lower() for addr in effective_wallets]
    total_contracts = len(all_contracts)
    total_batches = sum(
        (len(wallet_list) + TOKENBALANCEMULTI_BATCH_SIZE - 1) // TOKENBALANCEMULTI_BATCH_SIZE
        for _ in all_contracts
    )

    scan_label = "Tiered scan" if contract_override is not None else "Batch scan"
    logger.info(
        f"{scan_label}: {len(wallet_list)} wallets × {total_contracts} contracts "
        f"= {total_batches} tokenbalancemulti calls (batch size {TOKENBALANCEMULTI_BATCH_SIZE})"
    )

    contracts_done = 0
    calls_made = 0
    batch_failures = 0

    for contract_lower, info in all_contracts.items():
        contracts_done += 1
        decimals = info.get("decimals", 18)
        is_scored = contract_lower in scored_contracts
        sii_score = None
        sii_grade = None
        price = 1.0
        if is_scored:
            sid = scored_contracts[contract_lower]["stablecoin_id"]
            score_data = sii_scores.get(sid)
            if score_data:
                sii_score = score_data.get("overall_score")
                sii_grade = score_data.get("grade")
                if score_data.get("current_price") is not None:
                    price = score_data["current_price"]

        symbol = info.get("symbol", "???")
        name = info.get("name", "")

        # Batch wallets in groups of TOKENBALANCEMULTI_BATCH_SIZE
        nonzero_in_contract = 0
        contract_batch_failures = 0
        for i in range(0, len(wallet_list), TOKENBALANCEMULTI_BATCH_SIZE):
            batch = wallet_list[i : i + TOKENBALANCEMULTI_BATCH_SIZE]
            batch_num = i // TOKENBALANCEMULTI_BATCH_SIZE + 1
            balances = await fetch_token_balance_multi(client, contract_lower, batch, api_key)
            await asyncio.sleep(ETHERSCAN_RATE_LIMIT_DELAY)
            calls_made += 1

            if balances is None:
                # API failure — None distinguishes this from genuine zero-balance ({})
                contract_batch_failures += 1
                batch_failures += 1
                logger.warning(
                    f"  Batch failure: {symbol} batch {batch_num} "
                    f"({len(batch)} wallets) — treating as zero balance"
                )
                continue

            for addr_lower, balance_raw in balances.items():
                balance = balance_raw / (10 ** decimals)
                value_usd = balance * price
                holding = {
                    "token_address": contract_lower,
                    "symbol": symbol,
                    "name": name,
                    "decimals": decimals,
                    "balance": balance,
                    "value_usd": value_usd,
                    "is_scored": is_scored,
                    "sii_score": sii_score,
                    "sii_grade": sii_grade,
                }
                if addr_lower not in holdings_by_wallet:
                    holdings_by_wallet[addr_lower] = []
                holdings_by_wallet[addr_lower].append(holding)
                nonzero_in_contract += 1

        logger.info(
            f"  [{contracts_done}/{total_contracts}] {symbol}: "
            f"{nonzero_in_contract} non-zero balances across {len(wallet_list)} wallets"
            + (f", {contract_batch_failures} batch failures" if contract_batch_failures else "")
            + f" ({calls_made} calls total so far)"
        )

    logger.info(
        f"{scan_label} complete: {calls_made} API calls, "
        f"{batch_failures} failed batches, "
        f"{len(holdings_by_wallet)} wallets with holdings "
        f"out of {len(wallet_list)} total"
    )
    return holdings_by_wallet, batch_failures, calls_made


async def fetch_top_holders(
    client: httpx.AsyncClient,
    contract_address: str,
    api_key: str,
    page: int = 1,
    offset: int = 100,
) -> list[str]:
    """
    Fetch top token holders for a contract via Etherscan tokeholderlist.
    Returns list of holder addresses. Falls back to empty list on failure.
    Note: requires Etherscan Pro plan.
    """
    try:
        resp = await client.get(
            ETHERSCAN_V2_BASE,
            params={
                "chainid": 1,
                "module": "token",
                "action": "tokenholderlist",
                "contractaddress": contract_address,
                "page": page,
                "offset": offset,
                "apikey": api_key,
            },
            timeout=15.0,
        )
        data = resp.json()
        if data.get("status") == "1" and isinstance(data.get("result"), list):
            return [h.get("TokenHolderAddress", "") for h in data["result"] if h.get("TokenHolderAddress")]
        return []
    except Exception as e:
        logger.debug(f"Top holders fetch error for {contract_address[:10]}…: {e}")
        return []


async def fetch_large_transfers(
    client: httpx.AsyncClient,
    contract_address: str,
    api_key: str,
    decimals: int = 18,
    min_value_usd: float = 10_000,
    pages: int = 5,
    per_page: int = 100,
) -> list[dict]:
    """
    Fetch recent ERC-20 transfer events for a stablecoin contract.
    Uses tokentx (Lite-tier). Returns unique addresses that moved
    at least min_value_usd in a single transfer.

    Returns list of dicts: {address, total_transferred, transfer_count}
    """
    address_stats: dict[str, dict] = {}

    for page in range(1, pages + 1):
        try:
            resp = await client.get(
                ETHERSCAN_V2_BASE,
                params={
                    "chainid": 1,
                    "module": "account",
                    "action": "tokentx",
                    "contractaddress": contract_address,
                    "page": page,
                    "offset": per_page,
                    "sort": "desc",
                    "apikey": api_key,
                },
                timeout=15.0,
            )
            data = resp.json()
            await asyncio.sleep(ETHERSCAN_RATE_LIMIT_DELAY)

            if data.get("status") != "1" or not isinstance(data.get("result"), list):
                break

            txs = data["result"]
            if not txs:
                break

            for tx in txs:
                value_raw = int(tx.get("value", "0"))
                value = value_raw / (10 ** decimals)

                if value < min_value_usd:
                    continue

                # Track both sender and receiver
                for addr in (tx.get("from", ""), tx.get("to", "")):
                    if not addr or addr == "0x0000000000000000000000000000000000000000":
                        continue
                    if addr not in address_stats:
                        address_stats[addr] = {"total_transferred": 0, "transfer_count": 0}
                    address_stats[addr]["total_transferred"] += value
                    address_stats[addr]["transfer_count"] += 1

        except Exception as e:
            logger.debug(f"Transfer fetch error page {page} for {contract_address[:10]}…: {e}")
            break

    results = [
        {"address": addr, **stats}
        for addr, stats in address_stats.items()
    ]
    results.sort(key=lambda x: x["total_transferred"], reverse=True)
    return results
