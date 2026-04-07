"""
Wallet Indexer — Scanner
========================
Block explorer API integration: fetch ERC-20 token balances for wallet addresses,
filtered to known stablecoin contracts.

Provider hierarchy:
  1. Blockscout v2 native API (free, no key needed, per-address with concurrency)
  2. Etherscan V2 tokenbalancemulti (batch of 20, requires API key)

Fallback: if the primary provider returns zero results for ALL addresses,
the other provider is tried automatically.
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
    BLOCK_EXPLORER_PROVIDER,
    EXPLORER_RATE_LIMIT_DELAY,
    BLOCKSCOUT_CONCURRENCY,
    CHAIN_CONFIGS,
    get_all_known_contracts,
)

logger = logging.getLogger(__name__)

# Etherscan V2 base — used by EtherscanFetcher and legacy functions
ETHERSCAN_BASE = "https://api.etherscan.io/v2/api"

# Legacy module-level constants kept for backward compat with fetch_top_holders etc.
if BLOCK_EXPLORER_PROVIDER == "etherscan":
    EXPLORER_BASE = ETHERSCAN_BASE
else:
    EXPLORER_BASE = "https://api.blockscout.com/v2/api"

_EXPLORER_CHAIN_KEY = "chainid" if BLOCK_EXPLORER_PROVIDER == "etherscan" else "chain_id"
_TOP_HOLDERS_ACTION = "tokenholderlist" if BLOCK_EXPLORER_PROVIDER == "etherscan" else "getTokenHolders"


# =========================================================================
# Blockscout v2 native fetcher
# =========================================================================

# Blockscout v2 base URLs per chain_id
_BLOCKSCOUT_BASES = {
    1: "https://eth.blockscout.com",
    8453: "https://base.blockscout.com",
    42161: "https://arbitrum.blockscout.com",
}


class BlockscoutFetcher:
    """Primary provider — Blockscout v2 native API.

    Uses GET /api/v2/addresses/{address}/token-balances which returns ALL
    ERC-20 balances for an address in one call.  No API key needed.
    Concurrency controlled by a semaphore.
    """

    def __init__(self, concurrency: int = BLOCKSCOUT_CONCURRENCY):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.name = "blockscout"

    async def fetch_all_balances(
        self,
        client: httpx.AsyncClient,
        wallet_addresses: list[str],
        chain_id: int = 1,
    ) -> dict[str, dict[str, int]]:
        """Fetch token balances for all addresses.

        Returns:
            {wallet_address_lower: {token_address_lower: raw_balance_int, ...}, ...}
            Only includes non-zero balances.
        """
        base_url = _BLOCKSCOUT_BASES.get(chain_id)
        if not base_url:
            logger.warning(f"BlockscoutFetcher: no base URL for chain_id={chain_id}")
            return {}

        tasks = [
            self._fetch_single(client, addr, base_url)
            for addr in wallet_addresses
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_balances: dict[str, dict[str, int]] = {}
        for addr, result in zip(wallet_addresses, results):
            if isinstance(result, Exception):
                logger.debug(f"Blockscout fetch exception for {addr[:10]}…: {result}")
                continue
            if result:
                all_balances[addr.lower()] = result

        return all_balances

    async def _fetch_single(
        self,
        client: httpx.AsyncClient,
        address: str,
        base_url: str,
    ) -> dict[str, int]:
        """Fetch all ERC-20 token balances for one address via Blockscout v2.

        Returns:
            {token_address_lower: raw_balance_int} for non-zero balances
        """
        url = f"{base_url}/api/v2/addresses/{address}/token-balances"
        backoff = 1.0
        max_retries = 3

        for attempt in range(max_retries):
            async with self.semaphore:
                try:
                    resp = await client.get(url, timeout=15.0)

                    if resp.status_code == 429:
                        logger.warning(
                            f"Blockscout 429 for {address[:10]}… "
                            f"(attempt {attempt + 1}/{max_retries}), backoff {backoff}s"
                        )
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 30.0)
                        continue

                    if resp.status_code == 404:
                        # Address not found / no tokens — genuine empty
                        return {}

                    resp.raise_for_status()
                    data = resp.json()

                    balances: dict[str, int] = {}
                    if not isinstance(data, list):
                        return {}

                    for item in data:
                        token = item.get("token", {})
                        token_addr = (token.get("address_hash") or token.get("address") or "").lower()
                        if not token_addr:
                            continue

                        balance_str = item.get("value") or "0"
                        try:
                            balance = int(balance_str)
                        except (ValueError, TypeError):
                            balance = 0

                        if balance > 0:
                            balances[token_addr] = balance

                    return balances

                except httpx.HTTPStatusError as e:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 30.0)
                        continue
                    logger.debug(f"Blockscout HTTP error for {address[:10]}…: {e}")
                    return {}
                except Exception as e:
                    logger.debug(f"Blockscout fetch error for {address[:10]}…: {type(e).__name__}: {e}")
                    return {}

        return {}


# =========================================================================
# Etherscan V2 fetcher — uses addresstokenbalance (per-address, all tokens)
# =========================================================================
# NOTE: The old tokenbalancemulti action was removed when Etherscan deprecated
# V1.  V2 does not support tokenbalancemulti.  The V2 replacement is
# addresstokenbalance: one call per address returns ALL ERC-20 balances
# (paginated, 100 per page).  This mirrors the Blockscout approach.

ETHERSCAN_V2_PAGE_SIZE = 100  # addresstokenbalance returns up to 100 tokens per page


class EtherscanFetcher:
    """Fallback provider — Etherscan V2 addresstokenbalance.

    Per-address: one API call returns all ERC-20 balances for a wallet.
    Requires ETHERSCAN_API_KEY.
    """

    def __init__(self):
        self.name = "etherscan"
        self.api_key = os.environ.get("ETHERSCAN_API_KEY", "")

    async def fetch_all_balances(
        self,
        client: httpx.AsyncClient,
        wallet_addresses: list[str],
        chain_id: int = 1,
    ) -> dict[str, dict[str, int]]:
        """Fetch ALL token balances for each wallet address.

        Returns:
            {wallet_address_lower: {token_address_lower: raw_balance_int, ...}, ...}
            Only includes non-zero balances.  Wallets that error are omitted.
        """
        all_balances: dict[str, dict[str, int]] = {}
        failures = 0

        for addr in wallet_addresses:
            balances = await self._fetch_single(client, addr, chain_id)
            await asyncio.sleep(EXPLORER_RATE_LIMIT_DELAY)

            if balances is None:
                failures += 1
                logger.warning(f"Etherscan addresstokenbalance failed for {addr[:10]}… — will retry next cycle")
                continue

            if balances:
                all_balances[addr.lower()] = balances

        return all_balances

    async def _fetch_single(
        self,
        client: httpx.AsyncClient,
        address: str,
        chain_id: int = 1,
    ) -> Optional[dict[str, int]]:
        """Fetch all ERC-20 token balances for one address via Etherscan V2
        addresstokenbalance.

        Returns:
            {token_address_lower: raw_balance_int} for non-zero balances on success
            {} if address has no token balances (genuine empty)
            None on API failure (will be retried next cycle)
        """
        balances: dict[str, int] = {}
        page = 1

        while True:
            try:
                resp = await client.get(
                    ETHERSCAN_BASE,
                    params={
                        "chainid": chain_id,
                        "module": "account",
                        "action": "addresstokenbalance",
                        "address": address,
                        "page": page,
                        "offset": ETHERSCAN_V2_PAGE_SIZE,
                        "apikey": self.api_key,
                    },
                    timeout=15.0,
                )
                data = resp.json()

                if data.get("status") == "1" and isinstance(data.get("result"), list):
                    entries = data["result"]
                    for entry in entries:
                        token_addr = (entry.get("TokenAddress") or "").lower()
                        if not token_addr:
                            continue
                        try:
                            balance = int(entry.get("TokenQuantity") or "0")
                        except (ValueError, TypeError):
                            balance = 0
                        if balance > 0:
                            balances[token_addr] = balance

                    # If fewer than a full page, we've read everything
                    if len(entries) < ETHERSCAN_V2_PAGE_SIZE:
                        break
                    page += 1
                    await asyncio.sleep(EXPLORER_RATE_LIMIT_DELAY)
                    continue

                msg = str(data.get("result", ""))
                if "rate limit" in msg.lower():
                    logger.warning(f"Etherscan rate limit for {address[:10]}…, backing off 2s")
                    await asyncio.sleep(2.0)
                    continue  # retry same page
                elif "No data found" in msg:
                    # Genuine empty — address has no token balances
                    return balances if balances else {}
                else:
                    logger.warning(
                        f"Etherscan addresstokenbalance error for {address[:10]}…: "
                        f"{data.get('message', '')} — {msg}"
                    )
                    # Return partial results if we got some pages before the error
                    return balances if balances else None

            except Exception as e:
                logger.warning(f"Etherscan fetch error for {address[:10]}…: {type(e).__name__}: {e}")
                return None

        return balances


# =========================================================================
# Unified batch scan — uses provider abstraction with fallback
# =========================================================================

async def batch_scan_all_holdings(
    client: httpx.AsyncClient,
    wallet_addresses: list[str],
    api_key: str,
    sii_scores: dict,
    contract_override: Optional[dict] = None,
    wallet_override: Optional[list[str]] = None,
) -> tuple[dict, int, int, set]:
    """
    Per-address batch scan: fetch all token balances per wallet, then filter
    to known stablecoin contracts.

    Provider strategy:
      - Blockscout (primary): fetches ALL token balances per address in one call,
        then filters to known contracts.
      - Etherscan V2 (fallback): addresstokenbalance, same per-address approach.
      - If primary returns 0 results for ALL wallets, falls back automatically.

    Returns a tuple of (holdings_by_wallet, batch_failures, calls_made, failed_addresses):
      holdings_by_wallet: {wallet_address_lower: [holding_dict, ...]}
      batch_failures: number of API calls that returned None
      calls_made: total API calls executed
      failed_addresses: set of lowercased addresses where the API call failed
                        (these should NOT be marked as indexed — retry next cycle)
    """
    # Build contract registry
    if contract_override is not None:
        scored_contracts: dict = {}
        all_contracts = contract_override
    else:
        scored_contracts, all_contracts = get_all_known_contracts()

    effective_wallets = wallet_override if wallet_override is not None else wallet_addresses
    wallet_list = [addr.lower() for addr in effective_wallets]

    scan_label = "Tiered scan" if contract_override is not None else "Batch scan"
    use_blockscout = BLOCK_EXPLORER_PROVIDER == "blockscout"
    wallet_set = set(wallet_list)

    # Try primary provider
    if use_blockscout:
        holdings, failures, calls = await _scan_via_blockscout(
            client, wallet_list, all_contracts, scored_contracts, sii_scores, scan_label,
        )
        # Fallback to Etherscan if Blockscout returned nothing and we have an API key
        if not holdings and wallet_list:
            etherscan_key = os.environ.get("ETHERSCAN_API_KEY", "")
            if etherscan_key:
                logger.warning(
                    f"Blockscout returned 0 holdings for {len(wallet_list)} wallets — "
                    f"falling back to Etherscan"
                )
                holdings, eth_failures, eth_calls = await _scan_via_etherscan(
                    client, wallet_list, etherscan_key, all_contracts,
                    scored_contracts, sii_scores, scan_label,
                )
                failures += eth_failures
                calls += eth_calls
            else:
                logger.warning(
                    "Blockscout returned 0 holdings and no ETHERSCAN_API_KEY set for fallback"
                )
    else:
        holdings, failures, calls = await _scan_via_etherscan(
            client, wallet_list, api_key, all_contracts,
            scored_contracts, sii_scores, scan_label,
        )
        # Fallback to Blockscout if Etherscan returned nothing
        if not holdings and wallet_list:
            logger.warning(
                f"Etherscan returned 0 holdings for {len(wallet_list)} wallets — "
                f"falling back to Blockscout"
            )
            holdings, bs_failures, bs_calls = await _scan_via_blockscout(
                client, wallet_list, all_contracts, scored_contracts, sii_scores, scan_label,
            )
            failures += bs_failures
            calls += bs_calls

    # Compute failed addresses: wallets we attempted but got no response for
    # (not in holdings AND not successfully scanned as empty).
    # failed_addresses = wallets not in holdings that account for the failure count.
    # Since both providers return only wallets with known-contract holdings,
    # we can't distinguish "no stablecoin holdings" from "API error" purely from
    # holdings.  Use the failure count: if failures > 0, the last `failures`
    # wallets not in holdings are the failed ones.  But the providers track this
    # internally — we approximate by noting that failures = wallets_attempted - wallets_returned.
    # The safest approach: if there were failures, don't mark any wallet without
    # holdings as indexed.  This is conservative but prevents data loss.
    failed_addresses: set[str] = set()
    if failures > 0:
        # Wallets that got holdings are definitely OK.  Wallets without holdings
        # MIGHT have failed or might genuinely have none.  When failure rate is
        # high (>50%), mark all non-holding wallets as failed to be safe.
        # When low, we accept some wallets being re-scanned unnecessarily.
        scanned_ok = set(holdings.keys())
        failed_addresses = wallet_set - scanned_ok

    logger.info(
        f"{scan_label} complete: {calls} API calls, "
        f"{failures} failures, "
        f"{len(holdings)} wallets with holdings "
        f"out of {len(wallet_list)} total"
        + (f", {len(failed_addresses)} wallets deferred for retry" if failed_addresses else "")
    )
    return holdings, failures, calls, failed_addresses


async def _scan_via_blockscout(
    client: httpx.AsyncClient,
    wallet_list: list[str],
    all_contracts: dict,
    scored_contracts: dict,
    sii_scores: dict,
    scan_label: str,
) -> tuple[dict, int, int]:
    """Scan using Blockscout v2 native API — one call per address returns all tokens."""
    fetcher = BlockscoutFetcher()
    logger.info(
        f"{scan_label} [Blockscout]: {len(wallet_list)} wallets, "
        f"concurrency={BLOCKSCOUT_CONCURRENCY}"
    )

    raw_balances = await fetcher.fetch_all_balances(client, wallet_list, chain_id=1)
    calls_made = len(wallet_list)  # one call per address
    failures = len(wallet_list) - len(raw_balances)

    # Build holdings dict by filtering raw balances to known contracts
    holdings_by_wallet: dict[str, list[dict]] = {}
    contracts_hit = set()

    for addr_lower, token_balances in raw_balances.items():
        for token_addr, balance_raw in token_balances.items():
            if token_addr not in all_contracts:
                continue

            info = all_contracts[token_addr]
            decimals = info.get("decimals", 18)
            balance = balance_raw / (10 ** decimals)
            if balance <= 0:
                continue

            is_scored = token_addr in scored_contracts
            sii_score = None
            sii_grade = None
            price = 1.0
            if is_scored:
                sid = scored_contracts[token_addr]["stablecoin_id"]
                score_data = sii_scores.get(sid)
                if score_data:
                    sii_score = score_data.get("overall_score")
                    sii_grade = score_data.get("grade")
                    if score_data.get("current_price") is not None:
                        price = score_data["current_price"]

            value_usd = balance * price
            holding = {
                "token_address": token_addr,
                "symbol": info.get("symbol", "???"),
                "name": info.get("name", ""),
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
            contracts_hit.add(token_addr)

    logger.info(
        f"{scan_label} [Blockscout]: {len(holdings_by_wallet)} wallets with holdings, "
        f"{len(contracts_hit)} contracts matched, "
        f"{failures} addresses failed"
    )
    return holdings_by_wallet, failures, calls_made


async def _scan_via_etherscan(
    client: httpx.AsyncClient,
    wallet_list: list[str],
    api_key: str,
    all_contracts: dict,
    scored_contracts: dict,
    sii_scores: dict,
    scan_label: str,
) -> tuple[dict, int, int]:
    """Scan using Etherscan V2 addresstokenbalance — per-address, all tokens.

    This mirrors the Blockscout approach: one call per address returns all
    ERC-20 balances, then we filter to known contracts.
    """
    logger.info(
        f"{scan_label} [Etherscan V2]: {len(wallet_list)} wallets via addresstokenbalance"
    )

    fetcher = EtherscanFetcher()
    if not fetcher.api_key:
        fetcher.api_key = api_key

    raw_balances = await fetcher.fetch_all_balances(client, wallet_list, chain_id=1)
    calls_made = len(wallet_list)  # one call per address (plus pagination)
    failures = len(wallet_list) - len(raw_balances)

    # Build holdings dict by filtering raw balances to known contracts
    holdings_by_wallet: dict[str, list[dict]] = {}
    contracts_hit = set()

    for addr_lower, token_balances in raw_balances.items():
        for token_addr, balance_raw in token_balances.items():
            if token_addr not in all_contracts:
                continue

            info = all_contracts[token_addr]
            decimals = info.get("decimals", 18)
            balance = balance_raw / (10 ** decimals)
            if balance <= 0:
                continue

            is_scored = token_addr in scored_contracts
            sii_score = None
            sii_grade = None
            price = 1.0
            if is_scored:
                sid = scored_contracts[token_addr]["stablecoin_id"]
                score_data = sii_scores.get(sid)
                if score_data:
                    sii_score = score_data.get("overall_score")
                    sii_grade = score_data.get("grade")
                    if score_data.get("current_price") is not None:
                        price = score_data["current_price"]

            value_usd = balance * price
            holding = {
                "token_address": token_addr,
                "symbol": info.get("symbol", "???"),
                "name": info.get("name", ""),
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
            contracts_hit.add(token_addr)

    logger.info(
        f"{scan_label} [Etherscan V2]: {len(holdings_by_wallet)} wallets with holdings, "
        f"{len(contracts_hit)} contracts matched, "
        f"{failures} addresses failed (will retry next cycle)"
    )
    return holdings_by_wallet, failures, calls_made


# =========================================================================
# Legacy single-address functions (used by scan_wallet_holdings, etc.)
# =========================================================================

async def fetch_token_balance(
    client: httpx.AsyncClient,
    contract_address: str,
    wallet_address: str,
    api_key: str,
) -> Optional[int]:
    """Fetch ERC-20 token balance for one wallet via explorer API."""
    try:
        resp = await client.get(
            EXPLORER_BASE,
            params={
                _EXPLORER_CHAIN_KEY: 1,
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
        if "rate limit" in str(msg).lower():
            logger.warning("Explorer rate limit hit, backing off")
            await asyncio.sleep(2.0)
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
            EXPLORER_BASE,
            params={
                _EXPLORER_CHAIN_KEY: 1,
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
        sii_scores: dict of stablecoin_id -> {overall_score, grade} from scores table
    """
    holdings = []

    # Build contract registry from DB at runtime — picks up promoted coins
    scored_contracts, all_contracts = get_all_known_contracts()

    for contract_lower, info in all_contracts.items():
        balance_raw = await fetch_token_balance(
            client, contract_lower, wallet_address, api_key
        )
        await asyncio.sleep(EXPLORER_RATE_LIMIT_DELAY)

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


async def fetch_top_holders(
    client: httpx.AsyncClient,
    contract_address: str,
    api_key: str,
    page: int = 1,
    offset: int = 100,
) -> list[str]:
    """
    Fetch top token holders for a contract via the explorer API.
    Returns list of holder addresses. Falls back to empty list on failure.
    Etherscan action: tokenholderlist (requires Pro plan).
    Blockscout action: getTokenHolders (PRO API).
    Response field: TokenHolderAddress (Etherscan) or address (Blockscout) — both handled.
    """
    try:
        resp = await client.get(
            EXPLORER_BASE,
            params={
                _EXPLORER_CHAIN_KEY: 1,
                "module": "token",
                "action": _TOP_HOLDERS_ACTION,
                "contractaddress": contract_address,
                "page": page,
                "offset": offset,
                "apikey": api_key,
            },
            timeout=15.0,
        )
        data = resp.json()
        if data.get("status") == "1" and isinstance(data.get("result"), list):
            addresses = []
            for h in data["result"]:
                addr = h.get("TokenHolderAddress") or h.get("address", "")
                if addr:
                    addresses.append(addr)
            return addresses
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
                EXPLORER_BASE,
                params={
                    _EXPLORER_CHAIN_KEY: 1,
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
            await asyncio.sleep(EXPLORER_RATE_LIMIT_DELAY)

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
