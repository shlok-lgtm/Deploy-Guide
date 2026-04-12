"""
Blockscout API Client
======================
Mirrors Etherscan V2 call patterns used in the codebase.
Provides a drop-in compatible interface for shadow comparison testing.

Blockscout PRO API route format:
  https://api.blockscout.com/v2/api?chain_id={chain_id}&module={module}&action={action}&apikey={key}

Free tier: 100K credits/day, 5 req/sec.
"""

import os
import json
import time
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BLOCKSCOUT_BASE = "https://api.blockscout.com/v2/api"
BLOCKSCOUT_API_KEY = os.environ.get("BLOCKSCOUT_API_KEY", "")

# Match Etherscan V2 chain IDs
CHAIN_IDS = {
    "ethereum": 1,
    "arbitrum": 42161,
    "optimism": 10,
    "base": 8453,
    "polygon": 137,
}

# Rate limit: 5 req/sec max on free tier
RATE_LIMIT_DELAY = 0.25  # 4 req/sec to stay safe

# Credit tracking
_credit_counter = {"total": 0, "start_time": time.time()}


def _track_credit():
    """Track API credit consumption."""
    _credit_counter["total"] += 1


def get_credit_stats() -> dict:
    """Return credit consumption statistics."""
    elapsed = time.time() - _credit_counter["start_time"]
    return {
        "total_credits": _credit_counter["total"],
        "elapsed_seconds": round(elapsed, 0),
        "credits_per_hour": round(_credit_counter["total"] / max(1, elapsed / 3600), 1),
    }


def reset_credit_stats():
    """Reset credit counter."""
    _credit_counter["total"] = 0
    _credit_counter["start_time"] = time.time()


# =============================================================================
# Core API call
# =============================================================================

async def blockscout_call(
    client: httpx.AsyncClient,
    module: str,
    action: str,
    chain_id: int = 1,
    extra_params: dict = None,
    timeout: float = 20,
) -> dict:
    """
    Make a Blockscout API call matching Etherscan V2 interface.
    Returns the parsed JSON response with response_time_ms added.
    """
    params = {
        "chain_id": chain_id,
        "module": module,
        "action": action,
    }
    if BLOCKSCOUT_API_KEY:
        params["apikey"] = BLOCKSCOUT_API_KEY
    if extra_params:
        params.update(extra_params)

    start = time.monotonic()
    try:
        resp = await client.get(BLOCKSCOUT_BASE, params=params, timeout=timeout)
        elapsed_ms = int((time.monotonic() - start) * 1000)
        _track_credit()

        data = resp.json()
        data["_blockscout_response_time_ms"] = elapsed_ms
        return data

    except Exception as e:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        logger.debug(f"Blockscout call failed: {module}/{action} chain={chain_id}: {e}")
        return {
            "status": "0",
            "message": "NOTOK",
            "result": str(e),
            "_blockscout_response_time_ms": elapsed_ms,
            "_blockscout_error": True,
        }


# =============================================================================
# Etherscan-compatible wrapper functions
# =============================================================================

async def get_contract_abi(
    client: httpx.AsyncClient,
    address: str,
    chain_id: int = 1,
) -> dict:
    """
    Get contract ABI — equivalent to Etherscan getabi.
    """
    return await blockscout_call(
        client, "contract", "getabi",
        chain_id=chain_id,
        extra_params={"address": address},
    )


async def get_contract_source(
    client: httpx.AsyncClient,
    address: str,
    chain_id: int = 1,
) -> dict:
    """
    Get contract source code — equivalent to Etherscan getsourcecode.
    """
    return await blockscout_call(
        client, "contract", "getsourcecode",
        chain_id=chain_id,
        extra_params={"address": address},
    )


async def get_token_transfers(
    client: httpx.AsyncClient,
    address: str,
    contract_address: str = None,
    chain_id: int = 1,
    start_block: int = 0,
    end_block: int = 99999999,
    page: int = 1,
    offset: int = 100,
) -> dict:
    """
    Get ERC-20 token transfers — equivalent to Etherscan tokentx.
    Used by flows.py for mint/burn detection.
    """
    params = {
        "address": address,
        "startblock": start_block,
        "endblock": end_block,
        "page": page,
        "offset": offset,
        "sort": "desc",
    }
    if contract_address:
        params["contractaddress"] = contract_address

    return await blockscout_call(
        client, "account", "tokentx",
        chain_id=chain_id,
        extra_params=params,
    )


async def get_token_holder_count(
    client: httpx.AsyncClient,
    contract_address: str,
    chain_id: int = 1,
) -> dict:
    """
    Get holder count for a token — equivalent to Etherscan tokenholdercount.
    Used by holder_analysis.py.
    """
    return await blockscout_call(
        client, "token", "tokenholdercount",
        chain_id=chain_id,
        extra_params={"contractaddress": contract_address},
    )


async def get_token_holder_list(
    client: httpx.AsyncClient,
    contract_address: str,
    chain_id: int = 1,
    page: int = 1,
    offset: int = 100,
) -> dict:
    """
    Get list of token holders — equivalent to Etherscan tokenholderlist.
    Used by holder_analysis.py.
    """
    return await blockscout_call(
        client, "token", "tokenholderlist",
        chain_id=chain_id,
        extra_params={
            "contractaddress": contract_address,
            "page": page,
            "offset": offset,
        },
    )


async def get_address_token_balance(
    client: httpx.AsyncClient,
    address: str,
    chain_id: int = 1,
    page: int = 1,
    offset: int = 100,
) -> dict:
    """
    Get all token balances for an address — equivalent to Etherscan addresstokenbalance.
    Used by indexer/scanner.py.
    """
    return await blockscout_call(
        client, "account", "addresstokenbalance",
        chain_id=chain_id,
        extra_params={
            "address": address,
            "page": page,
            "offset": offset,
        },
    )
