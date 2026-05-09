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

from app.api_usage_tracker import track_api_call

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
    _status = None
    try:
        resp = await client.get(BLOCKSCOUT_BASE, params=params, timeout=timeout)
        _status = resp.status_code
        elapsed_ms = int((time.monotonic() - start) * 1000)
        _track_credit()

        data = resp.json()
        data["_blockscout_response_time_ms"] = elapsed_ms
        return data

    except Exception as e:
        _status = 0
        elapsed_ms = int((time.monotonic() - start) * 1000)
        logger.debug(f"Blockscout call failed: {module}/{action} chain={chain_id}: {e}")
        return {
            "status": "0",
            "message": "NOTOK",
            "result": str(e),
            "_blockscout_response_time_ms": elapsed_ms,
            "_blockscout_error": True,
        }
    finally:
        try:
            track_api_call(
                provider="blockscout",
                endpoint=f"/v2/api?module={module}&action={action}&chain_id={chain_id}",
                caller="utils.blockscout_client",
                status=_status,
                latency_ms=int((time.monotonic() - start) * 1000),
            )
        except Exception as e:
            logger.warning(f"blockscout_client: blockscout_call track_api_call failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="utils_blockscout_call_track_api_call_failure",
                    error_message=str(e)[:500],
                    cycle_phase="utils_blockscout_client",
                )
            except Exception:
                pass


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
    Get holder count for a token.
    Uses Blockscout V2 native /tokens/{address}/counters endpoint
    because the Etherscan-compatible tokenholdercount action returns 400.
    """
    # Map chain_id to Blockscout instance hostname
    chain_hosts = {
        1: "eth.blockscout.com",
        42161: "arbitrum.blockscout.com",
        10: "optimism.blockscout.com",
        8453: "base.blockscout.com",
        137: "polygon.blockscout.com",
    }
    host = chain_hosts.get(chain_id, "eth.blockscout.com")
    url = f"https://{host}/api/v2/tokens/{contract_address}/counters"

    start = time.monotonic()
    _status = None
    try:
        resp = await client.get(url, timeout=20)
        _status = resp.status_code
        elapsed_ms = int((time.monotonic() - start) * 1000)
        _track_credit()

        if resp.status_code != 200:
            return {
                "status": "0", "message": "NOTOK",
                "result": f"HTTP {resp.status_code}",
                "_blockscout_response_time_ms": elapsed_ms,
            }

        data = resp.json()
        holder_count = data.get("token_holders_count", "0")
        return {
            "status": "1", "message": "OK",
            "result": str(holder_count),
            "_blockscout_response_time_ms": elapsed_ms,
        }
    except Exception as e:
        _status = 0
        elapsed_ms = int((time.monotonic() - start) * 1000)
        logger.debug(f"Blockscout V2 token holder count failed: {e}")
        return {
            "status": "0", "message": "NOTOK",
            "result": str(e),
            "_blockscout_response_time_ms": elapsed_ms,
            "_blockscout_error": True,
        }
    finally:
        try:
            track_api_call(
                provider="blockscout",
                endpoint=f"/api/v2/tokens/counters?chain_id={chain_id}",
                caller="utils.blockscout_client",
                status=_status,
                latency_ms=int((time.monotonic() - start) * 1000),
            )
        except Exception as e:
            logger.warning(f"blockscout_client: get_token_holder_count track_api_call failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="utils_blockscout_get_token_holder_count_track_api_call_failure",
                    error_message=str(e)[:500],
                    cycle_phase="utils_blockscout_client",
                )
            except Exception:
                pass


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


async def get_token_holders(
    client: httpx.AsyncClient,
    contract_address: str,
    chain_id: int = 1,
    page: int = 1,
    offset: int = 100,
) -> dict:
    """Get token holder list — equivalent to Etherscan tokenholderlist."""
    return await blockscout_call(
        client, "token", "tokenholderlist",
        chain_id=chain_id,
        extra_params={
            "contractaddress": contract_address,
            "page": page,
            "offset": offset,
        },
    )


async def get_address_info(
    client: httpx.AsyncClient,
    address: str,
    chain_id: int = 1,
) -> dict:
    """Get address balance/tx count — equivalent to Etherscan balance + txlist count."""
    return await blockscout_call(
        client, "account", "balance",
        chain_id=chain_id,
        extra_params={"address": address},
    )


async def get_address_txcount(
    client: httpx.AsyncClient,
    address: str,
    chain_id: int = 1,
) -> int:
    """Get transaction count for an address via txlist with offset=1."""
    result = await blockscout_call(
        client, "account", "txlist",
        chain_id=chain_id,
        extra_params={
            "address": address,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": 1,
            "sort": "desc",
        },
    )
    if result.get("status") == "1" and result.get("result"):
        return len(result["result"])
    return 0
