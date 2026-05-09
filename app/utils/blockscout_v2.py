"""
Blockscout V2 REST Client — Historical Backfill
=================================================
Async client for Blockscout V2 REST API across three EVM chains.
Used by backfill scripts for historical data retrieval.

Endpoints:
  Ethereum:  https://eth.blockscout.com/api/v2
  Base:      https://base.blockscout.com/api/v2
  Arbitrum:  https://arbitrum.blockscout.com/api/v2

Rate limits: IP-level, asyncio.Semaphore(10) per chain.
"""

import asyncio
import logging
import time
from typing import Optional

import httpx

from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

CHAIN_HOSTS = {
    "ethereum": "eth.blockscout.com",
    "base": "base.blockscout.com",
    "arbitrum": "arbitrum.blockscout.com",
}

_semaphores: dict[str, asyncio.Semaphore] = {}


def _get_semaphore(chain: str) -> asyncio.Semaphore:
    if chain not in _semaphores:
        _semaphores[chain] = asyncio.Semaphore(10)
    return _semaphores[chain]


async def _request(
    client: httpx.AsyncClient,
    chain: str,
    path: str,
    params: dict = None,
    retries: int = 3,
) -> dict | list | None:
    """Make a rate-limited request with exponential backoff."""
    host = CHAIN_HOSTS.get(chain)
    if not host:
        logger.warning(f"Unknown chain: {chain}")
        return None

    url = f"https://{host}/api/v2{path}"
    sem = _get_semaphore(chain)

    for attempt in range(retries):
        async with sem:
            _t0 = time.monotonic()
            _status = None
            try:
                resp = await client.get(url, params=params, timeout=30)
                _status = resp.status_code
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    wait = min(2 ** (attempt + 1), 30)
                    logger.debug(f"Blockscout 429 on {chain}, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue
                if resp.status_code >= 500:
                    await asyncio.sleep(2 ** attempt)
                    continue
                logger.debug(f"Blockscout {resp.status_code}: {url}")
                return None
            except Exception as e:
                _status = 0
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.warning(f"Blockscout request failed: {url}: {e}")
                    return None
            finally:
                try:
                    track_api_call(
                        provider="blockscout",
                        endpoint=path,
                        caller="utils.blockscout_v2",
                        status=_status,
                        latency_ms=int((time.monotonic() - _t0) * 1000),
                    )
                except Exception as e:
                    logger.warning(f"blockscout_v2: track_api_call failed: {e}")
                    try:
                        from app.worker import _record_cycle_error
                        _record_cycle_error(
                            error_type="utils_blockscout_v2_track_api_call_failure",
                            error_message=str(e)[:500],
                            cycle_phase="utils_blockscout_v2",
                        )
                    except Exception:
                        pass
    return None


async def get_contract_source(client: httpx.AsyncClient, chain: str, address: str) -> dict | None:
    """Get verified contract source code."""
    return await _request(client, chain, f"/smart-contracts/{address}")


async def get_token_transfers(
    client: httpx.AsyncClient, chain: str, address: str,
    from_block: int = None, to_block: int = None,
    token_type: str = "ERC-20",
) -> list:
    """Get token transfers for an address."""
    params = {"type": token_type}
    if from_block:
        params["block_number_from"] = from_block
    if to_block:
        params["block_number_to"] = to_block
    result = await _request(client, chain, f"/addresses/{address}/token-transfers", params)
    return result.get("items", []) if isinstance(result, dict) else result or []


async def get_internal_transactions(
    client: httpx.AsyncClient, chain: str, address: str,
    from_block: int = None, to_block: int = None,
) -> list:
    """Get internal transactions for an address."""
    params = {}
    if from_block:
        params["block_number_from"] = from_block
    if to_block:
        params["block_number_to"] = to_block
    result = await _request(client, chain, f"/addresses/{address}/internal-transactions", params)
    return result.get("items", []) if isinstance(result, dict) else result or []


async def get_logs(
    client: httpx.AsyncClient, chain: str, address: str,
    topic: str = None, from_block: int = None, to_block: int = None,
) -> list:
    """Get event logs for a contract."""
    params = {}
    if topic:
        params["topic0"] = topic
    if from_block:
        params["block_number_from"] = from_block
    if to_block:
        params["block_number_to"] = to_block
    result = await _request(client, chain, f"/addresses/{address}/logs", params)
    return result.get("items", []) if isinstance(result, dict) else result or []


async def get_block_by_timestamp(
    client: httpx.AsyncClient, chain: str, timestamp: int,
) -> int | None:
    """Map a unix timestamp to the nearest block number."""
    result = await _request(client, chain, f"/blocks", params={
        "type": "block",
        "timestamp": timestamp,
    })
    if isinstance(result, dict) and result.get("items"):
        return result["items"][0].get("height") or result["items"][0].get("block_number")
    if isinstance(result, list) and result:
        return result[0].get("height") or result[0].get("block_number")
    return None


async def get_address_transactions(
    client: httpx.AsyncClient, chain: str, address: str,
    from_block: int = None, to_block: int = None,
) -> list:
    """Get transactions for an address."""
    params = {}
    if from_block:
        params["block_number_from"] = from_block
    if to_block:
        params["block_number_to"] = to_block
    result = await _request(client, chain, f"/addresses/{address}/transactions", params)
    return result.get("items", []) if isinstance(result, dict) else result or []
