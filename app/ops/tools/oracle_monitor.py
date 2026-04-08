"""
Oracle Monitor
==============
Tracks oracle contract activity on Base and Arbitrum.
- Keeper writes (ScoreUpdated events)
- TODO: Dune query for read tracking (view calls don't emit events)
"""

import logging
import os

import httpx

from app.database import get_conn, fetch_one

logger = logging.getLogger(__name__)

# ScoreUpdated(address indexed token, uint256 score, uint256 formulaVersion, uint256 timestamp)
SCORE_UPDATED_TOPIC = "0x"  # TODO: compute keccak256 of the event signature and paste here

CHAINS = {
    "base": {
        "rpc_env": "BASE_RPC_URL",
        "oracle_env": "BASE_ORACLE_ADDRESS",
        "fallback_rpc": "https://mainnet.base.org",
    },
    "arbitrum": {
        "rpc_env": "ARBITRUM_RPC_URL",
        "oracle_env": "ARBITRUM_ORACLE_ADDRESS",
        "fallback_rpc": "https://arb1.arbitrum.io/rpc",
    },
}


async def poll_oracle_events():
    """Poll both chains for recent ScoreUpdated events and store in keeper_publish_log."""
    results = {}
    for chain_name, chain_config in CHAINS.items():
        oracle_address = os.environ.get(chain_config["oracle_env"])
        if not oracle_address:
            logger.debug(f"Skipping {chain_name}: no oracle address configured")
            continue

        rpc_url = os.environ.get(chain_config["rpc_env"], chain_config["fallback_rpc"])
        try:
            count = await _poll_chain(chain_name, rpc_url, oracle_address)
            results[chain_name] = count
        except Exception as e:
            logger.error(f"Oracle poll failed for {chain_name}: {e}")
            results[chain_name] = {"error": str(e)}

    return results


async def _poll_chain(chain: str, rpc_url: str, oracle_address: str) -> int:
    """Poll a single chain for ScoreUpdated events in the last ~1000 blocks."""
    async with httpx.AsyncClient() as client:
        # Get latest block
        resp = await client.post(rpc_url, json={
            "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1
        }, timeout=10.0)
        latest_block = int(resp.json()["result"], 16)
        from_block = hex(max(0, latest_block - 1000))

        # Get logs
        resp = await client.post(rpc_url, json={
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{
                "address": oracle_address,
                "fromBlock": from_block,
                "toBlock": "latest",
            }],
            "id": 2
        }, timeout=15.0)

        logs = resp.json().get("result", [])
        new_events = 0

        with get_conn() as conn:
            with conn.cursor() as cur:
                for log in logs:
                    tx_hash = log.get("transactionHash", "")
                    # Check if already stored
                    existing = fetch_one(
                        "SELECT id FROM keeper_publish_log WHERE tx_hash = %s", (tx_hash,)
                    )
                    if existing:
                        continue

                    cur.execute("""
                        INSERT INTO keeper_publish_log (chain, tx_hash, success, scores_published)
                        VALUES (%s, %s, TRUE, 1)
                    """, (chain, tx_hash))
                    new_events += 1
                conn.commit()

        logger.info(f"Oracle monitor [{chain}]: {new_events} new events from {len(logs)} logs")
        return new_events
