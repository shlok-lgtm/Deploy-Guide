"""
Oracle Monitor
==============
Two responsibilities:

1. **Keeper write tracking** — poll Base and Arbitrum RPC nodes for
   ScoreUpdated events emitted by the oracle contract and store in
   ``keeper_publish_log`` (original behaviour, now with correct topic hash).

2. **External interaction tracking** — poll Basescan / Arbiscan for
   transactions TO our oracle and SBT contracts that are NOT from our
   keeper wallet.  These are evidence of external adoption.

Note: EVM view/pure function calls (e.g. getScore) do NOT create
transactions and cannot be tracked via block explorer APIs.  Only
write-type interactions that consume gas appear here.  To track view
calls at scale we would need call tracing infrastructure (own RPC node
with tracing, Tenderly, or Alchemy trace API).  This limitation is
documented in DUNE_QUERIES.md.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

import httpx

from app.database import execute, fetch_one, fetch_all, get_conn

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# keccak256("ScoreUpdated(address,uint256,uint256,uint256)")
SCORE_UPDATED_TOPIC = "0x4e129cd592c4928775615d37d8f0f5a5e1257dec2f17b8b5fe34881b83b1cdc5"

ORACLE_ADDRESS = "0x1651d7b2E238a952167E51A1263FFe607584DB83"
SBT_ADDRESS    = "0xf315411e49fC3EAbEF0D111A40e976802985E56c"
KEEPER_ADDRESS = "0x2dF0f62D1861Aa59A4430e3B2b2E7a0D29Cb723b".lower()

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

# Contracts to poll via block explorer APIs
CONTRACTS = [
    {
        "label": "Oracle (Base)",
        "chain": "base",
        "contract_type": "oracle",
        "address": ORACLE_ADDRESS,
        "explorer_api": "https://api.basescan.org/api",
        "api_key_env": "BASESCAN_API_KEY",
    },
    {
        "label": "Oracle (Arbitrum)",
        "chain": "arbitrum",
        "contract_type": "oracle",
        "address": ORACLE_ADDRESS,
        "explorer_api": "https://api.arbiscan.io/api",
        "api_key_env": "ARBISCAN_API_KEY",
    },
    {
        "label": "SBT (Base)",
        "chain": "base",
        "contract_type": "sbt",
        "address": SBT_ADDRESS,
        "explorer_api": "https://api.basescan.org/api",
        "api_key_env": "BASESCAN_API_KEY",
    },
]


# ===================================================================
# 1. Keeper write tracking (ScoreUpdated events via RPC)
# ===================================================================

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


def _store_events(chain_name, event_logs):
    """Store ScoreUpdated events into keeper_publish_log (sync — called via to_thread)."""
    new_events = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for log in event_logs:
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
                """, (chain_name, tx_hash))
                new_events += 1
            conn.commit()
    return new_events


async def _poll_chain(chain: str, rpc_url: str, oracle_address: str) -> int:
    """Poll a single chain for ScoreUpdated events in the last ~1000 blocks."""
    async with httpx.AsyncClient() as client:
        # Get latest block
        resp = await client.post(rpc_url, json={
            "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1
        }, timeout=10.0)
        latest_block = int(resp.json()["result"], 16)
        from_block = hex(max(0, latest_block - 1000))

        # Get logs for ScoreUpdated events
        resp = await client.post(rpc_url, json={
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{
                "address": oracle_address,
                "topics": [SCORE_UPDATED_TOPIC],
                "fromBlock": from_block,
                "toBlock": "latest",
            }],
            "id": 2
        }, timeout=15.0)

        logs = resp.json().get("result", [])

        new_events = await asyncio.to_thread(_store_events, chain, logs)

        logger.info(f"Oracle monitor [{chain}]: {new_events} new events from {len(logs)} logs")
        return new_events


# ===================================================================
# 2. External interaction tracking (block explorer APIs)
# ===================================================================

async def poll_external_interactions() -> dict:
    """
    Check Basescan and Arbiscan for transactions TO our oracle and SBT contracts
    that are NOT from our keeper.  These are external interactions.
    """
    await asyncio.to_thread(_ensure_table)

    summary = {}
    async with httpx.AsyncClient(timeout=20.0) as client:
        for contract in CONTRACTS:
            try:
                new_count = await _poll_contract(client, contract)
                summary[contract["contract_type"] + "_" + contract["chain"]] = new_count
            except Exception as e:
                logger.error(
                    f"Oracle monitor [{contract['label']}]: poll failed: {e}"
                )
                summary[contract["contract_type"] + "_" + contract["chain"]] = {"error": str(e)}

    totals = sum(v for v in summary.values() if isinstance(v, int))
    parts = ", ".join(f"{k}: {v}" for k, v in summary.items() if isinstance(v, int))
    logger.info(f"Oracle monitor: {totals} new external interactions found ({parts})")
    return summary


async def _poll_contract(client: httpx.AsyncClient, contract: dict) -> int:
    """Fetch recent txs to a contract, filter out keeper, store new ones."""
    api_key = os.environ.get(contract["api_key_env"], "")
    params = {
        "module": "account",
        "action": "txlist",
        "address": contract["address"],
        "startblock": "0",
        "endblock": "99999999",
        "sort": "desc",
        "page": "1",
        "offset": "100",
    }
    if api_key:
        params["apikey"] = api_key

    resp = await client.get(contract["explorer_api"], params=params)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "1" or not data.get("result"):
        if data.get("message") == "No transactions found":
            logger.debug(f"Oracle monitor [{contract['label']}]: no transactions found")
            return 0
        logger.warning(
            f"Oracle monitor [{contract['label']}]: API returned "
            f"status={data.get('status')}, message={data.get('message')}"
        )
        return 0

    txs = data["result"]
    new_count = 0

    for tx in txs:
        from_addr = (tx.get("from") or "").lower()

        # Filter out keeper transactions
        if from_addr == KEEPER_ADDRESS:
            continue

        # Filter out failed transactions
        if tx.get("isError") == "1":
            continue

        tx_hash = tx.get("hash", "")
        if not tx_hash:
            continue

        # Check duplicate
        existing = await asyncio.to_thread(
            fetch_one,
            "SELECT id FROM oracle_external_interactions WHERE tx_hash = %s",
            (tx_hash,),
        )
        if existing:
            continue

        # Parse function selector (first 10 chars: "0x" + 8 hex = 4 bytes)
        input_data = tx.get("input") or ""
        function_selector = input_data[:10] if len(input_data) >= 10 else None

        # Parse timestamp
        block_ts = None
        if tx.get("timeStamp"):
            try:
                block_ts = datetime.fromtimestamp(int(tx["timeStamp"]), tz=timezone.utc)
            except (ValueError, OSError):
                pass

        block_number = int(tx["blockNumber"]) if tx.get("blockNumber") else None
        gas_used = int(tx["gasUsed"]) if tx.get("gasUsed") else None

        await asyncio.to_thread(
            execute,
            """INSERT INTO oracle_external_interactions
               (chain, contract_type, tx_hash, from_address,
                function_selector, block_number, block_timestamp, gas_used)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (tx_hash) DO NOTHING""",
            (
                contract["chain"],
                contract["contract_type"],
                tx_hash,
                from_addr,
                function_selector,
                block_number,
                block_ts,
                gas_used,
            ),
        )
        new_count += 1

    logger.info(
        f"Oracle monitor [{contract['label']}]: "
        f"{new_count} new external interactions from {len(txs)} txs"
    )
    return new_count


# ===================================================================
# 3. Metrics (used by /api/ops/seed-metrics)
# ===================================================================

def get_oracle_external_metrics() -> dict:
    """Return oracle external interaction summary for the seed-metrics endpoint."""
    _ensure_table()

    ext_7d = fetch_one(
        "SELECT COUNT(*) as c FROM oracle_external_interactions "
        "WHERE block_timestamp > NOW() - INTERVAL '7 days'"
    )
    ext_30d = fetch_one(
        "SELECT COUNT(*) as c FROM oracle_external_interactions "
        "WHERE block_timestamp > NOW() - INTERVAL '30 days'"
    )
    ext_unique = fetch_one(
        "SELECT COUNT(DISTINCT from_address) as c FROM oracle_external_interactions "
        "WHERE block_timestamp > NOW() - INTERVAL '7 days'"
    )
    ext_latest = fetch_one(
        "SELECT block_timestamp FROM oracle_external_interactions "
        "ORDER BY block_timestamp DESC LIMIT 1"
    )
    ext_by_type = fetch_all(
        "SELECT contract_type, chain, COUNT(*) as c FROM oracle_external_interactions "
        "WHERE block_timestamp > NOW() - INTERVAL '7 days' "
        "GROUP BY contract_type, chain"
    ) or []

    return {
        "interactions_7d": ext_7d["c"] if ext_7d else 0,
        "interactions_30d": ext_30d["c"] if ext_30d else 0,
        "unique_addresses_7d": ext_unique["c"] if ext_unique else 0,
        "latest_interaction": (
            ext_latest["block_timestamp"].isoformat()
            if ext_latest and ext_latest["block_timestamp"] else None
        ),
        "by_chain_and_type": [dict(r) for r in ext_by_type] if ext_by_type else [],
    }


# ===================================================================
# Table bootstrap (idempotent)
# ===================================================================

def _ensure_table():
    """Create the oracle_external_interactions table if it doesn't exist."""
    execute("""
        CREATE TABLE IF NOT EXISTS oracle_external_interactions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            chain VARCHAR(20) NOT NULL,
            contract_type VARCHAR(20) NOT NULL,
            tx_hash VARCHAR(66) NOT NULL UNIQUE,
            from_address VARCHAR(42) NOT NULL,
            function_selector VARCHAR(10),
            block_number BIGINT,
            block_timestamp TIMESTAMPTZ,
            gas_used BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
