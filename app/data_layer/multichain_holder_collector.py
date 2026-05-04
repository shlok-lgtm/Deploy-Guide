"""
Phase 2 Sprint 2 Mode A — Multi-Chain Holder Scans
====================================================
Scans holder lists of multi-chain entities (stablecoins, LSTs) on
non-Ethereum chains via Blockscout. Discovers wallets that exist on
those chains and records chain presence.

Budget: ~40 Blockscout calls per weekly sweep (<0.05% of 100K/day).
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import httpx
import psycopg2

from app.database import (
    fetch_all, fetch_one, get_cursor,
    fetch_one_async, fetch_all_async, execute_async,
)
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

_client = httpx.AsyncClient(
    timeout=30, follow_redirects=True,
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
)

CHAIN_IDS = {
    "ethereum": 1, "base": 8453, "arbitrum": 42161,
    "optimism": 10, "polygon": 137,
}

USD_THRESHOLD = 10_000
BLOCKSCOUT_DAILY_CAP = 80_000


async def _get_blockscout_24h_usage() -> int:
    try:
        row = await fetch_one_async("""
            SELECT SUM(total_calls) AS total FROM api_usage_hourly
            WHERE provider = 'blockscout' AND hour > NOW() - INTERVAL '24 hours'
        """)
        return int(row["total"]) if row and row.get("total") else 0
    except Exception:
        return 0


def _load_multichain_entities() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "multichain_entities.json")
    try:
        with open(config_path) as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"[multichain_holder] failed to load multichain_entities.json: {e}")
        return {}


async def _fetch_holders_blockscout(
    client: httpx.AsyncClient, contract: str, chain: str
) -> list[dict]:
    """Fetch holders via Blockscout unified API."""
    from app.utils.blockscout_client import get_token_holders

    chain_id = CHAIN_IDS.get(chain)
    if not chain_id:
        return []

    result = await get_token_holders(client, contract, chain_id=chain_id, offset=100)
    if result.get("status") != "1" or not result.get("result"):
        if result.get("_blockscout_error"):
            logger.error(f"[multichain_holder] blockscout unified API error for {chain}/{contract[:12]}: {result.get('result', '')[:200]}")
        return []

    return [
        {
            "address": (h.get("TokenHolderAddress") or "").lower(),
            "value": h.get("TokenHolderQuantity", "0"),
            "token_decimals": None,
        }
        for h in result["result"] if h.get("TokenHolderAddress")
    ]


def _insert_chain_presence_sync(addr, chain, chain_id, symbol):
    """Sync helper: insert chain presence record."""
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO wallet_chain_presence
                (wallet_address, chain, chain_id, discovery_method, discovery_entity)
            VALUES (%s, %s, %s, 'holder_scan', %s)
            ON CONFLICT (wallet_address, chain) DO UPDATE SET
                last_verified_at = NOW()
        """, (addr, chain, chain_id, symbol))
        return cur.statusmessage


def _promote_wallets_sync(addresses, chain, symbol):
    """Sync helper: promote wallets to wallet_graph."""
    from psycopg2.extras import execute_values
    with get_cursor() as cur:
        execute_values(cur, """
            INSERT INTO wallet_graph.wallets (address, source, created_at)
            VALUES %s ON CONFLICT (address) DO NOTHING
        """, [(a, f"multichain:{chain}:{symbol}", datetime.now(timezone.utc)) for a in addresses],
            page_size=1000)
        return cur.rowcount


async def run_multichain_holder_scan() -> dict:
    logger.error("[multichain_holder] ENTRY — function called")
    usage = await _get_blockscout_24h_usage()
    if usage > BLOCKSCOUT_DAILY_CAP:
        logger.error(
            f"[multichain_holder] PAUSED: Blockscout 24h usage {usage:,} / 100,000. "
            f"Deferring to next day."
        )
        return {"status": "paused", "blockscout_24h": usage}

    entities = _load_multichain_entities()
    if not entities:
        return {"entities": 0}

    # Build scan list: skip ethereum (handled by SSS), scan all other chains
    scans = []
    for symbol, chains in entities.items():
        for chain, contract in chains.items():
            if chain == "ethereum":
                continue
            if chain not in CHAIN_IDS:
                continue
            scans.append({
                "symbol": symbol, "chain": chain,
                "contract": contract.lower(), "chain_id": CHAIN_IDS.get(chain, 0),
            })

    logger.error(f"[multichain_holder] starting: {len(scans)} non-ethereum holder scans across {len(entities)} entities")

    stats = defaultdict(lambda: {"scanned": 0, "holders": 0, "new_wallets": 0, "new_presences": 0, "errors": 0})
    total_calls = 0

    client = _client
    if True:
        for scan in scans:
            chain = scan["chain"]
            symbol = scan["symbol"]
            contract = scan["contract"]
            chain_id = scan["chain_id"]

            try:
                holders = await _fetch_holders_blockscout(client, contract, chain)
                total_calls += 1

                filtered = []
                for rank, h in enumerate(holders, 1):
                    addr = h["address"]
                    if not addr or not addr.startswith("0x") or len(addr) != 42:
                        continue
                    try:
                        decimals = int(h.get("token_decimals") or 18)
                        balance = float(int(h["value"])) / (10 ** decimals)
                    except (ValueError, OverflowError):
                        balance = 0
                    balance_usd = balance  # stablecoins ≈ $1, LSTs ≈ $3000
                    if symbol in ("wstETH", "rETH", "cbETH", "weETH"):
                        balance_usd = balance * 3000
                    if balance_usd < USD_THRESHOLD:
                        continue
                    filtered.append({"address": addr, "balance_usd": balance_usd, "rank": rank})

                stats[chain]["scanned"] += 1
                stats[chain]["holders"] += len(filtered)

                # Insert holder discovery records
                for h in filtered:
                    try:
                        await execute_async("""
                                INSERT INTO wallet_holder_discovery
                                    (wallet_address, entity_type, entity_id, entity_contract,
                                     chain, balance_usd, rank_in_entity, source)
                                VALUES (%s, 'stablecoin', %s, %s, %s, %s, %s, 'blockscout')
                                ON CONFLICT (wallet_address, entity_id, entity_contract, chain)
                                DO UPDATE SET balance_usd = EXCLUDED.balance_usd,
                                             rank_in_entity = EXCLUDED.rank_in_entity,
                                             discovered_at = NOW()
                            """, (h["address"], symbol, contract, chain, h["balance_usd"], h["rank"]))
                    except Exception:
                        pass

                # Insert chain presence
                new_presences = 0
                for h in filtered:
                    try:
                        _statusmsg = await asyncio.to_thread(
                            _insert_chain_presence_sync, h["address"], chain, chain_id, symbol
                        )
                        if _statusmsg and "INSERT" in _statusmsg:
                            new_presences += 1
                    except Exception:
                        pass
                stats[chain]["new_presences"] += new_presences

                # Promote new wallets to wallet_graph
                addresses = [h["address"] for h in filtered]
                if addresses:
                    try:
                        stats[chain]["new_wallets"] += await asyncio.to_thread(
                            _promote_wallets_sync, addresses, chain, symbol
                        )
                    except Exception as e:
                        stats[chain]["errors"] += 1

            except Exception as e:
                stats[chain]["errors"] += 1
                logger.error(f"[multichain_holder] {symbol}/{chain}: FAIL {e}")

    # Attestation
    try:
        from app.data_layer.provenance_scaling import attest_data_batch
        total_presences = sum(s["new_presences"] for s in stats.values())
        if total_presences > 0:
            await asyncio.to_thread(attest_data_batch, "wallet_chain_presence", [dict(stats)])
    except Exception:
        pass

    # SUMMARY
    for chain, s in sorted(stats.items()):
        logger.error(
            f"[multichain_holder] chain={chain}: scanned={s['scanned']} "
            f"holders={s['holders']} new_wallets={s['new_wallets']} "
            f"new_presences={s['new_presences']} errors={s['errors']}"
        )

    total_new = sum(s["new_wallets"] for s in stats.values())
    total_presences = sum(s["new_presences"] for s in stats.values())
    logger.error(
        f"[multichain_holder] TOTAL: new_wallets={total_new}, "
        f"new_presences={total_presences}, blockscout_calls={total_calls}"
    )

    return {
        "by_chain": {k: dict(v) for k, v in stats.items()},
        "total_new_wallets": total_new,
        "total_new_presences": total_presences,
        "blockscout_calls": total_calls,
    }


async def multichain_holder_background_loop():
    """Independent background loop — runs multi-chain holder scan weekly."""
    logger.error("[multichain_bg] background loop started")
    await asyncio.sleep(120)  # stagger behind holder_ingestion
    consecutive_db_failures = 0

    while True:
        try:
            logger.error("[multichain_bg] loop tick, checking gate")
            last = await fetch_one_async(
                "SELECT MAX(last_verified_at) AS latest FROM wallet_chain_presence WHERE discovery_method = 'holder_scan'"
            )
            latest = last.get("latest") if last else None

            if latest:
                if latest.tzinfo is None:
                    latest = latest.replace(tzinfo=datetime.now(timezone.utc).tzinfo)
                age_h = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
                if age_h < 168:
                    logger.error(f"[multichain_bg] gate closed, last run {age_h:.0f}h ago")
                    await asyncio.sleep(3600)
                    continue

            logger.error("[multichain_bg] gate open, running scan")
            result = await run_multichain_holder_scan()
            logger.error(f"[multichain_bg] scan complete: {result}")
            await asyncio.sleep(168 * 3600)
            consecutive_db_failures = 0
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            consecutive_db_failures += 1
            if consecutive_db_failures >= 10:
                logger.critical(f"[multichain_bg] {consecutive_db_failures} consecutive DB failures — exiting")
                raise SystemExit(1)
            elif consecutive_db_failures >= 3:
                logger.error(f"[multichain_bg] DB failure #{consecutive_db_failures}: {e}")
            else:
                logger.warning(f"[multichain_bg] DB failure (will retry): {e}")
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"[multichain_bg] ERROR: {type(e).__name__}: {e}")
            await asyncio.sleep(300)
