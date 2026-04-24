"""
Sprint 3 — Transfer Edge Builder
==================================
Builds wallet→wallet transfer edges from Etherscan V2 tokentx data.
Complements the existing shared-holder edge builder in app/indexer/edges.py.

Budget: ~500 Etherscan calls per 30-min batch = 24K/day (12% of 200K cap).
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timezone

import httpx

from app.database import fetch_all, fetch_one, execute, get_cursor

logger = logging.getLogger(__name__)

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
BATCH_SIZE = 500
BATCH_INTERVAL = 1800  # 30 min between batches
ETHERSCAN_DAILY_CAP = 120_000

_client = httpx.AsyncClient(
    timeout=30, follow_redirects=True,
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
)


def _get_etherscan_24h_usage() -> int:
    try:
        row = fetch_one("""
            SELECT SUM(total_calls) AS total FROM api_usage_hourly
            WHERE provider = 'etherscan' AND hour > NOW() - INTERVAL '24 hours'
        """)
        return int(row["total"]) if row and row.get("total") else 0
    except Exception:
        return 0


def _get_wallets_for_scan(limit: int) -> list[str]:
    rows = fetch_all(f"""
        SELECT w.address
        FROM wallet_graph.wallets w
        INNER JOIN (
            SELECT DISTINCT wallet_address FROM wallet_holder_discovery WHERE balance_usd > 10000
        ) h ON w.address = h.wallet_address
        LEFT JOIN wallet_graph.edge_build_status s
            ON w.address = s.wallet_address AND s.chain = 'ethereum'
        WHERE s.wallet_address IS NULL OR s.last_built_at < NOW() - INTERVAL '24 hours'
        ORDER BY COALESCE(s.last_built_at, '1970-01-01') ASC
        LIMIT {limit}
    """)
    return [r["address"] for r in rows] if rows else []


async def _fetch_tokentx(wallet: str, api_key: str) -> list[dict]:
    from app.shared_rate_limiter import rate_limiter
    await rate_limiter.acquire("etherscan")

    resp = await _client.get(ETHERSCAN_V2_BASE, params={
        "chainid": 1,
        "module": "account",
        "action": "tokentx",
        "address": wallet,
        "page": 1,
        "offset": 1000,
        "sort": "desc",
        "apikey": api_key,
    })

    data = resp.json()
    if data.get("status") != "1":
        return []
    return data.get("result", [])


async def _process_wallet(wallet: str, api_key: str) -> dict:
    wallet_lower = wallet.lower()
    txs = await _fetch_tokentx(wallet_lower, api_key)

    if not txs:
        return {"edges": 0, "txs": 0}

    # Aggregate edges: (from, to) → accumulated stats
    edge_map = {}
    for tx in txs:
        from_addr = (tx.get("from") or "").lower()
        to_addr = (tx.get("to") or "").lower()
        if not from_addr or not to_addr:
            continue
        if from_addr == "0x0000000000000000000000000000000000000000":
            continue
        if to_addr == "0x0000000000000000000000000000000000000000":
            continue

        token_addr = (tx.get("contractAddress") or "").lower()
        try:
            raw_value = int(tx.get("value", "0"))
        except (ValueError, TypeError):
            raw_value = 0

        try:
            decimals = int(tx.get("tokenDecimal", "18"))
        except (ValueError, TypeError):
            decimals = 18
        value_usd = raw_value / (10 ** decimals)  # stablecoins ≈ $1

        try:
            ts = datetime.fromtimestamp(int(tx.get("timeStamp", "0")), tz=timezone.utc)
        except (ValueError, TypeError):
            ts = datetime.now(timezone.utc)

        key = (from_addr, to_addr)
        if key not in edge_map:
            edge_map[key] = {"count": 0, "value_raw": 0, "value_usd": 0.0, "last_ts": ts, "token": token_addr}
        e = edge_map[key]
        e["count"] += 1
        e["value_raw"] += raw_value
        e["value_usd"] += value_usd
        e["last_ts"] = max(e["last_ts"], ts)

    # Bulk upsert
    edges_written = 0
    for (from_addr, to_addr), e in edge_map.items():
        try:
            execute("""
                INSERT INTO wallet_graph.wallet_edges
                    (from_address, to_address, chain, transfer_count, total_value_usd,
                     first_transfer_at, last_transfer_at, weight, tokens_transferred)
                VALUES (%s, %s, 'ethereum', %s, %s, %s, %s, %s, %s)
                ON CONFLICT (from_address, to_address, chain) DO UPDATE SET
                    transfer_count = wallet_graph.wallet_edges.transfer_count + EXCLUDED.transfer_count,
                    total_value_usd = wallet_graph.wallet_edges.total_value_usd + EXCLUDED.total_value_usd,
                    last_transfer_at = GREATEST(wallet_graph.wallet_edges.last_transfer_at, EXCLUDED.last_transfer_at),
                    weight = LN(1 + wallet_graph.wallet_edges.total_value_usd + EXCLUDED.total_value_usd),
                    updated_at = NOW()
            """, (
                from_addr, to_addr, e["count"], e["value_usd"],
                e["last_ts"], e["last_ts"],
                max(0.01, e["value_usd"]),
                '{}',
            ))
            edges_written += 1
        except Exception as ex:
            if edges_written == 0:
                logger.error(f"[transfer_edge_builder] edge upsert failed: {ex}")

    # Update scan state
    try:
        execute("""
            INSERT INTO wallet_graph.edge_build_status
                (wallet_address, chain, last_built_at, transfers_processed, edges_created, pages_fetched, status)
            VALUES (%s, 'ethereum', NOW(), %s, %s, 1, 'complete')
            ON CONFLICT (wallet_address, chain) DO UPDATE SET
                last_built_at = NOW(),
                transfers_processed = wallet_graph.edge_build_status.transfers_processed + EXCLUDED.transfers_processed,
                edges_created = wallet_graph.edge_build_status.edges_created + EXCLUDED.edges_created,
                status = 'complete'
        """, (wallet_lower, len(txs), edges_written))
    except Exception:
        pass

    return {"edges": edges_written, "txs": len(txs)}


async def transfer_edge_builder_background_loop():
    """Independent background loop for Sprint 3 transfer edges."""
    logger.error("[transfer_edge_bg] background loop started")
    await asyncio.sleep(300)  # stagger behind other loops

    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        logger.error("[transfer_edge_bg] no ETHERSCAN_API_KEY — disabled")
        return

    while True:
        try:
            usage = _get_etherscan_24h_usage()
            if usage > ETHERSCAN_DAILY_CAP:
                logger.error(f"[transfer_edge_bg] PAUSED: Etherscan 24h {usage:,}/{ETHERSCAN_DAILY_CAP:,}")
                await asyncio.sleep(3600)
                continue

            wallets = _get_wallets_for_scan(BATCH_SIZE)
            if not wallets:
                logger.error("[transfer_edge_bg] no wallets need scanning, sleeping 1h")
                await asyncio.sleep(3600)
                continue

            logger.error(f"[transfer_edge_bg] scanning {len(wallets)} wallets")

            total_edges = 0
            total_txs = 0
            errors = 0

            for i, wallet in enumerate(wallets):
                try:
                    result = await _process_wallet(wallet, api_key)
                    total_edges += result["edges"]
                    total_txs += result["txs"]

                    if i < 5 or (i + 1) % 100 == 0:
                        logger.error(
                            f"[transfer_edge_bg] wallet {wallet[:12]}... done: "
                            f"edges={result['edges']}, txs={result['txs']} "
                            f"({i + 1}/{len(wallets)})"
                        )
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        logger.error(f"[transfer_edge_bg] wallet {wallet[:12]}... FAILED: {e}")

                await asyncio.sleep(0.12)  # ~8 req/s

            logger.error(
                f"[transfer_edge_bg] BATCH SUMMARY: scanned={len(wallets)}, "
                f"new_edges={total_edges}, txs_processed={total_txs}, errors={errors}"
            )

            await asyncio.sleep(BATCH_INTERVAL)

        except Exception as e:
            logger.error(f"[transfer_edge_bg] loop error: {type(e).__name__}: {e}")
            await asyncio.sleep(600)
