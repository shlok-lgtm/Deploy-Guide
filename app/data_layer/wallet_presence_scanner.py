"""
Phase 2 Sprint 2 Mode B — Targeted Wallet Presence Checks
============================================================
For wallets with <2 chain presences, check remaining chains via
Blockscout address counters endpoint. Daily, capped at 80K calls.

Budget: 80K Blockscout calls/day max (80% of 100K free tier).
Throughput: ~20K wallets/day × 4 chains = 80K calls.
Full sweep: ~9 days for 175K wallets.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import httpx

from app.database import fetch_all, fetch_one, get_cursor

logger = logging.getLogger(__name__)

_client = httpx.AsyncClient(
    timeout=15, follow_redirects=True,
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
)

CHAIN_HOSTS = {
    "base": "base.blockscout.com",
    "arbitrum": "arbitrum.blockscout.com",
    "optimism": "optimism.blockscout.com",
    "polygon": "polygon.blockscout.com",
}

CHAIN_IDS = {"base": 8453, "arbitrum": 42161, "optimism": 10, "polygon": 137}

CHAINS_TO_CHECK = ["base", "arbitrum", "optimism", "polygon"]
DAILY_CALL_CAP = 80_000
BATCH_SIZE = 20_000


def _get_blockscout_24h_usage() -> int:
    try:
        row = fetch_one("""
            SELECT SUM(total_calls) AS total FROM api_usage_hourly
            WHERE provider = 'blockscout' AND hour > NOW() - INTERVAL '24 hours'
        """)
        return int(row["total"]) if row and row.get("total") else 0
    except Exception:
        return 0


async def run_wallet_presence_scan() -> dict:
    logger.error("[wallet_presence] ENTRY — function called")
    usage = _get_blockscout_24h_usage()
    if usage > DAILY_CALL_CAP:
        logger.error(
            f"[wallet_presence] PAUSED: Blockscout 24h usage {usage:,} / 100,000. "
            f"Deferring to next day."
        )
        return {"status": "paused", "blockscout_24h": usage}

    # Find wallets with fewer than len(CHAINS_TO_CHECK) presences
    # that haven't been fully scanned recently
    wallets = fetch_all(f"""
        SELECT w.address
        FROM wallet_graph.wallets w
        LEFT JOIN (
            SELECT wallet_address, COUNT(*) AS chain_count
            FROM wallet_chain_presence
            GROUP BY wallet_address
        ) p ON w.address = p.wallet_address
        WHERE COALESCE(p.chain_count, 0) < {len(CHAINS_TO_CHECK)}
        ORDER BY w.total_stablecoin_value DESC NULLS LAST
        LIMIT {BATCH_SIZE}
    """)

    if not wallets:
        logger.error("[wallet_presence] no wallets need presence scanning")
        return {"wallets_scanned": 0}

    addresses = [r["address"] for r in wallets]
    logger.error(f"[wallet_presence] starting: {len(addresses)} wallets to check across {len(CHAINS_TO_CHECK)} chains")

    # Get existing presences for these wallets
    existing = fetch_all("""
        SELECT wallet_address, chain FROM wallet_chain_presence
        WHERE wallet_address = ANY(%s)
    """, (addresses,))
    existing_set = set()
    for r in (existing or []):
        existing_set.add((r["wallet_address"], r["chain"]))

    total_calls = 0
    presences_by_chain = {c: 0 for c in CHAINS_TO_CHECK}
    errors = 0
    remaining_budget = DAILY_CALL_CAP - usage

    client = _client
    if True:
        for addr in addresses:
            if total_calls >= remaining_budget:
                logger.error(f"[wallet_presence] budget reached after {total_calls} calls, stopping")
                break

            for chain in CHAINS_TO_CHECK:
                if (addr, chain) in existing_set:
                    continue
                if total_calls >= remaining_budget:
                    break

                try:
                    from app.shared_rate_limiter import rate_limiter
                    await rate_limiter.acquire("blockscout")
                    total_calls += 1

                    host = CHAIN_HOSTS[chain]
                    resp = await client.get(
                        f"https://{host}/api/v2/addresses/{addr}/counters",
                        timeout=10,
                    )

                    if resp.status_code == 404:
                        continue
                    if resp.status_code != 200:
                        errors += 1
                        continue

                    data = resp.json()
                    tx_count = int(data.get("transactions_count") or data.get("tx_count") or 0)
                    token_count = int(data.get("token_transfers_count") or 0)

                    if tx_count == 0:
                        continue

                    chain_id = CHAIN_IDS[chain]
                    try:
                        with get_cursor() as cur:
                            cur.execute("""
                                INSERT INTO wallet_chain_presence
                                    (wallet_address, chain, chain_id, tx_count, token_count,
                                     discovery_method)
                                VALUES (%s, %s, %s, %s, %s, 'presence_check')
                                ON CONFLICT (wallet_address, chain) DO UPDATE SET
                                    tx_count = EXCLUDED.tx_count,
                                    token_count = EXCLUDED.token_count,
                                    last_verified_at = NOW()
                            """, (addr, chain, chain_id, tx_count, token_count))
                        presences_by_chain[chain] += 1
                    except Exception as e:
                        errors += 1
                        if errors <= 3:
                            logger.error(f"[wallet_presence] insert failed: {e}")

                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        logger.error(f"[wallet_presence] check failed {addr[:10]}.../{chain}: {e}")

    # Attestation
    total_presences = sum(presences_by_chain.values())
    try:
        from app.data_layer.provenance_scaling import attest_data_batch
        if total_presences > 0:
            attest_data_batch("wallet_chain_presence", [{"presences": total_presences, "mode": "B"}])
    except Exception:
        pass

    # SUMMARY
    presence_parts = ", ".join(f"{c}={n}" for c, n in presences_by_chain.items())
    logger.error(
        f"[wallet_presence] SUMMARY: scanned={len(addresses)} wallets × "
        f"{len(CHAINS_TO_CHECK)} chains = {total_calls} calls | "
        f"presences_found: {presence_parts} | "
        f"total_new_presences={total_presences} errors={errors}"
    )

    return {
        "wallets_scanned": len(addresses),
        "blockscout_calls": total_calls,
        "presences_by_chain": presences_by_chain,
        "total_new_presences": total_presences,
        "errors": errors,
    }


async def wallet_presence_background_loop():
    """Independent background loop — runs presence scan daily."""
    logger.error("[presence_bg] background loop started")
    await asyncio.sleep(180)  # stagger behind holder loops

    while True:
        try:
            logger.error("[presence_bg] loop tick, checking gate")
            count_row = fetch_one("SELECT COUNT(*) AS cnt FROM wallet_chain_presence WHERE discovery_method = 'presence_check'")
            row_count = int(count_row["cnt"]) if count_row else 0

            if row_count == 0:
                logger.error(f"[presence_bg] gate open: no presence_check rows yet")
            else:
                last = fetch_one(
                    "SELECT MAX(last_verified_at) AS latest FROM wallet_chain_presence WHERE discovery_method = 'presence_check'"
                )
                latest = last.get("latest") if last else None
                if latest:
                    if latest.tzinfo is None:
                        latest = latest.replace(tzinfo=timezone.utc)
                    age_h = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
                    if age_h < 22:
                        logger.error(f"[presence_bg] gate closed: {row_count} rows, last run {age_h:.0f}h ago")
                        await asyncio.sleep(3600)
                        continue
                    logger.error(f"[presence_bg] gate open: {row_count} rows, {age_h:.0f}h since last run")
                else:
                    logger.error(f"[presence_bg] gate open: {row_count} rows but no timestamp")

            logger.error("[presence_bg] running scan")
            result = await run_wallet_presence_scan()
            logger.error(f"[presence_bg] scan complete: {result}")
            await asyncio.sleep(22 * 3600)
        except Exception as e:
            logger.error(f"[presence_bg] ERROR: {type(e).__name__}: {e}")
            await asyncio.sleep(300)
