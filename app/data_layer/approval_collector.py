"""
LLL Phase 1 Pipeline 2 — Token Approval Snapshots
===================================================
Diff-capture of ERC-20 approval state for top wallets via Blockscout v2.
Budget: ~500 Blockscout calls/day (<0.5% of 100K daily budget).
"""

import asyncio
import hashlib
import logging
import time
from datetime import datetime, timezone

import httpx

from app.database import fetch_all, fetch_one, get_cursor

logger = logging.getLogger(__name__)

_client = httpx.AsyncClient(
    timeout=30, follow_redirects=True,
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
)

CHAIN_HOSTS = {
    "ethereum": "eth.blockscout.com",
    "base": "base.blockscout.com",
    "arbitrum": "arbitrum.blockscout.com",
}

MAX_WALLETS = 500
_disabled_until = 0.0


async def run_approval_collection() -> dict:
    logger.error("[approval_collector] ENTRY — function called")
    global _disabled_until

    if time.time() < _disabled_until:
        remaining = int((_disabled_until - time.time()) / 3600)
        logger.error(f"[approval_collector] DISABLED: cooldown active, {remaining}h remaining")
        return {"status": "disabled"}

    logger.error("[approval_collector] step 1: querying wallet_graph.wallets")
    wallets = fetch_all(f"""
        SELECT address, total_stablecoin_value
        FROM wallet_graph.wallets
        WHERE total_stablecoin_value IS NOT NULL
        ORDER BY total_stablecoin_value DESC
        LIMIT {MAX_WALLETS}
    """)

    if not wallets:
        logger.error("[approval_collector] no wallets found in wallet_graph.wallets")
        return {"wallets_scanned": 0}

    logger.error(f"[approval_collector] step 2: {len(wallets)} wallets found, entering loop")

    total_approvals_seen = 0
    total_unchanged = 0
    total_inserted = 0
    total_errors = 0
    total_calls = 0
    max_allowance_usd = 0.0

    logger.error("[approval_collector] step 3: entering main loop (using module-level httpx client)")
    client = _client
    for wi, wallet_row in enumerate(wallets):
        addr = wallet_row["address"]
        chain = "ethereum"
        host = CHAIN_HOSTS[chain]

        if wi < 3 or wi % 100 == 0:
            logger.error(f"[approval_collector] loop {wi}/{len(wallets)}: {addr[:12]}...")

        try:
            from app.shared_rate_limiter import rate_limiter
            if wi < 3:
                logger.error(f"[approval_collector] step C.{wi}: acquiring blockscout rate limiter")
            await rate_limiter.acquire("blockscout")
            if wi < 3:
                logger.error(f"[approval_collector] step D.{wi}: rate limiter acquired, making HTTP GET")
            total_calls += 1

            url = f"https://{host}/api/v2/addresses/{addr}/token-transfers"
            resp = await client.get(url, params={"type": "ERC-20", "filter": "from", "limit": 50})
            if wi < 3:
                logger.error(f"[approval_collector] step E.{wi}: HTTP {resp.status_code}")

            if resp.status_code == 404:
                continue
            if resp.status_code != 200:
                total_errors += 1
                continue

            data = resp.json()
            items = data.get("items", [])

            seen_approvals = set()
            for item in items:
                token_addr = (item.get("token", {}).get("address") or "").lower()
                to_addr = (item.get("to", {}).get("hash") or "").lower()
                if not token_addr or not to_addr:
                    continue

                key = (addr.lower(), token_addr, to_addr)
                if key in seen_approvals:
                    continue
                seen_approvals.add(key)

                amount_raw = item.get("total", {}).get("value", "0")
                try:
                    decimals = int(item.get("token", {}).get("decimals", "18") or "18")
                    allowance = float(int(amount_raw)) / (10 ** decimals)
                except (ValueError, OverflowError):
                    allowance = 0

                total_approvals_seen += 1

                prev = fetch_one("""
                    SELECT allowance FROM token_approval_snapshots
                    WHERE wallet_address = %s AND token_address = %s AND spender_address = %s AND chain = %s
                    ORDER BY snapshot_at DESC LIMIT 1
                """, (addr.lower(), token_addr, to_addr, chain))

                prev_allowance = float(prev["allowance"]) if prev else None

                if prev_allowance is not None and abs(prev_allowance - allowance) < 0.01:
                    total_unchanged += 1
                    continue

                allowance_usd = allowance
                if allowance_usd > max_allowance_usd:
                    max_allowance_usd = allowance_usd

                try:
                    with get_cursor() as cur:
                        cur.execute("""
                            INSERT INTO token_approval_snapshots
                                (wallet_address, token_address, spender_address,
                                 allowance, allowance_usd, chain, previous_allowance)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            addr.lower(), token_addr, to_addr,
                            allowance, allowance_usd, chain,
                            prev_allowance if prev_allowance is not None else 0,
                        ))
                    total_inserted += 1
                except Exception as e:
                    total_errors += 1
                    if total_errors <= 3:
                        logger.error(f"[approval_collector] insert failed: {e}")

        except Exception as e:
            total_errors += 1
            if total_errors <= 5:
                logger.error(f"[approval_collector] wallet {addr[:10]}... failed: {e}")

    # Kill signal: 5x expected calls
    if total_calls > 2500:
        _disabled_until = time.time() + 86400
        logger.error(f"[approval_collector] AUTO-DISABLED: {total_calls} calls exceeded 2500 budget threshold")

    # Attestation
    try:
        from app.data_layer.provenance_scaling import attest_data_batch
        if total_inserted > 0:
            attest_data_batch("token_approvals", [{"inserted": total_inserted}])
    except Exception:
        pass

    logger.error(
        f"[approval_collector] SUMMARY: wallets_scanned={len(wallets)}, "
        f"approvals_seen={total_approvals_seen}, unchanged_skipped={total_unchanged}, "
        f"new_or_changed={total_inserted}, max_allowance_usd=${max_allowance_usd:,.0f}, "
        f"blockscout_calls={total_calls}, errors={total_errors}"
    )

    return {
        "wallets_scanned": len(wallets),
        "approvals_seen": total_approvals_seen,
        "unchanged_skipped": total_unchanged,
        "new_or_changed": total_inserted,
        "errors": total_errors,
        "blockscout_calls": total_calls,
    }


# ---------------------------------------------------------------------------
# Independent background loop — sidestep pattern (matches trace_collector_bg,
# holder_ingestion_bg, multichain_holder_bg, wallet_presence_bg)
# ---------------------------------------------------------------------------

LOOP_CHECK_INTERVAL = 3600       # hourly tick
LOOP_GATE_HOURS = 24             # run at most daily


async def approval_collector_background_loop():
    logger.error("[approval_bg] background loop started")
    await asyncio.sleep(120)      # initial delay — let pool init + trace_bg start
    while True:
        try:
            last = fetch_one(
                "SELECT MAX(snapshot_at) AS latest FROM token_approval_snapshots"
            )
            latest = last.get("latest") if last else None
            if latest is None:
                age_h = float("inf")
            else:
                if latest.tzinfo is None:
                    latest = latest.replace(tzinfo=timezone.utc)
                age_h = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

            if age_h >= LOOP_GATE_HOURS:
                logger.error(
                    f"[approval_bg] gate open (last_run={age_h:.1f}h ago, "
                    f"threshold={LOOP_GATE_HOURS}h) — running scan"
                )
                result = await run_approval_collection()
                logger.error(f"[approval_bg] scan complete: {result}")
            else:
                logger.error(
                    f"[approval_bg] gate closed (last_run={age_h:.1f}h ago, "
                    f"threshold={LOOP_GATE_HOURS}h) — sleeping 1h"
                )
        except Exception as e:
            logger.error(f"[approval_bg] loop error: {type(e).__name__}: {e}")
            await asyncio.sleep(300)
            continue
        await asyncio.sleep(LOOP_CHECK_INTERVAL)
