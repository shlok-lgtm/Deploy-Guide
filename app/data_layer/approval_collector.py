"""
LLL Phase 1 Pipeline 2 — Token Approval Snapshots
===================================================
Diff-capture of ERC-20 approval state for top wallets via Blockscout v2.
Budget: ~500 Blockscout calls/day (<0.5% of 100K daily budget).
"""

import hashlib
import logging
import time

import httpx

from app.database import fetch_all, fetch_one, get_cursor

logger = logging.getLogger(__name__)

CHAIN_HOSTS = {
    "ethereum": "eth.blockscout.com",
    "base": "base.blockscout.com",
    "arbitrum": "arbitrum.blockscout.com",
}

MAX_WALLETS = 500
_disabled_until = 0.0


async def run_approval_collection() -> dict:
    global _disabled_until

    if time.time() < _disabled_until:
        remaining = int((_disabled_until - time.time()) / 3600)
        logger.error(f"[approval_collector] DISABLED: cooldown active, {remaining}h remaining")
        return {"status": "disabled"}

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

    logger.error(f"[approval_collector] starting: {len(wallets)} wallets to scan")

    total_approvals_seen = 0
    total_unchanged = 0
    total_inserted = 0
    total_errors = 0
    total_calls = 0
    max_allowance_usd = 0.0

    async with httpx.AsyncClient(timeout=30) as client:
        for wallet_row in wallets:
            addr = wallet_row["address"]
            chain = "ethereum"
            host = CHAIN_HOSTS[chain]

            try:
                from app.shared_rate_limiter import rate_limiter
                await rate_limiter.acquire("blockscout")
                total_calls += 1

                url = f"https://{host}/api/v2/addresses/{addr}/token-transfers"
                resp = await client.get(url, params={"type": "ERC-20", "filter": "from", "limit": 50})

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

                    # Diff-capture: check if allowance changed vs last snapshot
                    prev = fetch_one("""
                        SELECT allowance FROM token_approval_snapshots
                        WHERE wallet_address = %s AND token_address = %s AND spender_address = %s AND chain = %s
                        ORDER BY snapshot_at DESC LIMIT 1
                    """, (addr.lower(), token_addr, to_addr, chain))

                    prev_allowance = float(prev["allowance"]) if prev else None

                    if prev_allowance is not None and abs(prev_allowance - allowance) < 0.01:
                        total_unchanged += 1
                        continue

                    allowance_usd = allowance  # Stablecoins ≈ $1
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
