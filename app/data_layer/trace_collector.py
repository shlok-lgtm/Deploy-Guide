"""
LLL Phase 1 Pipeline 1 — Protocol Transaction Trace Observations
=================================================================
Fetches raw traces for top PSI protocol transactions via Blockscout v2.
Budget: ~143 Blockscout calls/day (<0.15% of 100K daily budget).
"""

import asyncio
import hashlib
import json
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

MAX_TXS_PER_PROTOCOL = 10
_disabled_until = 0.0


def _content_hash(tx_hash: str, chain: str, block_number: int) -> str:
    canonical = f"{tx_hash}|{chain}|{block_number}"
    return hashlib.sha256(canonical.encode()).hexdigest()


def _parse_trace_depth(trace) -> tuple[int, int]:
    """Walk trace tree to find max depth and internal call count."""
    if not trace:
        return 0, 0
    if isinstance(trace, list):
        max_d, total = 0, 0
        for item in trace:
            d, c = _parse_trace_depth(item)
            max_d = max(max_d, d)
            total += c
        return max_d, total + len(trace)
    if isinstance(trace, dict):
        calls = trace.get("calls") or trace.get("subtraces") or []
        if not calls:
            return 1, 1
        max_d, total = 0, 0
        for sub in (calls if isinstance(calls, list) else []):
            d, c = _parse_trace_depth(sub)
            max_d = max(max_d, d)
            total += c
        return max_d + 1, total + 1
    return 0, 0


async def run_trace_collection() -> dict:
    logger.error("[trace_collector] ENTRY — function called")
    global _disabled_until

    if time.time() < _disabled_until:
        remaining = int((_disabled_until - time.time()) / 3600)
        logger.error(f"[trace_collector] DISABLED: cooldown active, {remaining}h remaining")
        return {"status": "disabled"}

    logger.error("[trace_collector] step 1: querying rpi_protocol_config")
    protocols = fetch_all(
        "SELECT DISTINCT protocol_slug FROM rpi_protocol_config WHERE protocol_slug IS NOT NULL"
    )
    if not protocols:
        logger.error("[trace_collector] no protocols in rpi_protocol_config")
        return {"protocols": 0}

    slugs = [r["protocol_slug"] for r in protocols]
    logger.error(f"[trace_collector] step 2: {len(slugs)} protocols found")

    # Get contract addresses for each protocol
    logger.error("[trace_collector] step 3: querying protocol_pool_wallets")
    addr_rows = fetch_all("""
        SELECT DISTINCT protocol_slug, wallet_address, chain
        FROM protocol_pool_wallets
        WHERE protocol_slug = ANY(%s)
    """, (slugs,))

    proto_addrs = {}
    for r in (addr_rows or []):
        proto_addrs.setdefault(r["protocol_slug"], []).append(
            (r["wallet_address"], r.get("chain", "ethereum"))
        )

    logger.error(f"[trace_collector] step 4: {len(proto_addrs)} protocols have addresses, entering loop")

    total_traces = 0
    total_reverts = 0
    total_errors = 0
    total_calls = 0
    protocols_processed = 0

    logger.error("[trace_collector] step 5: entering main loop (using module-level httpx client)")
    client = _client
    # Pre-filter to only protocols that have on-chain addresses so the loop
    # counter matches user-visible progress. Previously the log read
    # "loop 0/33" → "step C.16" because `i` skipped protocols without
    # addresses and looked like a skip bug. The addressable set is what
    # actually drives API calls.
    addressable = [(slug, proto_addrs[slug][0]) for slug in slugs if proto_addrs.get(slug)]
    for i, (slug, addr_chain) in enumerate(addressable):
        addr, chain = addr_chain
        host = CHAIN_HOSTS.get(chain, CHAIN_HOSTS["ethereum"])

        if i < 3 or i % 10 == 0:
            logger.error(f"[trace_collector] loop {i}/{len(addressable)}: {slug} addr={addr[:12]}... chain={chain}")

        # Fetch recent txs for this protocol's primary address
        try:
            from app.shared_rate_limiter import rate_limiter
            logger.error(f"[trace_collector] step C.{i}: acquiring blockscout rate limiter")
            await rate_limiter.acquire("blockscout")
            logger.error(f"[trace_collector] step D.{i}: rate limiter acquired, making HTTP GET")
            total_calls += 1

            # Blockscout v9.0+ strictly validates parameters; `limit` is not
            # in the supported set for /addresses/{addr}/transactions.
            # Default page size is plenty for trace sampling.
            tx_url = f"https://{host}/api/v2/addresses/{addr}/transactions"
            resp = await client.get(tx_url, params={"filter": "to"})
            logger.error(f"[trace_collector] step E.{i}: HTTP {resp.status_code}")
            if resp.status_code != 200:
                total_errors += 1
                continue

            items = resp.json().get("items", [])
            protocols_processed += 1
        except Exception as e:
            total_errors += 1
            logger.error(f"[trace_collector] tx fetch failed for {slug}: {e}")
            continue

        # Fetch trace for each tx
        for tx_item in items[:MAX_TXS_PER_PROTOCOL]:
            tx_hash = tx_item.get("hash")
            if not tx_hash:
                continue

            try:
                await rate_limiter.acquire("blockscout")
                total_calls += 1

                trace_url = f"https://{host}/api/v2/transactions/{tx_hash}/raw-trace"
                trace_resp = await client.get(trace_url)

                if trace_resp.status_code != 200:
                    total_errors += 1
                    continue

                trace_data = trace_resp.json()
                depth, call_count = _parse_trace_depth(trace_data)
                block_num = tx_item.get("block_number") or tx_item.get("block", 0)
                revert = tx_item.get("revert_reason")

                ch = _content_hash(tx_hash, chain, block_num)

                with get_cursor() as cur:
                    cur.execute("""
                        INSERT INTO protocol_trace_observations
                            (tx_hash, protocol_slug, chain, block_number, value_usd,
                             trace_json, trace_depth, internal_call_count, revert_reason,
                             content_hash)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tx_hash, chain) DO NOTHING
                    """, (
                        tx_hash, slug, chain, block_num,
                        tx_item.get("value"),
                        json.dumps(trace_data),
                        depth, call_count, revert, ch,
                    ))

                total_traces += 1
                if revert:
                    total_reverts += 1

            except Exception as e:
                total_errors += 1
                if total_errors <= 3:
                    logger.error(f"[trace_collector] trace fetch failed {tx_hash[:12]}...: {e}")

    # Kill signal: >20% error rate
    total_fetches = total_calls - len(slugs)  # subtract tx list fetches
    if total_fetches > 0 and total_errors / max(total_fetches, 1) > 0.20:
        _disabled_until = time.time() + 86400
        logger.error(
            f"[trace_collector] AUTO-DISABLED: error rate "
            f"{round(total_errors / total_fetches * 100)}% exceeded 20% threshold"
        )

    # Attestation
    try:
        from app.data_layer.provenance_scaling import attest_data_batch
        if total_traces > 0:
            attest_data_batch("protocol_traces", [{"traces": total_traces}])
    except Exception:
        pass

    logger.error(
        f"[trace_collector] SUMMARY: protocols={protocols_processed}, "
        f"txs_queried={total_calls - protocols_processed}, traces_captured={total_traces}, "
        f"reverts_found={total_reverts}, errors={total_errors}, blockscout_calls={total_calls}"
    )

    return {
        "protocols": protocols_processed,
        "traces_captured": total_traces,
        "reverts_found": total_reverts,
        "errors": total_errors,
        "blockscout_calls": total_calls,
    }


# ---------------------------------------------------------------------------
# Independent background loop — sidestep pattern
# ---------------------------------------------------------------------------
# The enrichment_worker pipeline path was dispatching this collector with a
# `gate_check` that hung before the "starting: N..." log line could flush
# (same failure mode as the now-sidestepped SSS / multichain / presence
# collectors). This standalone loop runs the collector on its own cadence
# and is launched from app/worker.py at startup. Logs one tick per hour
# so Railway shows liveness.

LOOP_CHECK_INTERVAL = 3600       # hourly tick
LOOP_GATE_HOURS = 6              # run at most every 6h


async def trace_collector_background_loop():
    logger.error("[trace_bg] background loop started")
    await asyncio.sleep(90)       # initial delay — let pool init complete
    while True:
        try:
            last = fetch_one(
                "SELECT MAX(captured_at) AS latest FROM protocol_trace_observations"
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
                    f"[trace_bg] gate open (last_run={age_h:.1f}h ago, "
                    f"threshold={LOOP_GATE_HOURS}h) — running scan"
                )
                result = await run_trace_collection()
                logger.error(f"[trace_bg] scan complete: {result}")
            else:
                logger.error(
                    f"[trace_bg] gate closed (last_run={age_h:.1f}h ago, "
                    f"threshold={LOOP_GATE_HOURS}h) — sleeping 1h"
                )
        except Exception as e:
            logger.error(f"[trace_bg] loop error: {type(e).__name__}: {e}")
            await asyncio.sleep(300)
            continue
        await asyncio.sleep(LOOP_CHECK_INTERVAL)
