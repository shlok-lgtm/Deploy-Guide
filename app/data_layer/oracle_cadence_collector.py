"""
LLL Phase 1 Pipeline 3 — Oracle Update Cadence Capture
========================================================
Tracks Chainlink round updates at 5-min sampling intervals.
Budget: ~49K Alchemy CU/day (~4.9% of 1M free-tier daily budget).

Runs as an independent asyncio task in worker main(), NOT per-cycle.
"""

import asyncio
import hashlib
import logging
import os
import time
from datetime import datetime, timezone

import httpx

from app.database import fetch_all, fetch_one, get_cursor

logger = logging.getLogger(__name__)

SAMPLE_INTERVAL = 300  # 5 minutes
LATESTROUND_SELECTOR = "0x668a0f02"  # latestRound()
LATESTROUNDDATA_SELECTOR = "0xfeaf968c"  # latestRoundData()

# In-memory cache: oracle_address → (round_id, updated_at_timestamp)
_last_seen: dict[str, tuple[int, int]] = {}
_consecutive_errors = 0
_degraded_until = 0.0


def _content_hash(oracle_id: str, round_id: int, updated_at_block: int) -> str:
    canonical = f"{oracle_id}|{round_id}|{updated_at_block}"
    return hashlib.sha256(canonical.encode()).hexdigest()


def _get_rpc_url(chain: str) -> str:
    alchemy_key = os.environ.get("ALCHEMY_API_KEY", "")
    if alchemy_key:
        chain_map = {"ethereum": "eth-mainnet", "base": "base-mainnet", "arbitrum": "arb-mainnet"}
        network = chain_map.get(chain)
        if network:
            return f"https://{network}.g.alchemy.com/v2/{alchemy_key}"
    return ""


async def _eth_call(client: httpx.AsyncClient, rpc_url: str, to: str, data: str) -> str:
    resp = await client.post(rpc_url, json={
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": to, "data": data}, "latest"],
    })
    result = resp.json()
    if "error" in result:
        raise Exception(result["error"].get("message", str(result["error"])))
    return result.get("result", "0x")


def _parse_round_data(hex_result: str) -> tuple[int, int, int, int, int]:
    """Parse latestRoundData() return: (roundId, answer, startedAt, updatedAt, answeredInRound)."""
    hex_result = hex_result.replace("0x", "")
    if len(hex_result) < 320:
        return 0, 0, 0, 0, 0
    round_id = int(hex_result[0:64], 16)
    answer = int(hex_result[64:128], 16)
    # Handle signed int for answer
    if answer > 2**255:
        answer = answer - 2**256
    started_at = int(hex_result[128:192], 16)
    updated_at = int(hex_result[192:256], 16)
    answered_in = int(hex_result[256:320], 16)
    return round_id, answer, started_at, updated_at, answered_in


async def _sample_oracles(client: httpx.AsyncClient) -> dict:
    """One sampling pass across all active oracles."""
    global _consecutive_errors

    oracles = fetch_all("SELECT * FROM oracle_registry WHERE is_active = TRUE")
    if not oracles:
        return {"oracles": 0}

    new_rounds = 0
    unchanged = 0
    errors = 0

    for oracle in oracles:
        oracle_addr = oracle["oracle_address"]
        oracle_id = oracle.get("oracle_name") or oracle_addr[:10]
        chain = oracle.get("chain", "ethereum")
        rpc_url = _get_rpc_url(chain)
        if not rpc_url:
            continue

        try:
            from app.shared_rate_limiter import rate_limiter
            await rate_limiter.acquire("alchemy")

            raw = await _eth_call(client, rpc_url, oracle_addr, LATESTROUNDDATA_SELECTOR)
            round_id, answer, _, updated_at, _ = _parse_round_data(raw)

            if round_id == 0:
                errors += 1
                continue

            last = _last_seen.get(oracle_addr)
            if last and last[0] == round_id:
                unchanged += 1
                continue

            # New round detected
            gap_seconds = None
            if last:
                gap_seconds = updated_at - last[1]

            _last_seen[oracle_addr] = (round_id, updated_at)
            ch = _content_hash(oracle_id, round_id, updated_at)

            decimals = int(oracle.get("decimals") or 8)
            answer_float = answer / (10 ** decimals)
            updated_ts = datetime.fromtimestamp(updated_at, tz=timezone.utc)

            try:
                with get_cursor() as cur:
                    cur.execute("""
                        INSERT INTO oracle_update_cadence
                            (oracle_id, round_id, answer, updated_at_block, updated_at_timestamp,
                             gap_from_previous_seconds, content_hash)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (oracle_id, round_id) DO NOTHING
                    """, (
                        oracle_id, round_id, answer_float, updated_at, updated_ts,
                        gap_seconds, ch,
                    ))
                new_rounds += 1
            except Exception as e:
                errors += 1
                logger.error(f"[oracle_cadence] insert failed for {oracle_id}: {e}")

            _consecutive_errors = 0

        except Exception as e:
            errors += 1
            _consecutive_errors += 1
            if errors <= 3:
                logger.error(f"[oracle_cadence] sample failed for {oracle_id}: {e}")

    # Attestation
    if new_rounds > 0:
        try:
            from app.data_layer.provenance_scaling import attest_data_batch
            attest_data_batch("oracle_cadence", [{"new_rounds": new_rounds}])
        except Exception:
            pass

    return {
        "oracles": len(oracles),
        "new_rounds": new_rounds,
        "unchanged": unchanged,
        "errors": errors,
    }


async def run_oracle_cadence_loop():
    """Independent asyncio task — samples every 5 minutes, runs forever."""
    global _degraded_until, _consecutive_errors

    logger.error("[oracle_cadence] loop started, sampling every 300s")
    await asyncio.sleep(30)  # initial delay to let pool initialize

    cycle = 0
    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            cycle += 1
            interval = SAMPLE_INTERVAL

            # Degraded mode: revert to 20-min sampling
            if time.time() < _degraded_until:
                interval = 1200
                if cycle % 12 == 1:
                    logger.error("[oracle_cadence] DEGRADED: 20-min sampling active")

            try:
                result = await _sample_oracles(client)

                if cycle % 12 == 0 or result.get("new_rounds", 0) > 0:
                    logger.error(
                        f"[oracle_cadence] sample #{cycle}: "
                        f"new_rounds={result.get('new_rounds', 0)}, "
                        f"unchanged={result.get('unchanged', 0)}, "
                        f"errors={result.get('errors', 0)}"
                    )

                # Kill signal: 3 consecutive windows with >30% errors
                if _consecutive_errors >= 6:  # 6 errors across 3 windows × 2 avg
                    _degraded_until = time.time() + 3600
                    _consecutive_errors = 0
                    logger.error("[oracle_cadence] PAUSED: error rate exceeded threshold, degrading to 20-min for 1h")

            except Exception as e:
                logger.error(f"[oracle_cadence] loop error: {e}")

            await asyncio.sleep(interval)
