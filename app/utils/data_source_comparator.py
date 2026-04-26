"""
Data Source Comparator
=======================
Shadow comparison harness: makes the same API call to both Etherscan and
Blockscout, compares results, and logs parity statistics.

This is READ-ONLY evaluation — no scoring logic is changed.
Etherscan remains the source of truth for all scoring data.

Comparison runs are time-bounded: only active during the evaluation period.
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone

import httpx

from app.database import execute, fetch_one, fetch_all
from app.api_usage_tracker import track_api_call
from app.utils.blockscout_client import (
    get_contract_abi as bs_get_abi,
    get_token_transfers as bs_get_transfers,
    get_token_holder_count as bs_get_holder_count,
    get_token_holder_list as bs_get_holder_list,
    get_address_token_balance as bs_get_token_balance,
    get_credit_stats,
    RATE_LIMIT_DELAY as BS_RATE_LIMIT,
)

logger = logging.getLogger(__name__)

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"

# Feature flag: comparison period (1 week from first run)
COMPARISON_ENABLED_ENV = os.environ.get("BLOCKSCOUT_COMPARISON_ENABLED", "true")
COMPARISON_DURATION_DAYS = 7


def is_comparison_active() -> bool:
    """Check if the comparison window is still active."""
    if COMPARISON_ENABLED_ENV.lower() != "true":
        return False

    try:
        row = fetch_one("SELECT MIN(compared_at) AS first FROM data_source_comparisons")
        if not row or not row.get("first"):
            return True  # No comparisons yet — start now
        first = row["first"]
        if first.tzinfo is None:
            first = first.replace(tzinfo=timezone.utc)
        elapsed_days = (datetime.now(timezone.utc) - first).total_seconds() / 86400
        return elapsed_days < COMPARISON_DURATION_DAYS
    except Exception:
        return False  # Table doesn't exist yet — skip until migration runs


def _hash_result(data: dict) -> str:
    """Hash API result for comparison (strip metadata, sort keys)."""
    # Remove timing/metadata fields
    clean = {k: v for k, v in data.items() if not k.startswith("_")}
    canonical = json.dumps(clean, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _close_match(etherscan_data: dict, blockscout_data: dict) -> bool:
    """
    Check if results are close (within 1% for numeric fields).
    Used when hashes don't match exactly but data is functionally equivalent.
    """
    try:
        es_result = etherscan_data.get("result")
        bs_result = blockscout_data.get("result")

        # Both empty or equal
        if es_result == bs_result:
            return True

        # Compare list lengths
        if isinstance(es_result, list) and isinstance(bs_result, list):
            if abs(len(es_result) - len(bs_result)) <= max(1, len(es_result) * 0.01):
                return True

        # Compare numeric results
        if isinstance(es_result, str) and isinstance(bs_result, str):
            try:
                es_val = float(es_result)
                bs_val = float(bs_result)
                if es_val == 0 and bs_val == 0:
                    return True
                if es_val != 0 and abs(es_val - bs_val) / abs(es_val) < 0.01:
                    return True
            except ValueError:
                pass
    except Exception:
        pass
    return False


def _store_comparison(
    endpoint: str,
    params: dict,
    etherscan_hash: str,
    blockscout_hash: str,
    match_status: str,
    etherscan_ms: int,
    blockscout_ms: int,
):
    """Store a comparison result in the database."""
    try:
        execute(
            """
            INSERT INTO data_source_comparisons
                (endpoint, params, etherscan_hash, blockscout_hash,
                 match_status, etherscan_ms, blockscout_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                endpoint,
                json.dumps(params, default=str),
                etherscan_hash,
                blockscout_hash,
                match_status,
                etherscan_ms,
                blockscout_ms,
            ),
        )
    except Exception as e:
        logger.debug(f"Failed to store comparison: {e}")


# =============================================================================
# Comparison functions — one per Etherscan endpoint type
# =============================================================================

async def compare_contract_abi(
    client: httpx.AsyncClient,
    address: str,
    chain_id: int = 1,
):
    """Shadow-compare contract ABI fetch."""
    if not is_comparison_active():
        return

    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        return

    # Etherscan call
    es_start = time.monotonic()
    _es_status = None
    try:
        es_resp = await client.get(ETHERSCAN_V2_BASE, params={
            "chainid": chain_id, "module": "contract", "action": "getabi",
            "address": address, "apikey": api_key,
        }, timeout=20)
        _es_status = es_resp.status_code
        es_data = es_resp.json()
        es_ms = int((time.monotonic() - es_start) * 1000)
    except Exception as e:
        _es_status = 0
        _store_comparison("contract/getabi", {"address": address}, "", "",
                          "etherscan_error", 0, 0)
        return
    finally:
        try:
            track_api_call(provider="etherscan", endpoint="contract/getabi", caller="utils.data_source_comparator", status=_es_status, latency_ms=int((time.monotonic() - es_start) * 1000))
        except Exception:
            pass

    # Blockscout call
    try:
        bs_data = await bs_get_abi(client, address, chain_id)
        bs_ms = bs_data.pop("_blockscout_response_time_ms", 0)
        bs_data.pop("_blockscout_error", None)
    except Exception:
        _store_comparison("contract/getabi", {"address": address},
                          _hash_result(es_data), "", "blockscout_error", es_ms, 0)
        return

    # Compare
    es_hash = _hash_result(es_data)
    bs_hash = _hash_result(bs_data)

    if es_hash == bs_hash:
        status = "exact"
    elif _close_match(es_data, bs_data):
        status = "close"
    else:
        status = "mismatch"

    _store_comparison("contract/getabi", {"address": address},
                      es_hash, bs_hash, status, es_ms, bs_ms)


async def compare_token_transfers(
    client: httpx.AsyncClient,
    address: str,
    contract_address: str = None,
    chain_id: int = 1,
):
    """Shadow-compare token transfer fetch."""
    if not is_comparison_active():
        return

    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        return

    params = {
        "chainid": chain_id, "module": "account", "action": "tokentx",
        "address": address, "page": 1, "offset": 100, "sort": "desc",
        "apikey": api_key,
    }
    if contract_address:
        params["contractaddress"] = contract_address

    es_start = time.monotonic()
    _es_status = None
    try:
        es_resp = await client.get(ETHERSCAN_V2_BASE, params=params, timeout=20)
        _es_status = es_resp.status_code
        es_data = es_resp.json()
        es_ms = int((time.monotonic() - es_start) * 1000)
    except Exception:
        _es_status = 0
        _store_comparison("account/tokentx", {"address": address}, "", "",
                          "etherscan_error", 0, 0)
        return
    finally:
        try:
            track_api_call(provider="etherscan", endpoint="account/tokentx", caller="utils.data_source_comparator", status=_es_status, latency_ms=int((time.monotonic() - es_start) * 1000))
        except Exception:
            pass

    try:
        bs_data = await bs_get_transfers(client, address, contract_address, chain_id)
        bs_ms = bs_data.pop("_blockscout_response_time_ms", 0)
        bs_data.pop("_blockscout_error", None)
    except Exception:
        _store_comparison("account/tokentx", {"address": address},
                          _hash_result(es_data), "", "blockscout_error", es_ms, 0)
        return

    es_hash = _hash_result(es_data)
    bs_hash = _hash_result(bs_data)
    if es_hash == bs_hash:
        status = "exact"
    elif _close_match(es_data, bs_data):
        status = "close"
    else:
        status = "mismatch"

    _store_comparison("account/tokentx", {"address": address},
                      es_hash, bs_hash, status, es_ms, bs_ms)


async def compare_token_holder_count(
    client: httpx.AsyncClient,
    contract_address: str,
    chain_id: int = 1,
):
    """Shadow-compare token holder count."""
    if not is_comparison_active():
        return

    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        return

    es_start = time.monotonic()
    _es_status = None
    try:
        es_resp = await client.get(ETHERSCAN_V2_BASE, params={
            "chainid": chain_id, "module": "token", "action": "tokenholdercount",
            "contractaddress": contract_address, "apikey": api_key,
        }, timeout=20)
        _es_status = es_resp.status_code
        es_data = es_resp.json()
        es_ms = int((time.monotonic() - es_start) * 1000)
    except Exception:
        _es_status = 0
        _store_comparison("token/tokenholdercount", {"contract": contract_address},
                          "", "", "etherscan_error", 0, 0)
        return
    finally:
        try:
            track_api_call(provider="etherscan", endpoint="token/tokenholdercount", caller="utils.data_source_comparator", status=_es_status, latency_ms=int((time.monotonic() - es_start) * 1000))
        except Exception:
            pass

    try:
        bs_data = await bs_get_holder_count(client, contract_address, chain_id)
        bs_ms = bs_data.pop("_blockscout_response_time_ms", 0)
        bs_data.pop("_blockscout_error", None)
    except Exception:
        _store_comparison("token/tokenholdercount", {"contract": contract_address},
                          _hash_result(es_data), "", "blockscout_error", es_ms, 0)
        return

    es_hash = _hash_result(es_data)
    bs_hash = _hash_result(bs_data)
    if es_hash == bs_hash:
        status = "exact"
    elif _close_match(es_data, bs_data):
        status = "close"
    else:
        status = "mismatch"

    _store_comparison("token/tokenholdercount", {"contract": contract_address},
                      es_hash, bs_hash, status, es_ms, bs_ms)


# =============================================================================
# Summary endpoint helper
# =============================================================================

def get_comparison_summary() -> dict:
    """
    Generate a summary of all comparison runs.
    Used by GET /api/admin/blockscout-comparison.
    """
    try:
        total = fetch_one("SELECT COUNT(*) as cnt FROM data_source_comparisons")
        total_count = total["cnt"] if total else 0

        if total_count == 0:
            return {
                "total_comparisons": 0,
                "status": "no_data",
                "message": "No comparisons have been run yet",
            }

        # Match breakdown
        breakdown = fetch_all(
            "SELECT match_status, COUNT(*) as cnt FROM data_source_comparisons GROUP BY match_status"
        )
        status_counts = {r["match_status"]: r["cnt"] for r in breakdown}

        exact = status_counts.get("exact", 0)
        close = status_counts.get("close", 0)
        mismatch = status_counts.get("mismatch", 0)
        es_error = status_counts.get("etherscan_error", 0)
        bs_error = status_counts.get("blockscout_error", 0)

        # Average response times
        avg_times = fetch_one(
            """
            SELECT
                AVG(etherscan_ms) as avg_es_ms,
                AVG(blockscout_ms) as avg_bs_ms
            FROM data_source_comparisons
            WHERE etherscan_ms > 0 AND blockscout_ms > 0
            """
        )

        # Endpoint breakdown for mismatches
        mismatch_endpoints = []
        if mismatch > 0:
            mismatch_rows = fetch_all(
                """
                SELECT endpoint, COUNT(*) as cnt
                FROM data_source_comparisons
                WHERE match_status = 'mismatch'
                GROUP BY endpoint
                ORDER BY cnt DESC
                """
            )
            mismatch_endpoints = [{"endpoint": r["endpoint"], "count": r["cnt"]} for r in mismatch_rows]

        # Time range
        time_range = fetch_one(
            "SELECT MIN(compared_at) as first, MAX(compared_at) as last FROM data_source_comparisons"
        )

        # Blockscout credit stats
        credit_stats = get_credit_stats()

        return {
            "total_comparisons": total_count,
            "exact_match_pct": round(exact / total_count * 100, 1) if total_count else 0,
            "close_match_pct": round(close / total_count * 100, 1) if total_count else 0,
            "mismatch_pct": round(mismatch / total_count * 100, 1) if total_count else 0,
            "etherscan_error_pct": round(es_error / total_count * 100, 1) if total_count else 0,
            "blockscout_error_pct": round(bs_error / total_count * 100, 1) if total_count else 0,
            "combined_match_pct": round((exact + close) / total_count * 100, 1) if total_count else 0,
            "avg_etherscan_ms": round(float(avg_times.get("avg_es_ms", 0) or 0), 0) if avg_times else 0,
            "avg_blockscout_ms": round(float(avg_times.get("avg_bs_ms", 0) or 0), 0) if avg_times else 0,
            "mismatch_details": mismatch_endpoints,
            "first_comparison": str(time_range["first"]) if time_range and time_range.get("first") else None,
            "last_comparison": str(time_range["last"]) if time_range and time_range.get("last") else None,
            "blockscout_credits": credit_stats,
            "recommendation": (
                "PROCEED with migration" if total_count >= 50 and (exact + close) / total_count > 0.95
                else "CONTINUE evaluation" if total_count < 50
                else "INVESTIGATE mismatches before migration"
            ),
        }
    except Exception as e:
        logger.warning(f"Failed to generate comparison summary: {e}")
        return {"error": str(e)}
