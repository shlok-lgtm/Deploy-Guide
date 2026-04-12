#!/usr/bin/env python3
"""
Sprint 5 Verification: Blockscout Migration Evaluation
========================================================
Runs 10 comparison queries between Etherscan V2 and Blockscout,
printing match/mismatch for each.

Requirements:
  - ETHERSCAN_API_KEY env var
  - BLOCKSCOUT_API_KEY env var (optional — free tier works without key)

Usage:
  python scripts/test_sprint5.py
"""

import asyncio
import hashlib
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

# Well-known contracts for comparison
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
DAI = "0x6b175474e89094c44da98b954eedeac495271d0f"
AAVE_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
BLOCKSCOUT_BASE = "https://api.blockscout.com/v2/api"


def _check_env():
    if not os.environ.get("ETHERSCAN_API_KEY"):
        print("ERROR: Missing env var: ETHERSCAN_API_KEY")
        print("Set it and re-run.")
        sys.exit(1)


def _hash(data: dict) -> str:
    clean = {k: v for k, v in data.items() if not k.startswith("_")}
    return hashlib.sha256(
        json.dumps(clean, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()[:12]


def _compare_results(name: str, es_data: dict, bs_data: dict, es_ms: int, bs_ms: int) -> dict:
    """Compare two API results and print verdict."""
    es_status = es_data.get("status")
    bs_status = bs_data.get("status")

    # Check for errors
    if es_status != "1" and not isinstance(es_data.get("result"), list):
        return {"name": name, "status": "etherscan_error", "detail": es_data.get("message", "")}
    if bs_status != "1" and not isinstance(bs_data.get("result"), list):
        return {"name": name, "status": "blockscout_error", "detail": bs_data.get("message", "")}

    es_result = es_data.get("result")
    bs_result = bs_data.get("result")

    # Exact match check
    es_hash = _hash(es_data)
    bs_hash = _hash(bs_data)

    if es_hash == bs_hash:
        return {"name": name, "status": "exact", "es_ms": es_ms, "bs_ms": bs_ms}

    # Close match: for numeric results, check within 1%
    try:
        if isinstance(es_result, str) and isinstance(bs_result, str):
            es_val = int(es_result)
            bs_val = int(bs_result)
            diff = abs(es_val - bs_val)
            if es_val > 0 and diff / es_val < 0.01:
                return {
                    "name": name, "status": "close",
                    "es_val": es_val, "bs_val": bs_val, "diff": diff,
                    "es_ms": es_ms, "bs_ms": bs_ms,
                }
    except (ValueError, TypeError):
        pass

    # Close match: for list results, check length similarity
    if isinstance(es_result, list) and isinstance(bs_result, list):
        es_len = len(es_result)
        bs_len = len(bs_result)
        if es_len > 0 and abs(es_len - bs_len) <= max(1, int(es_len * 0.01)):
            return {
                "name": name, "status": "close",
                "es_len": es_len, "bs_len": bs_len,
                "es_ms": es_ms, "bs_ms": bs_ms,
            }

    return {"name": name, "status": "mismatch", "es_ms": es_ms, "bs_ms": bs_ms}


async def _query(client, base_url, params, timeout=20):
    """Make an API query and return (data, elapsed_ms)."""
    start = time.monotonic()
    resp = await client.get(base_url, params=params, timeout=timeout)
    elapsed = int((time.monotonic() - start) * 1000)
    return resp.json(), elapsed


async def run_tests():
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    bs_key = os.environ.get("BLOCKSCOUT_API_KEY", "")

    # Define 10 comparison queries
    queries = [
        {
            "name": "tokenholdercount USDC",
            "module": "token", "action": "tokenholdercount",
            "extra": {"contractaddress": USDC},
        },
        {
            "name": "tokenholdercount USDT",
            "module": "token", "action": "tokenholdercount",
            "extra": {"contractaddress": USDT},
        },
        {
            "name": "tokenholdercount DAI",
            "module": "token", "action": "tokenholdercount",
            "extra": {"contractaddress": DAI},
        },
        {
            "name": "getabi USDC",
            "module": "contract", "action": "getabi",
            "extra": {"address": USDC},
        },
        {
            "name": "getabi Aave Pool",
            "module": "contract", "action": "getabi",
            "extra": {"address": AAVE_POOL},
        },
        {
            "name": "getsourcecode USDC",
            "module": "contract", "action": "getsourcecode",
            "extra": {"address": USDC},
        },
        {
            "name": "tokenholderlist USDC (top 10)",
            "module": "token", "action": "tokenholderlist",
            "extra": {"contractaddress": USDC, "page": 1, "offset": 10},
        },
        {
            "name": "tokenholderlist USDT (top 10)",
            "module": "token", "action": "tokenholderlist",
            "extra": {"contractaddress": USDT, "page": 1, "offset": 10},
        },
        {
            "name": "tokenholdercount WETH",
            "module": "token", "action": "tokenholdercount",
            "extra": {"contractaddress": WETH},
        },
        {
            "name": "getabi WETH",
            "module": "contract", "action": "getabi",
            "extra": {"address": WETH},
        },
    ]

    results = []
    exact = 0
    close = 0
    mismatch = 0
    errors = 0

    async with httpx.AsyncClient(timeout=25) as client:
        for i, q in enumerate(queries):
            print(f"\n  Query {i+1}/{len(queries)}: {q['name']}")
            print("  " + "-" * 56)

            # Build params
            es_params = {
                "chainid": 1,
                "module": q["module"],
                "action": q["action"],
                "apikey": api_key,
                **q.get("extra", {}),
            }
            bs_params = {
                "chain_id": 1,
                "module": q["module"],
                "action": q["action"],
                **q.get("extra", {}),
            }
            if bs_key:
                bs_params["apikey"] = bs_key

            try:
                # Make both calls
                es_data, es_ms = await _query(client, ETHERSCAN_V2_BASE, es_params)
                await asyncio.sleep(0.25)  # Respect Blockscout rate limit
                bs_data, bs_ms = await _query(client, BLOCKSCOUT_BASE, bs_params)
                await asyncio.sleep(0.25)

                result = _compare_results(q["name"], es_data, bs_data, es_ms, bs_ms)
                results.append(result)

                status = result["status"]
                if status == "exact":
                    # Print comparison for specific types
                    es_result = es_data.get("result", "")
                    if q["action"] == "tokenholdercount":
                        print(f"  Etherscan={es_result} Blockscout={bs_data.get('result', '')} "
                              f"({es_ms}ms vs {bs_ms}ms)")
                    elif q["action"] == "getabi":
                        es_len = len(es_result) if isinstance(es_result, str) else 0
                        print(f"  Etherscan ABI: {es_len} chars, Blockscout ABI: "
                              f"{len(bs_data.get('result', '')) if isinstance(bs_data.get('result'), str) else 0} chars "
                              f"({es_ms}ms vs {bs_ms}ms)")
                    elif q["action"] == "tokenholderlist":
                        es_count = len(es_result) if isinstance(es_result, list) else "?"
                        bs_count = len(bs_data.get("result", [])) if isinstance(bs_data.get("result"), list) else "?"
                        print(f"  Etherscan={es_count} results, Blockscout={bs_count} results "
                              f"({es_ms}ms vs {bs_ms}ms)")
                    else:
                        print(f"  ({es_ms}ms vs {bs_ms}ms)")
                    print(f"  \u2713 EXACT MATCH")
                    exact += 1

                elif status == "close":
                    detail = ""
                    if "es_val" in result:
                        diff = result["diff"]
                        print(f"  Etherscan={result['es_val']:,} Blockscout={result['bs_val']:,} "
                              f"(off by {diff})")
                    elif "es_len" in result:
                        print(f"  Etherscan={result['es_len']} results, "
                              f"Blockscout={result['bs_len']} results")
                    print(f"  ~ CLOSE MATCH ({es_ms}ms vs {bs_ms}ms)")
                    close += 1

                elif status == "mismatch":
                    print(f"  \u2717 MISMATCH ({es_ms}ms vs {bs_ms}ms)")
                    mismatch += 1

                else:
                    print(f"  ERROR: {status} — {result.get('detail', '')}")
                    errors += 1

            except Exception as e:
                print(f"  ERROR: {e}")
                results.append({"name": q["name"], "status": "error", "detail": str(e)})
                errors += 1

    total = len(results)

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for r in results:
        name = r["name"]
        status = r["status"]
        if status == "exact":
            print(f"  {name}: \u2713 EXACT MATCH")
        elif status == "close":
            detail = ""
            if "diff" in r:
                detail = f" (off by {r['diff']})"
            elif "es_len" in r:
                detail = f" ({r['es_len']} vs {r['bs_len']} results)"
            print(f"  {name}: ~ CLOSE MATCH{detail}")
        elif status == "mismatch":
            print(f"  {name}: \u2717 MISMATCH")
        else:
            print(f"  {name}: ERROR ({r.get('detail', '')})")

    print(f"\n{total} queries. {exact} exact match. {close} close match. "
          f"{mismatch} mismatches. {errors} errors.")

    if exact + close > 0:
        parity = (exact + close) / max(1, exact + close + mismatch) * 100
        print(f"Data parity: {parity:.0f}%", end="")
        if parity >= 95:
            print(" — READY for migration")
        else:
            print(" — INVESTIGATE mismatches before migration")

    return mismatch == 0 and errors == 0


if __name__ == "__main__":
    _check_env()
    print("Sprint 5 Verification: Blockscout Migration Evaluation")
    print("=" * 60)
    ok = asyncio.run(run_tests())
    sys.exit(0 if ok else 1)
