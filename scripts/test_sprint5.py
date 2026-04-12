#!/usr/bin/env python3
"""
Sprint 5 Verification: Blockscout Migration Evaluation
========================================================
Self-contained test — no app imports, uses only requests + stdlib.

Runs 10 comparison queries between Etherscan V2 and Blockscout.

Requirements:
  - ETHERSCAN_API_KEY env var

Usage:
  python scripts/test_sprint5.py
"""

import hashlib
import json
import os
import sys
import time
import requests

ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
BLOCKSCOUT_API_KEY = os.environ.get("BLOCKSCOUT_API_KEY", "")

ES_BASE = "https://api.etherscan.io/v2/api"
BS_BASE = "https://api.blockscout.com/v2/api"

# Well-known contracts
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
DAI = "0x6b175474e89094c44da98b954eedeac495271d0f"
AAVE_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"


def _check_env():
    if not ETHERSCAN_API_KEY:
        print("ERROR: Missing env var: ETHERSCAN_API_KEY")
        print("Set it and re-run.")
        sys.exit(1)


def _query(base, params, timeout=20):
    t = time.monotonic()
    resp = requests.get(base, params=params, timeout=timeout)
    ms = int((time.monotonic() - t) * 1000)
    return resp.json(), ms


def _hash(data):
    clean = {k: v for k, v in data.items() if not k.startswith("_")}
    return hashlib.sha256(
        json.dumps(clean, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()[:12]


def run_tests():
    queries = [
        {"name": "tokenholdercount USDC", "module": "token", "action": "tokenholdercount", "extra": {"contractaddress": USDC}},
        {"name": "tokenholdercount USDT", "module": "token", "action": "tokenholdercount", "extra": {"contractaddress": USDT}},
        {"name": "tokenholdercount DAI",  "module": "token", "action": "tokenholdercount", "extra": {"contractaddress": DAI}},
        {"name": "getabi USDC",           "module": "contract", "action": "getabi", "extra": {"address": USDC}},
        {"name": "getabi Aave Pool",      "module": "contract", "action": "getabi", "extra": {"address": AAVE_POOL}},
        {"name": "getsourcecode USDC",    "module": "contract", "action": "getsourcecode", "extra": {"address": USDC}},
        {"name": "tokenholderlist USDC (top 10)", "module": "token", "action": "tokenholderlist", "extra": {"contractaddress": USDC, "page": 1, "offset": 10}},
        {"name": "tokenholderlist USDT (top 10)", "module": "token", "action": "tokenholderlist", "extra": {"contractaddress": USDT, "page": 1, "offset": 10}},
        {"name": "tokenholdercount WETH", "module": "token", "action": "tokenholdercount", "extra": {"contractaddress": WETH}},
        {"name": "getabi WETH",           "module": "contract", "action": "getabi", "extra": {"address": WETH}},
    ]

    exact = 0
    close = 0
    mismatch = 0
    errors = 0
    results = []

    for i, q in enumerate(queries):
        print(f"\n  Query {i+1}/{len(queries)}: {q['name']}")
        print("  " + "-" * 56)

        es_params = {"chainid": 1, "module": q["module"], "action": q["action"], "apikey": ETHERSCAN_API_KEY, **q.get("extra", {})}
        bs_params = {"chain_id": 1, "module": q["module"], "action": q["action"], **q.get("extra", {})}
        if BLOCKSCOUT_API_KEY:
            bs_params["apikey"] = BLOCKSCOUT_API_KEY

        try:
            es_data, es_ms = _query(ES_BASE, es_params)
            time.sleep(0.3)
            bs_data, bs_ms = _query(BS_BASE, bs_params)
            time.sleep(0.3)

            es_ok = es_data.get("status") == "1" or isinstance(es_data.get("result"), list)
            bs_ok = bs_data.get("status") == "1" or isinstance(bs_data.get("result"), list)

            if not es_ok:
                print(f"  Etherscan error: {es_data.get('message', '?')}")
                errors += 1
                results.append({"name": q["name"], "status": "etherscan_error"})
                continue
            if not bs_ok:
                print(f"  Blockscout error: {bs_data.get('message', '?')}")
                errors += 1
                results.append({"name": q["name"], "status": "blockscout_error"})
                continue

            es_hash = _hash(es_data)
            bs_hash = _hash(bs_data)
            es_result = es_data.get("result", "")
            bs_result = bs_data.get("result", "")

            if es_hash == bs_hash:
                # Exact match
                if q["action"] == "tokenholdercount":
                    print(f"  Etherscan={es_result} Blockscout={bs_result} ({es_ms}ms vs {bs_ms}ms)")
                elif q["action"] == "getabi":
                    es_len = len(es_result) if isinstance(es_result, str) else 0
                    bs_len = len(bs_result) if isinstance(bs_result, str) else 0
                    print(f"  Etherscan ABI: {es_len} chars, Blockscout ABI: {bs_len} chars ({es_ms}ms vs {bs_ms}ms)")
                elif q["action"] == "tokenholderlist":
                    es_cnt = len(es_result) if isinstance(es_result, list) else "?"
                    bs_cnt = len(bs_result) if isinstance(bs_result, list) else "?"
                    print(f"  Etherscan={es_cnt} results, Blockscout={bs_cnt} results ({es_ms}ms vs {bs_ms}ms)")
                else:
                    print(f"  ({es_ms}ms vs {bs_ms}ms)")
                print(f"  \u2713 EXACT MATCH")
                exact += 1
                results.append({"name": q["name"], "status": "exact"})

            else:
                # Check close match
                is_close = False
                detail = ""

                # Numeric close match
                if isinstance(es_result, str) and isinstance(bs_result, str):
                    try:
                        es_val = int(es_result)
                        bs_val = int(bs_result)
                        diff = abs(es_val - bs_val)
                        if es_val > 0 and diff / es_val < 0.01:
                            is_close = True
                            detail = f"off by {diff}"
                            print(f"  Etherscan={es_val:,} Blockscout={bs_val:,} ({detail})")
                    except (ValueError, TypeError):
                        pass

                # List length close match
                if not is_close and isinstance(es_result, list) and isinstance(bs_result, list):
                    es_len = len(es_result)
                    bs_len = len(bs_result)
                    if es_len > 0 and abs(es_len - bs_len) <= max(1, int(es_len * 0.01)):
                        is_close = True
                        detail = f"{es_len} vs {bs_len} results"
                        print(f"  Etherscan={es_len} results, Blockscout={bs_len} results")

                if is_close:
                    print(f"  ~ CLOSE MATCH ({es_ms}ms vs {bs_ms}ms)")
                    close += 1
                    results.append({"name": q["name"], "status": "close", "detail": detail})
                else:
                    if q["action"] == "tokenholdercount":
                        print(f"  Etherscan={es_result} Blockscout={bs_result}")
                    print(f"  \u2717 MISMATCH ({es_ms}ms vs {bs_ms}ms)")
                    mismatch += 1
                    results.append({"name": q["name"], "status": "mismatch"})

        except Exception as e:
            print(f"  ERROR: {e}")
            errors += 1
            results.append({"name": q["name"], "status": "error", "detail": str(e)})

    total = len(results)

    # --- Summary ---
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for r in results:
        name = r["name"]
        s = r["status"]
        if s == "exact":
            print(f"  {name}: \u2713 EXACT MATCH")
        elif s == "close":
            print(f"  {name}: ~ CLOSE MATCH ({r.get('detail', '')})")
        elif s == "mismatch":
            print(f"  {name}: \u2717 MISMATCH")
        else:
            print(f"  {name}: ERROR ({r.get('detail', s)})")

    print(f"\n{total} queries. {exact} exact match. {close} close match. {mismatch} mismatches. {errors} errors.")
    if exact + close + mismatch > 0:
        parity = (exact + close) / (exact + close + mismatch) * 100
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
    ok = run_tests()
    sys.exit(0 if ok else 1)
