#!/usr/bin/env python3
"""
Sprint 2 Verification: CoinGecko DEX API Integration
======================================================
Self-contained test — no app imports, uses only requests + stdlib.

Tests DEX pool data on 3 protocols: Aave, Uniswap, Compound.

Requirements:
  - COINGECKO_API_KEY env var (Pro/Analyst tier)

Usage:
  python scripts/test_sprint2.py
"""

import os
import sys
import time
import requests

COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
ONCHAIN_BASE = "https://pro-api.coingecko.com/api/v3/onchain"


def _check_env():
    if not COINGECKO_API_KEY:
        print("ERROR: Missing env var: COINGECKO_API_KEY")
        print("Set it and re-run. Needs Pro/Analyst tier for on-chain endpoints.")
        sys.exit(1)


def _headers():
    return {"Accept": "application/json", "x-cg-pro-api-key": COINGECKO_API_KEY}


def _fmt_usd(val):
    if val >= 1e9:
        return f"${val / 1e9:.2f}B"
    if val >= 1e6:
        return f"${val / 1e6:.1f}M"
    if val >= 1e3:
        return f"${val / 1e3:.0f}K"
    return f"${val:.0f}"


PROTOCOL_MAP = {
    "aave": {"network": "eth", "search": "aave"},
    "uniswap": {"network": "eth", "search": "uniswap v3"},
    "compound-finance": {"network": "eth", "search": "compound"},
}


def search_pools(search_term, network="eth"):
    """Search for pools via GeckoTerminal."""
    resp = requests.get(
        f"{ONCHAIN_BASE}/search/pools",
        params={"query": search_term, "network": network},
        headers=_headers(),
        timeout=15,
    )
    time.sleep(0.5)
    if resp.status_code != 200:
        return None, f"HTTP {resp.status_code}"
    data = resp.json()
    pools = []
    for p in data.get("data", [])[:5]:
        attrs = p.get("attributes", {})
        pools.append({
            "name": attrs.get("name", "unnamed"),
            "address": attrs.get("address", ""),
            "tvl": float(attrs.get("reserve_in_usd", 0) or 0),
            "volume_24h": float(attrs.get("volume_usd", {}).get("h24", 0) or 0),
        })
    return pools, None


def get_ohlcv(network, pool_address, days=30):
    """Get OHLCV for a pool."""
    resp = requests.get(
        f"{ONCHAIN_BASE}/networks/{network}/pools/{pool_address}/ohlcv/day",
        params={"aggregate": 1, "limit": days},
        headers=_headers(),
        timeout=15,
    )
    time.sleep(0.5)
    if resp.status_code != 200:
        return None, f"HTTP {resp.status_code}"
    data = resp.json()
    ohlcv = data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
    return ohlcv, None


def run_tests():
    passed = 0
    failed = 0
    total = 0
    results = []

    for slug, cfg in PROTOCOL_MAP.items():
        name = slug.replace("-", " ").title()
        network = cfg["network"]
        search = cfg["search"]

        print("\n" + "=" * 60)
        print(f"PROTOCOL: {name}")
        print("=" * 60)

        # --- Pool discovery ---
        total += 1
        print(f"\n  Test A: Pool discovery ({slug})")
        print("  " + "-" * 56)
        t = time.monotonic()
        pools, err = search_pools(search, network)
        elapsed = time.monotonic() - t

        if err:
            print(f"  ERROR: {err} ({elapsed:.1f}s)")
            failed += 1
            results.append((f"{slug}/pools", False, err))
            continue
        if not pools:
            print(f"  No pools found ({elapsed:.1f}s)")
            failed += 1
            results.append((f"{slug}/pools", False, "empty"))
            continue

        print(f"  Found {len(pools)} pools in {elapsed:.1f}s:")
        for i, p in enumerate(pools[:3]):
            print(f"    {i+1}. {p['name']}: TVL {_fmt_usd(p['tvl'])}, 24h volume {_fmt_usd(p['volume_24h'])}")
        print("  PASSED")
        passed += 1
        results.append((f"{slug}/pools", True, None))

        # --- OHLCV trend ---
        total += 1
        print(f"\n  Test B: OHLCV 30d trend ({slug})")
        print("  " + "-" * 56)
        biggest = max(pools, key=lambda p: p["tvl"])
        if not biggest.get("address"):
            print("  SKIPPED — no pool address")
            failed += 1
            results.append((f"{slug}/ohlcv", False, "no address"))
            continue

        t = time.monotonic()
        ohlcv, err = get_ohlcv(network, biggest["address"])
        elapsed = time.monotonic() - t

        if err:
            print(f"  ERROR: {err} ({elapsed:.1f}s)")
            failed += 1
            results.append((f"{slug}/ohlcv", False, err))
            continue

        if ohlcv and len(ohlcv) >= 7:
            recent = [float(c[5]) for c in ohlcv[:7] if len(c) > 5]
            older = [float(c[5]) for c in ohlcv[-7:] if len(c) > 5]
            if recent and older:
                recent_avg = sum(recent) / len(recent)
                older_avg = sum(older) / len(older)
                trend = ((recent_avg - older_avg) / older_avg * 100) if older_avg else 0
                total_tvl = sum(p["tvl"] for p in pools)
                total_vol = sum(p["volume_24h"] for p in pools)
                print(f"  {name} pools: TVL {_fmt_usd(total_tvl)}, 24h volume {_fmt_usd(total_vol)}, 30d trend: {trend:+.1f}%")
                print(f"  OHLCV data points: {len(ohlcv)}")
                print(f"  Time: {elapsed:.1f}s")
                print("  PASSED")
                passed += 1
                results.append((f"{slug}/ohlcv", True, None))
            else:
                print(f"  Not enough volume data points ({elapsed:.1f}s)")
                failed += 1
                results.append((f"{slug}/ohlcv", False, "insufficient data"))
        else:
            data_len = len(ohlcv) if ohlcv else 0
            print(f"  Only {data_len} OHLCV points returned ({elapsed:.1f}s)")
            failed += 1
            results.append((f"{slug}/ohlcv", False, f"only {data_len} points"))

    # --- Summary ---
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for n, ok, err in results:
        status = "PASSED" if ok else f"FAILED ({err})"
        print(f"  {n}: {status}")
    print(f"\n{total} tests run. {passed} passed. {failed} failed.", end="")
    if failed > 0:
        failures = [n for n, ok, _ in results if not ok]
        print(f" ({', '.join(failures)})")
    else:
        print()

    return failed == 0


if __name__ == "__main__":
    _check_env()
    print("Sprint 2 Verification: CoinGecko DEX API Integration")
    print("=" * 60)
    ok = run_tests()
    sys.exit(0 if ok else 1)
