#!/usr/bin/env python3
"""
Sprint 3 Verification: Bridge + Exchange Monitoring
=====================================================
Self-contained test — no app imports, uses only requests + stdlib.
All endpoints are public, no API keys needed.

Usage:
  python scripts/test_sprint3.py
"""

import sys
import time
import requests


# ===================================================================
# Bridge adapters
# ===================================================================

def check_wormhole():
    """Wormholescan API."""
    try:
        resp = requests.get(
            "https://api.wormholescan.io/api/v1/transactions",
            params={"page": 0, "pageSize": 100, "sortOrder": "DESC"},
            timeout=15,
        )
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"
        data = resp.json()
        txs = data.get("transactions", [])
        if not txs:
            return None, "no transactions in response"
        total = len(txs)
        ok = sum(1 for tx in txs if tx.get("status") == "completed")
        return {"total": total, "successful": ok, "failed": total - ok,
                "rate": round(ok / total * 100, 2) if total else 0}, None
    except Exception as e:
        return None, str(e)


def check_axelar():
    """Axelarscan API."""
    try:
        resp = requests.get(
            "https://api.axelarscan.io/gmp/searchGMP",
            params={"size": 100, "sort": "desc"},
            timeout=15,
        )
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"
        data = resp.json()
        msgs = data.get("data", [])
        if not msgs:
            return None, "no messages in response"
        total = len(msgs)
        ok = sum(1 for m in msgs if m.get("status") in ("executed", "approved", "confirmed"))
        return {"total": total, "successful": ok, "failed": total - ok,
                "rate": round(ok / total * 100, 2) if total else 0}, None
    except Exception as e:
        return None, str(e)


def check_defillama_bridge(slug, bridge_id):
    """DeFiLlama bridge volume as uptime proxy."""
    try:
        resp = requests.get(f"https://bridges.llama.fi/bridge/{bridge_id}", timeout=15)
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"
        data = resp.json()
        vol = data.get("currentDayVolume", {})
        deposit = float(vol.get("depositUSD", 0) or 0)
        withdraw = float(vol.get("withdrawUSD", 0) or 0)
        if deposit > 0 or withdraw > 0:
            return {"operational": True, "deposit_usd": deposit, "withdraw_usd": withdraw}, None
        return None, "zero volume"
    except Exception as e:
        return None, str(e)


# ===================================================================
# Exchange health
# ===================================================================

EXCHANGE_ENDPOINTS = {
    "binance": "https://api.binance.com/api/v3/ping",
    "coinbase": "https://api.exchange.coinbase.com/time",
    "kraken": "https://api.kraken.com/0/public/SystemStatus",
    "okx": "https://www.okx.com/api/v5/public/time",
    "bybit": "https://api.bybit.com/v5/market/time",
    "kucoin": "https://api.kucoin.com/api/v1/timestamp",
    "gate-io": "https://api.gateio.ws/api/v4/spot/time",
    "bitget": "https://api.bitget.com/api/v2/public/time",
}


def check_exchange(slug, url):
    """HTTP ping an exchange."""
    t = time.monotonic()
    try:
        resp = requests.get(url, timeout=5)
        ms = int((time.monotonic() - t) * 1000)
        return {"code": resp.status_code, "ms": ms, "healthy": 200 <= resp.status_code < 300}, None
    except requests.exceptions.Timeout:
        return {"code": 0, "ms": 5000, "healthy": False}, "timeout"
    except Exception as e:
        return {"code": 0, "ms": 0, "healthy": False}, str(e)


# ===================================================================
# Main
# ===================================================================

def run_tests():
    passed = 0
    failed = 0
    results = []

    # --- Part A: Bridges ---
    print("\n" + "=" * 60)
    print("PART A: Bridge Message Stats")
    print("=" * 60)

    # Wormhole
    print("\n  Bridge: Wormhole")
    print("  " + "-" * 56)
    t = time.monotonic()
    data, err = check_wormhole()
    elapsed = time.monotonic() - t
    if data:
        print(f"  Wormhole 24h: {data['total']:,} messages, {data['successful']:,} successful, {data['rate']}% success rate")
        print(f"  Time: {elapsed:.1f}s")
        print("  PASSED")
        passed += 1
        results.append(("bridge/wormhole", True, None))
    else:
        print(f"  ERROR: {err} ({elapsed:.1f}s)")
        failed += 1
        results.append(("bridge/wormhole", False, err))
    time.sleep(0.3)

    # Axelar
    print("\n  Bridge: Axelar")
    print("  " + "-" * 56)
    t = time.monotonic()
    data, err = check_axelar()
    elapsed = time.monotonic() - t
    if data:
        print(f"  Axelar 24h: {data['total']:,} messages, {data['successful']:,} successful, {data['rate']}% success rate")
        print(f"  Time: {elapsed:.1f}s")
        print("  PASSED")
        passed += 1
        results.append(("bridge/axelar", True, None))
    else:
        print(f"  ERROR: {err} ({elapsed:.1f}s)")
        failed += 1
        results.append(("bridge/axelar", False, err))
    time.sleep(0.3)

    # LayerZero via DeFiLlama
    llama_bridges = [
        ("layerzero", 14),
        ("stargate", 5),
        ("across", 10),
    ]
    for slug, bid in llama_bridges:
        print(f"\n  Bridge: {slug} (DeFiLlama volume proxy)")
        print("  " + "-" * 56)
        t = time.monotonic()
        data, err = check_defillama_bridge(slug, bid)
        elapsed = time.monotonic() - t
        if data:
            dep = data.get("deposit_usd", 0)
            wd = data.get("withdraw_usd", 0)
            dep_str = f"${dep/1e6:.1f}M" if dep >= 1e6 else f"${dep:,.0f}"
            wd_str = f"${wd/1e6:.1f}M" if wd >= 1e6 else f"${wd:,.0f}"
            print(f"  {slug.title()}: operational, today's deposits {dep_str}, withdrawals {wd_str}")
            print(f"  Time: {elapsed:.1f}s")
            print("  PASSED")
            passed += 1
            results.append((f"bridge/{slug}", True, None))
        else:
            print(f"  ERROR: {err} ({elapsed:.1f}s)")
            failed += 1
            results.append((f"bridge/{slug}", False, err))
        time.sleep(0.3)

    # --- Part B: Exchanges ---
    print("\n" + "=" * 60)
    print("PART B: Exchange API Health")
    print("=" * 60)

    for slug, url in EXCHANGE_ENDPOINTS.items():
        print(f"\n  Exchange: {slug}")
        print("  " + "-" * 56)
        data, err = check_exchange(slug, url)
        if data and data["healthy"]:
            print(f"  {slug.title()} API: {data['code']} OK, {data['ms']}ms")
            print("  PASSED")
            passed += 1
            results.append((f"exchange/{slug}", True, None))
        elif err:
            print(f"  {slug.title()} API: {err} ({data.get('ms', 0)}ms)")
            print("  FAILED")
            failed += 1
            results.append((f"exchange/{slug}", False, err))
        else:
            print(f"  {slug.title()} API: HTTP {data.get('code', '?')}, {data.get('ms', 0)}ms")
            print("  FAILED — non-2xx status")
            failed += 1
            results.append((f"exchange/{slug}", False, f"HTTP {data.get('code')}"))
        time.sleep(0.2)

    total = len(results)

    # --- Summary ---
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    print("\n  Bridges:")
    for n, ok, err in results:
        if n.startswith("bridge/"):
            print(f"    {n}: {'PASSED' if ok else f'FAILED ({err})'}")
    print("\n  Exchanges:")
    for n, ok, err in results:
        if n.startswith("exchange/"):
            print(f"    {n}: {'PASSED' if ok else f'FAILED ({err})'}")

    print(f"\n{total} checks run. {passed} passed. {failed} failed.", end="")
    if failed > 0:
        failures = [n for n, ok, _ in results if not ok]
        print(f" ({', '.join(failures)})")
    else:
        print()

    return failed == 0


if __name__ == "__main__":
    print("Sprint 3 Verification: Bridge + Exchange Monitoring")
    print("=" * 60)
    ok = run_tests()
    sys.exit(0 if ok else 1)
