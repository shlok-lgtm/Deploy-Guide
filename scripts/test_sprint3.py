#!/usr/bin/env python3
"""
Sprint 3 Verification: Bridge + Exchange Monitoring
=====================================================
Tests bridge message stats and exchange API health checks.

Requirements:
  - Internet access (public APIs, no keys needed)

Usage:
  python scripts/test_sprint3.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _fmt_pct(val):
    return f"{val:.1f}%"


def run_tests():
    from app.collectors.bridge_monitors import (
        get_message_stats,
        normalize_message_success_rate,
    )
    from app.collectors.exchange_health import (
        check_exchange,
        EXCHANGE_HEALTH_ENDPOINTS,
        normalize_api_availability,
    )

    passed = 0
    failed = 0
    results = []

    # ===================================================================
    # Part A: Bridge monitoring
    # ===================================================================
    test_bridges = ["wormhole", "axelar", "layerzero", "stargate", "across"]

    print("\n" + "=" * 60)
    print("PART A: Bridge Message Stats")
    print("=" * 60)

    for slug in test_bridges:
        print(f"\n  Bridge: {slug}")
        print("  " + "-" * 56)
        start = time.monotonic()
        try:
            stats = get_message_stats(slug)
            elapsed = time.monotonic() - start
            if stats and stats.get("success_rate") is not None:
                total = stats.get("total_messages", 0)
                successful = stats.get("successful", 0)
                fail_count = stats.get("failed", 0)
                rate = stats["success_rate"]
                score = normalize_message_success_rate(rate)

                if total > 0:
                    print(f"  {slug.title()} 24h: {total:,} messages, "
                          f"{successful:,} successful, {_fmt_pct(rate)} success rate")
                else:
                    # DeFiLlama volume proxy
                    print(f"  {slug.title()} 24h: operational (volume proxy), "
                          f"estimated {_fmt_pct(rate)} uptime")
                print(f"  Normalized score: {score:.0f}/100")
                print(f"  Time: {elapsed:.1f}s")
                print("  PASSED")
                passed += 1
                results.append((f"bridge/{slug}", True, None))
            else:
                print(f"  No data returned ({elapsed:.1f}s)")
                print("  FAILED — empty result")
                failed += 1
                results.append((f"bridge/{slug}", False, "no data"))
        except Exception as e:
            elapsed = time.monotonic() - start
            print(f"  ERROR: {e} ({elapsed:.1f}s)")
            failed += 1
            results.append((f"bridge/{slug}", False, str(e)))

    # ===================================================================
    # Part B: Exchange health checks
    # ===================================================================
    print("\n" + "=" * 60)
    print("PART B: Exchange API Health")
    print("=" * 60)

    for slug, endpoint in EXCHANGE_HEALTH_ENDPOINTS.items():
        print(f"\n  Exchange: {slug}")
        print("  " + "-" * 56)
        start = time.monotonic()
        try:
            result = check_exchange(slug, endpoint)
            elapsed = time.monotonic() - start
            status_code = result.get("status_code", 0)
            response_ms = result.get("response_time_ms", 0)
            healthy = result.get("is_healthy", False)
            error = result.get("error")

            if healthy:
                print(f"  {slug.title()} API: {status_code} OK, {response_ms}ms")
                print("  PASSED")
                passed += 1
                results.append((f"exchange/{slug}", True, None))
            elif error:
                print(f"  {slug.title()} API: {error} ({response_ms}ms)")
                print("  FAILED — not healthy")
                failed += 1
                results.append((f"exchange/{slug}", False, error))
            else:
                print(f"  {slug.title()} API: HTTP {status_code}, {response_ms}ms")
                print("  FAILED — non-2xx status")
                failed += 1
                results.append((f"exchange/{slug}", False, f"HTTP {status_code}"))
        except Exception as e:
            elapsed = time.monotonic() - start
            print(f"  ERROR: {e} ({elapsed:.1f}s)")
            failed += 1
            results.append((f"exchange/{slug}", False, str(e)))

        time.sleep(0.2)

    total = len(results)

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    bridge_results = [(n, o, e) for n, o, e in results if n.startswith("bridge/")]
    exchange_results = [(n, o, e) for n, o, e in results if n.startswith("exchange/")]

    print("\n  Bridges:")
    for name, ok, err in bridge_results:
        status = "PASSED" if ok else f"FAILED ({err})"
        print(f"    {name}: {status}")

    print("\n  Exchanges:")
    for name, ok, err in exchange_results:
        status = "PASSED" if ok else f"FAILED ({err})"
        print(f"    {name}: {status}")

    print(f"\n{total} checks run. {passed} passed. {failed} failed.", end="")
    if failed > 0:
        failures = [name for name, ok, _ in results if not ok]
        print(f" ({', '.join(failures)})")
    else:
        print()

    return failed == 0


if __name__ == "__main__":
    print("Sprint 3 Verification: Bridge + Exchange Monitoring")
    print("=" * 60)
    ok = run_tests()
    sys.exit(0 if ok else 1)
