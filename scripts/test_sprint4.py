#!/usr/bin/env python3
"""
Sprint 4 Verification: Parallel Web Research Components
=========================================================
Runs Parallel.ai Task API research on 2-3 known entities per function
and prints structured results.

Requirements:
  - PARALLEL_API_KEY env var

Usage:
  python scripts/test_sprint4.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _check_env():
    if not os.environ.get("PARALLEL_API_KEY"):
        print("ERROR: Missing env var: PARALLEL_API_KEY")
        print("Set it and re-run.")
        sys.exit(1)


async def run_tests():
    from app.collectors.web_research import (
        research_bridge_audits,
        research_por_frequency,
        research_compensation_transparency,
        research_meeting_cadence,
        research_por_method,
    )

    passed = 0
    failed = 0
    results = []

    # ===================================================================
    # Test 1: Bridge audits
    # ===================================================================
    test_bridges = ["Wormhole", "LayerZero"]
    print("\n" + "=" * 60)
    print("TEST 1: research_bridge_audits")
    print("=" * 60)

    for name in test_bridges:
        print(f"\n  Bridge: {name}")
        print("  " + "-" * 56)
        try:
            result = await research_bridge_audits(name)
            if result and result.get("audit_count") is not None:
                count = result["audit_count"]
                audits = result.get("audits", [])
                score = result["score"]
                auditor_names = [a.get("auditor", "?") for a in audits[:5]]
                if auditor_names:
                    print(f"  {name} audits found: {count} ({', '.join(auditor_names)})")
                else:
                    print(f"  {name} audits found: {count}")
                print(f"  Score: {score:.0f}/100")
                print("  PASSED")
                passed += 1
                results.append((f"audit/{name}", True, None))
            else:
                print(f"  Result: {result}")
                print("  FAILED — no data returned")
                failed += 1
                results.append((f"audit/{name}", False, "no data"))
        except Exception as e:
            print(f"  ERROR: {e}")
            failed += 1
            results.append((f"audit/{name}", False, str(e)))

    # ===================================================================
    # Test 2: PoR frequency
    # ===================================================================
    test_exchanges_freq = ["Binance", "Coinbase"]
    print("\n" + "=" * 60)
    print("TEST 2: research_por_frequency")
    print("=" * 60)

    for name in test_exchanges_freq:
        print(f"\n  Exchange: {name}")
        print("  " + "-" * 56)
        try:
            result = await research_por_frequency(name)
            if result and result.get("score") is not None:
                last = result.get("last_published", "unknown")
                freq = result.get("frequency_days", 0)
                score = result["score"]
                freq_str = f"every {freq} days" if freq > 0 else "unknown frequency"
                print(f"  {name} PoR last published: {last}, frequency: {freq_str}")
                print(f"  Score: {score:.0f}/100")
                print("  PASSED")
                passed += 1
                results.append((f"por_freq/{name}", True, None))
            else:
                print(f"  Result: {result}")
                print("  FAILED — no data returned")
                failed += 1
                results.append((f"por_freq/{name}", False, "no data"))
        except Exception as e:
            print(f"  ERROR: {e}")
            failed += 1
            results.append((f"por_freq/{name}", False, str(e)))

    # ===================================================================
    # Test 3: Compensation transparency
    # ===================================================================
    test_protocols_comp = ["Aave", "Uniswap"]
    print("\n" + "=" * 60)
    print("TEST 3: research_compensation_transparency")
    print("=" * 60)

    for name in test_protocols_comp:
        print(f"\n  Protocol: {name}")
        print("  " + "-" * 56)
        try:
            result = await research_compensation_transparency(name)
            if result and result.get("score") is not None:
                posts = result.get("post_count", 0)
                recent = result.get("most_recent_date", "unknown")
                score = result["score"]
                print(f"  {name} compensation posts (12mo): {posts}, most recent: {recent}")
                print(f"  Score: {score:.0f}/100")
                print("  PASSED")
                passed += 1
                results.append((f"comp/{name}", True, None))
            else:
                print(f"  Result: {result}")
                print("  FAILED — no data returned")
                failed += 1
                results.append((f"comp/{name}", False, "no data"))
        except Exception as e:
            print(f"  ERROR: {e}")
            failed += 1
            results.append((f"comp/{name}", False, str(e)))

    # ===================================================================
    # Test 4: Meeting cadence
    # ===================================================================
    test_protocols_mtg = ["Aave", "Compound"]
    print("\n" + "=" * 60)
    print("TEST 4: research_meeting_cadence")
    print("=" * 60)

    for name in test_protocols_mtg:
        print(f"\n  Protocol: {name}")
        print("  " + "-" * 56)
        try:
            result = await research_meeting_cadence(name)
            if result and result.get("score") is not None:
                has = result.get("has_regular_meetings", False)
                freq = result.get("frequency", "none")
                recent = result.get("most_recent_date", "unknown")
                score = result["score"]
                status = f"{freq} meetings" if has else "no regular meetings found"
                print(f"  {name}: {status}, last: {recent}")
                print(f"  Score: {score:.0f}/100")
                print("  PASSED")
                passed += 1
                results.append((f"meeting/{name}", True, None))
            else:
                print(f"  Result: {result}")
                print("  FAILED — no data returned")
                failed += 1
                results.append((f"meeting/{name}", False, "no data"))
        except Exception as e:
            print(f"  ERROR: {e}")
            failed += 1
            results.append((f"meeting/{name}", False, str(e)))

    # ===================================================================
    # Test 5: PoR method
    # ===================================================================
    test_exchanges_method = ["OKX", "Kraken"]
    print("\n" + "=" * 60)
    print("TEST 5: research_por_method")
    print("=" * 60)

    for name in test_exchanges_method:
        print(f"\n  Exchange: {name}")
        print("  " + "-" * 56)
        try:
            result = await research_por_method(name)
            if result and result.get("score") is not None:
                method = result.get("method_type", "unknown")
                merkle = result.get("uses_merkle_tree", False)
                zk = result.get("uses_zk_proof", False)
                auditor = result.get("third_party_auditor", "none")
                score = result["score"]

                features = []
                if merkle:
                    features.append("Merkle tree")
                if zk:
                    features.append("zk-proof")
                if auditor and auditor != "none":
                    features.append(f"auditor: {auditor}")
                feature_str = ", ".join(features) if features else "unknown method"

                print(f"  {name} PoR method: {method} ({feature_str})")
                print(f"  Score: {score:.0f}/100")
                print("  PASSED")
                passed += 1
                results.append((f"por_method/{name}", True, None))
            else:
                print(f"  Result: {result}")
                print("  FAILED — no data returned")
                failed += 1
                results.append((f"por_method/{name}", False, "no data"))
        except Exception as e:
            print(f"  ERROR: {e}")
            failed += 1
            results.append((f"por_method/{name}", False, str(e)))

    total = len(results)

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for name, ok, err in results:
        status = "PASSED" if ok else f"FAILED ({err})"
        print(f"  {name}: {status}")
    print(f"\n{total} research tasks run. {passed} passed. {failed} failed.", end="")
    if failed > 0:
        failures = [name for name, ok, _ in results if not ok]
        print(f" ({', '.join(failures)})")
    else:
        print()

    return failed == 0


if __name__ == "__main__":
    _check_env()
    print("Sprint 4 Verification: Parallel Web Research Components")
    print("=" * 60)
    ok = asyncio.run(run_tests())
    sys.exit(0 if ok else 1)
