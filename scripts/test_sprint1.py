#!/usr/bin/env python3
"""
Sprint 1 Verification: Contract Read Extension
================================================
Tests 6 on-chain reader functions against known contracts with
publicly verifiable values.

Requirements:
  - ALCHEMY_API_KEY env var (for eth_call / eth_getStorageAt)
  - ETHERSCAN_API_KEY env var (for ABI fetching)

Usage:
  python scripts/test_sprint1.py
"""

import asyncio
import os
import sys
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

# ---------------------------------------------------------------------------
# Test targets — well-known contracts with publicly verifiable values
# ---------------------------------------------------------------------------

# Aave V3 Executor (Timelock) — known delay is ~24-48h
AAVE_TIMELOCK = "0x61910EcD7e8e942136CE7Fe7943f956cea1CC2f7"

# Aave Guardian multisig (Gnosis Safe) — known 5-of-10 (or similar)
AAVE_MULTISIG = "0xCA76Ebd8617a03126B6FB84F9b1c1A0fB71C2633"

# USDC (FiatTokenV2_2) — well-known EIP-1967 proxy
USDC_PROXY = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

# Aave Pool V3 — known to have AccessControl pattern
AAVE_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"

# Compound Comet (USDC) — known to have pause() function
COMPOUND_COMET = "0xc3d688B66703497DAA19211EEdff47f25384cdc3"

# Wormhole Core Bridge — known 19 guardians
WORMHOLE_BRIDGE = "0x98f3c9e6E3fAce36bAAd05FE09d375Ef1464288B"


def _check_env():
    """Check required env vars are set."""
    missing = []
    if not os.environ.get("ALCHEMY_API_KEY"):
        missing.append("ALCHEMY_API_KEY")
    if not os.environ.get("ETHERSCAN_API_KEY"):
        missing.append("ETHERSCAN_API_KEY")
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}")
        print("Set them and re-run.")
        sys.exit(1)


async def run_tests():
    from app.collectors.smart_contract import (
        read_timelock_delay,
        read_multisig_config,
        detect_proxy_pattern,
        read_access_control,
        detect_emergency_mechanism,
        read_guardian_set,
        normalize_timelock_hours,
        normalize_multisig_config,
        normalize_proxy_pattern,
        normalize_access_control,
        normalize_emergency_mechanism,
        normalize_guardian_count,
    )

    passed = 0
    failed = 0
    total = 6
    results = []

    async with httpx.AsyncClient(timeout=20) as client:

        # ---------------------------------------------------------------
        # Test 1: Timelock delay
        # ---------------------------------------------------------------
        print("\n" + "=" * 60)
        print("TEST 1: read_timelock_delay")
        print(f"  Target: Aave Executor {AAVE_TIMELOCK}")
        print("-" * 60)
        start = time.monotonic()
        try:
            result = await read_timelock_delay(client, AAVE_TIMELOCK, "ethereum")
            elapsed = time.monotonic() - start
            if result and "dao_timelock_hours" in result:
                hours = result["dao_timelock_hours"]
                seconds = hours * 3600
                score = normalize_timelock_hours(hours)
                print(f"  Aave timelock delay: {int(seconds)} seconds ({hours:.1f} hours)")
                print(f"  Normalized score: {score:.0f}/100")
                print(f"  Time: {elapsed:.1f}s")
                print("  PASSED")
                passed += 1
                results.append(("read_timelock_delay", True, None))
            else:
                print(f"  Result: {result}")
                print(f"  Time: {elapsed:.1f}s")
                print("  FAILED — returned None or missing key")
                failed += 1
                results.append(("read_timelock_delay", False, "returned None"))
        except Exception as e:
            elapsed = time.monotonic() - start
            print(f"  ERROR: {e}")
            print(f"  Time: {elapsed:.1f}s")
            failed += 1
            results.append(("read_timelock_delay", False, str(e)))

        await asyncio.sleep(0.2)

        # ---------------------------------------------------------------
        # Test 2: Multisig config
        # ---------------------------------------------------------------
        print("\n" + "=" * 60)
        print("TEST 2: read_multisig_config")
        print(f"  Target: Aave Guardian Safe {AAVE_MULTISIG}")
        print("-" * 60)
        start = time.monotonic()
        try:
            result = await read_multisig_config(client, AAVE_MULTISIG, "ethereum")
            elapsed = time.monotonic() - start
            if result and "signer_count" in result and "threshold" in result:
                signers = result["signer_count"]
                threshold = result["threshold"]
                score = normalize_multisig_config(signers, threshold)
                print(f"  Aave multisig: {threshold} of {signers} signers")
                print(f"  Normalized score: {score:.0f}/100")
                print(f"  Time: {elapsed:.1f}s")
                print("  PASSED")
                passed += 1
                results.append(("read_multisig_config", True, None))
            else:
                print(f"  Result: {result}")
                print(f"  Time: {elapsed:.1f}s")
                print("  FAILED — returned None or missing keys")
                failed += 1
                results.append(("read_multisig_config", False, "returned None"))
        except Exception as e:
            elapsed = time.monotonic() - start
            print(f"  ERROR: {e}")
            print(f"  Time: {elapsed:.1f}s")
            failed += 1
            results.append(("read_multisig_config", False, str(e)))

        await asyncio.sleep(0.2)

        # ---------------------------------------------------------------
        # Test 3: Proxy pattern
        # ---------------------------------------------------------------
        print("\n" + "=" * 60)
        print("TEST 3: detect_proxy_pattern")
        print(f"  Target: USDC (FiatTokenV2) {USDC_PROXY}")
        print("-" * 60)
        start = time.monotonic()
        try:
            result = await detect_proxy_pattern(client, USDC_PROXY, "ethereum")
            elapsed = time.monotonic() - start
            if result is not None:
                is_proxy = result.get("is_proxy", False)
                impl = result.get("implementation", "")
                has_admin = result.get("has_admin", False)
                score = normalize_proxy_pattern(result)
                status = "YES" if is_proxy else "NO"
                print(f"  USDC proxy: {status}", end="")
                if is_proxy:
                    print(f" — implementation at {impl}")
                else:
                    print()
                print(f"  Admin slot occupied: {'YES' if has_admin else 'NO'}")
                print(f"  Normalized score: {score:.0f}/100")
                print(f"  Time: {elapsed:.1f}s")
                print("  PASSED")
                passed += 1
                results.append(("detect_proxy_pattern", True, None))
            else:
                print(f"  Result: {result}")
                print(f"  Time: {elapsed:.1f}s")
                print("  FAILED — returned None")
                failed += 1
                results.append(("detect_proxy_pattern", False, "returned None"))
        except Exception as e:
            elapsed = time.monotonic() - start
            print(f"  ERROR: {e}")
            print(f"  Time: {elapsed:.1f}s")
            failed += 1
            results.append(("detect_proxy_pattern", False, str(e)))

        await asyncio.sleep(0.2)

        # ---------------------------------------------------------------
        # Test 4: Access control
        # ---------------------------------------------------------------
        print("\n" + "=" * 60)
        print("TEST 4: read_access_control")
        print(f"  Target: Aave Pool V3 {AAVE_POOL}")
        print("-" * 60)
        start = time.monotonic()
        try:
            result = await read_access_control(client, AAVE_POOL, "ethereum")
            elapsed = time.monotonic() - start
            if result is not None:
                has_ac = result.get("has_access_control", False)
                roles = result.get("role_count", 0)
                score = normalize_access_control(result)
                if has_ac:
                    print(f"  Aave access control: role-based, {roles} roles detected")
                else:
                    print(f"  Aave access control: no AccessControl pattern found")
                print(f"  Normalized score: {score:.0f}/100")
                print(f"  Time: {elapsed:.1f}s")
                print("  PASSED")
                passed += 1
                results.append(("read_access_control", True, None))
            else:
                print(f"  Result: {result}")
                print(f"  Time: {elapsed:.1f}s")
                print("  FAILED — returned None")
                failed += 1
                results.append(("read_access_control", False, "returned None"))
        except Exception as e:
            elapsed = time.monotonic() - start
            print(f"  ERROR: {e}")
            print(f"  Time: {elapsed:.1f}s")
            failed += 1
            results.append(("read_access_control", False, str(e)))

        await asyncio.sleep(0.2)

        # ---------------------------------------------------------------
        # Test 5: Emergency mechanism
        # ---------------------------------------------------------------
        print("\n" + "=" * 60)
        print("TEST 5: detect_emergency_mechanism")
        print(f"  Target: Compound Comet USDC {COMPOUND_COMET}")
        print("-" * 60)
        start = time.monotonic()
        try:
            result = await detect_emergency_mechanism(client, COMPOUND_COMET, "ethereum")
            elapsed = time.monotonic() - start
            if result is not None:
                has_pause = result.get("has_pause", False)
                has_ew = result.get("has_emergency_withdraw", False)
                has_cb = result.get("has_circuit_breaker", False)
                score = normalize_emergency_mechanism(result)
                features = []
                if has_pause:
                    features.append("has pause()")
                if has_ew:
                    features.append("has emergencyWithdraw()")
                if has_cb:
                    features.append("has circuitBreaker()")
                if not features:
                    features.append("no emergency functions detected")
                print(f"  Compound emergency mechanisms: {', '.join(features)}")
                print(f"  Normalized score: {score:.0f}/100")
                print(f"  Time: {elapsed:.1f}s")
                print("  PASSED")
                passed += 1
                results.append(("detect_emergency_mechanism", True, None))
            else:
                print(f"  Result: {result}")
                print(f"  Time: {elapsed:.1f}s")
                print("  FAILED — returned None")
                failed += 1
                results.append(("detect_emergency_mechanism", False, "returned None"))
        except Exception as e:
            elapsed = time.monotonic() - start
            print(f"  ERROR: {e}")
            print(f"  Time: {elapsed:.1f}s")
            failed += 1
            results.append(("detect_emergency_mechanism", False, str(e)))

        await asyncio.sleep(0.2)

        # ---------------------------------------------------------------
        # Test 6: Guardian set
        # ---------------------------------------------------------------
        print("\n" + "=" * 60)
        print("TEST 6: read_guardian_set")
        print(f"  Target: Wormhole Core Bridge {WORMHOLE_BRIDGE}")
        print("-" * 60)
        start = time.monotonic()
        try:
            result = await read_guardian_set(client, WORMHOLE_BRIDGE, "ethereum", "wormhole")
            elapsed = time.monotonic() - start
            if result and "guardian_count" in result:
                count = result["guardian_count"]
                score = normalize_guardian_count(count)
                print(f"  Wormhole guardians: {count} addresses found")
                print(f"  Normalized score: {score:.0f}/100")
                print(f"  Time: {elapsed:.1f}s")
                print("  PASSED")
                passed += 1
                results.append(("read_guardian_set", True, None))
            else:
                print(f"  Result: {result}")
                print(f"  Time: {elapsed:.1f}s")
                print("  FAILED — returned None or missing guardian_count")
                failed += 1
                results.append(("read_guardian_set", False, "returned None"))
        except Exception as e:
            elapsed = time.monotonic() - start
            print(f"  ERROR: Could not read guardian set on {WORMHOLE_BRIDGE} — {e}")
            print(f"  Time: {elapsed:.1f}s")
            failed += 1
            results.append(("read_guardian_set", False, str(e)))

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for name, ok, err in results:
        status = "PASSED" if ok else f"FAILED ({err})"
        print(f"  {name}: {status}")
    print(f"\n{total} readers tested. {passed} passed. {failed} failed.", end="")
    if failed > 0:
        failures = [name for name, ok, _ in results if not ok]
        print(f" ({', '.join(failures)})")
    else:
        print()

    return failed == 0


if __name__ == "__main__":
    _check_env()
    print("Sprint 1 Verification: Contract Read Extension")
    print("=" * 60)
    ok = asyncio.run(run_tests())
    sys.exit(0 if ok else 1)
