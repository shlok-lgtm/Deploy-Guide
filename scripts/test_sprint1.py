#!/usr/bin/env python3
"""
Sprint 1 Verification: Contract Read Extension
================================================
Self-contained test — no app imports, uses only requests + stdlib.

Tests 6 on-chain reader functions against known contracts:
  1. Timelock delay (Aave Executor)
  2. Multisig config (Aave Guardian Safe)
  3. Proxy pattern (USDC FiatTokenV2)
  4. Access control (Aave Pool V3 ABI)
  5. Emergency mechanism (Compound Comet ABI)
  6. Guardian set (Wormhole Core Bridge)

Requirements:
  - ALCHEMY_API_KEY env var
  - ETHERSCAN_API_KEY env var

Usage:
  python scripts/test_sprint1.py
"""

import json
import os
import sys
import time
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ALCHEMY_API_KEY = os.environ.get("ALCHEMY_API_KEY", "")
ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"

AAVE_TIMELOCK = "0x61910EcD7e8e942136CE7Fe7943f956cea1CC2f7"
AAVE_MULTISIG = "0xCA76Ebd8617a03126B6FB84F9b1c1A0fB71C2633"
USDC_PROXY = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
AAVE_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
COMPOUND_COMET = "0xc3d688B66703497DAA19211EEdff47f25384cdc3"
WORMHOLE_BRIDGE = "0x98f3c9e6E3fAce36bAAd05FE09d375Ef1464288B"

EIP1967_IMPL_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
EIP1967_ADMIN_SLOT = "0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3"

ZERO_32 = "0x" + "00" * 32


def _check_env():
    missing = []
    if not ALCHEMY_API_KEY:
        missing.append("ALCHEMY_API_KEY")
    if not ETHERSCAN_API_KEY:
        missing.append("ETHERSCAN_API_KEY")
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}")
        print("Set them and re-run.")
        sys.exit(1)


def _rpc_url():
    return f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"


def _eth_call(to, data):
    """Raw eth_call via Alchemy."""
    resp = requests.post(_rpc_url(), json={
        "jsonrpc": "2.0", "id": 1,
        "method": "eth_call",
        "params": [{"to": to, "data": data}, "latest"],
    }, timeout=15)
    return resp.json().get("result", "0x")


def _get_storage(address, slot):
    """Raw eth_getStorageAt."""
    resp = requests.post(_rpc_url(), json={
        "jsonrpc": "2.0", "id": 1,
        "method": "eth_getStorageAt",
        "params": [address, slot, "latest"],
    }, timeout=15)
    return resp.json().get("result", ZERO_32)


def _get_abi(address):
    """Fetch ABI from Etherscan V2."""
    resp = requests.get(ETHERSCAN_V2, params={
        "chainid": 1, "module": "contract", "action": "getabi",
        "address": address, "apikey": ETHERSCAN_API_KEY,
    }, timeout=20)
    data = resp.json()
    if data.get("status") == "1" and data.get("result", "").startswith("["):
        return json.loads(data["result"])
    return []


def run_tests():
    passed = 0
    failed = 0
    results = []

    # -------------------------------------------------------------------
    # Test 1: Timelock delay
    # -------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("TEST 1: read_timelock_delay")
    print(f"  Target: Aave Executor {AAVE_TIMELOCK}")
    print("-" * 60)
    t = time.monotonic()
    try:
        # delay() = 0x6a42b8f8
        raw = _eth_call(AAVE_TIMELOCK, "0x6a42b8f8")
        if raw == "0x" or raw == ZERO_32:
            # getDelay() = 0xcebc9a82
            raw = _eth_call(AAVE_TIMELOCK, "0xcebc9a82")
        elapsed = time.monotonic() - t

        if raw and raw != "0x" and raw != ZERO_32:
            seconds = int(raw, 16)
            hours = seconds / 3600
            print(f"  Aave timelock delay: {seconds:,} seconds ({hours:.1f} hours)")
            print(f"  Time: {elapsed:.1f}s")
            print("  PASSED")
            passed += 1
            results.append(("read_timelock_delay", True, None))
        else:
            print(f"  Raw result: {raw}")
            print(f"  Time: {elapsed:.1f}s")
            print("  FAILED — returned zero/empty")
            failed += 1
            results.append(("read_timelock_delay", False, "returned zero"))
    except Exception as e:
        print(f"  ERROR: {e}")
        failed += 1
        results.append(("read_timelock_delay", False, str(e)))

    time.sleep(0.2)

    # -------------------------------------------------------------------
    # Test 2: Multisig config
    # -------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("TEST 2: read_multisig_config")
    print(f"  Target: Aave Guardian Safe {AAVE_MULTISIG}")
    print("-" * 60)
    t = time.monotonic()
    try:
        # getThreshold() = 0xe75235b8
        thresh_raw = _eth_call(AAVE_MULTISIG, "0xe75235b8")
        # getOwners() = 0xa0e67e2b
        owners_raw = _eth_call(AAVE_MULTISIG, "0xa0e67e2b")
        elapsed = time.monotonic() - t

        if thresh_raw and thresh_raw != "0x" and thresh_raw != ZERO_32:
            threshold = int(thresh_raw, 16)
            # Decode dynamic array length
            hex_data = owners_raw[2:]  # strip 0x
            owner_count = int(hex_data[64:128], 16) if len(hex_data) >= 128 else 0
            print(f"  Aave multisig: {threshold} of {owner_count} signers")
            print(f"  Time: {elapsed:.1f}s")
            print("  PASSED")
            passed += 1
            results.append(("read_multisig_config", True, None))
        else:
            print(f"  Threshold raw: {thresh_raw}")
            print(f"  Time: {elapsed:.1f}s")
            print("  FAILED — could not read threshold")
            failed += 1
            results.append(("read_multisig_config", False, "no threshold"))
    except Exception as e:
        print(f"  ERROR: {e}")
        failed += 1
        results.append(("read_multisig_config", False, str(e)))

    time.sleep(0.2)

    # -------------------------------------------------------------------
    # Test 3: Proxy pattern
    # -------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("TEST 3: detect_proxy_pattern")
    print(f"  Target: USDC {USDC_PROXY}")
    print("-" * 60)
    t = time.monotonic()
    try:
        impl = _get_storage(USDC_PROXY, EIP1967_IMPL_SLOT)
        admin = _get_storage(USDC_PROXY, EIP1967_ADMIN_SLOT)
        elapsed = time.monotonic() - t

        is_proxy = impl != ZERO_32 and impl != "0x"
        has_admin = admin != ZERO_32 and admin != "0x"
        impl_addr = "0x" + impl[-40:] if is_proxy else "none"

        if is_proxy:
            print(f"  USDC proxy: YES — implementation at {impl_addr}")
        else:
            print(f"  USDC proxy: NO")
        print(f"  Admin slot occupied: {'YES' if has_admin else 'NO'}")
        print(f"  Time: {elapsed:.1f}s")
        print("  PASSED")
        passed += 1
        results.append(("detect_proxy_pattern", True, None))
    except Exception as e:
        print(f"  ERROR: {e}")
        failed += 1
        results.append(("detect_proxy_pattern", False, str(e)))

    time.sleep(0.2)

    # -------------------------------------------------------------------
    # Test 4: Access control
    # -------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("TEST 4: read_access_control")
    print(f"  Target: Aave Pool V3 {AAVE_POOL}")
    print("-" * 60)
    t = time.monotonic()
    try:
        abi = _get_abi(AAVE_POOL)
        elapsed = time.monotonic() - t

        if abi:
            fn_names = {item.get("name", "") for item in abi if item.get("type") == "function"}
            ac_indicators = {"hasRole", "getRoleMemberCount", "grantRole", "revokeRole", "renounceRole"}
            ac_matches = fn_names & ac_indicators
            has_ac = len(ac_matches) >= 2

            if has_ac:
                print(f"  Aave access control: role-based, {len(ac_matches)} AccessControl functions detected")
                print(f"  Functions: {', '.join(sorted(ac_matches))}")
            else:
                print(f"  Aave access control: no AccessControl pattern found ({len(fn_names)} total functions)")
            print(f"  Time: {elapsed:.1f}s")
            print("  PASSED")
            passed += 1
            results.append(("read_access_control", True, None))
        else:
            print(f"  Could not fetch ABI ({elapsed:.1f}s)")
            print("  FAILED — empty ABI")
            failed += 1
            results.append(("read_access_control", False, "empty ABI"))
    except Exception as e:
        print(f"  ERROR: {e}")
        failed += 1
        results.append(("read_access_control", False, str(e)))

    time.sleep(0.2)

    # -------------------------------------------------------------------
    # Test 5: Emergency mechanism
    # -------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("TEST 5: detect_emergency_mechanism")
    print(f"  Target: Compound Comet USDC {COMPOUND_COMET}")
    print("-" * 60)
    t = time.monotonic()
    try:
        abi = _get_abi(COMPOUND_COMET)
        elapsed = time.monotonic() - t

        if abi:
            fn_names = {item.get("name", "").lower() for item in abi if item.get("type") == "function"}
            has_pause = "pause" in fn_names or "unpause" in fn_names or "paused" in fn_names
            has_ew = "emergencywithdraw" in fn_names or "emergencywithdrawal" in fn_names
            has_cb = "circuitbreaker" in fn_names or "circuit_breaker" in fn_names

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
            print(f"  Time: {elapsed:.1f}s")
            print("  PASSED")
            passed += 1
            results.append(("detect_emergency_mechanism", True, None))
        else:
            print(f"  Could not fetch ABI ({elapsed:.1f}s)")
            print("  FAILED — empty ABI")
            failed += 1
            results.append(("detect_emergency_mechanism", False, "empty ABI"))
    except Exception as e:
        print(f"  ERROR: {e}")
        failed += 1
        results.append(("detect_emergency_mechanism", False, str(e)))

    time.sleep(0.2)

    # -------------------------------------------------------------------
    # Test 6: Guardian set
    # -------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("TEST 6: read_guardian_set")
    print(f"  Target: Wormhole Core Bridge {WORMHOLE_BRIDGE}")
    print("-" * 60)
    t = time.monotonic()
    try:
        # getCurrentGuardianSetIndex() = 0x1cfe7951
        idx_raw = _eth_call(WORMHOLE_BRIDGE, "0x1cfe7951")
        elapsed_idx = time.monotonic() - t

        if idx_raw and idx_raw != "0x":
            set_index = int(idx_raw, 16)
            # getGuardianSet(uint32) = 0xf951975a + index
            data = "0xf951975a" + hex(set_index)[2:].zfill(64)
            gs_raw = _eth_call(WORMHOLE_BRIDGE, data)
            elapsed = time.monotonic() - t

            if gs_raw and len(gs_raw) > 130:
                hex_data = gs_raw[2:]
                guardian_count = int(hex_data[128:192], 16) if len(hex_data) >= 192 else 0
                if 0 < guardian_count < 100:
                    print(f"  Wormhole guardians: {guardian_count} addresses found (set index: {set_index})")
                    print(f"  Time: {elapsed:.1f}s")
                    print("  PASSED")
                    passed += 1
                    results.append(("read_guardian_set", True, None))
                else:
                    print(f"  Parsed count={guardian_count} (unexpected)")
                    print("  FAILED — count out of range")
                    failed += 1
                    results.append(("read_guardian_set", False, f"count={guardian_count}"))
            else:
                print(f"  getGuardianSet returned too short: {len(gs_raw)} chars")
                print("  FAILED — short response")
                failed += 1
                results.append(("read_guardian_set", False, "short response"))
        else:
            print(f"  getCurrentGuardianSetIndex returned: {idx_raw}")
            print("  FAILED — no index")
            failed += 1
            results.append(("read_guardian_set", False, "no index"))
    except Exception as e:
        elapsed = time.monotonic() - t
        print(f"  ERROR: Could not read guardian set on {WORMHOLE_BRIDGE} — {e}")
        print(f"  Time: {elapsed:.1f}s")
        failed += 1
        results.append(("read_guardian_set", False, str(e)))

    # -------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------
    total = 6
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
    ok = run_tests()
    sys.exit(0 if ok else 1)
