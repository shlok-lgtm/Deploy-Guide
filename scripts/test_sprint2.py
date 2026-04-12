#!/usr/bin/env python3
"""
Sprint 2 Verification: CoinGecko DEX API Integration
======================================================
Calls the DEX pool collector on 3 known protocols (Aave, Uniswap,
Compound) and prints pool-level data in plain English.

Requirements:
  - COINGECKO_API_KEY env var (Pro/Analyst tier with on-chain access)

Usage:
  python scripts/test_sprint2.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _check_env():
    if not os.environ.get("COINGECKO_API_KEY"):
        print("ERROR: Missing env var: COINGECKO_API_KEY")
        print("Set it and re-run. Needs Pro/Analyst tier for on-chain endpoints.")
        sys.exit(1)


def _fmt_usd(val):
    """Format a number as USD with appropriate suffix."""
    if val >= 1e9:
        return f"${val / 1e9:.2f}B"
    if val >= 1e6:
        return f"${val / 1e6:.1f}M"
    if val >= 1e3:
        return f"${val / 1e3:.0f}K"
    return f"${val:.0f}"


def _fmt_pct(val):
    """Format as signed percentage."""
    return f"{val:+.1f}%" if val != 0 else "0.0%"


def run_tests():
    from app.collectors.dex_pools import (
        get_protocol_pools,
        compute_position_liquidity,
        compute_collateral_diversity,
        PROTOCOL_DEX_MAP,
    )

    test_protocols = ["aave", "uniswap", "compound-finance"]
    passed = 0
    failed = 0
    total = len(test_protocols) * 3  # pools + liquidity + diversity per protocol
    results = []

    for slug in test_protocols:
        config = PROTOCOL_DEX_MAP.get(slug, {})
        network = config.get("network", "eth")
        name = slug.replace("-", " ").title()

        print("\n" + "=" * 60)
        print(f"PROTOCOL: {name}")
        print("=" * 60)

        # -------------------------------------------------------------------
        # Test A: Pool discovery
        # -------------------------------------------------------------------
        print(f"\n  Test A: Pool discovery ({slug})")
        print("  " + "-" * 56)
        start = time.monotonic()
        try:
            pools = get_protocol_pools(slug, network)
            elapsed = time.monotonic() - start
            if pools:
                print(f"  Found {len(pools)} pools in {elapsed:.1f}s:")
                for i, p in enumerate(pools[:3]):
                    tvl = _fmt_usd(p.get("reserve_in_usd", 0))
                    vol = _fmt_usd(p.get("volume_24h", 0))
                    pool_name = p.get("name", "unnamed")
                    print(f"    {i+1}. {pool_name}: TVL {tvl}, 24h volume {vol}")
                if len(pools) > 3:
                    print(f"    ... and {len(pools) - 3} more")
                print("  PASSED")
                passed += 1
                results.append((f"{slug}/pools", True, None))
            else:
                print(f"  No pools found ({elapsed:.1f}s)")
                print("  FAILED — empty result")
                failed += 1
                results.append((f"{slug}/pools", False, "no pools found"))
        except Exception as e:
            elapsed = time.monotonic() - start
            print(f"  ERROR: {e} ({elapsed:.1f}s)")
            failed += 1
            results.append((f"{slug}/pools", False, str(e)))

        # -------------------------------------------------------------------
        # Test B: Position liquidity composite
        # -------------------------------------------------------------------
        print(f"\n  Test B: Position liquidity ({slug})")
        print("  " + "-" * 56)
        start = time.monotonic()
        try:
            pl = compute_position_liquidity(slug)
            elapsed = time.monotonic() - start
            if pl and pl.get("score") is not None:
                tvl = _fmt_usd(pl.get("current_tvl", 0))
                vol = _fmt_usd(pl.get("volume_24h", 0))
                trend = _fmt_pct(pl.get("tvl_30d_trend", 0) * 100)
                stability = pl.get("volume_stability", 0)
                score = pl["score"]
                print(f"  {name} pools: TVL {tvl}, 24h volume {vol}, 30d trend: {trend}")
                print(f"  Volume stability: {stability:.2f} (0=volatile, 1=stable)")
                print(f"  Position liquidity score: {score:.0f}/100")
                print(f"  Time: {elapsed:.1f}s")
                print("  PASSED")
                passed += 1
                results.append((f"{slug}/position_liquidity", True, None))
            else:
                print(f"  Result: {pl} ({elapsed:.1f}s)")
                print("  FAILED — empty or missing score")
                failed += 1
                results.append((f"{slug}/position_liquidity", False, "no score"))
        except Exception as e:
            elapsed = time.monotonic() - start
            print(f"  ERROR: {e} ({elapsed:.1f}s)")
            failed += 1
            results.append((f"{slug}/position_liquidity", False, str(e)))

        # -------------------------------------------------------------------
        # Test C: Collateral diversity
        # -------------------------------------------------------------------
        print(f"\n  Test C: Collateral diversity ({slug})")
        print("  " + "-" * 56)
        start = time.monotonic()
        try:
            cd = compute_collateral_diversity(slug)
            elapsed = time.monotonic() - start
            if cd and cd.get("score") is not None:
                tokens = cd.get("unique_tokens", 0)
                conc = cd.get("concentration_top3", 0)
                has_stable = cd.get("has_stablecoin_exposure", False)
                score = cd["score"]
                print(f"  {name}: {tokens} unique tokens, top-3 concentration: {conc:.1f}%")
                print(f"  Stablecoin exposure: {'YES' if has_stable else 'NO'}")
                print(f"  Collateral diversity score: {score:.0f}/100")
                print(f"  Time: {elapsed:.1f}s")
                print("  PASSED")
                passed += 1
                results.append((f"{slug}/collateral_diversity", True, None))
            else:
                print(f"  Result: {cd} ({elapsed:.1f}s)")
                print("  FAILED — empty or missing score")
                failed += 1
                results.append((f"{slug}/collateral_diversity", False, "no score"))
        except Exception as e:
            elapsed = time.monotonic() - start
            print(f"  ERROR: {e} ({elapsed:.1f}s)")
            failed += 1
            results.append((f"{slug}/collateral_diversity", False, str(e)))

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for name, ok, err in results:
        status = "PASSED" if ok else f"FAILED ({err})"
        print(f"  {name}: {status}")
    print(f"\n{total} tests run. {passed} passed. {failed} failed.", end="")
    if failed > 0:
        failures = [name for name, ok, _ in results if not ok]
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
