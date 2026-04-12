#!/usr/bin/env python3
"""
Sprint 4 Verification: Parallel Web Research Components
=========================================================
Self-contained test — no app imports, uses only requests + stdlib.

Runs Parallel.ai Task API research on known entities.

Requirements:
  - PARALLEL_API_KEY env var

Usage:
  python scripts/test_sprint4.py
"""

import json
import os
import sys
import time
import requests

PARALLEL_API_KEY = os.environ.get("PARALLEL_API_KEY", "")
API_HOST = "https://api.parallel.ai"


def _check_env():
    if not PARALLEL_API_KEY:
        print("ERROR: Missing env var: PARALLEL_API_KEY")
        print("Set it and re-run.")
        sys.exit(1)


def _headers():
    return {"x-api-key": PARALLEL_API_KEY, "Content-Type": "application/json"}


def run_task(question, fields, timeout=300):
    """Run a Parallel Task and poll for result."""
    schema = {
        "type": "json",
        "json_schema": {
            "type": "object",
            "properties": {k: {"type": "string", "description": v} for k, v in fields.items()},
        },
    }
    body = {
        "input": question,
        "processor": "base",
        "task_spec": {"output_schema": schema},
    }

    # Create run
    resp = requests.post(f"{API_HOST}/v1/tasks/runs", headers=_headers(), json=body, timeout=30)
    resp.raise_for_status()
    run_id = resp.json().get("run_id")
    if not run_id:
        return None, "no run_id returned"

    # Poll for result
    resp = requests.get(
        f"{API_HOST}/v1/tasks/runs/{run_id}/result",
        headers=_headers(),
        params={"timeout": timeout},
        timeout=timeout + 30,
    )
    if resp.status_code == 408:
        return None, "task timed out"
    resp.raise_for_status()
    result = resp.json()
    output = result.get("output", {})
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            pass
    return output, None


def run_tests():
    passed = 0
    failed = 0
    results = []

    # --- Test 1: Bridge audits ---
    test_bridges = [("Wormhole", "wormhole"), ("LayerZero", "layerzero")]
    print("\n" + "=" * 60)
    print("TEST 1: Bridge Audit Research")
    print("=" * 60)

    for name, slug in test_bridges:
        print(f"\n  Bridge: {name}")
        print("  " + "-" * 56)
        t = time.monotonic()
        try:
            output, err = run_task(
                f"Find all public security audits for the {name} bridge protocol. Include auditor name, date, scope, and URL.",
                {"audits": "JSON array of audits with auditor, date, scope, url", "total_count": "Total number of audits found"},
            )
            elapsed = time.monotonic() - t
            if err:
                print(f"  ERROR: {err} ({elapsed:.1f}s)")
                failed += 1
                results.append((f"audit/{slug}", False, err))
                continue

            count = 0
            auditor_names = []
            if isinstance(output, dict):
                count = int(output.get("total_count", 0) or 0)
                audits = output.get("audits", [])
                if isinstance(audits, str):
                    try:
                        audits = json.loads(audits)
                    except Exception:
                        audits = []
                if isinstance(audits, list):
                    auditor_names = [a.get("auditor", "?") for a in audits if isinstance(a, dict)][:5]
                    count = max(count, len(auditor_names))

            if auditor_names:
                print(f"  {name} audits found: {count} ({', '.join(auditor_names)})")
            else:
                print(f"  {name} audits found: {count}")
            print(f"  Time: {elapsed:.1f}s")
            print("  PASSED")
            passed += 1
            results.append((f"audit/{slug}", True, None))
        except Exception as e:
            elapsed = time.monotonic() - t
            print(f"  ERROR: {e} ({elapsed:.1f}s)")
            failed += 1
            results.append((f"audit/{slug}", False, str(e)))

    # --- Test 2: PoR frequency ---
    test_exchanges = [("Binance", "binance"), ("Coinbase", "coinbase")]
    print("\n" + "=" * 60)
    print("TEST 2: PoR Frequency Research")
    print("=" * 60)

    for name, slug in test_exchanges:
        print(f"\n  Exchange: {name}")
        print("  " + "-" * 56)
        t = time.monotonic()
        try:
            output, err = run_task(
                f"When did {name} last publish their proof-of-reserves? What is the publication frequency?",
                {"last_published": "Date (YYYY-MM-DD)", "frequency_days": "Days between publications", "methodology": "Brief methodology description"},
            )
            elapsed = time.monotonic() - t
            if err:
                print(f"  ERROR: {err} ({elapsed:.1f}s)")
                failed += 1
                results.append((f"por_freq/{slug}", False, err))
                continue

            if isinstance(output, dict):
                last = output.get("last_published", "unknown")
                freq = output.get("frequency_days", "unknown")
                method = output.get("methodology", "")
                print(f"  {name} PoR last published: {last}, frequency: every {freq} days")
                if method:
                    print(f"  Method: {method[:100]}")
            else:
                print(f"  Raw output: {str(output)[:200]}")
            print(f"  Time: {elapsed:.1f}s")
            print("  PASSED")
            passed += 1
            results.append((f"por_freq/{slug}", True, None))
        except Exception as e:
            elapsed = time.monotonic() - t
            print(f"  ERROR: {e} ({elapsed:.1f}s)")
            failed += 1
            results.append((f"por_freq/{slug}", False, str(e)))

    # --- Test 3: Meeting cadence ---
    test_protocols = [("Aave", "aave"), ("Compound", "compound")]
    print("\n" + "=" * 60)
    print("TEST 3: Meeting Cadence Research")
    print("=" * 60)

    for name, slug in test_protocols:
        print(f"\n  Protocol: {name}")
        print("  " + "-" * 56)
        t = time.monotonic()
        try:
            output, err = run_task(
                f"Does {name} hold regular governance calls, community meetings, or publish regular updates? What is the cadence?",
                {"has_regular_meetings": "true or false", "frequency": "weekly/biweekly/monthly/irregular/none", "most_recent_date": "Date (YYYY-MM-DD)"},
            )
            elapsed = time.monotonic() - t
            if err:
                print(f"  ERROR: {err} ({elapsed:.1f}s)")
                failed += 1
                results.append((f"meeting/{slug}", False, err))
                continue

            if isinstance(output, dict):
                has = str(output.get("has_regular_meetings", "")).lower() in ("true", "yes")
                freq = output.get("frequency", "unknown")
                recent = output.get("most_recent_date", "unknown")
                status = f"{freq} meetings" if has else "no regular meetings found"
                print(f"  {name}: {status}, last: {recent}")
            else:
                print(f"  Raw output: {str(output)[:200]}")
            print(f"  Time: {elapsed:.1f}s")
            print("  PASSED")
            passed += 1
            results.append((f"meeting/{slug}", True, None))
        except Exception as e:
            elapsed = time.monotonic() - t
            print(f"  ERROR: {e} ({elapsed:.1f}s)")
            failed += 1
            results.append((f"meeting/{slug}", False, str(e)))

    total = len(results)

    # --- Summary ---
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for n, ok, err in results:
        status = "PASSED" if ok else f"FAILED ({err})"
        print(f"  {n}: {status}")
    print(f"\n{total} research tasks run. {passed} passed. {failed} failed.", end="")
    if failed > 0:
        failures = [n for n, ok, _ in results if not ok]
        print(f" ({', '.join(failures)})")
    else:
        print()

    return failed == 0


if __name__ == "__main__":
    _check_env()
    print("Sprint 4 Verification: Parallel Web Research Components")
    print("=" * 60)
    ok = run_tests()
    sys.exit(0 if ok else 1)
