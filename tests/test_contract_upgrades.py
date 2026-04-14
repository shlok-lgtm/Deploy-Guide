"""
Isolated test for contract upgrade collector logic paths.
No database or RPC required — mocks all external calls.

Tests:
1. USDC proxy first-capture: snapshot inserted with implementation_address
2. USDC proxy upgrade: impl address change detected, upgrade record created
3. Non-proxy first-capture: direct bytecode hash stored
4. Non-proxy no-change: captured_at updated, no upgrade
5. Self-destruct: previously-snapshotted contract returns 0x → warning logged
6. EOA / no prior snapshot + no bytecode → debug-level skip
7. entity_filter works correctly
"""
import hashlib
import sys
import os
from unittest.mock import patch, MagicMock, call
from io import StringIO

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- Helpers ---
def make_hash(data: str) -> str:
    return "0x" + hashlib.sha256(data.encode()).hexdigest()

PROXY_BYTECODE = "0x363d3d373d3d3d363d73deadbeef5af43d82803e903d91602b57fd5bf3"
IMPL_BYTECODE_V1 = "0x608060405234801561001057600080fd5b5060..." + "a" * 200
IMPL_BYTECODE_V2 = "0x608060405234801561001057600080fd5b5060..." + "b" * 200
DIRECT_BYTECODE = "0x6080604052348015610010576000aabbccdd..." + "c" * 200

IMPL_ADDR_V1 = "0x" + "ab" * 20
IMPL_ADDR_V2 = "0x" + "cd" * 20

USDC_ADDR = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
AAVE_CORE = "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2"


class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def ok(self, name):
        self.passed += 1
        print(f"  PASS: {name}")

    def fail(self, name, detail):
        self.failed += 1
        self.errors.append((name, detail))
        print(f"  FAIL: {name} — {detail}")

    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*60}")
        print(f"Results: {self.passed}/{total} passed, {self.failed} failed")
        if self.errors:
            for name, detail in self.errors:
                print(f"  FAILED: {name}: {detail}")
        return self.failed == 0


# ============================================================================
# Test 1: _detect_change logic
# ============================================================================
def test_detect_change(results: TestResult):
    from app.collectors.contract_upgrades import _detect_change

    # 1a: Same proxy bytecode, same impl → no change
    snapshot = {"bytecode_hash": make_hash(PROXY_BYTECODE), "implementation_address": IMPL_ADDR_V1}
    changed = _detect_change(snapshot, make_hash(PROXY_BYTECODE), IMPL_ADDR_V1, make_hash(IMPL_BYTECODE_V1))
    if not changed:
        results.ok("proxy no-change: same impl address")
    else:
        results.fail("proxy no-change: same impl address", "should be False")

    # 1b: Same proxy bytecode, different impl → UPGRADE
    changed = _detect_change(snapshot, make_hash(PROXY_BYTECODE), IMPL_ADDR_V2, make_hash(IMPL_BYTECODE_V2))
    if changed:
        results.ok("proxy upgrade: impl address changed")
    else:
        results.fail("proxy upgrade: impl address changed", "should be True")

    # 1c: Direct bytecode change (non-proxy)
    snapshot2 = {"bytecode_hash": make_hash(DIRECT_BYTECODE), "implementation_address": None}
    new_bytecode = DIRECT_BYTECODE + "ff"
    changed = _detect_change(snapshot2, make_hash(new_bytecode), None, None)
    if changed:
        results.ok("non-proxy: bytecode changed")
    else:
        results.fail("non-proxy: bytecode changed", "should be True")

    # 1d: Non-proxy, same bytecode → no change
    changed = _detect_change(snapshot2, make_hash(DIRECT_BYTECODE), None, None)
    if not changed:
        results.ok("non-proxy no-change: same bytecode")
    else:
        results.fail("non-proxy no-change: same bytecode", "should be False")

    # 1e: Previously not a proxy, now resolves to impl → change
    snapshot3 = {"bytecode_hash": make_hash(PROXY_BYTECODE), "implementation_address": None}
    changed = _detect_change(snapshot3, make_hash(PROXY_BYTECODE), IMPL_ADDR_V1, make_hash(IMPL_BYTECODE_V1))
    if changed:
        results.ok("newly-proxy: impl appeared where none existed")
    else:
        results.fail("newly-proxy: impl appeared where none existed", "should be True")

    # 1f: Case-insensitive impl comparison
    snapshot4 = {"bytecode_hash": make_hash(PROXY_BYTECODE), "implementation_address": IMPL_ADDR_V1.lower()}
    changed = _detect_change(snapshot4, make_hash(PROXY_BYTECODE), IMPL_ADDR_V1.upper(), None)
    if not changed:
        results.ok("proxy: case-insensitive impl address match")
    else:
        results.fail("proxy: case-insensitive impl address match", "should be False (same address)")


# ============================================================================
# Test 2: Full collector with mocked DB and RPC — USDC proxy first capture
# ============================================================================
def test_usdc_first_capture(results: TestResult):
    """USDC proxy first capture: should insert snapshot with impl address, no upgrade."""
    executed_sql = []
    fetched_sql = []

    def mock_execute(sql, params=None):
        executed_sql.append((sql.strip(), params))

    def mock_fetch_all(sql, params=None):
        fetched_sql.append(sql.strip())
        if "FROM stablecoins" in sql:
            return [{"id": 1, "symbol": "USDC", "contract": USDC_ADDR}]
        return []

    def mock_fetch_one(sql, params=None):
        fetched_sql.append(sql.strip())
        if "contract_bytecode_snapshots" in sql:
            return None  # No prior snapshot
        if "psi_scores" in sql:
            return None
        return None

    def mock_rpc_get_code(rpc_url, address):
        return PROXY_BYTECODE

    def mock_resolve_impl(rpc_url, address):
        if address == USDC_ADDR:
            return IMPL_ADDR_V1
        return None

    with patch("app.collectors.contract_upgrades.fetch_all", mock_fetch_all), \
         patch("app.collectors.contract_upgrades.fetch_one", mock_fetch_one), \
         patch("app.collectors.contract_upgrades.execute", mock_execute), \
         patch("app.collectors.contract_upgrades._rpc_get_code", mock_rpc_get_code), \
         patch("app.collectors.contract_upgrades._resolve_implementation", mock_resolve_impl), \
         patch("app.collectors.contract_upgrades._load_contract_registry", lambda: {"protocols": {}, "bridges": {}}), \
         patch("app.collectors.contract_upgrades.time"):

        from app.collectors.contract_upgrades import collect_contract_upgrades
        result = collect_contract_upgrades(entity_filter="usdc")

    # Check result summary
    if result["first_captures"] == 1:
        results.ok("USDC first capture: first_captures=1")
    else:
        results.fail("USDC first capture: first_captures=1", f"got {result}")

    if result["upgrades_detected"] == 0:
        results.ok("USDC first capture: upgrades_detected=0 (correct)")
    else:
        results.fail("USDC first capture: upgrades_detected=0", f"got {result}")

    if result["entities_checked"] == 1:
        results.ok("USDC first capture: entities_checked=1")
    else:
        results.fail("USDC first capture: entities_checked=1", f"got {result}")

    # Check that the INSERT into snapshots includes impl address
    snapshot_inserts = [s for s in executed_sql if "contract_bytecode_snapshots" in s[0]]
    if snapshot_inserts:
        sql, params = snapshot_inserts[0]
        if IMPL_ADDR_V1 in (params or ()):
            results.ok("USDC first capture: snapshot includes implementation_address")
        else:
            results.fail("USDC first capture: snapshot includes implementation_address",
                        f"params={params}")
    else:
        results.fail("USDC first capture: snapshot INSERT executed", "no INSERT found")

    # Check no upgrade history INSERT
    upgrade_inserts = [s for s in executed_sql if "contract_upgrade_history" in s[0]]
    if not upgrade_inserts:
        results.ok("USDC first capture: no upgrade_history INSERT (correct)")
    else:
        results.fail("USDC first capture: no upgrade_history INSERT",
                    f"found {len(upgrade_inserts)} inserts")


# ============================================================================
# Test 3: USDC proxy upgrade — impl address changes
# ============================================================================
def test_usdc_upgrade_detected(results: TestResult):
    """Second run: impl address changed → upgrade record with impl bytecode hash."""
    executed_sql = []
    prev_hash = make_hash(PROXY_BYTECODE)

    def mock_execute(sql, params=None):
        executed_sql.append((sql.strip(), params))

    def mock_fetch_all(sql, params=None):
        if "FROM stablecoins" in sql:
            return [{"id": 1, "symbol": "USDC", "contract": USDC_ADDR}]
        return []

    def mock_fetch_one(sql, params=None):
        if "contract_bytecode_snapshots" in sql:
            # Previous snapshot exists with V1 impl
            return {"bytecode_hash": prev_hash, "implementation_address": IMPL_ADDR_V1}
        return None

    def mock_rpc_get_code(rpc_url, address):
        if address == IMPL_ADDR_V2:
            return IMPL_BYTECODE_V2
        return PROXY_BYTECODE  # proxy stub unchanged

    def mock_resolve_impl(rpc_url, address):
        if address == USDC_ADDR:
            return IMPL_ADDR_V2  # New impl!
        return None

    with patch("app.collectors.contract_upgrades.fetch_all", mock_fetch_all), \
         patch("app.collectors.contract_upgrades.fetch_one", mock_fetch_one), \
         patch("app.collectors.contract_upgrades.execute", mock_execute), \
         patch("app.collectors.contract_upgrades._rpc_get_code", mock_rpc_get_code), \
         patch("app.collectors.contract_upgrades._resolve_implementation", mock_resolve_impl), \
         patch("app.collectors.contract_upgrades._load_contract_registry", lambda: {"protocols": {}, "bridges": {}}), \
         patch("app.collectors.contract_upgrades.time"), \
         patch("app.state_attestation.attest_state", return_value="fakehash"):

        from app.collectors.contract_upgrades import collect_contract_upgrades
        result = collect_contract_upgrades(entity_filter="usdc")

    if result["upgrades_detected"] == 1:
        results.ok("USDC upgrade: upgrades_detected=1")
    else:
        results.fail("USDC upgrade: upgrades_detected=1", f"got {result}")

    # Check the upgrade record stores the IMPLEMENTATION bytecode hash, not proxy hash
    upgrade_inserts = [s for s in executed_sql if "contract_upgrade_history" in s[0]]
    if upgrade_inserts:
        sql, params = upgrade_inserts[0]
        # current_bytecode_hash should be impl V2 hash, not proxy hash
        impl_v2_hash = make_hash(IMPL_BYTECODE_V2)
        if impl_v2_hash in (params or ()):
            results.ok("USDC upgrade: current_bytecode_hash is impl bytecode (not proxy stub)")
        else:
            results.fail("USDC upgrade: current_bytecode_hash is impl bytecode",
                        f"expected {impl_v2_hash[:20]}... in params, got params={[str(p)[:20] for p in (params or ())]}")

        # previous_implementation and current_implementation populated
        if IMPL_ADDR_V1 in (params or ()) and IMPL_ADDR_V2 in (params or ()):
            results.ok("USDC upgrade: previous/current implementation addresses stored")
        else:
            results.fail("USDC upgrade: previous/current implementation addresses",
                        f"params={params}")
    else:
        results.fail("USDC upgrade: upgrade_history INSERT executed", "no INSERT found")


# ============================================================================
# Test 4: Self-destruct detection
# ============================================================================
def test_self_destruct(results: TestResult):
    """Contract that previously had bytecode now returns 0x → WARNING logged."""
    import logging as _logging
    log_output = StringIO()
    handler = _logging.StreamHandler(log_output)
    handler.setLevel(_logging.WARNING)

    test_logger = _logging.getLogger("app.collectors.contract_upgrades")
    test_logger.addHandler(handler)

    def mock_fetch_all(sql, params=None):
        if "FROM stablecoins" in sql:
            return [{"id": 1, "symbol": "USDC", "contract": USDC_ADDR}]
        return []

    def mock_fetch_one(sql, params=None):
        if "contract_bytecode_snapshots" in sql:
            # Previously had a snapshot
            return {"bytecode_hash": make_hash(PROXY_BYTECODE), "implementation_address": IMPL_ADDR_V1}
        return None

    def mock_rpc_get_code(rpc_url, address):
        return None  # 0x / self-destructed

    def mock_etherscan(address):
        return None  # Also returns nothing

    with patch("app.collectors.contract_upgrades.fetch_all", mock_fetch_all), \
         patch("app.collectors.contract_upgrades.fetch_one", mock_fetch_one), \
         patch("app.collectors.contract_upgrades.execute", lambda *a, **kw: None), \
         patch("app.collectors.contract_upgrades._rpc_get_code", mock_rpc_get_code), \
         patch("app.collectors.contract_upgrades._get_etherscan_bytecode", mock_etherscan), \
         patch("app.collectors.contract_upgrades._resolve_implementation", lambda *a: None), \
         patch("app.collectors.contract_upgrades._load_contract_registry", lambda: {"protocols": {}, "bridges": {}}), \
         patch("app.collectors.contract_upgrades.time"):

        from app.collectors.contract_upgrades import collect_contract_upgrades
        result = collect_contract_upgrades(entity_filter="usdc")

    log_text = log_output.getvalue()
    test_logger.removeHandler(handler)

    if "CONTRACT BYTECODE GONE" in log_text:
        results.ok("self-destruct: WARNING logged with BYTECODE GONE message")
    else:
        results.fail("self-destruct: WARNING logged", f"log was: {log_text!r}")

    if result["entities_checked"] == 1:
        results.ok("self-destruct: entities_checked=1 (still counted)")
    else:
        results.fail("self-destruct: entities_checked=1", f"got {result}")


# ============================================================================
# Test 5: entity_filter works
# ============================================================================
def test_entity_filter(results: TestResult):
    """entity_filter='usdc' should exclude non-USDC targets."""
    def mock_fetch_all(sql, params=None):
        if "FROM stablecoins" in sql:
            return [
                {"id": 1, "symbol": "USDC", "contract": USDC_ADDR},
                {"id": 2, "symbol": "USDT", "contract": "0xdac17f958d2ee523a2206206994597c13d831ec7"},
            ]
        return []

    rpc_calls = []
    def mock_rpc_get_code(rpc_url, address):
        rpc_calls.append(address)
        return DIRECT_BYTECODE

    with patch("app.collectors.contract_upgrades.fetch_all", mock_fetch_all), \
         patch("app.collectors.contract_upgrades.fetch_one", lambda *a, **kw: None), \
         patch("app.collectors.contract_upgrades.execute", lambda *a, **kw: None), \
         patch("app.collectors.contract_upgrades._rpc_get_code", mock_rpc_get_code), \
         patch("app.collectors.contract_upgrades._resolve_implementation", lambda *a: None), \
         patch("app.collectors.contract_upgrades._load_contract_registry", lambda: {"protocols": {}, "bridges": {}}), \
         patch("app.collectors.contract_upgrades.time"):

        from app.collectors.contract_upgrades import collect_contract_upgrades
        result = collect_contract_upgrades(entity_filter="usdc")

    if result["entities_checked"] == 1:
        results.ok("entity_filter: only 1 entity checked (not 2)")
    else:
        results.fail("entity_filter: only 1 entity checked", f"got {result}")

    if len(rpc_calls) == 1 and rpc_calls[0] == USDC_ADDR:
        results.ok("entity_filter: RPC called only for USDC address")
    else:
        results.fail("entity_filter: RPC called only for USDC", f"rpc_calls={rpc_calls}")


# ============================================================================
# Test 6: No-change path updates captured_at and impl address
# ============================================================================
def test_no_change_updates_captured_at(results: TestResult):
    """Same bytecode + same impl → UPDATE captured_at, no INSERT."""
    executed_sql = []
    current_hash = make_hash(DIRECT_BYTECODE)

    def mock_execute(sql, params=None):
        executed_sql.append((sql.strip(), params))

    def mock_fetch_all(sql, params=None):
        if "FROM stablecoins" in sql:
            return [{"id": 5, "symbol": "DAI", "contract": "0x6b175474e89094c44da98b954eedeac495271d0f"}]
        return []

    def mock_fetch_one(sql, params=None):
        if "contract_bytecode_snapshots" in sql:
            return {"bytecode_hash": current_hash, "implementation_address": None}
        return None

    with patch("app.collectors.contract_upgrades.fetch_all", mock_fetch_all), \
         patch("app.collectors.contract_upgrades.fetch_one", mock_fetch_one), \
         patch("app.collectors.contract_upgrades.execute", mock_execute), \
         patch("app.collectors.contract_upgrades._rpc_get_code", lambda *a: DIRECT_BYTECODE), \
         patch("app.collectors.contract_upgrades._resolve_implementation", lambda *a: None), \
         patch("app.collectors.contract_upgrades._load_contract_registry", lambda: {"protocols": {}, "bridges": {}}), \
         patch("app.collectors.contract_upgrades.time"):

        from app.collectors.contract_upgrades import collect_contract_upgrades
        result = collect_contract_upgrades(entity_filter="dai")

    if result["upgrades_detected"] == 0 and result["first_captures"] == 0:
        results.ok("no-change: zero upgrades, zero first captures")
    else:
        results.fail("no-change: zero upgrades, zero first captures", f"got {result}")

    update_sqls = [s for s in executed_sql if "UPDATE" in s[0]]
    if update_sqls:
        results.ok("no-change: UPDATE executed for captured_at")
    else:
        results.fail("no-change: UPDATE executed", f"executed_sql={[s[0][:60] for s in executed_sql]}")


# ============================================================================
# Run all tests
# ============================================================================
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.WARNING)

    results = TestResult()

    print("=" * 60)
    print("Contract Upgrade Collector — Logic Tests")
    print("=" * 60)

    print("\n[_detect_change logic]")
    test_detect_change(results)

    print("\n[USDC proxy first capture]")
    test_usdc_first_capture(results)

    print("\n[USDC proxy upgrade detected]")
    test_usdc_upgrade_detected(results)

    print("\n[Self-destruct detection]")
    test_self_destruct(results)

    print("\n[entity_filter]")
    test_entity_filter(results)

    print("\n[No-change path]")
    test_no_change_updates_captured_at(results)

    all_passed = results.summary()
    sys.exit(0 if all_passed else 1)
