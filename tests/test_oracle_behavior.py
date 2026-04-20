"""
Isolated tests for Pipeline 10 (Oracle Behavioral Record) pre-stress tagging.

Mocks the database layer with an in-memory simulation so the test runs
without a real Postgres. The function under test is
`tag_pre_stress_readings`, which retroactively tags readings in the 72
hours preceding a newly-opened stress event.

Tests:
1. 100 hourly readings + stress event at now → exactly 72 readings tagged
   (the first 72 hours of prior data), 28 untagged (older than window)
2. Tagging is idempotent — a second call against the same event does not
   double-tag (pre_stress_event_id IS NULL guard)
3. A reading that belongs to a different oracle address is not tagged
4. The event row's pre_stress_readings_tagged counter is updated
5. Tagging failure does not raise (never blocks stress event open)
"""
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Stub heavy optional deps so the module under test imports cleanly without
# the prod runtime environment. We only exercise pure-Python logic here.
if "httpx" not in sys.modules:
    _httpx = types.ModuleType("httpx")

    class _AsyncClient:  # minimal surface used by oracle_behavior top-level imports
        def __init__(self, *a, **k):
            pass

    _httpx.AsyncClient = _AsyncClient
    sys.modules["httpx"] = _httpx

if "app.database" not in sys.modules:
    _dbmod = types.ModuleType("app.database")
    _dbmod.fetch_all = lambda *a, **k: []
    _dbmod.fetch_one = lambda *a, **k: None
    _dbmod.execute = lambda *a, **k: None

    from contextlib import contextmanager

    @contextmanager
    def _noop_cursor(dict_cursor=False):
        raise RuntimeError("get_cursor must be patched by the test")
        yield  # unreachable

    _dbmod.get_cursor = _noop_cursor
    sys.modules["app.database"] = _dbmod


# ---------------------------------------------------------------------------
# In-memory database simulation
# ---------------------------------------------------------------------------

class FakeCursor:
    """Minimal psycopg2-like cursor that operates on in-memory tables."""

    def __init__(self, db):
        self.db = db
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, sql, params=None):
        sql_stripped = " ".join(sql.split()).upper()
        if sql_stripped.startswith("UPDATE ORACLE_PRICE_READINGS"):
            event_id, oracle_addr, chain, asset, event_start, window_hours_str, event_start_2 = params
            window_hours = int(window_hours_str)
            window_start = event_start - timedelta(hours=window_hours)
            tagged = 0
            for r in self.db["readings"]:
                if (r["oracle_address"] == oracle_addr
                        and r["chain"] == chain
                        and r["asset_symbol"] == asset
                        and r["recorded_at"] >= window_start
                        and r["recorded_at"] < event_start
                        and r["pre_stress_event_id"] is None):
                    r["pre_stress_event_id"] = event_id
                    tagged += 1
            self.rowcount = tagged
        elif sql_stripped.startswith("UPDATE ORACLE_STRESS_EVENTS"):
            tagged, event_id = params
            for e in self.db["events"]:
                if e["id"] == event_id:
                    e["pre_stress_readings_tagged"] = tagged
            self.rowcount = 1


class FakeDB:
    def __init__(self):
        self.readings = []
        self.events = []

    def cursor(self):
        return FakeCursor({"readings": self.readings, "events": self.events})


def _make_readings(oracle_addr, chain, asset, now, count, step_hours=1):
    """Generate `count` hourly readings ending just before `now`."""
    readings = []
    for i in range(count):
        readings.append({
            "id": i + 1,
            "oracle_address": oracle_addr,
            "chain": chain,
            "asset_symbol": asset,
            "recorded_at": now - timedelta(hours=(i + 1) * step_hours),
            "pre_stress_event_id": None,
        })
    return readings


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def _get_tagger():
    """Import tag_pre_stress_readings after patching its DB helpers."""
    from app.collectors import oracle_behavior
    return oracle_behavior.tag_pre_stress_readings


def _run_tag(db, **kwargs):
    """Invoke tag_pre_stress_readings with get_cursor patched to use our fake db."""
    from contextlib import contextmanager

    @contextmanager
    def fake_get_cursor(dict_cursor=False):
        yield db.cursor()

    with patch("app.collectors.oracle_behavior.get_cursor", fake_get_cursor):
        return _get_tagger()(**kwargs)


def test_exactly_72_of_100_readings_tagged():
    """The user's acceptance test:
    100 hourly readings, stress event at now, window = 72h →
    the 72 most recent readings (within the window) are tagged;
    the 28 older ones are not.
    """
    oracle_addr = "0x" + "a" * 40
    chain = "ethereum"
    asset = "ETH"
    now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)

    db = FakeDB()
    db.readings = _make_readings(oracle_addr, chain, asset, now, count=100)
    db.events.append({
        "id": 42,
        "oracle_address": oracle_addr,
        "chain": chain,
        "asset_symbol": asset,
        "event_start": now,
        "event_end": None,
        "pre_stress_readings_tagged": None,
    })

    tagged_count = _run_tag(
        db,
        event_id=42,
        oracle_address=oracle_addr,
        chain=chain,
        asset_symbol=asset,
        event_start=now,
        window_hours=72,
    )

    assert tagged_count == 72, f"expected 72 tagged, got {tagged_count}"

    tagged = [r for r in db.readings if r["pre_stress_event_id"] == 42]
    untagged = [r for r in db.readings if r["pre_stress_event_id"] is None]

    assert len(tagged) == 72
    assert len(untagged) == 28

    # The 72 tagged readings are the 72 most-recent (closest to event_start).
    tagged_hours_ago = sorted(
        round((now - r["recorded_at"]).total_seconds() / 3600) for r in tagged
    )
    assert tagged_hours_ago[0] == 1   # oldest tagged = 1h before now
    assert tagged_hours_ago[-1] == 72  # newest tagged = 72h before now

    # The 28 untagged are older than 72h
    untagged_hours_ago = sorted(
        round((now - r["recorded_at"]).total_seconds() / 3600) for r in untagged
    )
    assert untagged_hours_ago[0] == 73
    assert untagged_hours_ago[-1] == 100

    # Counter was written back onto the event row
    assert db.events[0]["pre_stress_readings_tagged"] == 72


def test_idempotent_no_double_tag():
    """Second call against the same event tags zero additional rows."""
    oracle_addr = "0x" + "b" * 40
    now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)

    db = FakeDB()
    db.readings = _make_readings(oracle_addr, "ethereum", "USDC", now, count=80)
    db.events.append({
        "id": 7, "oracle_address": oracle_addr, "chain": "ethereum",
        "asset_symbol": "USDC", "event_start": now, "event_end": None,
        "pre_stress_readings_tagged": None,
    })

    first = _run_tag(db, event_id=7, oracle_address=oracle_addr, chain="ethereum",
                     asset_symbol="USDC", event_start=now, window_hours=72)
    second = _run_tag(db, event_id=7, oracle_address=oracle_addr, chain="ethereum",
                      asset_symbol="USDC", event_start=now, window_hours=72)

    assert first == 72
    assert second == 0, "re-tagging should be a no-op"


def test_other_oracle_not_tagged():
    """Readings from a different oracle address are not tagged."""
    now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)
    target = "0x" + "c" * 40
    other = "0x" + "d" * 40

    db = FakeDB()
    db.readings = _make_readings(target, "ethereum", "ETH", now, count=50)
    db.readings += _make_readings(other, "ethereum", "ETH", now, count=50)
    db.events.append({
        "id": 99, "oracle_address": target, "chain": "ethereum",
        "asset_symbol": "ETH", "event_start": now, "event_end": None,
        "pre_stress_readings_tagged": None,
    })

    tagged = _run_tag(db, event_id=99, oracle_address=target, chain="ethereum",
                      asset_symbol="ETH", event_start=now, window_hours=72)

    assert tagged == 50, "all 50 target readings within window should tag"
    for r in db.readings:
        if r["oracle_address"] == other:
            assert r["pre_stress_event_id"] is None


def test_tags_all_available_when_fewer_than_72_prior_readings():
    """Edge case: only 40 prior readings exist. The tagger should tag
    all 40 without erroring, not try to reach 72 that aren't there."""
    oracle_addr = "0x" + "f" * 40
    now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)

    db = FakeDB()
    db.readings = _make_readings(oracle_addr, "ethereum", "DAI", now, count=40)
    db.events.append({
        "id": 11, "oracle_address": oracle_addr, "chain": "ethereum",
        "asset_symbol": "DAI", "event_start": now, "event_end": None,
        "pre_stress_readings_tagged": None,
    })

    tagged = _run_tag(db, event_id=11, oracle_address=oracle_addr, chain="ethereum",
                      asset_symbol="DAI", event_start=now, window_hours=72)

    assert tagged == 40, f"expected 40 tagged (all available), got {tagged}"
    assert all(r["pre_stress_event_id"] == 11 for r in db.readings)
    assert db.events[0]["pre_stress_readings_tagged"] == 40


def test_tagging_never_raises_on_db_error():
    """A DB exception inside tagging is swallowed, returning 0.
    This guarantees stress-event opens never fail because of the tag step."""
    from contextlib import contextmanager

    @contextmanager
    def broken_cursor(dict_cursor=False):
        raise RuntimeError("simulated DB outage")
        yield  # unreachable

    with patch("app.collectors.oracle_behavior.get_cursor", broken_cursor):
        from app.collectors.oracle_behavior import tag_pre_stress_readings
        result = tag_pre_stress_readings(
            event_id=1, oracle_address="0x" + "e" * 40,
            chain="ethereum", asset_symbol="USDC",
            event_start=datetime.now(timezone.utc),
        )
    assert result == 0, "tagger must swallow DB errors and return 0"


if __name__ == "__main__":
    test_exactly_72_of_100_readings_tagged()
    test_idempotent_no_double_tag()
    test_other_oracle_not_tagged()
    test_tags_all_available_when_fewer_than_72_prior_readings()
    test_tagging_never_raises_on_db_error()
    print("ALL TESTS PASS")
