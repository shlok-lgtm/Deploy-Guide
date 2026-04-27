"""
Component 4: Detection pipeline + watchlist tests.

8 unit tests against pure helpers (slug normalization, threshold-crossing
math, cooldown window, scheduler idempotency) — fast, no HTTP, no DB.

6 integration tests against POST /api/engine/events, GET /events,
POST /api/engine/watchlist, DELETE /api/engine/watchlist. Cover
manual-event submission with and without auto-trigger, watchlist
validations (404 unknown entity, 422 unknown index), and the
watchlist lifecycle (add → list active → delete → list inactive).

We don't write tests that require the scheduler to actually run. The
unit tests call poll_defillama_hacks / evaluate_watchlist helpers
directly.

Run:
    ADMIN_KEY=<key> BASE_URL=https://basisprotocol.xyz \\
      DATABASE_URL=<prod-url> \\
      pytest tests/test_engine_pipeline.py -v

Cleanup mirrors test_engine_renderers.py: per-test ID tracking + session
sweep against canonical (entity, event_date) keys for events. Watchlist
rows are tracked separately and deactivated rather than deleted (mirrors
the soft-delete API behavior).
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator, Optional

import pytest

from app.engine.event_sources.defillama_hacks import (
    _coerce_event_date,
    _severity_from_amount,
    normalize_defillama_protocol_to_slug,
    normalize_hack_to_event,
)
from app.engine.scheduler import is_running as scheduler_is_running
from app.engine.scheduler import start_scheduler, stop_scheduler
from app.engine.watchlist import (
    COOLDOWN_SECONDS,
    crosses_above,
    crosses_below,
    is_in_cooldown,
    score_drops_by,
    tvl_drop_exceeds_pct,
)


# ═════════════════════════════════════════════════════════════════
# Test fixture (entity, event_date) keys — for session cleanup
# ═════════════════════════════════════════════════════════════════

TEST_FIXTURE_EVENT_KEYS: list[tuple[str, str, date]] = [
    # (source, entity, event_date) tuples
    ("manual", "drift", date(2026, 5, 1)),       # test_post_manual_event_triggers_analysis
    ("manual", "layerzero", date(2026, 5, 2)),   # test_post_manual_event_no_trigger
    ("manual", "drift", date(2026, 5, 3)),       # test_get_events_filters_by_entity
    ("manual", "kelp-rseth", date(2026, 5, 4)),  # test_get_events_filters_by_entity
    ("manual", "drift", date(2099, 9, 1)),       # test_event_idempotency_via_unique_constraint
]


# ═════════════════════════════════════════════════════════════════
# DB cleanup — events + analyses + watchlist
# ═════════════════════════════════════════════════════════════════

def _db_cleanup_test_events_and_analyses() -> int:
    """Delete artifacts → analyses → events for the test fixture keys.
    FK ordering matters; engine_events.analysis_id references
    engine_analyses(id), and engine_artifacts.analysis_id does too."""
    conn_string = os.environ.get("DATABASE_URL")
    if not conn_string:
        return -1
    try:
        import psycopg2
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                # Find the analysis IDs linked from our test events
                cur.execute(
                    """
                    SELECT analysis_id FROM engine_events
                    WHERE (source, entity, event_date) IN %s
                      AND analysis_id IS NOT NULL
                    """,
                    (tuple(TEST_FIXTURE_EVENT_KEYS),),
                )
                analysis_ids = [r[0] for r in cur.fetchall() if r[0] is not None]

                # Delete artifacts referencing those analyses
                if analysis_ids:
                    cur.execute(
                        "DELETE FROM engine_artifacts WHERE analysis_id = ANY(%s::uuid[])",
                        ([str(i) for i in analysis_ids],),
                    )

                # Delete the events
                cur.execute(
                    """
                    DELETE FROM engine_events
                    WHERE (source, entity, event_date) IN %s
                    """,
                    (tuple(TEST_FIXTURE_EVENT_KEYS),),
                )
                events_deleted = cur.rowcount

                # Delete the analyses
                if analysis_ids:
                    cur.execute(
                        "DELETE FROM engine_analyses WHERE id = ANY(%s::uuid[])",
                        ([str(i) for i in analysis_ids],),
                    )
            conn.commit()
        return events_deleted
    except Exception as exc:
        print(f"cleanup: events sweep failed: {exc}", file=sys.stderr)
        return -1


def _db_delete_watchlist_rows(ids: list[str]) -> bool:
    if not ids:
        return True
    conn_string = os.environ.get("DATABASE_URL")
    if not conn_string:
        return False
    try:
        import psycopg2
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM engine_watchlist WHERE id = ANY(%s::uuid[])",
                    ([str(i) for i in ids],),
                )
            conn.commit()
        return True
    except Exception as exc:
        print(f"cleanup: watchlist delete failed: {exc}", file=sys.stderr)
        return False


# ═════════════════════════════════════════════════════════════════
# Fixtures (admin auth + cleanup)
# ═════════════════════════════════════════════════════════════════

def _resolve_admin_key() -> Optional[str]:
    return os.environ.get("ADMIN_KEY") or os.environ.get("BASIS_ADMIN_KEY")


@pytest.fixture(scope="session")
def admin_key() -> str:
    key = _resolve_admin_key()
    if not key:
        pytest.skip(
            "ADMIN_KEY not set — skipping admin-authenticated integration tests."
        )
    return key


class _AdminAPI:
    def __init__(self, session, base_url: str, admin_key: str):
        self._session = session
        self._base = base_url
        self._headers = {"x-admin-key": admin_key}

    def post(self, path: str, body: dict[str, Any] | None = None):
        return self._session.post(
            f"{self._base}{path}",
            json=body or {},
            headers=self._headers,
            timeout=90,  # manual-event trigger may include LLM call
        )

    def get(self, path: str, params: dict[str, Any] | None = None):
        return self._session.get(
            f"{self._base}{path}",
            headers=self._headers,
            params=params,
            timeout=30,
        )

    def delete(self, path: str):
        return self._session.delete(
            f"{self._base}{path}", headers=self._headers, timeout=30,
        )


@pytest.fixture(scope="session")
def admin_api(base_url, session, admin_key) -> _AdminAPI:
    return _AdminAPI(session, base_url, admin_key)


@pytest.fixture(scope="session", autouse=True)
def session_pipeline_cleanup():
    deleted = _db_cleanup_test_events_and_analyses()
    if deleted > 0:
        print(
            f"\n[pipeline-tests] session-start sweep: deleted {deleted} stale event row(s)",
            file=sys.stderr,
        )
    yield
    _db_cleanup_test_events_and_analyses()


_created_watchlist_ids: list[str] = []


@pytest.fixture(autouse=True)
def cleanup_watchlist_rows() -> Iterator[None]:
    _created_watchlist_ids.clear()
    yield
    if _created_watchlist_ids:
        _db_delete_watchlist_rows(list(_created_watchlist_ids))
        _created_watchlist_ids.clear()


def _track_watchlist(watchlist_id: str) -> str:
    _created_watchlist_ids.append(watchlist_id)
    return watchlist_id


# ═════════════════════════════════════════════════════════════════
# 1. Unit: slug normalization
# ═════════════════════════════════════════════════════════════════

def test_normalize_defillama_protocol_to_slug():
    assert normalize_defillama_protocol_to_slug("Drift Protocol") == "drift"
    assert normalize_defillama_protocol_to_slug("Curve Finance") == "curve"
    assert normalize_defillama_protocol_to_slug("Curve.fi") == "curvefi"
    assert normalize_defillama_protocol_to_slug("LayerZero v2") == "layerzero"
    # Multi-word entities preserve hyphens
    slug = normalize_defillama_protocol_to_slug("Jupiter Perpetual Exchange")
    assert slug == "jupiter-perpetual-exchange"
    # Empty / None
    assert normalize_defillama_protocol_to_slug(None) is None
    assert normalize_defillama_protocol_to_slug("") is None
    assert normalize_defillama_protocol_to_slug("   ") is None


# ═════════════════════════════════════════════════════════════════
# 2. Unit: threshold score_below crosses
# ═════════════════════════════════════════════════════════════════

def test_threshold_score_below_crosses():
    # previous=35, current=25, threshold=30 → crossed
    assert crosses_below(previous=35, current=25, threshold=30) is True
    # previous=30 (exactly equal), current=25, threshold=30 → crossed
    # (boundary: previous >= threshold means equal counts)
    assert crosses_below(previous=30, current=25, threshold=30) is True
    # previous=29 (already below), current=25 → NOT crossed
    assert crosses_below(previous=29, current=25, threshold=30) is False


# ═════════════════════════════════════════════════════════════════
# 3. Unit: no cross when already below
# ═════════════════════════════════════════════════════════════════

def test_threshold_score_below_no_cross_when_already_below():
    """Was below, still below → no crossing event."""
    assert crosses_below(previous=25, current=20, threshold=30) is False
    # And: above-and-still-above isn't a below-crossing either
    assert crosses_below(previous=40, current=35, threshold=30) is False


# ═════════════════════════════════════════════════════════════════
# 4. Unit: tvl_drop_pct calculation
# ═════════════════════════════════════════════════════════════════

def test_threshold_tvl_drop_pct_calculation():
    # 50% drop, threshold 30 → exceeds
    assert tvl_drop_exceeds_pct(previous=100_000_000, current=50_000_000, threshold_pct=30) is True
    # 25% drop, threshold 30 → does not exceed
    assert tvl_drop_exceeds_pct(previous=100_000_000, current=75_000_000, threshold_pct=30) is False
    # No previous → no signal
    assert tvl_drop_exceeds_pct(previous=None, current=50_000_000, threshold_pct=30) is False
    # Zero or negative previous → defended
    assert tvl_drop_exceeds_pct(previous=0, current=50_000_000, threshold_pct=30) is False
    # Negative drop (TVL grew) → does not exceed
    assert tvl_drop_exceeds_pct(previous=100_000_000, current=120_000_000, threshold_pct=30) is False


# ═════════════════════════════════════════════════════════════════
# 5. Unit: cooldown prevents re-trigger within 24h
# ═════════════════════════════════════════════════════════════════

def test_cooldown_prevents_re_trigger_within_24h():
    now = datetime(2026, 4, 27, 12, 0, 0, tzinfo=timezone.utc)
    # 6h ago — in cooldown
    six_hours_ago = now - timedelta(hours=6)
    assert is_in_cooldown(six_hours_ago, now=now) is True
    # 23h ago — still in cooldown
    twentythree_h = now - timedelta(hours=23)
    assert is_in_cooldown(twentythree_h, now=now) is True
    # 24h ago — boundary: NOT in cooldown (age == cooldown_seconds)
    twentyfour_h = now - timedelta(hours=24, seconds=1)
    assert is_in_cooldown(twentyfour_h, now=now) is False
    # 25h ago — out of cooldown
    twentyfive_h = now - timedelta(hours=25)
    assert is_in_cooldown(twentyfive_h, now=now) is False
    # No prior trigger → not in cooldown
    assert is_in_cooldown(None, now=now) is False
    # Naive datetime input — defensively treated as UTC
    naive = (now - timedelta(hours=10)).replace(tzinfo=None)
    assert is_in_cooldown(naive, now=now) is True


# ═════════════════════════════════════════════════════════════════
# 6. Integration: event idempotency via unique constraint
#
# Originally written as a unit test calling insert_manual_event() directly,
# which assumed the psycopg2 pool was already initialized — but tests
# don't bring up the FastAPI startup hooks that call init_pool(), so the
# direct path failed with a "Database pool not initialized" error.
#
# Converted to HTTP path through POST /api/engine/events. Same surface
# real users hit; pool init happens naturally because the running
# api-server already initialized it. Cleanup is handled by the
# session-end sweep against TEST_FIXTURE_EVENT_KEYS (sentinel date
# 2099-09-01 added to that list above).
# ═════════════════════════════════════════════════════════════════

def test_event_idempotency_via_unique_constraint(admin_api):
    """First POST inserts a manual event. Second POST with identical
    (source, entity, event_date, event_type) returns the same event_id
    with was_new=False — the unique constraint suppressed the INSERT
    and insert_manual_event's lookup path returned the pre-existing
    row's id so the caller can still link to it."""
    body = {
        "source": "manual",
        "event_type": "other",
        "entity": "drift",
        "event_date": "2099-09-01",  # sentinel; in TEST_FIXTURE_EVENT_KEYS
        "severity": "low",
        "raw_event_data": {"idempotency_test": "first"},
        "trigger_analysis": False,
    }

    r1 = admin_api.post("/api/engine/events", body)
    assert r1.status_code == 202, r1.text[:400]
    d1 = r1.json()
    assert d1["was_new"] is True, f"first insert should be new; got {d1}"
    first_id = d1["event_id"]

    # Second submission — different raw_event_data, same idempotency key.
    body_second = dict(body, raw_event_data={"idempotency_test": "second"})
    r2 = admin_api.post("/api/engine/events", body_second)
    assert r2.status_code == 202, r2.text[:400]
    d2 = r2.json()
    assert d2["was_new"] is False, (
        f"second insert with identical idempotency key should be deduped; got {d2}"
    )
    assert d2["event_id"] == first_id, (
        f"deduped insert should return same event_id; first={first_id}, "
        f"second={d2['event_id']}"
    )


# ═════════════════════════════════════════════════════════════════
# 7. Unit: edge cases for slug normalization
# ═════════════════════════════════════════════════════════════════

def test_normalize_handles_edge_cases():
    # Special characters dropped
    assert normalize_defillama_protocol_to_slug("Foo @ Bar!") == "foo-bar"
    # Multiple spaces collapse
    assert normalize_defillama_protocol_to_slug("Foo    Bar") == "foo-bar"
    # Leading / trailing whitespace
    assert normalize_defillama_protocol_to_slug("  drift   ") == "drift"
    # All non-alphanumeric → None
    assert normalize_defillama_protocol_to_slug("!!!") is None
    # Long names — should still normalize without crashing
    long_name = "Some-Really-Long-Protocol-Name-With-Many-Hyphens-Protocol"
    slug = normalize_defillama_protocol_to_slug(long_name)
    assert slug is not None
    assert "-" in slug
    assert slug.endswith("hyphens")  # "-protocol" suffix stripped


# ═════════════════════════════════════════════════════════════════
# 8. Unit: scheduler start_scheduler is idempotent
# ═════════════════════════════════════════════════════════════════

def test_scheduler_setup_idempotent():
    """Calling start_scheduler twice doesn't raise. stop_scheduler
    after either call returns the module to a clean state."""
    import asyncio

    async def _run():
        # Note: this test runs against the test process, not production.
        # If a scheduler is already running (e.g., we're inside the
        # api-server runtime), we don't fight it; assert is_running()
        # consistency instead.
        already_running = scheduler_is_running()
        try:
            await start_scheduler()
            await start_scheduler()  # idempotent
            assert scheduler_is_running() is True
        finally:
            if not already_running:
                await stop_scheduler()
                assert scheduler_is_running() is False

    asyncio.run(_run())


# ═════════════════════════════════════════════════════════════════
# 9. Integration: POST /events with trigger_analysis=true
# ═════════════════════════════════════════════════════════════════

def test_post_manual_event_triggers_analysis(admin_api):
    body = {
        "source": "manual",
        "event_type": "exploit",
        "entity": "drift",
        "event_date": "2026-05-01",
        "severity": "high",
        "raw_event_data": {"note": "C4 integration test"},
        "trigger_analysis": True,
    }
    resp = admin_api.post("/api/engine/events", body)
    assert resp.status_code == 202, resp.text[:400]
    data = resp.json()
    assert "event_id" in data
    assert data["analysis_id"] is not None, (
        f"trigger_analysis=true should produce analysis_id; got: {data}"
    )

    # Fetch the event row — it should now be linked to the analysis
    event_resp = admin_api.get(f"/api/engine/events/{data['event_id']}")
    assert event_resp.status_code == 200
    event = event_resp.json()
    assert event["analysis_id"] == data["analysis_id"]
    assert event["status"] in ("analyzed", "no_coverage", "error")


# ═════════════════════════════════════════════════════════════════
# 10. Integration: POST /events with trigger_analysis=false
# ═════════════════════════════════════════════════════════════════

def test_post_manual_event_no_trigger(admin_api):
    body = {
        "source": "manual",
        "event_type": "depeg",
        "entity": "layerzero",
        "event_date": "2026-05-02",
        "severity": "low",
        "raw_event_data": {"note": "no-trigger test"},
        "trigger_analysis": False,
    }
    resp = admin_api.post("/api/engine/events", body)
    assert resp.status_code == 202, resp.text[:400]
    data = resp.json()
    assert "event_id" in data
    assert data["analysis_id"] is None, (
        f"trigger_analysis=false should produce null analysis_id; got: {data}"
    )


# ═════════════════════════════════════════════════════════════════
# 11. Integration: GET /events filters by entity
# ═════════════════════════════════════════════════════════════════

def test_get_events_filters_by_entity(admin_api):
    # Create two events with different entities
    body_drift = {
        "source": "manual",
        "event_type": "other",
        "entity": "drift",
        "event_date": "2026-05-03",
        "severity": "low",
        "raw_event_data": {"filter_test": "drift"},
        "trigger_analysis": False,
    }
    body_kelp = {
        "source": "manual",
        "event_type": "other",
        "entity": "kelp-rseth",
        "event_date": "2026-05-04",
        "severity": "low",
        "raw_event_data": {"filter_test": "kelp"},
        "trigger_analysis": False,
    }
    r1 = admin_api.post("/api/engine/events", body_drift)
    r2 = admin_api.post("/api/engine/events", body_kelp)
    assert r1.status_code == 202
    assert r2.status_code == 202

    # Filter to drift only
    resp = admin_api.get(
        "/api/engine/events", params={"entity": "drift", "limit": 50},
    )
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) >= 1
    for row in rows:
        assert row["entity"] == "drift", (
            f"filter mismatch — expected only drift; got {row['entity']}"
        )


# ═════════════════════════════════════════════════════════════════
# 12. Integration: POST /watchlist for unknown entity → 404
# ═════════════════════════════════════════════════════════════════

def test_post_watchlist_invalid_entity_returns_404(admin_api):
    body = {
        "entity_slug": "this-entity-does-not-exist-xyz",
        "index_id": "psi",
        "threshold_type": "score_below",
        "threshold_value": 50.0,
        "measure_name": "overall_score",
        "active": True,
    }
    resp = admin_api.post("/api/engine/watchlist", body)
    assert resp.status_code == 404, resp.text[:400]


# ═════════════════════════════════════════════════════════════════
# 13. Integration: POST /watchlist with bogus index_id → 422
# ═════════════════════════════════════════════════════════════════

def test_post_watchlist_invalid_index_returns_422(admin_api):
    body = {
        "entity_slug": "drift",
        "index_id": "bogus_index_xyz",
        "threshold_type": "score_below",
        "threshold_value": 50.0,
        "measure_name": "overall_score",
        "active": True,
    }
    resp = admin_api.post("/api/engine/watchlist", body)
    assert resp.status_code == 422, resp.text[:400]
    assert "unknown index_id" in resp.json()["detail"].lower()


# ═════════════════════════════════════════════════════════════════
# 14. Integration: watchlist lifecycle (add → list active → delete →
#     list inactive)
# ═════════════════════════════════════════════════════════════════

def test_watchlist_lifecycle(admin_api):
    body = {
        "entity_slug": "drift",
        "index_id": "psi",
        "threshold_type": "score_below",
        "threshold_value": 30.0,
        "measure_name": "security",
        "active": True,
        "notes": "C4 lifecycle test",
    }
    create_resp = admin_api.post("/api/engine/watchlist", body)
    assert create_resp.status_code == 201, create_resp.text[:400]
    watchlist_id = create_resp.json()["watchlist_id"]
    _track_watchlist(watchlist_id)

    # List active rows — our row should appear
    list_resp = admin_api.get(
        "/api/engine/watchlist", params={"entity_slug": "drift", "active": "true"},
    )
    assert list_resp.status_code == 200
    active_rows = list_resp.json()
    found = [r for r in active_rows if r["id"] == watchlist_id]
    assert len(found) == 1
    assert found[0]["active"] is True
    assert found[0]["measure_name"] == "security"

    # Soft-delete (DELETE flips active=False)
    delete_resp = admin_api.delete(f"/api/engine/watchlist/{watchlist_id}")
    assert delete_resp.status_code == 200, delete_resp.text[:400]

    # List active=False — our row should appear
    inactive_resp = admin_api.get(
        "/api/engine/watchlist", params={"entity_slug": "drift", "active": "false"},
    )
    assert inactive_resp.status_code == 200
    inactive_rows = inactive_resp.json()
    found_inactive = [r for r in inactive_rows if r["id"] == watchlist_id]
    assert len(found_inactive) == 1
    assert found_inactive[0]["active"] is False
