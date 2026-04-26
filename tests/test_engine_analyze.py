"""
Component 2a: Analysis endpoint skeleton tests.

Style: HTTP integration tests against a live server (same pattern as
tests/test_engine_coverage.py). All admin-authenticated endpoints; tests
skip cleanly if ADMIN_KEY isn't available in the environment.

Run:
    ADMIN_KEY=<key> BASE_URL=https://basisprotocol.xyz \\
      DATABASE_URL=<prod-or-replica-url> \\
      pytest tests/test_engine_analyze.py -v

DATABASE_URL is optional — if set, teardown deletes test rows directly via
psycopg2 (fast, doesn't trip rate limit, works on test failure). If unset,
teardown falls back to HTTP DELETE with a 0.5s sleep between calls so the
admin rate budget stays intact.

Rate-limit budget: with the auth-based exemption fix in app/server.py,
requests carrying a valid X-Admin-Key are exempt from rate limiting
entirely. The HTTP-DELETE fallback path still includes inter-call sleeps
in case the test is run against a server without that fix applied.

Test cleanup (two layers):
  1. Session-start orphan sweep — at the start of the pytest session,
     DELETE every (entity, event_date) row from the canonical test set.
     Catches debris from prior failed runs.
  2. Per-test cleanup — each test appends any analysis IDs it creates to
     a shared list. After each test, DELETE them by ID. Direct DB delete
     when DATABASE_URL is set; HTTP DELETE with sleep fallback otherwise.

Tests:
  1. test_analyze_drift_returns_202              — happy path
  2. test_analyze_pending_flips_to_draft         — async state machine
  3. test_analyze_missing_peer_set_returns_422   — Pydantic validation
  4. test_analyze_unknown_entity_returns_404     — coverage check
  5. test_analyze_duplicate_returns_409          — uniqueness constraint
  6. test_analyze_force_new_archives_previous    — revision chain
  6a. test_analyze_force_new_archives_previous_uuid_adapter
                                                  — psycopg2 UUID regression
  7. test_list_analyses_filters_by_entity        — list endpoint
  8. test_get_analysis_unknown_id_returns_404    — GET 404
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import date
from typing import Any, Iterator, Optional

import pytest


# ═════════════════════════════════════════════════════════════════
# Test fixture row keys
#
# Every (entity, event_date) pair used by tests in this file plus any
# operator-run diagnostic curls. The session-start orphan sweep DELETEs
# all rows matching this set so a hard test crash leaves no debris next
# session.
#
# Adding a new test that uses a new (entity, event_date)? Add it here too.
# ═════════════════════════════════════════════════════════════════

TEST_FIXTURE_KEYS: list[tuple[str, date]] = [
    ("drift", date(2026, 4, 1)),    # test_analyze_drift_returns_202
    ("drift", date(2026, 4, 2)),    # test_analyze_duplicate_returns_409
    ("drift", date(2026, 4, 3)),    # test_analyze_force_new_archives_previous
    ("kelp-rseth", date(2026, 4, 18)),  # test_analyze_pending_flips_to_draft
    ("usdc", date(2026, 4, 5)),     # uuid-adapter regression
    ("layerzero", date(2026, 4, 4)),    # test_list_analyses_filters_by_entity
    ("layerzero", date(2026, 4, 15)),   # operator diagnostic curl debris
    ("layerzero", date(2026, 4, 30)),   # operator verification curl
]


# ═════════════════════════════════════════════════════════════════
# DB connection helpers (used by cleanup; optional)
#
# psycopg2 import is local so the test module still imports cleanly when
# the host doesn't have psycopg2 installed. Cleanup degrades to HTTP-only
# in that case.
# ═════════════════════════════════════════════════════════════════

def _db_delete_by_ids(ids: list[str]) -> bool:
    """Direct DB delete by primary key. Returns True if cleanup succeeded
    (or there was nothing to clean up), False if DATABASE_URL isn't set
    or the connection/query failed — caller falls back to HTTP."""
    conn_string = os.environ.get("DATABASE_URL")
    if not conn_string:
        return False
    try:
        import psycopg2  # local import — may not be installed in the test env
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM engine_analyses WHERE id = ANY(%s::uuid[])",
                    ([str(i) for i in ids],),
                )
            conn.commit()
        return True
    except Exception as exc:
        print(f"cleanup: DB delete by id failed: {exc}", file=sys.stderr)
        return False


def _db_delete_by_fixture_keys() -> int:
    """Session-start sweep: delete every row matching the canonical test
    (entity, event_date) set. Returns the row count deleted on success,
    -1 if cleanup wasn't performed (no DATABASE_URL or connection error).
    """
    conn_string = os.environ.get("DATABASE_URL")
    if not conn_string:
        return -1
    try:
        import psycopg2
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                # Postgres tuple-IN: (entity, event_date) IN ((%s, %s), ...)
                # Use a single bound tuple-of-tuples; psycopg2 renders this
                # as a row-constructor IN list.
                cur.execute(
                    """
                    DELETE FROM engine_analyses
                    WHERE (entity, event_date) IN %s
                    """,
                    (tuple(TEST_FIXTURE_KEYS),),
                )
                deleted = cur.rowcount
            conn.commit()
        return deleted
    except Exception as exc:
        print(f"cleanup: session-start sweep failed: {exc}", file=sys.stderr)
        return -1


# ═════════════════════════════════════════════════════════════════
# Fixtures
# ═════════════════════════════════════════════════════════════════

# Accept either ADMIN_KEY (server convention) or BASIS_ADMIN_KEY (S2a
# prompt variant). Tests skip if neither is present so CI without the
# secret doesn't break the build.
def _resolve_admin_key() -> Optional[str]:
    return os.environ.get("ADMIN_KEY") or os.environ.get("BASIS_ADMIN_KEY")


@pytest.fixture(scope="session")
def admin_key() -> str:
    key = _resolve_admin_key()
    if not key:
        pytest.skip(
            "ADMIN_KEY not set — skipping admin-authenticated engine tests. "
            "Export ADMIN_KEY (or BASIS_ADMIN_KEY) to run these tests."
        )
    return key


class _AdminAPI:
    """Thin wrapper around requests.Session that injects the admin header."""

    def __init__(self, session, base_url: str, admin_key: str):
        self._session = session
        self._base = base_url
        self._headers = {"x-admin-key": admin_key}

    def post(self, path: str, body: dict[str, Any] | None = None):
        return self._session.post(
            f"{self._base}{path}",
            json=body or {},
            headers=self._headers,
            timeout=30,
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
            f"{self._base}{path}",
            headers=self._headers,
            timeout=30,
        )


@pytest.fixture(scope="session")
def admin_api(base_url, session, admin_key) -> _AdminAPI:
    return _AdminAPI(session, base_url, admin_key)


# Session-start orphan sweep — runs once per pytest session, before any
# tests in this file. Catches debris from crashed prior runs by DELETEing
# rows for every (entity, event_date) in the canonical test set.
#
# Silently skips if DATABASE_URL isn't set; per-test cleanup will do its
# best via HTTP. If the sweep fails for any other reason, the session
# continues — tests will still run, they'll just have to coexist with any
# stale rows (and 409 on duplicates).
@pytest.fixture(scope="session", autouse=True)
def session_engine_cleanup():
    deleted = _db_delete_by_fixture_keys()
    if deleted > 0:
        print(
            f"\n[engine-tests] session-start sweep: deleted {deleted} stale row(s)",
            file=sys.stderr,
        )
    yield
    # End-of-session sweep: same cleanup, in case crashes left debris.
    _db_delete_by_fixture_keys()


# Shared list of analysis IDs created during a single test. Reset per test.
_created_ids: list[str] = []


@pytest.fixture(autouse=True)
def cleanup_created_analyses(admin_api) -> Iterator[None]:
    """Per-test cleanup. Direct DB DELETE if DATABASE_URL is set;
    HTTP DELETE with sleeps as fallback. Best-effort — failures are
    logged but don't fail the test."""
    _created_ids.clear()
    yield
    if not _created_ids:
        return

    ids_to_delete = list(_created_ids)
    if _db_delete_by_ids(ids_to_delete):
        _created_ids.clear()
        return

    # Fallback: HTTP DELETE with inter-call sleep so the admin rate budget
    # stays intact even on a server without the auth-based rate-limit
    # exemption. With the exemption applied, the sleep is unnecessary but
    # harmless.
    for aid in ids_to_delete:
        try:
            admin_api.delete(f"/api/engine/analyses/{aid}")
            time.sleep(0.5)
        except Exception as exc:
            print(f"cleanup: HTTP delete failed for {aid}: {exc}", file=sys.stderr)
    _created_ids.clear()


def _track(analysis_id: str) -> str:
    """Record an analysis id for post-test cleanup and pass it through."""
    _created_ids.append(analysis_id)
    return analysis_id


# ═════════════════════════════════════════════════════════════════
# 1. Happy path — POST returns 202 with pending status
# ═════════════════════════════════════════════════════════════════

def test_analyze_drift_returns_202(admin_api):
    resp = admin_api.post(
        "/api/engine/analyze",
        {
            "entity": "drift",
            "event_date": "2026-04-01",
            "peer_set": ["jupiter-perpetual-exchange"],
        },
    )
    assert resp.status_code == 202, resp.text[:400]
    data = resp.json()

    assert "analysis_id" in data
    assert data["status"] == "pending"
    assert data["entity"] == "drift"
    assert data["poll_url"] == f"/api/engine/analyses/{data['analysis_id']}"

    # UUID format check
    uuid.UUID(data["analysis_id"])
    _track(data["analysis_id"])


# ═════════════════════════════════════════════════════════════════
# 2. Async state machine — pending flips to draft after ~2s
# ═════════════════════════════════════════════════════════════════

def test_analyze_pending_flips_to_draft(admin_api):
    resp = admin_api.post(
        "/api/engine/analyze",
        {
            "entity": "kelp-rseth",
            "event_date": "2026-04-18",
            "peer_set": [],
        },
    )
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])

    # Background task: ~2s scheduler delay + up to ~15s for the LLM
    # roundtrip in S2c. Poll until the status leaves pending or we
    # hit the 30s ceiling.
    deadline = time.time() + 30.0
    analysis: dict = {}
    while time.time() < deadline:
        get_resp = admin_api.get(f"/api/engine/analyses/{aid}")
        assert get_resp.status_code == 200, get_resp.text[:400]
        analysis = get_resp.json()
        if analysis.get("status") != "pending":
            break
        time.sleep(0.7)

    assert analysis.get("status") == "draft", (
        f"status should have flipped to draft within 30s, still: "
        f"{analysis.get('status')}"
    )

    # Real LLM interpretation tags (S2c). Fallback path is template:fallback
    # when the API is unavailable; accept both, skip the strict checks if
    # the production server fell back so the operator sees the cause without
    # CI failing on a known-degraded path.
    interp = analysis["interpretation"]
    assert interp["model_id"] in ("claude-sonnet-4-6", "template:fallback"), (
        f"unexpected model_id: {interp['model_id']!r}"
    )
    if interp["model_id"] == "template:fallback":
        pytest.skip(
            f"LLM unavailable (fallback returned). Reason: "
            f"{interp.get('confidence_reasoning')!r}. Set ANTHROPIC_API_KEY "
            "and ensure budget headroom, then re-run."
        )
    assert interp["prompt_version"] == "v1"
    # confidence is one of the four valid levels — actual value depends on
    # production data and the LLM's call; don't pin it.
    assert interp["confidence"] in ("high", "medium", "low", "insufficient")

    # Stage stamp: bumped from v0.1-s2a-stub when S2b started populating real
    # signal data; bumped again to v0.1-s2c when LLM interpretation landed.
    assert analysis["analysis_version"] == "v0.1-s2c-llm-interpretation"
    # Signal is populated (S2b shipped real observations). kelp-rseth has
    # live LSTI coverage with deep history, so at least one event window has
    # observations. Don't assert exact counts — production data evolves.
    signal = analysis["signal"]
    assert signal["baseline"] == [], (
        "baseline must be empty when event_date is set (schema invariant)"
    )
    total_event_obs = (
        len(signal["pre_event"])
        + len(signal["event_window"])
        + len(signal["post_event"])
    )
    assert total_event_obs > 0, (
        f"expected populated signal post-S2b for kelp-rseth/2026-04-18; "
        f"got pre={len(signal['pre_event'])}, "
        f"event={len(signal['event_window'])}, "
        f"post={len(signal['post_event'])}"
    )
    # Recommendation still blocks all artifact types — stays stub through S2c
    assert analysis["artifact_recommendation"]["recommended"] == "nothing"


# ═════════════════════════════════════════════════════════════════
# 3. Pydantic validation — missing peer_set → 422
# ═════════════════════════════════════════════════════════════════

def test_analyze_missing_peer_set_returns_422(admin_api):
    resp = admin_api.post(
        "/api/engine/analyze",
        {"entity": "drift", "event_date": "2026-04-01"},
    )
    assert resp.status_code == 422, resp.text[:400]
    body = resp.json()
    # FastAPI's 422 payload has a "detail" list of validation errors
    assert "detail" in body
    # Confirm peer_set is the missing field flagged
    missing_fields = {tuple(err["loc"]) for err in body["detail"]}
    assert ("body", "peer_set") in missing_fields, (
        f"expected body.peer_set to be flagged; got {missing_fields}"
    )


# ═════════════════════════════════════════════════════════════════
# 4. Coverage check — unknown entity → 404
# ═════════════════════════════════════════════════════════════════

def test_analyze_unknown_entity_returns_404(admin_api):
    resp = admin_api.post(
        "/api/engine/analyze",
        {
            "entity": "this-entity-does-not-exist-xyz",
            "peer_set": [],
        },
    )
    assert resp.status_code == 404, resp.text[:400]
    body = resp.json()
    assert "detail" in body
    assert "coverage" in body["detail"].lower()


# ═════════════════════════════════════════════════════════════════
# 5. Uniqueness — second analyze for same (entity, event_date) → 409
# ═════════════════════════════════════════════════════════════════

def test_analyze_duplicate_returns_409(admin_api):
    body = {
        "entity": "drift",
        "event_date": "2026-04-02",
        "peer_set": ["jupiter-perpetual-exchange"],
    }
    first = admin_api.post("/api/engine/analyze", body)
    assert first.status_code == 202, first.text[:400]
    aid_first = _track(first.json()["analysis_id"])

    second = admin_api.post("/api/engine/analyze", body)
    assert second.status_code == 409, second.text[:400]
    detail = second.json()["detail"]
    assert detail["error"] == "analysis_already_exists"
    assert detail["existing_analysis_id"] == aid_first


# ═════════════════════════════════════════════════════════════════
# 6. force_new=true archives the previous row and creates a new one
# ═════════════════════════════════════════════════════════════════

def test_analyze_force_new_archives_previous(admin_api):
    body = {
        "entity": "drift",
        "event_date": "2026-04-03",
        "peer_set": ["jupiter-perpetual-exchange"],
    }
    first = admin_api.post("/api/engine/analyze", body)
    assert first.status_code == 202, first.text[:400]
    aid_first = _track(first.json()["analysis_id"])

    body_force = dict(body, force_new=True)
    second = admin_api.post("/api/engine/analyze", body_force)
    assert second.status_code == 202, second.text[:400]
    aid_second = _track(second.json()["analysis_id"])
    assert aid_second != aid_first

    # Old row is archived with supersedes_reason set
    old_resp = admin_api.get(f"/api/engine/analyses/{aid_first}")
    assert old_resp.status_code == 200, old_resp.text[:400]
    old = old_resp.json()
    assert old["status"] == "archived"
    assert old["supersedes_reason"] is not None
    # Doubly-linked revision chain: old.superseded_by_id == new.id
    assert old["superseded_by_id"] == aid_second

    # New row points back to the old via previous_analysis_id
    new_resp = admin_api.get(f"/api/engine/analyses/{aid_second}")
    assert new_resp.status_code == 200, new_resp.text[:400]
    new = new_resp.json()
    assert new["previous_analysis_id"] == aid_first


# ═════════════════════════════════════════════════════════════════
# 6a. Regression guard — psycopg2 UUID adapter registered at import
#
# Observed in Railway logs after S2a deploy:
#   psycopg2.ProgrammingError: can't adapt type 'UUID'
#   app/engine/analysis_persistence.py:132 in _insert_analysis_sync
#
# Root cause: psycopg2 doesn't adapt Python uuid.UUID objects by default.
# The force_new=true path INSERTs with previous_analysis_id as a UUID and
# crashes without register_uuid() called at module import time. Fixed by
# adding psycopg2.extras.register_uuid() at the top of
# app/engine/analysis_persistence.py.
#
# This test explicitly exercises the previous_analysis_id path and asserts
# the second POST returns 202, not 500, so a future regression (e.g., a
# refactor that drops the register_uuid call) surfaces as a named test
# failure rather than a diagnostic-free 500.
# ═════════════════════════════════════════════════════════════════

def test_analyze_force_new_archives_previous_uuid_adapter(admin_api):
    """Regression: force_new path must not 500 due to UUID adapter
    registration being absent. See commentary above."""
    body = {
        "entity": "usdc",
        "event_date": "2026-04-05",
        "peer_set": [],
    }
    first = admin_api.post("/api/engine/analyze", body)
    assert first.status_code == 202, first.text[:400]
    _track(first.json()["analysis_id"])

    body_force = dict(body, force_new=True)
    second = admin_api.post("/api/engine/analyze", body_force)
    # If register_uuid() is missing from analysis_persistence.py, this
    # returns 500 with "can't adapt type 'UUID'" in the traceback.
    assert second.status_code == 202, (
        f"force_new POST returned {second.status_code} — suspected "
        f"psycopg2 UUID adapter regression in analysis_persistence.py. "
        f"Body: {second.text[:400]}"
    )
    _track(second.json()["analysis_id"])


# ═════════════════════════════════════════════════════════════════
# 7. List endpoint filters by entity
# ═════════════════════════════════════════════════════════════════

def test_list_analyses_filters_by_entity(admin_api):
    resp = admin_api.post(
        "/api/engine/analyze",
        {
            "entity": "layerzero",
            "event_date": "2026-04-04",
            "peer_set": [],
        },
    )
    assert resp.status_code == 202, resp.text[:400]
    _track(resp.json()["analysis_id"])

    list_resp = admin_api.get(
        "/api/engine/analyses", params={"entity": "layerzero", "limit": 50}
    )
    assert list_resp.status_code == 200, list_resp.text[:400]
    rows = list_resp.json()
    assert isinstance(rows, list)
    assert len(rows) >= 1
    # Every row should match the filter (including archived rows from prior
    # test runs — the list endpoint doesn't hide archived by default)
    for row in rows:
        assert row["entity"] == "layerzero"


# ═════════════════════════════════════════════════════════════════
# 8. GET with unknown id → 404
# ═════════════════════════════════════════════════════════════════

def test_get_analysis_unknown_id_returns_404(admin_api):
    random_id = uuid.uuid4()
    resp = admin_api.get(f"/api/engine/analyses/{random_id}")
    assert resp.status_code == 404, resp.text[:400]
    body = resp.json()
    assert "detail" in body
    assert str(random_id) in body["detail"]
