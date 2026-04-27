"""
Component 3: Renderer + recommendation tests.

Four unit tests against pure helpers (recommendation derivation,
empty-window handling) — fast, no HTTP, no DB.

Eight integration tests against POST /api/engine/render and the artifact
GET endpoints. Verify gating logic (force can't override V9.6 blocks),
recommendation correctness across coverage qualities, end-to-end render
+ persist + retrieve, list-artifacts-by-analysis.

Run:
    ADMIN_KEY=<key> BASE_URL=https://basisprotocol.xyz \\
      DATABASE_URL=<prod-url> \\
      pytest tests/test_engine_renderers.py -v

Cleanup mirrors test_engine_observations.py: per-test ID tracking +
session sweep against the canonical (entity, event_date) keys. The
artifacts FK on engine_analyses.id means we DELETE artifacts before
DELETE-ing analyses; the cleanup helpers below do this in order.
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any, Iterator, Optional

import pytest

from app.engine.recommendation import derive_recommendation
from app.engine.renderers._shared import render_observations_table
from app.engine.schemas import (
    CoverageResponse,
    EntityCoverage,
    Interpretation,
    Signal,
)


# ═════════════════════════════════════════════════════════════════
# Test fixture (entity, event_date) keys used by integration tests.
# Distinct event_dates from the C2 suites so all four files can run
# without colliding on the (entity, event_date) uniqueness constraint.
# ═════════════════════════════════════════════════════════════════

TEST_FIXTURE_KEYS: list[tuple[str, date]] = [
    ("drift", date(2026, 4, 15)),     # test_render_internal_memo_for_drift
    ("drift", date(2026, 4, 16)),     # test_render_incident_page_blocked_for_drift
    ("drift", date(2026, 4, 17)),     # test_render_force_does_not_override_blocked_incident
    ("drift", date(2026, 4, 18)),     # test_render_retrospective_for_drift
    ("drift", date(2026, 4, 19)),     # test_render_unknown_artifact_type_returns_422
    ("drift", date(2026, 4, 20)),     # test_get_artifacts_for_analysis_returns_list
    ("drift", date(2026, 4, 21)),     # test_get_artifact_by_id
    ("layerzero", date(2026, 4, 22)), # test_render_one_pager_for_layerzero
]


# ═════════════════════════════════════════════════════════════════
# DB cleanup helpers — artifacts before analyses (FK ordering)
# ═════════════════════════════════════════════════════════════════

def _db_delete_for_test_keys() -> int:
    """Session-start sweep. Deletes artifacts referencing test analyses
    first, then the analyses themselves. Returns total rows touched
    (analyses + artifacts) or -1 on failure."""
    conn_string = os.environ.get("DATABASE_URL")
    if not conn_string:
        return -1
    try:
        import psycopg2
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                # 1. Find the analysis_ids matching our test keys
                cur.execute(
                    """
                    SELECT id FROM engine_analyses
                    WHERE (entity, event_date) IN %s
                    """,
                    (tuple(TEST_FIXTURE_KEYS),),
                )
                ids = [r[0] for r in cur.fetchall()]
                if not ids:
                    return 0
                # 2. Delete artifacts for those analyses
                cur.execute(
                    "DELETE FROM engine_artifacts WHERE analysis_id = ANY(%s::uuid[])",
                    ([str(i) for i in ids],),
                )
                arts_deleted = cur.rowcount
                # 3. Delete the analyses
                cur.execute(
                    "DELETE FROM engine_analyses WHERE id = ANY(%s::uuid[])",
                    ([str(i) for i in ids],),
                )
                analyses_deleted = cur.rowcount
            conn.commit()
        return arts_deleted + analyses_deleted
    except Exception as exc:
        print(f"cleanup: session sweep failed: {exc}", file=sys.stderr)
        return -1


def _db_delete_for_analysis_ids(ids: list[str]) -> bool:
    """Per-test cleanup. Same artifact-then-analysis ordering."""
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
                    "DELETE FROM engine_artifacts WHERE analysis_id = ANY(%s::uuid[])",
                    ([str(i) for i in ids],),
                )
                cur.execute(
                    "DELETE FROM engine_analyses WHERE id = ANY(%s::uuid[])",
                    ([str(i) for i in ids],),
                )
            conn.commit()
        return True
    except Exception as exc:
        print(f"cleanup: per-test delete failed: {exc}", file=sys.stderr)
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
            timeout=60,
        )

    def get(self, path: str):
        return self._session.get(
            f"{self._base}{path}", headers=self._headers, timeout=30,
        )


@pytest.fixture(scope="session")
def admin_api(base_url, session, admin_key) -> _AdminAPI:
    return _AdminAPI(session, base_url, admin_key)


@pytest.fixture(scope="session", autouse=True)
def session_renderer_cleanup():
    deleted = _db_delete_for_test_keys()
    if deleted > 0:
        print(
            f"\n[render-tests] session-start sweep: deleted {deleted} stale "
            "row(s) (artifacts + analyses)",
            file=sys.stderr,
        )
    yield
    _db_delete_for_test_keys()


_created_ids: list[str] = []


@pytest.fixture(autouse=True)
def cleanup_created_analyses() -> Iterator[None]:
    _created_ids.clear()
    yield
    if _created_ids:
        _db_delete_for_analysis_ids(list(_created_ids))
        _created_ids.clear()


def _track(analysis_id: str) -> str:
    _created_ids.append(analysis_id)
    return analysis_id


def _wait_for_draft(admin_api, analysis_id: str, timeout: float = 30.0) -> dict:
    """Same pattern as test_engine_observations.py — poll up to 30s for
    the background finalize task to flip status to draft."""
    deadline = time.time() + timeout
    body: dict = {}
    while time.time() < deadline:
        resp = admin_api.get(f"/api/engine/analyses/{analysis_id}")
        if resp.status_code == 200:
            body = resp.json()
            if body.get("status") != "pending":
                return body
        time.sleep(0.7)
    return body


# ═════════════════════════════════════════════════════════════════
# Helper builders for unit tests
# ═════════════════════════════════════════════════════════════════

def _make_coverage(quality: str = "full-live") -> CoverageResponse:
    return CoverageResponse(
        identifier="test-entity",
        matched_entities=[
            EntityCoverage(
                index_id="psi",
                entity_slug="test-entity",
                entity_name="Test Entity",
                coverage_type="live",
                live=True,
                density="daily",
                earliest_record=date(2026, 1, 1),
                latest_record=date(2026, 4, 25),
                unique_days=115,
                days_since_last_record=0,
                coverage_window_days=114,
                data_source="generic_index_scores",
                available_endpoints=[],
            ),
        ],
        related_entities=[],
        adjacent_indexes_not_covering=[],
        coverage_summary="Test coverage summary",
        coverage_quality=quality,  # type: ignore[arg-type]
        recommended_analysis_types=[],
        blocks_incident_page=False,
        blocks_reasons=[],
        data_snapshot_hash="sha256:test",
        computed_at=datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc),
    )


def _make_interpretation(confidence: str = "medium") -> Interpretation:
    return Interpretation(
        event_summary="Test event summary.",
        what_this_does_not_claim="Tests claim nothing.",
        headline="Test headline",
        confidence=confidence,  # type: ignore[arg-type]
        confidence_reasoning="Test confidence reasoning.",
        prompt_version="v1",
        input_hash="sha256:test",
        model_id="test-model",
        generated_at=datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc),
        from_cache=False,
    )


# ═════════════════════════════════════════════════════════════════
# 1. Unit: full-live + sufficient confidence → all 6 in
#    {recommended} ∪ supports
# ═════════════════════════════════════════════════════════════════

def test_recommendation_full_live_unblocks_all():
    coverage = _make_coverage("full-live")
    signal = Signal()
    interp = _make_interpretation("high")
    rec = derive_recommendation(coverage, signal, interp)

    allowed = {rec.recommended, *rec.supports}
    expected = {
        "incident_page", "retrospective_internal", "case_study",
        "internal_memo", "talking_points", "one_pager",
    }
    assert allowed == expected, (
        f"full-live + high confidence should permit all 6 types; "
        f"got allowed={allowed} blocked={rec.blocked}"
    )
    assert rec.blocked == [], (
        f"full-live should have no blocks; got blocked={rec.blocked}"
    )


# ═════════════════════════════════════════════════════════════════
# 2. Unit: partial-reconstructable blocks incident_page + short
#    public artifacts (V9.6 constitutional)
# ═════════════════════════════════════════════════════════════════

def test_recommendation_partial_reconstructable_blocks_incident_page():
    coverage = _make_coverage("partial-reconstructable")
    signal = Signal()
    interp = _make_interpretation("medium")
    rec = derive_recommendation(coverage, signal, interp)

    assert "incident_page" in rec.blocked
    assert "talking_points" in rec.blocked
    assert "one_pager" in rec.blocked
    # Internal-only paths still allowed
    allowed = {rec.recommended, *rec.supports}
    assert "retrospective_internal" in allowed
    assert "internal_memo" in allowed
    assert "case_study" in allowed


# ═════════════════════════════════════════════════════════════════
# 3. Unit: insufficient confidence floors to internal_memo only
# ═════════════════════════════════════════════════════════════════

def test_recommendation_insufficient_confidence_only_internal_memo():
    coverage = _make_coverage("partial-live")
    signal = Signal()
    interp = _make_interpretation("insufficient")
    rec = derive_recommendation(coverage, signal, interp)

    allowed = {rec.recommended, *rec.supports}
    assert allowed == {"internal_memo"}, (
        f"insufficient confidence should restrict to internal_memo only; "
        f"got allowed={allowed}"
    )
    # Other types that would have been allowed under partial-live + medium
    # confidence are now in blocked
    for t in ("retrospective_internal", "case_study", "talking_points", "one_pager"):
        assert t in rec.blocked, f"expected {t!r} in blocked; got {rec.blocked}"


# ═════════════════════════════════════════════════════════════════
# 4. Unit: render_observations_table handles empty input
# ═════════════════════════════════════════════════════════════════

def test_render_observations_table_handles_empty_window():
    out = render_observations_table([])
    assert out == "_No observations._"

    out_custom = render_observations_table(
        [], empty_placeholder="_Window empty._"
    )
    assert out_custom == "_Window empty._"


# ═════════════════════════════════════════════════════════════════
# 5. Integration: render internal_memo for Drift
#
# Drift is partial-reconstructable; internal_memo is in the allowed set.
# ═════════════════════════════════════════════════════════════════

def test_render_internal_memo_for_drift(admin_api):
    body = {
        "entity": "drift",
        "event_date": "2026-04-15",
        "peer_set": ["jupiter-perpetual-exchange"],
    }
    resp = admin_api.post("/api/engine/analyze", body)
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])

    full = _wait_for_draft(admin_api, aid)
    assert full.get("status") == "draft"

    render_resp = admin_api.post(
        "/api/engine/render",
        {"analysis_id": aid, "artifact_type": "internal_memo"},
    )
    assert render_resp.status_code == 202, render_resp.text[:400]

    artifact = render_resp.json()
    assert artifact["artifact_type"] == "internal_memo"
    assert artifact["status"] == "draft"
    assert artifact["analysis_id"] == aid
    md = artifact["content_markdown"]
    assert "Internal Memo: drift" in md
    assert "What we know" in md
    assert "What we don't know" in md
    # internal_memo is never published — suggested_url stays null
    assert artifact["suggested_url"] is None


# ═════════════════════════════════════════════════════════════════
# 6. Integration: render incident_page for Drift returns 422 (V9.6 block)
# ═════════════════════════════════════════════════════════════════

def test_render_incident_page_blocked_for_drift(admin_api):
    body = {
        "entity": "drift",
        "event_date": "2026-04-16",
        "peer_set": ["jupiter-perpetual-exchange"],
    }
    resp = admin_api.post("/api/engine/analyze", body)
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])
    full = _wait_for_draft(admin_api, aid)
    assert full.get("status") == "draft"

    render_resp = admin_api.post(
        "/api/engine/render",
        {"analysis_id": aid, "artifact_type": "incident_page"},
    )
    assert render_resp.status_code == 422, render_resp.text[:400]
    detail = render_resp.json()["detail"]
    assert detail["error"] == "artifact_type_blocked"
    assert "incident_page" in detail["blocked"]


# ═════════════════════════════════════════════════════════════════
# 7. Integration: force=true cannot override the V9.6 incident_page block
# ═════════════════════════════════════════════════════════════════

def test_render_force_does_not_override_blocked_incident(admin_api):
    body = {
        "entity": "drift",
        "event_date": "2026-04-17",
        "peer_set": ["jupiter-perpetual-exchange"],
    }
    resp = admin_api.post("/api/engine/analyze", body)
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])
    full = _wait_for_draft(admin_api, aid)
    assert full.get("status") == "draft"

    # force=true must STILL produce 422 — V9.6 constitutional
    render_resp = admin_api.post(
        "/api/engine/render",
        {"analysis_id": aid, "artifact_type": "incident_page", "force": True},
    )
    assert render_resp.status_code == 422, render_resp.text[:400]
    detail = render_resp.json()["detail"]
    assert detail["error"] == "artifact_type_blocked"


# ═════════════════════════════════════════════════════════════════
# 8. Integration: render retrospective_internal for Drift (allowed path)
# ═════════════════════════════════════════════════════════════════

def test_render_retrospective_for_drift(admin_api):
    body = {
        "entity": "drift",
        "event_date": "2026-04-18",
        "peer_set": ["jupiter-perpetual-exchange"],
    }
    resp = admin_api.post("/api/engine/analyze", body)
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])
    full = _wait_for_draft(admin_api, aid)
    assert full.get("status") == "draft"

    render_resp = admin_api.post(
        "/api/engine/render",
        {"analysis_id": aid, "artifact_type": "retrospective_internal"},
    )
    assert render_resp.status_code == 202, render_resp.text[:400]
    artifact = render_resp.json()
    md = artifact["content_markdown"]
    assert "Retrospective: drift" in md
    assert "Internal audit" in md
    assert artifact["suggested_url"] is None
    assert artifact["suggested_path"] is not None
    assert artifact["suggested_path"].startswith("audits/internal/")


# ═════════════════════════════════════════════════════════════════
# 9. Integration: unknown artifact_type → 422
# ═════════════════════════════════════════════════════════════════

def test_render_unknown_artifact_type_returns_422(admin_api):
    body = {
        "entity": "drift",
        "event_date": "2026-04-19",
        "peer_set": [],
    }
    resp = admin_api.post("/api/engine/analyze", body)
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])
    _wait_for_draft(admin_api, aid)

    # Pydantic AnalysisType is a Literal; unknown values produce a
    # validation error at the request body parse layer (also 422).
    render_resp = admin_api.post(
        "/api/engine/render",
        {"analysis_id": aid, "artifact_type": "totally_invalid_type"},
    )
    assert render_resp.status_code == 422, render_resp.text[:400]


# ═════════════════════════════════════════════════════════════════
# 10. Integration: list artifacts for an analysis
# ═════════════════════════════════════════════════════════════════

def test_get_artifacts_for_analysis_returns_list(admin_api):
    body = {
        "entity": "drift",
        "event_date": "2026-04-20",
        "peer_set": [],
    }
    resp = admin_api.post("/api/engine/analyze", body)
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])
    _wait_for_draft(admin_api, aid)

    # Render two artifacts of different types
    r1 = admin_api.post(
        "/api/engine/render",
        {"analysis_id": aid, "artifact_type": "internal_memo"},
    )
    assert r1.status_code == 202
    r2 = admin_api.post(
        "/api/engine/render",
        {"analysis_id": aid, "artifact_type": "retrospective_internal"},
    )
    assert r2.status_code == 202

    # List should include both, ordered by rendered_at DESC
    list_resp = admin_api.get(f"/api/engine/analyses/{aid}/artifacts")
    assert list_resp.status_code == 200, list_resp.text[:400]
    artifacts = list_resp.json()
    types = [a["artifact_type"] for a in artifacts]
    assert "internal_memo" in types
    assert "retrospective_internal" in types
    assert len(artifacts) >= 2


# ═════════════════════════════════════════════════════════════════
# 11. Integration: GET /artifacts/{id} returns full artifact with markdown
# ═════════════════════════════════════════════════════════════════

def test_get_artifact_by_id(admin_api):
    body = {
        "entity": "drift",
        "event_date": "2026-04-21",
        "peer_set": [],
    }
    resp = admin_api.post("/api/engine/analyze", body)
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])
    _wait_for_draft(admin_api, aid)

    render_resp = admin_api.post(
        "/api/engine/render",
        {"analysis_id": aid, "artifact_type": "internal_memo"},
    )
    assert render_resp.status_code == 202
    artifact_id = render_resp.json()["id"]

    fetched = admin_api.get(f"/api/engine/artifacts/{artifact_id}")
    assert fetched.status_code == 200, fetched.text[:400]
    body_full = fetched.json()
    assert body_full["id"] == artifact_id
    assert body_full["artifact_type"] == "internal_memo"
    assert "content_markdown" in body_full
    assert len(body_full["content_markdown"]) > 100


# ═════════════════════════════════════════════════════════════════
# 12. Integration: render one_pager for LayerZero (partial-live BRI)
# ═════════════════════════════════════════════════════════════════

def test_render_one_pager_for_layerzero(admin_api):
    body = {
        "entity": "layerzero",
        "event_date": "2026-04-22",
        "peer_set": [],
    }
    resp = admin_api.post("/api/engine/analyze", body)
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])
    full = _wait_for_draft(admin_api, aid)
    assert full.get("status") == "draft"

    # Confirm coverage_quality is partial-live (BRI is live with deep history)
    coverage_quality = full["coverage"]["coverage_quality"]
    assert coverage_quality == "partial-live", (
        f"expected partial-live for layerzero/BRI; got {coverage_quality}. "
        "Coverage may have shifted; the test asserts the rendering path "
        "more than the specific quality."
    )

    render_resp = admin_api.post(
        "/api/engine/render",
        {"analysis_id": aid, "artifact_type": "one_pager"},
    )
    assert render_resp.status_code == 202, render_resp.text[:400]
    artifact = render_resp.json()
    md = artifact["content_markdown"]
    # Compact format — soft word ceiling. Don't pin a hard number; LLM
    # output varies. Assert it's not absurdly long.
    word_count = len(md.split())
    assert word_count <= 600, (
        f"one_pager exceeded soft ceiling (600 words); got {word_count}. "
        "Templates may need tighter truncation."
    )
    assert "layerzero" in md.lower()
    # Public artifact — has a suggested_url
    assert artifact["suggested_url"] is not None
    assert artifact["suggested_url"].startswith("/one-pagers/")
