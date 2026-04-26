"""
Component 2c: LLM interpretation tests.

Five unit tests against pure helpers in app.engine.interpretation
(input-hash determinism + canonicalization, fallback shape) — fast, no
HTTP, no Anthropic API, no DB. The Pydantic Interpretation model and the
SHAPE_API_UNAVAILABLE template construction need only the schema imports.

Three integration tests against POST /api/engine/analyze — real LLM
interpretation, cache hit on identical re-request, GET /api/engine/budget.
These need ADMIN_KEY for the admin-protected endpoints AND the production
server to have ANTHROPIC_API_KEY available so the LLM call can run.

Run:
    ADMIN_KEY=<key> BASE_URL=https://basisprotocol.xyz \\
      DATABASE_URL=<prod-url> \\
      pytest tests/test_engine_interpretation.py -v

Cleanup mirrors test_engine_observations.py: per-test ID tracking + session
sweep against this file's TEST_FIXTURE_KEYS. Distinct event_dates from
S2a/S2b suites so the three suites coexist cleanly.

Test mapping:
  1. test_compute_inputs_hash_deterministic
  2. test_compute_inputs_hash_changes_with_signal_change
  3. test_compute_inputs_hash_changes_with_prompt_version
  4. test_api_unavailable_template_has_correct_shape
  5. test_canonicalize_signal_observation_order_independent
  6. test_drift_analysis_produces_real_interpretation
  7. test_cache_hit_on_second_identical_call
  8. test_budget_status_endpoint
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any, Iterator, Optional

import pytest

from app.engine.interpretation import (
    ACTIVE_PROMPT_VERSION,
    FALLBACK_MODEL_ID,
    FALLBACK_PROMPT_VERSION,
    _api_unavailable_template,
    _canonicalize_signal,
    compute_inputs_hash,
)
from app.engine.schemas import (
    EntityCoverage,
    Interpretation,
    Observation,
    Signal,
)


# ═════════════════════════════════════════════════════════════════
# Cleanup — DB-direct, mirrors test_engine_observations.py
# ═════════════════════════════════════════════════════════════════

TEST_FIXTURE_KEYS: list[tuple[str, date]] = [
    ("drift", date(2026, 4, 8)),       # test_drift_analysis_produces_real_interpretation
    ("drift", date(2026, 4, 9)),       # test_cache_hit_on_second_identical_call (first POST)
]


def _db_delete_by_ids(ids: list[str]) -> bool:
    conn_string = os.environ.get("DATABASE_URL")
    if not conn_string:
        return False
    try:
        import psycopg2
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
    conn_string = os.environ.get("DATABASE_URL")
    if not conn_string:
        return -1
    try:
        import psycopg2
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM engine_analyses WHERE (entity, event_date) IN %s",
                    (tuple(TEST_FIXTURE_KEYS),),
                )
                deleted = cur.rowcount
            conn.commit()
        return deleted
    except Exception as exc:
        print(f"cleanup: session-start sweep failed: {exc}", file=sys.stderr)
        return -1


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
        # LLM roundtrip can take up to ~15s; give the POST extra headroom.
        return self._session.post(
            f"{self._base}{path}",
            json=body or {},
            headers=self._headers,
            timeout=90,
        )

    def get(self, path: str):
        return self._session.get(
            f"{self._base}{path}", headers=self._headers, timeout=60,
        )


@pytest.fixture(scope="session")
def admin_api(base_url, session, admin_key) -> _AdminAPI:
    return _AdminAPI(session, base_url, admin_key)


@pytest.fixture(scope="session", autouse=True)
def session_interpretation_cleanup():
    deleted = _db_delete_by_fixture_keys()
    if deleted > 0:
        print(
            f"\n[interp-tests] session-start sweep: deleted {deleted} stale row(s)",
            file=sys.stderr,
        )
    yield
    _db_delete_by_fixture_keys()


_created_ids: list[str] = []


@pytest.fixture(autouse=True)
def cleanup_created_analyses() -> Iterator[None]:
    _created_ids.clear()
    yield
    if not _created_ids:
        return
    _db_delete_by_ids(list(_created_ids))
    _created_ids.clear()


def _track(analysis_id: str) -> str:
    _created_ids.append(analysis_id)
    return analysis_id


def _wait_for_draft(admin_api, analysis_id: str, timeout: float = 30.0) -> dict:
    """Poll until status != pending. LLM call can take 5-15s in addition
    to the ~2s background-task delay; allow 30s ceiling."""
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
# Fixture builders for unit tests
# ═════════════════════════════════════════════════════════════════

def _make_observation(
    *,
    measure: str = "overall_score",
    window: str = "pre_event",
    metric_value: float = 50.0,
    at_date: date = date(2026, 3, 15),
) -> Observation:
    return Observation(
        index_id="psi",
        entity_slug="drift",
        measure=measure,
        window=window,  # type: ignore[arg-type]
        kind="value",
        metric_value=metric_value,
        unit="score_0_100",
        at_date=at_date,
        window_start=at_date,
        window_end=at_date,
    )


def _baseline_signal() -> Signal:
    """Signal with three observations across pre_event, ordered."""
    return Signal(
        pre_event=[
            _make_observation(measure="overall_score", metric_value=42.0),
            _make_observation(measure="balance_sheet", metric_value=55.0),
            _make_observation(measure="security", metric_value=30.0),
        ],
    )


# ═════════════════════════════════════════════════════════════════
# 1. Hash determinism — identical inputs → identical hash
# ═════════════════════════════════════════════════════════════════

def test_compute_inputs_hash_deterministic():
    signal = _baseline_signal()
    args = dict(
        entity="drift",
        event_date=date(2026, 4, 1),
        peer_set=["jupiter-perpetual-exchange"],
        coverage_snapshot_hash="sha256:abc123",
        signal=signal,
    )
    h1 = compute_inputs_hash(**args)
    h2 = compute_inputs_hash(**args)
    assert h1 == h2
    assert h1.startswith("sha256:")
    assert len(h1.split(":")[1]) == 64


# ═════════════════════════════════════════════════════════════════
# 2. Hash changes when signal changes
# ═════════════════════════════════════════════════════════════════

def test_compute_inputs_hash_changes_with_signal_change():
    args = dict(
        entity="drift",
        event_date=date(2026, 4, 1),
        peer_set=["jupiter-perpetual-exchange"],
        coverage_snapshot_hash="sha256:abc123",
    )
    h1 = compute_inputs_hash(signal=_baseline_signal(), **args)

    # Different metric_value on one observation
    s2 = _baseline_signal()
    s2.pre_event[0] = _make_observation(measure="overall_score", metric_value=99.9)
    h2 = compute_inputs_hash(signal=s2, **args)

    assert h1 != h2


# ═════════════════════════════════════════════════════════════════
# 3. Hash changes when prompt_version is bumped
# ═════════════════════════════════════════════════════════════════

def test_compute_inputs_hash_changes_with_prompt_version():
    """Bumping the prompt version invalidates all old cache entries by
    construction — the hash includes prompt_version."""
    common = dict(
        entity="drift",
        event_date=date(2026, 4, 1),
        peer_set=[],
        coverage_snapshot_hash="sha256:abc123",
        signal=_baseline_signal(),
    )
    h_v1 = compute_inputs_hash(prompt_version="v1", **common)
    h_v2 = compute_inputs_hash(prompt_version="v2", **common)
    assert h_v1 != h_v2


# ═════════════════════════════════════════════════════════════════
# 4. Fallback template shape
# ═════════════════════════════════════════════════════════════════

def test_api_unavailable_template_has_correct_shape():
    interp = _api_unavailable_template(
        inputs_hash="sha256:test", reason="forced for test",
    )
    assert isinstance(interp, Interpretation)
    assert interp.confidence == "insufficient"
    assert interp.model_id == FALLBACK_MODEL_ID
    assert interp.prompt_version == FALLBACK_PROMPT_VERSION
    assert interp.input_hash == "sha256:test"
    assert interp.from_cache is False
    # Story fields are all None when service is unavailable
    assert interp.pre_event_story is None
    assert interp.event_story is None
    assert interp.post_event_story is None
    assert interp.cross_peer_reading is None
    # Reason surfaces in confidence_reasoning so the operator knows why
    assert "forced for test" in interp.confidence_reasoning
    # Required fields are populated, not empty strings
    assert interp.event_summary
    assert interp.headline
    assert interp.what_this_does_not_claim


# ═════════════════════════════════════════════════════════════════
# 5. Signal canonicalization is order-independent
# ═════════════════════════════════════════════════════════════════

def test_canonicalize_signal_observation_order_independent():
    """The canonicalized representation must sort observations within
    each window so that two semantically-identical signals built in
    different orders produce the same hash."""
    s_ordered = Signal(
        pre_event=[
            _make_observation(measure="aaa"),
            _make_observation(measure="bbb"),
            _make_observation(measure="ccc"),
        ]
    )
    s_reversed = Signal(
        pre_event=[
            _make_observation(measure="ccc"),
            _make_observation(measure="bbb"),
            _make_observation(measure="aaa"),
        ]
    )
    canon_a = _canonicalize_signal(s_ordered)
    canon_b = _canonicalize_signal(s_reversed)
    assert canon_a == canon_b

    # And: the resulting input hashes match
    args = dict(
        entity="drift",
        event_date=date(2026, 4, 1),
        peer_set=[],
        coverage_snapshot_hash="sha256:same",
    )
    assert (
        compute_inputs_hash(signal=s_ordered, **args)
        == compute_inputs_hash(signal=s_reversed, **args)
    )


# ═════════════════════════════════════════════════════════════════
# 6. Real LLM interpretation populates required fields
# ═════════════════════════════════════════════════════════════════

def test_drift_analysis_produces_real_interpretation(admin_api):
    """POST analyze for drift; after pending→draft flip, interpretation
    has populated content fields (not the stub) and is tagged with the
    Sonnet 4.6 model_id and prompt v1."""
    body = {
        "entity": "drift",
        "event_date": "2026-04-08",
        "peer_set": ["jupiter-perpetual-exchange"],
    }
    resp = admin_api.post("/api/engine/analyze", body)
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])

    full = _wait_for_draft(admin_api, aid, timeout=30.0)
    assert full.get("status") == "draft", (
        f"status didn't flip within 30s; last seen: {full}"
    )

    interp = full["interpretation"]
    # The Sonnet 4.6 path stamps model_id; on fallback, it'd be
    # template:fallback. Either is acceptable for this test as long as
    # the schema is honored — but we want production to actually be
    # reaching the LLM. Assert by accepting both, then surface the
    # fallback case explicitly so the operator sees it.
    assert interp["model_id"] in ("claude-sonnet-4-6", "template:fallback"), (
        f"unexpected model_id: {interp['model_id']!r}"
    )
    if interp["model_id"] == "template:fallback":
        pytest.skip(
            f"LLM unavailable (fallback returned). Reason: "
            f"{interp.get('confidence_reasoning')!r}. Set ANTHROPIC_API_KEY "
            "and ensure budget headroom, then re-run."
        )

    # Real LLM path
    assert interp["prompt_version"] == ACTIVE_PROMPT_VERSION
    assert interp["confidence"] in ("high", "medium", "low", "insufficient")
    assert interp["from_cache"] is False
    # Required content fields populated
    assert interp["event_summary"]
    assert interp["headline"]
    assert interp["what_this_does_not_claim"]
    assert interp["confidence_reasoning"]
    # Mentions the entity by name
    assert "drift" in interp["event_summary"].lower()
    # input_hash is sha256-prefixed
    assert interp["input_hash"].startswith("sha256:")
    # Forbidden tone — a well-tuned prompt should reliably avoid these.
    # Soft assertion: report and skip rather than hard-fail, because tone
    # drift is something the operator wants to see, not block CI on.
    forbidden = ("basis caught", "this proves", "predicts", "should have avoided")
    blob = " ".join(
        str(interp.get(k) or "") for k in (
            "event_summary", "headline", "what_this_does_not_claim",
            "pre_event_story", "event_story", "post_event_story",
            "cross_peer_reading", "confidence_reasoning",
        )
    ).lower()
    for phrase in forbidden:
        if phrase in blob:
            pytest.fail(
                f"Forbidden tone {phrase!r} appeared in interpretation; "
                f"review prompt v1 and consider a v2 bump. Full text: {blob}"
            )


# ═════════════════════════════════════════════════════════════════
# 7. Cache hit on second identical call
# ═════════════════════════════════════════════════════════════════

def test_cache_hit_on_second_identical_call(admin_api):
    """First POST creates analysis A with from_cache=False. Second POST
    with force_new=true creates analysis B with the SAME (entity,
    event_date, peer_set, coverage, signal) → SAME input_hash → cache
    hit, from_cache=True, identical content fields."""
    body = {
        "entity": "drift",
        "event_date": "2026-04-09",
        "peer_set": ["jupiter-perpetual-exchange"],
    }
    r1 = admin_api.post("/api/engine/analyze", body)
    assert r1.status_code == 202, r1.text[:400]
    aid_1 = _track(r1.json()["analysis_id"])
    a1 = _wait_for_draft(admin_api, aid_1, timeout=30.0)
    assert a1.get("status") == "draft"

    interp_1 = a1["interpretation"]
    if interp_1["model_id"] == "template:fallback":
        pytest.skip("first call fell back to template — cache test cannot run")

    # Second call — force_new=true so the (entity, event_date) uniqueness
    # constraint doesn't 409. Coverage + signal should be identical
    # (same entity/date/peers; same production data within seconds).
    body_force = dict(body, force_new=True)
    r2 = admin_api.post("/api/engine/analyze", body_force)
    assert r2.status_code == 202, r2.text[:400]
    aid_2 = _track(r2.json()["analysis_id"])
    a2 = _wait_for_draft(admin_api, aid_2, timeout=30.0)
    assert a2.get("status") == "draft"

    interp_2 = a2["interpretation"]
    # Same input_hash → cache hit
    assert interp_2["input_hash"] == interp_1["input_hash"], (
        "input_hash differs between identical calls — canonicalization broken"
    )
    assert interp_2["from_cache"] is True, (
        "second call should have hit the cache"
    )
    # Content fields are identical (cached payload returned verbatim)
    assert interp_2["event_summary"] == interp_1["event_summary"]
    assert interp_2["headline"] == interp_1["headline"]
    assert interp_2["confidence"] == interp_1["confidence"]
    # Model + prompt version match the original cache write
    assert interp_2["model_id"] == interp_1["model_id"]
    assert interp_2["prompt_version"] == interp_1["prompt_version"]


# ═════════════════════════════════════════════════════════════════
# 8. Budget endpoint returns expected shape
# ═════════════════════════════════════════════════════════════════

def test_budget_status_endpoint(admin_api):
    """GET /api/engine/budget returns the shape consumers (ops dashboards,
    runbooks) depend on."""
    resp = admin_api.get("/api/engine/budget")
    assert resp.status_code == 200, resp.text[:400]
    body = resp.json()
    expected_keys = {
        "today_utc",
        "month_start_utc",
        "today_calls",
        "today_calls_remaining",
        "today_calls_ceiling",
        "month_calls",
        "month_input_tokens",
        "month_output_tokens",
        "month_cost_usd",
        "month_budget_usd",
        "month_budget_remaining_usd",
        "input_price_per_m_usd",
        "output_price_per_m_usd",
    }
    missing = expected_keys - set(body.keys())
    assert not missing, f"budget response missing keys: {missing}"

    # Sanity: numeric fields are numbers, dates are ISO strings
    assert isinstance(body["today_calls"], int)
    assert isinstance(body["month_cost_usd"], (int, float))
    assert isinstance(body["month_budget_usd"], (int, float))
    assert body["today_calls"] >= 0
    assert body["today_calls_remaining"] >= 0
    # Pricing values match cost_tracker constants
    assert body["input_price_per_m_usd"] == 3.0
    assert body["output_price_per_m_usd"] == 15.0
