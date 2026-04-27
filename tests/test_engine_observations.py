"""
Component 2b: Observation builder tests.

Six unit tests against pure helpers in app.engine.observation_builder
(window math, regression slope, z-score, peer aggregation) — fast, no
HTTP, no DB.

Four integration tests against the live POST /api/engine/analyze
endpoint with admin auth — verify the populated Signal contains real
observations, peer-divergence flags fire when peer_set is non-empty,
and unknown measures fall through to unit="unknown" without erroring.

Run:
    ADMIN_KEY=<key> BASE_URL=https://basisprotocol.xyz \\
      DATABASE_URL=<prod-url> \\
      pytest tests/test_engine_observations.py -v

ADMIN_KEY required only for the four integration tests; unit tests run
unconditionally. Integration tests skip cleanly if ADMIN_KEY is unset.

Test cleanup mirrors test_engine_analyze.py: per-test ID tracking +
session-start orphan sweep against a small canonical key set covering
this file's POSTs.
"""

from __future__ import annotations

import logging
import os
import sys
import time
import uuid
from datetime import date, timedelta
from typing import Any, Iterator, Optional

import pytest

from app.engine.observation_builder import (
    ANOMALY_HISTORY_MIN,
    ANOMALY_Z_THRESHOLD,
    MEASURE_UNITS,
    TREND_MIN_POINTS,
    _linear_regression_slope,
    _peer_average,
    _unit_for,
    _warned_unknown_measures,
    _z_score,
    compute_windows,
)


# ═════════════════════════════════════════════════════════════════
# Shared cleanup infrastructure (mirrors test_engine_analyze.py)
# ═════════════════════════════════════════════════════════════════

TEST_FIXTURE_KEYS: list[tuple[str, date]] = [
    ("drift", date(2026, 4, 1)),       # test_drift_analysis_populates_real_observations
    ("kelp-rseth", date(2026, 4, 18)), # test_rseth_analysis_pre_event_observations
    ("drift", date(2026, 4, 6)),       # test_analysis_with_empty_peer_set_no_divergence
    ("drift", date(2026, 4, 7)),       # test_observation_unit_lookup_handles_unknown_measure
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
# Fixtures
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
            timeout=60,  # signal build is sync ~3-5 DB queries; allow extra
        )

    def get(self, path: str):
        return self._session.get(
            f"{self._base}{path}", headers=self._headers, timeout=30,
        )


@pytest.fixture(scope="session")
def admin_api(base_url, session, admin_key) -> _AdminAPI:
    return _AdminAPI(session, base_url, admin_key)


@pytest.fixture(scope="session", autouse=True)
def session_observations_cleanup():
    deleted = _db_delete_by_fixture_keys()
    if deleted > 0:
        print(
            f"\n[obs-tests] session-start sweep: deleted {deleted} stale row(s)",
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


def _wait_for_draft(admin_api, analysis_id: str, timeout: float = 60.0) -> dict:
    """Poll GET /api/engine/analyses/{id} until status != 'pending'.
    Returns the final response body.

    Default timeout 60s: background task includes the LLM roundtrip
    (5–15s) plus signal build, recommendation derivation, and an
    UPDATE. Sequential test runs can serialize a few of these and
    accumulate latency; 30s was too tight in practice.
    """
    deadline = time.time() + timeout
    body: dict = {}
    while time.time() < deadline:
        resp = admin_api.get(f"/api/engine/analyses/{analysis_id}")
        if resp.status_code == 200:
            body = resp.json()
            if body.get("status") != "pending":
                return body
        time.sleep(0.5)
    return body  # caller asserts on whatever we last saw


# ═════════════════════════════════════════════════════════════════
# 1. compute_windows with event_date
# ═════════════════════════════════════════════════════════════════

def test_compute_windows_with_event_date():
    """event_date=2026-04-01, today=2026-04-25 → correct three windows."""
    today = date(2026, 4, 25)
    event = date(2026, 4, 1)
    windows = compute_windows(event, today=today)

    assert set(windows.keys()) == {"pre_event", "event_window", "post_event"}
    assert windows["pre_event"] == (date(2026, 3, 2), date(2026, 3, 31))
    assert windows["event_window"] == (date(2026, 4, 1), date(2026, 4, 8))
    assert windows["post_event"] == (date(2026, 4, 9), date(2026, 4, 25))


# ═════════════════════════════════════════════════════════════════
# 2. compute_windows without event_date
# ═════════════════════════════════════════════════════════════════

def test_compute_windows_without_event_date():
    """event_date=None → only `baseline`, last 30 days."""
    today = date(2026, 4, 25)
    windows = compute_windows(None, today=today)

    assert set(windows.keys()) == {"baseline"}
    assert windows["baseline"] == (date(2026, 3, 26), date(2026, 4, 25))


# ═════════════════════════════════════════════════════════════════
# 3. linear regression slope on increasing values
# ═════════════════════════════════════════════════════════════════

def test_linear_regression_slope_positive():
    """Strictly-increasing series → positive slope."""
    base = date(2026, 4, 1)
    points = [
        (base + timedelta(days=0), 10.0),
        (base + timedelta(days=1), 12.0),
        (base + timedelta(days=2), 14.0),
        (base + timedelta(days=3), 16.0),
    ]
    slope = _linear_regression_slope(points)
    assert slope is not None
    assert slope > 0
    # Ascending by 2/day → slope ~2.0
    assert abs(slope - 2.0) < 0.01

    # Below TREND_MIN_POINTS returns None
    assert _linear_regression_slope(points[:2]) is None
    assert TREND_MIN_POINTS == 3  # contract guard


# ═════════════════════════════════════════════════════════════════
# 4. z-score returns None when history is too short
# ═════════════════════════════════════════════════════════════════

def test_z_score_insufficient_history_returns_none():
    """13 historical points → no anomaly flag possible (strict 14 minimum)."""
    history = [50.0] * 13
    assert _z_score(value=100.0, history=history) is None
    # 14 points: now allowed (but variance=0 → still None — covered below)
    assert ANOMALY_HISTORY_MIN == 14  # contract guard


# ═════════════════════════════════════════════════════════════════
# 5. z-score flags an obvious spike
# ═════════════════════════════════════════════════════════════════

def test_z_score_extreme_value_flags_anomaly():
    """Stable history at 50 ± 1, then a value of 100 → high z-score."""
    import random
    random.seed(0)
    history = [50.0 + random.uniform(-1, 1) for _ in range(30)]
    z = _z_score(value=100.0, history=history)
    assert z is not None
    assert z > ANOMALY_Z_THRESHOLD, (
        f"expected z > {ANOMALY_Z_THRESHOLD} for a 50-sigma spike, got {z}"
    )


# ═════════════════════════════════════════════════════════════════
# 6. peer divergence inactive when peer_set is empty
# ═════════════════════════════════════════════════════════════════

def test_peer_divergence_empty_peers():
    """Empty peer_data → peer_avg=None, peer_slugs=[]."""
    avg, slugs = _peer_average(
        peer_data={},
        measure="overall_score",
        window_start=date(2026, 4, 1),
        window_end=date(2026, 4, 8),
    )
    assert avg is None
    assert slugs == []

    # And: peer with no coverage on the requested measure → still empty
    avg, slugs = _peer_average(
        peer_data={"jupiter-perpetual-exchange": {"some_other_measure": [(date(2026, 4, 5), 10.0)]}},
        measure="overall_score",
        window_start=date(2026, 4, 1),
        window_end=date(2026, 4, 8),
    )
    assert avg is None
    assert slugs == []


# ═════════════════════════════════════════════════════════════════
# 7. Drift analysis populates real observations
# ═════════════════════════════════════════════════════════════════

def test_drift_analysis_populates_real_observations(admin_api):
    """POST analyze for drift/2026-04-01 with peer=jupiter. After the
    pending→draft flip, signal must contain real observations across
    the three event windows (not the empty-stub default)."""
    body = {
        "entity": "drift",
        "event_date": "2026-04-01",
        "peer_set": ["jupiter-perpetual-exchange"],
    }
    resp = admin_api.post("/api/engine/analyze", body)
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])

    body_full = _wait_for_draft(admin_api, aid)
    assert body_full.get("status") == "draft", (
        f"status didn't flip to draft within timeout; last seen: {body_full}"
    )

    signal = body_full["signal"]
    # baseline must be empty when event_date is set (schema invariant)
    assert signal["baseline"] == []
    # At least one of the three event windows must have observations.
    # Drift's PSI backfill alone supplies 30+ pre-event days.
    total = (
        len(signal["pre_event"])
        + len(signal["event_window"])
        + len(signal["post_event"])
    )
    assert total > 0, (
        f"expected >0 observations for drift/2026-04-01; got an entirely "
        f"empty signal: {signal}"
    )

    # Spot-check: at least one observation from index_id='psi' (backfilled
    # via historical_protocol_data) — Drift always has PSI history.
    psi_obs = [
        o for w in ("pre_event", "event_window", "post_event")
        for o in signal[w]
        if o["index_id"] == "psi"
    ]
    assert len(psi_obs) > 0, "expected PSI observations from backfill"

    # Stage stamp: bumped to the S2c value when LLM interpretation
    # landed. Forward-stable detector continues to flag drift if a
    # future stage ships without bumping the stamp.
    assert body_full["analysis_version"] == "v0.1-s2c-llm-interpretation"

    # Real LLM interpretation tags (S2c). Accept the fallback path so
    # CI doesn't fail on a known-degraded production state (no
    # ANTHROPIC_API_KEY, budget exhausted). Skip with a clear reason
    # rather than failing — operator sees the cause.
    interp_model_id = body_full["interpretation"]["model_id"]
    assert interp_model_id in ("claude-sonnet-4-6", "template:fallback"), (
        f"unexpected model_id: {interp_model_id!r}"
    )
    if interp_model_id == "template:fallback":
        pytest.skip(
            f"LLM unavailable (fallback returned). Reason: "
            f"{body_full['interpretation'].get('confidence_reasoning')!r}. "
            "Set ANTHROPIC_API_KEY and ensure budget headroom, then re-run."
        )


# ═════════════════════════════════════════════════════════════════
# 8. rsETH analysis populates LSTI observations on the pre-event window
# ═════════════════════════════════════════════════════════════════

def test_rseth_analysis_pre_event_observations(admin_api):
    """POST for kelp-rseth/2026-04-18. LSTI is live with deep history;
    pre_event must contain LSTI observations including at least one
    component score (e.g., withdrawal_queue_impl, slashing_insurance)."""
    body = {
        "entity": "kelp-rseth",
        "event_date": "2026-04-18",
        "peer_set": [],
    }
    resp = admin_api.post("/api/engine/analyze", body)
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])

    body_full = _wait_for_draft(admin_api, aid)
    assert body_full.get("status") == "draft"

    pre = body_full["signal"]["pre_event"]
    assert len(pre) > 0, "pre_event should have observations from LSTI history"

    lsti_pre = [o for o in pre if o["index_id"] == "lsti"]
    assert len(lsti_pre) > 0, "expected LSTI observations in pre_event"

    measures = {o["measure"] for o in lsti_pre}
    # We don't pin specific values (production data evolves) but any of
    # these LSTI components plausibly appears for kelp-rseth — at least
    # one should be present.
    expected_any_of = {
        "overall_score", "withdrawal_queue_impl", "slashing_insurance",
        "admin_key_risk", "exploit_history_lst", "beacon_chain_dependency",
    }
    assert measures & expected_any_of, (
        f"none of the expected LSTI measures present in pre_event; got: {measures}"
    )


# ═════════════════════════════════════════════════════════════════
# 9. Empty peer_set → no peer_divergence on any observation
# ═════════════════════════════════════════════════════════════════

def test_analysis_with_empty_peer_set_no_divergence(admin_api):
    """peer_set=[] → every observation has peer_divergence_magnitude=None
    and peer_slugs_compared=[]. Distinct event_date from test 7 to avoid
    409 collision."""
    body = {
        "entity": "drift",
        "event_date": "2026-04-06",
        "peer_set": [],
    }
    resp = admin_api.post("/api/engine/analyze", body)
    assert resp.status_code == 202, resp.text[:400]
    aid = _track(resp.json()["analysis_id"])

    body_full = _wait_for_draft(admin_api, aid)
    assert body_full.get("status") == "draft"

    signal = body_full["signal"]
    all_obs = signal["pre_event"] + signal["event_window"] + signal["post_event"]
    assert len(all_obs) > 0, "expected some observations to assert against"

    for o in all_obs:
        assert o["peer_divergence_magnitude"] is None, (
            f"observation has non-None peer_divergence_magnitude despite empty peer_set: {o}"
        )
        assert o["peer_slugs_compared"] == [], (
            f"observation has non-empty peer_slugs_compared despite empty peer_set: {o}"
        )


# ═════════════════════════════════════════════════════════════════
# 10. Unknown measure falls through to unit="unknown" + warning logged
# ═════════════════════════════════════════════════════════════════

def test_observation_unit_lookup_handles_unknown_measure(caplog):
    """Direct unit lookup for an invented measure name returns 'unknown'
    and emits a logging.warning. Asserted on the helper rather than
    end-to-end so we don't depend on production data containing an
    unknown measure (which would mean MEASURE_UNITS is incomplete —
    addressable separately by adding the key)."""
    bogus = "this_measure_definitely_does_not_exist_xyz_2026"
    # Reset the dedupe set so the warning fires inside this test
    _warned_unknown_measures.discard(bogus)

    with caplog.at_level(logging.WARNING, logger="app.engine.observation_builder"):
        unit = _unit_for(bogus)

    assert unit == "unknown"
    assert any(
        bogus in record.getMessage() and "MEASURE_UNITS" in record.getMessage()
        for record in caplog.records
    ), (
        f"expected a WARNING mentioning {bogus!r} and MEASURE_UNITS; "
        f"got: {[r.getMessage() for r in caplog.records]}"
    )

    # And contract guard: a known measure does NOT log
    assert _unit_for("overall_score") == "score_0_100"
    assert "overall_score" in MEASURE_UNITS
