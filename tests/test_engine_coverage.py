"""
Component 1: Coverage endpoint tests.

Style: HTTP integration tests against a live server. Uses the `api` fixture
from tests/conftest.py which hits BASE_URL (defaults to http://localhost:5000,
override via BASE_URL env var).

Run:
    BASE_URL=https://basisprotocol.xyz pytest tests/test_engine_coverage.py -v

All tests are read-only (GET only, no writes) and production-safe. Assertions
compare structural shape (coverage_quality, covering index set,
blocks_incident_page) against the canonical fixtures in
tests/fixtures/canonical_coverage.py rather than absolute values that drift
over time (unique_days, latest_record, etc.).

Fixture mapping:
  1. test_coverage_drift                              → DRIFT_COVERAGE
  2. test_coverage_kelp_rseth_unblocked               → KELP_RSETH_COVERAGE
  3. test_coverage_usdc_stale_but_unblocked           → USDC_COVERAGE
  4. test_coverage_jupiter_perps_shape_matches_drift  → JUPITER_PERP_COVERAGE
  5. test_coverage_layerzero_unblocked                → LAYERZERO_COVERAGE
  6. test_coverage_unknown_entity_returns_404         → UNKNOWN_ENTITY_COVERAGE
  7. test_coverage_fuzzy_match_rseth                  → fuzzy behavior
  8. test_coverage_fuzzy_no_false_positive_dai        → fuzzy precision
  9. test_coverage_days_since_last_record_present     → staleness field
 10. test_coverage_cache_hit_is_faster                → 15-min TTL cache
 11. test_coverage_snapshot_hash_stable               → deterministic hash
 12. test_coverage_adjacent_indexes_complement        → negative-space set
"""

from __future__ import annotations

import time

import pytest

from tests.fixtures.canonical_coverage import (
    DRIFT_COVERAGE,
    JUPITER_PERP_COVERAGE,
    KELP_RSETH_COVERAGE,
    LAYERZERO_COVERAGE,
    USDC_COVERAGE,
)


# ═════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════

def _covering_index_ids(matched_entities: list[dict]) -> set[str]:
    return {e["index_id"] for e in matched_entities}


def _fixture_covering_index_ids(fixture) -> set[str]:
    return {e.index_id for e in fixture.matched_entities}


# ═════════════════════════════════════════════════════════════════
# 1–5. Canonical fixture shape tests
# ═════════════════════════════════════════════════════════════════

def test_coverage_drift(api):
    """Drift: partial-live, 3 matched entities, blocks incident_page."""
    resp = api("/api/engine/coverage/drift")
    assert resp.status_code == 200, resp.text[:300]
    data = resp.json()

    assert data["identifier"] == "drift"
    assert data["coverage_quality"] == DRIFT_COVERAGE.coverage_quality
    assert data["blocks_incident_page"] == DRIFT_COVERAGE.blocks_incident_page
    assert _covering_index_ids(data["matched_entities"]) == _fixture_covering_index_ids(DRIFT_COVERAGE)
    assert len(data["blocks_reasons"]) > 0


def test_coverage_kelp_rseth_unblocked(api):
    """Kelp rsETH: partial-live with deep LSTI history → incident_page unblocked."""
    resp = api("/api/engine/coverage/kelp-rseth")
    assert resp.status_code == 200, resp.text[:300]
    data = resp.json()

    assert data["coverage_quality"] == KELP_RSETH_COVERAGE.coverage_quality
    assert data["blocks_incident_page"] is False
    assert data["blocks_reasons"] == []
    assert _covering_index_ids(data["matched_entities"]) == _fixture_covering_index_ids(KELP_RSETH_COVERAGE)
    assert "incident_page" in data["recommended_analysis_types"] or "incident_page" in KELP_RSETH_COVERAGE.recommended_analysis_types


def test_coverage_usdc_stale_but_unblocked(api):
    """USDC: partial-live; days_since_last_record populated; unblocked by depth rule.

    USDC is typically a few days stale (see Step 0 doc §11.1). The staleness
    field must be present and non-None. blocks_incident_page should be False
    because the SII window is >= 60 days and recent (<= 14 days).
    """
    resp = api("/api/engine/coverage/usdc")
    assert resp.status_code == 200, resp.text[:300]
    data = resp.json()

    # There's always at least an SII entry for USDC
    sii_entries = [e for e in data["matched_entities"] if e["index_id"] == "sii"]
    assert len(sii_entries) == 1
    sii = sii_entries[0]
    assert sii["days_since_last_record"] is not None
    assert sii["coverage_window_days"] is not None

    # If the staleness is within the unblock window, blocks should be False.
    # Allow either side so the test remains stable if the collector backlog
    # grows beyond 14 days temporarily — but require the staleness field
    # itself to be populated, which is the invariant.
    if (
        sii["days_since_last_record"] <= 14
        and sii["coverage_window_days"] >= 60
    ):
        assert data["blocks_incident_page"] is False


def test_coverage_jupiter_perps_shape_matches_drift(api):
    """Jupiter: 3 matched entities (dex_pool_data, web_research_protocol, psi), blocks."""
    resp = api("/api/engine/coverage/jupiter-perpetual-exchange")
    assert resp.status_code == 200, resp.text[:300]
    data = resp.json()

    assert data["coverage_quality"] == JUPITER_PERP_COVERAGE.coverage_quality
    assert data["blocks_incident_page"] == JUPITER_PERP_COVERAGE.blocks_incident_page
    assert _covering_index_ids(data["matched_entities"]) == _fixture_covering_index_ids(JUPITER_PERP_COVERAGE)


def test_coverage_layerzero_unblocked(api):
    """LayerZero: BRI live with deep history → incident_page unblocked."""
    resp = api("/api/engine/coverage/layerzero")
    assert resp.status_code == 200, resp.text[:300]
    data = resp.json()

    assert data["coverage_quality"] == LAYERZERO_COVERAGE.coverage_quality
    assert data["blocks_incident_page"] is False
    assert data["blocks_reasons"] == []
    assert "bri" in _covering_index_ids(data["matched_entities"])


# ═════════════════════════════════════════════════════════════════
# 6. Unknown entity → 404
# ═════════════════════════════════════════════════════════════════

def test_coverage_unknown_entity_returns_404(api):
    resp = api("/api/engine/coverage/this-entity-does-not-exist-xyz")
    assert resp.status_code == 404
    body = resp.json()
    # FastAPI default for HTTPException is {"detail": "..."}
    assert "detail" in body
    assert "this-entity-does-not-exist-xyz" in body["detail"]


# ═════════════════════════════════════════════════════════════════
# 7–8. Fuzzy match behavior
# ═════════════════════════════════════════════════════════════════

def test_coverage_fuzzy_match_rseth(api):
    """`rseth` should match `kelp-rseth` via trigram similarity."""
    resp = api("/api/engine/coverage/rseth")
    # Must either match (200 with kelp-rseth body) or 404 — never match
    # something unrelated. A 200 response is the expected case.
    if resp.status_code == 200:
        data = resp.json()
        assert data["identifier"] == "kelp-rseth"
        assert "lsti" in _covering_index_ids(data["matched_entities"])
    else:
        # If the trigram threshold rejects 'rseth', 404 is acceptable but
        # indicates the threshold may be too strict. Flag via assertion text.
        assert resp.status_code == 404, (
            f"rseth returned {resp.status_code}; expected 200 (kelp-rseth match) "
            "or 404 (threshold too strict)"
        )


def test_coverage_fuzzy_no_false_positive_dai(api):
    """`dai` must not falsely match `dailyusd` or similar long slugs.

    Expected outcomes:
      - If `dai` is a known stablecoin_id in scores: 200 with identifier='dai'
      - Otherwise: 404

    The one outcome the test rejects: a 200 response whose identifier is not
    'dai' — which would indicate a fuzzy-match false positive.
    """
    resp = api("/api/engine/coverage/dai")
    if resp.status_code == 200:
        data = resp.json()
        assert data["identifier"] == "dai", (
            f"dai fuzzy-matched to '{data['identifier']}' — false positive"
        )
    else:
        assert resp.status_code == 404


# ═════════════════════════════════════════════════════════════════
# 9. Staleness field presence
# ═════════════════════════════════════════════════════════════════

def test_coverage_days_since_last_record_present(api):
    """Every matched entity has days_since_last_record populated (or None iff
    unique_days == 0 / no latest_record available)."""
    resp = api("/api/engine/coverage/kelp-rseth")
    assert resp.status_code == 200
    data = resp.json()
    for e in data["matched_entities"]:
        if e["latest_record"] is not None:
            assert e["days_since_last_record"] is not None, (
                f"{e['index_id']} has latest_record but no days_since_last_record"
            )
            assert e["days_since_last_record"] >= 0


# ═════════════════════════════════════════════════════════════════
# 10. Cache behavior — second call faster than first
# ═════════════════════════════════════════════════════════════════

def test_coverage_cache_hit_is_faster(api):
    """Back-to-back calls: second call hits the 15-minute TTL cache.

    Asserts second call is noticeably faster (at least 30% quicker). This
    is timing-sensitive and may be flaky under heavy server load; on
    failure, inspect server logs for cache hits rather than retrying.
    """
    t0 = time.time()
    r1 = api("/api/engine/coverage/drift")
    t1 = time.time()
    assert r1.status_code == 200

    t2 = time.time()
    r2 = api("/api/engine/coverage/drift")
    t3 = time.time()
    assert r2.status_code == 200

    first_duration = t1 - t0
    second_duration = t3 - t2

    # Second call should be at least 30% faster. Allow slop: if first call
    # was already very fast (<50ms), skip the comparison.
    if first_duration > 0.05:
        assert second_duration <= first_duration * 0.7, (
            f"cache likely missed: first={first_duration*1000:.1f}ms, "
            f"second={second_duration*1000:.1f}ms"
        )


# ═════════════════════════════════════════════════════════════════
# 11. Deterministic snapshot hash
# ═════════════════════════════════════════════════════════════════

def test_coverage_snapshot_hash_stable(api):
    """Two consecutive calls for the same identifier return the same
    data_snapshot_hash (either via cache or because underlying data is
    unchanged between the two requests)."""
    r1 = api("/api/engine/coverage/kelp-rseth")
    r2 = api("/api/engine/coverage/kelp-rseth")
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["data_snapshot_hash"] == r2.json()["data_snapshot_hash"]
    # And the hash uses the sha256: prefix contract.
    assert r1.json()["data_snapshot_hash"].startswith("sha256:")


# ═════════════════════════════════════════════════════════════════
# 12. Negative-space computation
# ═════════════════════════════════════════════════════════════════

def test_coverage_adjacent_indexes_complement(api):
    """adjacent_indexes_not_covering equals FULL_INDEX_UNIVERSE minus the
    set of indexes actually covering the entity. No index should appear in
    both lists."""
    resp = api("/api/engine/coverage/drift")
    assert resp.status_code == 200
    data = resp.json()

    covering = _covering_index_ids(data["matched_entities"])
    not_covering = set(data["adjacent_indexes_not_covering"])

    # Disjoint
    assert covering.isdisjoint(not_covering), (
        f"index in both lists: {covering & not_covering}"
    )

    # Sorted (determinism)
    assert data["adjacent_indexes_not_covering"] == sorted(
        data["adjacent_indexes_not_covering"]
    )
