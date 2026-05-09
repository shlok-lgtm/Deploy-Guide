"""
Tests for the integrity work-availability heuristic.

Verifies that classify_freshness distinguishes:
  * 'ok'     — fresh enough (age <= 2 * expected_hours)
  * 'broken' — stale AND upstream work was due
  * 'quiet'  — stale BUT no upstream work was due (gate-blocked / no signals)

Also verifies that domains without a registered heuristic preserve the
legacy "stale = broken" behaviour.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch


def _ts_hours_ago(hours: float):
    return datetime.now(timezone.utc) - timedelta(hours=hours)


# ---------------------------------------------------------------------------
# classify_freshness — basic state machine
# ---------------------------------------------------------------------------


def test_classify_returns_ok_when_within_window():
    from app.integrity_heuristics import classify_freshness

    # 1.5h old, expected every 2h -> within 2x window
    assert classify_freshness("sii_components", age_hours=1.5, expected_hours=2) == "ok"


def test_classify_returns_ok_at_window_boundary():
    from app.integrity_heuristics import classify_freshness

    # exactly 2x expected -> still ok (matches existing _check_freshness rule)
    assert classify_freshness("sii_components", age_hours=4.0, expected_hours=2) == "ok"


def test_classify_returns_broken_when_stale_and_work_due():
    from app.integrity_heuristics import classify_freshness

    with patch("app.integrity_heuristics.work_should_have_happened", return_value=True):
        result = classify_freshness("psi_discoveries", age_hours=72, expected_hours=24)
    assert result == "broken"


def test_classify_returns_quiet_when_stale_but_no_work_due():
    from app.integrity_heuristics import classify_freshness

    with patch("app.integrity_heuristics.work_should_have_happened", return_value=False):
        result = classify_freshness("psi_discoveries", age_hours=72, expected_hours=24)
    assert result == "quiet"


def test_classify_with_no_attestation_and_heuristic_says_quiet():
    """No attestation row + heuristic says no work due -> quiet, not broken."""
    from app.integrity_heuristics import classify_freshness

    with patch("app.integrity_heuristics.work_should_have_happened", return_value=False):
        result = classify_freshness("psi_discoveries", age_hours=None, expected_hours=24)
    assert result == "quiet"


# ---------------------------------------------------------------------------
# Default behaviour — domains without a heuristic
# ---------------------------------------------------------------------------


def test_unregistered_domain_defaults_to_broken_when_stale():
    """Preserve legacy alert behaviour for any domain without a heuristic."""
    from app.integrity_heuristics import classify_freshness, has_heuristic, work_should_have_happened

    # 'edges' deliberately has no heuristic registered (see docs).
    assert not has_heuristic("edges")
    # work_should_have_happened defaults to True for unregistered domains.
    assert work_should_have_happened("edges") is True
    assert classify_freshness("edges", age_hours=200, expected_hours=24) == "broken"


def test_heuristic_exception_defaults_to_work_due():
    """If a heuristic raises, we default to alerting (conservative)."""
    from app.integrity_heuristics import work_should_have_happened, _HEURISTICS

    with patch.dict(_HEURISTICS, {"psi_discoveries": lambda: (_ for _ in ()).throw(RuntimeError("boom"))}):
        # Heuristic raises -> default to True (work was due) -> alert preserved.
        assert work_should_have_happened("psi_discoveries") is True


# ---------------------------------------------------------------------------
# Per-domain heuristics — psi_discoveries
# ---------------------------------------------------------------------------


def test_psi_discoveries_quiet_when_gate_closed_and_no_pending():
    """Snapshot fresh + zero pending promotions -> no work due -> quiet."""
    from app.integrity_heuristics import _psi_discoveries_work_due

    with patch("app.integrity_heuristics._safe_fetch_one") as mock_fetch:
        mock_fetch.side_effect = [
            {"latest": _ts_hours_ago(2)},   # gate closed (snapshot 2h old)
            {"cnt": 0},                     # nothing pending in backlog
        ]
        assert _psi_discoveries_work_due() is False


def test_psi_discoveries_broken_when_gate_open():
    """Snapshot stale -> gate open -> psi_expansion should have run -> broken."""
    from app.integrity_heuristics import _psi_discoveries_work_due

    with patch("app.integrity_heuristics._safe_fetch_one") as mock_fetch:
        mock_fetch.return_value = {"latest": _ts_hours_ago(48)}  # gate open
        assert _psi_discoveries_work_due() is True


def test_psi_discoveries_broken_when_pending_promotions_exist():
    """Snapshot fresh but pending promotions -> work was due -> broken."""
    from app.integrity_heuristics import _psi_discoveries_work_due

    with patch("app.integrity_heuristics._safe_fetch_one") as mock_fetch:
        mock_fetch.side_effect = [
            {"latest": _ts_hours_ago(2)},   # gate closed
            {"cnt": 4},                     # 4 protocols ready to promote
        ]
        assert _psi_discoveries_work_due() is True


# ---------------------------------------------------------------------------
# Per-domain heuristics — provenance / actors / rpi / sii / divergence
# ---------------------------------------------------------------------------


def test_provenance_quiet_when_recent_proof_exists():
    from app.integrity_heuristics import _provenance_work_due

    with patch("app.integrity_heuristics._safe_fetch_one",
               return_value={"latest": _ts_hours_ago(1)}):
        assert _provenance_work_due() is False


def test_provenance_broken_when_no_recent_proofs():
    from app.integrity_heuristics import _provenance_work_due

    with patch("app.integrity_heuristics._safe_fetch_one",
               return_value={"latest": _ts_hours_ago(12)}):
        assert _provenance_work_due() is True


def test_actors_quiet_when_recent_classification_cycle():
    from app.integrity_heuristics import _actors_work_due

    with patch("app.integrity_heuristics._safe_fetch_one",
               return_value={"latest": _ts_hours_ago(0.25)}):
        assert _actors_work_due() is False


def test_rpi_components_quiet_when_score_table_fresh():
    from app.integrity_heuristics import _rpi_components_work_due

    # rpi_scoring is gated on rpi_scores within 24h
    with patch("app.integrity_heuristics._safe_fetch_one",
               return_value={"latest": _ts_hours_ago(8)}):
        assert _rpi_components_work_due() is False


def test_sii_components_quiet_when_scoring_just_ran():
    from app.integrity_heuristics import _sii_components_work_due

    with patch("app.integrity_heuristics._safe_fetch_one",
               return_value={"latest": _ts_hours_ago(0.5)}):
        assert _sii_components_work_due() is False


def test_divergence_quiet_when_detector_ran_in_window():
    from app.integrity_heuristics import _divergence_signals_work_due

    with patch("app.integrity_heuristics._safe_fetch_one",
               return_value={"latest": _ts_hours_ago(2)}):
        assert _divergence_signals_work_due() is False


def test_divergence_broken_when_detector_silent_too_long():
    from app.integrity_heuristics import _divergence_signals_work_due

    with patch("app.integrity_heuristics._safe_fetch_one",
               return_value={"latest": _ts_hours_ago(8)}):
        assert _divergence_signals_work_due() is True


# ---------------------------------------------------------------------------
# Coherence integration — ensure quiet domains don't surface as issues
# ---------------------------------------------------------------------------


def test_check_freshness_suppresses_quiet_domains():
    """A stale domain whose heuristic returns 'quiet' should not appear in issues."""
    from app import coherence

    fake_rows = [
        {"domain": "psi_discoveries", "cycle_timestamp": _ts_hours_ago(72)},
    ]

    with patch.object(coherence, "ALL_DOMAINS", ["psi_discoveries"]), \
         patch.object(coherence, "DOMAIN_FREQUENCIES", {"psi_discoveries": 24}), \
         patch.object(coherence, "fetch_all", return_value=fake_rows), \
         patch("app.integrity_heuristics.work_should_have_happened", return_value=False):

        issues = coherence._check_freshness()

    # Stale by raw cadence (72h > 48h threshold) but quiet -> no issue
    assert issues == []


def test_check_freshness_still_alerts_on_broken_domain():
    """A stale domain whose heuristic returns 'broken' should still produce an alert."""
    from app import coherence

    fake_rows = [
        {"domain": "psi_discoveries", "cycle_timestamp": _ts_hours_ago(72)},
    ]

    with patch.object(coherence, "ALL_DOMAINS", ["psi_discoveries"]), \
         patch.object(coherence, "DOMAIN_FREQUENCIES", {"psi_discoveries": 24}), \
         patch.object(coherence, "fetch_all", return_value=fake_rows), \
         patch("app.integrity_heuristics.work_should_have_happened", return_value=True):

        issues = coherence._check_freshness()

    assert len(issues) == 1
    assert issues[0]["domain"] == "psi_discoveries"
    assert issues[0]["check"] == "freshness"


def test_check_freshness_unregistered_domain_preserves_legacy_behaviour():
    """A domain with no heuristic should still alert when stale (default broken)."""
    from app import coherence

    fake_rows = [
        {"domain": "edges", "cycle_timestamp": _ts_hours_ago(200)},
    ]

    with patch.object(coherence, "ALL_DOMAINS", ["edges"]), \
         patch.object(coherence, "DOMAIN_FREQUENCIES", {"edges": 12}), \
         patch.object(coherence, "fetch_all", return_value=fake_rows):

        issues = coherence._check_freshness()

    assert len(issues) == 1
    assert issues[0]["domain"] == "edges"
