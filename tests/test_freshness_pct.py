"""Tests for _calc_freshness_pct helper in app/worker.py."""

from app.worker import _calc_freshness_pct


def test_data_freshness_pct_scales_correctly():
    """42 out of 84 components -> 50.0%."""
    score_data = {"component_count": 42, "components_total": 84}
    assert _calc_freshness_pct(score_data) == 50.0


def test_data_freshness_pct_clamps_at_100():
    """84 components with only 51 total should clamp at 100.0, not 164.7."""
    score_data = {"component_count": 84, "components_total": 51}
    assert _calc_freshness_pct(score_data) == 100.0


def test_data_freshness_pct_handles_missing_total():
    """When components_total is absent, fallback to have/have = 100.0."""
    score_data = {"component_count": 42}
    assert _calc_freshness_pct(score_data) == 100.0


def test_data_freshness_pct_handles_zero():
    """0 components with 0 total -> 0.0."""
    score_data = {"component_count": 0, "components_total": 0}
    assert _calc_freshness_pct(score_data) == 0.0
