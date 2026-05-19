"""
Tests for the rebuilt system-health dashboard (app/data_layer/system_health.py).

Covers the verdict logic (the task's red/yellow/green rules) and the
JSONB / detail-summary / category helpers. Pure functions — no DB, runs
in CI. The DB-backed assembly (get_system_status) is exercised against a
live database via the /api/ops/system-status endpoint.
"""

import unittest

from app.data_layer.system_health import (
    compute_verdict,
    _as_dict,
    _summarize_details,
    _category_for,
)


def _sys(name, status):
    return {"system": name, "status": status}


class TestComputeVerdict(unittest.TestCase):
    """Verdict rules: red if any down; yellow if any degraded; green only
    if all healthy AND zero unacknowledged alerts in 24h."""

    def test_any_down_is_red(self):
        systems = [_sys("api", "down"), _sys("graph_edges", "down"),
                   _sys("scores", "healthy")]
        v = compute_verdict(systems, unacked_24h=0)
        assert v["level"] == "red"
        assert v["systems_down"] == 2
        assert "api" in v["reason"] and "graph_edges" in v["reason"]

    def test_degraded_no_down_is_yellow(self):
        systems = [_sys("integrity", "degraded"), _sys("scores", "healthy")]
        v = compute_verdict(systems, unacked_24h=0)
        assert v["level"] == "yellow"
        assert v["systems_degraded"] == 1

    def test_all_healthy_but_alerts_is_yellow(self):
        """Green requires zero unacknowledged alerts — alerts force yellow."""
        systems = [_sys("api", "healthy"), _sys("scores", "healthy")]
        v = compute_verdict(systems, unacked_24h=3)
        assert v["level"] == "yellow"
        assert "unacknowledged" in v["reason"]

    def test_all_healthy_no_alerts_is_green(self):
        systems = [_sys("api", "healthy"), _sys("scores", "healthy")]
        v = compute_verdict(systems, unacked_24h=0)
        assert v["level"] == "green"

    def test_down_takes_precedence_over_degraded_and_alerts(self):
        systems = [_sys("api", "down"), _sys("integrity", "degraded")]
        v = compute_verdict(systems, unacked_24h=9)
        assert v["level"] == "red"

    def test_headline_counts_only_healthy(self):
        """Headline must not claim 'all green' when it isn't — it reports
        healthy/total."""
        systems = [
            _sys("api", "down"), _sys("graph_edges", "down"),
            _sys("generic_indices", "degraded"), _sys("integrity", "degraded"),
        ] + [_sys(f"sys{i}", "healthy") for i in range(14)]
        v = compute_verdict(systems, unacked_24h=16)
        assert v["headline"] == "14/18 systems healthy"
        assert v["level"] == "red"

    def test_empty_systems(self):
        v = compute_verdict([], unacked_24h=0)
        assert v["headline"] == "no monitored systems found"
        assert v["systems_total"] == 0


class TestHelpers(unittest.TestCase):

    def test_as_dict_handles_dict_str_and_none(self):
        assert _as_dict({"a": 1}) == {"a": 1}
        assert _as_dict('{"a": 1}') == {"a": 1}
        assert _as_dict(None) == {}
        assert _as_dict("not json") == {}
        assert _as_dict("[1,2,3]") == {}  # non-object JSON -> {}

    def test_summarize_details_picks_known_keys(self):
        s = _summarize_details({"age_hours": 74.7, "last_built": "2026-05-15"})
        assert "age_hours=74.7" in s
        assert "last_built=2026-05-15" in s

    def test_summarize_details_empty(self):
        assert _summarize_details(None) == ""
        assert _summarize_details({}) == ""

    def test_category_for_uses_schema_then_prefix(self):
        assert _category_for("wallet_graph", "wallet_edges_archive") == "wallet"
        assert _category_for("public", "rpi_doc_scores") == "rpi"
        assert _category_for("public", "mempool_observations") == "other"


if __name__ == "__main__":
    unittest.main()
