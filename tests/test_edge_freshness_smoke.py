"""
Edge builder freshness — smoke test + freshness-gate regression test.

Smoke test (DB-backed):
    Fails when wallet_graph.wallet_edges has not received a NEW row within
    N hours. N defaults to 8 and is overridable via EDGE_FRESHNESS_MAX_HOURS.
    "New row" is measured by MAX(created_at) — created_at is set only on a
    genuine INSERT, never on ON CONFLICT DO UPDATE, so it is the true signal
    of edge-construction progress (same column the graph_edges health check
    reads). Skips cleanly when DATABASE_URL is unset.

    Run:
        DATABASE_URL=<url> pytest tests/test_edge_freshness_smoke.py -v
        EDGE_FRESHNESS_MAX_HOURS=14 DATABASE_URL=<url> pytest ... -v

Regression test (no DB, runs in CI):
    Pins run_edge_builder_scheduled's freshness gate to edge_build_status
    .build_attempted_at. The pre-fix gate keyed on wallet_graph.wallet_edges
    .updated_at, which decay_edges() and the sibling transfer_edge_builder
    keep perpetually fresh — that closed the gate forever and silently
    stalled the edge builder from 2026-05-15 onward.
"""

import asyncio
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Smoke test — DB-backed edge freshness
# ---------------------------------------------------------------------------

DEFAULT_MAX_HOURS = 8.0


def _max_hours() -> float:
    """Freshness budget in hours. Default 8, overridable via env."""
    raw = os.environ.get("EDGE_FRESHNESS_MAX_HOURS")
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return DEFAULT_MAX_HOURS


@pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="DATABASE_URL not set — skipping DB-backed edge freshness smoke test.",
)
def test_wallet_edges_received_a_row_recently():
    """wallet_graph.wallet_edges must have a row created within N hours."""
    import psycopg2

    max_hours = _max_hours()
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(created_at),
                       EXTRACT(EPOCH FROM (NOW() - MAX(created_at))) / 3600.0
                FROM wallet_graph.wallet_edges
                """
            )
            last_created, age_hours = cur.fetchone()

    assert last_created is not None, (
        "wallet_graph.wallet_edges is empty — the edge builder has never "
        "produced a row."
    )
    assert age_hours is not None and age_hours <= max_hours, (
        f"Edge builder stalled: the newest wallet_graph.wallet_edges row is "
        f"{age_hours:.1f}h old (created_at={last_created}), which exceeds the "
        f"{max_hours:.1f}h freshness budget. New rows are not landing — check "
        f"run_edge_builder_scheduled's freshness gate in app/indexer/edges.py."
    )


# ---------------------------------------------------------------------------
# Regression test — freshness gate keys on the right column
# ---------------------------------------------------------------------------

class TestEdgeBuilderFreshnessGate(unittest.TestCase):
    """run_edge_builder_scheduled must gate on edge_build_status
    .build_attempted_at, not wallet_graph.wallet_edges.updated_at."""

    def _run_scheduled(self, fetch_return):
        """Invoke run_edge_builder_scheduled('ethereum') with the freshness
        query mocked to return `fetch_return`. Returns (payload, mocks)."""
        from app.indexer import edges

        fetch_mock = AsyncMock(return_value=fetch_return)
        builder_mock = AsyncMock(return_value={
            "wallets_processed": 200,
            "total_edges_created": 1234,
            "total_transfers": 5678,
        })
        coherence_mock = AsyncMock(return_value=None)

        with patch.object(edges, "fetch_one_async", fetch_mock), \
             patch.object(edges, "run_edge_builder", builder_mock), \
             patch.object(edges, "_assert_edges_coherence", coherence_mock), \
             patch("app.state_attestation.attest_state", lambda *a, **k: None):
            payload = asyncio.run(edges.run_edge_builder_scheduled("ethereum"))

        return payload, fetch_mock, builder_mock

    def test_gate_queries_build_attempted_at_not_updated_at(self):
        """The freshness query must read edge_build_status.build_attempted_at.
        Keying on wallet_edges.updated_at is the bug that stalled the builder."""
        stale = datetime.now(timezone.utc) - timedelta(hours=30)
        _, fetch_mock, _ = self._run_scheduled({"t": stale})

        sql = " ".join(str(fetch_mock.call_args[0][0]).lower().split())
        assert "build_attempted_at" in sql, (
            f"freshness gate must query build_attempted_at; got: {sql}"
        )
        assert "edge_build_status" in sql, (
            f"freshness gate must query edge_build_status; got: {sql}"
        )
        assert "updated_at" not in sql, (
            "freshness gate must NOT key on updated_at — decay_edges() and "
            f"transfer_edge_builder keep it perpetually fresh. SQL: {sql}"
        )

    def test_stale_build_attempt_runs_the_builder(self):
        """A build attempt 30h ago is past the 10h cadence — builder runs."""
        stale = datetime.now(timezone.utc) - timedelta(hours=30)
        payload, _, builder_mock = self._run_scheduled({"t": stale})

        assert payload["status"] == "ran", payload
        builder_mock.assert_awaited_once()

    def test_fresh_build_attempt_skips_the_builder(self):
        """A build attempt 1h ago is within the 10h cadence — builder skips."""
        fresh = datetime.now(timezone.utc) - timedelta(hours=1)
        payload, _, builder_mock = self._run_scheduled({"t": fresh})

        assert payload["status"] == "skipped_fresh", payload
        builder_mock.assert_not_awaited()

    def test_no_prior_attempt_runs_the_builder(self):
        """No build_attempted_at row yet — builder must run, not stall."""
        payload, _, builder_mock = self._run_scheduled({"t": None})

        assert payload["status"] == "ran", payload
        builder_mock.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
