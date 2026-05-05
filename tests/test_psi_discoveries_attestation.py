"""Tests for psi_discoveries attestation in the enrichment pipeline."""

import asyncio
from unittest.mock import patch, MagicMock


def test_psi_expansion_attests_when_discovered_or_promoted():
    """When discover_protocols or promote_eligible_protocols return >0,
    attest_state should be called with domain='psi_discoveries'."""
    mock_attest = MagicMock()

    with (
        patch("app.collectors.psi_collector.collect_collateral_exposure"),
        patch("app.collectors.psi_collector.sync_collateral_to_backlog", return_value=5),
        patch("app.collectors.psi_collector.discover_protocols", return_value=3),
        patch("app.collectors.psi_collector.enrich_protocol_backlog", return_value=2),
        patch("app.collectors.psi_collector.promote_eligible_protocols", return_value=1),
        patch("app.state_attestation.attest_state", mock_attest),
    ):
        from app.enrichment_worker import _run_psi_expansion

        result = asyncio.run(_run_psi_expansion())

        assert result == {"synced": 5, "discovered": 3, "enriched": 2, "promoted": 1}
        mock_attest.assert_called_once()
        call_args = mock_attest.call_args
        assert call_args[0][0] == "psi_discoveries"
        assert call_args[0][1] == [
            {"synced": 5, "discovered": 3, "enriched": 2, "promoted": 1}
        ]


def test_psi_expansion_skips_attestation_when_nothing_discovered():
    """When both discovered=0 and promoted=0, attest_state should NOT be called."""
    mock_attest = MagicMock()

    with (
        patch("app.collectors.psi_collector.collect_collateral_exposure"),
        patch("app.collectors.psi_collector.sync_collateral_to_backlog", return_value=2),
        patch("app.collectors.psi_collector.discover_protocols", return_value=0),
        patch("app.collectors.psi_collector.enrich_protocol_backlog", return_value=0),
        patch("app.collectors.psi_collector.promote_eligible_protocols", return_value=0),
        patch("app.state_attestation.attest_state", mock_attest),
    ):
        from app.enrichment_worker import _run_psi_expansion

        result = asyncio.run(_run_psi_expansion())

        assert result == {"synced": 2, "discovered": 0, "enriched": 0, "promoted": 0}
        mock_attest.assert_not_called()
