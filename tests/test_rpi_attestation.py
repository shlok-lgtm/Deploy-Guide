"""Verify that _run_rpi() in enrichment_worker calls attest_state for rpi_components."""

import asyncio
from unittest.mock import patch, MagicMock, AsyncMock


def test_run_rpi_calls_attest_state():
    """The rpi_scoring enrichment task must attest rpi_components after scoring."""
    fake_results = [{"protocol_slug": "aave", "overall_score": 75.5}]

    with (
        patch("app.rpi.snapshot_collector.collect_snapshot_proposals"),
        patch("app.rpi.tally_collector.collect_tally_proposals"),
        patch("app.rpi.parameter_collector.collect_parameter_changes"),
        patch("app.rpi.forum_scraper.scrape_all_forums"),
        patch("app.rpi.forum_scraper.update_vendor_diversity_lens"),
        patch("app.rpi.docs_scorer.score_all_docs"),
        patch("app.rpi.incident_detector.run_incident_detection"),
        patch("app.rpi.scorer.run_rpi_scoring", return_value=fake_results),
        patch("app.state_attestation.attest_state") as mock_attest,
    ):
        from app.enrichment_worker import run_enrichment_pipeline, EnrichmentPipeline

        # Patch the pipeline so we only run the rpi_scoring task
        original_add = EnrichmentPipeline.add

        captured_rpi_func = None

        def selective_add(self, task):
            nonlocal captured_rpi_func
            if task.name == "rpi_scoring":
                captured_rpi_func = task.func
                # Remove the gate so it always runs
                task.gate_check = None
                original_add(self, task)
            # Skip all other tasks

        with patch.object(EnrichmentPipeline, "add", selective_add):
            asyncio.run(run_enrichment_pipeline())

        mock_attest.assert_called_once()
        call_args = mock_attest.call_args
        assert call_args[0][0] == "rpi_components"
        assert call_args[0][1] == [{"slug": "aave", "score": 75.5}]
