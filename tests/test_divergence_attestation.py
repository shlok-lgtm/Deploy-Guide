"""Tests that divergence attestation is wired into the enrichment pipeline."""
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock


def test_divergence_attestation_called():
    """_run_divergence should call attest_state with the divergence_signals domain."""
    fake_result = {
        "divergence_signals": [
            {"type": "capital_flow", "severity": "notable"},
        ],
        "summary": {"total_signals": 1},
    }

    with patch("app.divergence.detect_all_divergences", new_callable=AsyncMock, return_value=fake_result) as mock_detect, \
         patch("app.state_attestation.attest_state") as mock_attest:

        # Import after patching so the lazy import inside _run_divergence picks up the mock
        from app.enrichment_worker import build_pipeline
        pipeline = build_pipeline()

        # Find the divergence task
        task = next(t for t in pipeline.tasks if t.name == "divergence_detection")
        result = asyncio.get_event_loop().run_until_complete(task.func())

        # detect_all_divergences was called
        mock_detect.assert_awaited_once_with(store=True)

        # attest_state was called with the right domain and records shape
        mock_attest.assert_called_once()
        call_args = mock_attest.call_args
        assert call_args[0][0] == "divergence_signals"
        assert call_args[0][1] == [{"type": "capital_flow", "severity": "notable"}]

        # Original result is returned unchanged
        assert result == fake_result


def test_divergence_attestation_skipped_when_no_signals():
    """attest_state should NOT be called when there are zero signals."""
    fake_result = {
        "divergence_signals": [],
        "summary": {"total_signals": 0},
    }

    with patch("app.divergence.detect_all_divergences", new_callable=AsyncMock, return_value=fake_result), \
         patch("app.state_attestation.attest_state") as mock_attest:

        from app.enrichment_worker import build_pipeline
        pipeline = build_pipeline()
        task = next(t for t in pipeline.tasks if t.name == "divergence_detection")
        result = asyncio.get_event_loop().run_until_complete(task.func())

        mock_attest.assert_not_called()
        assert result == fake_result


def test_divergence_attestation_failure_does_not_break_result():
    """If attest_state raises, _run_divergence still returns the result."""
    fake_result = {
        "divergence_signals": [
            {"type": "capital_flow", "severity": "notable"},
        ],
        "summary": {"total_signals": 1},
    }

    with patch("app.divergence.detect_all_divergences", new_callable=AsyncMock, return_value=fake_result), \
         patch("app.state_attestation.attest_state", side_effect=RuntimeError("db down")), \
         patch("app.worker._record_cycle_error") as mock_record:

        from app.enrichment_worker import build_pipeline
        pipeline = build_pipeline()
        task = next(t for t in pipeline.tasks if t.name == "divergence_detection")
        result = asyncio.get_event_loop().run_until_complete(task.func())

        # Result still returned despite attestation failure
        assert result == fake_result

        # Error was recorded
        mock_record.assert_called_once()
        call_kwargs = mock_record.call_args
        assert call_kwargs[1].get("error_type", call_kwargs[0][0] if call_kwargs[0] else None) == "divergence_attestation_failure" or \
               (len(call_kwargs[0]) > 0 and call_kwargs[0][0] == "divergence_attestation_failure")
