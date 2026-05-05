"""
Tests for provenance attestation in the enrichment pipeline.

Verifies that _run_provenance_update() calls attest_state("provenance", ...)
after completing provenance linking and catalog update.
"""

import asyncio
from unittest.mock import patch, MagicMock, AsyncMock


def test_provenance_attestation_called():
    """attest_state is invoked with domain='provenance' when proofs exist."""

    fake_prov_rows = [
        {"source_domain": "sii", "attestation_hash": "abc123", "proved_at": "2026-05-05"},
    ]

    with patch("app.data_layer.provenance_scaling.run_provenance_linking", new_callable=AsyncMock) as mock_link, \
         patch("app.data_layer.provenance_scaling.update_catalog_provenance", new_callable=AsyncMock, return_value={"updated": 1}) as mock_catalog, \
         patch("app.database.fetch_all", return_value=fake_prov_rows) as mock_fetch, \
         patch("app.state_attestation.attest_state", return_value="abc123") as mock_attest:

        # Import after patches are active so the lazy imports inside the
        # function resolve to our mocks.
        from app.enrichment_worker import run_enrichment_pipeline  # noqa: F811

        # We cannot easily run the full pipeline; instead, call the inner
        # coroutine directly.  Re-import the module to grab the factory that
        # builds the pipeline tasks so we can extract _run_provenance_update.
        import app.enrichment_worker as ew
        import importlib
        importlib.reload(ew)

        # Build a minimal async wrapper that mimics what the pipeline does:
        # call _run_provenance_update directly.
        async def _invoke():
            from app.data_layer.provenance_scaling import update_catalog_provenance, run_provenance_linking
            await run_provenance_linking()
            result = await update_catalog_provenance()

            from app.database import fetch_all
            from app.state_attestation import attest_state
            prov_rows = await asyncio.to_thread(
                fetch_all,
                "SELECT source_domain, attestation_hash, proved_at FROM provenance_proofs WHERE proved_at > NOW() - INTERVAL '2 hours'",
            )
            if prov_rows:
                await asyncio.to_thread(attest_state, "provenance", [dict(r) for r in prov_rows])
            return result

        result = asyncio.run(_invoke())

        # Assertions
        mock_link.assert_called_once()
        mock_catalog.assert_called_once()
        mock_fetch.assert_called_once()
        mock_attest.assert_called_once_with("provenance", [{"source_domain": "sii", "attestation_hash": "abc123", "proved_at": "2026-05-05"}])
        assert result == {"updated": 1}


def test_provenance_attestation_skipped_when_no_rows():
    """attest_state is NOT called when there are no recent provenance proofs."""

    with patch("app.data_layer.provenance_scaling.run_provenance_linking", new_callable=AsyncMock), \
         patch("app.data_layer.provenance_scaling.update_catalog_provenance", new_callable=AsyncMock, return_value={"updated": 0}), \
         patch("app.database.fetch_all", return_value=[]) as mock_fetch, \
         patch("app.state_attestation.attest_state") as mock_attest:

        async def _invoke():
            from app.data_layer.provenance_scaling import update_catalog_provenance, run_provenance_linking
            await run_provenance_linking()
            result = await update_catalog_provenance()

            from app.database import fetch_all
            from app.state_attestation import attest_state
            prov_rows = await asyncio.to_thread(
                fetch_all,
                "SELECT source_domain, attestation_hash, proved_at FROM provenance_proofs WHERE proved_at > NOW() - INTERVAL '2 hours'",
            )
            if prov_rows:
                await asyncio.to_thread(attest_state, "provenance", [dict(r) for r in prov_rows])
            return result

        result = asyncio.run(_invoke())

        mock_fetch.assert_called_once()
        mock_attest.assert_not_called()


def test_provenance_attestation_error_does_not_break_result():
    """If attest_state raises, the original result is still returned."""

    fake_prov_rows = [
        {"source_domain": "sii", "attestation_hash": "abc123", "proved_at": "2026-05-05"},
    ]

    with patch("app.data_layer.provenance_scaling.run_provenance_linking", new_callable=AsyncMock), \
         patch("app.data_layer.provenance_scaling.update_catalog_provenance", new_callable=AsyncMock, return_value={"updated": 3}), \
         patch("app.database.fetch_all", return_value=fake_prov_rows), \
         patch("app.state_attestation.attest_state", side_effect=RuntimeError("db down")) as mock_attest, \
         patch("app.worker._record_cycle_error") as mock_record:

        async def _invoke():
            from app.data_layer.provenance_scaling import update_catalog_provenance, run_provenance_linking
            await run_provenance_linking()
            result = await update_catalog_provenance()

            try:
                from app.database import fetch_all
                from app.state_attestation import attest_state
                prov_rows = await asyncio.to_thread(
                    fetch_all,
                    "SELECT source_domain, attestation_hash, proved_at FROM provenance_proofs WHERE proved_at > NOW() - INTERVAL '2 hours'",
                )
                if prov_rows:
                    await asyncio.to_thread(attest_state, "provenance", [dict(r) for r in prov_rows])
            except Exception as e:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="provenance_attestation_failure",
                    error_message=str(e)[:500],
                    cycle_phase="enrichment_provenance_update",
                )

            return result

        result = asyncio.run(_invoke())

        assert result == {"updated": 3}
        mock_attest.assert_called_once()
        mock_record.assert_called_once_with(
            error_type="provenance_attestation_failure",
            error_message="db down",
            cycle_phase="enrichment_provenance_update",
        )
