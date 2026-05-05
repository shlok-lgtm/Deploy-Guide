"""
Test: edge_build_status is NOT marked 'complete' when API fails on first page.
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock


class TestEdgeBuildStatusOnApiFailure(unittest.TestCase):
    """Verify that build_edges_for_wallet writes status='api_failure'
    when _fetch_tokentx_page returns None on page 1."""

    @patch("app.indexer.edges.execute_async", new_callable=AsyncMock)
    @patch("app.indexer.edges.get_chain_contracts")
    @patch("app.indexer.edges._fetch_tokentx_page", new_callable=AsyncMock)
    def test_first_page_none_writes_api_failure(self, mock_fetch, mock_contracts, mock_execute):
        from app.indexer.edges import _FetchResult
        mock_fetch.return_value = _FetchResult(error_type="explorer_timeout", error_detail="test")
        mock_contracts.return_value = {}

        client = AsyncMock()

        result = asyncio.run(
            __import__("app.indexer.edges", fromlist=["build_edges_for_wallet"]).build_edges_for_wallet(
                client, "0x1234567890abcdef1234567890abcdef12345678", "fake_key",
                max_pages=3, chain="ethereum",
            )
        )

        assert result["api_failure"] is True
        assert result["transfers_processed"] == 0
        assert result["edges_upserted"] == 0

        # Find the execute_async call that writes to edge_build_status
        status_calls = [
            c for c in mock_execute.call_args_list
            if "edge_build_status" in str(c)
        ]
        assert len(status_calls) >= 1, "Expected at least one edge_build_status write"

        status_sql = str(status_calls[0])
        assert "api_failure" in status_sql, f"Expected status='api_failure' in SQL, got: {status_sql}"
        assert "last_built_at = NOW()" not in status_sql or "build_attempted_at = NOW()" in status_sql, \
            "last_built_at should NOT be updated on API failure"

    @patch("app.indexer.edges.execute_async", new_callable=AsyncMock)
    @patch("app.indexer.edges.get_chain_contracts")
    @patch("app.indexer.edges._fetch_tokentx_page", new_callable=AsyncMock)
    def test_successful_fetch_writes_complete(self, mock_fetch, mock_contracts, mock_execute):
        from app.indexer.edges import _FetchResult
        mock_fetch.side_effect = [
            _FetchResult(transfers=[{"contractAddress": "0xabc", "from": "0x111", "to": "0x222",
              "value": "1000000", "timeStamp": "1700000000"}]),
            _FetchResult(transfers=[]),
        ]
        mock_contracts.return_value = {
            "0xabc": {"decimals": 6, "symbol": "USDC"},
        }

        client = AsyncMock()

        result = asyncio.run(
            __import__("app.indexer.edges", fromlist=["build_edges_for_wallet"]).build_edges_for_wallet(
                client, "0x1234567890abcdef1234567890abcdef12345678", "fake_key",
                max_pages=3, chain="ethereum",
            )
        )

        assert result["api_failure"] is False

        status_calls = [
            c for c in mock_execute.call_args_list
            if "edge_build_status" in str(c)
        ]
        assert len(status_calls) >= 1
        status_sql = str(status_calls[0])
        assert "'complete'" in status_sql, f"Expected status='complete', got: {status_sql}"


if __name__ == "__main__":
    unittest.main()
