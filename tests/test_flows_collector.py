"""
Tests for flows collector: structured _FlowsFetchResult and cycle_errors recording.
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx


class TestFetchReturnsStructuredResult(unittest.TestCase):
    """_fetch_recent_transfers must return _FlowsFetchResult, not bare list."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_client(self, side_effect=None, response=None):
        client = AsyncMock(spec=httpx.AsyncClient)
        if side_effect:
            client.get.side_effect = side_effect
        elif response:
            client.get.return_value = response
        return client

    def _make_response(self, json_data):
        resp = MagicMock()
        resp.json.return_value = json_data
        return resp

    def test_fetch_returns_structured_result_on_rate_limit(self):
        """Rate-limit in body -> success=False, error_type='rate_limit'."""
        from app.collectors.flows import _fetch_recent_transfers, _FlowsFetchResult

        json_data = {"status": "0", "result": "Max rate limit reached"}
        client = self._make_client(response=self._make_response(json_data))

        result = self._run(_fetch_recent_transfers(client, "0xABC", "key"))

        self.assertIsInstance(result, _FlowsFetchResult)
        self.assertFalse(result.success)
        self.assertEqual(result.error_type, "rate_limit")
        self.assertEqual(result.transfers, [])
        self.assertIn("Max rate limit", result.error_message)

    def test_fetch_returns_structured_result_on_timeout(self):
        """httpx.TimeoutException -> success=False, error_type='timeout'."""
        from app.collectors.flows import _fetch_recent_transfers, _FlowsFetchResult

        client = self._make_client(side_effect=httpx.TimeoutException("timed out"))

        result = self._run(_fetch_recent_transfers(client, "0xABC", "key"))

        self.assertIsInstance(result, _FlowsFetchResult)
        self.assertFalse(result.success)
        self.assertEqual(result.error_type, "timeout")
        self.assertEqual(result.transfers, [])

    def test_fetch_returns_structured_result_on_transport_error(self):
        """httpx.HTTPError -> success=False, error_type='transport'."""
        from app.collectors.flows import _fetch_recent_transfers, _FlowsFetchResult

        client = self._make_client(side_effect=httpx.NetworkError("conn refused"))

        result = self._run(_fetch_recent_transfers(client, "0xABC", "key"))

        self.assertIsInstance(result, _FlowsFetchResult)
        self.assertFalse(result.success)
        self.assertEqual(result.error_type, "transport")

    def test_fetch_returns_structured_result_on_api_error(self):
        """status != '1' (non-rate-limit) -> success=False, error_type='api_error'."""
        from app.collectors.flows import _fetch_recent_transfers, _FlowsFetchResult

        json_data = {"status": "0", "result": "Invalid API Key"}
        client = self._make_client(response=self._make_response(json_data))

        result = self._run(_fetch_recent_transfers(client, "0xABC", "key"))

        self.assertIsInstance(result, _FlowsFetchResult)
        self.assertFalse(result.success)
        self.assertEqual(result.error_type, "api_error")

    def test_fetch_returns_structured_result_on_success(self):
        """Normal 200 with status '1' -> success=True, transfers populated."""
        from app.collectors.flows import _fetch_recent_transfers, _FlowsFetchResult

        transfers = [{"hash": "0x1", "from": "0xa", "to": "0xb", "value": "1000"}]
        json_data = {"status": "1", "result": transfers}
        client = self._make_client(response=self._make_response(json_data))

        result = self._run(_fetch_recent_transfers(client, "0xABC", "key"))

        self.assertIsInstance(result, _FlowsFetchResult)
        self.assertTrue(result.success)
        self.assertEqual(result.transfers, transfers)
        self.assertIsNone(result.error_type)


class TestCollectFlowsWritesCycleError(unittest.TestCase):
    """collect_flows_components must write cycle_errors on API failure."""

    def _run(self, coro):
        return asyncio.run(coro)

    @patch("app.collectors.flows._fetch_recent_transfers")
    @patch("app.collectors.flows._get_market_cap", return_value=1_000_000)
    @patch("app.collectors.flows._get_current_price", return_value=1.0)
    @patch("app.collectors.flows._get_stablecoin_config", return_value={
        "id": "usdc", "symbol": "USDC", "contract": "0xA0b8",
        "decimals": 6, "coingecko_id": "usd-coin",
    })
    @patch.dict("os.environ", {"ETHERSCAN_API_KEY": "fake_key"})
    def test_collect_flows_writes_cycle_error_on_failure(
        self, _cfg, _price, _mcap, mock_fetch
    ):
        """When _fetch_recent_transfers returns failure, cycle_errors gets a row."""
        from app.collectors.flows import collect_flows_components, _FlowsFetchResult

        mock_fetch.return_value = _FlowsFetchResult(
            success=False,
            transfers=[],
            error_type="rate_limit",
            error_message="Max rate limit reached",
        )

        with patch("app.collectors.flows._write_cycle_error") as mock_write:
            result = self._run(collect_flows_components(
                AsyncMock(spec=httpx.AsyncClient), "usdc"
            ))

            self.assertEqual(result, [])
            mock_write.assert_called_once()
            call_kwargs = mock_write.call_args
            # Check error_type includes the flows_ prefix and the error class
            self.assertIn("flows_rate_limit", str(call_kwargs))
            # Check error_message includes the stablecoin_id
            self.assertIn("usdc", str(call_kwargs))


if __name__ == "__main__":
    unittest.main()
