"""
Test: _fetch_tokentx_page error classification.
Each exception type should produce a specific error_type and write to cycle_errors.
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

import httpx


class TestFetchTokentxErrorClassification(unittest.TestCase):

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_client(self, side_effect=None, response=None):
        client = AsyncMock(spec=httpx.AsyncClient)
        if side_effect:
            client.get.side_effect = side_effect
        elif response:
            client.get.return_value = response
        return client

    def _make_response(self, status_code, json_data=None, text=""):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_data or {}
        resp.text = text
        return resp

    @patch("app.indexer.edges._record_explorer_error")
    def test_timeout_returns_transient(self, mock_record):
        from app.indexer.edges import _fetch_tokentx_page
        client = self._make_client(side_effect=httpx.TimeoutException("timed out"))
        result = self._run(_fetch_tokentx_page(client, "0x1234", "key"))
        assert not result.ok
        assert result.error_type == "explorer_timeout"
        assert result.transient
        mock_record.assert_called_once()
        assert mock_record.call_args[0][0] == "explorer_timeout"

    @patch("app.indexer.edges._record_explorer_error")
    def test_network_error_returns_transient(self, mock_record):
        from app.indexer.edges import _fetch_tokentx_page
        client = self._make_client(side_effect=httpx.NetworkError("connection reset"))
        result = self._run(_fetch_tokentx_page(client, "0x1234", "key"))
        assert not result.ok
        assert result.error_type == "explorer_network_error"
        assert result.transient

    @patch("app.indexer.edges._record_explorer_error")
    def test_http_500_returns_transient(self, mock_record):
        from app.indexer.edges import _fetch_tokentx_page
        resp = self._make_response(502)
        client = self._make_client(response=resp)
        result = self._run(_fetch_tokentx_page(client, "0x1234", "key"))
        assert not result.ok
        assert result.error_type == "explorer_server_error"
        assert result.transient

    @patch("app.indexer.edges._record_explorer_error")
    def test_http_401_returns_auth_failure(self, mock_record):
        from app.indexer.edges import _fetch_tokentx_page
        resp = self._make_response(401)
        client = self._make_client(response=resp)
        result = self._run(_fetch_tokentx_page(client, "0x1234", "key"))
        assert not result.ok
        assert result.error_type == "explorer_auth_failure"
        assert not result.transient

    @patch("app.indexer.edges._record_explorer_error")
    def test_http_429_returns_rate_limit(self, mock_record):
        from app.indexer.edges import _fetch_tokentx_page
        resp = self._make_response(429)
        client = self._make_client(response=resp)
        result = self._run(_fetch_tokentx_page(client, "0x1234", "key"))
        assert not result.ok
        assert result.error_type == "explorer_rate_limit"
        assert not result.transient

    @patch("app.indexer.edges._record_explorer_error")
    def test_malformed_json_returns_error(self, mock_record):
        from app.indexer.edges import _fetch_tokentx_page
        resp = self._make_response(200)
        resp.json.side_effect = ValueError("invalid json")
        client = self._make_client(response=resp)
        result = self._run(_fetch_tokentx_page(client, "0x1234", "key"))
        assert not result.ok
        assert result.error_type == "explorer_malformed_response"
        assert not result.transient

    @patch("app.indexer.edges._record_explorer_error")
    def test_rate_limit_in_body_returns_error(self, mock_record):
        from app.indexer.edges import _fetch_tokentx_page
        resp = self._make_response(200, json_data={"status": "0", "result": "Max rate limit reached"})
        client = self._make_client(response=resp)
        result = self._run(_fetch_tokentx_page(client, "0x1234", "key"))
        assert not result.ok
        assert result.error_type == "explorer_rate_limit"

    def test_success_returns_transfers(self):
        from app.indexer.edges import _fetch_tokentx_page
        transfers = [{"hash": "0xabc", "from": "0x1", "to": "0x2"}]
        resp = self._make_response(200, json_data={"status": "1", "result": transfers})
        client = self._make_client(response=resp)
        result = self._run(_fetch_tokentx_page(client, "0x1234", "key"))
        assert result.ok
        assert result.transfers == transfers
        assert not result.transient

    def test_empty_result_returns_ok_empty(self):
        from app.indexer.edges import _fetch_tokentx_page
        resp = self._make_response(200, json_data={"status": "0", "result": "No transactions found"})
        client = self._make_client(response=resp)
        result = self._run(_fetch_tokentx_page(client, "0x1234", "key"))
        assert result.ok
        assert result.transfers == []


if __name__ == "__main__":
    unittest.main()
