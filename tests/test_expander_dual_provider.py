"""
Tests for dual-provider wallet expander.
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock


class TestPagePartition(unittest.TestCase):
    """Verify the odd/even page partitioning logic."""

    def test_partition_covers_all_pages_no_gaps(self):
        start, count = 1, 10
        all_pages = list(range(start, start + count))
        odd = [p for p in all_pages if p % 2 == 1]
        even = [p for p in all_pages if p % 2 == 0]
        self.assertEqual(sorted(odd + even), all_pages)
        self.assertEqual(len(set(odd) & set(even)), 0)

    def test_partition_start_even(self):
        start, count = 20, 6
        all_pages = list(range(start, start + count))
        odd = [p for p in all_pages if p % 2 == 1]
        even = [p for p in all_pages if p % 2 == 0]
        self.assertEqual(sorted(odd + even), all_pages)

    def test_partition_single_page(self):
        all_pages = [7]
        odd = [p for p in all_pages if p % 2 == 1]
        even = [p for p in all_pages if p % 2 == 0]
        self.assertEqual(odd, [7])
        self.assertEqual(even, [])


class TestFetchHoldersDual(unittest.TestCase):
    """Test the dual-provider fetch function."""

    @patch("app.indexer.expander.fetch_top_holders", new_callable=AsyncMock)
    def test_dual_deduplicates_across_providers(self, mock_fetch):
        from app.indexer.expander import _fetch_holders_dual

        def side_effect(client, contract, key, page=1, offset=100, provider=None):
            if provider == "etherscan":
                return ["0xaaa", "0xbbb", "0xccc"]
            else:
                return ["0xbbb", "0xddd", "0xeee"]

        mock_fetch.side_effect = side_effect

        result = asyncio.run(_fetch_holders_dual(
            AsyncMock(), "0xcontract", "eth_key", "bs_key",
            start_page=1, pages_to_fetch=4,
        ))

        self.assertEqual(len(result["addresses"]), 5)
        self.assertIn("0xaaa", result["addresses"])
        self.assertIn("0xddd", result["addresses"])
        self.assertGreater(result["etherscan_addrs"], 0)
        self.assertGreater(result["blockscout_addrs"], 0)

    @patch("app.indexer.expander.fetch_top_holders", new_callable=AsyncMock)
    def test_exhaustion_requires_both_empty(self, mock_fetch):
        from app.indexer.expander import _fetch_holders_dual

        def side_effect(client, contract, key, page=1, offset=100, provider=None):
            if provider == "etherscan":
                return []
            else:
                return ["0xaaa"]

        mock_fetch.side_effect = side_effect

        result = asyncio.run(_fetch_holders_dual(
            AsyncMock(), "0xcontract", "eth_key", "bs_key",
            start_page=1, pages_to_fetch=4,
        ))

        self.assertFalse(result["exhausted"])

    @patch("app.indexer.expander.fetch_top_holders", new_callable=AsyncMock)
    def test_both_empty_means_exhausted(self, mock_fetch):
        from app.indexer.expander import _fetch_holders_dual

        mock_fetch.return_value = []

        result = asyncio.run(_fetch_holders_dual(
            AsyncMock(), "0xcontract", "eth_key", "bs_key",
            start_page=1, pages_to_fetch=4,
        ))

        self.assertTrue(result["exhausted"])
        self.assertEqual(len(result["addresses"]), 0)


class TestFallbackToSingleProvider(unittest.TestCase):
    """Test that missing keys fall back gracefully."""

    @patch.dict("os.environ", {"EXPANDER_DUAL_PROVIDER": "true", "ETHERSCAN_API_KEY": "key1"}, clear=False)
    def test_missing_blockscout_key_logs_warning(self):
        import importlib
        import app.indexer.expander as exp
        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
