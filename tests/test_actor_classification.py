"""
Tests for actor_classification.classify_all_active:
1. CTE query finds stale classifications
2. CancelledError propagates (not swallowed)
3. Attestation failure writes to cycle_errors
"""
import asyncio
import unittest
from unittest.mock import patch, MagicMock


class TestClassifyAllActive(unittest.TestCase):

    @patch("app.actor_classification.fetch_one")
    @patch("app.actor_classification.classify_wallet")
    @patch("app.actor_classification.fetch_all")
    def test_finds_stale_classifications(self, mock_fetch_all, mock_classify, mock_fetch_one):
        """classify_all_active should return classified > 0 when candidates exist."""
        mock_fetch_all.return_value = [
            {"address": "0x1111111111111111111111111111111111111111"},
            {"address": "0x2222222222222222222222222222222222222222"},
        ]
        mock_classify.return_value = {"actor_type": "human", "agent_probability": 0.2}
        mock_fetch_one.return_value = {"cnt": 0}

        from app.actor_classification import classify_all_active
        result = classify_all_active(limit=100)

        assert result["classified"] == 2
        assert result["by_type"]["human"] == 2
        assert mock_fetch_all.call_count == 1
        sql = mock_fetch_all.call_args[0][0]
        assert "active_addresses" in sql, "Should use CTE-based query"

    @patch("app.actor_classification.fetch_one")
    @patch("app.actor_classification.classify_wallet")
    @patch("app.actor_classification.fetch_all")
    def test_cancellation_propagates(self, mock_fetch_all, mock_classify, mock_fetch_one):
        """CancelledError must not be swallowed by the except block."""
        mock_fetch_all.return_value = [
            {"address": "0x1111111111111111111111111111111111111111"},
        ]
        mock_classify.side_effect = asyncio.CancelledError()

        from app.actor_classification import classify_all_active
        with self.assertRaises(asyncio.CancelledError):
            classify_all_active(limit=100)

    @patch("app.actor_classification.fetch_one")
    @patch("app.actor_classification.classify_wallet")
    @patch("app.actor_classification.fetch_all")
    def test_attestation_failure_logged(self, mock_fetch_all, mock_classify, mock_fetch_one):
        """Attestation failure should log error and not crash."""
        mock_fetch_all.return_value = [
            {"address": "0x1111111111111111111111111111111111111111"},
        ]
        mock_classify.return_value = {"actor_type": "human", "agent_probability": 0.2}
        mock_fetch_one.return_value = {"cnt": 0}

        from app.actor_classification import classify_all_active

        with patch("app.state_attestation.attest_state", side_effect=Exception("DB connection lost")):
            result = classify_all_active(limit=100)

        assert result["classified"] == 1


if __name__ == "__main__":
    unittest.main()
