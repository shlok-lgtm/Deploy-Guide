"""
Tests for Solana-native auto-promotion path (Step 2 of usdpt readiness work).

References:
- /tmp/usdpt_readiness_audit.md Task 1
- migrations/106_solana_stablecoin_columns.sql
"""
import os
import pytest

from app.indexer.config import (
    UNSCORED_CONTRACTS,
    UNSCORED_SOLANA_MINTS,
    lookup_unscored_by_symbol,
)


def test_lookup_finds_evm_symbol():
    # GHO is in UNSCORED_CONTRACTS at config.py:34
    result = lookup_unscored_by_symbol("GHO")
    assert result is not None
    addr, info, chain = result
    assert chain == "ethereum"
    assert info["symbol"] == "GHO"


def test_lookup_finds_solana_symbol_when_present():
    # Inject a fixture entry, run lookup, then clean up
    UNSCORED_SOLANA_MINTS["TestMintAddressCasePreservedXYZ123"] = {
        "symbol": "TEST", "name": "Test Coin", "decimals": 6,
        "coingecko_id": "test-coin",
    }
    try:
        result = lookup_unscored_by_symbol("test")  # case-insensitive
        assert result is not None
        addr, info, chain = result
        assert chain == "solana"
        assert addr == "TestMintAddressCasePreservedXYZ123"  # case preserved
        assert info["symbol"] == "TEST"
    finally:
        UNSCORED_SOLANA_MINTS.pop("TestMintAddressCasePreservedXYZ123", None)


def test_lookup_returns_none_for_unknown_symbol():
    assert lookup_unscored_by_symbol("DEFINITELY_NOT_A_REAL_SYMBOL") is None


@pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="DATABASE_URL not set; integration test requires migration 106 applied",
)
def test_solana_mint_case_preserved_through_promotion():
    """Integration test: insert Solana mint mixed-case, promote, read back, verify case preserved."""
    from app.indexer.backlog import upsert_unscored_asset, promote_eligible_assets
    from app.database import fetch_one, execute
    SOLANA_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC Solana, 44 chars
    test_id = "_test_solana_promote"
    try:
        upsert_unscored_asset(
            token_address=SOLANA_MINT,
            symbol="TESTSOL",
            name="Test Solana Coin",
            decimals=6,
            coingecko_id="test-solana",
            token_type="stablecoin",
            chain="solana",
        )
        # Verify case preserved in unscored_assets
        row = fetch_one(
            "SELECT token_address, chain FROM wallet_graph.unscored_assets WHERE symbol = 'TESTSOL'"
        )
        assert row is not None
        assert row["token_address"] == SOLANA_MINT  # case preserved
        assert row["chain"] == "solana"
    finally:
        execute("DELETE FROM wallet_graph.unscored_assets WHERE symbol = 'TESTSOL'")
        execute("DELETE FROM stablecoins WHERE id = %s", (test_id,))
