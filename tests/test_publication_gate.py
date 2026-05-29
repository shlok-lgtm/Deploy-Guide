"""Unit tests for app/publication_gate.py (migration 112).

These exercise the helper module in isolation with a mocked DB layer
so they run without a Postgres connection. Integration coverage —
that the views actually filter, that the admin endpoint flips state —
belongs in the scenarios harness
(`publication_gate_excludes_unpublished_from_serving`, proposed in
the PR description).
"""

import asyncio
from unittest.mock import patch

from app import publication_gate


def _async(value):
    async def _coro(*args, **kwargs):
        return value
    return _coro


def test_is_sii_published_true_when_row_present():
    with patch.object(publication_gate, "fetch_one", return_value={"?column?": 1}):
        assert publication_gate.is_sii_published("usdc") is True


def test_is_sii_published_false_when_row_missing():
    with patch.object(publication_gate, "fetch_one", return_value=None):
        assert publication_gate.is_sii_published("eurr") is False


def test_is_psi_published_true_when_row_present():
    with patch.object(publication_gate, "fetch_one", return_value={"?column?": 1}):
        assert publication_gate.is_psi_published("aave") is True


def test_is_psi_published_false_when_row_missing():
    """A protocol with no row in protocol_publication_state is treated as unpublished."""
    with patch.object(publication_gate, "fetch_one", return_value=None):
        assert publication_gate.is_psi_published("morpho-vault-v999") is False


def test_require_sii_published_raises_404_when_unpublished():
    with patch.object(publication_gate, "fetch_one_async", _async(None)):
        from fastapi import HTTPException
        try:
            asyncio.run(publication_gate.require_sii_published("eurr"))
        except HTTPException as e:
            assert e.status_code == 404
            assert "eurr" in e.detail
        else:
            raise AssertionError("require_sii_published did not raise")


def test_require_sii_published_silent_when_published():
    with patch.object(publication_gate, "fetch_one_async", _async({"?column?": 1})):
        # No exception expected.
        asyncio.run(publication_gate.require_sii_published("usdc"))


def test_require_psi_published_raises_404_when_unpublished():
    with patch.object(publication_gate, "fetch_one_async", _async(None)):
        from fastapi import HTTPException
        try:
            asyncio.run(publication_gate.require_psi_published("unknown-psi"))
        except HTTPException as e:
            assert e.status_code == 404
            assert "unknown-psi" in e.detail
        else:
            raise AssertionError("require_psi_published did not raise")


if __name__ == "__main__":
    # Allow direct execution without pytest.
    tests = [v for k, v in globals().items() if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except Exception as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
    if failed:
        raise SystemExit(1)
