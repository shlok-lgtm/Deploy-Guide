"""
Unit tests for the CoinGecko webhook handler.

Covers the four required scenarios:
  1. Valid signature → 200 + row inserted + attest_state called
  2. Invalid signature → 401 + cycle_errors row written
  3. Unmapped slug → 200 + row inserted with null basis_entity_id + cycle_errors row
  4. contract_address change → ops alert fired

These tests exercise the handler logic directly via mocked DB
helpers — the table doesn't need to exist for the tests to run, and
they don't require a live FastAPI server.
"""

import asyncio
import hashlib
import hmac
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

SECRET = "test-secret-do-not-use-in-prod"


def _sign(body: bytes, secret: str = SECRET) -> str:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


class _CaseInsensitiveHeaders:
    """Mimic Starlette's case-insensitive headers .get()."""
    def __init__(self, headers: dict):
        self._h = {k.lower(): v for k, v in headers.items()}

    def get(self, key, default=None):
        return self._h.get(key.lower(), default)


def _mock_request(body_bytes: bytes, headers: dict | None = None) -> MagicMock:
    """Build a minimal FastAPI-shaped Request mock."""
    req = MagicMock()
    req.body = AsyncMock(return_value=body_bytes)
    req.headers = _CaseInsensitiveHeaders(headers or {})
    req.client = MagicMock(host="127.0.0.1")
    return req


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_signature_verification_constant_time():
    """verify_signature accepts hex digest and sha256= prefix; rejects garbage."""
    from app.webhooks.coingecko import verify_signature

    body = b'{"coin_id":"usd-coin"}'
    good = _sign(body)

    assert verify_signature(SECRET, body, good) is True
    assert verify_signature(SECRET, body, f"sha256={good}") is True
    assert verify_signature(SECRET, body, "deadbeef") is False
    assert verify_signature(SECRET, body, "") is False
    assert verify_signature("", body, good) is False


def test_valid_signature_inserts_row_and_attests(monkeypatch):
    """Happy path: valid sig → 200, row inserted, attest_state called."""
    monkeypatch.setenv("COINGECKO_WEBHOOK_SECRET", SECRET)
    from app.webhooks import coingecko as cg

    payload = {
        "event": "cg.coin.info.updated",
        "coin_id": "usd-coin",
        "changed_fields": {"name": "USD Coin (updated)"},
    }
    raw = json.dumps(payload).encode("utf-8")
    sig = _sign(raw)

    req = _mock_request(raw, headers={cg.SIGNATURE_HEADER: sig})

    def _fetch_one(sql, params=None):
        if "FROM coingecko_metadata_events" in sql:
            return None  # idempotency check: no existing row
        if "FROM stablecoins" in sql:
            return {"id": "usdc"}
        return None

    with patch.object(cg, "fetch_one", side_effect=_fetch_one) as mock_fetch, \
         patch.object(cg, "execute") as mock_exec, \
         patch("app.state_attestation.attest_state") as mock_attest, \
         patch.object(cg, "_record_cycle_error") as mock_err, \
         patch("app.ops.tools.alerter.send_alert", new_callable=AsyncMock) as mock_alert:

        resp = _run(cg.handle_webhook(req))

        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["status"] == "ok"
        assert body["basis_entity_type"] == "stablecoin"
        assert body["basis_entity_id"] == "usdc"
        assert body["event_id"]

        # Insert happened
        insert_calls = [c for c in mock_exec.call_args_list
                        if "INSERT INTO coingecko_metadata_events" in c.args[0]]
        assert len(insert_calls) == 1, f"expected 1 insert, got {len(insert_calls)}"

        # Attestation happened
        mock_attest.assert_called_once()
        attest_args = mock_attest.call_args
        assert attest_args.args[0] == "coingecko_metadata_events"
        records = attest_args.args[1]
        assert len(records) == 1
        assert records[0]["coingecko_id"] == "usd-coin"
        assert records[0]["basis_entity_id"] == "usdc"

        # No cycle_errors for happy path
        mock_err.assert_not_called()

        # No alert — changed_fields didn't touch contract_address or platforms
        mock_alert.assert_not_called()


def test_invalid_signature_returns_401_and_logs_cycle_error(monkeypatch):
    """Bad sig → 401, cycle_errors row written, no insert, no attestation."""
    monkeypatch.setenv("COINGECKO_WEBHOOK_SECRET", SECRET)
    from app.webhooks import coingecko as cg

    raw = b'{"coin_id":"usd-coin"}'
    req = _mock_request(raw, headers={cg.SIGNATURE_HEADER: "wrong-hex"})

    with patch.object(cg, "execute") as mock_exec, \
         patch("app.state_attestation.attest_state") as mock_attest, \
         patch.object(cg, "_record_cycle_error") as mock_err:

        resp = _run(cg.handle_webhook(req))

        assert resp.status_code == 401
        body = json.loads(resp.body)
        assert body["status"] == "invalid_signature"

        # cycle_errors recorded with the right error_type
        mock_err.assert_called_once()
        kwargs = mock_err.call_args.kwargs
        assert kwargs["error_type"] == "webhook_signature_invalid"
        assert kwargs["cycle_phase"] == "webhook_coingecko"

        # No insert, no attestation
        insert_calls = [c for c in mock_exec.call_args_list
                        if "INSERT INTO coingecko_metadata_events" in c.args[0]]
        assert len(insert_calls) == 0
        mock_attest.assert_not_called()


def test_unmapped_slug_inserts_with_null_entity_and_logs_cycle_error(monkeypatch):
    """Unknown coingecko_id → 200, row inserted with NULL entity, cycle_errors row."""
    monkeypatch.setenv("COINGECKO_WEBHOOK_SECRET", SECRET)
    from app.webhooks import coingecko as cg

    payload = {
        "event": "cg.coin.info.updated",
        "coin_id": "some-token-we-dont-track",
        "changed_fields": {"description": "x"},
    }
    raw = json.dumps(payload).encode("utf-8")
    sig = _sign(raw)
    req = _mock_request(raw, headers={cg.SIGNATURE_HEADER: sig})

    # fetch_one returns None (no stablecoin match); PROTOCOL_GOVERNANCE_TOKENS
    # is a real dict — "some-token-we-dont-track" won't be a value in it.
    with patch.object(cg, "fetch_one", return_value=None), \
         patch.object(cg, "execute") as mock_exec, \
         patch("app.state_attestation.attest_state") as mock_attest, \
         patch.object(cg, "_record_cycle_error") as mock_err:

        resp = _run(cg.handle_webhook(req))

        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["status"] == "ok"
        assert body["basis_entity_type"] is None
        assert body["basis_entity_id"] is None

        # Row was still inserted (we want the audit trail even for unmapped slugs)
        insert_calls = [c for c in mock_exec.call_args_list
                        if "INSERT INTO coingecko_metadata_events" in c.args[0]]
        assert len(insert_calls) == 1
        insert_params = insert_calls[0].args[1]
        # tuple positions: event_id, coingecko_id, basis_entity_type, basis_entity_id, ...
        assert insert_params[1] == "some-token-we-dont-track"
        assert insert_params[2] is None  # basis_entity_type
        assert insert_params[3] is None  # basis_entity_id

        # cycle_errors row with webhook_unmapped_slug
        err_calls = [c for c in mock_err.call_args_list
                     if c.kwargs.get("error_type") == "webhook_unmapped_slug"]
        assert len(err_calls) == 1

        # Attestation still fired — provenance value is in the receipt itself
        mock_attest.assert_called_once()


def test_contract_address_change_fires_alert(monkeypatch):
    """contract_address mutation → send_alert called with severity=critical."""
    monkeypatch.setenv("COINGECKO_WEBHOOK_SECRET", SECRET)
    from app.webhooks import coingecko as cg

    payload = {
        "event": "cg.coin.info.updated",
        "coin_id": "tether",
        "changed_fields": {
            "contract_address": {
                "before": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                "after": "0xNEWADDRESS00000000000000000000000000000",
            },
        },
    }
    raw = json.dumps(payload).encode("utf-8")
    sig = _sign(raw)
    req = _mock_request(raw, headers={cg.SIGNATURE_HEADER: sig})

    def _fetch_one(sql, params=None):
        if "FROM coingecko_metadata_events" in sql:
            return None
        if "FROM stablecoins" in sql:
            return {"id": "usdt"}
        return None

    with patch.object(cg, "fetch_one", side_effect=_fetch_one), \
         patch.object(cg, "execute"), \
         patch("app.state_attestation.attest_state"), \
         patch.object(cg, "_record_cycle_error"), \
         patch("app.ops.tools.alerter.send_alert", new_callable=AsyncMock) as mock_alert:

        resp = _run(cg.handle_webhook(req))

        assert resp.status_code == 200
        mock_alert.assert_awaited_once()
        kwargs = mock_alert.call_args.kwargs
        assert kwargs["alert_type"] == "coingecko_metadata_change"
        assert kwargs["severity"] == "critical"
        assert "contract_address" in kwargs["context"]["fields"]
        assert kwargs["context"]["basis_entity_id"] == "usdt"


def test_platforms_change_fires_alert(monkeypatch):
    """platforms mutation (chain additions) also fires alert."""
    monkeypatch.setenv("COINGECKO_WEBHOOK_SECRET", SECRET)
    from app.webhooks import coingecko as cg

    payload = {
        "event": "cg.coin.info.updated",
        "coin_id": "usd-coin",
        "changed_fields": {
            "platforms": {"new_chain": "0xabc..."},
        },
    }
    raw = json.dumps(payload).encode("utf-8")
    sig = _sign(raw)
    req = _mock_request(raw, headers={cg.SIGNATURE_HEADER: sig})

    def _fetch_one(sql, params=None):
        if "FROM coingecko_metadata_events" in sql:
            return None
        if "FROM stablecoins" in sql:
            return {"id": "usdc"}
        return None

    with patch.object(cg, "fetch_one", side_effect=_fetch_one), \
         patch.object(cg, "execute"), \
         patch("app.state_attestation.attest_state"), \
         patch.object(cg, "_record_cycle_error"), \
         patch("app.ops.tools.alerter.send_alert", new_callable=AsyncMock) as mock_alert:

        resp = _run(cg.handle_webhook(req))

        assert resp.status_code == 200
        mock_alert.assert_awaited_once()
        assert "platforms" in mock_alert.call_args.kwargs["context"]["fields"]


def test_duplicate_delivery_is_idempotent(monkeypatch):
    """Same content_hash arriving twice → second insert is a no-op."""
    monkeypatch.setenv("COINGECKO_WEBHOOK_SECRET", SECRET)
    from app.webhooks import coingecko as cg

    payload = {"event": "cg.coin.info.updated", "coin_id": "usd-coin",
               "changed_fields": {"name": "x"}}
    raw = json.dumps(payload).encode("utf-8")
    sig = _sign(raw)
    req = _mock_request(raw, headers={cg.SIGNATURE_HEADER: sig})

    # fetch_one returns the existing event row first (idempotency
    # guard), then returns the stablecoin mapping is not reached.
    existing_event = {"event_id": "00000000-0000-0000-0000-000000000001"}

    def _fetch_one_side_effect(sql, params=None):
        if "FROM coingecko_metadata_events" in sql:
            return existing_event
        if "FROM stablecoins" in sql:
            return {"id": "usdc"}
        return None

    with patch.object(cg, "fetch_one", side_effect=_fetch_one_side_effect), \
         patch.object(cg, "execute") as mock_exec, \
         patch("app.state_attestation.attest_state"), \
         patch.object(cg, "_record_cycle_error"):

        resp = _run(cg.handle_webhook(req))

        assert resp.status_code == 200
        # No INSERT executed because dup was detected
        insert_calls = [c for c in mock_exec.call_args_list
                        if "INSERT INTO coingecko_metadata_events" in c.args[0]]
        assert len(insert_calls) == 0
        # Returned the same event_id as the existing row
        body = json.loads(resp.body)
        assert body["event_id"] == existing_event["event_id"]


def test_resolve_basis_entity_stablecoin(monkeypatch):
    """resolve_basis_entity returns ('stablecoin', id) for known CG ids."""
    from app.webhooks import coingecko as cg

    with patch.object(cg, "fetch_one", return_value={"id": "usdc"}):
        assert cg.resolve_basis_entity("usd-coin") == ("stablecoin", "usdc")


def test_resolve_basis_entity_protocol():
    """resolve_basis_entity reverse-maps governance-token CG ids."""
    from app.webhooks import coingecko as cg

    with patch.object(cg, "fetch_one", return_value=None):
        # "aave" is in PROTOCOL_GOVERNANCE_TOKENS with key "aave" → "aave"
        result = cg.resolve_basis_entity("aave")
        assert result[0] == "protocol"
        assert result[1] == "aave"


def test_resolve_basis_entity_unmapped():
    """resolve_basis_entity returns (None, None) for unknown slugs."""
    from app.webhooks import coingecko as cg

    with patch.object(cg, "fetch_one", return_value=None):
        assert cg.resolve_basis_entity("totally-made-up") == (None, None)
        assert cg.resolve_basis_entity("") == (None, None)
