"""
Determinism tests for the publication-ready composition outputs (V9.13 §N).

Each of the three writers — ``compute_cqi_matrix``, ``compute_rqs_for_protocol``,
``compute_rqs_all`` — must satisfy four properties:

  1. Reproducibility within a single process.
  2. Permutation invariance (input dict-key order doesn't matter).
  3. Sub-hour timestamp invariance (``computed_at`` truncates to hour).
  4. Pure-input idempotence (no hidden state mutation).

12 tests total (4 per function × 3 functions). If any fails, the failure
documents non-determinism in the writer — fix the writer, do NOT add
``sorted()`` inside the test.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from app import composition
from app.composition_serialization import canonical_hash, canonical_serialize


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "composition_inputs_v1.json"


def _load_fixture() -> dict:
    with open(FIXTURE_PATH) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# DB stub: a fake fetch_one / fetch_all that routes by SQL fragments.
# ---------------------------------------------------------------------------
# composition.py reads from three tables: scores (joined to stablecoins),
# psi_scores, and protocol_treasury_holdings. The stub returns the
# fixture's projection of each. Routing by SQL substring rather than
# regex keeps the test resilient to whitespace tweaks.

class CompositionDBStub:
    def __init__(self, fixture: dict):
        self.fixture = fixture
        self.attestations: list = []  # captured attest_state calls

    def fetch_all(self, sql, params=None):
        sql = sql or ""
        # SII snapshot (scores joined to stablecoins)
        if "FROM scores s" in sql and "stablecoins" in sql:
            return [dict(r) for r in self.fixture["scores"]]
        # PSI snapshot
        if "FROM psi_scores" in sql and "DISTINCT ON" in sql:
            return [dict(r) for r in self.fixture["psi_scores"]]
        if "FROM protocol_treasury_holdings" in sql:
            slug = params[0] if params else None
            holdings = self.fixture["treasury_holdings"].get(slug, [])
            return [dict(r) for r in holdings]
        return []

    def fetch_one(self, sql, params=None):
        sql = sql or ""
        # Per-symbol SII fetch (compute_rqs)
        if "FROM scores s" in sql and "UPPER(st.symbol)" in sql:
            symbol = (params[0] if params else "").upper()
            for r in self.fixture["scores"]:
                if r["symbol"].upper() == symbol:
                    out = dict(r)
                    out["computed_at"] = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
                    return out
            return None
        # Per-protocol PSI fetch
        if "FROM psi_scores" in sql and "protocol_slug" in sql and "ORDER BY computed_at" in sql:
            slug = params[0] if params else None
            for r in self.fixture["psi_scores"]:
                if r["protocol_slug"] == slug:
                    return dict(r)
            return None
        return None

    def attest_state(self, domain, records, entity_id=None):
        # Capture but do not write. Return a stable hash for callers.
        self.attestations.append({"domain": domain, "records": records, "entity_id": entity_id})
        return canonical_hash(records)


@pytest.fixture
def db_stub(monkeypatch):
    fixture = _load_fixture()
    stub = CompositionDBStub(fixture)
    monkeypatch.setattr(composition, "fetch_all", stub.fetch_all)
    monkeypatch.setattr(composition, "fetch_one", stub.fetch_one)
    # Stub attest_state at import sites (compute_*; build_publication_payload
    # imports inside try/except so we patch the module the function imports
    # from rather than the lazy import).
    import app.state_attestation as sa
    monkeypatch.setattr(sa, "attest_state", stub.attest_state)
    # Stub TARGET_PROTOCOLS for compute_rqs_all
    import app.index_definitions.psi_v01 as psi_v01
    monkeypatch.setattr(psi_v01, "TARGET_PROTOCOLS", tuple(fixture["target_protocols"]))
    return stub


def _normalize_for_compare(obj):
    """Strip the attestation-relevant timestamp from a result before
    comparing, so two runs at slightly different wall-clock points still
    compare equal. The ``computed_at`` field of the *attestation payload*
    is hour-truncated and tested separately; this normalization handles
    other timestamp surfaces that may bleed into the result body
    (sii_scored_at, etc.).
    """
    return canonical_serialize(obj)


# ---------------------------------------------------------------------------
# compute_cqi_matrix
# ---------------------------------------------------------------------------

class TestComputeCqiMatrix:
    def test_repeatable_in_process(self, db_stub):
        """Two calls in the same process produce structurally and
        byte-identically equal output."""
        a = composition.compute_cqi_matrix()
        b = composition.compute_cqi_matrix()
        assert a == b
        assert _normalize_for_compare(a) == _normalize_for_compare(b)

    def test_permutation_invariant(self, db_stub):
        """Permuting input dict order does not change output. Reverse the
        ``component_scores`` dict order on every PSI row and re-run."""
        a = composition.compute_cqi_matrix()

        # Permute: reverse every dict in psi_scores.
        original_psi = list(db_stub.fixture["psi_scores"])
        permuted = []
        for r in original_psi:
            new_r = {k: r[k] for k in reversed(list(r.keys()))}
            if isinstance(new_r.get("component_scores"), dict):
                cs = new_r["component_scores"]
                new_r["component_scores"] = {k: cs[k] for k in reversed(list(cs.keys()))}
            permuted.append(new_r)
        db_stub.fixture["psi_scores"] = permuted

        b = composition.compute_cqi_matrix()
        assert _normalize_for_compare(a) == _normalize_for_compare(b)

    def test_sub_hour_timestamp_invariant(self, db_stub):
        """Two runs with ``datetime.now`` mocked to two different times
        within the same hour produce identical ``computed_at`` (and
        therefore identical attestation payloads)."""
        t1 = datetime(2026, 5, 9, 12, 5, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 5, 9, 12, 55, 0, tzinfo=timezone.utc)

        attestations: list = []

        with patch("app.composition.datetime") as mock_dt:
            mock_dt.now.return_value = t1
            mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
            composition.compute_cqi_matrix()
            attestations.append(dict(db_stub.attestations[-1]))

            db_stub.attestations.clear()
            mock_dt.now.return_value = t2
            composition.compute_cqi_matrix()
            attestations.append(dict(db_stub.attestations[-1]))

        # The hour-truncated computed_at must match for both runs, so
        # the publication payload (output_hash + computed_at + input
        # hashes) is byte-identical.
        p1 = attestations[0]["records"][0]
        p2 = attestations[1]["records"][0]
        assert p1["computed_at"] == p2["computed_at"]
        assert p1["output_hash"] == p2["output_hash"]

    def test_idempotent_no_input_mutation(self, db_stub):
        """Running compute_cqi_matrix does not mutate the fixture's
        internal lists/dicts. If it did, a second run would see drift."""
        before = json.loads(json.dumps(db_stub.fixture))
        composition.compute_cqi_matrix()
        after = json.loads(json.dumps(db_stub.fixture))
        assert before == after


# ---------------------------------------------------------------------------
# compute_rqs_for_protocol
# ---------------------------------------------------------------------------

PROTOCOL = "aave-v3"


class TestComputeRqsForProtocol:
    def test_repeatable_in_process(self, db_stub):
        a = composition.compute_rqs_for_protocol(PROTOCOL)
        b = composition.compute_rqs_for_protocol(PROTOCOL)
        assert _normalize_for_compare(a) == _normalize_for_compare(b)

    def test_permutation_invariant(self, db_stub):
        a = composition.compute_rqs_for_protocol(PROTOCOL)

        # Reverse the order of treasury rows for this protocol.
        rows = list(db_stub.fixture["treasury_holdings"][PROTOCOL])
        permuted_rows = list(reversed(rows))
        db_stub.fixture["treasury_holdings"][PROTOCOL] = permuted_rows

        b = composition.compute_rqs_for_protocol(PROTOCOL)
        assert _normalize_for_compare(a) == _normalize_for_compare(b)

    def test_sub_hour_timestamp_invariant(self, db_stub):
        t1 = datetime(2026, 5, 9, 12, 1, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 5, 9, 12, 59, 0, tzinfo=timezone.utc)

        with patch("app.composition.datetime") as mock_dt:
            mock_dt.now.return_value = t1
            mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
            composition.compute_rqs_for_protocol(PROTOCOL)
            p1 = dict(db_stub.attestations[-1]["records"][0])

            db_stub.attestations.clear()
            mock_dt.now.return_value = t2
            composition.compute_rqs_for_protocol(PROTOCOL)
            p2 = dict(db_stub.attestations[-1]["records"][0])

        assert p1["computed_at"] == p2["computed_at"]
        assert p1["output_hash"] == p2["output_hash"]

    def test_idempotent_no_input_mutation(self, db_stub):
        before = json.loads(json.dumps(db_stub.fixture))
        composition.compute_rqs_for_protocol(PROTOCOL)
        after = json.loads(json.dumps(db_stub.fixture))
        assert before == after


# ---------------------------------------------------------------------------
# compute_rqs_all
# ---------------------------------------------------------------------------

class TestComputeRqsAll:
    def test_repeatable_in_process(self, db_stub):
        a = composition.compute_rqs_all()
        b = composition.compute_rqs_all()
        assert _normalize_for_compare(a) == _normalize_for_compare(b)

    def test_permutation_invariant(self, db_stub):
        a = composition.compute_rqs_all()

        # Reverse holdings for every protocol.
        new_holdings = {}
        for slug, rows in db_stub.fixture["treasury_holdings"].items():
            new_holdings[slug] = list(reversed(rows))
        db_stub.fixture["treasury_holdings"] = new_holdings
        # Reverse psi_scores order too.
        db_stub.fixture["psi_scores"] = list(reversed(db_stub.fixture["psi_scores"]))

        b = composition.compute_rqs_all()
        assert _normalize_for_compare(a) == _normalize_for_compare(b)

    def test_sub_hour_timestamp_invariant(self, db_stub):
        t1 = datetime(2026, 5, 9, 12, 2, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 5, 9, 12, 58, 0, tzinfo=timezone.utc)

        with patch("app.composition.datetime") as mock_dt:
            mock_dt.now.return_value = t1
            mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
            composition.compute_rqs_all()
            # Pick the rqs_compositions batch attestation.
            p1 = next(
                a for a in db_stub.attestations
                if a["domain"] == "rqs_compositions"
            )["records"][0]
            db_stub.attestations.clear()

            mock_dt.now.return_value = t2
            composition.compute_rqs_all()
            p2 = next(
                a for a in db_stub.attestations
                if a["domain"] == "rqs_compositions"
            )["records"][0]

        assert p1["computed_at"] == p2["computed_at"]
        assert p1["output_hash"] == p2["output_hash"]

    def test_idempotent_no_input_mutation(self, db_stub):
        before = json.loads(json.dumps(db_stub.fixture))
        composition.compute_rqs_all()
        after = json.loads(json.dumps(db_stub.fixture))
        assert before == after
