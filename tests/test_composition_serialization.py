"""
Tests for ``app.composition_serialization`` (V9.13 §N).

Four tests, each one of the four properties the canonical serializer must
guarantee. If any of these fail, the property is broken — do NOT paper
over the test by sorting in the producer; fix the serializer.

  - ``test_snapshot_hash_is_stable``       — hardcoded hash regression gate
  - ``test_serialize_is_byte_stable``      — same input → same bytes (twice)
  - ``test_permutation_invariant``         — dict-key permutation is invisible
  - ``test_float_decimal_equivalence``     — float and Decimal forms agree
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from app.composition_serialization import (
    SERIALIZER_VERSION,
    canonical_hash,
    canonical_serialize,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "composition_inputs_v1.json"


# ---------------------------------------------------------------------------
# Hardcoded snapshot hash.
# ---------------------------------------------------------------------------
# Computed on first run from FIXTURE_PATH. Hardcoded here so any future
# format change (precision, wrapper shape, key ordering, ...) makes this
# test fail loudly rather than silently invalidating previously-published
# composition output_hashes. If you change the serializer, you MUST bump
# SERIALIZER_VERSION and update this constant in the SAME PR.
EXPECTED_SNAPSHOT_HASH = (
    "38097c6c6e3fd029daedc72b6824577c26805a94f92fd5e086485ae870766338"
)


def _load_fixture() -> dict:
    with open(FIXTURE_PATH) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def fixture_obj():
    return _load_fixture()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_snapshot_hash_is_stable(fixture_obj):
    """The canonical hash of the v1 fixture is byte-stable across runs.

    Failure mode: the serializer's format has drifted. Either (a) the
    SERIALIZER_VERSION was not bumped, in which case previously-published
    composition output_hashes silently broke; or (b) the version was
    bumped but this test wasn't updated. Either way, fail loudly so the
    drift is auditable.
    """
    actual = canonical_hash(fixture_obj)
    assert actual == EXPECTED_SNAPSHOT_HASH, (
        f"Canonical hash drift detected. Expected {EXPECTED_SNAPSHOT_HASH}, got {actual}. "
        f"If this is intentional, bump SERIALIZER_VERSION (currently {SERIALIZER_VERSION}) "
        f"and update EXPECTED_SNAPSHOT_HASH in the same commit."
    )


def test_serialize_is_byte_stable(fixture_obj):
    """Two calls on the same input produce byte-identical output."""
    a = canonical_serialize(fixture_obj)
    b = canonical_serialize(fixture_obj)
    assert a == b
    # Compactness invariant: no JSON-structural whitespace. String values
    # may legitimately contain spaces; we strip those out by alternating
    # between in-string and out-of-string segments via ``split(b'"')`` —
    # even-indexed segments are structural JSON, odd-indexed are quoted
    # string contents.
    structural = b"".join(a.split(b'"')[0::2])
    assert b", " not in structural, "canonical structure must use ',' (no space) as separator"
    assert b": " not in structural, "canonical structure must use ':' (no space) as separator"
    assert not a.endswith(b"\n"), "canonical output must not have trailing newline"


def test_permutation_invariant(fixture_obj):
    """Permuting the key order of any dict in the input does not change
    the output bytes. This is the property that makes the serializer
    canonical — without it, two clients constructing structurally equal
    dicts in different orders would publish different hashes.
    """
    original = canonical_serialize(fixture_obj)

    # Reverse top-level key order.
    permuted = {k: fixture_obj[k] for k in reversed(list(fixture_obj.keys()))}
    assert canonical_serialize(permuted) == original

    # Reverse keys at every nested dict (one level down — covers psi_scores
    # entries, treasury_holdings, _meta).
    def reverse_dict_keys(o):
        if isinstance(o, dict):
            return {k: reverse_dict_keys(o[k]) for k in reversed(list(o.keys()))}
        if isinstance(o, list):
            return [reverse_dict_keys(x) for x in o]
        return o

    deeply_permuted = reverse_dict_keys(fixture_obj)
    assert canonical_serialize(deeply_permuted) == original


def test_float_decimal_equivalence():
    """``1.5`` (float) and ``Decimal('1.5')`` produce byte-identical
    serialization. This invariant matters because composition functions
    receive scores from the DB as Decimal but route through float in
    several intermediate steps; the hash must not change based on which
    type the value happens to be at any given moment.
    """
    a = {"score": 1.5, "weight": 0.25, "n": 100}
    b = {"score": Decimal("1.5"), "weight": Decimal("0.25"), "n": 100}
    assert canonical_serialize(a) == canonical_serialize(b)
    assert canonical_hash(a) == canonical_hash(b)


def test_wrapper_records_version(fixture_obj):
    """The serializer wrapper records SERIALIZER_VERSION explicitly so
    future verifiers can detect format drift without reading source."""
    raw = canonical_serialize(fixture_obj)
    parsed = json.loads(raw)
    assert parsed["v"] == SERIALIZER_VERSION
    assert "d" in parsed
