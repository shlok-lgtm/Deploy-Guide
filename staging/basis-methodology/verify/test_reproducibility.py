"""
Reproducibility test — runs the reference implementation against every
vector in test_vectors/ and asserts byte-identical score and hash.

A single mismatch fails the test. A single missing vector directory
skips (not fails) with a warning, so that CI fails cleanly on empty
test_vectors/ during initial scaffolding.
"""

from __future__ import annotations

import json
import pathlib
import sys

import pytest

# Allow running against the sibling reference/ directory.
ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "reference"))

from basis_reference.sii import compute  # noqa: E402
from basis_reference.hashing import input_hash, computation_hash  # noqa: E402


VECTORS_DIR = ROOT / "test_vectors"

MIN_VECTORS_REQUIRED = 5


def _load_vectors() -> list[pathlib.Path]:
    if not VECTORS_DIR.exists():
        return []
    return sorted(VECTORS_DIR.glob("*.json"))


def test_minimum_vectors_present():
    vectors = _load_vectors()
    if len(vectors) < MIN_VECTORS_REQUIRED:
        pytest.skip(
            f"{len(vectors)}/{MIN_VECTORS_REQUIRED} vectors captured. "
            f"Run verify/capture_vectors.py against a live basis-hub to "
            f"populate test_vectors/."
        )


@pytest.mark.parametrize("vector_path", _load_vectors())
def test_reproducibility(vector_path: pathlib.Path):
    vector = json.loads(vector_path.read_text())
    inputs = vector["inputs"]
    expected_score = vector["expected_score"]
    expected_input_hash = vector["expected_input_hash"]
    expected_computation_hash = vector["expected_computation_hash"]

    result = compute(inputs)

    assert result["input_hash"] == expected_input_hash, (
        f"{vector_path.name}: input hash drift. "
        f"spec={result['input_hash']} hub={expected_input_hash}"
    )
    if expected_score is None:
        assert result["score"] is None
    else:
        assert result["score"] is not None
        assert abs(result["score"] - expected_score) < 1e-9, (
            f"{vector_path.name}: score drift. "
            f"spec={result['score']} hub={expected_score}"
        )
    assert result["computation_hash"] == expected_computation_hash, (
        f"{vector_path.name}: computation hash drift. "
        f"spec={result['computation_hash']} hub={expected_computation_hash}"
    )


def test_determinism_self_check():
    """Sanity: two runs against the same inputs produce the same hash."""
    inputs = {
        "peg_stability": 95.5,
        "liquidity_depth": 80.0,
        "mint_burn_dynamics": 70.0,
        "holder_distribution": 60.0,
        "structural_risk_composite": 85.0,
    }
    r1 = compute(inputs)
    r2 = compute(inputs)
    assert r1 == r2
    assert r1["input_hash"] == input_hash(inputs)
    assert r1["computation_hash"] == computation_hash(
        inputs, "v1.0.0", r1["score"]
    )


def test_renormalization_on_missing_category():
    """
    If a category is None, its weight is excluded and the total is
    renormalized to the used weight. Spec section 3.
    """
    inputs = {
        "peg_stability": 100.0,
        "liquidity_depth": 100.0,
        "mint_burn_dynamics": 100.0,
        "holder_distribution": 100.0,
        "structural_risk_composite": None,
    }
    result = compute(inputs)
    assert result["score"] == pytest.approx(100.0)
    assert result["missing_categories"] == ["structural_risk_composite"]
    assert result["used_weight"] == pytest.approx(0.80)


def test_all_missing_returns_none():
    inputs = {
        "peg_stability": None,
        "liquidity_depth": None,
        "mint_burn_dynamics": None,
        "holder_distribution": None,
        "structural_risk_composite": None,
    }
    result = compute(inputs)
    assert result["score"] is None
    assert result["used_weight"] == 0.0
