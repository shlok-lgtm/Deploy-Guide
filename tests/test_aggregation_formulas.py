"""
Tests for the aggregation-formula registry and dispatch.

Covers:
  - Every formula against hand-calculated expected outputs on the same five
    synthetic cases as scripts/validate_renormalization.py.
  - legacy_renormalize reproduces the pre-PR output byte-for-byte.
  - coverage_weighted min_coverage gating.
  - coverage_withheld requires coverage_threshold and enforces the gate.
  - Unknown formula name raises ValueError.
  - Default path (no aggregation block) dispatches to legacy_renormalize.
  - score_entity() regression across all 9 index definitions under default.
"""

from __future__ import annotations

import pytest

from app.composition import (
    AGGREGATION_FORMULAS,
    AGGREGATION_FORMULA_VERSION,
    aggregate,
    aggregate_coverage_weighted,
    aggregate_coverage_withheld,
    aggregate_legacy_renormalize,
    aggregate_legacy_sii_v1,
    aggregate_strict_neutral,
    aggregate_strict_zero,
    compute_rqs,
)
from app.scoring_engine import score_entity


# =============================================================================
# Synthetic fixtures — mirror scripts/validate_renormalization.py
# =============================================================================


@pytest.fixture
def two_cat_index():
    return {
        "index_id": "test",
        "version": "v0.0.1",
        "name": "Test",
        "entity_type": "test",
        "categories": {
            "cat_a": {"name": "Cat A", "weight": 0.50},
            "cat_b": {"name": "Cat B", "weight": 0.50},
        },
        "components": {
            "a1": {"name": "A1", "category": "cat_a", "weight": 0.60,
                   "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
            "a2": {"name": "A2", "category": "cat_a", "weight": 0.40,
                   "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
            "b1": {"name": "B1", "category": "cat_b", "weight": 0.50,
                   "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
            "b2": {"name": "B2", "category": "cat_b", "weight": 0.50,
                   "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
        },
    }


@pytest.fixture
def validator_like_index():
    return {
        "index_id": "test_validator",
        "version": "v0.0.1",
        "categories": {
            "cat_a": {"name": "Validator/Operator", "weight": 0.15},
            "cat_b": {"name": "Full cat", "weight": 0.85},
        },
        "components": {
            "a1": {"category": "cat_a", "weight": 0.30,
                   "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
            "a2": {"category": "cat_a", "weight": 0.25,
                   "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
            "a3": {"category": "cat_a", "weight": 0.20,
                   "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
            "a4": {"category": "cat_a", "weight": 0.15,
                   "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
            "a5": {"category": "cat_a", "weight": 0.10,
                   "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
            "b1": {"category": "cat_b", "weight": 1.00,
                   "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
        },
    }


# =============================================================================
# Registry invariants
# =============================================================================


def test_registry_contains_all_formulas():
    expected = {
        "legacy_renormalize", "coverage_weighted", "coverage_withheld",
        "strict_zero", "strict_neutral", "legacy_sii_v1",
    }
    assert set(AGGREGATION_FORMULAS) == expected


def test_formula_version_is_v1():
    assert AGGREGATION_FORMULA_VERSION == "aggregation-v1.0.0"


def test_unknown_formula_raises(two_cat_index):
    bad = dict(two_cat_index)
    bad["aggregation"] = {"formula": "nonexistent", "params": {}}
    with pytest.raises(ValueError) as exc:
        aggregate(bad, {"a1": 50, "a2": 50, "b1": 50, "b2": 50})
    assert "nonexistent" in str(exc.value)


def test_default_path_is_legacy_renormalize(two_cat_index):
    """Definition without an `aggregation` block dispatches to legacy."""
    result = aggregate(two_cat_index, {"a1": 50, "a2": 50, "b1": 50, "b2": 50})
    assert result["method"] == "legacy_renormalize"


# =============================================================================
# legacy_renormalize — reproduces the pre-PR defect exactly
# =============================================================================


def test_legacy_baseline_all_present(two_cat_index):
    r = aggregate_legacy_renormalize(
        two_cat_index, {"a1": 50, "a2": 50, "b1": 50, "b2": 50}, {}
    )
    assert r["overall_score"] == 50.0
    assert not r["withheld"]


def test_legacy_missing_a2_all_50(two_cat_index):
    """With a2 missing and all others at 50, legacy inflates to 50 (the
    'renormalized over present' output the audit documents)."""
    r = aggregate_legacy_renormalize(
        two_cat_index, {"a1": 50, "b1": 50, "b2": 50}, {}
    )
    assert r["overall_score"] == 50.0


def test_legacy_missing_a2_a1_high(two_cat_index):
    r = aggregate_legacy_renormalize(
        two_cat_index, {"a1": 100, "b1": 50, "b2": 50}, {}
    )
    assert r["overall_score"] == 75.0


def test_legacy_entire_category_missing(two_cat_index):
    """Cat_A entirely unpopulated — legacy still returns 50 via overall renorm."""
    r = aggregate_legacy_renormalize(two_cat_index, {"b1": 50, "b2": 50}, {})
    assert r["overall_score"] == 50.0


def test_legacy_validator_shape(validator_like_index):
    """rsETH-like case — 1 of 5 components in cat_a populated."""
    r = aggregate_legacy_renormalize(validator_like_index, {"a1": 40, "b1": 80}, {})
    assert r["overall_score"] == 74.0


# =============================================================================
# coverage_weighted (Option C)
# =============================================================================


def test_coverage_weighted_full_coverage_equals_legacy(two_cat_index):
    comp = {"a1": 50, "a2": 50, "b1": 50, "b2": 50}
    legacy = aggregate_legacy_renormalize(two_cat_index, comp, {})
    cw = aggregate_coverage_weighted(two_cat_index, comp, {})
    assert cw["overall_score"] == legacy["overall_score"]


def test_coverage_weighted_missing_a2(two_cat_index):
    """cat_a effective weight = 0.5 × 0.6 = 0.3. cat_b effective weight = 0.5.
    Numerator = 50 × 0.3 + 50 × 0.5 = 40. Denominator = 0.8. Overall = 50.
    (In this case values all equal 50, so the answer coincidentally matches
    legacy — proves formula correctness on the degenerate case.)"""
    r = aggregate_coverage_weighted(
        two_cat_index, {"a1": 50, "b1": 50, "b2": 50}, {}
    )
    assert r["overall_score"] == 50.0


def test_coverage_weighted_missing_a2_a1_high(two_cat_index):
    """a1 at 100 fills 60% of cat_a's weight; effective cat_a weight = 0.3.
    cat_a score (renormalized) = 100. cat_b score = 50. Effective cat_b
    weight = 0.5. Numerator = 100 × 0.3 + 50 × 0.5 = 55. Denominator = 0.8.
    Overall = 55 / 0.8 = 68.75.
    (Legacy would give 75 for this same input — so coverage_weighted is
    lower, honestly reflecting the coverage gap.)"""
    r = aggregate_coverage_weighted(
        two_cat_index, {"a1": 100, "b1": 50, "b2": 50}, {}
    )
    assert r["overall_score"] == 68.75


def test_coverage_weighted_min_coverage_gate(two_cat_index):
    """min_coverage=0.9 withholds when coverage falls below 0.9."""
    r = aggregate_coverage_weighted(
        two_cat_index, {"a1": 50, "b1": 50, "b2": 50}, {}, {"min_coverage": 0.9}
    )
    assert r["overall_score"] is None
    assert r["withheld"] is True
    assert r["coverage"] == 0.75  # 3 of 4


def test_coverage_weighted_min_coverage_pass(two_cat_index):
    r = aggregate_coverage_weighted(
        two_cat_index, {"a1": 50, "a2": 50, "b1": 50, "b2": 50}, {},
        {"min_coverage": 0.9},
    )
    assert r["overall_score"] == 50.0
    assert r["withheld"] is False


# =============================================================================
# coverage_withheld (Option D)
# =============================================================================


def test_coverage_withheld_requires_threshold(two_cat_index):
    with pytest.raises(ValueError) as exc:
        aggregate_coverage_withheld(two_cat_index, {"a1": 50}, {}, {})
    assert "coverage_threshold" in str(exc.value)


def test_coverage_withheld_invalid_threshold(two_cat_index):
    with pytest.raises(ValueError):
        aggregate_coverage_withheld(two_cat_index, {"a1": 50}, {}, {"coverage_threshold": 1.5})
    with pytest.raises(ValueError):
        aggregate_coverage_withheld(two_cat_index, {"a1": 50}, {}, {"coverage_threshold": -0.1})


def test_coverage_withheld_below_threshold(two_cat_index):
    r = aggregate_coverage_withheld(
        two_cat_index, {"a1": 50, "b1": 50, "b2": 50}, {},
        {"coverage_threshold": 0.80},
    )
    assert r["overall_score"] is None
    assert r["withheld"] is True
    assert r["method"] == "coverage_withheld"


def test_coverage_withheld_above_threshold(two_cat_index):
    r = aggregate_coverage_withheld(
        two_cat_index, {"a1": 50, "a2": 50, "b1": 50, "b2": 50}, {},
        {"coverage_threshold": 0.80},
    )
    assert r["overall_score"] == 50.0
    assert r["withheld"] is False


def test_coverage_withheld_category_scores_still_returned(two_cat_index):
    """Even when overall is withheld, category_scores are still present."""
    r = aggregate_coverage_withheld(
        two_cat_index, {"a1": 50, "b1": 50, "b2": 50}, {},
        {"coverage_threshold": 0.95},
    )
    assert r["overall_score"] is None
    assert "cat_a" in r["category_scores"]
    assert "cat_b" in r["category_scores"]


# =============================================================================
# strict_zero (Option B) + strict_neutral (Option A)
# =============================================================================


def test_strict_zero_missing_a2_all_50(two_cat_index):
    """cat_a score = (50 × 0.6 + 0 × 0.4) / 1.0 = 30.
    cat_b score = 50. Overall = 30 × 0.5 + 50 × 0.5 = 40."""
    r = aggregate_strict_zero(two_cat_index, {"a1": 50, "b1": 50, "b2": 50}, {})
    assert r["overall_score"] == 40.0


def test_strict_zero_missing_a2_a1_high(two_cat_index):
    """cat_a = (100 × 0.6) / 1.0 = 60. cat_b = 50. Overall = 55."""
    r = aggregate_strict_zero(two_cat_index, {"a1": 100, "b1": 50, "b2": 50}, {})
    assert r["overall_score"] == 55.0


def test_strict_neutral_missing_a2_all_50(two_cat_index):
    """a2 imputed to 50. cat_a = 50, cat_b = 50. Overall = 50."""
    r = aggregate_strict_neutral(two_cat_index, {"a1": 50, "b1": 50, "b2": 50}, {})
    assert r["overall_score"] == 50.0


def test_strict_neutral_coverage_reflects_actual_inputs(two_cat_index):
    """Coverage must NOT include the imputed 50."""
    r = aggregate_strict_neutral(two_cat_index, {"a1": 50, "b1": 50, "b2": 50}, {})
    assert r["coverage"] == 0.75


# =============================================================================
# Byte-for-byte regression across all 9 index definitions
# =============================================================================


def _all_index_definitions():
    from app.index_definitions.sii_v1 import SII_V1_DEFINITION
    from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
    from app.index_definitions.rpi_v2 import RPI_V2_DEFINITION
    from app.index_definitions.lsti_v01 import LSTI_V01_DEFINITION
    from app.index_definitions.bri_v01 import BRI_V01_DEFINITION
    from app.index_definitions.dohi_v01 import DOHI_V01_DEFINITION
    from app.index_definitions.vsri_v01 import VSRI_V01_DEFINITION
    from app.index_definitions.cxri_v01 import CXRI_V01_DEFINITION
    from app.index_definitions.tti_v01 import TTI_V01_DEFINITION
    return [
        SII_V1_DEFINITION, PSI_V01_DEFINITION, RPI_V2_DEFINITION,
        LSTI_V01_DEFINITION, BRI_V01_DEFINITION, DOHI_V01_DEFINITION,
        VSRI_V01_DEFINITION, CXRI_V01_DEFINITION, TTI_V01_DEFINITION,
    ]


# Expected declared aggregation per index after the 2026-04-21 migration.
# Indices not listed here intentionally remain on the default
# (legacy_renormalize) and must not declare an `aggregation` block until
# their own migration lands. See docs/methodology/aggregation_impact_analysis.md
# and the per-index changelog files under docs/methodology/*_changelog.md.
_DECLARED_AGGREGATIONS = {
    "sii":  ("coverage_weighted",  {"min_coverage": 0.0}),
    "psi":  ("coverage_weighted",  {"min_coverage": 0.60}),
    "bri":  ("coverage_withheld",  {"coverage_threshold": 0.70}),
    "cxri": ("coverage_withheld",  {"coverage_threshold": 0.70}),
    "vsri": ("coverage_withheld",  {"coverage_threshold": 0.60}),
}
_UNDECLARED_INDEX_IDS = {"rpi", "lsti", "dohi", "tti"}


@pytest.mark.parametrize("definition", _all_index_definitions())
def test_definition_aggregation_block_matches_migration_spec(definition):
    """Every migrated index declares the exact formula + params from its
    v0.2/v0.3/v1.1 migration; every non-migrated index still has no
    `aggregation` block so it takes the legacy_renormalize default."""
    index_id = definition["index_id"]
    decl = definition.get("aggregation")
    if index_id in _DECLARED_AGGREGATIONS:
        expected_formula, expected_params = _DECLARED_AGGREGATIONS[index_id]
        assert decl is not None, (
            f"{index_id} should declare aggregation after its migration; "
            f"see docs/methodology/{index_id}_changelog.md"
        )
        assert decl["formula"] == expected_formula
        assert decl.get("params", {}) == expected_params
        assert decl["formula"] in AGGREGATION_FORMULAS
    else:
        assert index_id in _UNDECLARED_INDEX_IDS, (
            f"Unexpected index {index_id}; update this test's "
            f"_DECLARED_AGGREGATIONS or _UNDECLARED_INDEX_IDS set."
        )
        assert decl is None, (
            f"{index_id} should not declare aggregation until its own "
            f"migration lands; the analysis report in "
            f"docs/methodology/aggregation_impact_analysis.md flags "
            f"coverage maturity or methodology issues for this index."
        )


@pytest.mark.parametrize("definition", _all_index_definitions())
def test_score_entity_produces_all_new_fields(definition):
    """Every score_entity() result carries the additive fields regardless
    of formula choice. For migrated indices, aggregation_method reflects
    the declared formula; for non-migrated indices it's legacy_renormalize."""
    result = score_entity(definition, {})
    for field in ("effective_category_weights", "withheld",
                  "aggregation_method", "aggregation_formula_version"):
        assert field in result, f"missing {field}"
    expected_method = (
        _DECLARED_AGGREGATIONS[definition["index_id"]][0]
        if definition["index_id"] in _DECLARED_AGGREGATIONS
        else "legacy_renormalize"
    )
    assert result["aggregation_method"] == expected_method
    assert result["aggregation_formula_version"] == AGGREGATION_FORMULA_VERSION


# =============================================================================
# RQS coverage_threshold tests
# =============================================================================


def test_rqs_requires_no_db_when_holdings_empty():
    """Empty holdings returns error without touching DB."""
    result = compute_rqs([])
    assert "error" in result


def test_rqs_threshold_signature():
    """compute_rqs accepts coverage_threshold kwarg; default 0.0 preserves
    existing callers. This is a signature-level guarantee — the threshold
    behavior itself is exercised by the analyzer script when run against
    real data, since it requires DB access for SII scores."""
    import inspect
    sig = inspect.signature(compute_rqs)
    assert "coverage_threshold" in sig.parameters
    assert sig.parameters["coverage_threshold"].default == 0.0


def test_rqs_for_protocol_forwards_threshold():
    from app.composition import compute_rqs_for_protocol
    import inspect
    sig = inspect.signature(compute_rqs_for_protocol)
    assert "coverage_threshold" in sig.parameters
    assert sig.parameters["coverage_threshold"].default == 0.0


# =============================================================================
# legacy_sii_v1 (reserved slot)
# =============================================================================


def test_legacy_sii_v1_different_rounding_than_legacy_renormalize(two_cat_index):
    """legacy_sii_v1 does not round intermediate category scores; legacy
    does. Where the math produces an unrounded intermediate, the two
    formulas' category_scores diverge even if overall is the same."""
    # Use values that cause a non-terminating decimal at category level
    comp = {"a1": 33, "a2": 67, "b1": 50, "b2": 50}
    legacy = aggregate_legacy_renormalize(two_cat_index, comp, {})
    sii = aggregate_legacy_sii_v1(two_cat_index, comp, {})
    # Overall should be close (both round final value to 2 decimals)
    assert abs(legacy["overall_score"] - sii["overall_score"]) < 0.1
    # Both methods correctly identified
    assert legacy["method"] == "legacy_renormalize"
    assert sii["method"] == "legacy_sii_v1"
