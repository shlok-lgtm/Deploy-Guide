"""
SII v1.1.0 — Stablecoin Integrity Index expressed as an index definition.

This mirrors the canonical weights and component normalizations in app/scoring.py
but expressed in the generic index definition format so SII and PSI share a schema.

v1.1.0 — aggregation migrated from legacy SII v1.0.0 renormalization to
coverage_weighted with min_coverage=0.0. Weights, categories, and
components are unchanged. The production scoring path in app/scoring.py
(calculate_sii + calculate_structural_composite) is NOT yet re-routed
through the aggregation registry; that wiring is the follow-up tracked
by the TODO in app/scoring.py::calculate_sii. This declaration change
lets the analyzer and any downstream consumer that reads the definition
see the target formula. See docs/methodology/aggregation_impact_analysis.md
and docs/methodology/sii_changelog.md.
"""

from app.scoring import SII_V1_WEIGHTS, STRUCTURAL_SUBWEIGHTS, COMPONENT_NORMALIZATIONS

# Map Python function references to string names for the definition format
_FN_NAME_MAP = {
    "normalize_inverse_linear": "inverse_linear",
    "normalize_linear": "linear",
    "normalize_log": "log",
    "normalize_centered": "centered",
    "normalize_exponential_penalty": "exponential_penalty",
    "normalize_direct": "direct",
}


def _fn_to_name(fn) -> str:
    return _FN_NAME_MAP.get(fn.__name__, fn.__name__)


def _build_components():
    """Convert COMPONENT_NORMALIZATIONS to generic index definition format."""
    components = {}
    for comp_id, spec in COMPONENT_NORMALIZATIONS.items():
        components[comp_id] = {
            "name": comp_id.replace("_", " ").title(),
            "category": spec["category"],
            "weight": spec["weight"],
            "normalization": {
                "function": _fn_to_name(spec["fn"]),
                "params": spec["params"],
            },
            "data_source": "sii_collectors",
        }
    return components


SII_V1_DEFINITION = {
    "index_id": "sii",
    "version": "v1.1.0",
    "name": "Stablecoin Integrity Index",
    "description": "Deterministic, versioned scoring system for stablecoin risk",
    "entity_type": "stablecoin",
    "aggregation": {
        "formula": "coverage_weighted",
        "params": {"min_coverage": 0.0},
    },
    "categories": {
        cat_id: {"name": cat_id.replace("_", " ").title(), "weight": weight}
        for cat_id, weight in SII_V1_WEIGHTS.items()
    },
    "structural_subcategories": {
        sub_id: {"name": sub_id.replace("_", " ").title(), "weight": weight}
        for sub_id, weight in STRUCTURAL_SUBWEIGHTS.items()
    },
    "components": _build_components(),
}
