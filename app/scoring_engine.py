"""
Generic Scoring Engine
=======================
Scores any entity using an index definition dict.
Reuses normalization functions from app/scoring.py — no duplication.

Pattern: raw values -> normalize per component -> aggregate by category -> weighted sum = score.
"""

from app.scoring import (
    normalize_inverse_linear, normalize_linear, normalize_log,
    normalize_centered, normalize_exponential_penalty, normalize_direct,
)

NORMALIZATION_FUNCTIONS = {
    "inverse_linear": normalize_inverse_linear,
    "linear": normalize_linear,
    "log": normalize_log,
    "centered": normalize_centered,
    "exponential_penalty": normalize_exponential_penalty,
    "direct": normalize_direct,
}


def score_entity(definition, raw_values):
    """
    Score an entity using an index definition.

    Args:
        definition: An index definition dict (see app/index_definitions/schema.py)
        raw_values: Dict of component_id -> raw numeric value

    Returns dict with: index_id, version, overall_score,
        category_scores, component_scores, components_available,
        components_total, coverage
    """
    # Step 1: Normalize each component
    component_scores = {}
    for comp_id, comp_def in definition["components"].items():
        if comp_id in raw_values and raw_values[comp_id] is not None:
            fn_name = comp_def["normalization"]["function"]
            fn = NORMALIZATION_FUNCTIONS.get(fn_name)
            if fn:
                params = comp_def["normalization"]["params"]
                try:
                    component_scores[comp_id] = round(fn(raw_values[comp_id], **params), 2)
                except Exception:
                    pass

    # Step 2: Aggregate by category (weighted average within category)
    category_scores = {}
    for cat_id, cat_def in definition["categories"].items():
        cat_components = {
            cid: cdef for cid, cdef in definition["components"].items()
            if cdef["category"] == cat_id
        }
        total = 0.0
        weight_used = 0.0
        for cid, cdef in cat_components.items():
            if cid in component_scores:
                total += component_scores[cid] * cdef["weight"]
                weight_used += cdef["weight"]
        if weight_used > 0:
            category_scores[cat_id] = round(total / weight_used, 2)

    # Step 3: Weighted sum across categories
    overall = 0.0
    cat_weight_used = 0.0
    for cat_id, cat_def in definition["categories"].items():
        weight = cat_def["weight"] if isinstance(cat_def, dict) else 0
        if cat_id in category_scores:
            overall += category_scores[cat_id] * weight
            cat_weight_used += weight

    if cat_weight_used > 0 and cat_weight_used < 1.0:
        overall = overall / cat_weight_used

    overall = round(overall, 2)

    # Confidence tagging based on coverage
    components_available = len(component_scores)
    components_total = len(definition["components"])
    coverage = round(components_available / max(components_total, 1), 2)

    # Identify categories with zero populated components
    all_categories = set(definition["categories"].keys())
    missing_categories = sorted(all_categories - set(category_scores.keys()))

    confidence_meta = compute_confidence_tag(
        len(category_scores), len(all_categories), coverage, missing_categories
    )

    return {
        "index_id": definition["index_id"],
        "version": definition["version"],
        "overall_score": overall,
        "category_scores": category_scores,
        "component_scores": component_scores,
        "components_available": components_available,
        "components_total": components_total,
        "coverage": coverage,
        "confidence": confidence_meta["confidence"],
        "confidence_tag": confidence_meta["tag"],
        "missing_categories": confidence_meta["missing_categories"],
    }


def is_category_complete(raw_values: dict, index_definition: dict) -> tuple:
    """
    Check if every weighted category in the index definition has
    at least one populated component.

    Returns:
        (is_complete: bool, missing_categories: list[str])

    A stablecoin/protocol is eligible for scored status only if
    is_complete is True.
    """
    missing = []
    for cat_id, cat_def in index_definition["categories"].items():
        # Find components belonging to this category
        cat_components = [
            comp_id for comp_id, comp_def in index_definition["components"].items()
            if comp_def.get("category") == cat_id
        ]
        # Check if at least one has a non-None value
        has_data = any(
            comp_id in raw_values and raw_values[comp_id] is not None
            for comp_id in cat_components
        )
        if not has_data:
            name = cat_def["name"] if isinstance(cat_def, dict) and "name" in cat_def else cat_id
            missing.append(cat_id)

    return (len(missing) == 0, missing)


def is_sii_category_complete_legacy(components: list) -> tuple:
    """
    Check SII category completeness using legacy component readings
    (the list[dict] format from collect_all_components).

    SII v1.0.0 has 5 weighted categories. Legacy component categories
    map to v1 categories as follows:
      peg_stability → peg_stability
      liquidity → liquidity_depth
      flows, market_activity → mint_burn_dynamics
      holder_distribution → holder_distribution
      smart_contract, governance, transparency, network, reserves, oracle → structural_risk_composite

    Returns:
        (is_complete: bool, missing_v1_categories: list[str])
    """
    legacy_to_v1 = {
        "peg_stability": "peg_stability",
        "liquidity": "liquidity_depth",
        "market_activity": "mint_burn_dynamics",
        "flows": "mint_burn_dynamics",
        "holder_distribution": "holder_distribution",
        "smart_contract": "structural_risk_composite",
        "governance": "structural_risk_composite",
        "transparency": "structural_risk_composite",
        "regulatory": "structural_risk_composite",
        "network": "structural_risk_composite",
        "reserves": "structural_risk_composite",
        "oracle": "structural_risk_composite",
    }

    v1_categories_found = set()
    for comp in components:
        cat = comp.get("category", "")
        # A category is "present" if any component has data collected for it.
        # We check raw_value (not normalized_score) because some normalizers
        # return None for edge-case raw values like 0.0 while the data IS present.
        has_data = comp.get("normalized_score") is not None or comp.get("raw_value") is not None
        if has_data and cat in legacy_to_v1:
            v1_categories_found.add(legacy_to_v1[cat])

    all_v1 = {"peg_stability", "liquidity_depth", "mint_burn_dynamics",
              "holder_distribution", "structural_risk_composite"}
    missing = sorted(all_v1 - v1_categories_found)
    return (len(missing) == 0, missing)


def compute_confidence_tag(populated_categories, total_categories, component_coverage, missing_categories=None):
    """
    Returns confidence metadata for a score based on data coverage.

    - high: >= 80% component coverage
    - standard: >= 60% component coverage
    - limited: < 60% component coverage
    """
    if missing_categories is None:
        missing_categories = []

    if component_coverage >= 0.80:
        return {"confidence": "high", "tag": None, "missing_categories": []}
    elif component_coverage >= 0.60:
        return {"confidence": "standard", "tag": "STANDARD", "missing_categories": missing_categories}
    else:
        return {"confidence": "limited", "tag": "LIMITED DATA", "missing_categories": missing_categories}
