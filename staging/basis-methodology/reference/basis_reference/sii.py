"""
SII v1.0.0 — reference implementation.

This module implements exactly the procedure in spec/sii_formula.md.
It uses only the Python standard library. It is intentionally short;
the goal is end-to-end auditability, not performance.

DO NOT add convenience features, logging, or caching. DO NOT import
anything from basis-hub. If the spec is ambiguous, the spec is wrong;
fix the spec and update this file.
"""

from __future__ import annotations

from typing import Optional

from basis_reference.hashing import computation_hash, input_hash


VERSION = "v1.0.0"

# Section 2 — category weights. Order matters (section 6).
CATEGORY_WEIGHTS = (
    ("peg_stability",             0.30),
    ("liquidity_depth",           0.25),
    ("mint_burn_dynamics",        0.15),
    ("holder_distribution",       0.10),
    ("structural_risk_composite", 0.20),
)

# Section 4 — structural subcategory weights. Order matters.
STRUCTURAL_SUBWEIGHTS = (
    ("reserves_collateral",   0.30),
    ("smart_contract_risk",   0.20),
    ("oracle_integrity",      0.15),
    ("governance_operations", 0.20),
    ("network_chain_risk",    0.15),
)


def _weighted_aggregate(
    values: dict, weight_order: tuple
) -> tuple[Optional[float], float]:
    total = 0.0
    used_wt = 0.0
    for key, weight in weight_order:
        score = values.get(key)
        if score is None:
            continue
        total += float(score) * weight
        used_wt += weight
    if used_wt == 0.0:
        return None, 0.0
    if used_wt < 1.0:
        return total / used_wt, used_wt
    return total, used_wt


def compute_structural(subscores: dict) -> Optional[float]:
    """Section 4 — weighted aggregate of the five structural subcategories."""
    value, _ = _weighted_aggregate(subscores, STRUCTURAL_SUBWEIGHTS)
    return value


def compute(category_scores: dict) -> dict:
    """
    Run the SII v1.0.0 formula on a category score vector.

    `category_scores` is a dict keyed by the five SII categories. Values are
    floats in [0, 100] or None. Returns section-7 output dict plus
    computation_hash.
    """
    score, used_wt = _weighted_aggregate(category_scores, CATEGORY_WEIGHTS)
    missing = [
        k for k, _ in CATEGORY_WEIGHTS if category_scores.get(k) is None
    ]
    return {
        "score": score,
        "version": VERSION,
        "used_weight": used_wt,
        "missing_categories": missing,
        "input_hash": input_hash(category_scores),
        "computation_hash": computation_hash(category_scores, VERSION, score),
    }
