"""
Validate the renormalization claim empirically.

Claim from rsETH audit (audits/internal/lsti_rseth_audit_2026-04-20.md, Q1):

    "score_entity() silently renormalizes weighted sums over whatever components
    happen to be populated, so a missing component behaves as if its category
    peers were perfect."

Test plan:
1. Construct a minimal synthetic index: 2 categories, 4 components.
   - Category A (weight 0.5): 2 components with weights 0.6, 0.4
   - Category B (weight 0.5): 2 components with weights 0.5, 0.5
2. Populate ALL 4 components with score = 50. Expected overall = 50.
3. Populate 3 of 4 components. Observe what the engine returns and compare to:
   (a) honest-coverage expected score (treating missing as 0 with full denominator)
   (b) the renormalization-inflated score the audit predicts
4. Do this for two distinct missing-component cases to show the bias depends
   on which component is missing.
5. Print a report that either confirms or refutes the audit's claim.
"""

from app.scoring_engine import score_entity

# Synthetic minimal index
INDEX = {
    "index_id": "test",
    "version": "v0.0.1",
    "name": "Test",
    "description": "Minimal synthetic index for renormalization validation",
    "entity_type": "test",
    "categories": {
        "cat_a": {"name": "Cat A", "weight": 0.50},
        "cat_b": {"name": "Cat B", "weight": 0.50},
    },
    "components": {
        "a1": {"name": "A1", "category": "cat_a", "weight": 0.60,
               "normalization": {"function": "direct", "params": {}},
               "data_source": "test"},
        "a2": {"name": "A2", "category": "cat_a", "weight": 0.40,
               "normalization": {"function": "direct", "params": {}},
               "data_source": "test"},
        "b1": {"name": "B1", "category": "cat_b", "weight": 0.50,
               "normalization": {"function": "direct", "params": {}},
               "data_source": "test"},
        "b2": {"name": "B2", "category": "cat_b", "weight": 0.50,
               "normalization": {"function": "direct", "params": {}},
               "data_source": "test"},
    },
}


def run_case(label, raw_values, expected_if_missing_treated_as_zero,
             expected_if_renormalized):
    result = score_entity(INDEX, raw_values)
    engine_score = result["overall_score"]
    print(f"\n{'=' * 70}\nCASE: {label}\n{'=' * 70}")
    print(f"Inputs: {raw_values}\n")
    print(f"  Engine output:                   {engine_score:6.2f}")
    print(f"  If missing treated as 0:         {expected_if_missing_treated_as_zero:6.2f}")
    print(f"  If renormalized over present:    {expected_if_renormalized:6.2f}\n")
    print(f"  Engine matches 'renormalized'?    {abs(engine_score - expected_if_renormalized) < 0.01}")
    print(f"  Engine matches 'zero-for-miss'?   {abs(engine_score - expected_if_missing_treated_as_zero) < 0.01}\n")
    print(f"  coverage reported:   {result['coverage']}")
    print(f"  confidence tag:      {result['confidence_tag']}")
    return result


print("\n" + "=" * 70)
print("BASELINE: all 4 components populated at 50")
print("=" * 70)
baseline = score_entity(INDEX, {"a1": 50, "a2": 50, "b1": 50, "b2": 50})
print(f"Overall: {baseline['overall_score']} (expect 50.0)")
assert baseline["overall_score"] == 50.0, "baseline broken"

# Case 1: Drop a2. All remaining at 50.
# Honest: cat_a = (50*0.60 + 0*0.40) / 1.0 = 30; cat_b = 50; overall = 40
# Renormalized: cat_a = (50*0.60) / 0.60 = 50; cat_b = 50; overall = 50
run_case(
    "Missing a2 (weight 0.40 within cat_a) — all populated values are 50",
    {"a1": 50, "b1": 50, "b2": 50},
    expected_if_missing_treated_as_zero=40.0,
    expected_if_renormalized=50.0,
)

# Case 2: Same missing component, a1=100.
# Honest: cat_a = (100*0.60) / 1.0 = 60; cat_b = 50; overall = 55
# Renormalized: cat_a = 100; cat_b = 50; overall = 75
run_case(
    "Missing a2 — a1=100, b1=50, b2=50 (shows component with high value carries whole category)",
    {"a1": 100, "b1": 50, "b2": 50},
    expected_if_missing_treated_as_zero=55.0,
    expected_if_renormalized=75.0,
)

# Case 3: Mixed quality.
# Honest: cat_a = 30; cat_b = 60; overall = 45
# Renormalized: cat_a = 50; cat_b = 60; overall = 55
run_case(
    "Missing a2 — a1=50, b1=100, b2=20 (mixed quality — shows direction of bias)",
    {"a1": 50, "b1": 100, "b2": 20},
    expected_if_missing_treated_as_zero=45.0,
    expected_if_renormalized=55.0,
)

# Case 4: Entire category missing.
# Honest: overall = 0*0.5 + 50*0.5 = 25
# Renormalized: overall = (50*0.5) / 0.5 = 50
run_case(
    "Cat_A missing entirely — only b1=50, b2=50 populated",
    {"b1": 50, "b2": 50},
    expected_if_missing_treated_as_zero=25.0,
    expected_if_renormalized=50.0,
)

# Case 5: rsETH-shaped validator category.
INDEX_VALIDATOR = {
    "index_id": "test_validator",
    "version": "v0.0.1",
    "name": "Test — validator category shape",
    "description": "Mimics rsETH's validator_operator category",
    "entity_type": "test",
    "categories": {
        "cat_a": {"name": "Validator/Operator", "weight": 0.15},
        "cat_b": {"name": "Full cat", "weight": 0.85},
    },
    "components": {
        "a1": {"name": "A1", "category": "cat_a", "weight": 0.30,
               "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
        "a2": {"name": "A2", "category": "cat_a", "weight": 0.25,
               "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
        "a3": {"name": "A3", "category": "cat_a", "weight": 0.20,
               "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
        "a4": {"name": "A4", "category": "cat_a", "weight": 0.15,
               "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
        "a5": {"name": "A5", "category": "cat_a", "weight": 0.10,
               "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
        "b1": {"name": "B1", "category": "cat_b", "weight": 1.00,
               "normalization": {"function": "direct", "params": {}}, "data_source": "test"},
    },
}

# 1 of 5 in cat_a populated at 40, b1=80.
# Honest: cat_a = (40*0.30)/1.0 = 12; cat_b = 80; overall = 12*0.15 + 80*0.85 = 69.8
# Renormalized: cat_a = 40; overall = 40*0.15 + 80*0.85 = 74.0
result = score_entity(INDEX_VALIDATOR, {"a1": 40, "b1": 80})
print(f"\n{'=' * 70}\nCASE: rsETH-like validator category — 1 of 5 components populated\n{'=' * 70}")
print(f"Inputs: a1=40 (only populated in cat_a), b1=80 (cat_b full)\n")
print(f"  Engine output:                   {result['overall_score']:6.2f}")
print(f"  If missing treated as 0:         {69.80:6.2f}")
print(f"  If renormalized over present:    {74.00:6.2f}\n")
print(f"  Inflation (engine - honest):     {result['overall_score'] - 69.80:+.2f}")

print(f"\n{'=' * 70}\nCONCLUSION\n{'=' * 70}")
print("""
If the engine output matches the 'renormalized' column in every case,
the audit's claim is confirmed empirically: missing components are
silently rescaled rather than contributing zero to the overall score.
""")
