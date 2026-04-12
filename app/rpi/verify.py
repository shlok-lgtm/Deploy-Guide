"""
RPI Verification
=================
Verify Aave and Compound RPI scores against known facts.
Run standalone: python -m app.rpi.verify
"""

from app.rpi.scorer import (
    normalize_spend_ratio,
    normalize_vendor_diversity,
    normalize_parameter_velocity,
    normalize_parameter_recency,
    normalize_incident_severity,
    normalize_recovery_ratio,
    normalize_external_scoring,
    normalize_governance_health,
)
from app.scoring_engine import score_entity
from app.index_definitions.rpi_v01 import RPI_V01_DEFINITION


def verify_aave():
    """Verify Aave RPI against known facts."""
    print("=" * 60)
    print("AAVE RPI VERIFICATION")
    print("=" * 60)

    # spend_ratio: $5M / $142M = 3.5%
    spend = normalize_spend_ratio(3.5)
    print(f"spend_ratio: 3.5% → {spend}")
    assert 43 <= spend <= 44, f"Expected ~43.75, got {spend}"

    # vendor_diversity: 1 vendor (LlamaRisk only)
    vendor = normalize_vendor_diversity(1, False)
    print(f"vendor_diversity: 1 vendor → {vendor}")
    assert vendor == 30.0, f"Expected 30, got {vendor}"

    # parameter_velocity: assume ~5 changes/month (active protocol)
    param_vel = normalize_parameter_velocity(5)
    print(f"parameter_velocity: 5/month → {param_vel}")
    assert param_vel == 80.0, f"Expected 80, got {param_vel}"

    # parameter_recency: assume recent change within 7 days
    param_rec = normalize_parameter_recency(5)
    print(f"parameter_recency: 5 days → {param_rec}")
    assert param_rec == 100.0, f"Expected 100, got {param_rec}"

    # incident_severity: CAPO oracle incident (major, weight=3)
    # Single major incident: 100 * exp(-0.5 * 3) ≈ 22.31
    import math
    incident = normalize_incident_severity(3.0)
    expected = round(100.0 * math.exp(-0.5 * 3.0), 2)
    print(f"incident_severity: weight 3.0 → {incident} (expected {expected})")
    assert abs(incident - expected) < 0.1, f"Expected {expected}, got {incident}"

    # recovery_ratio: $0 recovered / $26.9M at risk = 0%
    recovery = normalize_recovery_ratio(0.0)
    print(f"recovery_ratio: 0% → {recovery}")
    assert recovery == 0.0, f"Expected 0, got {recovery}"

    # external_scoring: none
    ext = normalize_external_scoring(0)
    print(f"external_scoring: 0 → {ext}")
    assert ext == 0.0

    # documentation_depth: 80 (4/5 criteria)
    doc = 80.0
    print(f"documentation_depth: {doc}")

    # governance_health: ~15% participation
    gov = normalize_governance_health(15)
    print(f"governance_health: 15% → {gov}")
    assert gov == 60.0

    # Now run through score_entity
    raw_values = {
        "spend_ratio": spend,
        "vendor_diversity": vendor,
        "parameter_velocity": param_vel,
        "parameter_recency": param_rec,
        "incident_severity": incident,
        "recovery_ratio": recovery,
        "external_scoring": ext,
        "documentation_depth": doc,
        "governance_health": gov,
    }

    result = score_entity(RPI_V01_DEFINITION, raw_values)
    print(f"\nOverall RPI: {result['overall_score']}")
    print(f"Category scores: {result['category_scores']}")
    print(f"Coverage: {result['coverage']}")
    print(f"Confidence: {result['confidence']}")

    # Verify: should be moderate-to-low given Chaos Labs departure and CAPO incident
    overall = result["overall_score"]
    print(f"\nAave RPI = {overall} → {'PASS' if 35 <= overall <= 60 else 'UNEXPECTED (should be moderate-to-low)'}")

    return result


def verify_compound():
    """Verify Compound RPI against known facts."""
    print("\n" + "=" * 60)
    print("COMPOUND RPI VERIFICATION")
    print("=" * 60)

    # spend_ratio: ~4%
    spend = normalize_spend_ratio(4.0)
    print(f"spend_ratio: 4.0% → {spend}")
    assert spend == 50.0, f"Expected 50, got {spend}"

    # vendor_diversity: 2 vendors (Gauntlet + OpenZeppelin)
    vendor = normalize_vendor_diversity(2, False)
    print(f"vendor_diversity: 2 vendors → {vendor}")
    assert vendor == 60.0, f"Expected 60, got {vendor}"

    # parameter_velocity: assume ~4 changes/month (Compound Configurator)
    param_vel = normalize_parameter_velocity(4)
    print(f"parameter_velocity: 4/month → {param_vel}")
    assert param_vel == 80.0

    # parameter_recency: assume within 14 days
    param_rec = normalize_parameter_recency(10)
    print(f"parameter_recency: 10 days → {param_rec}")
    assert param_rec == 80.0

    # incident_severity: deUSD incident (major, weight=3)
    import math
    incident = normalize_incident_severity(3.0)
    print(f"incident_severity: weight 3.0 → {incident}")

    # recovery_ratio: $12M / $15.6M = 76.9%
    recovery = normalize_recovery_ratio(0.769)
    print(f"recovery_ratio: 76.9% → {recovery}")
    assert recovery == 80.0, f"Expected 80, got {recovery}"

    # external_scoring: none
    ext = normalize_external_scoring(0)
    print(f"external_scoring: 0 → {ext}")

    # documentation_depth: 60 (3/5 criteria)
    doc = 60.0
    print(f"documentation_depth: {doc}")

    # governance_health: ~12%
    gov = normalize_governance_health(12)
    print(f"governance_health: 12% → {gov}")
    assert gov == 60.0

    raw_values = {
        "spend_ratio": spend,
        "vendor_diversity": vendor,
        "parameter_velocity": param_vel,
        "parameter_recency": param_rec,
        "incident_severity": incident,
        "recovery_ratio": recovery,
        "external_scoring": ext,
        "documentation_depth": doc,
        "governance_health": gov,
    }

    result = score_entity(RPI_V01_DEFINITION, raw_values)
    print(f"\nOverall RPI: {result['overall_score']}")
    print(f"Category scores: {result['category_scores']}")

    overall = result["overall_score"]
    # Compound should score meaningfully higher than Aave on vendor_diversity
    print(f"\nCompound RPI = {overall}")

    return result


def verify_comparison(aave_result, compound_result):
    """Verify cross-protocol expectations."""
    print("\n" + "=" * 60)
    print("CROSS-PROTOCOL COMPARISON")
    print("=" * 60)

    aave_score = aave_result["overall_score"]
    compound_score = compound_result["overall_score"]

    print(f"Aave RPI:     {aave_score}")
    print(f"Compound RPI: {compound_score}")
    print(f"Delta:        {compound_score - aave_score:+.2f}")

    # Compound should score higher on vendor_diversity
    aave_org = aave_result["category_scores"].get("organization", 0)
    comp_org = compound_result["category_scores"].get("organization", 0)
    print(f"\nOrganization: Aave={aave_org}, Compound={comp_org} → {'PASS' if comp_org > aave_org else 'FAIL'}")

    # Compound should score higher on recovery_ratio (80 vs 0)
    aave_hist = aave_result["category_scores"].get("history", 0)
    comp_hist = compound_result["category_scores"].get("history", 0)
    print(f"History:      Aave={aave_hist}, Compound={comp_hist} → {'PASS' if comp_hist > aave_hist else 'FAIL'}")

    # Overall: Compound should score meaningfully higher
    print(f"\nCompound scores {'higher' if compound_score > aave_score else 'LOWER'} than Aave: {'PASS' if compound_score > aave_score else 'UNEXPECTED'}")


if __name__ == "__main__":
    aave = verify_aave()
    compound = verify_compound()
    verify_comparison(aave, compound)
    print("\n✓ Verification complete")
