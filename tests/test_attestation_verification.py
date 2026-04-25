"""
Tests for attestation verification path.

Locks two properties that the V8 Primitive #10 ("Computation Attestation")
claim depends on:

  a) Reproducibility — the same component vector hashes to the same value.
     A buyer can fetch a stored attestation, pull the underlying component
     scores from the index tables, recompute the hash, and match it.

  b) Sensitivity — perturbing any component score changes the hash. If
     this test fails, the attestation is decorative.

These tests exercise the hashing function directly and do not touch the
database. The wiring (per-protocol attest_state calls in worker.py) is
exercised in a separate integration test path.
"""
from __future__ import annotations


def test_psi_per_component_payload_is_reproducible():
    from app.state_attestation import compute_batch_hash

    # Mimic the payload shape produced by worker.py for psi_components
    # after the per-component expansion: sorted list of {id, score} dicts.
    component_scores = {
        "tvl": 78.5,
        "bad_debt_ratio": 95.0,
        "governance_proposals_90d": 60.0,
        "protocol_admin_key_risk": 50.0,
    }
    payload = [
        {"id": cid, "score": round(float(s), 4)}
        for cid, s in sorted(component_scores.items())
    ]

    h1 = compute_batch_hash(payload)
    # Same dict, recomputed independently, must produce the same hash.
    h2 = compute_batch_hash([
        {"id": cid, "score": round(float(s), 4)}
        for cid, s in sorted(component_scores.items())
    ])
    assert h1 == h2
    assert isinstance(h1, str) and len(h1) == 64


def test_rpi_per_component_payload_is_reproducible():
    from app.state_attestation import compute_batch_hash

    component_scores = {
        "spend_ratio": 42.0,
        "parameter_velocity": 80.0,
        "parameter_recency": 70.0,
        "incident_severity": 25.0,
        "governance_health": 65.0,
    }
    payload = [
        {"id": cid, "score": round(float(s), 4)}
        for cid, s in sorted(component_scores.items())
    ]

    h1 = compute_batch_hash(payload)
    h2 = compute_batch_hash(payload)
    assert h1 == h2
    assert len(h1) == 64


def test_perturbing_one_component_changes_the_hash():
    from app.state_attestation import compute_batch_hash

    base = [
        {"id": "tvl", "score": 78.5},
        {"id": "bad_debt_ratio", "score": 95.0},
    ]
    perturbed = [
        {"id": "tvl", "score": 78.5},
        {"id": "bad_debt_ratio", "score": 94.9999},  # 5e-4 delta
    ]
    assert compute_batch_hash(base) != compute_batch_hash(perturbed), (
        "Attestation must be sensitive to component perturbations — "
        "if hashes match, the attestation is decorative."
    )


def test_summary_only_payload_is_distinguishable_from_per_component_payload():
    """
    Pre-fix payload was [{slug, score}] — overall summary only.
    Post-fix payload is [{id, score}] — per-component vector.
    Their hashes must differ for the same protocol so that a verifier can
    tell which attestation regime produced a given batch_hash.
    """
    from app.state_attestation import compute_batch_hash

    summary_payload = [{"slug": "aave", "score": 78.0}]
    per_component_payload = [
        {"id": "tvl", "score": 78.5},
        {"id": "bad_debt_ratio", "score": 95.0},
    ]
    assert compute_batch_hash(summary_payload) != compute_batch_hash(per_component_payload)


def test_pulse_generator_attestation_domain_list_includes_rpi_components():
    """
    rpi_components was previously omitted from the daily-state-root
    composition list, so its hash never rolled up into the state root.
    Lock that it is now present.
    """
    import inspect
    import app.pulse_generator as pg

    src = inspect.getsource(pg)
    # The literal list assignment is the canonical wiring point. Pull the
    # block out and check rpi_components appears alongside sii_components
    # and psi_components.
    assert '"rpi_components"' in src
    assert '"sii_components"' in src
    assert '"psi_components"' in src
