"""
Seed initial methodology hashes into the methodology_hashes table.

Idempotent — skips any methodology_id that already exists.

Run as:
    python scripts/seed_initial_methodology.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.methodology_hashes import register_methodology


# =============================================================================
# 1. Track Record Rules v1
# =============================================================================

TRACK_RECORD_RULES_V1 = """Track Record Auto-Entry Rules v1
==================================

The track record system detects qualifying signals from existing attested state
and logs them as track_record_entries. It runs at the end of each slow cycle.
Each entry captures a frozen baseline snapshot at trigger time for future
follow-up evaluation.

Active Rules:

Rule A — Material Score Change (>=10 points in 7 days)
  Monitors SII and PSI scores for absolute changes of 10 or more points over a
  rolling 7-day window. Captures current_score, previous_score, delta, and
  direction (up/down). Source domains: sii_components, psi_components.

Rule B — Divergence Signal (critical/alert severity)
  Monitors divergence_signals table for signals created in the last 2 hours with
  severity of 'critical' or 'alert'. Captures detector_name, signal_direction,
  magnitude, and severity. Source domain: divergence_signals.

Rule C — Coherence Drop (issues_found > 0)
  Monitors coherence_reports for reports with issues_found > 0 created in the
  last 2 hours. Creates a system-level entry capturing domains_checked,
  issues_found, and first 5 detail items. Source domain: coherence_reports.

Rule D — Oracle Stress Event (open events)
  Monitors oracle_stress_events for events with no event_end that started in the
  last 2 hours. Captures oracle_address, oracle_name, asset_symbol, chain,
  event_type, max_deviation_pct, and max_latency_seconds. Only triggers for
  entities with existing SII scores. Source domain: oracle_stress_events.

Rule E — Governance Proposal Edit (body_changed)
  Monitors governance_proposals where body_changed=TRUE and captured_at is within
  the last 2 hours. Captures proposal_id, protocol_slug, title, first and current
  body hashes. Only triggers for entities with existing PSI scores. Source domain:
  governance_proposals.

Rule F — Contract Upgrade Detected
  Monitors contract_upgrade_history for upgrades detected in the last 2 hours.
  Captures contract_address, chain, previous/current bytecode hashes,
  previous/current implementation addresses, block_number. Tries SII first,
  then PSI for entity mapping. Source domain: contract_upgrades.

Rule G — RPI Week-over-Week Delta >= 10 Points
  Monitors rpi_scores for week-over-week absolute changes of 10 or more points.
  Captures score_before, score_after, delta, direction, and per-component deltas
  (for components with changes >= 1 point). Source domain: rpi_scores.

Dispatch list:
  score_change    -> _rule_a_score_changes   (sii_components)
  divergence      -> _rule_b_divergence      (divergence_signals)
  coherence_drop  -> _rule_c_coherence_drop  (coherence_reports)
  oracle_stress   -> _rule_d_oracle_stress   (oracle_stress_events)
  governance_edit -> _rule_e_governance_edit  (governance_proposals)
  contract_upgrade-> _rule_f_contract_upgrade(contract_upgrades)
  rpi_delta       -> _rule_g_rpi_delta       (rpi_scores)

Freshness gate: Each rule's source domain is checked against the latest
coherence report. If the domain is stale, the rule is skipped entirely.

Idempotency: Each entry is keyed by a SHA-256 content_hash computed from
(entity_slug, trigger_kind, trigger_detail, triggered_at, baseline_snapshot).
Duplicate hashes are rejected via ON CONFLICT DO NOTHING.
"""

# =============================================================================
# 2. Track Record Outcome Rubric v1
# =============================================================================

TRACK_RECORD_OUTCOME_RUBRIC_V1 = """Track Record Outcome Classification Rubric v1
================================================

Follow-up evaluations are performed at 30, 60, and 90 day checkpoints after
each track record entry's triggered_at timestamp. Each checkpoint produces an
independent outcome classification.

Evaluation Windows:
  30d — first checkpoint, 30 days after trigger
  60d — second checkpoint, 60 days after trigger
  90d — third and final checkpoint, 90 days after trigger

Outcome Categories:

  validated
    Clear confirmation of the signal's direction. Criteria by trigger kind:
    - score_change (down): score continued declining (delta < -5)
    - score_change (up): score continued improving (delta > 5)
    - divergence (deteriorating): score declined by more than 5 points
    - coherence_drop: coherence issues resolved (current issues_found = 0)

  mixed
    Partial confirmation or ambiguous outcome. Criteria:
    - score_change (down): score roughly stable after decline signal (|delta| <= 5)
    - score_change (up): score roughly stable after improvement signal (|delta| <= 5)
    - divergence: minimal score movement (|delta| <= 5) or ambiguous direction
    - coherence_drop: coherence issues persist
    - Any unhandled trigger_kind defaults to mixed

  not_borne_out
    Opposite of what was flagged. Criteria:
    - score_change (down): score recovered significantly (delta > 5)
    - score_change (up): score reversed after improvement signal (delta < -5)
    - divergence (deteriorating): score improved (delta > 5)

  insufficient_data
    Cannot determine outcome. Criteria:
    - Entity dropped from coverage (no longer scored)
    - Missing scores (baseline or current score is None)
    - Cannot determine coherence resolution

Score delta threshold: 5 points (absolute) separates meaningful movement from noise.

Idempotency: Each followup is keyed by (entry_id, checkpoint) with a SHA-256
content_hash computed from (entry_id, checkpoint, current_snapshot,
outcome_category, outcome_detail). Duplicates rejected via ON CONFLICT DO NOTHING.
"""

# =============================================================================
# 3. SII Component Weights v1
# =============================================================================

SII_COMPONENT_WEIGHTS_V1 = """SII Component Weights v1.0.0
==============================

Stablecoin Integrity Index — canonical formula weights.

Overall SII (5 v1 categories):
  peg_stability:              0.30
  liquidity_depth:            0.25
  mint_burn_dynamics:         0.15
  holder_distribution:        0.10
  structural_risk_composite:  0.20

Structural Subcategory Weights (legacy, informational):
  reserves_collateral:   0.30
  smart_contract_risk:   0.20
  oracle_integrity:      0.15
  governance_operations: 0.20
  network_chain_risk:    0.15

Legacy-to-v1 Category Mapping:
  peg_stability       -> peg_stability
  liquidity           -> liquidity_depth
  market_activity     -> mint_burn_dynamics
  flows               -> mint_burn_dynamics
  holder_distribution -> holder_distribution
  smart_contract      -> structural_risk_composite
  governance          -> structural_risk_composite
  transparency        -> structural_risk_composite
  regulatory          -> structural_risk_composite
  network             -> structural_risk_composite
  reserves            -> structural_risk_composite
  oracle              -> structural_risk_composite

Aggregation: coverage_weighted with min_coverage=0.0
Formula version: v1.0.0
Score range: 0-100, grades A+ through F
"""


# =============================================================================
# Main
# =============================================================================

# =============================================================================
# 4. Confidence Tier Codes v1
# =============================================================================

CONFIDENCE_TIER_CODES_V1 = """Confidence Tier Codes v1
=========================

The grade field in /api/scores and /api/psi/scores carries a confidence tier
code, not a credit rating. This is not an NRSRO rating, a CRA rating, or any
assessment of creditworthiness. It is a two-character code representing the
methodological confidence level at which the underlying score was computed.

Mapping:

| confidence_tag | Code | Meaning                                    |
|----------------|------|--------------------------------------------|
| (null/high)    | HI   | High confidence — >=80% component coverage |
| STANDARD       | ST   | Standard confidence — >=60% coverage       |
| LIMITED DATA   | LD   | Limited data — <60% component coverage     |
| (unknown)      | XX   | Fallback for unrecognized tags             |

On-chain storage: the bytes2 grade slot on the BasisOracle contract is a legacy
schema field name. The semantic meaning as of this version is confidence tier
code. The keeper's gradeToBytes2() function packs two ASCII characters into
bytes2 — encoding-agnostic.

Examples:
  HI -> 0x4849 (H=0x48, I=0x49)
  ST -> 0x5354 (S=0x53, T=0x54)
  LD -> 0x4c44 (L=0x4c, D=0x44)
"""


METHODOLOGIES = [
    (
        "track_record_rules_v1",
        TRACK_RECORD_RULES_V1,
        "Track record auto-entry trigger rules (A-G) and dispatch table",
    ),
    (
        "track_record_outcome_rubric_v1",
        TRACK_RECORD_OUTCOME_RUBRIC_V1,
        "30/60/90-day outcome classification rubric for track record follow-ups",
    ),
    (
        "sii_component_weights_v1",
        SII_COMPONENT_WEIGHTS_V1,
        "SII v1.0.0 canonical category weights and structural subcategory weights",
    ),
    (
        "confidence_tier_codes_v1",
        CONFIDENCE_TIER_CODES_V1,
        "Mapping of SII confidence_tag values to bytes2 tier codes used in on-chain grade field.",
    ),
]


def main():
    registered = []
    skipped = []

    for methodology_id, content, description in METHODOLOGIES:
        try:
            content_hash = register_methodology(methodology_id, content, description)
            print(f"  registered: {methodology_id}  hash={content_hash[:16]}...")
            registered.append(methodology_id)
        except ValueError:
            print(f"  skipped (already exists): {methodology_id}")
            skipped.append(methodology_id)

    print(f"\nDone. Registered: {len(registered)}, Skipped: {len(skipped)}")


if __name__ == "__main__":
    main()
