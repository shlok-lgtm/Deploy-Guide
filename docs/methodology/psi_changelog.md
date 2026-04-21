# PSI Methodology Changelog

## v0.3.0 — 2026-04-21

**Aggregation migration:** `legacy_renormalize` → `coverage_weighted` with `min_coverage=0.60`.

- Declared via `PSI_V01_DEFINITION.aggregation` in `app/index_definitions/psi_v01.py`.
- Weights, categories, and components unchanged.
- Justification: `docs/methodology/aggregation_impact_analysis.md` (Section A + Section B — PSI). Protocols below 60% component coverage now have `overall_score=null` and `withheld=true`; above 0.60, categories contribute in proportion to their populated-weight fraction. 0.60 is the floor below which the Security and Governance categories have too few populated components to anchor a meaningful overall.
- PSI dispatches through the generic scoring engine (`score_entity` → `aggregate`), so the declaration takes effect on the next scoring cycle — no separate wiring PR required.

## v0.2.0 — 2026-04-03

Added governance_stability, collateral_coverage_ratio, market_listing_velocity to Security category. Security expanded from 3 to 6 components with rebalanced weights. Motivated by the Drift Protocol exploit analysis (April 1, 2026). 27 components total.

## v0.1.0 — 2026-03-15

Initial PSI release. 24 components across 6 categories scoring 13 DeFi protocols.
