# SII Methodology Changelog

## v1.1.0 — 2026-04-21

**Aggregation migration:** `legacy_renormalize` → `coverage_weighted` with `min_coverage=0.0`.

- Declared via `SII_V1_DEFINITION.aggregation` in `app/index_definitions/sii_v1.py`.
- Weights, categories, and components unchanged.
- Justification: `docs/methodology/aggregation_impact_analysis.md` (Section B — SII). Under coverage-weighted, categories contribute to the overall in proportion to their populated-weight fraction, so partially-populated categories are no longer silently over-weighted relative to fully-populated peers. Net shift for the USDC anchor is +~1 point (93.36 → ~94.60 per Section B of the report).
- **Production scoring path wiring deferred.** `app/scoring.py::calculate_sii` and `app/worker.py::compute_sii_from_components` do not yet dispatch through `app.composition.aggregate`; the TODO in `app/scoring.py::calculate_sii` tracks that follow-up. This release carries the declaration change only; stored SII scores will not shift until the wiring PR lands.

## v1.0.0 — 2025-12-28

Initial public release. Formula: `SII = 0.30·Peg + 0.25·Liquidity + 0.15·MintBurn + 0.10·Distribution + 0.20·Structural`.
