# VSRI Methodology Changelog

## v0.2.0 — 2026-04-21

**Aggregation migration:** `legacy_renormalize` → `coverage_withheld` with `coverage_threshold=0.60`.

- Declared via `VSRI_V01_DEFINITION.aggregation` in `app/index_definitions/vsri_v01.py`.
- Weights, categories, and components unchanged.
- Status: VSRI **remains accruing**. Promotion is a separate methodology step tracked independently from this aggregation migration.
- Justification: `docs/methodology/aggregation_impact_analysis.md` (Section A — VSRI). 0.60 is the floor below which strategy-transparency and underlying-asset categories have too few populated components to anchor a meaningful overall; above it, coverage-weighted overall is reliable.
- VSRI dispatches through the generic scoring engine, so both the formula change and the withholding behavior take effect on the next scoring cycle.

## v0.1.0 — (initial accruing release)

Initial VSRI release. 28 components across 6 categories. Accruing.
