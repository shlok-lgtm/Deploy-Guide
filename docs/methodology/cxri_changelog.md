# CXRI Methodology Changelog

## v0.2.0 — 2026-04-21

**Promotion:** accruing → scored.

**Aggregation migration:** `legacy_renormalize` → `coverage_withheld` with `coverage_threshold=0.70`.

- Declared via `CXRI_V01_DEFINITION.aggregation` in `app/index_definitions/cxri_v01.py`.
- Weights, categories, and components unchanged.
- Justification: `docs/methodology/aggregation_impact_analysis.md` (Section A — CXRI). CXRI coverage distribution separates exchanges with meaningful PoR and on-chain signal coverage from those without at the 0.70 line; exchanges below the gate show `LIMITED DATA` rather than a synthesized overall.
- Promotion effects: ops rankings page no longer shows the ACCRUING tag for CXRI entities; `app/server.py::entity_page` response sets `accruing: false`; analyzer treats CXRI as a scored index in `GENERIC_TABLE_INDEX_IDS`.
- CXRI dispatches through the generic scoring engine, so both the formula change and the withholding behavior take effect on the next scoring cycle.

## v0.1.0 — (initial accruing release)

Initial CXRI release. 29 components across 6 categories. Accruing.
