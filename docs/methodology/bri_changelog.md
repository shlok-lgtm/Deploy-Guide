# BRI Methodology Changelog

## v0.2.0 — 2026-04-21

**Promotion:** accruing → scored.

**Aggregation migration:** `legacy_renormalize` → `coverage_withheld` with `coverage_threshold=0.70`.

- Declared via `BRI_V01_DEFINITION.aggregation` in `app/index_definitions/bri_v01.py`.
- Weights, categories, and components unchanged.
- Justification: `docs/methodology/aggregation_impact_analysis.md` (Section A — BRI). The BRI coverage distribution is mature enough that 0.70 marks a meaningful quality gate without withholding the bulk of the bridge roster.
- Promotion effects: ops rankings page no longer shows the ACCRUING tag for BRI entities; `app/server.py::entity_page` response sets `accruing: false`; analyzer treats BRI as a scored index in `GENERIC_TABLE_INDEX_IDS`.
- BRI dispatches through the generic scoring engine, so both the formula change and the withholding behavior take effect on the next scoring cycle.

## v0.1.0 — (initial accruing release)

Initial BRI release. 29 components across 6 categories. Accruing.
