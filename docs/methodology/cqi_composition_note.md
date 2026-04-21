# CQI composition and the aggregation-formula registry

**Date:** 2026-04-21
**Relates to:** aggregation-infrastructure PR (composition.py registry extension)
**Code:** `app/composition.py` — `compute_cqi()` at line 63, `compute_cqi_matrix()` at line 358

## What changed in the infra PR

The aggregation-formula registry was added to `app/composition.py`, extending the pattern already established by the composition formulas (`compose_geometric_mean`, `compose_weighted_average`, `compose_minimum`) to within-index aggregation. Every index's `score_entity()` call now dispatches through a named, versioned formula. Default is `legacy_renormalize`, which preserves pre-registry output byte-for-byte.

CQI's composition formula was **not** changed. CQI continues to be a geometric mean of SII × PSI, stamped with `"method": "geometric_mean"` and `"formula_version": "composition-v1.0.0"`.

## CQI inherits input null-safety correctly — no code change required

`compute_cqi()` already performs the null-safe checks at the inputs:

- `app/composition.py:76-77` — `if not sii_row or sii_row.get("overall_score") is None: return error`
- `app/composition.py:88-89` — `if not psi_row or psi_row.get("overall_score") is None: return error`

When a future SII or PSI migration adopts `coverage_withheld` and an entity falls below its threshold, the underlying score record will have `overall_score = NULL`. CQI's existing guards correctly return a structured error for that pair rather than computing a geometric mean with a missing input. Same behavior for `compute_cqi_matrix()`, which reads both rows and only composes when both `overall_score` values are present.

This is the right behavior. A withheld input to a composite means the composite itself is undefined; silently substituting a neutral value, or extrapolating, would reintroduce exactly the kind of false-complete number the aggregation-registry work is designed to eliminate.

## CQI's formula version does not bump when SII/PSI migrate

CQI's methodology — "geometric mean of SII × PSI" — is unchanged by an input migration. What changes is the input values themselves. So `"formula_version": "composition-v1.0.0"` stays put.

However: when SII or PSI flips from `legacy_renormalize` to `coverage_weighted` (or `coverage_withheld`), **every composed CQI pair in the matrix will shift** — because the inputs shift. This is the intended effect of the aggregation migration (more honest inputs → more honest composite), but it is a material, visible change that must be part of the communication plan for each staged migration.

Expected magnitudes (actual numbers to be populated from `docs/methodology/aggregation_impact_analysis.md` once that analyzer runs against production):

- **SII migration first:** CQI shifts for every pair, proportional to SII's per-stablecoin coverage. Stablecoins with near-complete SII coverage shift least.
- **PSI migration next:** additional shift, proportional to PSI's per-protocol coverage.
- **After both migrations:** CQI matrix has resettled at its honest-input equilibrium. No further shifts until one of the inputs adopts a different formula or raises its threshold.

## The Kelp context

The Kelp DAO / rsETH / LayerZero incident this week propagated through composability into Aave's bad debt via rsETH collateral. CQI(rsETH × Aave) — if it existed — would be the single number that measures this pairing. rsETH is in LSTI (accruing), not SII, so CQI in its current definition does not cover LSTI-backed collateral. That's a known methodology gap, separate from the aggregation-registry work.

What the aggregation registry enables when LSTI is eventually folded into a broader collateral-quality composite: an LSTI migration to `coverage_withheld` with a threshold around 0.70 would force rsETH's effective contribution to any composite to either land at an honest coverage-weighted number or be withheld entirely. No more "70% of the rsETH signal is missing but the composite looks clean."

## Attestation

CQI compositions attest via `attest_state("cqi_compositions", ...)` in `compute_cqi_matrix()` at line 401-406. The attestation payload is `{asset, protocol, cqi_score}`. When an input is NULL and CQI returns an error, that pair is absent from the attested list — historically this was also the behavior (no score, no attestation row). No attestation-chain change required.

## Summary — what the infra PR does NOT change in CQI

- `compute_cqi` code: unchanged
- `compute_cqi_matrix` code: unchanged
- Composition formula version: stays at `composition-v1.0.0`
- Attestation payload shape: unchanged
- Null-safety behavior: already correct, preserved verbatim

## What future migration PRs will need to coordinate

When SII or PSI stages a formula migration:

1. Name the expected CQI matrix shift in the migration PR description, referencing the numeric summary from `aggregation_impact_analysis.md`.
2. Re-attest the CQI matrix after the migration deploys so the on-chain state-root covers the shifted values under the new formula version of the underlying inputs.
3. Note whether any specific CQI pairs newly withhold (CQI returns error instead of a number) because one of the inputs fell below its new threshold.

Those coordination points are bookkeeping for each staged migration, not blockers. The registry design means no CQI code changes across the whole migration arc.
