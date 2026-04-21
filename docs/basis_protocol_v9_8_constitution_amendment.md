# Constitution Amendment v9.8 — V7.3 Confidence Tag System, Uniform Emission

**Date:** 2026-04-21
**Status:** Approved
**Supersedes:** (none — clarifies V7.3 Addendum 2 by extending it across all scored indices)

## Context

The V7.3 Confidence Tag System (Addendum 2) defines six visibility fields
attached to every score:

- `confidence` — `high` / `standard` / `limited`
- `confidence_tag` — `null` / `"STANDARD"` / `"LIMITED DATA"`
- `component_coverage` — populated / total, as a ratio
- `components_populated` — count of components with a non-null normalized score
- `components_total` — count of components defined by the index
- `missing_categories` — categories with zero populated components

The thresholds are fixed by the addendum: `coverage >= 0.80` is high;
`>= 0.60` is standard; `< 0.60` is limited.

Prior to this amendment, only **SII** and **PSI** surfaced these fields, and
they did so by synthesis at the API layer rather than by persistence. **RPI**
and the six **Circle 7** indices (LSTI, BRI, DOHI, VSRI, CXRI, TTI) did not
emit the fields at all. On the ops rankings page, the Coverage column rendered
`—` for seven of the nine scored indices.

## Decision

The V7.3 confidence tag fields MUST be emitted uniformly by every scored index.
The canonical computation lives in `app/scoring_engine.py::score_entity()` and
`compute_confidence_tag()`. Every scorer either uses `score_entity()` directly
(PSI, Circle 7) or computes the same fields through the same helper (SII,
RPI) and persists them on write.

### Scope (nine indices)

| Index | Table | Scorer |
|---|---|---|
| SII | `scores` | `app/worker.py::compute_sii_from_components` + `store_score` |
| PSI | `psi_scores` | `app/collectors/psi_collector.py::score_protocol` |
| RPI | `rpi_scores` | `app/rpi/scorer.py::score_rpi_base` + `store_rpi_score` |
| LSTI | `generic_index_scores` (index_id='lsti') | `app/collectors/lst_collector.py` |
| BRI | `generic_index_scores` (index_id='bri') | `app/collectors/bridge_collector.py` |
| DOHI | `generic_index_scores` (index_id='dohi') | `app/collectors/dao_collector.py` |
| VSRI | `generic_index_scores` (index_id='vsri') | `app/collectors/vault_collector.py` |
| CXRI | `generic_index_scores` (index_id='cxri') | `app/collectors/cex_collector.py` |
| TTI | `generic_index_scores` (index_id='tti') | `app/collectors/tti_collector.py` |

Circle 7 indices share one table `generic_index_scores` (keyed by
`index_id`) — this is not nine tables. It is four: `scores`, `psi_scores`,
`rpi_scores`, `generic_index_scores`.

### Constraints preserved

- No change to any index formula, category weight, or promotion gate.
- No change to `is_category_complete()` or any category-completeness logic.
- Circle 7 indices **remain accruing**. This amendment writes visibility
  columns onto rows that are being persisted already; it does not promote
  any index from accruing to scored.
- Numbers shown are derived from actual stored components, not synthesized
  from proxies. When an entity has zero populated components, the ops page
  renders `0/N` with a `LIMITED DATA` tag — never `—`.
- RPI confidence fields reflect **base components only**. Lens variants are
  ephemeral and remain out of scope for persistence.

### Canonical field names vs. legacy aliases

`score_entity()` previously returned `coverage` and `components_available`.
Those names are retained for backwards compatibility with existing callers.
The V7.3 canonical names `component_coverage` and `components_populated` are
added as aliases on the same return object. The duplicated names will be
collapsed in a later migration; open ticket tag:
`basis-hub#confidence-rename`.

## Corrections

Post-deploy, SII and PSI Coverage values on the ops rankings page will
change. The prior synthesis used `component_count` (total readings
collected, including those with null normalized scores) as the numerator
against `len(COMPONENT_NORMALIZATIONS)` as the denominator. The corrected
V7.3 ratio uses **populated components present in the index definition**
as the numerator. This is a correction, not a regression: the prior
values over-counted coverage by including collected-but-unnormalized
readings.

## Migration

`migrations/085_confidence_tag_universal.sql` — additive, idempotent. Adds
the six visibility columns to every score table (four `confidence_tag`
columns were already present on `generic_index_scores` from
`052_circle7_collectors.sql`).

## Backfill

`scripts/backfill_confidence_tags.py` — one-shot. For each row on each of
the four score tables, recomputes the V7.3 fields from already-stored
`component_scores` JSON (or `component_readings` for SII) and writes them
in place. Does not re-run scoring. Idempotent; safe to re-run.

## Kill signal

If a future index is added that does not pass through `score_entity()` or
its custom-scorer equivalent, the ops Coverage column will regress to `—`
for that index. Every new scorer MUST emit the six fields and every new
score table MUST carry the six columns.
