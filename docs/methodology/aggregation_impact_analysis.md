> **TBD — populates on first production run.** Run `python scripts/analyze_aggregation_impact.py` against production Neon to complete. **No index migration PRs until this report is populated.**

# Aggregation Formula Impact Analysis

**Generated:** (pending first run)
**Source:** `scripts/analyze_aggregation_impact.py`
**Formulas evaluated:** `legacy_renormalize`, `coverage_weighted`, `coverage_withheld`, `strict_zero`, `strict_neutral`, `legacy_sii_v1`
**Withheld thresholds:** `0.60, 0.65, 0.70, 0.75, 0.80, 0.85`

This report compares every current score under every registered aggregation formula and candidate threshold. It is the decision artifact for each index's migration PR. No index should migrate without citing a specific row in this report.

## How to populate

```bash
# Required environment
export DATABASE_URL="postgresql://..."   # production Neon, or read-replica
export COINGECKO_API_KEY="..."           # only if any collector re-runs

# Run analyzer — read-only against scores, psi_scores, rpi_scores,
# generic_index_scores, protocol_treasury_holdings.
python scripts/analyze_aggregation_impact.py
```

The analyzer:
- Loads the most recent score record for every entity across all 9 indices.
- Recomputes overall scores under each `(formula, threshold)` combination using `app.composition.aggregate()`.
- Computes CQI shifts under the cross-product of input-formula choices.
- Computes RQS shifts at three candidate thresholds (0.50, 0.70, 0.85).
- Overwrites this file with real data.

No writes to scoring tables. No side effects beyond the markdown output.

---

## SQL queries the analyzer runs

Every numeric claim in this report, once populated, traces back to one of these queries. Operators reproducing the report can paste these directly into psql.

### SII (stablecoins)
```sql
SELECT st.symbol AS entity_slug, st.name AS entity_name,
       s.overall_score, s.component_count AS components_available,
       s.computed_at,
       s.reserve_score, s.smart_contract_score, s.oracle_score,
       s.governance_score, s.network_score
FROM scores s
JOIN stablecoins st ON st.id = s.stablecoin_id
ORDER BY s.computed_at DESC;
```

### PSI (protocols)
```sql
SELECT DISTINCT ON (protocol_slug)
       protocol_slug AS entity_slug, protocol_name AS entity_name,
       overall_score, component_scores, raw_values, computed_at,
       formula_version
FROM psi_scores
ORDER BY protocol_slug, computed_at DESC;
```

### RPI (protocols)
```sql
SELECT DISTINCT ON (protocol_slug)
       protocol_slug AS entity_slug, protocol_name AS entity_name,
       overall_score, component_scores, raw_values, computed_at,
       methodology_version AS formula_version
FROM rpi_scores
ORDER BY protocol_slug, computed_at DESC;
```

### Accruing indices (LSTI / BRI / DOHI / VSRI / CXRI / TTI)
```sql
SELECT DISTINCT ON (entity_slug)
       entity_slug, entity_name, overall_score,
       category_scores, component_scores, raw_values,
       formula_version, computed_at
FROM generic_index_scores
WHERE index_id = %s   -- parameterized for each of the 6 accruing index ids
ORDER BY entity_slug, computed_at DESC;
```

### RQS treasury holdings
```sql
SELECT DISTINCT protocol_slug FROM protocol_treasury_holdings;
-- then per protocol:
-- compute_rqs_for_protocol(slug, coverage_threshold=t)
```

---

## Section A — Per-index coverage distribution

**(TBD — populates from analyzer run.)**

Expected table shape:

| Index | n | min | 25th | median | 75th | max |
|---|---|---|---|---|---|---|
| sii  | — | — | — | — | — | — |
| psi  | — | — | — | — | — | — |
| rpi  | — | — | — | — | — | — |
| lsti | — | — | — | — | — | — |
| bri  | — | — | — | — | — | — |
| dohi | — | — | — | — | — | — |
| vsri | — | — | — | — | — | — |
| cxri | — | — | — | — | — | — |
| tti  | — | — | — | — | — | — |

This section sets Option D thresholds empirically. If an accruing index has all entities clustered at 0.68–0.74, threshold 0.70 means "half withheld." Threshold must be chosen with the distribution in hand.

---

## Section B — Per-index delta tables

**(TBD — one sub-table per index.)**

Per-entity table columns:

| entity | legacy | cw@0.0 | cwh@0.60 | cwh@0.65 | cwh@0.70 | cwh@0.75 | cwh@0.80 | cwh@0.85 |

Where:
- `legacy` = current engine output under `legacy_renormalize`
- `cw@0.0` = overall under `coverage_weighted` (no threshold gate)
- `cwh@T` = overall under `coverage_withheld` at threshold T; cell reads "withheld" when coverage falls below T

Cells where `|delta| ≥ 3` get a subtle highlight; `≥ 5` stronger; `≥ 10` strongest. (Highlight styling pending; the analyzer emits plain values for v1.)

---

## Section C — CQI matrix shift

**(TBD — one row per stablecoin × protocol pair.)**

| asset | protocol | legacy CQI | shift under PSI migrations |

The "shift" column summarizes how CQI changes when PSI moves to `coverage_weighted` or `coverage_withheld` at each threshold. SII-side rescoring is deferred to the SII-specific follow-up analyzer.

**Kelp context note.** Once LSTI is folded into a broader collateral-quality composite (separate methodology work), CQI(rsETH × Aave) will become computable. Under `coverage_withheld` thresholds at 0.75+, rsETH would have withheld on April 20, making the pair undefined — the right behavior given the coverage gap the incident exposed.

---

## Section D — RQS portfolio impact

**(TBD — one row per protocol with treasury data.)**

| protocol | scored_coverage | baseline_rqs | t=0.50 | t=0.70 | t=0.85 |

`baseline_rqs` uses the current threshold=0.0 path. Threshold columns show RQS under each candidate minimum scored_coverage; "withheld" appears when a protocol's portfolio coverage falls below the column's threshold.

---

## Section E — Per-index migration recommendation

**(TBD — one paragraph per index.)**

Each paragraph, once written from real data, proposes:

- **Target formula** (`coverage_weighted` for scored indices, `coverage_withheld` for accruing indices with a threshold justified by Section A).
- **Threshold** — derived from the coverage distribution, not a prior.
- **Expected movement** — median and worst-case deltas from Section B.
- **Entities likely to be withheld** — explicit list from Section B's `cwh@T` columns.
- **Communication posture** — minimal shift / material shift / category-level shift. Drives the public-communication text in each migration PR.

### Migration order (proposed, subject to analysis)

1. **SII** — once the SII-specific analyzer lands. Expected minimal movement; proves the pattern.
2. **PSI** — shifts CQI matrix.
3. **RPI** — last scored; CQI matrix fully resettled.
4. **Accruing**: LSTI → BRI → TTI → CXRI → VSRI → DOHI (coverage-maturity order; actual order set by Section A).
5. **RQS default threshold** — after RQS impact (Section D) is populated.

---

## Hand-worked case studies

### USDC under SII
_(The minimal-movement reference case. Populate category-by-category from the analyzer run.)_

### rsETH under LSTI
_(The audit's reference case — see `audits/internal/lsti_rseth_audit_2026-04-20.md` Q1. Expected behavior: with `coverage_withheld` at threshold ≥ 0.75 and rsETH's ~0.72 component coverage, rsETH's LSTI overall withholds. The 8 components contributing to the coverage gap are enumerated in the audit; each walkthrough in this case study traces one component to its data source.)_

### Aave V3 under PSI
_(Live in Kelp context. Walk through Aave's PSI components, identify coverage gap if any, and show how CQI(rsETH × Aave) behaves when PSI migrates to `coverage_weighted`. Note that the pair itself becomes undefined only if LSTI folds into the collateral-composite methodology — see the CQI composition note at `docs/methodology/cqi_composition_note.md`.)_

---

## Reference

- Audit documenting the defect: `audits/internal/lsti_rseth_audit_2026-04-20.md` (Q1 + Q4 Blockers)
- Validation script confirming defect on HEAD: `scripts/validate_renormalization.py`
- Registry source: `app/composition.py` — `AGGREGATION_FORMULAS`, `aggregate()`
- Schema doc: `app/index_definitions/schema.py`
- CQI composition note: `docs/methodology/cqi_composition_note.md`
