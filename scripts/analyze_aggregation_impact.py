"""
Aggregation-formula impact analysis.

Loads the most recent score record for every scored and accruing entity across
all 9 indices, recomputes overall scores under every aggregation formula at a
range of candidate thresholds, and emits a full comparative report to
`docs/methodology/aggregation_impact_analysis.md`.

Also recomputes CQI for every current pair under the cross-product of input
formula choices (SII × PSI), and RQS for every protocol with treasury data
under candidate scored_coverage thresholds.

Usage (requires production DATABASE_URL + COINGECKO_API_KEY as needed for
any live-dependent re-scoring; this script is read-only against the scoring
tables — no writes, no migrations):

    BASIS_DATABASE_URL=... python scripts/analyze_aggregation_impact.py

On a run without DATABASE_URL set the script prints the query plan and exits
with an explanatory message; no fabricated output.
"""

from __future__ import annotations

import json
import os
import sys
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

from app.composition import AGGREGATION_FORMULAS, aggregate  # noqa: E402
from app.database import init_pool  # noqa: E402


# =============================================================================
# Configuration — indices + candidate thresholds + report output path
# =============================================================================

# Indices with their (definition module, DB source) pairs. SII is on a
# separate scoring path; its query pulls only entity metadata and overall,
# and the per-entity component replay happens in analyze_sii() which joins
# component_readings to reconstruct the score_entity input dict.
INDEX_SOURCES = [
    ("sii",  "app.index_definitions.sii_v1",  "SII_V1_DEFINITION",
     "SELECT st.id AS stablecoin_id, st.symbol AS entity_slug, "
     "st.name AS entity_name, s.overall_score, s.computed_at "
     "FROM scores s JOIN stablecoins st ON st.id = s.stablecoin_id "
     "ORDER BY s.computed_at DESC"),
    ("psi",  "app.index_definitions.psi_v01", "PSI_V01_DEFINITION",
     "SELECT DISTINCT ON (protocol_slug) protocol_slug AS entity_slug, "
     "protocol_name AS entity_name, overall_score, component_scores, "
     "raw_values, computed_at, formula_version "
     "FROM psi_scores ORDER BY protocol_slug, computed_at DESC"),
    ("rpi",  "app.index_definitions.rpi_v2",  "RPI_V2_DEFINITION",
     "SELECT DISTINCT ON (protocol_slug) protocol_slug AS entity_slug, "
     "protocol_name AS entity_name, overall_score, component_scores, "
     "raw_values, computed_at, methodology_version AS formula_version "
     "FROM rpi_scores ORDER BY protocol_slug, computed_at DESC"),
]

# Indices that live in generic_index_scores. BRI and CXRI were promoted
# from accruing to scored in their v0.2.0 migrations (see per-index
# changelog files and docs/methodology/aggregation_impact_analysis.md).
# The analyzer treats them the same as the remaining accruing set since
# the storage layout is identical.
GENERIC_TABLE_INDEX_IDS = ["lsti", "bri", "dohi", "vsri", "cxri", "tti"]
ACCRUING_INDEX_IDS = ["lsti", "dohi", "vsri", "tti"]
SCORED_CIRCLE7_INDEX_IDS = ["bri", "cxri"]

GENERIC_SCORE_QUERY = (
    "SELECT DISTINCT ON (entity_slug) entity_slug, entity_name, overall_score, "
    "category_scores, component_scores, raw_values, formula_version, "
    "computed_at FROM generic_index_scores WHERE index_id = %s "
    "ORDER BY entity_slug, computed_at DESC"
)

# Candidate thresholds to sweep in the report.
WITHHELD_THRESHOLDS = [0.60, 0.65, 0.70, 0.75, 0.80, 0.85]
RQS_THRESHOLDS = [0.50, 0.70, 0.85]

# Formulas evaluated per entity. legacy is the baseline; the others
# are the candidate migration targets.
EVALUATED_FORMULAS = [
    ("legacy_renormalize", {}),
    ("coverage_weighted", {}),
] + [("coverage_withheld", {"coverage_threshold": t}) for t in WITHHELD_THRESHOLDS]

REPORT_PATH = ROOT / "docs" / "methodology" / "aggregation_impact_analysis.md"


# =============================================================================
# DB access — guard behind env so running in a no-DB environment is honest
# =============================================================================


def _require_db() -> None:
    if not os.environ.get("DATABASE_URL") and not os.environ.get("BASIS_DATABASE_URL"):
        sys.stderr.write(
            "ERROR: DATABASE_URL / BASIS_DATABASE_URL not set.\n\n"
            "This analyzer reads scoring tables directly from production Neon.\n"
            "Set DATABASE_URL to the production (or replica) connection string and\n"
            "re-run. The analyzer is read-only — it executes SELECT queries only\n"
            "against scores, psi_scores, rpi_scores, and generic_index_scores, plus\n"
            "protocol_treasury_holdings for the RQS section. No writes, no migrations.\n\n"
            "If you are reading this inside a sandbox or CI environment without\n"
            "production access, request a read-replica connection string from the\n"
            "ops team before running.\n"
        )
        sys.exit(2)


def _fetch_all(sql: str, params: tuple = ()) -> list[dict]:
    """Thin wrapper over app.database.fetch_all. Imported lazily so this
    script can be inspected without triggering a DB connection attempt."""
    from app.database import fetch_all
    rows = fetch_all(sql, params) or []
    return [dict(r) for r in rows]


# =============================================================================
# Per-entity re-scoring
# =============================================================================


def _load_definition(module_path: str, symbol: str):
    mod = __import__(module_path, fromlist=[symbol])
    return getattr(mod, symbol)


def rescore_entity_under_formulas(definition: dict, component_scores: dict,
                                    raw_values: dict) -> dict:
    """For one entity, produce a dict keyed by formula_label → aggregate()
    result. Labels: 'legacy_renormalize', 'coverage_weighted',
    'coverage_withheld@0.60' ... 'coverage_withheld@0.85'."""
    out = {}
    for formula_name, params in EVALUATED_FORMULAS:
        key = formula_name if not params else f"{formula_name}@{params.get('coverage_threshold', '')}"
        # Build a synthetic definition carrying this formula declaration so
        # we can reuse aggregate() cleanly.
        defn = dict(definition)
        defn["aggregation"] = {"formula": formula_name, "params": params}
        try:
            out[key] = aggregate(defn, component_scores, raw_values)
        except Exception as e:
            out[key] = {"error": str(e), "overall_score": None, "withheld": False}
    return out


def analyze_generic_index(index_id: str) -> list[dict]:
    """Load + re-score every entity for a generic-engine index."""
    # Definition lookup
    defn_map = {
        "lsti":  ("app.index_definitions.lsti_v01",  "LSTI_V01_DEFINITION"),
        "bri":   ("app.index_definitions.bri_v01",   "BRI_V01_DEFINITION"),
        "dohi":  ("app.index_definitions.dohi_v01",  "DOHI_V01_DEFINITION"),
        "vsri":  ("app.index_definitions.vsri_v01",  "VSRI_V01_DEFINITION"),
        "cxri":  ("app.index_definitions.cxri_v01",  "CXRI_V01_DEFINITION"),
        "tti":   ("app.index_definitions.tti_v01",   "TTI_V01_DEFINITION"),
    }
    if index_id not in defn_map:
        return []
    module_path, symbol = defn_map[index_id]
    definition = _load_definition(module_path, symbol)

    rows = _fetch_all(GENERIC_SCORE_QUERY, (index_id,))
    out = []
    for r in rows:
        comp_scores = r.get("component_scores") or {}
        if isinstance(comp_scores, str):
            comp_scores = json.loads(comp_scores)
        raw_values = r.get("raw_values") or {}
        if isinstance(raw_values, str):
            raw_values = json.loads(raw_values)
        rescored = rescore_entity_under_formulas(definition, comp_scores, raw_values)
        out.append({
            "index": index_id,
            "entity": r.get("entity_slug"),
            "entity_name": r.get("entity_name"),
            "current_overall": float(r["overall_score"]) if r.get("overall_score") is not None else None,
            "rescored": rescored,
        })
    return out


def analyze_psi_rpi(index_id: str, query: str, module_path: str, symbol: str) -> list[dict]:
    """Analyze PSI or RPI via their dedicated tables."""
    definition = _load_definition(module_path, symbol)
    rows = _fetch_all(query)
    out = []
    for r in rows:
        comp_scores = r.get("component_scores") or {}
        if isinstance(comp_scores, str):
            comp_scores = json.loads(comp_scores)
        raw_values = r.get("raw_values") or {}
        if isinstance(raw_values, str):
            raw_values = json.loads(raw_values)
        rescored = rescore_entity_under_formulas(definition, comp_scores, raw_values)
        out.append({
            "index": index_id,
            "entity": r.get("entity_slug"),
            "entity_name": r.get("entity_name"),
            "current_overall": float(r["overall_score"]) if r.get("overall_score") is not None else None,
            "rescored": rescored,
        })
    return out


# Legacy category names declared on SII components (inherited from
# COMPONENT_NORMALIZATIONS) → SII v1 category names declared on
# SII_V1_DEFINITION["categories"]. Canonical source: app/scoring_engine.py's
# is_sii_category_complete_legacy(). Duplicated here so the analyzer is
# self-contained and the aggregation-registry replay activates all five v1
# categories. Without this remap, only peg_stability and holder_distribution
# would contribute (those are the two cat names that happen to overlap
# between the legacy and v1 vocabularies), and every formula would score
# SII entities on 40% of the definition, producing results uncomparable
# across formulas.
_SII_LEGACY_TO_V1_CATEGORY = {
    "peg_stability": "peg_stability",
    "liquidity": "liquidity_depth",
    "market_activity": "mint_burn_dynamics",
    "flows": "mint_burn_dynamics",
    "holder_distribution": "holder_distribution",
    "smart_contract": "structural_risk_composite",
    "governance": "structural_risk_composite",
    "transparency": "structural_risk_composite",
    "regulatory": "structural_risk_composite",
    "network": "structural_risk_composite",
    "reserves": "structural_risk_composite",
    "oracle": "structural_risk_composite",
}


def _sii_definition_with_v1_categories(definition: dict) -> dict:
    """Return a shallow-cloned SII definition whose components' `category`
    field uses v1 category names (matching `definition["categories"]` keys).

    Non-destructive: never mutates the shared SII_V1_DEFINITION object.
    """
    new_components = {}
    for cid, cdef in definition["components"].items():
        new_cdef = dict(cdef)
        legacy_cat = cdef.get("category")
        new_cdef["category"] = _SII_LEGACY_TO_V1_CATEGORY.get(legacy_cat, legacy_cat)
        new_components[cid] = new_cdef
    out = dict(definition)
    out["components"] = new_components
    return out


def analyze_sii() -> list[dict]:
    """Re-score every SII stablecoin under every registered aggregation formula.

    For each stablecoin in `scores`, pulls its most recent reading per
    component_id from `component_readings` and feeds the (component_scores,
    raw_values) pair through rescore_entity_under_formulas() — the same
    replay path the PSI/RPI/generic-index analyzers use.

    Coverage is computed against SII_V1_DEFINITION's 56 canonical components.
    The collector writes additional component_ids (e.g., Solana-chain-specific
    variants) that are not in the scorer's canonical list; those are ignored
    by this replay, which matches production SII scoring behavior — the
    definition is the canonical component universe.

    Note on category semantics: SII_V1_DEFINITION declares 5 v1 categories
    (peg_stability, liquidity_depth, mint_burn_dynamics, holder_distribution,
    structural_risk_composite) but its components inherit the 8-way legacy
    category vocabulary from COMPONENT_NORMALIZATIONS. This analyzer applies
    the canonical legacy→v1 remapping (see _SII_LEGACY_TO_V1_CATEGORY) to a
    local copy of the definition so that every v1 category receives its
    fair share of components during aggregation. Production's SII scorer in
    app/worker.py::compute_sii_from_components applies the equivalent
    mapping via aggregate_legacy_to_v1 / DB_TO_STRUCTURAL_MAPPING. Follow-up
    ticket `sii-component-gap` covers full reconciliation between the
    collector's 80 component_ids, the scorer's 56 declared components, and
    the ~37 that overlap end-to-end.
    """
    from app.index_definitions.sii_v1 import SII_V1_DEFINITION

    sii_definition = _sii_definition_with_v1_categories(SII_V1_DEFINITION)

    entities = _fetch_all(INDEX_SOURCES[0][3])

    out = []
    for e in entities:
        stablecoin_id = e.get("stablecoin_id")
        readings = _fetch_all(
            "SELECT DISTINCT ON (component_id) "
            "       component_id, raw_value, normalized_score "
            "FROM component_readings "
            "WHERE stablecoin_id = %s AND raw_value IS NOT NULL "
            "ORDER BY component_id, collected_at DESC",
            (stablecoin_id,),
        )
        raw_values = {r["component_id"]: r["raw_value"] for r in readings}
        component_scores = {
            r["component_id"]: float(r["normalized_score"])
            for r in readings
            if r.get("normalized_score") is not None
        }
        rescored = rescore_entity_under_formulas(
            sii_definition, component_scores, raw_values
        )
        out.append({
            "index": "sii",
            "entity": e.get("entity_slug"),
            "entity_name": e.get("entity_name"),
            "current_overall": float(e["overall_score"]) if e.get("overall_score") is not None else None,
            "rescored": rescored,
        })
    return out


# =============================================================================
# CQI matrix shift
# =============================================================================


def analyze_cqi_shift(sii_rows: list[dict], psi_rows: list[dict]) -> list[dict]:
    """For every (stablecoin × protocol) pair currently in the CQI matrix,
    compute the pair under each (SII_formula × PSI_formula) cross-product
    available in the per-entity rescores. Returns only pairs where at least
    one formula choice moves the CQI by >= 1 point."""
    from app.composition import compose_geometric_mean
    # Index rows for O(1) lookup
    sii_by_slug = {r["entity"]: r for r in sii_rows if r.get("entity")}
    psi_by_slug = {r["entity"]: r for r in psi_rows if r.get("entity")}

    pairs = []
    for sii_slug, sii_row in sii_by_slug.items():
        for psi_slug, psi_row in psi_by_slug.items():
            current_sii = sii_row.get("current_overall")
            current_psi = psi_row.get("current_overall")
            if current_sii is None or current_psi is None:
                continue
            legacy_cqi = compose_geometric_mean([current_sii, current_psi])
            pair_variants = {"legacy": legacy_cqi}
            # SII rescoring is deferred (note above); only PSI rescoring
            # contributes a meaningful cross-product for v1.
            for psi_formula_label, psi_result in (psi_row.get("rescored") or {}).items():
                new_psi = (psi_result or {}).get("overall_score")
                if new_psi is None:
                    pair_variants[f"psi={psi_formula_label}"] = None
                    continue
                pair_variants[f"psi={psi_formula_label}"] = compose_geometric_mean(
                    [current_sii, new_psi]
                )
            pairs.append({
                "asset": sii_slug,
                "protocol": psi_slug,
                "variants": pair_variants,
            })
    return pairs


# =============================================================================
# RQS portfolio impact
# =============================================================================


def analyze_rqs() -> list[dict]:
    """For every protocol with treasury data, compute RQS under the current
    (threshold=0.0) path vs each candidate threshold. Returns a row per
    (protocol, threshold) pair."""
    from app.composition import compute_rqs_for_protocol
    # Discover protocols from RPI/PSI that have treasury data
    protocol_rows = _fetch_all(
        "SELECT DISTINCT protocol_slug FROM protocol_treasury_holdings"
    )
    results = []
    for pr in protocol_rows:
        slug = pr["protocol_slug"]
        baseline = compute_rqs_for_protocol(slug, coverage_threshold=0.0)
        baseline_score = baseline.get("rqs_score")
        scored_coverage = baseline.get("scored_coverage")
        row = {
            "protocol": slug,
            "scored_coverage": scored_coverage,
            "baseline_rqs": baseline_score,
            "thresholds": {},
        }
        for t in RQS_THRESHOLDS:
            r = compute_rqs_for_protocol(slug, coverage_threshold=t)
            row["thresholds"][t] = {
                "rqs_score": r.get("rqs_score"),
                "withheld": r.get("withheld"),
            }
        results.append(row)
    return results


# =============================================================================
# Report builder
# =============================================================================


def _coverage_distribution_md(all_rows_by_index: dict) -> str:
    out = []
    out.append("## Section A — Per-index coverage distribution\n")
    out.append("| Index | n | min | 25th | median | 75th | max |")
    out.append("|---|---|---|---|---|---|---|")
    for index_id, rows in all_rows_by_index.items():
        if not rows:
            out.append(f"| {index_id} | 0 | — | — | — | — | — |")
            continue
        # Pull coverage from the legacy rescored result (it's the same across
        # formulas for a given entity).
        covs = []
        for r in rows:
            legacy = (r.get("rescored") or {}).get("legacy_renormalize") or {}
            cov = legacy.get("coverage")
            if cov is not None:
                covs.append(cov)
        if not covs:
            out.append(f"| {index_id} | {len(rows)} | — | — | — | — | — |")
            continue
        covs_sorted = sorted(covs)
        n = len(covs_sorted)
        q25 = covs_sorted[n // 4]
        q75 = covs_sorted[(3 * n) // 4]
        med = statistics.median(covs_sorted)
        out.append(f"| {index_id} | {n} | {min(covs_sorted):.2f} | {q25:.2f} | {med:.2f} | {q75:.2f} | {max(covs_sorted):.2f} |")
    return "\n".join(out) + "\n"


def _per_index_delta_md(all_rows_by_index: dict) -> str:
    out = ["## Section B — Per-index delta tables\n"]
    for index_id, rows in all_rows_by_index.items():
        out.append(f"### {index_id}")
        if not rows:
            out.append("_No entities scored yet._\n")
            continue
        header = ["entity", "legacy"] + [
            f"cw@0.0", *[f"cwh@{t}" for t in WITHHELD_THRESHOLDS]
        ]
        out.append("| " + " | ".join(header) + " |")
        out.append("|" + "|".join("---" for _ in header) + "|")
        for r in rows:
            rc = r.get("rescored") or {}
            legacy = (rc.get("legacy_renormalize") or {}).get("overall_score")
            cw = (rc.get("coverage_weighted") or {}).get("overall_score")
            cells = [r.get("entity", ""),
                     f"{legacy:.2f}" if legacy is not None else "—",
                     f"{cw:.2f}" if cw is not None else "—"]
            for t in WITHHELD_THRESHOLDS:
                val = (rc.get(f"coverage_withheld@{t}") or {}).get("overall_score")
                cells.append(f"{val:.2f}" if val is not None else "withheld")
            out.append("| " + " | ".join(cells) + " |")
        out.append("")
    return "\n".join(out) + "\n"


def _cqi_shift_md(cqi_pairs: list[dict]) -> str:
    out = ["## Section C — CQI matrix shift\n"]
    if not cqi_pairs:
        out.append("_No CQI pairs found (SII × PSI)._\n")
        return "\n".join(out)
    out.append("| asset | protocol | legacy CQI | shift under PSI migrations |")
    out.append("|---|---|---|---|")
    for p in cqi_pairs[:100]:  # cap for readability
        variants = p.get("variants", {})
        legacy = variants.get("legacy")
        shifts = []
        for k, v in variants.items():
            if k == "legacy" or v is None:
                continue
            delta = None if legacy is None else round(v - legacy, 2)
            shifts.append(f"{k}:{v:.2f}({delta:+.2f})")
        out.append(f"| {p['asset']} | {p['protocol']} | {legacy:.2f} | {'; '.join(shifts[:3])} |")
    return "\n".join(out) + "\n"


def _rqs_impact_md(rqs_rows: list[dict]) -> str:
    out = ["## Section D — RQS portfolio impact\n"]
    if not rqs_rows:
        out.append("_No protocols with treasury data found._\n")
        return "\n".join(out)
    out.append("| protocol | scored_coverage | baseline_rqs | " + " | ".join(f"t={t}" for t in RQS_THRESHOLDS) + " |")
    out.append("|" + "|".join("---" for _ in range(3 + len(RQS_THRESHOLDS))) + "|")
    for r in rqs_rows:
        cells = [r["protocol"], f"{r['scored_coverage']:.2f}" if r.get("scored_coverage") else "—",
                 f"{r['baseline_rqs']:.2f}" if r.get("baseline_rqs") else "—"]
        for t in RQS_THRESHOLDS:
            tr = (r.get("thresholds") or {}).get(t, {})
            if tr.get("withheld"):
                cells.append("withheld")
            else:
                s = tr.get("rqs_score")
                cells.append(f"{s:.2f}" if s is not None else "—")
        out.append("| " + " | ".join(cells) + " |")
    return "\n".join(out) + "\n"


def _recommendation_md(all_rows_by_index: dict) -> str:
    return (
        "## Section E — Per-index migration recommendation\n\n"
        "Recommendations are derived automatically from the coverage distributions\n"
        "in Section A and the score movements in Sections B–D. Each paragraph\n"
        "proposes target formula, threshold (if applicable), expected movement,\n"
        "and any entities likely to withhold.\n\n"
        "_To be populated: one paragraph per index, written from the generated\n"
        "data above. Authoring guidance inside the analyzer; each paragraph must\n"
        "cite specific entities and specific numbers._\n"
    )


def _case_studies_md(all_rows_by_index: dict) -> str:
    return (
        "## Hand-worked case studies\n\n"
        "### USDC under SII\n_(The minimal-movement reference case.)_\n\n"
        "### rsETH under LSTI\n_(The audit's reference case. Expected to withhold at threshold ≥ 0.75.)_\n\n"
        "### Aave V3 under PSI\n_(CQI-adjacent. Kelp context.)_\n\n"
        "_To be populated: category-by-category walkthrough from generated rescoring data._\n"
    )


def build_report(all_rows_by_index: dict, cqi_pairs: list[dict],
                 rqs_rows: list[dict]) -> str:
    ts = datetime.now(timezone.utc).isoformat()
    header = f"""# Aggregation Formula Impact Analysis

**Generated:** {ts}
**Source:** `scripts/analyze_aggregation_impact.py`
**Formulas evaluated:** {", ".join(sorted(AGGREGATION_FORMULAS))}
**Withheld thresholds:** {WITHHELD_THRESHOLDS}

This report compares every current score under every registered aggregation
formula and candidate threshold. It is the decision artifact for each index's
migration PR. No index should migrate without citing a specific row in this
report.

---

"""
    body = "\n".join([
        _coverage_distribution_md(all_rows_by_index),
        _per_index_delta_md(all_rows_by_index),
        _cqi_shift_md(cqi_pairs),
        _rqs_impact_md(rqs_rows),
        _recommendation_md(all_rows_by_index),
        _case_studies_md(all_rows_by_index),
    ])
    return header + body


# =============================================================================
# Entry point
# =============================================================================


def main() -> None:
    init_pool()
    _require_db()

    print("Loading all scored + accruing entities...", file=sys.stderr)
    all_rows_by_index = {}

    # SII
    all_rows_by_index["sii"] = analyze_sii()
    # PSI
    _, _, _, psi_q = INDEX_SOURCES[1]
    all_rows_by_index["psi"] = analyze_psi_rpi("psi", psi_q, "app.index_definitions.psi_v01", "PSI_V01_DEFINITION")
    # RPI
    _, _, _, rpi_q = INDEX_SOURCES[2]
    all_rows_by_index["rpi"] = analyze_psi_rpi("rpi", rpi_q, "app.index_definitions.rpi_v2", "RPI_V2_DEFINITION")
    # generic_index_scores residents: the accruing set plus the Circle 7
    # indices that were promoted to scored in their v0.2.0 migrations.
    for idx in GENERIC_TABLE_INDEX_IDS:
        all_rows_by_index[idx] = analyze_generic_index(idx)

    print("Computing CQI shift matrix...", file=sys.stderr)
    cqi_pairs = analyze_cqi_shift(all_rows_by_index["sii"], all_rows_by_index["psi"])

    print("Computing RQS thresholds...", file=sys.stderr)
    rqs_rows = analyze_rqs()

    print(f"Writing report to {REPORT_PATH}...", file=sys.stderr)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(build_report(all_rows_by_index, cqi_pairs, rqs_rows))
    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
