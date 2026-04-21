"""
Composition Engine
===================
Composes indices (SII, PSI) into composite risk views (CQI).
All scores computed on-demand — no storage.
"""

import math

from app.database import fetch_all, fetch_one


def compose_geometric_mean(scores):
    """Geometric mean — penalizes weakness in any component."""
    if not scores or any(s is None or s <= 0 for s in scores):
        return None
    product = 1.0
    for s in scores:
        product *= s
    return round(product ** (1.0 / len(scores)), 2)


def compose_weighted_average(scores, weights=None):
    """Weighted average — linear blend."""
    if not scores:
        return None
    if weights is None:
        weights = [1.0] * len(scores)
    total = sum(s * w for s, w in zip(scores, weights) if s is not None)
    weight_sum = sum(w for s, w in zip(scores, weights) if s is not None)
    return round(total / weight_sum, 2) if weight_sum > 0 else None


def compose_minimum(scores):
    """Minimum — only as strong as weakest link."""
    valid = [s for s in scores if s is not None]
    return min(valid) if valid else None


def _sii_confidence(component_count, sii_components_total=39):
    """Compute SII confidence from component count."""
    from app.scoring_engine import compute_confidence_tag
    coverage = round(component_count / max(sii_components_total, 1), 2)
    return compute_confidence_tag(0, 0, coverage)


def _psi_confidence(component_scores_dict, psi_components_total=27):
    """Compute PSI confidence from component scores dict."""
    from app.scoring_engine import compute_confidence_tag
    populated = len(component_scores_dict) if component_scores_dict else 0
    coverage = round(populated / max(psi_components_total, 1), 2)
    return compute_confidence_tag(0, 0, coverage)


def _lower_confidence(conf_a, conf_b):
    """Return the lower of two confidence levels."""
    order = {"limited": 0, "standard": 1, "high": 2}
    a_rank = order.get(conf_a.get("confidence", "high"), 2)
    b_rank = order.get(conf_b.get("confidence", "high"), 2)
    return conf_a if a_rank <= b_rank else conf_b


def compute_cqi(asset_symbol, protocol_slug):
    """
    Compute Collateral Quality Index for an asset-in-protocol pair.
    Fetches SII and PSI scores from the database on demand.
    """
    # Get SII score from scores table joined to stablecoins
    sii_row = fetch_one("""
        SELECT s.overall_score, s.grade, s.component_count
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        WHERE UPPER(st.symbol) = UPPER(%s)
    """, (asset_symbol,))

    if not sii_row or sii_row.get("overall_score") is None:
        return {"error": f"SII score not found for {asset_symbol}"}

    # Get PSI score
    psi_row = fetch_one("""
        SELECT overall_score, grade, protocol_name, component_scores
        FROM psi_scores
        WHERE protocol_slug = %s
        ORDER BY computed_at DESC
        LIMIT 1
    """, (protocol_slug,))

    if not psi_row or psi_row.get("overall_score") is None:
        return {"error": f"PSI score not found for {protocol_slug}. Run PSI scoring first."}

    sii_score = float(sii_row["overall_score"])
    psi_score = float(psi_row["overall_score"])
    cqi_score = compose_geometric_mean([sii_score, psi_score])

    sii_conf = _sii_confidence(sii_row.get("component_count") or 0)
    psi_conf = _psi_confidence(psi_row.get("component_scores") or {})
    cqi_conf = _lower_confidence(sii_conf, psi_conf)

    return {
        "composite_id": "cqi",
        "name": "Collateral Quality Index",
        "asset": asset_symbol.upper(),
        "protocol": psi_row.get("protocol_name", protocol_slug),
        "protocol_slug": protocol_slug,
        "cqi_score": cqi_score,
        "confidence": cqi_conf["confidence"],
        "confidence_tag": cqi_conf["tag"],
        "inputs": {
            "sii": {"score": sii_score, "confidence": sii_conf["confidence"]},
            "psi": {"score": psi_score, "confidence": psi_conf["confidence"]},
        },
        "method": "geometric_mean",
        "formula_version": "composition-v1.0.0",
    }


# =============================================================================
# RQS (Reserve Quality Score) — weighted-average SII over protocol holdings
# =============================================================================


def compute_rqs(holdings: list[dict], coverage_threshold: float = 0.0) -> dict:
    """
    Compute Reserve Quality Score from a list of stablecoin holdings.

    Each holding dict must have:
      - symbol: stablecoin ticker (e.g. "USDC")
      - weight: proportion of portfolio (0-1, must sum to ~1)

    Fetches current SII score for each stablecoin.
    Returns weighted-average SII as the RQS, plus component breakdown.

    Args:
        holdings: list of {symbol, weight} dicts.
        coverage_threshold: minimum `scored_coverage` (0..1) required to
            return a non-null rqs_score. Default 0.0 preserves pre-registry
            behavior (always returns a score as long as scored_weight > 0).
            When scored_coverage < coverage_threshold, rqs_score is None and
            withheld=True. Parallels `aggregate_coverage_withheld` for the
            within-index case.
    """
    if not holdings:
        return {"error": "No holdings provided"}

    # Normalise weights to ensure they sum to 1
    raw_weight_sum = sum(h.get("weight", 0) for h in holdings)
    if raw_weight_sum <= 0:
        return {"error": "Holdings weights must be positive"}

    breakdown = []
    scored_weight = 0.0
    weighted_sum = 0.0
    warnings = []
    oldest_sii_at = None  # track the oldest SII computed_at

    for h in holdings:
        symbol = h.get("symbol", "").upper()
        weight = h.get("weight", 0) / raw_weight_sum  # normalise

        sii_row = fetch_one("""
            SELECT s.overall_score, s.component_count, s.computed_at
            FROM scores s
            JOIN stablecoins st ON st.id = s.stablecoin_id
            WHERE UPPER(st.symbol) = UPPER(%s)
        """, (symbol,))

        sii_score = None
        if sii_row and sii_row.get("overall_score") is not None:
            sii_score = float(sii_row["overall_score"])
            contribution = round(weight * sii_score, 4)
            weighted_sum += contribution
            scored_weight += weight

            sii_at = sii_row.get("computed_at")
            if sii_at and (oldest_sii_at is None or sii_at < oldest_sii_at):
                oldest_sii_at = sii_at

            breakdown.append({
                "symbol": symbol,
                "weight": round(weight, 4),
                "sii_score": round(sii_score, 2),
                "contribution": contribution,
                "scored": True,
            })
        else:
            warnings.append(f"{symbol} has no SII score — excluded from RQS")
            breakdown.append({
                "symbol": symbol,
                "weight": round(weight, 4),
                "sii_score": None,
                "contribution": 0,
                "scored": False,
            })

    scored_coverage = round(scored_weight, 4)

    if scored_weight <= 0:
        return {
            "error": "None of the provided holdings have SII scores",
            "breakdown": breakdown,
            "warnings": warnings,
        }

    # Re-normalise over scored-only weight so RQS stays on 0-100 scale
    rqs_score = round(weighted_sum / scored_weight, 2)

    # Confidence based on how much of the portfolio is scored
    from app.scoring_engine import compute_confidence_tag
    conf = compute_confidence_tag(0, 0, scored_coverage)

    # Threshold-gated withhold — honest-coverage guard for portfolio
    # aggregation, parallels aggregate_coverage_withheld for within-index.
    withheld = False
    if coverage_threshold > 0 and scored_coverage < coverage_threshold:
        withheld = True
        warnings.append(
            f"RQS withheld: scored_coverage {scored_coverage} below "
            f"coverage_threshold {coverage_threshold}"
        )

    result = {
        "composite_id": "rqs",
        "name": "Reserve Quality Score",
        "rqs_score": None if withheld else rqs_score,
        "scored_coverage": scored_coverage,
        "coverage_threshold": coverage_threshold,
        "withheld": withheld,
        "confidence": conf["confidence"],
        "confidence_tag": conf["tag"],
        "breakdown": sorted(breakdown, key=lambda x: x["contribution"], reverse=True),
        "warnings": warnings,
        "method": "weighted_average",
        "formula_version": "composition-v1.0.0",
    }

    if oldest_sii_at:
        result["sii_scored_at"] = oldest_sii_at.isoformat() if hasattr(oldest_sii_at, "isoformat") else str(oldest_sii_at)

    return result


def compute_rqs_for_protocol(protocol_slug: str, coverage_threshold: float = 0.0) -> dict:
    """
    Compute RQS for a specific PSI-scored protocol using its treasury holdings.

    Reads from protocol_treasury_holdings table (stablecoin rows with SII scores).
    Weights are proportional to USD value held.

    Args:
        protocol_slug: PSI protocol slug.
        coverage_threshold: forwarded to `compute_rqs`. Default 0.0 keeps
            existing non-threshold behavior.
    """
    # Verify protocol exists in PSI
    psi_row = fetch_one("""
        SELECT protocol_name, overall_score
        FROM psi_scores
        WHERE protocol_slug = %s
        ORDER BY computed_at DESC LIMIT 1
    """, (protocol_slug,))

    if not psi_row:
        return {"error": f"Protocol '{protocol_slug}' not found in PSI scores"}

    # Fetch stablecoin treasury holdings + snapshot date
    rows = fetch_all("""
        SELECT token_symbol, usd_value, sii_score, is_stablecoin, snapshot_date
        FROM protocol_treasury_holdings
        WHERE protocol_slug = %s AND is_stablecoin = TRUE
          AND snapshot_date = (
              SELECT MAX(snapshot_date)
              FROM protocol_treasury_holdings
              WHERE protocol_slug = %s
          )
        ORDER BY usd_value DESC
    """, (protocol_slug, protocol_slug))

    if not rows:
        return {
            "error": f"No stablecoin treasury holdings found for '{protocol_slug}'",
            "protocol": psi_row.get("protocol_name", protocol_slug),
            "protocol_slug": protocol_slug,
        }

    holdings_snapshot_date = rows[0].get("snapshot_date") if rows else None

    # Aggregate by symbol (may appear on multiple chains)
    by_symbol: dict[str, float] = {}
    for r in rows:
        sym = r["token_symbol"].upper()
        by_symbol[sym] = by_symbol.get(sym, 0.0) + float(r["usd_value"])

    total_usd = sum(by_symbol.values())
    if total_usd <= 0:
        return {"error": f"Zero stablecoin value in treasury for '{protocol_slug}'"}

    # Build holdings list with USD-proportional weights
    holdings = [
        {"symbol": sym, "weight": usd / total_usd}
        for sym, usd in by_symbol.items()
    ]

    result = compute_rqs(holdings, coverage_threshold=coverage_threshold)

    if "error" in result and "breakdown" not in result:
        result["protocol"] = psi_row.get("protocol_name", protocol_slug)
        result["protocol_slug"] = protocol_slug
        return result

    # Enrich with protocol context
    psi_score = float(psi_row["overall_score"]) if psi_row.get("overall_score") else None
    result["protocol"] = psi_row.get("protocol_name", protocol_slug)
    result["protocol_slug"] = protocol_slug
    result["psi_score"] = round(psi_score, 2) if psi_score else None
    result["treasury_total_usd"] = round(total_usd, 2)

    # Add USD values to breakdown
    for item in result.get("breakdown", []):
        item["usd_value"] = round(by_symbol.get(item["symbol"], 0), 2)

    # Surface staleness: data_as_of = older of holdings snapshot vs SII scores
    if holdings_snapshot_date:
        snap_str = holdings_snapshot_date.isoformat() if hasattr(holdings_snapshot_date, "isoformat") else str(holdings_snapshot_date)
        result["holdings_as_of"] = snap_str

    sii_at_str = result.get("sii_scored_at")
    if holdings_snapshot_date and sii_at_str:
        # Compare date portion of both timestamps
        snap_date_str = str(holdings_snapshot_date)[:10]
        sii_date_str = sii_at_str[:10]
        result["data_as_of"] = min(snap_date_str, sii_date_str)
    elif holdings_snapshot_date:
        result["data_as_of"] = str(holdings_snapshot_date)[:10]
    elif sii_at_str:
        result["data_as_of"] = sii_at_str[:10]

    # Attest state
    try:
        from app.state_attestation import attest_state
        attest_state("rqs_composition", [
            {"protocol": protocol_slug, "rqs_score": result.get("rqs_score")},
        ], entity_id=protocol_slug)
    except Exception:
        pass  # attestation is non-critical

    return result


def compute_rqs_all() -> dict:
    """Compute RQS for all protocols that have treasury holdings data."""
    from app.index_definitions.psi_v01 import TARGET_PROTOCOLS

    results = []
    errors = []

    for slug in TARGET_PROTOCOLS:
        result = compute_rqs_for_protocol(slug)
        if "error" in result and "rqs_score" not in result:
            errors.append({"protocol_slug": slug, "error": result["error"]})
        else:
            results.append(result)

    results.sort(key=lambda x: x.get("rqs_score", 0), reverse=True)

    # Attest batch
    try:
        from app.state_attestation import attest_state
        if results:
            attest_state("rqs_compositions", [
                {"protocol": r["protocol_slug"], "rqs_score": round(r["rqs_score"], 2)}
                for r in results if r.get("rqs_score") is not None
            ])
    except Exception:
        pass

    return {
        "protocols": results,
        "count": len(results),
        "skipped": errors,
        "formula_version": "composition-v1.0.0",
    }


def compute_cqi_matrix():
    """Compute CQI for all stablecoin x protocol combinations."""
    stablecoins = fetch_all("""
        SELECT st.symbol, s.overall_score, s.grade, s.component_count
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        WHERE s.overall_score IS NOT NULL
        ORDER BY s.overall_score DESC
    """)

    protocols = fetch_all("""
        SELECT DISTINCT ON (protocol_slug)
            protocol_slug, protocol_name, overall_score, grade, component_scores
        FROM psi_scores
        ORDER BY protocol_slug, computed_at DESC
    """)

    if not protocols:
        return {"error": "No PSI scores available. Run PSI scoring first.", "matrix": []}

    matrix = []
    for coin in stablecoins:
        sii = float(coin["overall_score"]) if coin.get("overall_score") else None
        sii_conf = _sii_confidence(coin.get("component_count") or 0)
        for proto in protocols:
            psi = float(proto["overall_score"]) if proto.get("overall_score") else None
            if sii and psi:
                cqi = compose_geometric_mean([sii, psi])
                psi_conf = _psi_confidence(proto.get("component_scores") or {})
                cqi_conf = _lower_confidence(sii_conf, psi_conf)
                matrix.append({
                    "asset": coin["symbol"],
                    "protocol": proto.get("protocol_name", proto["protocol_slug"]),
                    "protocol_slug": proto["protocol_slug"],
                    "cqi_score": cqi,
                    "confidence": cqi_conf["confidence"],
                    "sii_score": sii,
                    "psi_score": psi,
                })

    matrix.sort(key=lambda x: x.get("cqi_score", 0), reverse=True)

    # Attest CQI compositions
    try:
        from app.state_attestation import attest_state
        if matrix:
            attest_state("cqi_compositions", [{"asset": r["asset"], "protocol": r["protocol_slug"], "cqi_score": round(r["cqi_score"], 2)} for r in matrix])
    except Exception:
        pass  # attestation is non-critical

    return {"matrix": matrix, "count": len(matrix)}


# =============================================================================
# Aggregation Formulas — within-index weighted-sum dispatch
# =============================================================================
#
# Parallel to the composition formulas at the top of this file, these named
# aggregation formulas extend the registry pattern to within-index
# aggregation. Every index's score_entity() call dispatches through one of
# these via aggregate(). The default is legacy_renormalize, which preserves
# the pre-registry behavior exactly so historical scores remain reproducible.
#
# Each formula takes (definition, component_scores, raw_values, params) and
# returns the same dict shape:
#
#     {
#         "overall_score":              float | None,
#         "category_scores":            dict[str, float],
#         "effective_category_weights": dict[str, float],
#         "coverage":                   float,   # populated/total, 0..1
#         "withheld":                   bool,
#         "method":                     str,     # formula name
#         "formula_version":            str,
#     }
#
# Every index without an `aggregation` block on its definition produces
# byte-for-byte identical output to pre-registry score_entity(), because
# legacy_renormalize is the default path.

AGGREGATION_FORMULA_VERSION = "aggregation-v1.0.0"


def _cat_nominal_weight(cat_def):
    """Nominal category weight from a definition entry. Preserves the
    current engine's tolerance for non-dict entries (returns 0)."""
    return cat_def.get("weight", 0) if isinstance(cat_def, dict) else 0


def _component_coverage(definition, component_scores):
    """populated_count / total_count, rounded to 2 decimals. Matches the
    legacy score_entity() coverage field exactly so existing thresholds and
    confidence-tag mappings continue to apply."""
    total = len(definition.get("components", {}))
    populated = sum(
        1 for cid in definition.get("components", {})
        if cid in component_scores
    )
    return round(populated / max(total, 1), 2)


def aggregate_legacy_renormalize(definition, component_scores, raw_values, params=None):
    """
    Current (pre-fix) behavior. Silently renormalizes weighted sums over
    populated components at both category and overall levels.

    Preserved forever so historical scores remain reproducible. This is the
    canonical formula for every record written before its index migrated its
    aggregation declaration.
    """
    category_scores = {}
    effective_weights = {}

    for cat_id, cat_def in definition["categories"].items():
        cat_weight = _cat_nominal_weight(cat_def)
        cat_components = {
            cid: cdef for cid, cdef in definition["components"].items()
            if cdef.get("category") == cat_id
        }
        cat_total_comp_weight = sum(cdef.get("weight", 0) for cdef in cat_components.values())
        total = 0.0
        weight_used = 0.0
        for cid, cdef in cat_components.items():
            if cid in component_scores:
                w = cdef.get("weight", 0)
                total += component_scores[cid] * w
                weight_used += w
        if weight_used > 0:
            category_scores[cat_id] = round(total / weight_used, 2)
            if cat_total_comp_weight > 0:
                effective_weights[cat_id] = round(
                    cat_weight * (weight_used / cat_total_comp_weight), 4
                )
            else:
                effective_weights[cat_id] = 0.0
        else:
            effective_weights[cat_id] = 0.0

    # Overall: renormalize over populated categories (legacy behavior — this
    # is the defect the audit documents, preserved here verbatim).
    overall = 0.0
    cat_weight_used = 0.0
    for cat_id, cat_def in definition["categories"].items():
        cat_weight = _cat_nominal_weight(cat_def)
        if cat_id in category_scores:
            overall += category_scores[cat_id] * cat_weight
            cat_weight_used += cat_weight

    if cat_weight_used > 0 and cat_weight_used < 1.0:
        overall = overall / cat_weight_used

    overall_out = round(overall, 2) if cat_weight_used > 0 else None

    return {
        "overall_score": overall_out,
        "category_scores": category_scores,
        "effective_category_weights": effective_weights,
        "coverage": _component_coverage(definition, component_scores),
        "withheld": False,
        "method": "legacy_renormalize",
        "formula_version": AGGREGATION_FORMULA_VERSION,
    }


def aggregate_coverage_weighted(definition, component_scores, raw_values, params=None):
    """
    Option C. Category scores renormalize within category (same as legacy,
    so a partial category still produces a meaningful category reading),
    but the overall weighted sum uses effective category weights:

        effective_weight = nominal_weight
                           × (populated_component_weight
                              / total_component_weight_in_category)

    Missing categories contribute 0 to both numerator and denominator.
    Overall = sum(cat_score × effective_weight) / sum(effective_weights).

    params:
      min_coverage (optional, default 0.0): if the index's overall component
        coverage is below this threshold, overall_score is None and
        withheld=True. Category scores are still returned.
    """
    params = params or {}
    min_coverage = float(params.get("min_coverage", 0.0))

    category_scores = {}
    effective_weights = {}

    for cat_id, cat_def in definition["categories"].items():
        cat_weight = _cat_nominal_weight(cat_def)
        cat_components = {
            cid: cdef for cid, cdef in definition["components"].items()
            if cdef.get("category") == cat_id
        }
        cat_total_comp_weight = sum(cdef.get("weight", 0) for cdef in cat_components.values())
        total = 0.0
        weight_used = 0.0
        for cid, cdef in cat_components.items():
            if cid in component_scores:
                w = cdef.get("weight", 0)
                total += component_scores[cid] * w
                weight_used += w
        if weight_used > 0:
            category_scores[cat_id] = round(total / weight_used, 2)
        if cat_total_comp_weight > 0 and weight_used > 0:
            effective_weights[cat_id] = round(
                cat_weight * (weight_used / cat_total_comp_weight), 4
            )
        else:
            effective_weights[cat_id] = 0.0

    eff_sum = sum(effective_weights.values())
    if eff_sum > 0:
        overall_num = sum(
            category_scores[cat_id] * effective_weights[cat_id]
            for cat_id in category_scores
        )
        overall = round(overall_num / eff_sum, 2)
    else:
        overall = None

    coverage = _component_coverage(definition, component_scores)
    withheld = False
    if min_coverage > 0 and coverage < min_coverage:
        overall = None
        withheld = True

    return {
        "overall_score": overall,
        "category_scores": category_scores,
        "effective_category_weights": effective_weights,
        "coverage": coverage,
        "withheld": withheld,
        "method": "coverage_weighted",
        "formula_version": AGGREGATION_FORMULA_VERSION,
    }


def aggregate_coverage_withheld(definition, component_scores, raw_values, params=None):
    """
    Option D. Same math as coverage_weighted. `coverage_threshold` is
    required in params. Below threshold, overall_score is None and
    withheld=True. Category scores are still returned regardless.

    params:
      coverage_threshold (required): fraction in [0, 1].
    """
    params = params or {}
    if "coverage_threshold" not in params:
        raise ValueError(
            "aggregate_coverage_withheld requires params['coverage_threshold']"
        )
    threshold = float(params["coverage_threshold"])
    if not (0.0 <= threshold <= 1.0):
        raise ValueError(
            f"coverage_threshold must be in [0, 1], got {threshold}"
        )

    result = aggregate_coverage_weighted(
        definition, component_scores, raw_values,
        params={"min_coverage": threshold},
    )
    result["method"] = "coverage_withheld"
    return result


def aggregate_strict_zero(definition, component_scores, raw_values, params=None):
    """
    Option B. Missing components treated as 0. Category denominators are
    full (not renormalized). Category weights at full nominal value.

    Not adopted by any index by default; shipped for completeness.
    """
    category_scores = {}
    effective_weights = {}

    for cat_id, cat_def in definition["categories"].items():
        cat_weight = _cat_nominal_weight(cat_def)
        cat_components = {
            cid: cdef for cid, cdef in definition["components"].items()
            if cdef.get("category") == cat_id
        }
        cat_total_comp_weight = sum(cdef.get("weight", 0) for cdef in cat_components.values())
        if cat_total_comp_weight <= 0:
            effective_weights[cat_id] = 0.0
            continue
        total = 0.0
        for cid, cdef in cat_components.items():
            score = component_scores.get(cid, 0)
            total += score * cdef.get("weight", 0)
        category_scores[cat_id] = round(total / cat_total_comp_weight, 2)
        effective_weights[cat_id] = round(cat_weight, 4)

    total_cat_weight = sum(
        _cat_nominal_weight(cat_def)
        for cat_def in definition["categories"].values()
    )
    if total_cat_weight > 0 and category_scores:
        overall_num = sum(
            category_scores[cat_id]
            * _cat_nominal_weight(definition["categories"][cat_id])
            for cat_id in category_scores
        )
        overall = round(overall_num / total_cat_weight, 2)
    else:
        overall = None

    return {
        "overall_score": overall,
        "category_scores": category_scores,
        "effective_category_weights": effective_weights,
        "coverage": _component_coverage(definition, component_scores),
        "withheld": False,
        "method": "strict_zero",
        "formula_version": AGGREGATION_FORMULA_VERSION,
    }


def aggregate_strict_neutral(definition, component_scores, raw_values, params=None):
    """
    Option A. Missing components imputed to 50 (neutral). Category weights
    at full nominal. Not adopted by any index by default; shipped for
    completeness and future-use.
    """
    imputed = dict(component_scores)
    for cid in definition.get("components", {}):
        if cid not in imputed:
            imputed[cid] = 50
    result = aggregate_strict_zero(definition, imputed, raw_values, params)
    # Coverage reflects actual inputs, not imputations.
    result["coverage"] = _component_coverage(definition, component_scores)
    result["method"] = "strict_neutral"
    return result


def aggregate_legacy_sii_v1(definition, component_scores, raw_values, params=None):
    """
    SII v1.0.0's historical weighted-sum behavior — distinct from
    legacy_renormalize because `app.scoring.calculate_sii` and
    `calculate_structural_composite` do NOT round intermediate category
    scores, whereas legacy_renormalize rounds to 2 decimals per category.

    This formula exists so the registry has SII's slot reserved. The actual
    wire-up of `app.scoring.calculate_sii` to dispatch through this formula
    is deferred to a follow-up PR — routing it now risks changing stored
    SII values (rounding drift) which would violate the no-external-change
    constraint of the aggregation-infrastructure PR.

    Semantics: same as legacy_renormalize EXCEPT no per-category rounding.
    Overall is rounded at the very end (matches calculate_sii's return).
    """
    category_scores = {}
    effective_weights = {}

    for cat_id, cat_def in definition["categories"].items():
        cat_weight = _cat_nominal_weight(cat_def)
        cat_components = {
            cid: cdef for cid, cdef in definition["components"].items()
            if cdef.get("category") == cat_id
        }
        cat_total_comp_weight = sum(cdef.get("weight", 0) for cdef in cat_components.values())
        total = 0.0
        weight_used = 0.0
        for cid, cdef in cat_components.items():
            if cid in component_scores:
                w = cdef.get("weight", 0)
                total += component_scores[cid] * w
                weight_used += w
        if weight_used > 0:
            # No rounding on category score — preserves SII's precision
            category_scores[cat_id] = total / weight_used
            if cat_total_comp_weight > 0:
                effective_weights[cat_id] = round(
                    cat_weight * (weight_used / cat_total_comp_weight), 4
                )
            else:
                effective_weights[cat_id] = 0.0
        else:
            effective_weights[cat_id] = 0.0

    overall = 0.0
    cat_weight_used = 0.0
    for cat_id, cat_def in definition["categories"].items():
        cat_weight = _cat_nominal_weight(cat_def)
        if cat_id in category_scores:
            overall += category_scores[cat_id] * cat_weight
            cat_weight_used += cat_weight

    if cat_weight_used > 0 and cat_weight_used < 1.0:
        overall = overall / cat_weight_used

    overall_out = round(overall, 2) if cat_weight_used > 0 else None

    # Also round category_scores at output time so the API remains consistent
    rounded_cats = {k: round(v, 2) for k, v in category_scores.items()}

    return {
        "overall_score": overall_out,
        "category_scores": rounded_cats,
        "effective_category_weights": effective_weights,
        "coverage": _component_coverage(definition, component_scores),
        "withheld": False,
        "method": "legacy_sii_v1",
        "formula_version": AGGREGATION_FORMULA_VERSION,
    }


AGGREGATION_FORMULAS = {
    "legacy_renormalize": aggregate_legacy_renormalize,
    "coverage_weighted": aggregate_coverage_weighted,
    "coverage_withheld": aggregate_coverage_withheld,
    "strict_zero": aggregate_strict_zero,
    "strict_neutral": aggregate_strict_neutral,
    "legacy_sii_v1": aggregate_legacy_sii_v1,
}


def aggregate(definition, component_scores, raw_values=None):
    """Dispatch aggregation through the formula declared in the index definition.

    If no `aggregation` block is present on the definition, defaults to
    legacy_renormalize with empty params — preserves the pre-registry
    behavior exactly. This is the contract that lets this PR ship without
    any index migrating simultaneously.

    Raises ValueError if the declared formula name is not in
    AGGREGATION_FORMULAS.
    """
    raw_values = raw_values or {}
    agg_config = definition.get("aggregation") or {}
    formula_name = agg_config.get("formula", "legacy_renormalize")
    params = agg_config.get("params", {}) or {}
    formula = AGGREGATION_FORMULAS.get(formula_name)
    if formula is None:
        raise ValueError(
            f"Unknown aggregation formula: {formula_name!r}. "
            f"Valid formulas: {sorted(AGGREGATION_FORMULAS)}"
        )
    return formula(definition, component_scores, raw_values, params)
