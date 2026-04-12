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


def compute_rqs(holdings: list[dict]) -> dict:
    """
    Compute Reserve Quality Score from a list of stablecoin holdings.

    Each holding dict must have:
      - symbol: stablecoin ticker (e.g. "USDC")
      - weight: proportion of portfolio (0-1, must sum to ~1)

    Fetches current SII score for each stablecoin.
    Returns weighted-average SII as the RQS, plus component breakdown.
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

    result = {
        "composite_id": "rqs",
        "name": "Reserve Quality Score",
        "rqs_score": rqs_score,
        "scored_coverage": scored_coverage,
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


def compute_rqs_for_protocol(protocol_slug: str) -> dict:
    """
    Compute RQS for a specific PSI-scored protocol using its treasury holdings.

    Reads from protocol_treasury_holdings table (stablecoin rows with SII scores).
    Weights are proportional to USD value held.
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

    result = compute_rqs(holdings)

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
