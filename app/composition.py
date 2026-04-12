"""
Composition Engine
===================
Composes indices (SII, PSI) into composite risk views (CQI).
All scores computed on-demand — no storage.
"""

import logging
import math

from app.database import execute, fetch_all, fetch_one

logger = logging.getLogger(__name__)


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


def _get_latest_hash(domain, entity_id=None):
    """Fetch the latest batch hash from state_attestations for a domain/entity."""
    try:
        if entity_id:
            row = fetch_one(
                """SELECT batch_hash FROM state_attestations
                   WHERE domain = %s AND entity_id = %s
                   ORDER BY cycle_timestamp DESC LIMIT 1""",
                (domain, entity_id),
            )
        else:
            row = fetch_one(
                """SELECT batch_hash FROM state_attestations
                   WHERE domain = %s
                   ORDER BY cycle_timestamp DESC LIMIT 1""",
                (domain,),
            )
        return row["batch_hash"] if row else None
    except Exception:
        return None


def _store_cqi_attestation(sii_symbol, psi_slug, sii_score, psi_score,
                           cqi_score, method="geometric_mean"):
    """Persist a CQI attestation row for full history retention."""
    sii_hash = _get_latest_hash("sii_components", sii_symbol.lower())
    psi_hash = _get_latest_hash("psi_components")
    try:
        execute(
            """
            INSERT INTO cqi_attestations
                (sii_symbol, psi_slug, sii_score, psi_score, cqi_score,
                 sii_hash, psi_hash, composition_method, methodology_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (sii_symbol.upper(), psi_slug, sii_score, psi_score, cqi_score,
             sii_hash, psi_hash, method, "composition-v1.0.0"),
        )
    except Exception as e:
        logger.debug(f"CQI attestation skipped for {sii_symbol}/{psi_slug}: {e}")


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

    # Persist attestation for full history
    _store_cqi_attestation(asset_symbol, protocol_slug, sii_score, psi_score, cqi_score)

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

    # Persist per-pair attestations for full history
    for row in matrix:
        _store_cqi_attestation(
            row["asset"], row["protocol_slug"],
            row["sii_score"], row["psi_score"], row["cqi_score"],
        )

    # Batch-level attestation (state hash)
    try:
        from app.state_attestation import attest_state
        if matrix:
            attest_state("cqi_compositions", [{"asset": r["asset"], "protocol": r["protocol_slug"], "cqi_score": round(r["cqi_score"], 2)} for r in matrix])
    except Exception:
        pass  # attestation is non-critical

    return {"matrix": matrix, "count": len(matrix)}
