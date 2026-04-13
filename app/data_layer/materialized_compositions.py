"""
Materialized Compositions
===========================
Move CQI (and future compositions) from on-demand to materialized.
Recompute after each scoring cycle. Store results.
The API reads from materialized tables, not live computation.

Scales to thousands of composition pairs when third-party indices
start composing.

Schedule: After each scoring cycle
"""

import json
import logging
import math
import time
from datetime import datetime, timezone

from app.database import fetch_all, fetch_one, execute, get_cursor

logger = logging.getLogger(__name__)


def _compute_cqi_scores() -> list[dict]:
    """
    Compute CQI (Composite Quality Index) for all stablecoin-protocol pairs.
    CQI = geometric_mean(SII_score, PSI_score) for each pair in collateral exposure.
    """
    from app.composition import compose_geometric_mean

    # Get all SII scores
    sii_rows = fetch_all(
        "SELECT stablecoin_id, overall_score FROM scores WHERE overall_score IS NOT NULL"
    )
    if not sii_rows:
        return []
    sii_scores = {r["stablecoin_id"]: float(r["overall_score"]) for r in sii_rows}

    # Get all PSI scores
    psi_rows = fetch_all(
        """SELECT protocol_slug, overall_score FROM psi_scores
           WHERE overall_score IS NOT NULL"""
    )
    if not psi_rows:
        return []
    psi_scores = {r["protocol_slug"]: float(r["overall_score"]) for r in psi_rows}

    # Get collateral exposure pairs
    pairs = fetch_all(
        """SELECT DISTINCT protocol_slug, stablecoin_id
           FROM protocol_collateral_exposure
           WHERE snapshot_date >= CURRENT_DATE - 7"""
    )
    if not pairs:
        # Fall back to all combinations
        results = []
        for sid, sii in sii_scores.items():
            for pslug, psi in psi_scores.items():
                cqi = compose_geometric_mean([sii, psi])
                if cqi:
                    results.append({
                        "entity_id": f"{sid}:{pslug}",
                        "index_id": "cqi",
                        "overall_score": cqi,
                        "components": {"sii": sii, "psi": psi},
                        "entity_type": "pair",
                        "stablecoin_id": sid,
                        "protocol_slug": pslug,
                    })
        return results

    results = []
    for pair in pairs:
        sid = pair["stablecoin_id"]
        pslug = pair["protocol_slug"]

        sii = sii_scores.get(sid)
        psi = psi_scores.get(pslug)

        if sii and psi:
            cqi = compose_geometric_mean([sii, psi])
            if cqi:
                results.append({
                    "entity_id": f"{sid}:{pslug}",
                    "index_id": "cqi",
                    "overall_score": cqi,
                    "components": {"sii": sii, "psi": psi},
                    "entity_type": "pair",
                    "stablecoin_id": sid,
                    "protocol_slug": pslug,
                })

    return results


def _sanitize_float(val):
    """Return None if val is NaN or Infinity, else return val."""
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _store_materialized_scores(scores: list[dict]):
    """Store materialized composition scores (per-row transactions)."""
    if not scores:
        return

    stored = 0
    errors = 0

    for score in scores:
        try:
            # Sanitize numeric fields in components
            components = score.get("components")
            if isinstance(components, dict):
                components = {
                    k: _sanitize_float(v) if isinstance(v, float) else v
                    for k, v in components.items()
                }

            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO generic_index_scores
                       (index_id, entity_id, overall_score, component_scores,
                        methodology_version, computed_at)
                       VALUES (%s, %s, %s, %s, 'materialized_v1', NOW())
                       ON CONFLICT (index_id, entity_id, immutable_date(computed_at))
                       DO UPDATE SET
                           overall_score = EXCLUDED.overall_score,
                           component_scores = EXCLUDED.component_scores,
                           computed_at = NOW()""",
                    (
                        score["index_id"],
                        score["entity_id"],
                        _sanitize_float(score["overall_score"]),
                        json.dumps(components),
                    ),
                )
            stored += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.error(
                    "Failed to store materialized score for %s/%s: %s",
                    score.get("index_id"), score.get("entity_id"), e,
                )

    if errors:
        logger.error(
            "Materialized scores store: %d stored, %d errors", stored, errors,
        )


def run_materialized_compositions() -> dict:
    """
    Recompute all materialized compositions.
    Called after each scoring cycle.
    """
    start = time.time()
    results = {}

    # CQI
    try:
        cqi_scores = _compute_cqi_scores()
        if cqi_scores:
            _store_materialized_scores(cqi_scores)
            results["cqi"] = {
                "pairs_computed": len(cqi_scores),
                "avg_score": round(
                    sum(s["overall_score"] for s in cqi_scores) / len(cqi_scores), 2
                ),
            }
        else:
            results["cqi"] = {"pairs_computed": 0}
    except Exception as e:
        logger.warning(f"CQI materialization failed: {e}")
        results["cqi"] = {"error": str(e)}

    elapsed = time.time() - start
    logger.info(
        f"Materialized compositions complete in {elapsed:.1f}s: {results}"
    )

    return results
