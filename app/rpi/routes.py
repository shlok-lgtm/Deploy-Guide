"""
RPI API Routes
===============
RESTful endpoints for the Risk Posture Index, following the exact
same conventions as PSI endpoints in app/server.py.

Free endpoints:
  GET /api/rpi/scores          — All protocols with RPI scores
  GET /api/rpi/scores/{slug}   — Single protocol with full component breakdown
  GET /api/rpi/rankings        — Ranked by RPI score
  GET /api/rpi/components/{slug} — Component-level detail
  GET /api/rpi/history/{slug}  — Score history
  GET /api/rpi/compare         — Multi-protocol comparison
  GET /api/rpi/definition      — Full index definition
  GET /api/rpi/scores/{slug}/verify — Verify score from stored raw values
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.database import fetch_all, fetch_one
from app.index_definitions.rpi_v01 import RPI_V01_DEFINITION
from app.scoring_engine import compute_confidence_tag

logger = logging.getLogger(__name__)

rpi_router = APIRouter(prefix="/api/rpi", tags=["rpi"])

_SOLANA_PROTOCOL_SLUGS = {"drift", "jupiter-perpetual-exchange", "raydium"}


# =============================================================================
# GET /api/rpi/scores — All scored protocols (latest per protocol)
# =============================================================================

@rpi_router.get("/scores")
async def rpi_scores():
    """All scored protocols — latest RPI score per protocol."""
    rows = fetch_all("""
        SELECT DISTINCT ON (protocol_slug)
            id, protocol_slug, protocol_name, overall_score, grade,
            category_scores, component_scores, raw_values,
            formula_version, computed_at
        FROM rpi_scores
        ORDER BY protocol_slug, computed_at DESC
    """)

    rpi_cats_total = len(RPI_V01_DEFINITION["categories"])
    rpi_comps_total = len(RPI_V01_DEFINITION["components"])

    results = []
    for row in rows:
        slug = row["protocol_slug"]
        cat_scores = row.get("category_scores") or {}
        comp_scores = row.get("component_scores") or {}
        comps_populated = len(comp_scores)
        coverage = round(comps_populated / max(rpi_comps_total, 1), 2)
        missing = sorted(set(RPI_V01_DEFINITION["categories"].keys()) - set(cat_scores.keys()))
        conf = compute_confidence_tag(rpi_cats_total - len(missing), rpi_cats_total, coverage, missing)

        results.append({
            "protocol_slug": slug,
            "protocol_name": row["protocol_name"],
            "score": float(row["overall_score"]) if row.get("overall_score") else None,
            "confidence": conf["confidence"],
            "confidence_tag": conf["tag"],
            "missing_categories": conf["missing_categories"],
            "component_coverage": coverage,
            "components_populated": comps_populated,
            "components_total": rpi_comps_total,
            "chain": "solana" if slug in _SOLANA_PROTOCOL_SLUGS else "ethereum",
            "category_scores": cat_scores,
            "formula_version": row.get("formula_version"),
            "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        })

    return {
        "protocols": results,
        "count": len(results),
        "index": "rpi",
        "version": RPI_V01_DEFINITION["version"],
        "methodology_version": f"rpi-{RPI_V01_DEFINITION['version']}",
    }


# =============================================================================
# GET /api/rpi/scores/{slug} — Detailed breakdown for one protocol
# =============================================================================

@rpi_router.get("/scores/{slug}")
async def rpi_score_detail(slug: str):
    """Detailed RPI breakdown for one protocol."""
    row = fetch_one("""
        SELECT id, protocol_slug, protocol_name, overall_score, grade,
               category_scores, component_scores, raw_values,
               formula_version, computed_at
        FROM rpi_scores
        WHERE protocol_slug = %s
        ORDER BY computed_at DESC
        LIMIT 1
    """, (slug,))
    if not row:
        raise HTTPException(status_code=404, detail=f"Protocol '{slug}' not found in RPI scores")

    cat_scores = row.get("category_scores") or {}
    comp_scores = row.get("component_scores") or {}
    comps_populated = len(comp_scores)
    comps_total = len(RPI_V01_DEFINITION["components"])
    coverage = round(comps_populated / max(comps_total, 1), 2)
    missing = sorted(set(RPI_V01_DEFINITION["categories"].keys()) - set(cat_scores.keys()))
    conf = compute_confidence_tag(
        len(RPI_V01_DEFINITION["categories"]) - len(missing),
        len(RPI_V01_DEFINITION["categories"]),
        coverage, missing,
    )

    return {
        "protocol_slug": row["protocol_slug"],
        "protocol_name": row["protocol_name"],
        "score": float(row["overall_score"]) if row.get("overall_score") else None,
        "confidence": conf["confidence"],
        "confidence_tag": conf["tag"],
        "missing_categories": conf["missing_categories"],
        "component_coverage": coverage,
        "components_populated": comps_populated,
        "components_total": comps_total,
        "category_scores": cat_scores,
        "component_scores": comp_scores,
        "raw_values": row.get("raw_values"),
        "formula_version": row.get("formula_version"),
        "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
    }


# =============================================================================
# GET /api/rpi/rankings — Ranked by RPI score descending
# =============================================================================

@rpi_router.get("/rankings")
async def rpi_rankings():
    """All protocols ranked by RPI score (highest first)."""
    rows = fetch_all("""
        SELECT DISTINCT ON (protocol_slug)
            protocol_slug, protocol_name, overall_score, grade,
            category_scores, formula_version, computed_at
        FROM rpi_scores
        ORDER BY protocol_slug, computed_at DESC
    """)

    # Sort by score descending
    ranked = sorted(rows, key=lambda r: float(r["overall_score"]) if r.get("overall_score") else 0, reverse=True)

    results = []
    for i, row in enumerate(ranked, 1):
        results.append({
            "rank": i,
            "protocol_slug": row["protocol_slug"],
            "protocol_name": row["protocol_name"],
            "score": float(row["overall_score"]) if row.get("overall_score") else None,
            "category_scores": row.get("category_scores"),
            "chain": "solana" if row["protocol_slug"] in _SOLANA_PROTOCOL_SLUGS else "ethereum",
            "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        })

    return {
        "rankings": results,
        "count": len(results),
        "index": "rpi",
        "version": RPI_V01_DEFINITION["version"],
    }


# =============================================================================
# GET /api/rpi/components/{slug} — Component-level detail
# =============================================================================

@rpi_router.get("/components/{slug}")
async def rpi_components(slug: str):
    """Component-level detail for one protocol's RPI."""
    # Get latest component readings
    components = fetch_all("""
        SELECT DISTINCT ON (component_id)
            component_id, raw_value, normalized_score, source, source_url,
            metadata, collected_at
        FROM rpi_components
        WHERE protocol_slug = %s
        ORDER BY component_id, collected_at DESC
    """, (slug,))

    # Get latest score for context
    score_row = fetch_one("""
        SELECT overall_score, component_scores, raw_values, formula_version, computed_at
        FROM rpi_scores WHERE protocol_slug = %s
        ORDER BY computed_at DESC LIMIT 1
    """, (slug,))

    # Enrich components with definition metadata
    comp_defs = RPI_V01_DEFINITION["components"]
    enriched = []
    for c in components:
        cid = c["component_id"]
        definition = comp_defs.get(cid, {})
        enriched.append({
            "component_id": cid,
            "name": definition.get("name", cid),
            "category": definition.get("category", "unknown"),
            "weight": definition.get("weight", 0),
            "raw_value": c["raw_value"],
            "normalized_score": round(float(c["normalized_score"]), 2) if c.get("normalized_score") is not None else None,
            "source": c["source"],
            "source_url": c.get("source_url"),
            "metadata": c.get("metadata"),
            "collected_at": c["collected_at"].isoformat() if c.get("collected_at") else None,
        })

    return {
        "protocol_slug": slug,
        "components": enriched,
        "components_count": len(enriched),
        "overall_score": float(score_row["overall_score"]) if score_row and score_row.get("overall_score") else None,
        "formula_version": score_row.get("formula_version") if score_row else None,
        "computed_at": score_row["computed_at"].isoformat() if score_row and score_row.get("computed_at") else None,
    }


# =============================================================================
# GET /api/rpi/history/{slug} — Score history
# =============================================================================

@rpi_router.get("/history/{slug}")
async def rpi_history(
    slug: str,
    limit: int = Query(default=90, ge=1, le=365),
):
    """RPI score history for a protocol (one per day)."""
    rows = fetch_all("""
        SELECT scored_date, overall_score, category_scores, formula_version, computed_at
        FROM rpi_scores
        WHERE protocol_slug = %s
        ORDER BY scored_date DESC
        LIMIT %s
    """, (slug, limit))

    if not rows:
        raise HTTPException(status_code=404, detail=f"No RPI history for '{slug}'")

    return {
        "protocol_slug": slug,
        "history": [
            {
                "date": row["scored_date"].isoformat() if hasattr(row["scored_date"], "isoformat") else str(row["scored_date"]),
                "score": float(row["overall_score"]) if row.get("overall_score") else None,
                "category_scores": row.get("category_scores"),
                "formula_version": row.get("formula_version"),
            }
            for row in rows
        ],
        "count": len(rows),
        "index": "rpi",
        "version": RPI_V01_DEFINITION["version"],
    }


# =============================================================================
# GET /api/rpi/compare — Multi-protocol comparison
# =============================================================================

@rpi_router.get("/compare")
async def rpi_compare(
    slugs: str = Query(..., description="Comma-separated protocol slugs"),
):
    """Compare RPI scores across multiple protocols."""
    slug_list = [s.strip() for s in slugs.split(",") if s.strip()]
    if len(slug_list) < 2:
        raise HTTPException(status_code=400, detail="Provide at least 2 protocol slugs (comma-separated)")
    if len(slug_list) > 13:
        raise HTTPException(status_code=400, detail="Maximum 13 protocols per comparison")

    results = []
    for slug in slug_list:
        row = fetch_one("""
            SELECT protocol_slug, protocol_name, overall_score, category_scores,
                   component_scores, formula_version, computed_at
            FROM rpi_scores WHERE protocol_slug = %s
            ORDER BY computed_at DESC LIMIT 1
        """, (slug,))

        if row:
            results.append({
                "protocol_slug": row["protocol_slug"],
                "protocol_name": row["protocol_name"],
                "score": float(row["overall_score"]) if row.get("overall_score") else None,
                "category_scores": row.get("category_scores"),
                "component_scores": row.get("component_scores"),
                "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
            })
        else:
            results.append({
                "protocol_slug": slug,
                "protocol_name": None,
                "score": None,
                "category_scores": None,
                "component_scores": None,
                "computed_at": None,
                "error": "not_found",
            })

    return {
        "comparison": results,
        "count": len(results),
        "index": "rpi",
        "version": RPI_V01_DEFINITION["version"],
    }


# =============================================================================
# GET /api/rpi/definition — Full index definition
# =============================================================================

@rpi_router.get("/definition")
async def rpi_definition():
    """Return the full RPI index definition."""
    return {
        **RPI_V01_DEFINITION,
        "methodology_version": f"rpi-{RPI_V01_DEFINITION['version']}",
    }


# =============================================================================
# GET /api/rpi/scores/{slug}/verify — Verify score integrity
# =============================================================================

@rpi_router.get("/scores/{slug}/verify")
async def verify_rpi_score(slug: str):
    """Verify the latest RPI score by re-deriving from stored raw values."""
    row = fetch_one("""
        SELECT protocol_slug, overall_score, grade, raw_values,
               inputs_hash, formula_version, computed_at
        FROM rpi_scores WHERE protocol_slug = %s
        ORDER BY computed_at DESC LIMIT 1
    """, (slug,))

    if not row:
        raise HTTPException(status_code=404, detail=f"No RPI score found for {slug}")

    stored_score = float(row["overall_score"]) if row.get("overall_score") else None
    stored_hash = row.get("inputs_hash")
    raw_values = row.get("raw_values", {})

    # Re-derive hash from stored raw values
    raw_canonical = json.dumps(raw_values, sort_keys=True, default=str)
    recomputed_hash = "0x" + hashlib.sha256(raw_canonical.encode()).hexdigest()

    # Re-derive score from stored raw values
    from app.rpi.scorer import score_protocol_from_raw
    recomputed = score_protocol_from_raw(slug, raw_values)
    recomputed_score = recomputed.get("overall_score") if recomputed else None

    hash_match = stored_hash == recomputed_hash if stored_hash else None
    score_match = abs(stored_score - recomputed_score) < 0.01 if stored_score and recomputed_score else None

    if hash_match and score_match:
        status = "verified"
    elif hash_match is False or score_match is False:
        status = "mismatch"
    else:
        status = "no_hash_stored"

    return {
        "protocol": slug,
        "verification": {
            "status": status,
            "stored_score": stored_score,
            "recomputed_score": round(recomputed_score, 2) if recomputed_score else None,
            "score_match": score_match,
            "hash_match": hash_match,
            "formula_version": row.get("formula_version"),
        },
        "hashes": {
            "stored_inputs_hash": stored_hash,
            "recomputed_inputs_hash": recomputed_hash,
        },
        "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
    }
