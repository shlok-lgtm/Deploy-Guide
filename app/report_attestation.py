"""
Report Attestation — Content Hashing & Storage
================================================
Computes deterministic content hashes for reports and stores
attestation records. The hash covers structured data (not rendered HTML),
so the same report in HTML and JSON produces the same hash.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone

from app.database import execute, fetch_one

logger = logging.getLogger(__name__)


def compute_report_hash(report_data: dict, template: str, lens: str | None,
                        lens_version: str | None, timestamp: str,
                        state_hashes: dict | None = None) -> str:
    """
    Compute deterministic SHA-256 hash of report content.

    Covers: entity_type, entity_id, all score values, all referenced
    score hashes, template version, lens version, timestamp.
    NOT the rendered HTML — the structured data before rendering.

    state_hashes: optional dict of {domain: batch_hash} for state attestations
    that were current when this report was generated.
    """
    canonical_input = {
        "entity_type": report_data.get("entity_type"),
        "entity_id": report_data.get("entity_id"),
        "scores": _extract_scores(report_data),
        "score_hashes": sorted(report_data.get("score_hashes", [])),
        "template": template,
        "lens": lens,
        "lens_version": lens_version,
        "timestamp": timestamp,
    }
    if state_hashes:
        canonical_input["state_attestations"] = {k: v for k, v in sorted(state_hashes.items())}
    canonical = json.dumps(canonical_input, sort_keys=True, separators=(",", ":"),
                           default=_json_default)
    return "0x" + hashlib.sha256(canonical.encode()).hexdigest()


def store_report_attestation(entity_type: str, entity_id: str, template: str,
                             lens: str | None, lens_version: str | None,
                             report_hash: str, score_hashes: list,
                             cqi_hashes: list | None,
                             methodology_version: str) -> None:
    """Store a report attestation record."""
    try:
        execute(
            """
            INSERT INTO report_attestations
                (entity_type, entity_id, template, lens, lens_version,
                 report_hash, score_hashes, cqi_hashes, methodology_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_hash) DO NOTHING
            """,
            (entity_type, entity_id, template, lens, lens_version,
             report_hash, json.dumps(score_hashes), json.dumps(cqi_hashes) if cqi_hashes else None,
             methodology_version),
        )
    except Exception as e:
        logger.warning(f"Failed to store report attestation: {e}")


def verify_report(report_hash: str) -> dict:
    """Verify a report's attestation chain."""
    row = fetch_one(
        "SELECT * FROM report_attestations WHERE report_hash = %s",
        (report_hash,),
    )
    if not row:
        return {"status": "not_found", "report_hash": report_hash}

    score_hashes = row.get("score_hashes") or []
    verified_hashes = []
    for h in score_hashes:
        # Check if each referenced score hash exists in assessment_events
        ref = fetch_one(
            "SELECT id FROM assessment_events WHERE content_hash = %s",
            (h,),
        )
        verified_hashes.append({"hash": h, "resolved": ref is not None})

    all_resolved = all(v["resolved"] for v in verified_hashes) if verified_hashes else True

    return {
        "status": "verified" if all_resolved else "partial",
        "report_hash": report_hash,
        "entity_type": row["entity_type"],
        "entity_id": row["entity_id"],
        "template": row["template"],
        "lens": row.get("lens"),
        "lens_version": row.get("lens_version"),
        "methodology_version": row["methodology_version"],
        "generated_at": row["generated_at"].isoformat() if row.get("generated_at") else None,
        "score_hashes": verified_hashes,
        "cqi_hashes": row.get("cqi_hashes"),
        "all_hashes_resolved": all_resolved,
    }


def _extract_scores(report_data: dict) -> dict:
    """Extract all score values from report data for hashing."""
    scores = {}
    if report_data.get("score") is not None:
        scores["primary_score"] = round(float(report_data["score"]), 2)
    for key in ("categories", "category_scores"):
        if report_data.get(key):
            scores[key] = {k: round(float(v), 2) if isinstance(v, (int, float)) else v
                           for k, v in sorted(report_data[key].items())}
    if report_data.get("cqi_pairs"):
        scores["cqi_pairs"] = [
            {"asset": p["asset"], "protocol": p["protocol"], "cqi_score": round(float(p["cqi_score"]), 2)}
            for p in sorted(report_data["cqi_pairs"], key=lambda x: x["asset"])
        ]
    if report_data.get("holdings"):
        scores["holdings"] = [
            {"symbol": h.get("symbol", ""), "value_usd": round(float(h.get("value_usd", 0)), 2)}
            for h in sorted(report_data["holdings"], key=lambda x: x.get("symbol", ""))
        ]
    return scores


def _json_default(obj):
    """JSON serializer for non-standard types."""
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "__float__"):
        return float(obj)
    return str(obj)
