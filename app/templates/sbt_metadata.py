"""
SBT Metadata Template
======================
Returns JSON (not HTML). ERC-721 compatible metadata for Basis Rating SBTs.
"""

import json

CANONICAL_BASE_URL = "https://basisprotocol.xyz"


def render(report_data: dict, lens_result: dict = None,
           report_hash: str = "", timestamp: str = "", format: str = "json") -> str:
    """Render SBT metadata as JSON string."""
    d = report_data
    entity_type = d.get("entity_type", "unknown")
    entity_id = d.get("entity_id", "")
    name = d.get("name") or d.get("symbol") or entity_id
    score = d.get("score")

    # Build proof URL
    if entity_type == "stablecoin":
        proof_url = f"{CANONICAL_BASE_URL}/proof/sii/{entity_id}"
        surface = "SII"
    elif entity_type == "protocol":
        proof_url = f"{CANONICAL_BASE_URL}/proof/psi/{entity_id}"
        surface = "PSI"
    else:
        proof_url = f"{CANONICAL_BASE_URL}/report/wallet/{entity_id}"
        surface = "WRG"

    # Confidence from report data
    confidence = "high"
    if d.get("coverage_quality"):
        confidence = d["coverage_quality"]

    metadata = {
        "name": f"Basis {surface} Score — {name}",
        "description": f"Attested {surface} risk score for {name}. "
                       f"Score: {score:.1f}/100. "
                       f"Methodology: {d.get('formula_version', '')}. "
                       f"Independently verifiable via report hash.",
        "image": f"{CANONICAL_BASE_URL}/api/reports/badge/{entity_type}/{entity_id}",
        "external_url": proof_url,
        "attributes": [
            {"trait_type": "Score", "value": round(float(score), 1) if score is not None else 0},
            {"trait_type": "Surface", "value": surface},
            {"trait_type": "Confidence", "value": confidence},
            {"trait_type": "Methodology Version", "value": d.get("formula_version", "")},
            {"trait_type": "Report Hash", "value": report_hash},
            {"trait_type": "Generated At", "value": timestamp},
        ],
        "report_hash": report_hash,
        "verification_url": f"{CANONICAL_BASE_URL}/api/reports/verify/{report_hash}",
    }

    if lens_result:
        metadata["attributes"].append({
            "trait_type": "Regulatory Lens",
            "value": lens_result.get("lens_id", ""),
        })
        metadata["attributes"].append({
            "trait_type": "Regulatory Classification",
            "value": "Eligible" if lens_result.get("overall_pass") else "Not Eligible",
        })

    return json.dumps(metadata, indent=2)
