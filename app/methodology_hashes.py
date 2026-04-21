"""
Methodology Hash Registry
==========================
Stores immutable methodology definitions with SHA-256 content hashes.
Each methodology_id is write-once — to update a methodology, register
a new version ID (e.g. track_record_rules_v2).

On-chain anchoring is handled by the keeper (separate process); this
module only tracks committed status and tx hashes.
"""

import hashlib
import json
import logging
from app.database import fetch_all, fetch_one, execute

logger = logging.getLogger(__name__)


def register_methodology(methodology_id: str, content: str, description: str) -> str:
    """Register an immutable methodology. Returns content_hash. Raises if methodology_id already exists."""
    existing = fetch_one(
        "SELECT methodology_id FROM methodology_hashes WHERE methodology_id = %s",
        (methodology_id,),
    )
    if existing:
        raise ValueError(f"Methodology '{methodology_id}' already registered — create a new version ID instead")

    content_hash = hashlib.sha256(content.encode()).hexdigest()
    execute(
        """INSERT INTO methodology_hashes (methodology_id, content, content_hash, description)
           VALUES (%s, %s, %s, %s)""",
        (methodology_id, content, content_hash, description),
    )
    logger.info(f"Registered methodology '{methodology_id}' with hash {content_hash[:16]}...")
    return content_hash


def get_methodology(methodology_id: str) -> dict | None:
    """Get a single methodology by ID, including full content."""
    row = fetch_one(
        "SELECT * FROM methodology_hashes WHERE methodology_id = %s",
        (methodology_id,),
    )
    return dict(row) if row else None


def list_methodologies() -> list[dict]:
    """List all methodologies (without content — use get_methodology for full content)."""
    rows = fetch_all(
        """SELECT methodology_id, content_hash, description,
                  committed_on_chain_base, committed_on_chain_arbitrum,
                  registered_at
           FROM methodology_hashes
           ORDER BY registered_at DESC"""
    )
    return [dict(r) for r in rows] if rows else []


def compute_on_chain_entity_id(methodology: dict) -> str:
    """Deterministic bytes32 entityId for on-chain anchoring via publishReportHash."""
    canonical = methodology.get("methodology_id", "")
    try:
        import sha3
        h = sha3.keccak_256(canonical.encode()).hexdigest()
    except ImportError:
        h = hashlib.sha256(canonical.encode()).hexdigest()
    return "0x" + h
