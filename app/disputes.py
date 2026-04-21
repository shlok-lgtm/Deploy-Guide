"""
Disputes — Score Dispute Lifecycle
====================================
Manages the submission, counter-evidence, and resolution lifecycle
for disputes against scored entities. Each lifecycle step produces
a content-hashed transition for on-chain anchoring.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

from app.database import fetch_all, fetch_one, execute, get_cursor

logger = logging.getLogger(__name__)


def _serialize(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)


def _compute_transition_hash(dispute_id: str, transition_index: int, payload: dict) -> str:
    """SHA-256 of canonical JSON of {dispute_id, transition_index, payload}."""
    canonical = json.dumps({
        "dispute_id": str(dispute_id),
        "transition_index": transition_index,
        "payload": payload,
    }, sort_keys=True, separators=(",", ":"), default=_serialize)
    return hashlib.sha256(canonical.encode()).hexdigest()


def compute_on_chain_entity_id(transition: dict) -> str:
    """Compute deterministic bytes32 entityId for on-chain anchoring.

    Uses keccak256 (with sha256 fallback) of canonical
    {dispute_id, transition_index, transition_kind}.
    Returns 0x-prefixed hex string.
    """
    canonical = json.dumps({
        "dispute_id": str(transition.get("dispute_id", "")),
        "transition_index": transition.get("transition_index", 0),
        "transition_kind": transition.get("transition_kind", ""),
    }, sort_keys=True, separators=(",", ":"))
    try:
        import sha3
        h = sha3.keccak_256(canonical.encode()).hexdigest()
    except ImportError:
        h = hashlib.sha256(canonical.encode()).hexdigest()
    return "0x" + h


def submit_dispute(entity_slug: str, submitter_identifier: str, submitter_type: str,
                   submission_text: str, submission_evidence_url: str = None,
                   disputed_score_content_hash: str = None) -> str:
    """Submit a new dispute. Creates the dispute row and transition_index=0 submission transition.

    Returns dispute_id (UUID string).
    """
    with get_cursor() as cur:
        # Insert dispute
        cur.execute(
            """INSERT INTO disputes
               (entity_slug, disputed_score_content_hash, submitter_identifier,
                submitter_type, submission_text, submission_evidence_url, status, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, 'submitted', NOW())
               RETURNING dispute_id""",
            (entity_slug, disputed_score_content_hash, submitter_identifier,
             submitter_type, submission_text, submission_evidence_url),
        )
        row = cur.fetchone()
        dispute_id = str(row[0]) if isinstance(row, tuple) else str(row["dispute_id"])

        # Build submission transition payload
        payload = {
            "entity_slug": entity_slug,
            "submitter_identifier": submitter_identifier,
            "submitter_type": submitter_type,
            "submission_text": submission_text,
            "submission_evidence_url": submission_evidence_url,
            "disputed_score_content_hash": disputed_score_content_hash,
        }
        content_hash = _compute_transition_hash(dispute_id, 0, payload)

        cur.execute(
            """INSERT INTO dispute_transitions
               (dispute_id, transition_index, transition_kind, transition_payload,
                content_hash, created_at)
               VALUES (%s::uuid, 0, 'submission', %s, %s, NOW())""",
            (dispute_id, json.dumps(payload, default=_serialize), content_hash),
        )

    logger.info(f"Dispute submitted: {dispute_id} for entity {entity_slug}")
    return dispute_id


def issue_counter_evidence(dispute_id: str, payload_dict: dict, author: str) -> str:
    """Add counter-evidence transition (transition_index=1).

    Updates dispute status to 'counter_evidence_issued'.
    Returns transition_id (UUID string).
    """
    payload = {
        **payload_dict,
        "author": author,
    }
    content_hash = _compute_transition_hash(dispute_id, 1, payload)

    with get_cursor() as cur:
        cur.execute(
            """INSERT INTO dispute_transitions
               (dispute_id, transition_index, transition_kind, transition_payload,
                content_hash, created_at)
               VALUES (%s::uuid, 1, 'counter_evidence', %s, %s, NOW())
               RETURNING transition_id""",
            (dispute_id, json.dumps(payload, default=_serialize), content_hash),
        )
        row = cur.fetchone()
        transition_id = str(row[0]) if isinstance(row, tuple) else str(row["transition_id"])

        cur.execute(
            """UPDATE disputes SET status = 'counter_evidence_issued'
               WHERE dispute_id = %s::uuid""",
            (dispute_id,),
        )

    logger.info(f"Counter-evidence issued for dispute {dispute_id}")
    return transition_id


def resolve_dispute(dispute_id: str, resolution_category: str,
                    resolution_narrative: str, author: str) -> str:
    """Add resolution transition (transition_index=2).

    Updates dispute: status='resolved', resolved_at=NOW(), resolution fields.
    Returns transition_id (UUID string).
    """
    payload = {
        "resolution_category": resolution_category,
        "resolution_narrative": resolution_narrative,
        "author": author,
    }
    content_hash = _compute_transition_hash(dispute_id, 2, payload)

    with get_cursor() as cur:
        cur.execute(
            """INSERT INTO dispute_transitions
               (dispute_id, transition_index, transition_kind, transition_payload,
                content_hash, created_at)
               VALUES (%s::uuid, 2, 'resolution', %s, %s, NOW())
               RETURNING transition_id""",
            (dispute_id, json.dumps(payload, default=_serialize), content_hash),
        )
        row = cur.fetchone()
        transition_id = str(row[0]) if isinstance(row, tuple) else str(row["transition_id"])

        cur.execute(
            """UPDATE disputes
               SET status = 'resolved',
                   resolved_at = NOW(),
                   resolution_category = %s,
                   resolution_narrative = %s
               WHERE dispute_id = %s::uuid""",
            (resolution_category, resolution_narrative, dispute_id),
        )

    logger.info(f"Dispute resolved: {dispute_id} — {resolution_category}")
    return transition_id
