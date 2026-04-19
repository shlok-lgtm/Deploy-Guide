"""
Track Record — Auto-Entry Writer
==================================
Detects qualifying signals from existing attested state and logs them
as track_record_entries. Runs at the end of the slow cycle.

Active rules (based on available source tables):
  A — Material score change (>=10 points in 7 days, high confidence)
  B — Divergence signal (from divergence_signals table)
  C — Coherence drop (from coherence_reports, issues_found > 0)

Deferred rules (source tables don't exist yet):
  D — Oracle stress events (oracle_stress_events table missing)
  E — Governance proposal edits (governance_proposal_snapshots missing)
  F — Contract upgrades (contract_upgrade_history missing)
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


def _compute_content_hash(entity_slug: str, trigger_kind: str,
                          trigger_detail: dict, triggered_at: str,
                          baseline_snapshot: dict) -> str:
    """SHA-256 of canonical representation for idempotency."""
    canonical = json.dumps({
        "entity_slug": entity_slug,
        "trigger_kind": trigger_kind,
        "trigger_detail": trigger_detail,
        "triggered_at": str(triggered_at),
        "baseline_snapshot": baseline_snapshot,
    }, sort_keys=True, separators=(",", ":"), default=_serialize)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _get_entity_baseline(entity_slug: str, index_name: str) -> dict:
    """Capture the entity's current scored state as a frozen baseline."""
    baseline = {"entity_slug": entity_slug, "index_name": index_name, "captured_at": datetime.now(timezone.utc).isoformat()}

    if index_name == "sii":
        row = fetch_one(
            "SELECT * FROM scores WHERE stablecoin_id = %s", (entity_slug,)
        )
        if row:
            baseline["score"] = float(row.get("overall_score") or 0)
            baseline["grade"] = row.get("grade")
            baseline["peg_score"] = float(row.get("peg_score") or 0)
            baseline["liquidity_score"] = float(row.get("liquidity_score") or 0)
            baseline["structural_score"] = float(row.get("structural_score") or 0)
            baseline["component_count"] = row.get("component_count")
            baseline["formula_version"] = row.get("formula_version")

    elif index_name == "psi":
        row = fetch_one(
            "SELECT * FROM psi_scores WHERE protocol_slug = %s ORDER BY scored_at DESC LIMIT 1",
            (entity_slug,),
        )
        if row:
            baseline["score"] = float(row.get("overall_score") or 0)
            baseline["grade"] = row.get("grade")
            baseline["category_scores"] = row.get("category_scores")

    elif index_name in ("lsti", "bri", "dohi", "vsri", "cxri", "tti"):
        row = fetch_one(
            "SELECT * FROM generic_index_scores WHERE index_id = %s AND entity_id = %s ORDER BY computed_at DESC LIMIT 1",
            (index_name, entity_slug),
        )
        if row:
            baseline["score"] = float(row.get("overall_score") or 0)
            baseline["confidence"] = row.get("confidence")
            baseline["confidence_tag"] = row.get("confidence_tag")

    return baseline


def _get_state_root() -> str:
    """Get the latest state root hash from daily pulses, if available."""
    try:
        row = fetch_one("SELECT summary FROM daily_pulses ORDER BY pulse_date DESC LIMIT 1")
        if row and row.get("summary"):
            summary = row["summary"]
            if isinstance(summary, str):
                summary = json.loads(summary)
            return summary.get("state_root", "")
    except Exception:
        pass
    return ""


def _is_domain_stale(domain: str) -> bool:
    """Check if a domain is flagged as stale in the latest coherence report."""
    try:
        row = fetch_one("SELECT details FROM coherence_reports ORDER BY created_at DESC LIMIT 1")
        if row and row.get("details"):
            details = row["details"]
            if isinstance(details, str):
                details = json.loads(details)
            if isinstance(details, list):
                for d in details:
                    if isinstance(d, dict) and d.get("domain") == domain and d.get("status") in ("stale", "error"):
                        return True
    except Exception:
        pass
    return False


def _entry_exists(content_hash: str) -> bool:
    """Check if an entry with this content_hash already exists."""
    row = fetch_one(
        "SELECT 1 FROM track_record_entries WHERE content_hash = %s",
        (content_hash,),
    )
    return row is not None


def _insert_entry(entry: dict):
    """Insert a track record entry."""
    with get_cursor() as cur:
        cur.execute(
            """INSERT INTO track_record_entries
               (entry_type, entity_slug, index_name, trigger_kind,
                trigger_detail, triggered_at, state_root_at_trigger,
                source_attestation_domain, baseline_snapshot,
                content_hash, created_at)
               VALUES ('auto', %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
               ON CONFLICT (content_hash) DO NOTHING""",
            (
                entry["entity_slug"], entry["index_name"], entry["trigger_kind"],
                json.dumps(entry["trigger_detail"], default=_serialize),
                entry["triggered_at"], entry.get("state_root", ""),
                entry.get("source_domain", ""),
                json.dumps(entry["baseline_snapshot"], default=_serialize),
                entry["content_hash"],
            ),
        )


# =============================================================================
# Rule implementations
# =============================================================================

def _rule_a_score_changes() -> list[dict]:
    """Rule A: Material score change (>=10 points in 7 days)."""
    entries = []

    # SII score changes
    try:
        rows = fetch_all("""
            SELECT s.stablecoin_id, s.overall_score as current_score,
                   h.overall_score as prev_score,
                   s.overall_score - h.overall_score as delta
            FROM scores s
            JOIN score_history h ON s.stablecoin_id = h.stablecoin
            WHERE h.score_date = CURRENT_DATE - 7
              AND ABS(s.overall_score - h.overall_score) >= 10
        """)
        for row in (rows or []):
            entity = row["stablecoin_id"]
            delta = float(row["delta"])
            baseline = _get_entity_baseline(entity, "sii")
            trigger_detail = {
                "current_score": float(row["current_score"]),
                "previous_score": float(row["prev_score"]),
                "delta": delta,
                "window_days": 7,
                "direction": "up" if delta > 0 else "down",
            }
            content_hash = _compute_content_hash(
                entity, "score_change", trigger_detail,
                datetime.now(timezone.utc).isoformat()[:10], baseline,
            )
            entries.append({
                "entity_slug": entity, "index_name": "sii",
                "trigger_kind": "score_change",
                "trigger_detail": trigger_detail,
                "triggered_at": datetime.now(timezone.utc),
                "baseline_snapshot": baseline,
                "source_domain": "sii_components",
                "content_hash": content_hash,
            })
    except Exception as e:
        logger.warning(f"Rule A (SII) failed: {e}")

    # PSI score changes
    try:
        rows = fetch_all("""
            SELECT p1.protocol_slug,
                   p1.overall_score as current_score,
                   p2.overall_score as prev_score,
                   p1.overall_score - p2.overall_score as delta
            FROM (SELECT DISTINCT ON (protocol_slug) protocol_slug, overall_score
                  FROM psi_scores ORDER BY protocol_slug, scored_at DESC) p1
            JOIN (SELECT DISTINCT ON (protocol_slug) protocol_slug, overall_score
                  FROM psi_scores WHERE scored_at <= NOW() - INTERVAL '7 days'
                  ORDER BY protocol_slug, scored_at DESC) p2
            ON p1.protocol_slug = p2.protocol_slug
            WHERE ABS(p1.overall_score - p2.overall_score) >= 10
        """)
        for row in (rows or []):
            entity = row["protocol_slug"]
            delta = float(row["delta"])
            baseline = _get_entity_baseline(entity, "psi")
            trigger_detail = {
                "current_score": float(row["current_score"]),
                "previous_score": float(row["prev_score"]),
                "delta": delta, "window_days": 7,
                "direction": "up" if delta > 0 else "down",
            }
            content_hash = _compute_content_hash(
                entity, "score_change", trigger_detail,
                datetime.now(timezone.utc).isoformat()[:10], baseline,
            )
            entries.append({
                "entity_slug": entity, "index_name": "psi",
                "trigger_kind": "score_change",
                "trigger_detail": trigger_detail,
                "triggered_at": datetime.now(timezone.utc),
                "baseline_snapshot": baseline,
                "source_domain": "psi_components",
                "content_hash": content_hash,
            })
    except Exception as e:
        logger.warning(f"Rule A (PSI) failed: {e}")

    return entries


def _rule_b_divergence() -> list[dict]:
    """Rule B: Divergence signals added since last run."""
    entries = []
    try:
        # Get signals from the last 2 hours (one slow cycle window)
        rows = fetch_all("""
            SELECT detector_name, entity_type, entity_id, signal_direction,
                   magnitude, severity, detail, cycle_timestamp
            FROM divergence_signals
            WHERE created_at >= NOW() - INTERVAL '2 hours'
              AND severity IN ('critical', 'alert')
        """)
        for row in (rows or []):
            entity = row.get("entity_id", "")
            if not entity:
                continue

            # Determine index from entity type
            index_name = "sii" if row.get("entity_type") == "stablecoin" else "psi"
            baseline = _get_entity_baseline(entity, index_name)

            trigger_detail = {
                "detector": row.get("detector_name"),
                "direction": row.get("signal_direction"),
                "magnitude": float(row.get("magnitude") or 0),
                "severity": row.get("severity"),
                "detail": row.get("detail"),
            }
            content_hash = _compute_content_hash(
                entity, "divergence", trigger_detail,
                str(row.get("cycle_timestamp", "")), baseline,
            )
            entries.append({
                "entity_slug": entity, "index_name": index_name,
                "trigger_kind": "divergence",
                "trigger_detail": trigger_detail,
                "triggered_at": row.get("cycle_timestamp") or datetime.now(timezone.utc),
                "baseline_snapshot": baseline,
                "source_domain": "divergence_signals",
                "content_hash": content_hash,
            })
    except Exception as e:
        logger.warning(f"Rule B (divergence) failed: {e}")

    return entries


def _rule_c_coherence_drop() -> list[dict]:
    """Rule C: Coherence report with issues."""
    entries = []
    try:
        row = fetch_one("""
            SELECT id, domains_checked, issues_found, details, created_at
            FROM coherence_reports
            WHERE created_at >= NOW() - INTERVAL '2 hours'
              AND issues_found > 0
            ORDER BY created_at DESC LIMIT 1
        """)
        if row and row.get("issues_found", 0) > 0:
            details = row.get("details", [])
            if isinstance(details, str):
                details = json.loads(details)

            # Create one entry for the coherence drop event
            trigger_detail = {
                "domains_checked": row.get("domains_checked"),
                "issues_found": row.get("issues_found"),
                "details": details[:5] if isinstance(details, list) else details,
            }
            baseline = {"coherence_report_id": row.get("id"), "captured_at": datetime.now(timezone.utc).isoformat()}
            content_hash = _compute_content_hash(
                "system", "coherence_drop", trigger_detail,
                str(row.get("created_at", "")), baseline,
            )
            entries.append({
                "entity_slug": "system", "index_name": "cross_domain",
                "trigger_kind": "coherence_drop",
                "trigger_detail": trigger_detail,
                "triggered_at": row.get("created_at") or datetime.now(timezone.utc),
                "baseline_snapshot": baseline,
                "source_domain": "coherence_reports",
                "content_hash": content_hash,
            })
    except Exception as e:
        logger.warning(f"Rule C (coherence) failed: {e}")

    return entries


# =============================================================================
# Main entry point
# =============================================================================

def detect_and_log_entries() -> dict:
    """
    Run all trigger rules and log qualifying entries.
    Called at the end of the slow cycle.
    Returns summary of entries logged.
    """
    state_root = _get_state_root()
    all_entries = []
    skipped_stale = 0
    skipped_duplicate = 0

    # Run each rule
    for rule_name, rule_fn, source_domain in [
        ("score_change", _rule_a_score_changes, "sii_components"),
        ("divergence", _rule_b_divergence, "divergence_signals"),
        ("coherence_drop", _rule_c_coherence_drop, "coherence_reports"),
    ]:
        try:
            # Freshness gate
            if _is_domain_stale(source_domain):
                logger.info(f"Skipped track_record rule {rule_name}: source domain {source_domain} stale")
                skipped_stale += 1
                continue

            entries = rule_fn()
            for entry in entries:
                entry["state_root"] = state_root
                if _entry_exists(entry["content_hash"]):
                    skipped_duplicate += 1
                    continue
                try:
                    _insert_entry(entry)
                    all_entries.append(entry["trigger_kind"])
                except Exception as e:
                    logger.warning(f"Failed to insert track_record entry: {e}")
        except Exception as e:
            logger.warning(f"Track record rule {rule_name} failed: {e}")

    logger.info(
        f"Track record: {len(all_entries)} entries logged, "
        f"{skipped_duplicate} duplicates skipped, "
        f"{skipped_stale} rules skipped (stale domain)"
    )

    return {
        "entries_logged": len(all_entries),
        "by_trigger": {k: all_entries.count(k) for k in set(all_entries)},
        "skipped_duplicate": skipped_duplicate,
        "skipped_stale": skipped_stale,
    }
