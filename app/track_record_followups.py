"""
Track Record — Follow-up Evaluator
====================================
Automated 30/60/90-day outcome checks against frozen baselines.
Classifies outcomes conservatively — 'mixed' and 'insufficient_data'
are honest answers.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from app.database import fetch_all, fetch_one, get_cursor

logger = logging.getLogger(__name__)


def _serialize(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)


def _compute_followup_hash(entry_id: str, checkpoint: str,
                           current_snapshot: dict, outcome_category: str,
                           outcome_detail: dict) -> str:
    canonical = json.dumps({
        "entry_id": str(entry_id),
        "checkpoint": checkpoint,
        "current_snapshot": current_snapshot,
        "outcome_category": outcome_category,
        "outcome_detail": outcome_detail,
    }, sort_keys=True, separators=(",", ":"), default=_serialize)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _get_current_entity_state(entity_slug: str, index_name: str) -> dict:
    """Get the current state of an entity for comparison."""
    state = {"entity_slug": entity_slug, "index_name": index_name,
             "evaluated_at": datetime.now(timezone.utc).isoformat()}

    if index_name == "sii":
        row = fetch_one("SELECT overall_score, grade FROM scores WHERE stablecoin_id = %s", (entity_slug,))
        if row:
            state["score"] = float(row.get("overall_score") or 0)
            state["grade"] = row.get("grade")
        else:
            state["score"] = None
            state["dropped_from_coverage"] = True

    elif index_name == "psi":
        row = fetch_one(
            "SELECT overall_score, grade FROM psi_scores WHERE protocol_slug = %s ORDER BY scored_at DESC LIMIT 1",
            (entity_slug,),
        )
        if row:
            state["score"] = float(row.get("overall_score") or 0)
            state["grade"] = row.get("grade")
        else:
            state["score"] = None
            state["dropped_from_coverage"] = True

    elif index_name in ("lsti", "bri", "dohi", "vsri", "cxri", "tti"):
        row = fetch_one(
            "SELECT overall_score, confidence_tag FROM generic_index_scores WHERE index_id = %s AND entity_id = %s ORDER BY computed_at DESC LIMIT 1",
            (index_name, entity_slug),
        )
        if row:
            state["score"] = float(row.get("overall_score") or 0)
            state["confidence_tag"] = row.get("confidence_tag")
        else:
            state["score"] = None
            state["dropped_from_coverage"] = True

    elif index_name == "cross_domain":
        state["score"] = None

    return state


def _classify_outcome(entry: dict, baseline: dict, current: dict) -> tuple[str, dict]:
    """
    Classify the outcome of a track record entry.
    Returns (outcome_category, outcome_detail).

    Conservative classification:
    - 'validated': clear confirmation of the signal's direction
    - 'mixed': partial confirmation or ambiguous
    - 'not_borne_out': opposite of what was flagged
    - 'insufficient_data': can't determine (entity dropped, data gaps)
    """
    trigger_kind = entry.get("trigger_kind", "")
    trigger_detail = entry.get("trigger_detail", {})
    if isinstance(trigger_detail, str):
        trigger_detail = json.loads(trigger_detail)

    baseline_snap = baseline if isinstance(baseline, dict) else json.loads(baseline) if baseline else {}
    current_snap = current if isinstance(current, dict) else {}

    detail = {}

    # Entity dropped from coverage
    if current_snap.get("dropped_from_coverage"):
        return "insufficient_data", {"reason": "entity_dropped_from_coverage"}

    baseline_score = baseline_snap.get("score")
    current_score = current_snap.get("score")

    # No scores to compare
    if baseline_score is None or current_score is None:
        return "insufficient_data", {"reason": "missing_scores", "baseline": baseline_score, "current": current_score}

    score_delta = current_score - baseline_score
    detail["baseline_score"] = baseline_score
    detail["current_score"] = current_score
    detail["score_delta"] = round(score_delta, 2)

    # Grade change
    baseline_grade = baseline_snap.get("grade")
    current_grade = current_snap.get("grade")
    if baseline_grade and current_grade and baseline_grade != current_grade:
        detail["grade_change"] = f"{baseline_grade} → {current_grade}"

    if trigger_kind == "score_change":
        flagged_direction = trigger_detail.get("direction", "")
        flagged_delta = trigger_detail.get("delta", 0)

        if flagged_direction == "down":
            if score_delta < -5:
                return "validated", {**detail, "reason": "score continued declining"}
            elif score_delta > 5:
                return "not_borne_out", {**detail, "reason": "score recovered significantly"}
            else:
                return "mixed", {**detail, "reason": "score roughly stable after decline signal"}
        elif flagged_direction == "up":
            if score_delta > 5:
                return "validated", {**detail, "reason": "score continued improving"}
            elif score_delta < -5:
                return "not_borne_out", {**detail, "reason": "score reversed after improvement signal"}
            else:
                return "mixed", {**detail, "reason": "score roughly stable after improvement signal"}

    elif trigger_kind == "divergence":
        severity = trigger_detail.get("severity", "")
        direction = trigger_detail.get("direction", "")

        if direction == "deteriorating" and score_delta < -5:
            return "validated", {**detail, "reason": "deterioration signal confirmed by score decline"}
        elif direction == "deteriorating" and score_delta > 5:
            return "not_borne_out", {**detail, "reason": "deterioration signal not confirmed — score improved"}
        elif abs(score_delta) <= 5:
            return "mixed", {**detail, "reason": "divergence signal with minimal score movement"}
        else:
            return "mixed", {**detail, "reason": "ambiguous outcome"}

    elif trigger_kind == "coherence_drop":
        # For system-level coherence drops, check if issues resolved
        try:
            latest = fetch_one("SELECT issues_found FROM coherence_reports ORDER BY created_at DESC LIMIT 1")
            if latest:
                current_issues = latest.get("issues_found", 0)
                detail["current_issues"] = current_issues
                if current_issues == 0:
                    return "validated", {**detail, "reason": "coherence issues resolved"}
                else:
                    return "mixed", {**detail, "reason": "coherence issues persist"}
        except Exception:
            pass
        return "insufficient_data", {**detail, "reason": "cannot determine coherence resolution"}

    return "mixed", {**detail, "reason": "unhandled trigger_kind"}


def evaluate_pending_followups() -> dict:
    """
    Find entries needing 30/60/90-day followups and evaluate them.
    Returns summary.
    """
    now = datetime.now(timezone.utc)
    results = {"evaluated": 0, "by_checkpoint": {}, "by_outcome": {}}

    checkpoints = [
        ("30d", 30),
        ("60d", 60),
        ("90d", 90),
    ]

    for checkpoint_name, days in checkpoints:
        try:
            # Find entries old enough that don't have this checkpoint yet
            entries = fetch_all("""
                SELECT e.entry_id, e.entity_slug, e.index_name, e.trigger_kind,
                       e.trigger_detail, e.baseline_snapshot, e.triggered_at
                FROM track_record_entries e
                LEFT JOIN track_record_followups f
                    ON e.entry_id = f.entry_id AND f.checkpoint = %s
                WHERE e.triggered_at <= NOW() - INTERVAL '%s days'
                  AND f.followup_id IS NULL
                LIMIT 100
            """, (checkpoint_name, days))

            if not entries:
                continue

            for entry in entries:
                try:
                    entity = entry["entity_slug"]
                    index_name = entry["index_name"]

                    # Get current entity state
                    current = _get_current_entity_state(entity, index_name)

                    # Parse baseline
                    baseline = entry.get("baseline_snapshot", {})
                    if isinstance(baseline, str):
                        baseline = json.loads(baseline)

                    # Classify outcome
                    outcome_category, outcome_detail = _classify_outcome(
                        entry, baseline, current,
                    )

                    # Compute hash
                    content_hash = _compute_followup_hash(
                        str(entry["entry_id"]), checkpoint_name,
                        current, outcome_category, outcome_detail,
                    )

                    # Insert followup
                    with get_cursor() as cur:
                        cur.execute("""
                            INSERT INTO track_record_followups
                            (entry_id, checkpoint, evaluated_at, current_snapshot,
                             outcome_category, outcome_detail, content_hash)
                            VALUES (%s, %s, NOW(), %s, %s, %s, %s)
                            ON CONFLICT (entry_id, checkpoint) DO NOTHING
                        """, (
                            str(entry["entry_id"]), checkpoint_name,
                            json.dumps(current, default=_serialize),
                            outcome_category,
                            json.dumps(outcome_detail, default=_serialize),
                            content_hash,
                        ))

                    results["evaluated"] += 1
                    results["by_checkpoint"][checkpoint_name] = results["by_checkpoint"].get(checkpoint_name, 0) + 1
                    results["by_outcome"][outcome_category] = results["by_outcome"].get(outcome_category, 0) + 1

                except Exception as e:
                    logger.warning(f"Followup evaluation failed for {entry.get('entry_id')}: {e}")

        except Exception as e:
            logger.warning(f"Followup query failed for {checkpoint_name}: {e}")

    logger.info(f"Track record followups: {results}")
    return results
