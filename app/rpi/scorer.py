"""
RPI Scorer
===========
Scores protocols on the Risk Posture Index using the same scoring engine
as SII and PSI. Follows the PSI pipeline pattern exactly:
  1. Load raw component values from collector tables + seed data
  2. Normalize each to 0-100 using RPI-specific thresholds
  3. Call score_entity() from the generic scoring engine
  4. Compute inputs_hash
  5. Store in rpi_scores table
  6. Attest state
"""

import hashlib
import json
import logging
import math
from datetime import datetime, timezone, timedelta

from app.database import execute, fetch_all, fetch_one
from app.index_definitions.rpi_v01 import RPI_V01_DEFINITION, TARGET_PROTOCOLS
from app.scoring_engine import score_entity

logger = logging.getLogger(__name__)


# =============================================================================
# RPI-specific normalization (pre-scoring)
# =============================================================================

def normalize_spend_ratio(ratio_pct: float) -> float:
    """0% → 0, >=8% → 100, linear between."""
    if ratio_pct is None:
        return None
    if ratio_pct <= 0:
        return 0.0
    if ratio_pct >= 8.0:
        return 100.0
    return round((ratio_pct / 8.0) * 100.0, 2)


def normalize_vendor_diversity(count: int, has_external: bool = False) -> float:
    """0→0, 1→30, 2→60, 3+→80, 3+ with external→100."""
    if count is None:
        return None
    if count == 0:
        return 0.0
    if count == 1:
        return 30.0
    if count == 2:
        return 60.0
    # 3+
    if has_external:
        return 100.0
    return 80.0


def normalize_parameter_velocity(changes_per_month: float) -> float:
    """0→0, 1-3→50, 4-8→80, 9+→100."""
    if changes_per_month is None:
        return None
    if changes_per_month == 0:
        return 0.0
    if changes_per_month <= 3:
        return 50.0
    if changes_per_month <= 8:
        return 80.0
    return 100.0


def normalize_parameter_recency(days_since: float) -> float:
    """<=7→100, <=14→80, <=30→60, <=60→40, <=90→20, >90→0."""
    if days_since is None:
        return None
    if days_since <= 7:
        return 100.0
    if days_since <= 14:
        return 80.0
    if days_since <= 30:
        return 60.0
    if days_since <= 60:
        return 40.0
    if days_since <= 90:
        return 20.0
    return 0.0


def normalize_incident_severity(weighted_count: float) -> float:
    """100 for 0 incidents, scale down with 12-month decay.

    Score = 100 * exp(-0.5 * weighted_count)
    A single major incident (weight 3) → score ≈ 22
    A single moderate incident (weight 1) → score ≈ 61
    """
    if weighted_count is None:
        return None
    if weighted_count <= 0:
        return 100.0
    return round(100.0 * math.exp(-0.5 * weighted_count), 2)


def normalize_recovery_ratio(ratio: float) -> float:
    """>=90%→100, >=70%→80, >=50%→60, >=30%→40, <30%→0.
    If no incidents (ratio is None), score is 100.
    """
    if ratio is None:
        return 100.0  # No incidents = perfect recovery
    if ratio >= 0.90:
        return 100.0
    if ratio >= 0.70:
        return 80.0
    if ratio >= 0.50:
        return 60.0
    if ratio >= 0.30:
        return 40.0
    return 0.0


def normalize_external_scoring(level: int) -> float:
    """0 (none)→0, 1 (references)→40, 2 (API integration)→70, 3 (bound in decisions)→100."""
    if level is None:
        return None
    levels = {0: 0.0, 1: 40.0, 2: 70.0, 3: 100.0}
    return levels.get(level, 0.0)


def normalize_governance_health(participation_pct: float) -> float:
    """>=30%→100, >=20%→80, >=10%→60, >=5%→40, <5%→0."""
    if participation_pct is None:
        return None
    if participation_pct >= 30:
        return 100.0
    if participation_pct >= 20:
        return 80.0
    if participation_pct >= 10:
        return 60.0
    if participation_pct >= 5:
        return 40.0
    return 0.0


# =============================================================================
# Raw value extraction from database
# =============================================================================

SEVERITY_WEIGHTS = {
    "critical": 5.0,
    "major": 3.0,
    "moderate": 1.0,
    "minor": 0.5,
}


def extract_raw_values(protocol_slug: str) -> dict:
    """Extract raw values for all RPI components from the database.

    Combines data from:
    - governance_proposals table (spend_ratio, governance_health)
    - parameter_changes table (parameter_velocity, parameter_recency)
    - risk_incidents table (incident_severity, recovery_ratio)
    - rpi_components table (seed data for manual components)
    """
    raw = {}
    now = datetime.now(timezone.utc)

    # --- spend_ratio: risk spending / annual revenue ---
    try:
        # Get annual revenue from DeFiLlama (via PSI raw_values if available)
        psi_row = fetch_one("""
            SELECT raw_values FROM psi_scores
            WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1
        """, (protocol_slug,))

        annual_revenue = None
        if psi_row and psi_row.get("raw_values"):
            rv = psi_row["raw_values"]
            rev_30d = rv.get("revenue_30d") or rv.get("fees_30d")
            if rev_30d:
                annual_revenue = float(rev_30d) * 12

        # Sum risk-related budget amounts from governance proposals (last 12 months)
        budget_row = fetch_one("""
            SELECT COALESCE(SUM(budget_amount), 0) AS total_risk_budget
            FROM governance_proposals
            WHERE protocol_slug = %s
              AND is_risk_related = TRUE
              AND budget_amount IS NOT NULL
              AND created_at >= NOW() - INTERVAL '12 months'
        """, (protocol_slug,))

        risk_budget = float(budget_row["total_risk_budget"]) if budget_row else 0

        if annual_revenue and annual_revenue > 0:
            spend_ratio_pct = (risk_budget / annual_revenue) * 100
            raw["spend_ratio"] = normalize_spend_ratio(spend_ratio_pct)
        else:
            # Fall back to seed data
            seed = _get_seed_value(protocol_slug, "spend_ratio")
            if seed is not None:
                raw["spend_ratio"] = seed
    except Exception as e:
        logger.debug(f"spend_ratio extraction failed for {protocol_slug}: {e}")

    # --- vendor_diversity: from seed data (manual) ---
    try:
        seed = _get_seed_value(protocol_slug, "vendor_diversity")
        if seed is not None:
            raw["vendor_diversity"] = seed
    except Exception as e:
        logger.debug(f"vendor_diversity extraction failed for {protocol_slug}: {e}")

    # --- parameter_velocity: changes per month in last 90 days ---
    try:
        param_row = fetch_one("""
            SELECT COUNT(*) AS change_count
            FROM parameter_changes
            WHERE protocol_slug = %s
              AND changed_at >= NOW() - INTERVAL '90 days'
        """, (protocol_slug,))

        if param_row:
            count_90d = int(param_row["change_count"])
            changes_per_month = count_90d / 3.0  # 90 days = 3 months
            raw["parameter_velocity"] = normalize_parameter_velocity(changes_per_month)
        else:
            seed = _get_seed_value(protocol_slug, "parameter_velocity")
            if seed is not None:
                raw["parameter_velocity"] = seed
    except Exception as e:
        logger.debug(f"parameter_velocity extraction failed for {protocol_slug}: {e}")

    # --- parameter_recency: days since most recent change ---
    try:
        recent_row = fetch_one("""
            SELECT MAX(changed_at) AS latest
            FROM parameter_changes
            WHERE protocol_slug = %s
        """, (protocol_slug,))

        if recent_row and recent_row.get("latest"):
            latest = recent_row["latest"]
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            days_since = (now - latest).total_seconds() / 86400
            raw["parameter_recency"] = normalize_parameter_recency(days_since)
        else:
            seed = _get_seed_value(protocol_slug, "parameter_recency")
            if seed is not None:
                raw["parameter_recency"] = seed
    except Exception as e:
        logger.debug(f"parameter_recency extraction failed for {protocol_slug}: {e}")

    # --- incident_severity: weighted sum of incidents in last 12 months ---
    try:
        incidents = fetch_all("""
            SELECT severity, severity_weight, incident_date
            FROM risk_incidents
            WHERE protocol_slug = %s
              AND incident_date >= CURRENT_DATE - INTERVAL '12 months'
        """, (protocol_slug,))

        if incidents:
            # Apply 12-month exponential decay
            weighted_total = 0.0
            for inc in incidents:
                weight = float(inc.get("severity_weight") or SEVERITY_WEIGHTS.get(inc["severity"], 1.0))
                inc_date = inc["incident_date"]
                if hasattr(inc_date, "toordinal"):
                    days_ago = (now.date() - inc_date).days
                else:
                    days_ago = 0
                decay = math.exp(-days_ago / 365.0)  # 12-month half-life
                weighted_total += weight * decay
            raw["incident_severity"] = normalize_incident_severity(weighted_total)
        else:
            # Check seed data; if no seed and no incidents, score is 100 (clean record)
            seed = _get_seed_value(protocol_slug, "incident_severity")
            raw["incident_severity"] = seed if seed is not None else 100.0
    except Exception as e:
        logger.debug(f"incident_severity extraction failed for {protocol_slug}: {e}")

    # --- recovery_ratio: aggregate recovery across incidents ---
    try:
        recovery_row = fetch_one("""
            SELECT
                COALESCE(SUM(funds_at_risk_usd), 0) AS total_at_risk,
                COALESCE(SUM(funds_recovered_usd), 0) AS total_recovered
            FROM risk_incidents
            WHERE protocol_slug = %s
              AND incident_date >= CURRENT_DATE - INTERVAL '24 months'
        """, (protocol_slug,))

        if recovery_row and float(recovery_row["total_at_risk"]) > 0:
            ratio = float(recovery_row["total_recovered"]) / float(recovery_row["total_at_risk"])
            raw["recovery_ratio"] = normalize_recovery_ratio(ratio)
        else:
            # Check seed; no incidents means perfect score
            seed = _get_seed_value(protocol_slug, "recovery_ratio")
            raw["recovery_ratio"] = seed if seed is not None else 100.0
    except Exception as e:
        logger.debug(f"recovery_ratio extraction failed for {protocol_slug}: {e}")

    # --- external_scoring: from seed data (manual) ---
    try:
        seed = _get_seed_value(protocol_slug, "external_scoring")
        if seed is not None:
            raw["external_scoring"] = seed
    except Exception as e:
        logger.debug(f"external_scoring extraction failed for {protocol_slug}: {e}")

    # --- documentation_depth: from seed data (manual) ---
    try:
        seed = _get_seed_value(protocol_slug, "documentation_depth")
        if seed is not None:
            raw["documentation_depth"] = seed
    except Exception as e:
        logger.debug(f"documentation_depth extraction failed for {protocol_slug}: {e}")

    # --- governance_health: average participation rate from recent proposals ---
    try:
        health_row = fetch_one("""
            SELECT AVG(participation_rate) AS avg_participation
            FROM governance_proposals
            WHERE protocol_slug = %s
              AND participation_rate IS NOT NULL
              AND created_at >= NOW() - INTERVAL '6 months'
              AND state = 'closed'
        """, (protocol_slug,))

        if health_row and health_row.get("avg_participation") is not None:
            raw["governance_health"] = normalize_governance_health(
                float(health_row["avg_participation"])
            )
        else:
            seed = _get_seed_value(protocol_slug, "governance_health")
            if seed is not None:
                raw["governance_health"] = seed
    except Exception as e:
        logger.debug(f"governance_health extraction failed for {protocol_slug}: {e}")

    return raw


def _get_seed_value(protocol_slug: str, component_id: str) -> float | None:
    """Get the latest seed/manual value for a component from rpi_components table."""
    row = fetch_one("""
        SELECT normalized_score FROM rpi_components
        WHERE protocol_slug = %s AND component_id = %s
        ORDER BY collected_at DESC LIMIT 1
    """, (protocol_slug, component_id))
    if row and row.get("normalized_score") is not None:
        return float(row["normalized_score"])
    return None


# =============================================================================
# Score a single protocol
# =============================================================================

def score_protocol(slug: str) -> dict | None:
    """Score a single protocol on the RPI.

    Follows the PSI pipeline pattern:
    1. Extract raw values (already normalized 0-100)
    2. Call score_entity()
    3. Return result dict ready for storage
    """
    raw_values = extract_raw_values(slug)

    if not raw_values:
        logger.warning(f"RPI: No data available for {slug}")
        return None

    # The raw_values are already normalized to 0-100 by our custom normalizers.
    # score_entity will apply `direct` normalization (identity function) and
    # aggregate by category weights.
    result = score_entity(RPI_V01_DEFINITION, raw_values)

    # Look up protocol name from PSI scores
    name_row = fetch_one(
        "SELECT protocol_name FROM psi_scores WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1",
        (slug,),
    )
    protocol_name = name_row["protocol_name"] if name_row else slug.replace("-", " ").title()

    result["protocol_slug"] = slug
    result["protocol_name"] = protocol_name
    result["raw_values"] = raw_values

    return result


# =============================================================================
# Score from stored raw values (for verification)
# =============================================================================

def score_protocol_from_raw(slug: str, raw_values: dict) -> dict | None:
    """Re-derive RPI score from stored raw values (for verification endpoint)."""
    if not raw_values:
        return None
    return score_entity(RPI_V01_DEFINITION, raw_values)


# =============================================================================
# Full scoring pipeline
# =============================================================================

def run_rpi_scoring(protocols: list[str] | None = None) -> list[dict]:
    """Score all target protocols on the RPI.

    Follows the same pattern as run_psi_scoring():
    1. Collect data (already done by daily collector)
    2. Score each protocol
    3. Store results with inputs_hash
    4. Generate assessment events on significant changes
    5. Attest state
    """
    slugs = protocols or TARGET_PROTOCOLS
    results = []

    for slug in slugs:
        logger.info(f"RPI scoring: {slug}")
        result = score_protocol(slug)
        if not result:
            continue

        # Compute inputs_hash (same pattern as PSI)
        raw_for_storage = result["raw_values"]
        raw_canonical = json.dumps(raw_for_storage, sort_keys=True, default=str)
        inputs_hash = "0x" + hashlib.sha256(raw_canonical.encode()).hexdigest()

        # Store in rpi_scores table (same schema as psi_scores)
        try:
            execute("""
                INSERT INTO rpi_scores (protocol_slug, protocol_name, overall_score, grade,
                    category_scores, component_scores, raw_values, formula_version, inputs_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT rpi_scores_protocol_slug_scored_date_key
                DO UPDATE SET
                    protocol_name = EXCLUDED.protocol_name,
                    overall_score = EXCLUDED.overall_score,
                    grade = EXCLUDED.grade,
                    category_scores = EXCLUDED.category_scores,
                    component_scores = EXCLUDED.component_scores,
                    raw_values = EXCLUDED.raw_values,
                    inputs_hash = EXCLUDED.inputs_hash,
                    computed_at = NOW()
            """, (
                result["protocol_slug"],
                result["protocol_name"],
                result["overall_score"],
                None,  # grade deprecated
                json.dumps(result["category_scores"]),
                json.dumps(result["component_scores"]),
                json.dumps(raw_for_storage, default=str),
                f"rpi-{result['version']}",
                inputs_hash,
            ))
        except Exception as e:
            logger.error(f"Failed to store RPI score for {slug}: {e}")
            continue

        results.append(result)
        logger.info(
            f"  {result['protocol_name']}: {result['overall_score']} "
            f"- {result['components_available']}/{result['components_total']} components"
        )

        # Auto-generate assessment event on significant score change (same as PSI)
        try:
            prev_row = fetch_one("""
                SELECT overall_score FROM rpi_scores
                WHERE protocol_slug = %s AND scored_date < CURRENT_DATE
                ORDER BY computed_at DESC LIMIT 1
            """, (slug,))

            if prev_row and prev_row.get("overall_score"):
                prev_score = float(prev_row["overall_score"])
                current_score = result["overall_score"]
                delta = current_score - prev_score

                if abs(delta) >= 3.0:
                    direction = "declined" if delta < 0 else "improved"
                    if abs(delta) >= 10:
                        severity = "critical"
                    elif abs(delta) >= 5:
                        severity = "alert"
                    else:
                        severity = "notable"

                    try:
                        from app.agent.store import store_assessment
                        event = {
                            "wallet_address": f"protocol:{slug}",
                            "chain": "multi",
                            "trigger_type": "rpi_score_change",
                            "trigger_detail": {
                                "entity_type": "protocol",
                                "entity_id": slug,
                                "title": f"{result['protocol_name']} RPI {direction} {abs(delta):.1f} pts",
                                "description": f"RPI score moved from {prev_score:.1f} to {current_score:.1f} ({delta:+.1f}).",
                                "previous_score": prev_score,
                                "current_score": current_score,
                                "delta": round(delta, 2),
                            },
                            "wallet_risk_score": current_score,
                            "wallet_risk_grade": None,
                            "wallet_risk_score_prev": prev_score,
                            "concentration_hhi": None,
                            "concentration_hhi_prev": None,
                            "coverage_ratio": None,
                            "total_stablecoin_value": None,
                            "holdings_snapshot": [],
                            "severity": severity,
                            "broadcast": severity in ("alert", "critical"),
                            "content_hash": inputs_hash,
                            "methodology_version": f"rpi-{result['version']}",
                        }
                        event_id = store_assessment(event)
                        if event_id:
                            logger.info(f"RPI event: {slug} {severity} ({delta:+.1f} pts)")
                    except Exception as ae:
                        logger.debug(f"RPI event store failed for {slug}: {ae}")
        except Exception as e:
            logger.debug(f"RPI event generation error for {slug}: {e}")

    # State attestation (domain: rpi_components)
    try:
        from app.state_attestation import attest_state
        if results:
            attest_state(
                "rpi_components",
                [{"slug": r.get("protocol_slug", ""), "score": r.get("overall_score")} for r in results if isinstance(r, dict)],
            )
    except Exception as ae:
        logger.debug(f"RPI attestation skipped: {ae}")

    logger.info(f"RPI scoring complete: {len(results)} protocols scored")
    return results
