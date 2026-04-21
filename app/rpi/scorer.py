"""
RPI Scorer
============
Computes base and lensed RPI scores for protocols.

Base score: 5 automated, ungameable components — always computed and stored.
Lensed score: optional overlays computed on-the-fly when requested.

Uses the generic scoring engine (score_entity) for the base calculation,
then applies lens blending on top.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone

from app.database import execute, fetch_one, fetch_all
from app.index_definitions.rpi_v2 import (
    RPI_V2_DEFINITION, RPI_LENSES, LENS_BLEND, RPI_TARGET_PROTOCOLS,
)
from app.scoring_engine import score_entity
from app.scoring import score_to_grade

logger = logging.getLogger(__name__)

RPI_VERSION = RPI_V2_DEFINITION["version"]

# =============================================================================
# Normalization helpers for components not handled by the generic engine
# =============================================================================

def _normalize_spend_ratio(pct: float) -> float:
    """0 at 0%, 100 at >=8%."""
    if pct <= 0:
        return 0.0
    if pct >= 8.0:
        return 100.0
    return (pct / 8.0) * 100.0


def _normalize_parameter_velocity(changes_per_month: int) -> float:
    """0 (0 changes), 50 (1-3), 80 (4-8), 100 (9+)."""
    if changes_per_month <= 0:
        return 0.0
    if changes_per_month <= 3:
        return 50.0
    if changes_per_month <= 8:
        return 80.0
    return 100.0


def _normalize_parameter_recency(days: int | None) -> float:
    """100 (<=7d), 80 (<=14d), 60 (<=30d), 40 (<=60d), 20 (<=90d), 0 (>90d)."""
    if days is None:
        return 0.0
    if days <= 7:
        return 100.0
    if days <= 14:
        return 80.0
    if days <= 30:
        return 60.0
    if days <= 60:
        return 40.0
    if days <= 90:
        return 20.0
    return 0.0


def _normalize_incident_severity(slug: str) -> float:
    """100 (0 weighted incidents), scale down by weighted count with 12-month decay.
    Only incidents with reviewed=true are included.
    """
    rows = fetch_all("""
        SELECT severity, funds_at_risk_usd,
               EXTRACT(EPOCH FROM (NOW() - incident_date)) / 86400 AS days_ago
        FROM risk_incidents
        WHERE protocol_slug = %s AND reviewed = TRUE
          AND incident_date >= NOW() - INTERVAL '12 months'
        ORDER BY incident_date DESC
    """, (slug,))

    if not rows:
        return 100.0

    severity_weights = {
        "critical": 40,
        "major": 25,
        "moderate": 10,
        "minor": 5,
    }

    weighted_sum = 0.0
    for row in rows:
        sev = row.get("severity", "minor")
        base_weight = severity_weights.get(sev, 5)
        days = row.get("days_ago", 0) or 0
        # Exponential decay: more recent incidents weigh more
        decay = max(0.1, 1.0 - (days / 365.0))
        weighted_sum += base_weight * decay

    # Scale: 0 → 100, penalty increases with weighted sum
    # 100 points of weighted sum → score 0
    score = max(0.0, 100.0 - weighted_sum)
    return round(score, 2)


def _normalize_governance_health(participation_pct: float | None) -> float:
    """100 (>=30%), 80 (>=20%), 60 (>=10%), 40 (>=5%), 0 (<5%)."""
    if participation_pct is None:
        return 0.0
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
# Lens component normalization
# =============================================================================

def _normalize_vendor_diversity(count: int) -> float:
    """0 (none), 30 (1), 60 (2), 80 (3+), 100 (3+ with external scoring)."""
    if count <= 0:
        return 0.0
    if count == 1:
        return 30.0
    if count == 2:
        return 60.0
    return 80.0  # 3+ without external scoring; 100 is set at lens application


def _normalize_recovery_ratio(pct: float | None) -> float:
    """100 (>=90% or no incidents), 80 (>=70%), 60 (>=50%), 40 (>=30%), 0 (<30%)."""
    if pct is None:
        return 100.0  # no incidents = perfect recovery
    if pct >= 90:
        return 100.0
    if pct >= 70:
        return 80.0
    if pct >= 50:
        return 60.0
    if pct >= 30:
        return 40.0
    return 0.0


# =============================================================================
# Raw value assembly
# =============================================================================

def _get_governance_participation(slug: str) -> float | None:
    """Get average participation rate from recent governance proposals."""
    row = fetch_one("""
        SELECT AVG(participation_rate) AS avg_participation
        FROM governance_proposals
        WHERE protocol_slug = %s
          AND participation_rate IS NOT NULL
          AND created_at >= NOW() - INTERVAL '90 days'
    """, (slug,))
    if row and row["avg_participation"] is not None:
        return float(row["avg_participation"])
    return None


def _get_risk_spend_ratio(slug: str, annualized_revenue: float | None) -> float | None:
    """Compute risk spend as % of revenue from budget proposals."""
    if not annualized_revenue or annualized_revenue <= 0:
        return None

    row = fetch_one("""
        SELECT SUM(budget_amount_usd) AS total_budget
        FROM governance_proposals
        WHERE protocol_slug = %s
          AND is_risk_related = TRUE
          AND budget_amount_usd IS NOT NULL
          AND created_at >= NOW() - INTERVAL '365 days'
    """, (slug,))

    if row and row["total_budget"]:
        return (float(row["total_budget"]) / annualized_revenue) * 100.0
    return None


def collect_raw_values(slug: str, revenue_cache: dict[str, float] | None = None) -> dict:
    """Collect all raw component values for a protocol's RPI base score."""
    raw = {}

    # spend_ratio
    annualized_revenue = (revenue_cache or {}).get(slug)
    spend = _get_risk_spend_ratio(slug, annualized_revenue)
    if spend is not None:
        raw["spend_ratio"] = spend

    # parameter_velocity (changes per month in last 30 days)
    from app.rpi.parameter_collector import get_parameter_velocity, get_parameter_recency
    velocity = get_parameter_velocity(slug, days=30)
    raw["parameter_velocity"] = velocity

    # parameter_recency (days since last change)
    recency = get_parameter_recency(slug)
    if recency is not None:
        raw["parameter_recency"] = recency

    # incident_severity (computed directly from DB)
    raw["incident_severity"] = _normalize_incident_severity(slug)

    # governance_health (average participation rate)
    participation = _get_governance_participation(slug)
    if participation is not None:
        raw["governance_health"] = participation

    return raw


# =============================================================================
# Base scoring
# =============================================================================

def score_rpi_base(slug: str, raw_values: dict) -> dict:
    """Compute the base RPI score for a protocol.

    Uses custom normalization (not the generic engine) because RPI
    components have specific threshold-based normalization that doesn't
    map cleanly to the generic normalizers.

    Returns dict with overall_score, component_scores, raw_values, grade, etc.
    """
    component_scores = {}

    # Normalize each component
    if "spend_ratio" in raw_values:
        component_scores["spend_ratio"] = round(_normalize_spend_ratio(raw_values["spend_ratio"]), 2)

    if "parameter_velocity" in raw_values:
        component_scores["parameter_velocity"] = round(
            _normalize_parameter_velocity(raw_values["parameter_velocity"]), 2
        )

    if "parameter_recency" in raw_values:
        component_scores["parameter_recency"] = round(
            _normalize_parameter_recency(raw_values["parameter_recency"]), 2
        )

    if "incident_severity" in raw_values:
        # Already normalized in collect_raw_values
        component_scores["incident_severity"] = round(raw_values["incident_severity"], 2)

    if "governance_health" in raw_values:
        component_scores["governance_health"] = round(
            _normalize_governance_health(raw_values["governance_health"]), 2
        )

    # Weighted sum with renormalization for missing components
    weights = {
        "spend_ratio": 0.20,
        "parameter_velocity": 0.25,
        "parameter_recency": 0.15,
        "incident_severity": 0.20,
        "governance_health": 0.20,
    }

    total_score = 0.0
    weight_used = 0.0
    for comp_id, weight in weights.items():
        if comp_id in component_scores:
            total_score += component_scores[comp_id] * weight
            weight_used += weight

    overall = round(total_score / weight_used, 2) if weight_used > 0 else 0.0

    # V7.3 confidence tag fields — base components only. Lens variants are out
    # of scope for persistence and do not contribute to the canonical score.
    from app.scoring_engine import compute_confidence_tag
    from app.index_definitions.rpi_v2 import RPI_V2_DEFINITION as _RPI_DEF
    _comp_to_cat = {
        cid: cdef["category"]
        for cid, cdef in _RPI_DEF["components"].items()
    }
    _populated_cats = {
        _comp_to_cat[cid] for cid in component_scores if cid in _comp_to_cat
    }
    _all_cats = set(_RPI_DEF["categories"].keys())
    missing_categories = sorted(_all_cats - _populated_cats)
    components_populated = len(component_scores)
    components_total = len(_RPI_DEF["components"])
    component_coverage = round(components_populated / max(components_total, 1), 4)
    _conf = compute_confidence_tag(
        len(_populated_cats), len(_all_cats),
        component_coverage, missing_categories,
    )

    return {
        "index_id": "rpi",
        "version": RPI_VERSION,
        "protocol_slug": slug,
        "overall_score": overall,
        "grade": score_to_grade(overall),
        "component_scores": component_scores,
        "raw_values": raw_values,
        "components_available": components_populated,
        "components_total": components_total,
        "coverage": component_coverage,
        "components_populated": components_populated,
        "component_coverage": component_coverage,
        "missing_categories": missing_categories,
        "confidence": _conf["confidence"],
        "confidence_tag": _conf["tag"],
    }


# =============================================================================
# Lens scoring (computed on-the-fly)
# =============================================================================

def _load_lens_components(slug: str, lens_ids: list[str]) -> dict[str, dict]:
    """Load lens component values from the rpi_components table.

    Returns dict of lens_id -> {component_id -> normalized_score}.
    """
    if not lens_ids:
        return {}

    results = {}
    for lens_id in lens_ids:
        if lens_id not in RPI_LENSES:
            continue

        lens_def = RPI_LENSES[lens_id]
        comp_scores = {}

        for comp_id in lens_def["components"]:
            row = fetch_one("""
                SELECT normalized_score
                FROM rpi_components
                WHERE protocol_slug = %s
                  AND component_id = %s
                  AND component_type = 'lens'
                  AND lens_id = %s
                ORDER BY collected_at DESC
                LIMIT 1
            """, (slug, comp_id, lens_id))

            if row and row["normalized_score"] is not None:
                comp_scores[comp_id] = float(row["normalized_score"])

        results[lens_id] = comp_scores

    return results


def compute_lensed_score(base_score: float, lens_ids: list[str],
                         lens_components: dict[str, dict]) -> dict:
    """Compute the lensed RPI score.

    RPI_lensed = (1 - LENS_BLEND) * base + LENS_BLEND * lens_weighted_average

    If only some lenses are requested, the unrequested lens weight
    stays with the base.
    """
    if not lens_ids or not lens_components:
        return {
            "rpi_lensed": None,
            "lens_scores": {},
            "lens_blend_used": 0.0,
        }

    # Compute per-lens scores
    lens_scores = {}
    total_lens_weight = 0.0
    weighted_lens_sum = 0.0

    for lens_id in lens_ids:
        if lens_id not in RPI_LENSES or lens_id not in lens_components:
            continue

        lens_def = RPI_LENSES[lens_id]
        comps = lens_components[lens_id]

        if not comps:
            continue

        # Weighted average within the lens
        score_sum = 0.0
        w_used = 0.0
        for comp_id, comp_def in lens_def["components"].items():
            if comp_id in comps:
                score_sum += comps[comp_id] * comp_def["weight"]
                w_used += comp_def["weight"]

        if w_used > 0:
            lens_score = round(score_sum / w_used, 2)
            lens_scores[lens_id] = {
                "score": lens_score,
                "components": comps,
            }
            # Use the original weights from the definition for blending
            original_weights = {
                "risk_organization": 0.18,  # 0.10 + 0.08
                "risk_infrastructure": 0.07,
                "risk_transparency": 0.05,
            }
            w = original_weights.get(lens_id, 0.10)
            weighted_lens_sum += lens_score * w
            total_lens_weight += w

    if total_lens_weight <= 0:
        return {
            "rpi_lensed": None,
            "lens_scores": lens_scores,
            "lens_blend_used": 0.0,
        }

    # Normalize lens weights to the LENS_BLEND portion
    lens_avg = weighted_lens_sum / total_lens_weight
    # Actual blend fraction: scale LENS_BLEND by fraction of total lens weight used
    total_possible_weight = 0.18 + 0.07 + 0.05  # 0.30
    blend_fraction = LENS_BLEND * (total_lens_weight / total_possible_weight)

    rpi_lensed = round((1.0 - blend_fraction) * base_score + blend_fraction * lens_avg, 2)

    return {
        "rpi_lensed": rpi_lensed,
        "lens_scores": lens_scores,
        "lens_blend_used": round(blend_fraction, 4),
    }


# =============================================================================
# Store results
# =============================================================================

def store_rpi_score(slug: str, result: dict):
    """Store the base RPI score in the database."""
    raw_canonical = json.dumps(result["raw_values"], sort_keys=True, default=str)
    inputs_hash = "0x" + hashlib.sha256(raw_canonical.encode()).hexdigest()

    protocol_name = _get_protocol_name(slug)

    execute("""
        INSERT INTO rpi_scores
            (protocol_slug, protocol_name, overall_score, grade,
             component_scores, raw_values, inputs_hash, methodology_version,
             confidence, confidence_tag, component_coverage,
             components_populated, components_total, missing_categories)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT rpi_scores_protocol_slug_scored_date_key
        DO UPDATE SET
            protocol_name = EXCLUDED.protocol_name,
            overall_score = EXCLUDED.overall_score,
            grade = EXCLUDED.grade,
            component_scores = EXCLUDED.component_scores,
            raw_values = EXCLUDED.raw_values,
            inputs_hash = EXCLUDED.inputs_hash,
            confidence = EXCLUDED.confidence,
            confidence_tag = EXCLUDED.confidence_tag,
            component_coverage = EXCLUDED.component_coverage,
            components_populated = EXCLUDED.components_populated,
            components_total = EXCLUDED.components_total,
            missing_categories = EXCLUDED.missing_categories,
            computed_at = NOW()
    """, (
        slug, protocol_name, result["overall_score"], result["grade"],
        json.dumps(result["component_scores"]),
        json.dumps(result["raw_values"], default=str),
        inputs_hash, RPI_VERSION,
        result.get("confidence"),
        result.get("confidence_tag"),
        result.get("component_coverage"),
        result.get("components_populated"),
        result.get("components_total"),
        json.dumps(result.get("missing_categories") or []),
    ))

    # Store history
    execute("""
        INSERT INTO rpi_score_history
            (protocol_slug, overall_score, component_scores, methodology_version)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (protocol_slug, score_date) DO UPDATE SET
            overall_score = EXCLUDED.overall_score,
            component_scores = EXCLUDED.component_scores
    """, (
        slug, result["overall_score"],
        json.dumps(result["component_scores"]),
        RPI_VERSION,
    ))

    # Store individual component readings
    for comp_id, score in result["component_scores"].items():
        raw_val = result["raw_values"].get(comp_id)
        execute("""
            INSERT INTO rpi_components
                (protocol_slug, component_id, component_type, raw_value,
                 normalized_score, source_type, data_source, collected_at)
            VALUES (%s, %s, 'base', %s, %s, 'automated', %s, NOW())
        """, (
            slug, comp_id, raw_val, score,
            RPI_V2_DEFINITION["components"].get(comp_id, {}).get("data_source", "unknown"),
        ))

    return inputs_hash


def _get_protocol_name(slug: str) -> str:
    """Get protocol display name from PSI scores or slug."""
    row = fetch_one(
        "SELECT protocol_name FROM psi_scores WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1",
        (slug,),
    )
    if row and row.get("protocol_name"):
        return row["protocol_name"]
    return slug.replace("-", " ").title()


# =============================================================================
# Phase 1: Auto-write lens components from existing data sources
# =============================================================================

def _sync_lens_vendor_diversity(slug: str) -> float | None:
    """Write vendor_diversity lens component from governance_forum_posts data.

    Counts distinct risk vendors mentioned in forum posts for this protocol.
    The forum_scraper already collects vendor mentions — we just need to
    read the count and write the normalized score to rpi_components.
    """
    try:
        row = fetch_one("""
            SELECT COUNT(DISTINCT vendor_name) AS vendor_count
            FROM (
                SELECT UNNEST(vendor_mentions) AS vendor_name
                FROM governance_forum_posts
                WHERE protocol_slug = %s
                  AND collected_at >= NOW() - INTERVAL '365 days'
                  AND vendor_mentions IS NOT NULL
                  AND ARRAY_LENGTH(vendor_mentions, 1) > 0
            ) sub
        """, (slug,))

        if row and row.get("vendor_count") is not None:
            count = int(row["vendor_count"])
            normalized = _normalize_vendor_diversity(count)

            # Write to rpi_components for the lens system to pick up
            execute("""
                INSERT INTO rpi_components
                    (protocol_slug, component_id, component_type, lens_id,
                     raw_value, normalized_score, source_type, data_source, collected_at)
                VALUES (%s, 'vendor_diversity', 'lens', 'risk_organization',
                        %s, %s, 'automated', 'forum_scraper', NOW())
            """, (slug, count, normalized))

            logger.info(f"RPI lens vendor_diversity {slug}: {count} vendors → {normalized}")
            return normalized
    except Exception as e:
        logger.debug(f"RPI vendor_diversity sync failed for {slug}: {e}")
    return None


def _sync_lens_documentation_depth(slug: str) -> float | None:
    """Write documentation_depth lens component from rpi_doc_scores data.

    The docs_scorer already scores protocols on a 5-criterion rubric and stores
    results in rpi_doc_scores. We just read the total score and write it
    as a lens component.
    """
    try:
        row = fetch_one("""
            SELECT SUM(score) AS total_score
            FROM rpi_doc_scores
            WHERE protocol_slug = %s
              AND scored_at >= NOW() - INTERVAL '30 days'
        """, (slug,))

        if row and row.get("total_score") is not None:
            total_score = float(row["total_score"])
            # rpi_doc_scores is 0-100 (5 criteria × 20 pts each)
            normalized = min(100, max(0, total_score))

            execute("""
                INSERT INTO rpi_components
                    (protocol_slug, component_id, component_type, lens_id,
                     raw_value, normalized_score, source_type, data_source, collected_at)
                VALUES (%s, 'documentation_depth', 'lens', 'risk_transparency',
                        %s, %s, 'automated', 'docs_scorer', NOW())
            """, (slug, total_score, normalized))

            logger.info(f"RPI lens documentation_depth {slug}: {total_score} → {normalized}")
            return normalized
    except Exception as e:
        logger.debug(f"RPI documentation_depth sync failed for {slug}: {e}")
    return None


def sync_all_lens_components(protocols: list[str]) -> dict:
    """Sync all auto-derived lens components for all RPI protocols.

    Called during RPI scoring to ensure lens data is fresh.
    Returns dict of {slug: {component_id: normalized_score}}.
    """
    results = {}
    for slug in protocols:
        slug_results = {}

        vd = _sync_lens_vendor_diversity(slug)
        if vd is not None:
            slug_results["vendor_diversity"] = vd

        dd = _sync_lens_documentation_depth(slug)
        if dd is not None:
            slug_results["documentation_depth"] = dd

        if slug_results:
            results[slug] = slug_results

    return results


# =============================================================================
# Orchestrator
# =============================================================================

def run_rpi_scoring() -> list[dict]:
    """Score all RPI target protocols. Returns list of result dicts."""
    from app.rpi.revenue_collector import get_all_revenues

    # Pre-fetch all revenues (avoids repeated API calls)
    revenue_cache = get_all_revenues()

    # Phase 1: Sync auto-derived lens components from existing data sources
    try:
        lens_results = sync_all_lens_components(list(RPI_TARGET_PROTOCOLS))
        if lens_results:
            logger.info(f"RPI lens sync: updated {len(lens_results)} protocols")
    except Exception as e:
        logger.warning(f"RPI lens sync failed: {e}")

    results = []
    for slug in RPI_TARGET_PROTOCOLS:
        try:
            raw_values = collect_raw_values(slug, revenue_cache)
            result = score_rpi_base(slug, raw_values)
            result["protocol_name"] = _get_protocol_name(slug)

            inputs_hash = store_rpi_score(slug, result)
            result["inputs_hash"] = inputs_hash

            results.append(result)
            logger.info(
                f"RPI {slug}: {result['overall_score']} "
                f"({result['components_available']}/{result['components_total']} components)"
            )
        except Exception as e:
            logger.error(f"RPI scoring failed for {slug}: {e}")

    return results
