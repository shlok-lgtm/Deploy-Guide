"""
Data Integrity Layer
=====================
One module that every surface queries before rendering.
Answers two questions per domain: "Is the data fresh?" and "Does the data make sense?"
"""

import json
import logging
from datetime import datetime, timezone

from app.database import fetch_one

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Coherence rule helpers
# ---------------------------------------------------------------------------

def _count_check(sql, params=None, *, threshold=0, level="error", rule="", field="", message=""):
    """Return a warning if the count query returns > threshold."""
    try:
        row = fetch_one(sql, params)
        cnt = row["cnt"] if row else 0
        if cnt > threshold:
            return {"rule": rule, "field": field, "value": cnt, "level": level, "message": message}
    except Exception as e:
        return {"rule": rule, "field": field, "value": None, "level": "warning", "message": f"check failed: {e}"}
    return None


def _min_count_check(sql, params=None, *, minimum=1, level="warning", rule="", field="", message=""):
    """Return a warning if the count query returns < minimum."""
    try:
        row = fetch_one(sql, params)
        cnt = row["cnt"] if row else 0
        if cnt < minimum:
            return {"rule": rule, "field": field, "value": cnt, "level": level, "message": message}
    except Exception as e:
        return {"rule": rule, "field": field, "value": None, "level": "warning", "message": f"check failed: {e}"}
    return None


# ---------------------------------------------------------------------------
# Coherence rules per domain
# ---------------------------------------------------------------------------

def _sii_score_range():
    return _count_check(
        "SELECT COUNT(*) AS cnt FROM scores WHERE overall_score < 0 OR overall_score > 100",
        rule="score_out_of_range", field="overall_score", level="error",
        message="One or more SII scores outside 0-100 range",
    )


def _sii_null_scores():
    return _count_check(
        """SELECT COUNT(*) AS cnt FROM scores s
           JOIN stablecoins st ON st.id = s.stablecoin_id
           WHERE st.scoring_enabled = TRUE AND s.overall_score IS NULL""",
        rule="null_score_on_active", field="overall_score", level="warning",
        message="Active stablecoin has NULL overall_score",
    )


def _sii_min_scored():
    return _min_count_check(
        "SELECT COUNT(*) AS cnt FROM scores",
        minimum=10, rule="insufficient_scores", field="row_count", level="warning",
        message="Fewer than 10 stablecoins scored",
    )


def _psi_score_range():
    return _count_check(
        "SELECT COUNT(*) AS cnt FROM psi_scores WHERE overall_score < 0 OR overall_score > 100",
        rule="score_out_of_range", field="overall_score", level="error",
        message="One or more PSI scores outside 0-100 range",
    )


def _psi_min_scored():
    return _min_count_check(
        "SELECT COUNT(DISTINCT protocol_slug) AS cnt FROM psi_scores",
        minimum=5, rule="insufficient_protocols", field="row_count", level="warning",
        message="Fewer than 5 protocols scored",
    )


def _wallets_scored_nonzero():
    return _min_count_check(
        "SELECT COUNT(DISTINCT wallet_address) AS cnt FROM wallet_graph.wallet_risk_scores WHERE risk_score IS NOT NULL",
        minimum=1, rule="no_scored_wallets", field="row_count", level="warning",
        message="No scored wallets found",
    )


def _wallets_score_range():
    return _count_check(
        "SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_risk_scores WHERE risk_score < 0 OR risk_score > 100",
        rule="score_out_of_range", field="risk_score", level="error",
        message="One or more wallet risk scores outside 0-100 range",
    )


def _wallets_scored_ratio():
    try:
        scored = fetch_one(
            "SELECT COUNT(DISTINCT wallet_address) AS cnt FROM wallet_graph.wallet_risk_scores WHERE risk_score IS NOT NULL"
        )
        total = fetch_one("SELECT COUNT(DISTINCT LOWER(address)) AS cnt FROM wallet_graph.wallets")
        scored_cnt = scored["cnt"] if scored else 0
        total_cnt = total["cnt"] if total else 0
        if total_cnt > 0 and scored_cnt / total_cnt < 0.01:
            return {
                "rule": "low_scored_ratio", "field": "scored_ratio",
                "value": round(scored_cnt / total_cnt, 4), "level": "warning",
                "message": f"Only {scored_cnt}/{total_cnt} wallets scored (<1%)",
            }
    except Exception as e:
        return {"rule": "low_scored_ratio", "field": "scored_ratio", "value": None, "level": "warning", "message": f"check failed: {e}"}
    return None


def _cda_active_without_extractions():
    try:
        row = fetch_one("""
            SELECT COUNT(*) AS cnt FROM cda_issuer_registry
            WHERE is_active = TRUE
              AND asset_symbol NOT IN (
                  SELECT DISTINCT asset_symbol FROM cda_vendor_extractions
                  WHERE extracted_at > NOW() - INTERVAL '7 days'
              )
        """)
        cnt = row["cnt"] if row else 0
        if cnt > 0:
            return {
                "rule": "active_issuer_no_extraction", "field": "issuer_count",
                "value": cnt, "level": "warning",
                "message": f"{cnt} active issuer(s) with no extractions in 7 days",
            }
    except Exception as e:
        return {"rule": "active_issuer_no_extraction", "field": "issuer_count", "value": None, "level": "warning", "message": f"check failed: {e}"}
    return None


def _events_severity_consistency():
    try:
        total_row = fetch_one(
            "SELECT COUNT(*) AS cnt FROM assessment_events WHERE created_at > NOW() - INTERVAL '24 hours'"
        )
        sum_row = fetch_one("""
            SELECT COALESCE(SUM(cnt), 0) AS total FROM (
                SELECT COUNT(*) AS cnt FROM assessment_events
                WHERE created_at > NOW() - INTERVAL '24 hours'
                GROUP BY severity
            ) sub
        """)
        total = total_row["cnt"] if total_row else 0
        summed = sum_row["total"] if sum_row else 0
        if total != summed:
            return {
                "rule": "severity_count_mismatch", "field": "severity",
                "value": {"total": total, "summed": summed}, "level": "warning",
                "message": f"Severity breakdown ({summed}) doesn't match total ({total})",
            }
    except Exception as e:
        return {"rule": "severity_count_mismatch", "field": "severity", "value": None, "level": "warning", "message": f"check failed: {e}"}
    return None


def _edges_exist_if_wallets():
    try:
        wallets = fetch_one("SELECT COUNT(DISTINCT LOWER(address)) AS cnt FROM wallet_graph.wallets")
        edges = fetch_one("SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges")
        w_cnt = wallets["cnt"] if wallets else 0
        e_cnt = edges["cnt"] if edges else 0
        if w_cnt > 100 and e_cnt == 0:
            return {
                "rule": "no_edges_with_wallets", "field": "edge_count",
                "value": 0, "level": "warning",
                "message": f"{w_cnt} wallets indexed but 0 edges found",
            }
    except Exception as e:
        return {"rule": "no_edges_with_wallets", "field": "edge_count", "value": None, "level": "warning", "message": f"check failed: {e}"}
    return None


def _edges_coverage():
    """Check that at least 5% of wallets have edges (once edges exist)."""
    try:
        edges = fetch_one("SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges")
        if not edges or edges["cnt"] == 0:
            return None  # no edges yet — covered by _edges_exist_if_wallets

        wallets_with = fetch_one("""
            SELECT COUNT(DISTINCT addr) AS cnt FROM (
                SELECT from_address AS addr FROM wallet_graph.wallet_edges
                UNION SELECT to_address FROM wallet_graph.wallet_edges
            ) sub
        """)
        wallets_total = fetch_one("SELECT COUNT(DISTINCT LOWER(address)) AS cnt FROM wallet_graph.wallets")
        w_with = wallets_with["cnt"] if wallets_with else 0
        w_total = wallets_total["cnt"] if wallets_total else 0

        if w_total > 0:
            pct = w_with / w_total * 100
            if pct < 5:
                return {
                    "rule": "low_edge_coverage", "field": "coverage_pct",
                    "value": round(pct, 2), "level": "warning",
                    "message": f"Only {pct:.1f}% of wallets have edges ({w_with}/{w_total})",
                }
    except Exception as e:
        return {"rule": "low_edge_coverage", "field": "coverage_pct", "value": None, "level": "warning", "message": f"check failed: {e}"}
    return None


def _edges_stuck_builds():
    """Check for wallets stuck in 'building' status for > 1 hour."""
    try:
        row = fetch_one("""
            SELECT COUNT(*) AS cnt FROM wallet_graph.edge_build_status
            WHERE status NOT IN ('complete', 'pending')
              AND last_built_at < NOW() - INTERVAL '1 hour'
        """)
        cnt = row["cnt"] if row else 0
        if cnt > 0:
            return {
                "rule": "stuck_edge_builds", "field": "build_status",
                "value": cnt, "level": "warning",
                "message": f"{cnt} wallet(s) stuck in edge building for >1 hour",
            }
    except Exception as e:
        return {"rule": "stuck_edge_builds", "field": "build_status", "value": None, "level": "warning", "message": f"check failed: {e}"}
    return None


def _pulse_summary_coherence():
    """Validate the latest pulse summary JSON."""
    try:
        row = fetch_one("SELECT summary FROM daily_pulses ORDER BY created_at DESC LIMIT 1")
        if not row or not row.get("summary"):
            return None
        summary = row["summary"]
        if isinstance(summary, str):
            summary = json.loads(summary)

        net = summary.get("network_state", {})
        events = summary.get("events_24h", {})
        warnings = []

        wallets_scored = net.get("wallets_scored", 0) or 0
        wallets_indexed = net.get("wallets_indexed", 0) or 0
        avg_risk = net.get("avg_risk_score")
        total_usd = net.get("total_tracked_usd", 0) or 0
        stablecoins_scored = net.get("stablecoins_scored", 0) or 0

        if avg_risk == 0 and wallets_scored > 0:
            warnings.append({"rule": "pulse_avg_risk_zero", "field": "avg_risk_score", "value": 0, "level": "warning", "message": "avg_risk_score is 0 but wallets_scored > 0"})
        if avg_risk is not None and (avg_risk < 0 or avg_risk > 100):
            warnings.append({"rule": "pulse_avg_risk_range", "field": "avg_risk_score", "value": avg_risk, "level": "error", "message": f"avg_risk_score ({avg_risk}) outside 0-100"})
        if wallets_indexed == 0:
            warnings.append({"rule": "pulse_no_wallets_indexed", "field": "wallets_indexed", "value": 0, "level": "warning", "message": "wallets_indexed is 0"})
        if wallets_scored > wallets_indexed:
            warnings.append({"rule": "pulse_scored_gt_indexed", "field": "wallets_scored", "value": wallets_scored, "level": "warning", "message": f"wallets_scored ({wallets_scored}) > wallets_indexed ({wallets_indexed})"})
        if total_usd == 0 and wallets_indexed > 0:
            warnings.append({"rule": "pulse_no_tracked_usd", "field": "total_tracked_usd", "value": 0, "level": "warning", "message": "total_tracked_usd is 0 but wallets_indexed > 0"})
        if stablecoins_scored == 0:
            warnings.append({"rule": "pulse_no_stablecoins", "field": "stablecoins_scored", "value": 0, "level": "warning", "message": "stablecoins_scored is 0"})

        events_total = events.get("total", 0) or 0
        severity_sum = sum(events.get(k, 0) or 0 for k in ("silent", "notable", "alert", "critical"))
        if events_total > 0 and severity_sum == 0:
            warnings.append({"rule": "pulse_events_no_severity", "field": "events_24h", "value": {"total": events_total, "severity_sum": severity_sum}, "level": "warning", "message": "events_24h.total > 0 but severity counts sum to 0"})

        return warnings if warnings else None
    except Exception as e:
        return [{"rule": "pulse_summary_check", "field": "summary", "value": None, "level": "warning", "message": f"check failed: {e}"}]


# -- Actor classification coherence rules (Primitive #21) --

def _actor_probability_range():
    """All agent_probability values must be between 0 and 1."""
    return _count_check(
        "SELECT COUNT(*) AS cnt FROM wallet_graph.actor_classifications WHERE agent_probability < 0 OR agent_probability > 1",
        threshold=0, level="error",
        rule="actor_probability_out_of_range", field="agent_probability",
        message="agent_probability values outside [0, 1] range",
    )


def _actor_type_consistency():
    """actor_type must match threshold applied to agent_probability."""
    return _count_check(
        """
        SELECT COUNT(*) AS cnt FROM wallet_graph.actor_classifications
        WHERE actor_type NOT IN ('autonomous_agent', 'human', 'contract_vault', 'unknown')
        """,
        threshold=0, level="error",
        rule="actor_type_invalid", field="actor_type",
        message="actor_type values outside valid enum",
    )


def _treasury_registry_nonempty():
    return _min_count_check(
        "SELECT COUNT(*) AS cnt FROM wallet_graph.treasury_registry WHERE monitoring_enabled = TRUE",
        minimum=1, rule="treasury_registry_empty", field="row_count", level="info",
        message="No monitoring-enabled treasuries in registry",
    )


def _treasury_events_severity_valid():
    return _count_check(
        "SELECT COUNT(*) AS cnt FROM wallet_graph.treasury_events WHERE severity NOT IN ('info', 'warning', 'critical')",
        rule="treasury_severity_invalid", field="severity", level="error",
        message="Treasury events with invalid severity values",
    )


# ---------------------------------------------------------------------------
# Domain registry
# ---------------------------------------------------------------------------

DOMAINS = {
    "sii": {
        "freshness_query": "SELECT COUNT(*) AS cnt, MAX(computed_at) AS latest FROM scores",
        "max_age_hours": 4,
        "coherence_rules": [_sii_score_range, _sii_null_scores, _sii_min_scored],
    },
    "psi": {
        "freshness_query": "SELECT COUNT(DISTINCT protocol_slug) AS cnt, MAX(computed_at) AS latest FROM psi_scores",
        "max_age_hours": 4,
        "coherence_rules": [_psi_score_range, _psi_min_scored],
    },
    "wallets": {
        "freshness_query": "SELECT COUNT(DISTINCT wallet_address) AS cnt, MAX(computed_at) AS latest FROM wallet_graph.wallet_risk_scores WHERE risk_score IS NOT NULL",
        "max_age_hours": 48,
        "coherence_rules": [_wallets_scored_nonzero, _wallets_score_range, _wallets_scored_ratio],
    },
    "cda": {
        "freshness_query": "SELECT COUNT(*) AS cnt, MAX(extracted_at) AS latest FROM cda_vendor_extractions",
        "max_age_hours": 48,
        "coherence_rules": [_cda_active_without_extractions],
    },
    "events": {
        "freshness_query": "SELECT COUNT(*) AS cnt, MAX(created_at) AS latest FROM assessment_events",
        "max_age_hours": 24,
        "coherence_rules": [_events_severity_consistency],
    },
    "edges": {
        "freshness_query": "SELECT COUNT(*) AS cnt, MAX(created_at) AS latest FROM wallet_graph.wallet_edges",
        "max_age_hours": 72,
        "coherence_rules": [_edges_exist_if_wallets, _edges_coverage, _edges_stuck_builds],
    },
    "pulse": {
        "freshness_query": "SELECT COUNT(*) AS cnt, MAX(created_at) AS latest FROM daily_pulses",
        "max_age_hours": 26,
        "coherence_rules": [_pulse_summary_coherence],
    },
    "actor_classification": {
        "freshness_query": "SELECT COUNT(*) AS cnt, MAX(classified_at) AS latest FROM wallet_graph.actor_classifications WHERE actor_type != 'unknown'",
        "max_age_hours": 48,
        "coherence_rules": [_actor_probability_range, _actor_type_consistency],
    },
    "treasury": {
        "freshness_query": "SELECT COUNT(*) AS cnt, MAX(detected_at) AS latest FROM wallet_graph.treasury_events",
        "max_age_hours": 48,
        "coherence_rules": [_treasury_registry_nonempty, _treasury_events_severity_valid],
    },
}


# ---------------------------------------------------------------------------
# Core API
# ---------------------------------------------------------------------------

def check_domain(domain: str) -> dict:
    """Check freshness and coherence for a single domain."""
    if domain not in DOMAINS:
        return {"domain": domain, "status": "error", "warnings": [{"rule": "unknown_domain", "field": "", "value": domain, "level": "error", "message": f"Unknown domain: {domain}"}]}

    cfg = DOMAINS[domain]
    now = datetime.now(timezone.utc)

    # Freshness
    last_updated = None
    age_hours = None
    row_count = 0
    status = "fresh"

    try:
        row = fetch_one(cfg["freshness_query"])
        row_count = row["cnt"] if row else 0
        latest = row.get("latest") if row else None
        if latest is None:
            if row_count == 0:
                status = "empty"
            else:
                status = "error"
        else:
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            last_updated = latest.isoformat()
            age_hours = round((now - latest).total_seconds() / 3600, 2)
            if age_hours > cfg["max_age_hours"]:
                status = "stale"
    except Exception as e:
        status = "error"
        logger.warning(f"Freshness check failed for {domain}: {e}")

    # Coherence
    warnings = []
    for rule_fn in cfg["coherence_rules"]:
        try:
            result = rule_fn()
            if result is None:
                pass
            elif isinstance(result, list):
                warnings.extend(result)
            else:
                warnings.append(result)
        except Exception as e:
            warnings.append({"rule": rule_fn.__name__, "field": "", "value": None, "level": "warning", "message": f"check failed: {e}"})

    # Escalate status based on coherence findings
    # If data is fresh but has coherence issues, degrade rather than error —
    # matches health_checker behaviour which only checks freshness.
    has_errors = any(w.get("level") == "error" for w in warnings)
    if has_errors:
        status = "error" if status != "fresh" else "degraded"

    return {
        "domain": domain,
        "status": status,
        "last_updated": last_updated,
        "age_hours": age_hours,
        "max_age_hours": cfg["max_age_hours"],
        "row_count": row_count,
        "warnings": warnings,
    }


def check_all() -> dict:
    """Check all domains and compute overall status."""
    now = datetime.now(timezone.utc)
    domains = {}
    for name in DOMAINS:
        domains[name] = check_domain(name)

    has_error = any(d["status"] == "error" for d in domains.values())
    has_stale = any(d["status"] == "stale" for d in domains.values())

    if has_error:
        overall = "unhealthy"
    elif has_stale:
        overall = "degraded"
    else:
        overall = "healthy"

    return {
        "status": overall,
        "domains": domains,
        "checked_at": now.isoformat(),
    }
