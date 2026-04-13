"""
Cross-Domain Coherence Sweep
==============================
Daily validation that the hub's state domains are internally consistent.

Four checks:
1. **Freshness gaps** — every attested domain updated within its expected cadence.
2. **Record count drift** — domain record counts haven't changed by >50 % day-over-day.
3. **SII / PSI alignment** — every scored stablecoin that appears in a PSI protocol
   has a recent SII score, and vice versa.
4. **State root coverage** — the latest pulse state root references all expected domains.
"""

import json
import logging
from datetime import datetime, timezone

from app.database import fetch_one, fetch_all, execute, get_cursor

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Expected update frequencies (hours) per domain
# ---------------------------------------------------------------------------

ALL_DOMAINS = [
    "sii_components",
    "psi_components",
    "cda_extractions",
    "wallets",
    "wallet_profiles",
    "edges",
    "actors",
    "psi_discoveries",
    "smart_contracts",
    "flows",
    "cqi_compositions",
    "discovery_signals",
    "provenance",
    "governance_events",
    "divergence_signals",
    "lsti_components",
    "bri_components",
    "dohi_components",
    "vsri_components",
    "cxri_components",
    "tti_components",
    "rpi_components",
]

DOMAIN_FREQUENCIES = {
    "sii_components": 2,
    "psi_components": 2,
    "cda_extractions": 24,
    "wallets": 4,
    "wallet_profiles": 24,
    "edges": 12,
    "actors": 24,
    "psi_discoveries": 24,
    "smart_contracts": 4,
    "flows": 4,
    "cqi_compositions": 4,
    "discovery_signals": 24,
    "provenance": 24,
    "governance_events": 24,
    "divergence_signals": 4,
    "lsti_components": 24,
    "bri_components": 24,
    "dohi_components": 24,
    "vsri_components": 24,
    "cxri_components": 24,
    "tti_components": 24,
    "rpi_components": 48,
}


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_freshness() -> list[dict]:
    """Flag domains whose latest attestation is older than their expected cadence."""
    issues = []
    # Single query instead of N+1: get latest timestamp per domain in one pass
    rows = fetch_all(
        """
        SELECT DISTINCT ON (domain) domain, cycle_timestamp
        FROM state_attestations
        WHERE domain = ANY(%s)
        ORDER BY domain, cycle_timestamp DESC
        """,
        (ALL_DOMAINS,),
    )
    latest_by_domain = {r["domain"]: r["cycle_timestamp"] for r in (rows or [])}
    now = datetime.now(timezone.utc)
    for domain in ALL_DOMAINS:
        expected_hours = DOMAIN_FREQUENCIES.get(domain, 24)
        ts = latest_by_domain.get(domain)
        if not ts:
            issues.append({
                "check": "freshness",
                "domain": domain,
                "severity": "warning",
                "detail": "No attestation found",
            })
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_hours = (now - ts).total_seconds() / 3600
        if age_hours > expected_hours * 2:
            issues.append({
                "check": "freshness",
                "domain": domain,
                "severity": "alert" if age_hours > expected_hours * 4 else "warning",
                "detail": f"Last attested {age_hours:.1f}h ago (expected every {expected_hours}h)",
            })
    return issues


def _check_record_count_drift() -> list[dict]:
    """Flag domains whose record count changed by >50 % day-over-day."""
    issues = []
    # Single query: get latest 2 record counts per domain using window function
    rows = fetch_all(
        """
        SELECT domain, record_count, rn FROM (
            SELECT domain, record_count,
                   ROW_NUMBER() OVER (PARTITION BY domain ORDER BY cycle_timestamp DESC) AS rn
            FROM state_attestations
            WHERE domain = ANY(%s) AND entity_id IS NULL
        ) sub
        WHERE rn <= 2
        ORDER BY domain, rn
        """,
        (ALL_DOMAINS,),
    )
    # Group by domain
    by_domain: dict[str, list] = {}
    for r in (rows or []):
        by_domain.setdefault(r["domain"], []).append(r["record_count"] or 0)
    for domain, counts in by_domain.items():
        if len(counts) < 2:
            continue
        current, previous = counts[0], counts[1]
        if previous == 0:
            continue
        drift_pct = abs(current - previous) / previous * 100
        if drift_pct > 50:
            issues.append({
                "check": "record_count_drift",
                "domain": domain,
                "severity": "warning",
                "detail": (
                    f"Record count changed {drift_pct:.0f}% "
                    f"({previous} -> {current})"
                ),
            })
    return issues


def _check_sii_psi_alignment() -> list[dict]:
    """Every scored stablecoin should have a recent SII score, and
    every PSI-scored protocol's stablecoin exposures should be SII-covered."""
    issues = []
    # SII coins without recent scores
    try:
        stale_sii = fetch_all(
            """
            SELECT stablecoin_id, calculated_at
            FROM scores
            WHERE calculated_at < NOW() - INTERVAL '6 hours'
            """
        )
        for row in stale_sii:
            issues.append({
                "check": "sii_psi_alignment",
                "domain": "sii_components",
                "severity": "warning",
                "detail": f"SII score for {row['stablecoin_id']} is stale",
            })
    except Exception as e:
        logger.debug(f"SII staleness check skipped: {e}")

    # PSI protocols that reference stablecoins not in the SII registry
    try:
        psi_rows = fetch_all(
            """
            SELECT DISTINCT protocol_slug FROM psi_scores
            WHERE scored_at > NOW() - INTERVAL '48 hours'
            """
        )
        sii_rows = fetch_all("SELECT stablecoin_id FROM scores")
        sii_ids = {r["stablecoin_id"] for r in sii_rows} if sii_rows else set()

        # Check collateral exposure table for stablecoins referenced by PSI
        exposure_rows = fetch_all(
            """
            SELECT DISTINCT stablecoin_id
            FROM protocol_collateral_exposure
            WHERE snapshot_date > CURRENT_DATE - 7
            """
        )
        for row in (exposure_rows or []):
            sid = row["stablecoin_id"]
            if sid and sid not in sii_ids:
                issues.append({
                    "check": "sii_psi_alignment",
                    "domain": "psi_components",
                    "severity": "info",
                    "detail": f"PSI-referenced stablecoin '{sid}' has no SII score",
                })
    except Exception as e:
        logger.debug(f"SII/PSI alignment check skipped: {e}")

    return issues


def _check_state_root_coverage() -> list[dict]:
    """The latest pulse state root should reference all expected domains."""
    issues = []
    try:
        pulse = fetch_one(
            """
            SELECT state_root
            FROM daily_pulses
            ORDER BY pulse_date DESC LIMIT 1
            """
        )
        if not pulse or not pulse.get("state_root"):
            issues.append({
                "check": "state_root_coverage",
                "domain": "pulse",
                "severity": "warning",
                "detail": "No daily pulse with state root found",
            })
            return issues

        state_root = pulse["state_root"]
        if isinstance(state_root, str):
            try:
                state_root = json.loads(state_root)
            except (json.JSONDecodeError, TypeError):
                state_root = {}

        covered_domains = set(state_root.keys()) if isinstance(state_root, dict) else set()
        for domain in ALL_DOMAINS:
            if domain not in covered_domains:
                issues.append({
                    "check": "state_root_coverage",
                    "domain": domain,
                    "severity": "info",
                    "detail": f"Domain '{domain}' missing from latest pulse state root",
                })
    except Exception as e:
        logger.debug(f"State root coverage check skipped: {e}")

    return issues


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_coherence_sweep() -> dict:
    """
    Run all coherence checks and persist a report.
    Returns the report dict.
    """
    all_issues = []
    all_issues.extend(_check_freshness())
    all_issues.extend(_check_record_count_drift())
    all_issues.extend(_check_sii_psi_alignment())
    all_issues.extend(_check_state_root_coverage())

    report = {
        "domains_checked": len(ALL_DOMAINS),
        "issues_found": len(all_issues),
        "details": all_issues,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    # Persist
    try:
        execute(
            """
            INSERT INTO coherence_reports (domains_checked, issues_found, details)
            VALUES (%s, %s, %s)
            """,
            (report["domains_checked"], report["issues_found"], json.dumps(all_issues)),
        )
    except Exception as e:
        logger.warning(f"Failed to persist coherence report: {e}")

    return report


def get_latest_report() -> dict | None:
    """Fetch the most recent coherence report."""
    row = fetch_one(
        """
        SELECT id, created_at, domains_checked, issues_found, details
        FROM coherence_reports
        ORDER BY created_at DESC LIMIT 1
        """
    )
    if not row:
        return None
    return {
        "id": row["id"],
        "created_at": row["created_at"].isoformat() if hasattr(row["created_at"], "isoformat") else row["created_at"],
        "domains_checked": row["domains_checked"],
        "issues_found": row["issues_found"],
        "details": row["details"] if isinstance(row["details"], list) else json.loads(row["details"] or "[]"),
    }


def get_report_history(days: int = 7) -> list[dict]:
    """Fetch recent coherence reports."""
    rows = fetch_all(
        """
        SELECT id, created_at, domains_checked, issues_found, details
        FROM coherence_reports
        WHERE created_at > NOW() - make_interval(days => %s)
        ORDER BY created_at DESC
        """,
        (days,),
    )
    results = []
    for row in (rows or []):
        results.append({
            "id": row["id"],
            "created_at": row["created_at"].isoformat() if hasattr(row["created_at"], "isoformat") else row["created_at"],
            "domains_checked": row["domains_checked"],
            "issues_found": row["issues_found"],
            "details": row["details"] if isinstance(row["details"], list) else json.loads(row["details"] or "[]"),
        })
    return results
