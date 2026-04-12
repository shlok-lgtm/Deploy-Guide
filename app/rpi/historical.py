"""
RPI Historical Reconstruction
================================
Backfills governance proposals, parameter changes, and incidents from
historical data, then reconstructs BASE RPI scores over time.

Only base components are reconstructed. Lens components do not get
historical scores — their data isn't reliably available historically.

Confidence surface: tags each historical score based on which base
components had data at that point in time.

Key historical moments for Aave:
- Nov 2022: Chaos Labs onboards → spend_ratio appears, parameter_velocity up
- 2023-2024: Active management → RPI near peak
- Late 2025: BGD Labs departs → governance_health declines
- Feb 2026: ACI departs → further decline
- March 2026: CAPO oracle incident → incident_severity spike
- April 2026: Chaos Labs departs → spend_ratio collapses
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timezone, timedelta, date

import requests

from app.database import execute, fetch_one, fetch_all
from app.rpi.scorer import (
    score_rpi_base,
    _normalize_spend_ratio,
    _normalize_parameter_velocity,
    _normalize_parameter_recency,
    _normalize_governance_health,
)

logger = logging.getLogger(__name__)

SNAPSHOT_API = "https://hub.snapshot.org/graphql"

# Snapshot spaces for historical queries
HISTORICAL_SNAPSHOT_SPACES = {
    "aave": "aavedao.eth",
    "lido": "lido-snapshot.eth",
    "compound-finance": "comp-vote.eth",
    "uniswap": "uniswapgovernance.eth",
    "curve-finance": "curve.eth",
    "convex-finance": "cvx.eth",
    "eigenlayer": "eigenlayer-community.eth",
    "morpho": "morpho.eth",
    "sky": "makerdao.eth",
}

# Known historical risk budgets (annual, USD) — reconstructed from governance records
# These are approximate and used for spend_ratio when proposal-level data isn't available
HISTORICAL_RISK_BUDGETS = {
    "aave": [
        {"start": "2022-01-01", "end": "2022-10-31", "budget_usd": 2_000_000, "note": "Pre-Chaos Labs"},
        {"start": "2022-11-01", "end": "2024-12-31", "budget_usd": 8_000_000, "note": "Chaos Labs + Gauntlet era"},
        {"start": "2025-01-01", "end": "2025-12-31", "budget_usd": 6_000_000, "note": "BGD Labs departure, budget reduced"},
        {"start": "2026-01-01", "end": "2026-03-31", "budget_usd": 5_000_000, "note": "ACI departure"},
        {"start": "2026-04-01", "end": "2026-12-31", "budget_usd": 3_000_000, "note": "Chaos Labs departed April 2026"},
    ],
    "compound-finance": [
        {"start": "2022-01-01", "end": "2026-09-30", "budget_usd": 4_000_000, "note": "Gauntlet + OpenZeppelin stable"},
    ],
    "sky": [
        {"start": "2022-01-01", "end": "2026-12-31", "budget_usd": 8_000_000, "note": "Robust internal risk team"},
    ],
}

# Known historical incidents for backfill (all reviewed=true since well-documented)
HISTORICAL_INCIDENTS = [
    {
        "protocol_slug": "aave",
        "incident_date": "2026-03-15",
        "title": "CAPO Oracle Misconfiguration",
        "severity": "critical",
        "funds_at_risk_usd": 26_900_000,
        "funds_recovered_usd": 0,
    },
    {
        "protocol_slug": "compound-finance",
        "incident_date": "2026-02-20",
        "title": "deUSD/sdeUSD Collateral Collapse",
        "severity": "major",
        "funds_at_risk_usd": 15_600_000,
        "funds_recovered_usd": 12_000_000,
    },
    {
        "protocol_slug": "curve-finance",
        "incident_date": "2023-07-30",
        "title": "Vyper Compiler Reentrancy Exploit",
        "severity": "critical",
        "funds_at_risk_usd": 70_000_000,
        "funds_recovered_usd": 52_000_000,
    },
    {
        "protocol_slug": "drift",
        "incident_date": "2026-04-01",
        "title": "Bad Debt Accumulation",
        "severity": "critical",
        "funds_at_risk_usd": 270_000_000,
        "funds_recovered_usd": 0,
    },
]


def backfill_snapshot_proposals(slug: str, space_id: str,
                                since_days: int = 730) -> int:
    """Backfill historical governance proposals from Snapshot.

    Fetches up to 2 years of proposals. Snapshot API supports historical queries.
    """
    from app.rpi.snapshot_collector import _classify_risk_related, _extract_budget_usd

    since_ts = int((datetime.now(timezone.utc) - timedelta(days=since_days)).timestamp())
    total_stored = 0
    skip = 0
    batch_size = 100

    while True:
        time.sleep(1)
        query = """
        query ($space: String!, $since: Int!, $skip: Int!) {
          proposals(
            first: 100,
            skip: $skip,
            where: {space: $space, created_gte: $since},
            orderBy: "created",
            orderDirection: desc
          ) {
            id
            title
            body
            state
            scores_total
            scores
            quorum
            votes
            created
            end
          }
        }
        """
        try:
            resp = requests.post(
                SNAPSHOT_API,
                json={
                    "query": query,
                    "variables": {"space": space_id, "since": since_ts, "skip": skip},
                },
                timeout=15,
            )
            if resp.status_code != 200:
                break
            proposals = resp.json().get("data", {}).get("proposals", [])
            if not proposals:
                break
        except Exception as e:
            logger.warning(f"Snapshot backfill failed for {slug}: {e}")
            break

        for prop in proposals:
            title = prop.get("title", "")
            body = (prop.get("body") or "")[:500]
            is_risk, risk_kws = _classify_risk_related(title, body)
            budget = _extract_budget_usd(title, body) if is_risk else None

            scores_total = prop.get("scores_total", 0) or 0
            quorum = prop.get("quorum", 0) or 0
            participation = (scores_total / quorum * 100) if quorum > 0 else None
            scores = prop.get("scores", [])

            created_ts = prop.get("created")
            end_ts = prop.get("end")

            try:
                execute("""
                    INSERT INTO governance_proposals
                        (protocol_slug, proposal_id, source, title, body_excerpt,
                         is_risk_related, risk_keywords, budget_amount_usd,
                         vote_for, vote_against, vote_abstain,
                         quorum_reached, participation_rate,
                         proposal_state, created_at, closed_at, scraped_at)
                    VALUES (%s, %s, 'snapshot', %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s, %s, NOW())
                    ON CONFLICT (protocol_slug, proposal_id, source) DO NOTHING
                """, (
                    slug, prop["id"], title, body,
                    is_risk, risk_kws, budget,
                    scores[0] if len(scores) > 0 else None,
                    scores[1] if len(scores) > 1 else None,
                    scores[2] if len(scores) > 2 else None,
                    quorum > 0 and scores_total >= quorum,
                    participation,
                    prop.get("state"),
                    datetime.fromtimestamp(created_ts, tz=timezone.utc) if created_ts else None,
                    datetime.fromtimestamp(end_ts, tz=timezone.utc) if end_ts else None,
                ))
                total_stored += 1
            except Exception:
                pass

        skip += batch_size
        if len(proposals) < batch_size:
            break

    logger.info(f"RPI backfill: {slug} — {total_stored} historical proposals")
    return total_stored


def backfill_incidents():
    """Insert known historical incidents with reviewed=true."""
    stored = 0
    for incident in HISTORICAL_INCIDENTS:
        try:
            execute("""
                INSERT INTO risk_incidents
                    (protocol_slug, incident_date, title, severity,
                     funds_at_risk_usd, funds_recovered_usd, reviewed)
                VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT DO NOTHING
            """, (
                incident["protocol_slug"],
                incident["incident_date"],
                incident["title"],
                incident["severity"],
                incident["funds_at_risk_usd"],
                incident["funds_recovered_usd"],
            ))
            stored += 1
        except Exception:
            pass
    logger.info(f"RPI backfill: {stored} historical incidents")
    return stored


def _get_historical_risk_budget(slug: str, target_date: date) -> float | None:
    """Get the risk budget for a protocol at a specific date."""
    budgets = HISTORICAL_RISK_BUDGETS.get(slug, [])
    for entry in budgets:
        start = date.fromisoformat(entry["start"])
        end = date.fromisoformat(entry["end"])
        if start <= target_date <= end:
            return entry["budget_usd"]
    return None


def _get_revenue_at_date(slug: str, target_date: date) -> float | None:
    """Get approximate annualized revenue from historical protocol data."""
    row = fetch_one("""
        SELECT fees_24h FROM historical_protocol_data
        WHERE protocol_slug = %s AND record_date <= %s AND fees_24h IS NOT NULL
        ORDER BY record_date DESC LIMIT 1
    """, (slug, target_date))
    if row and row.get("fees_24h"):
        return float(row["fees_24h"]) * 365
    return None


def _get_proposal_stats_at_date(slug: str, target_date: date) -> dict:
    """Get governance proposal statistics for the 90-day window ending at target_date."""
    start_date = target_date - timedelta(days=90)
    rows = fetch_all("""
        SELECT participation_rate, is_risk_related, budget_amount_usd
        FROM governance_proposals
        WHERE protocol_slug = %s
          AND created_at >= %s AND created_at <= %s
    """, (slug, start_date, target_date))

    if not rows:
        return {}

    total = len(rows)
    risk_count = sum(1 for r in rows if r.get("is_risk_related"))
    participation_rates = [float(r["participation_rate"]) for r in rows
                          if r.get("participation_rate") is not None]
    avg_participation = sum(participation_rates) / len(participation_rates) if participation_rates else None
    risk_budget = sum(float(r["budget_amount_usd"]) for r in rows
                      if r.get("budget_amount_usd") and r.get("is_risk_related"))

    return {
        "proposal_count": total,
        "risk_proposal_count": risk_count,
        "avg_participation": avg_participation,
        "risk_budget_total": risk_budget,
    }


def _get_incident_severity_at_date(slug: str, target_date: date) -> float:
    """Compute incident severity score at a historical date.

    Only uses incidents with reviewed=true and within 12 months of target_date.
    """
    start_date = target_date - timedelta(days=365)
    rows = fetch_all("""
        SELECT severity, funds_at_risk_usd, incident_date
        FROM risk_incidents
        WHERE protocol_slug = %s AND reviewed = TRUE
          AND incident_date >= %s AND incident_date <= %s
    """, (slug, start_date, target_date))

    if not rows:
        return 100.0

    severity_weights = {"critical": 40, "major": 25, "moderate": 10, "minor": 5}
    weighted_sum = 0.0
    for row in rows:
        sev = row.get("severity", "minor")
        base_weight = severity_weights.get(sev, 5)
        days_ago = (target_date - row["incident_date"]).days if row.get("incident_date") else 0
        decay = max(0.1, 1.0 - (days_ago / 365.0))
        weighted_sum += base_weight * decay

    return max(0.0, round(100.0 - weighted_sum, 2))


def reconstruct_rpi_score(slug: str, target_date: date) -> dict:
    """Reconstruct the BASE RPI score for a protocol at a historical date.

    Assembles raw values from historical data, normalizes, and scores.
    Tags with confidence based on data availability.
    """
    raw_values = {}
    sources = {}
    components_available = 0

    # 1. spend_ratio
    risk_budget = _get_historical_risk_budget(slug, target_date)
    revenue = _get_revenue_at_date(slug, target_date)
    if risk_budget and revenue and revenue > 0:
        raw_values["spend_ratio"] = (risk_budget / revenue) * 100
        sources["spend_ratio"] = "historical_budget"
        components_available += 1
    else:
        # Try from proposal data
        stats = _get_proposal_stats_at_date(slug, target_date)
        if stats.get("risk_budget_total") and revenue and revenue > 0:
            raw_values["spend_ratio"] = (stats["risk_budget_total"] / revenue) * 100
            sources["spend_ratio"] = "proposal_extraction"
            components_available += 1

    # 2. parameter_velocity — from parameter_changes table
    start_30d = target_date - timedelta(days=30)
    param_row = fetch_one("""
        SELECT COUNT(*) AS cnt
        FROM parameter_changes
        WHERE protocol_slug = %s
          AND detected_at >= %s AND detected_at <= %s
    """, (slug, start_30d, target_date))
    if param_row:
        raw_values["parameter_velocity"] = param_row["cnt"]
        sources["parameter_velocity"] = "etherscan"
        components_available += 1

    # 3. parameter_recency
    recency_row = fetch_one("""
        SELECT detected_at
        FROM parameter_changes
        WHERE protocol_slug = %s AND detected_at <= %s
        ORDER BY detected_at DESC LIMIT 1
    """, (slug, target_date))
    if recency_row and recency_row.get("detected_at"):
        days_since = (target_date - recency_row["detected_at"].date()).days
        raw_values["parameter_recency"] = days_since
        sources["parameter_recency"] = "etherscan"
        components_available += 1

    # 4. incident_severity
    raw_values["incident_severity"] = _get_incident_severity_at_date(slug, target_date)
    sources["incident_severity"] = "risk_incidents"
    components_available += 1

    # 5. governance_health
    stats = _get_proposal_stats_at_date(slug, target_date) if "stats" not in dir() else stats
    if not stats:
        stats = _get_proposal_stats_at_date(slug, target_date)
    if stats.get("avg_participation") is not None:
        raw_values["governance_health"] = stats["avg_participation"]
        sources["governance_health"] = "governance_proposals"
        components_available += 1

    # Score
    result = score_rpi_base(slug, raw_values)

    # Confidence tag
    if components_available >= 5:
        confidence = "high"
    elif components_available >= 3:
        confidence = "standard"
    else:
        confidence = "limited"

    result["confidence"] = confidence
    result["sources"] = sources
    result["target_date"] = target_date.isoformat()

    return result


def reconstruct_rpi_range(slug: str, start: date, end: date,
                          interval_days: int = 30) -> list[dict]:
    """Reconstruct RPI scores over a date range at regular intervals."""
    scores = []
    current = start
    while current <= end:
        try:
            result = reconstruct_rpi_score(slug, current)
            scores.append(result)
        except Exception as e:
            logger.debug(f"Reconstruction failed for {slug} @ {current}: {e}")
        current += timedelta(days=interval_days)
    return scores


def store_historical_scores(slug: str, scores: list[dict]):
    """Store reconstructed historical scores in rpi_score_history."""
    stored = 0
    for score in scores:
        target_date = score.get("target_date")
        if not target_date:
            continue
        try:
            execute("""
                INSERT INTO rpi_score_history
                    (protocol_slug, score_date, overall_score,
                     component_scores, methodology_version)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (protocol_slug, score_date) DO UPDATE SET
                    overall_score = EXCLUDED.overall_score,
                    component_scores = EXCLUDED.component_scores
            """, (
                slug, target_date, score["overall_score"],
                json.dumps(score["component_scores"]),
                score.get("version", "rpi-v2.0.0-reconstructed"),
            ))

            # Also store in historical_rpi_data for reference
            execute("""
                INSERT INTO historical_rpi_data
                    (protocol_slug, record_date,
                     spend_ratio, parameter_velocity, parameter_recency,
                     incident_severity, governance_health,
                     confidence, data_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'reconstruction')
                ON CONFLICT (protocol_slug, record_date) DO UPDATE SET
                    spend_ratio = EXCLUDED.spend_ratio,
                    parameter_velocity = EXCLUDED.parameter_velocity,
                    parameter_recency = EXCLUDED.parameter_recency,
                    incident_severity = EXCLUDED.incident_severity,
                    governance_health = EXCLUDED.governance_health,
                    confidence = EXCLUDED.confidence
            """, (
                slug, target_date,
                score["raw_values"].get("spend_ratio"),
                score["raw_values"].get("parameter_velocity"),
                score["raw_values"].get("parameter_recency"),
                score["raw_values"].get("incident_severity"),
                score["raw_values"].get("governance_health"),
                score.get("confidence", "standard"),
            ))
            stored += 1
        except Exception as e:
            logger.debug(f"Failed to store historical score for {slug} @ {target_date}: {e}")

    return stored


def run_historical_backfill(protocols: list[str] = None,
                            since_years: int = 2,
                            interval_days: int = 30) -> dict:
    """Run the full historical reconstruction pipeline.

    1. Backfill governance proposals from Snapshot
    2. Backfill known incidents
    3. Reconstruct BASE RPI scores at monthly intervals
    4. Store in rpi_score_history
    """
    if protocols is None:
        protocols = list(HISTORICAL_SNAPSHOT_SPACES.keys())

    # Step 1: Backfill proposals
    total_proposals = 0
    for slug in protocols:
        space_id = HISTORICAL_SNAPSHOT_SPACES.get(slug)
        if space_id:
            count = backfill_snapshot_proposals(slug, space_id, since_days=since_years * 365)
            total_proposals += count

    # Step 2: Backfill incidents
    incidents = backfill_incidents()

    # Step 3: Reconstruct scores
    end_date = date.today()
    start_date = end_date - timedelta(days=since_years * 365)

    total_scores = 0
    for slug in protocols:
        scores = reconstruct_rpi_range(slug, start_date, end_date, interval_days)
        stored = store_historical_scores(slug, scores)
        total_scores += stored
        logger.info(f"RPI backfill: {slug} — {stored} historical scores reconstructed")

    summary = {
        "protocols": len(protocols),
        "proposals_backfilled": total_proposals,
        "incidents_backfilled": incidents,
        "scores_reconstructed": total_scores,
        "date_range": f"{start_date} to {end_date}",
        "interval_days": interval_days,
    }
    logger.info(f"RPI historical backfill complete: {summary}")
    return summary
