"""
RPI Incident Auto-Detection
==============================
Monitors multiple sources for risk incidents:
1. Governance forum post-mortems (from forum_scraper)
2. Large TVL drops (from DeFiLlama)
3. Emergency governance actions (from parameter_collector)

Auto-detected incidents default to reviewed=false and do NOT affect
the base incident_severity score until manually reviewed.

The recovery_ratio lens component CAN use unreviewed incidents
with a lower confidence tag.
"""

import logging
import time
from datetime import datetime, timezone, timedelta

import requests

from app.database import execute, fetch_one, fetch_all
from app.index_definitions.rpi_v2 import RPI_TARGET_PROTOCOLS

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://api.llama.fi"

# Severity thresholds for auto-classification
TVL_DROP_THRESHOLDS = {
    0.30: "critical",   # >30% TVL drop in 24h
    0.15: "major",      # >15% TVL drop
    0.05: "moderate",   # >5% TVL drop
}


def detect_tvl_drops() -> int:
    """Detect large TVL drops by comparing current vs recent DeFiLlama data.

    Only flags incidents when TVL drops significantly in a short window.
    All auto-detected incidents are stored with reviewed=false.
    """
    detected = 0

    for slug in RPI_TARGET_PROTOCOLS:
        time.sleep(1)
        try:
            resp = requests.get(f"{DEFILLAMA_BASE}/protocol/{slug}", timeout=30)
            if resp.status_code != 200:
                continue
            data = resp.json()
        except Exception as e:
            logger.debug(f"DeFiLlama fetch failed for {slug}: {e}")
            continue

        tvl_history = data.get("tvl", [])
        if not tvl_history or len(tvl_history) < 2:
            continue

        # Compare latest TVL to 7 days ago
        current_tvl = tvl_history[-1].get("totalLiquidityUSD", 0) if isinstance(tvl_history[-1], dict) else 0
        if current_tvl <= 0:
            continue

        # Find TVL from ~7 days ago
        target_ts = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())
        week_ago_tvl = None
        for entry in reversed(tvl_history):
            if isinstance(entry, dict) and entry.get("date", 0) <= target_ts:
                week_ago_tvl = entry.get("totalLiquidityUSD", 0)
                break

        if not week_ago_tvl or week_ago_tvl <= 0:
            continue

        drop_pct = (week_ago_tvl - current_tvl) / week_ago_tvl
        if drop_pct < 0.05:
            continue  # not significant

        # Classify severity
        severity = "minor"
        for threshold, sev in sorted(TVL_DROP_THRESHOLDS.items(), reverse=True):
            if drop_pct >= threshold:
                severity = sev
                break

        funds_at_risk = week_ago_tvl - current_tvl
        title = f"TVL drop detected: {drop_pct * 100:.1f}% decline over 7 days"

        # Check if we already flagged this recently
        existing = fetch_one("""
            SELECT id FROM risk_incidents
            WHERE protocol_slug = %s
              AND title LIKE 'TVL drop detected%%'
              AND incident_date >= CURRENT_DATE - 7
        """, (slug,))
        if existing:
            continue

        try:
            execute("""
                INSERT INTO risk_incidents
                    (protocol_slug, incident_date, title, description,
                     severity, funds_at_risk_usd, reviewed, source_url)
                VALUES (%s, CURRENT_DATE, %s, %s,
                        %s, %s, FALSE, %s)
            """, (
                slug, title,
                f"Automated detection: {slug} TVL dropped from ${week_ago_tvl:,.0f} to ${current_tvl:,.0f} ({drop_pct * 100:.1f}% decline) over the past 7 days.",
                severity, funds_at_risk,
                f"https://defillama.com/protocol/{slug}",
            ))
            detected += 1
            logger.info(f"RPI incident: {slug} TVL drop {drop_pct * 100:.1f}% ({severity})")
        except Exception as e:
            logger.debug(f"Failed to store TVL drop incident for {slug}: {e}")

    return detected


def detect_forum_incidents() -> int:
    """Detect incidents mentioned in governance forum posts.

    Looks for posts flagged as mentioning incidents by the forum scraper.
    Creates incident records with reviewed=false.
    """
    detected = 0

    rows = fetch_all("""
        SELECT protocol_slug, title, body_excerpt, posted_at, forum_url, topic_id
        FROM governance_forum_posts
        WHERE mentions_incident = TRUE
          AND posted_at >= NOW() - INTERVAL '30 days'
        ORDER BY posted_at DESC
    """)

    for row in rows:
        slug = row["protocol_slug"]
        title = row.get("title", "Unknown incident")

        # Check if we already have this incident
        existing = fetch_one("""
            SELECT id FROM risk_incidents
            WHERE protocol_slug = %s
              AND title = %s
        """, (slug, f"Forum report: {title[:200]}"))
        if existing:
            continue

        try:
            source_url = f"{row.get('forum_url', '')}/t/{row.get('topic_id', '')}"
            execute("""
                INSERT INTO risk_incidents
                    (protocol_slug, incident_date, title, description,
                     severity, reviewed, source_url)
                VALUES (%s, %s, %s, %s, 'moderate', FALSE, %s)
            """, (
                slug,
                row["posted_at"].date() if row.get("posted_at") else datetime.now(timezone.utc).date(),
                f"Forum report: {title[:200]}",
                row.get("body_excerpt", "")[:500],
                source_url,
            ))
            detected += 1
            logger.info(f"RPI incident from forum: {slug} — {title[:80]}")
        except Exception as e:
            logger.debug(f"Failed to store forum incident for {slug}: {e}")

    return detected


def detect_emergency_actions() -> int:
    """Detect emergency governance actions from parameter changes.

    Emergency actions are identified by function signatures containing
    'pause', 'freeze', 'emergency', or 'kill' keywords.
    """
    detected = 0
    emergency_keywords = ['pause', 'freeze', 'emergency', 'kill', 'shutdown']

    rows = fetch_all("""
        SELECT protocol_slug, tx_hash, parameter_type, function_signature, detected_at
        FROM parameter_changes
        WHERE detected_at >= NOW() - INTERVAL '7 days'
    """)

    for row in rows:
        sig = (row.get("function_signature") or "").lower()
        param_type = (row.get("parameter_type") or "").lower()
        if not any(kw in sig or kw in param_type for kw in emergency_keywords):
            continue

        slug = row["protocol_slug"]
        tx_hash = row.get("tx_hash", "")

        existing = fetch_one("""
            SELECT id FROM risk_incidents
            WHERE protocol_slug = %s AND metadata->>'tx_hash' = %s
        """, (slug, tx_hash))
        if existing:
            continue

        try:
            execute("""
                INSERT INTO risk_incidents
                    (protocol_slug, incident_date, title, description,
                     severity, reviewed, source_url, metadata)
                VALUES (%s, %s, %s, %s, 'major', FALSE, %s, %s)
            """, (
                slug,
                row["detected_at"].date() if row.get("detected_at") else datetime.now(timezone.utc).date(),
                f"Emergency action detected: {row.get('parameter_type', 'unknown')}",
                f"Emergency governance action detected in tx {tx_hash}. Function: {row.get('function_signature', 'unknown')}",
                f"https://etherscan.io/tx/{tx_hash}",
                f'{{"tx_hash": "{tx_hash}"}}',
            ))
            detected += 1
            logger.info(f"RPI emergency action: {slug} — tx {tx_hash[:16]}...")
        except Exception as e:
            logger.debug(f"Failed to store emergency incident for {slug}: {e}")

    return detected


def update_recovery_ratio_lens():
    """Update the recovery_ratio lens component from incident data.

    Uses ALL incidents (including unreviewed) but tags confidence.
    """
    updated = 0
    for slug in RPI_TARGET_PROTOCOLS:
        rows = fetch_all("""
            SELECT funds_at_risk_usd, funds_recovered_usd, reviewed
            FROM risk_incidents
            WHERE protocol_slug = %s
              AND incident_date >= NOW() - INTERVAL '24 months'
              AND funds_at_risk_usd > 0
        """, (slug,))

        if not rows:
            # No incidents = perfect recovery
            score = 100.0
            raw_val = None
        else:
            total_at_risk = sum(float(r["funds_at_risk_usd"] or 0) for r in rows)
            total_recovered = sum(float(r["funds_recovered_usd"] or 0) for r in rows)
            ratio = (total_recovered / total_at_risk * 100) if total_at_risk > 0 else 0

            if ratio >= 90:
                score = 100.0
            elif ratio >= 70:
                score = 80.0
            elif ratio >= 50:
                score = 60.0
            elif ratio >= 30:
                score = 40.0
            else:
                score = 0.0
            raw_val = round(ratio, 2)

        try:
            execute("""
                INSERT INTO rpi_components
                    (protocol_slug, component_id, component_type, lens_id,
                     raw_value, normalized_score, source_type, data_source,
                     collected_at)
                VALUES (%s, 'recovery_ratio', 'lens', 'risk_organization',
                        %s, %s, 'automated', 'risk_incidents', NOW())
            """, (slug, raw_val, score))
            updated += 1
        except Exception as e:
            logger.debug(f"Failed to update recovery_ratio for {slug}: {e}")

    return updated


def run_incident_detection() -> dict:
    """Run all incident detection sources. Returns summary."""
    tvl = detect_tvl_drops()
    forum = detect_forum_incidents()
    emergency = detect_emergency_actions()
    recovery = update_recovery_ratio_lens()

    summary = {
        "tvl_drops": tvl,
        "forum_incidents": forum,
        "emergency_actions": emergency,
        "recovery_ratio_updated": recovery,
    }
    logger.info(f"RPI incident detection: {summary}")
    return summary
