"""
Publisher — Pulse Renderer
============================
Generates daily pulse summaries from the day's assessment events + SII scores.
"""

import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal

from app.database import fetch_all, fetch_one, execute


class _DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

logger = logging.getLogger(__name__)


def generate_daily_pulse(pulse_date: date | None = None) -> dict | None:
    """
    Generate the daily pulse summary for the given date.
    Aggregates SII scores, wallet stats, and notable events.
    Returns the pulse dict, or None if already generated.
    """
    if pulse_date is None:
        pulse_date = date.today()

    # Check if pulse already exists
    existing = fetch_one(
        "SELECT id FROM daily_pulses WHERE pulse_date = %s",
        (pulse_date,)
    )
    if existing:
        logger.info(f"Pulse for {pulse_date} already exists — skipping")
        return None

    # Current SII scores
    score_rows = fetch_all("""
        SELECT s.stablecoin_id, s.overall_score, s.grade, s.daily_change
        FROM scores s
        ORDER BY s.overall_score DESC
    """)
    scores = []
    for r in score_rows:
        scores.append({
            "symbol": r["stablecoin_id"],
            "score": float(r["overall_score"]) if r["overall_score"] is not None else None,
            "delta_24h": float(r["daily_change"]) if r.get("daily_change") is not None else 0.0,
        })

    # Wallet stats
    wallet_stats = fetch_one("""
        SELECT COUNT(*) AS wallets_indexed,
               COALESCE(SUM(total_stablecoin_value), 0) AS total_tracked
        FROM wallet_graph.wallets
        WHERE total_stablecoin_value > 0
    """)

    # Today's assessment stats
    event_stats = fetch_one("""
        SELECT COUNT(*) AS total_events,
               COUNT(*) FILTER (WHERE severity = 'alert' OR severity = 'critical') AS alerts_today
        FROM assessment_events
        WHERE created_at >= %s::date AND created_at < %s::date + INTERVAL '1 day'
    """, (pulse_date, pulse_date))

    # Notable events (alert + critical)
    notable_rows = fetch_all("""
        SELECT id::text, wallet_address, trigger_type, severity,
               wallet_risk_score, wallet_risk_grade, total_stablecoin_value
        FROM assessment_events
        WHERE severity IN ('alert', 'critical')
        AND created_at >= %s::date AND created_at < %s::date + INTERVAL '1 day'
        ORDER BY created_at DESC
        LIMIT 20
    """, (pulse_date, pulse_date))

    notable_events = []
    for r in notable_rows:
        notable_events.append({
            "id": r["id"],
            "wallet": r["wallet_address"],
            "trigger": r["trigger_type"],
            "severity": r["severity"],
            "risk_score": float(r["wallet_risk_score"]) if r["wallet_risk_score"] is not None else None,
            "value": float(r["total_stablecoin_value"]) if r["total_stablecoin_value"] is not None else None,
        })

    summary = {
        "scores": scores,
        "total_tracked": float(wallet_stats["total_tracked"]) if wallet_stats and wallet_stats["total_tracked"] is not None else 0.0,
        "wallets_indexed": int(wallet_stats["wallets_indexed"]) if wallet_stats else 0,
        "total_events": int(event_stats["total_events"]) if event_stats else 0,
        "alerts_today": int(event_stats["alerts_today"]) if event_stats else 0,
        "notable_events": notable_events,
    }

    # Store pulse
    execute("""
        INSERT INTO daily_pulses (pulse_date, summary)
        VALUES (%s, %s)
        ON CONFLICT (pulse_date) DO UPDATE SET summary = EXCLUDED.summary
    """, (pulse_date, json.dumps(summary, cls=_DecimalEncoder)))

    logger.info(
        f"Daily pulse generated for {pulse_date}: "
        f"{len(scores)} scores, {event_stats['alerts_today'] if event_stats else 0} alerts"
    )
    return summary
