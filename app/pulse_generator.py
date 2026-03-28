"""
Daily Pulse Generator
======================
Computes a snapshot of the entire risk surface: stablecoin scores,
wallet stats, assessment events. Stored in daily_pulses table.
Idempotent — safe to call multiple times per day.
"""

import hashlib
import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal

from app.database import execute, fetch_all, fetch_one
from app.scoring import FORMULA_VERSION

logger = logging.getLogger(__name__)


def _default_serializer(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def run_daily_pulse():
    """Generate today's daily pulse. Idempotent — safe to call multiple times."""

    today = date.today().isoformat()
    logger.info(f"Generating daily pulse for {today}")

    # 1. All current stablecoin scores (from scores table, not stablecoins)
    stablecoins = fetch_all("""
        SELECT st.symbol, s.overall_score, s.grade, s.formula_version
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        ORDER BY s.overall_score DESC
    """)

    # 2. Yesterday's pulse for delta calculation
    yesterday_pulse = fetch_one(
        "SELECT summary FROM daily_pulses WHERE pulse_date < %s ORDER BY pulse_date DESC LIMIT 1",
        (today,),
    )
    yesterday_scores = {}
    if yesterday_pulse and yesterday_pulse.get("summary"):
        summary = yesterday_pulse["summary"]
        if isinstance(summary, str):
            summary = json.loads(summary)
        for s in summary.get("scores", []):
            yesterday_scores[s["symbol"]] = s.get("score")

    # 3. Build scores list with deltas
    scores_list = []
    for coin in stablecoins:
        symbol = coin.get("symbol", "")
        score = float(coin["overall_score"]) if coin.get("overall_score") is not None else None
        prev = yesterday_scores.get(symbol)
        delta = round(score - prev, 2) if score is not None and prev is not None else None
        scores_list.append({
            "symbol": symbol,
            "score": score,
            "grade": coin.get("grade"),
            "delta_24h": delta,
        })

    # 4. Aggregate wallet stats
    wallet_stats = fetch_one("""
        SELECT
            COUNT(DISTINCT wallet_address) as wallets_scored,
            COALESCE(AVG(risk_score), 0) as avg_risk_score
        FROM wallet_graph.wallet_risk_scores
        WHERE computed_at > NOW() - INTERVAL '48 hours'
    """)

    wallets_total = fetch_one("SELECT COUNT(*) as count FROM wallet_graph.wallets")

    wallet_value = fetch_one("""
        SELECT COALESCE(SUM(total_stablecoin_value), 0) as total_tracked
        FROM wallet_graph.wallets
        WHERE total_stablecoin_value > 0
    """)

    # 5. Event counts (assessment_events may be empty)
    event_counts = {"silent": 0, "notable": 0, "alert": 0, "critical": 0, "total": 0}
    try:
        events = fetch_all("""
            SELECT severity, COUNT(*) as count
            FROM assessment_events
            WHERE created_at > NOW() - INTERVAL '24 hours'
            GROUP BY severity
        """)
        for e in events:
            sev = e.get("severity", "silent")
            cnt = e.get("count", 0)
            event_counts[sev] = cnt
            event_counts["total"] += cnt
    except Exception:
        pass

    # 6. Notable events (top 5)
    notable_events = []
    try:
        notables = fetch_all("""
            SELECT id, wallet_address, trigger_type, severity, wallet_risk_score, created_at
            FROM assessment_events
            WHERE severity IN ('notable', 'alert', 'critical')
              AND created_at > NOW() - INTERVAL '24 hours'
            ORDER BY created_at DESC
            LIMIT 5
        """)
        notable_events = [
            {
                "id": str(n.get("id", "")),
                "wallet": n.get("wallet_address", ""),
                "trigger": n.get("trigger_type", ""),
                "severity": n.get("severity", ""),
                "score": float(n["wallet_risk_score"]) if n.get("wallet_risk_score") is not None else None,
            }
            for n in notables
        ]
    except Exception:
        pass

    # 7. PSI scores summary (if available)
    psi_summary = []
    try:
        psi_rows = fetch_all("""
            SELECT DISTINCT ON (protocol_slug)
                protocol_slug, protocol_name, overall_score, grade
            FROM psi_scores
            ORDER BY protocol_slug, computed_at DESC
        """)
        psi_summary = [
            {
                "protocol": r.get("protocol_name", r["protocol_slug"]),
                "score": float(r["overall_score"]) if r.get("overall_score") is not None else None,
                "grade": r.get("grade"),
            }
            for r in psi_rows
        ]
    except Exception:
        pass

    # 8. Assemble summary
    summary = {
        "pulse_date": today,
        "methodology_version": FORMULA_VERSION,
        "scores": scores_list,
        "network_state": {
            "wallets_indexed": wallets_total.get("count", 0) if wallets_total else 0,
            "wallets_scored": wallet_stats.get("wallets_scored", 0) if wallet_stats else 0,
            "total_tracked_usd": float(wallet_value.get("total_tracked", 0)) if wallet_value else 0,
            "avg_risk_score": round(float(wallet_stats.get("avg_risk_score", 0)), 2) if wallet_stats else 0,
            "stablecoins_scored": len(scores_list),
            "protocols_scored": len(psi_summary),
        },
        "events_24h": event_counts,
        "notable_events": notable_events,
        "psi_scores": psi_summary,
    }

    # 9. Compute content hash
    canonical = json.dumps(summary, sort_keys=True, separators=(",", ":"), default=_default_serializer)
    content_hash = "0x" + hashlib.sha256(canonical.encode()).hexdigest()

    # 10. Upsert into daily_pulses
    execute("""
        INSERT INTO daily_pulses (pulse_date, summary, page_url)
        VALUES (%s, %s, %s)
        ON CONFLICT (pulse_date) DO UPDATE SET
            summary = EXCLUDED.summary,
            created_at = NOW()
    """, (today, json.dumps(summary, default=_default_serializer), f"/pulse/{today}"))

    logger.info(
        f"Daily pulse generated for {today}: "
        f"{len(scores_list)} stablecoins, {len(psi_summary)} protocols, "
        f"content_hash={content_hash[:18]}..."
    )
    return summary, content_hash
