"""
Wallet Profile Generator
=========================
Generates a full wallet risk profile (reputation primitive) by querying
existing tables. No new storage — computed on-demand.
"""

import hashlib
import json
import math
from datetime import datetime, timezone

from app.database import fetch_all, fetch_one
from app.scoring import score_to_grade

GRADE_ORDER = {
    "A+": 11, "A": 10, "A-": 9,
    "B+": 8, "B": 7, "B-": 6,
    "C+": 5, "C": 4, "C-": 3,
    "D": 2, "F": 1,
}


def generate_wallet_profile(address):
    """Generate a full wallet risk profile (reputation primitive)."""

    addr = address.lower().strip()

    # 1. Current state — latest risk score
    current = fetch_one(
        "SELECT * FROM wallet_graph.wallet_risk_scores WHERE wallet_address = %s ORDER BY computed_at DESC LIMIT 1",
        (addr,),
    )
    if not current:
        return None

    # 2. Score history — all scores for behavioral signals
    history = fetch_all(
        "SELECT risk_score, risk_grade, concentration_hhi, computed_at FROM wallet_graph.wallet_risk_scores WHERE wallet_address = %s ORDER BY computed_at ASC",
        (addr,),
    )

    # 3. Current holdings
    holdings = fetch_all(
        """SELECT symbol, value_usd, pct_of_wallet, is_scored, sii_score, sii_grade
           FROM wallet_graph.wallet_holdings
           WHERE wallet_address = %s
             AND indexed_at = (SELECT MAX(indexed_at) FROM wallet_graph.wallet_holdings WHERE wallet_address = %s)
           ORDER BY value_usd DESC""",
        (addr, addr),
    )

    # 4. Compute behavioral signals
    scores = [float(h["risk_score"]) for h in history if h.get("risk_score") is not None]
    grades = [h["risk_grade"] for h in history if h.get("risk_grade")]
    hhis = [float(h["concentration_hhi"]) for h in history if h.get("concentration_hhi") is not None]

    days_tracked = 0
    if len(history) >= 2:
        first = history[0].get("computed_at")
        last = history[-1].get("computed_at")
        if first and last:
            days_tracked = max(1, (last - first).days)

    # Score stability (std dev of last 30 entries or all if fewer)
    recent_scores = scores[-30:] if len(scores) > 30 else scores
    score_stability = None
    avg_score_30d = None
    if len(recent_scores) >= 2:
        mean = sum(recent_scores) / len(recent_scores)
        variance = sum((s - mean) ** 2 for s in recent_scores) / len(recent_scores)
        score_stability = round(math.sqrt(variance), 2)
        avg_score_30d = round(mean, 2)

    # Max drawdown (largest peak-to-trough drop in last 90 entries)
    recent_90 = scores[-90:] if len(scores) > 90 else scores
    max_drawdown = 0.0
    if recent_90:
        peak = recent_90[0]
        for s in recent_90:
            if s > peak:
                peak = s
            drawdown = peak - s
            if drawdown > max_drawdown:
                max_drawdown = drawdown
    max_drawdown = round(max_drawdown, 2)

    # Diversification trend (compare first half HHI to second half)
    div_trend = "stable"
    if len(hhis) >= 4:
        mid = len(hhis) // 2
        first_half_avg = sum(hhis[:mid]) / mid
        second_half_avg = sum(hhis[mid:]) / (len(hhis) - mid)
        diff = second_half_avg - first_half_avg
        if diff < -200:
            div_trend = "improving"   # HHI decreasing = more diversified
        elif diff > 200:
            div_trend = "deteriorating"  # HHI increasing = more concentrated

    # Quality history
    pct_a = None
    pct_b = None
    worst_grade = None
    if grades:
        pct_a = round(sum(1 for g in grades if GRADE_ORDER.get(g, 0) >= GRADE_ORDER["A-"]) / len(grades) * 100, 1)
        pct_b = round(sum(1 for g in grades if GRADE_ORDER.get(g, 0) >= GRADE_ORDER["B-"]) / len(grades) * 100, 1)
        worst_grade = min(grades, key=lambda g: GRADE_ORDER.get(g, 0))

    # Data maturity — signals are meaningless without enough history
    data_maturity = "mature" if days_tracked >= 90 else ("developing" if days_tracked >= 30 else "early")

    # Compute actual total from current holdings (not stale risk score table)
    actual_total = sum(float(h.get("value_usd") or 0) for h in holdings if float(h.get("value_usd") or 0) >= 0.01)

    # 5. Assemble profile
    profile = {
        "schema_version": "1.0.0",
        "address": addr,
        "chain": "ethereum",
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "current_state": {
            "risk_score": float(current["risk_score"]) if current.get("risk_score") is not None else None,
            "risk_grade": current.get("risk_grade"),
            "concentration_hhi": float(current["concentration_hhi"]) if current.get("concentration_hhi") is not None else None,
            "total_value_usd": actual_total if actual_total > 0 else (float(current["total_stablecoin_value"]) if current.get("total_stablecoin_value") is not None else None),
            "holdings_count": current.get("num_total_holdings"),
            "dominant_asset": current.get("dominant_asset"),
            "dominant_pct": float(current["dominant_asset_pct"]) if current.get("dominant_asset_pct") is not None else None,
        },
        "behavioral_signals": {
            "days_tracked": days_tracked,
            "score_stability_30d": score_stability if days_tracked >= 14 else None,
            "avg_score_30d": avg_score_30d if days_tracked >= 7 else None,
            "max_drawdown_90d": max_drawdown if days_tracked >= 30 else None,
            "diversification_trend": div_trend if days_tracked >= 14 else "insufficient_data",
            "data_maturity": data_maturity,
        },
        "quality_history": {
            "pct_days_a_grade": pct_a if days_tracked >= 30 else None,
            "pct_days_b_grade": pct_b if days_tracked >= 30 else None,
            "worst_grade_ever": worst_grade,
            "best_score_ever": round(max(scores), 2) if scores else None,
            "worst_score_ever": round(min(scores), 2) if scores else None,
        },
        "holdings": [
            {
                "symbol": h.get("symbol"),
                "value_usd": float(h["value_usd"]) if h.get("value_usd") is not None else None,
                "pct_of_wallet": float(h["pct_of_wallet"]) if h.get("pct_of_wallet") is not None else None,
                "sii_score": float(h["sii_score"]) if h.get("sii_score") is not None else None,
                "sii_grade": h.get("sii_grade"),
            }
            for h in holdings
            if float(h.get("value_usd") or 0) >= 0.01
        ],
    }

    # 6. Compute profile hash (BEFORE adding the hash itself)
    canonical = json.dumps(profile, sort_keys=True, separators=(",", ":"), default=str)
    profile["profile_hash"] = "0x" + hashlib.sha256(canonical.encode()).hexdigest()

    return profile
