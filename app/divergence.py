"""
Divergence Detector
===================
Detects capital-flow / quality mismatches across the wallet risk graph.

Three signal types:
  A. Asset Quality   — SII score declining while capital flows in
  B. Wallet Concentration — HHI rising while wallet value grows
  C. Quality-Flow    — stablecoin score declining with net inflows from wallet graph
"""

import json
import logging
from datetime import datetime, timezone

from app.database import fetch_all, fetch_one
from app.scoring import score_to_grade

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Signal A: Asset Quality Divergence
# ---------------------------------------------------------------------------

def detect_asset_divergence():
    """Detect stablecoins where score is declining but capital is flowing in."""
    results = []

    # Use the latest daily pulse for 24h deltas (most reliable source)
    latest_pulse = fetch_one(
        "SELECT summary FROM daily_pulses ORDER BY pulse_date DESC LIMIT 1"
    )

    if not latest_pulse or not latest_pulse.get("summary"):
        return results

    summary = latest_pulse["summary"]
    if isinstance(summary, str):
        summary = json.loads(summary)

    for score_entry in summary.get("scores", []):
        delta = score_entry.get("delta_24h")
        if delta is None or delta >= -1.0:
            continue

        symbol = score_entry.get("symbol", "")
        if not symbol:
            continue

        # Get aggregate wallet holdings for this symbol
        holdings_agg = fetch_one("""
            SELECT COUNT(DISTINCT wallet_address) AS wallet_count,
                   COALESCE(SUM(value_usd), 0) AS total_held
            FROM wallet_graph.wallet_holdings
            WHERE UPPER(symbol) = UPPER(%s)
              AND indexed_at > NOW() - INTERVAL '48 hours'
        """, (symbol,))

        wallet_count = holdings_agg.get("wallet_count", 0) if holdings_agg else 0
        total_held = float(holdings_agg.get("total_held", 0)) if holdings_agg else 0

        # Determine severity
        if delta < -5.0 and total_held > 100_000_000:
            severity = "critical"
        elif delta < -3.0:
            severity = "alert"
        else:
            severity = "notable"

        results.append({
            "type": "asset_quality",
            "symbol": symbol,
            "current_score": score_entry.get("score"),
            "score_delta": delta,
            "grade": score_entry.get("grade"),
            "wallets_holding": wallet_count,
            "total_held_usd": total_held,
            "signal": f"{symbol} score declined {abs(delta):.1f} pts while held by {wallet_count} wallets (${total_held / 1e6:.1f}M)",
            "severity": severity,
        })

    return results


# ---------------------------------------------------------------------------
# Signal B: Wallet Concentration Divergence
# ---------------------------------------------------------------------------

def detect_wallet_concentration_divergence(limit=20):
    """Detect wallets getting more concentrated while growing in value."""
    rows = fetch_all("""
        WITH latest AS (
            SELECT DISTINCT ON (wallet_address)
                wallet_address, risk_score, concentration_hhi,
                total_stablecoin_value, computed_at
            FROM wallet_graph.wallet_risk_scores
            ORDER BY wallet_address, computed_at DESC
        ),
        previous AS (
            SELECT DISTINCT ON (wrs.wallet_address)
                wrs.wallet_address,
                wrs.risk_score AS prev_score,
                wrs.concentration_hhi AS prev_hhi,
                wrs.total_stablecoin_value AS prev_value,
                wrs.computed_at AS prev_computed
            FROM wallet_graph.wallet_risk_scores wrs
            WHERE wrs.computed_at < (
                SELECT MAX(wrs2.computed_at)
                FROM wallet_graph.wallet_risk_scores wrs2
                WHERE wrs2.wallet_address = wrs.wallet_address
            )
            ORDER BY wrs.wallet_address, wrs.computed_at DESC
        )
        SELECT l.wallet_address,
               l.risk_score, l.concentration_hhi, l.total_stablecoin_value,
               p.prev_score, p.prev_hhi, p.prev_value
        FROM latest l
        JOIN previous p ON p.wallet_address = l.wallet_address
        WHERE l.concentration_hhi > p.prev_hhi + 300
          AND p.prev_value > 0
          AND l.total_stablecoin_value > p.prev_value * 1.05
          AND l.total_stablecoin_value > 100000
        ORDER BY (l.concentration_hhi - p.prev_hhi) DESC
        LIMIT %s
    """, (limit,))

    results = []
    for r in rows:
        hhi_change = float(r["concentration_hhi"]) - float(r["prev_hhi"])
        value_change_pct = ((float(r["total_stablecoin_value"]) - float(r["prev_value"])) / float(r["prev_value"])) * 100

        if hhi_change > 2000 and float(r["total_stablecoin_value"]) > 10_000_000:
            severity = "critical"
        elif hhi_change > 1000:
            severity = "alert"
        else:
            severity = "notable"

        results.append({
            "type": "wallet_concentration",
            "wallet_address": r["wallet_address"],
            "current_hhi": float(r["concentration_hhi"]),
            "previous_hhi": float(r["prev_hhi"]),
            "hhi_change": round(hhi_change, 0),
            "current_value_usd": float(r["total_stablecoin_value"]),
            "value_change_pct": round(value_change_pct, 1),
            "risk_score": float(r["risk_score"]) if r.get("risk_score") else None,
            "signal": f"Wallet {r['wallet_address'][:10]}... HHI +{hhi_change:.0f} while value +{value_change_pct:.1f}%",
            "severity": severity,
        })

    return results


# ---------------------------------------------------------------------------
# Signal C: Quality-Flow Divergence
# ---------------------------------------------------------------------------

def detect_quality_flow_divergence():
    """Detect stablecoins with declining scores but net inflows from the wallet graph."""
    results = []

    latest_pulse = fetch_one(
        "SELECT summary FROM daily_pulses ORDER BY pulse_date DESC LIMIT 1"
    )
    if not latest_pulse or not latest_pulse.get("summary"):
        return results

    summary = latest_pulse["summary"]
    if isinstance(summary, str):
        summary = json.loads(summary)

    # Find stablecoins with score decline > 2 points
    declining = [s for s in summary.get("scores", []) if (s.get("delta_24h") or 0) < -2.0]

    for sc in declining:
        symbol = sc.get("symbol", "")
        if not symbol:
            continue

        # Compare current aggregate holdings to older holdings
        # "current" = last 48h, "previous" = 48-96h window
        current = fetch_one("""
            SELECT COALESCE(SUM(value_usd), 0) AS total,
                   COUNT(DISTINCT wallet_address) AS wallets
            FROM wallet_graph.wallet_holdings
            WHERE UPPER(symbol) = UPPER(%s)
              AND indexed_at > NOW() - INTERVAL '48 hours'
        """, (symbol,))

        previous = fetch_one("""
            SELECT COALESCE(SUM(value_usd), 0) AS total,
                   COUNT(DISTINCT wallet_address) AS wallets
            FROM wallet_graph.wallet_holdings
            WHERE UPPER(symbol) = UPPER(%s)
              AND indexed_at BETWEEN NOW() - INTERVAL '96 hours' AND NOW() - INTERVAL '48 hours'
        """, (symbol,))

        cur_total = float(current["total"]) if current else 0
        prev_total = float(previous["total"]) if previous else 0

        if prev_total <= 0 or cur_total <= prev_total:
            continue

        flow_change_pct = ((cur_total - prev_total) / prev_total) * 100

        if sc["delta_24h"] < -5.0 and cur_total > 100_000_000:
            severity = "critical"
        elif sc["delta_24h"] < -3.0:
            severity = "alert"
        else:
            severity = "notable"

        results.append({
            "type": "quality_flow",
            "symbol": symbol,
            "current_score": sc.get("score"),
            "score_delta": sc["delta_24h"],
            "grade": sc.get("grade"),
            "current_holdings_usd": cur_total,
            "previous_holdings_usd": prev_total,
            "flow_change_pct": round(flow_change_pct, 1),
            "wallets_current": current.get("wallets", 0) if current else 0,
            "signal": f"{symbol} score down {abs(sc['delta_24h']):.1f} pts but holdings grew +{flow_change_pct:.1f}% (${cur_total / 1e6:.1f}M)",
            "severity": severity,
        })

    return results


# ---------------------------------------------------------------------------
# Combined
# ---------------------------------------------------------------------------

_SEVERITY_ORDER = {"critical": 3, "alert": 2, "notable": 1, "silent": 0}


def detect_all_divergences():
    """Run all divergence detectors and return combined results."""
    asset_divs = []
    wallet_divs = []
    flow_divs = []

    try:
        asset_divs = detect_asset_divergence()
    except Exception as e:
        logger.warning(f"Asset divergence detection failed: {e}")

    try:
        wallet_divs = detect_wallet_concentration_divergence()
    except Exception as e:
        logger.warning(f"Wallet concentration divergence detection failed: {e}")

    try:
        flow_divs = detect_quality_flow_divergence()
    except Exception as e:
        logger.warning(f"Quality-flow divergence detection failed: {e}")

    all_signals = asset_divs + wallet_divs + flow_divs
    all_signals.sort(
        key=lambda s: _SEVERITY_ORDER.get(s.get("severity", "silent"), 0),
        reverse=True,
    )

    return {
        "divergence_signals": all_signals,
        "summary": {
            "total_signals": len(all_signals),
            "asset_quality": len(asset_divs),
            "wallet_concentration": len(wallet_divs),
            "quality_flow": len(flow_divs),
            "alerts": sum(1 for s in all_signals if s.get("severity") == "alert"),
            "critical": sum(1 for s in all_signals if s.get("severity") == "critical"),
        },
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "version": "divergence-v1.0.0",
    }


# ---------------------------------------------------------------------------
# Spec
# ---------------------------------------------------------------------------

DIVERGENCE_SPEC = {
    "version": "1.0.0",
    "signal_types": [
        {
            "type": "asset_quality",
            "description": "Stablecoin score declining while capital flows in",
            "detection": "Score delta < -1.0 point over 24 hours",
            "severity_thresholds": {
                "notable": "score decline > 1 point",
                "alert": "score decline > 3 points",
                "critical": "score decline > 5 points with > $100M in holdings",
            },
        },
        {
            "type": "wallet_concentration",
            "description": "Wallet concentration increasing while value grows",
            "detection": "HHI increase > 300 AND value increase > 5%",
            "severity_thresholds": {
                "notable": "HHI change > 300",
                "alert": "HHI change > 1000",
                "critical": "HHI change > 2000 with value > $10M",
            },
        },
        {
            "type": "quality_flow",
            "description": "Stablecoin score declining with net inflows from wallet graph",
            "detection": "Score decline > 2 points AND aggregate holdings increased",
            "severity_thresholds": {
                "notable": "score decline > 2 points with any inflow",
                "alert": "score decline > 3 points with inflow",
                "critical": "score decline > 5 points with > $100M inflow",
            },
        },
    ],
}
