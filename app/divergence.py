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


# ---------------------------------------------------------------------------
# Signal D: Protocol Solvency Divergence (PSI declining)
# ---------------------------------------------------------------------------

def detect_protocol_divergence():
    """Detect protocols where PSI score is declining day-over-day."""
    results = []

    rows = fetch_all("""
        WITH latest AS (
            SELECT DISTINCT ON (protocol_slug)
                protocol_slug, protocol_name, overall_score, computed_at
            FROM psi_scores ORDER BY protocol_slug, computed_at DESC
        ),
        previous AS (
            SELECT DISTINCT ON (protocol_slug)
                protocol_slug, overall_score AS prev_score
            FROM psi_scores
            WHERE scored_date < CURRENT_DATE
            ORDER BY protocol_slug, computed_at DESC
        )
        SELECT l.protocol_slug, l.protocol_name, l.overall_score,
               p.prev_score,
               (l.overall_score - p.prev_score) AS delta
        FROM latest l
        LEFT JOIN previous p ON l.protocol_slug = p.protocol_slug
        WHERE p.prev_score IS NOT NULL
          AND (l.overall_score - p.prev_score) < -2.0
    """)

    for r in rows:
        delta = float(r["delta"])
        if delta < -10:
            severity = "critical"
        elif delta < -5:
            severity = "alert"
        else:
            severity = "notable"

        results.append({
            "type": "protocol_solvency",
            "protocol": r["protocol_slug"],
            "protocol_name": r.get("protocol_name", r["protocol_slug"]),
            "current_score": float(r["overall_score"]),
            "previous_score": float(r["prev_score"]),
            "score_delta": round(delta, 1),
            "signal": f"{r.get('protocol_name', r['protocol_slug'])} PSI declined {abs(delta):.1f} pts ({float(r['prev_score']):.0f} → {float(r['overall_score']):.0f})",
            "severity": severity,
        })

    return results


# ---------------------------------------------------------------------------
# Signal E: Cross-Index Divergence (SII stable + PSI declining = CQI gap)
# ---------------------------------------------------------------------------

def detect_cross_index_divergence():
    """
    Detect when SII and PSI move in opposite directions for related pairs.
    A stablecoin SII stable/improving while a protocol holding it has declining PSI
    means the CQI gap is widening — composed risk is worse than SII alone suggests.
    """
    results = []

    # SII deltas from daily pulse
    latest_pulse = fetch_one(
        "SELECT summary FROM daily_pulses ORDER BY pulse_date DESC LIMIT 1"
    )
    if not latest_pulse or not latest_pulse.get("summary"):
        return results

    summary = latest_pulse["summary"]
    if isinstance(summary, str):
        summary = json.loads(summary)

    sii_scores = {
        s["symbol"].lower(): s
        for s in summary.get("scores", [])
        if s.get("symbol")
    }

    # PSI deltas
    psi_rows = fetch_all("""
        WITH latest AS (
            SELECT DISTINCT ON (protocol_slug)
                protocol_slug, overall_score, computed_at
            FROM psi_scores ORDER BY protocol_slug, computed_at DESC
        ),
        previous AS (
            SELECT DISTINCT ON (protocol_slug)
                protocol_slug, overall_score AS prev_score
            FROM psi_scores WHERE scored_date < CURRENT_DATE
            ORDER BY protocol_slug, computed_at DESC
        )
        SELECT l.protocol_slug, l.overall_score AS psi_score,
               p.prev_score AS psi_prev,
               (l.overall_score - p.prev_score) AS psi_delta
        FROM latest l
        LEFT JOIN previous p ON l.protocol_slug = p.protocol_slug
        WHERE p.prev_score IS NOT NULL
    """)

    # Protocol → stablecoins exposure map
    try:
        exposure_rows = fetch_all("""
            SELECT DISTINCT protocol_slug, token_symbol
            FROM protocol_collateral_exposure
            WHERE is_stablecoin = TRUE AND tvl_usd > 100000
              AND snapshot_date = (SELECT MAX(snapshot_date) FROM protocol_collateral_exposure)
        """)
    except Exception:
        exposure_rows = []

    proto_stables: dict[str, list[str]] = {}
    for r in exposure_rows:
        slug = r["protocol_slug"]
        sym = r["token_symbol"].lower()
        if slug not in proto_stables:
            proto_stables[slug] = []
        proto_stables[slug].append(sym)

    for psi_row in psi_rows:
        slug = psi_row["protocol_slug"]
        psi_delta = float(psi_row["psi_delta"])

        if psi_delta >= -3.0:
            continue

        stables = proto_stables.get(slug, [])
        for sym in stables:
            sii_entry = sii_scores.get(sym)
            if not sii_entry:
                continue

            sii_delta = sii_entry.get("delta_24h", 0) or 0

            # Divergence: SII flat/up but PSI down
            if sii_delta >= -1.0:
                severity = "alert" if psi_delta < -5 else "notable"
                results.append({
                    "type": "cross_index",
                    "stablecoin": sym.upper(),
                    "protocol": slug,
                    "sii_score": sii_entry.get("score"),
                    "sii_delta": round(sii_delta, 1),
                    "psi_score": float(psi_row["psi_score"]),
                    "psi_delta": round(psi_delta, 1),
                    "signal": f"{sym.upper()} SII stable ({sii_delta:+.1f}) but {slug} PSI declining ({psi_delta:+.1f}) — CQI gap widening",
                    "severity": severity,
                })

    return results


# ---------------------------------------------------------------------------
# Primitive #21: Actor-flow divergence
# ---------------------------------------------------------------------------

def detect_actor_flow_divergence():
    """Detect when agents and humans flow in opposite directions for a stablecoin.

    Agents exiting while humans entering = information asymmetry signal.
    Uses wallet_graph.wallet_edges joined with actor_classifications to compute
    net flow direction by actor type over the last 7 days.
    """
    results = []

    # Get all scored stablecoins with contract addresses
    coins = fetch_all(
        """
        SELECT s.stablecoin_id, st.symbol, st.contract
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        WHERE st.contract IS NOT NULL AND st.contract != ''
        """
    )

    for coin in coins:
        contract = coin["contract"].lower()
        symbol = coin["symbol"]

        # Compute net flow by actor type over last 7 days
        # Positive = net inflow (more received than sent), Negative = net outflow
        flow_rows = fetch_all(
            """
            SELECT
                COALESCE(ac.actor_type, 'unknown') AS actor_type,
                SUM(CASE WHEN LOWER(e.to_address) = wh.wallet_address THEN e.total_value_usd
                         WHEN LOWER(e.from_address) = wh.wallet_address THEN -e.total_value_usd
                         ELSE 0 END) AS net_flow_usd,
                COUNT(DISTINCT wh.wallet_address) AS wallet_count
            FROM wallet_graph.wallet_holdings wh
            JOIN wallet_graph.wallet_edges e
                ON (LOWER(e.from_address) = wh.wallet_address OR LOWER(e.to_address) = wh.wallet_address)
            LEFT JOIN wallet_graph.actor_classifications ac
                ON ac.wallet_address = wh.wallet_address
            WHERE LOWER(wh.token_address) = %s
              AND wh.indexed_at > NOW() - INTERVAL '7 days'
              AND e.last_transfer_at > NOW() - INTERVAL '7 days'
            GROUP BY COALESCE(ac.actor_type, 'unknown')
            """,
            (contract,),
        )

        if len(flow_rows) < 2:
            continue

        flows = {r["actor_type"]: float(r["net_flow_usd"] or 0) for r in flow_rows}
        agent_flow = flows.get("autonomous_agent", 0)
        human_flow = flows.get("human", 0)

        # Detect opposing flows with meaningful magnitude
        if abs(agent_flow) < 1000 or abs(human_flow) < 1000:
            continue

        # Agents exiting, humans entering = information asymmetry (most concerning)
        if agent_flow < 0 and human_flow > 0:
            magnitude = min(abs(agent_flow), abs(human_flow))
            severity = "critical" if magnitude > 100000 else "alert" if magnitude > 10000 else "notable"
            results.append({
                "type": "actor_flow_divergence",
                "symbol": symbol,
                "agent_net_flow_usd": round(agent_flow, 2),
                "human_net_flow_usd": round(human_flow, 2),
                "signal": f"Agents net-exiting {symbol} (${abs(agent_flow):,.0f}) while humans net-entering (${human_flow:,.0f})",
                "direction": "agent_exit_human_enter",
                "magnitude": round(magnitude, 2),
                "severity": severity,
            })
        # Humans exiting, agents entering = potential accumulation
        elif human_flow < 0 and agent_flow > 0:
            magnitude = min(abs(agent_flow), abs(human_flow))
            severity = "notable"
            results.append({
                "type": "actor_flow_divergence",
                "symbol": symbol,
                "agent_net_flow_usd": round(agent_flow, 2),
                "human_net_flow_usd": round(human_flow, 2),
                "signal": f"Agents net-entering {symbol} (${agent_flow:,.0f}) while humans net-exiting (${abs(human_flow):,.0f})",
                "direction": "agent_enter_human_exit",
                "magnitude": round(magnitude, 2),
                "severity": severity,
            })

    return results


def detect_all_divergences():
    """Run all divergence detectors and return combined results."""
    asset_divs = []
    wallet_divs = []
    flow_divs = []
    protocol_divs = []
    cross_divs = []

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

    try:
        protocol_divs = detect_protocol_divergence()
    except Exception as e:
        logger.warning(f"Protocol solvency divergence detection failed: {e}")

    try:
        cross_divs = detect_cross_index_divergence()
    except Exception as e:
        logger.warning(f"Cross-index divergence detection failed: {e}")

    actor_divs = []
    try:
        actor_divs = detect_actor_flow_divergence()
    except Exception as e:
        logger.warning(f"Actor-flow divergence detection failed: {e}")

    all_signals = asset_divs + wallet_divs + flow_divs + protocol_divs + cross_divs + actor_divs
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
            "protocol_solvency": len(protocol_divs),
            "cross_index": len(cross_divs),
            "actor_flow": len(actor_divs),
            "alerts": sum(1 for s in all_signals if s.get("severity") == "alert"),
            "critical": sum(1 for s in all_signals if s.get("severity") == "critical"),
        },
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "version": "divergence-v1.2.0",
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
