"""
PSI Temporal Reconstruction Engine
=====================================
Reconstructs PSI scores at any historical date using:
- DeFiLlama TVL history (from historical_protocol_data table)
- CoinGecko token price/mcap/volume history
- Carry-forward for static components (audits, admin key, bad debt)
- Calculated components derived from TVL + token data

Same pattern as SII temporal_engine.py — assembles raw_values, feeds them
to score_entity(), returns score + provenance + confidence.
"""

import logging
import statistics
from datetime import date, timedelta

from app.database import fetch_one, fetch_all
from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
from app.scoring_engine import score_entity
from app.collectors.psi_collector import (
    _PROTOCOL_ADMIN_SCORES,
    KNOWN_BAD_DEBT,
)

logger = logging.getLogger(__name__)


def _get_historical_data(slug: str, target: date) -> dict | None:
    """Get the closest historical data point on or before target date."""
    row = fetch_one("""
        SELECT * FROM historical_protocol_data
        WHERE protocol_slug = %s AND record_date <= %s
        ORDER BY record_date DESC LIMIT 1
    """, (slug, target))
    return dict(row) if row else None


def _get_tvl_series(slug: str, target: date, lookback_days: int = 30) -> list[float]:
    """Get daily TVL values for the lookback period."""
    from_date = target - timedelta(days=lookback_days)
    rows = fetch_all("""
        SELECT tvl FROM historical_protocol_data
        WHERE protocol_slug = %s AND record_date BETWEEN %s AND %s AND tvl IS NOT NULL
        ORDER BY record_date ASC
    """, (slug, from_date, target))
    return [float(r["tvl"]) for r in rows if r.get("tvl")]


def _get_price_series(slug: str, target: date, lookback_days: int = 30) -> list[float]:
    """Get daily token price values for the lookback period."""
    from_date = target - timedelta(days=lookback_days)
    rows = fetch_all("""
        SELECT token_price FROM historical_protocol_data
        WHERE protocol_slug = %s AND record_date BETWEEN %s AND %s AND token_price IS NOT NULL
        ORDER BY record_date ASC
    """, (slug, from_date, target))
    return [float(r["token_price"]) for r in rows if r.get("token_price")]


def _get_latest_psi_raw(slug: str) -> dict:
    """Get the most recent live PSI raw_values for carry-forward of pool/fee components."""
    row = fetch_one("""
        SELECT raw_values FROM psi_scores
        WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1
    """, (slug,))
    if row and row.get("raw_values"):
        rv = row["raw_values"]
        return rv if isinstance(rv, dict) else {}
    return {}


def reconstruct_psi_score(slug: str, target_date: date) -> dict:
    """
    Reconstruct a PSI score for a protocol at a historical date.
    Assembles raw_values from historical data + carry-forward, then
    runs them through score_entity(PSI_V01_DEFINITION, raw_values).
    """
    raw_values = {}
    sources = {}  # track provenance per component

    # 1. Historical data point (TVL + token)
    hist = _get_historical_data(slug, target_date)
    if not hist:
        return {
            "protocol_slug": slug,
            "target_date": target_date.isoformat(),
            "error": f"No historical data for {slug} at {target_date}",
            "confidence": "none",
        }

    # 2. TVL components
    tvl_val = float(hist["tvl"]) if hist.get("tvl") else None
    if tvl_val and tvl_val > 0:
        raw_values["tvl"] = tvl_val
        sources["tvl"] = "historical"

        raw_values["protocol_dex_tvl"] = tvl_val
        sources["protocol_dex_tvl"] = "historical_proxy"

        chain_count = int(hist["chain_count"]) if hist.get("chain_count") else None
        if chain_count:
            raw_values["chain_count"] = chain_count
            sources["chain_count"] = "historical"
            raw_values["pool_depth"] = chain_count * 3
            sources["pool_depth"] = "estimated"

        # TVL changes from series
        tvl_series = _get_tvl_series(slug, target_date, 30)
        if tvl_series and len(tvl_series) >= 2:
            tvl_30d_ago = tvl_series[0]
            if tvl_30d_ago > 0:
                raw_values["tvl_30d_change"] = ((tvl_val - tvl_30d_ago) / tvl_30d_ago) * 100
                sources["tvl_30d_change"] = "calculated_from_historical"

            if len(tvl_series) >= 7:
                tvl_7d_ago = tvl_series[-min(7, len(tvl_series))]
                if tvl_7d_ago > 0:
                    raw_values["tvl_7d_change"] = ((tvl_val - tvl_7d_ago) / tvl_7d_ago) * 100
                    sources["tvl_7d_change"] = "calculated_from_historical"

    # 3. Token components
    token_mcap = float(hist["token_mcap"]) if hist.get("token_mcap") else None
    token_volume = float(hist["token_volume"]) if hist.get("token_volume") else None

    if token_mcap:
        raw_values["token_mcap"] = token_mcap
        sources["token_mcap"] = "historical"
    if token_volume:
        raw_values["token_volume_24h"] = token_volume
        sources["token_volume_24h"] = "historical"

    # Token liquidity depth (volume/mcap ratio)
    if token_volume and token_mcap and token_mcap > 0:
        raw_values["token_liquidity_depth"] = token_volume / token_mcap
        sources["token_liquidity_depth"] = "calculated_from_historical"

    # mcap/tvl ratio
    if token_mcap and tvl_val and tvl_val > 0:
        raw_values["mcap_tvl_ratio"] = token_mcap / tvl_val
        sources["mcap_tvl_ratio"] = "calculated_from_historical"

    # Token volatility from price series
    price_series = _get_price_series(slug, target_date, 30)
    if len(price_series) >= 7:
        try:
            daily_returns = [
                (price_series[i] - price_series[i - 1]) / price_series[i - 1]
                for i in range(1, len(price_series))
                if price_series[i - 1] > 0
            ]
            if len(daily_returns) > 1:
                raw_values["token_price_volatility_30d"] = statistics.stdev(daily_returns) * 100
                sources["token_price_volatility_30d"] = "calculated_from_historical"
        except Exception:
            pass

    # 4. Carry-forward from latest live scoring (fees, utilization, audits)
    latest_raw = _get_latest_psi_raw(slug)
    carry_forward_keys = [
        "fees_30d", "revenue_30d", "fees_tvl_ratio", "fees_tvl_efficiency",
        "utilization_rate", "audit_count", "audit_recency_days",
        "governance_token_holders", "governance_proposals_90d",
    ]
    for key in carry_forward_keys:
        if key not in raw_values and key in latest_raw:
            raw_values[key] = latest_raw[key]
            sources[key] = "carry_forward"

    # 5. Static config components
    admin_score = _PROTOCOL_ADMIN_SCORES.get(slug, 50)
    raw_values["protocol_admin_key_risk"] = admin_score
    sources["protocol_admin_key_risk"] = "config_static"

    bad_debt_entry = KNOWN_BAD_DEBT.get(slug, 0)
    if isinstance(bad_debt_entry, dict):
        since = date.fromisoformat(bad_debt_entry["since"])
        bad_debt = bad_debt_entry["amount"] if target_date >= since else 0
    else:
        bad_debt = bad_debt_entry
    if tvl_val and tvl_val > 0:
        raw_values["bad_debt_ratio"] = (bad_debt / tvl_val) * 100
    else:
        raw_values["bad_debt_ratio"] = 0
    sources["bad_debt_ratio"] = "config_static"

    # 6. Score via the generic scoring engine
    result = score_entity(PSI_V01_DEFINITION, raw_values)

    # Confidence based on coverage
    scored = result.get("components_available", 0)
    total = result.get("components_total", 24)
    if scored >= 18:
        confidence = "high"
    elif scored >= 12:
        confidence = "medium"
    elif scored >= 6:
        confidence = "low"
    else:
        confidence = "very_low"

    # Build component detail
    component_detail = {}
    for comp_id, norm_score in result.get("component_scores", {}).items():
        component_detail[comp_id] = {
            "raw_value": raw_values.get(comp_id),
            "normalized_score": norm_score,
            "source": sources.get(comp_id, "unknown"),
        }

    return {
        "protocol_slug": slug,
        "target_date": target_date.isoformat(),
        "score": result["overall_score"],
        "confidence": confidence,
        "components_scored": scored,
        "components_total": total,
        "coverage": result.get("coverage", 0),
        "category_scores": result.get("category_scores", {}),
        "components": component_detail,
        "reconstructed_at": target_date.isoformat(),
    }


def reconstruct_psi_range(slug: str, from_date: date, to_date: date) -> list[dict]:
    """Reconstruct daily PSI scores for a date range."""
    results = []
    current = from_date
    while current <= to_date:
        result = reconstruct_psi_score(slug, current)
        if result.get("score", 0) > 0:
            results.append({
                "date": current.isoformat(),
                "score": result["score"],
                        "confidence": result["confidence"],
                "components_scored": result["components_scored"],
                "category_scores": result.get("category_scores", {}),
            })
        current += timedelta(days=1)
    return results
