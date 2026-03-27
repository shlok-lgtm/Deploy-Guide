"""
Verification Agent — Assessor
==============================
Generates canonical assessment event objects from wallet state + SII scores.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone

from app.database import fetch_all, fetch_one
from app.indexer.scorer import compute_wallet_risk

logger = logging.getLogger(__name__)


def _keccak256_hex(data: bytes) -> str:
    """Compute keccak256 hash. Falls back to sha256 if pysha3 not available."""
    try:
        import sha3
        return "0x" + sha3.keccak_256(data).hexdigest()
    except ImportError:
        return "0x" + hashlib.sha256(data).hexdigest()


def _get_sii_scores() -> dict:
    """Fetch current SII scores for all stablecoins, keyed by symbol."""
    rows = fetch_all(
        "SELECT stablecoin_id, overall_score, grade FROM scores"
    )
    result = {}
    for r in rows:
        result[r["stablecoin_id"]] = {
            "score": r["overall_score"],
            "grade": r["grade"],
        }
    return result


def _get_sii_7d_deltas() -> dict:
    """Fetch 7-day score deltas from score_history, keyed by stablecoin_id."""
    rows = fetch_all("""
        SELECT s.stablecoin_id,
               s.overall_score - COALESCE(h.overall_score, s.overall_score) AS delta_7d
        FROM scores s
        LEFT JOIN score_history h
            ON h.stablecoin = s.stablecoin_id
            AND h.score_date = CURRENT_DATE - INTERVAL '7 days'
    """)
    return {r["stablecoin_id"]: round(r["delta_7d"] or 0, 2) for r in rows}


def _get_wallet_holdings(wallet_address: str) -> list[dict]:
    """Fetch current holdings for a wallet from wallet_graph."""
    rows = fetch_all("""
        SELECT symbol, value_usd, is_scored, sii_score, sii_grade, pct_of_wallet
        FROM wallet_graph.wallet_holdings
        WHERE wallet_address = %s
        AND indexed_at = (
            SELECT MAX(indexed_at) FROM wallet_graph.wallet_holdings
            WHERE wallet_address = %s
        )
    """, (wallet_address, wallet_address))
    return [dict(r) for r in rows]


def _get_previous_assessment(wallet_address: str) -> dict | None:
    """Get the most recent assessment for a wallet."""
    row = fetch_one("""
        SELECT wallet_risk_score, wallet_risk_grade, concentration_hhi,
               coverage_ratio, total_stablecoin_value, holdings_snapshot
        FROM assessment_events
        WHERE wallet_address = %s
        ORDER BY created_at DESC LIMIT 1
    """, (wallet_address,))
    return dict(row) if row else None


def generate_assessment(
    wallet_address: str,
    trigger_type: str,
    trigger_detail: dict,
    current_holdings: list[dict] | None = None,
    current_risk: dict | None = None,
    previous_assessment: dict | None = None,
    sii_scores: dict | None = None,
) -> dict | None:
    """
    Generate a canonical assessment event.

    Returns a dict matching the assessment_events schema, or None if
    the wallet has no holdings data.
    """
    # Fetch data if not provided
    if current_holdings is None:
        current_holdings = _get_wallet_holdings(wallet_address)
    if not current_holdings:
        return None

    if sii_scores is None:
        sii_scores = _get_sii_scores()

    deltas = _get_sii_7d_deltas()

    # Compute risk from holdings
    if current_risk is None:
        current_risk = compute_wallet_risk(current_holdings)
    if current_risk is None:
        return None

    if previous_assessment is None:
        previous_assessment = _get_previous_assessment(wallet_address)

    # Build holdings snapshot with SII data
    holdings_snapshot = []
    for h in current_holdings:
        symbol = h.get("symbol", "").lower()
        sii = sii_scores.get(symbol, {})
        raw_score = sii.get("score")
        holdings_snapshot.append({
            "symbol": h.get("symbol", "???"),
            "value_usd": float(round(h.get("value_usd", 0) or 0, 2)),
            "pct_of_wallet": float(h.get("pct_of_wallet", 0) or 0),
            "sii_score": float(raw_score) if raw_score is not None else None,
            "sii_grade": sii.get("grade"),
            "sii_7d_delta": float(deltas.get(symbol, 0)),
        })

    # Compute content hash from canonical payload
    canonical = {
        "wallet_address": wallet_address,
        "trigger_type": trigger_type,
        "trigger_detail": trigger_detail,
        "wallet_risk_score": current_risk.get("risk_score"),
        "concentration_hhi": current_risk.get("concentration_hhi"),
        "holdings_snapshot": holdings_snapshot,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    content_hash = _keccak256_hex(
        json.dumps(canonical, sort_keys=True, default=str).encode()
    )

    prev_score = (previous_assessment or {}).get("wallet_risk_score")
    prev_hhi = (previous_assessment or {}).get("concentration_hhi")

    return {
        "wallet_address": wallet_address,
        "chain": "ethereum",
        "trigger_type": trigger_type,
        "trigger_detail": trigger_detail,
        "wallet_risk_score": current_risk.get("risk_score"),
        "wallet_risk_grade": current_risk.get("risk_grade"),
        "wallet_risk_score_prev": prev_score,
        "concentration_hhi": current_risk.get("concentration_hhi"),
        "concentration_hhi_prev": prev_hhi,
        "coverage_ratio": round(
            (100 - current_risk.get("unscored_pct", 0)) / 100, 4
        ),
        "total_stablecoin_value": current_risk.get("total_stablecoin_value"),
        "holdings_snapshot": holdings_snapshot,
        "content_hash": content_hash,
        "methodology_version": "wallet-v1.0.0",
    }
