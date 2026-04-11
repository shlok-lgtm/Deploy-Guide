"""
Wallet Indexer — Scorer
=======================
Computes wallet-level risk score, concentration HHI, and coverage quality
from a wallet's stablecoin holdings.

Formula version: wallet-v1.0.0
"""

import logging
from typing import Optional

from app.indexer.config import (
    classify_size_tier,
    classify_coverage,
    FORMULA_VERSION,
)

logger = logging.getLogger(__name__)


def compute_wallet_risk(holdings: list[dict]) -> Optional[dict]:
    """
    Compute wallet risk score and enrichment signals from holdings.

    Args:
        holdings: list of dicts with keys:
            symbol, value_usd, is_scored, sii_score, sii_grade

    Returns:
        dict with risk_score, risk_grade, concentration_hhi, concentration_grade,
        unscored_pct, coverage_quality, composition stats, size_tier, etc.
        Returns None if holdings list is empty.
    """
    if not holdings:
        return None

    total_value = sum(h["value_usd"] for h in holdings)
    if total_value <= 0:
        return None

    # -- Wallet risk score (value-weighted avg SII, scored holdings only) --
    scored = [h for h in holdings if h.get("is_scored") and h.get("sii_score") is not None]
    unscored = [h for h in holdings if not h.get("is_scored") or h.get("sii_score") is None]

    risk_score = None
    if scored:
        total_scored_value = sum(h["value_usd"] for h in scored)
        if total_scored_value > 0:
            risk_score = sum(
                h["value_usd"] * h["sii_score"] for h in scored
            ) / total_scored_value
            risk_score = round(risk_score, 2)

    # -- Concentration HHI --
    shares = [(h["value_usd"] / total_value) * 100 for h in holdings]
    hhi = sum(s ** 2 for s in shares)
    # Normalize: higher = more diversified
    hhi_normalized = 100 - ((hhi / 10000) * 100)
    hhi_normalized = round(max(0, min(100, hhi_normalized)), 2)

    # -- Unscored exposure --
    unscored_value = sum(h["value_usd"] for h in unscored)
    unscored_pct = round((unscored_value / total_value) * 100, 2) if total_value > 0 else 0
    coverage_quality = classify_coverage(unscored_pct)

    # -- Dominant asset --
    dominant = max(holdings, key=lambda h: h["value_usd"])
    dominant_pct = round((dominant["value_usd"] / total_value) * 100, 2)

    # -- Size tier --
    size_tier = classify_size_tier(total_value)

    # -- Percent of wallet for each holding --
    for h in holdings:
        h["pct_of_wallet"] = round((h["value_usd"] / total_value) * 100, 2) if total_value > 0 else 0

    return {
        "risk_score": risk_score,
        "concentration_hhi": round(hhi, 2),
        "unscored_pct": unscored_pct,
        "coverage_quality": coverage_quality,
        "num_scored_holdings": len(scored),
        "num_unscored_holdings": len(unscored),
        "num_total_holdings": len(holdings),
        "dominant_asset": dominant.get("symbol", "???"),
        "dominant_asset_pct": dominant_pct,
        "total_stablecoin_value": round(total_value, 2),
        "size_tier": size_tier,
        "formula_version": FORMULA_VERSION,
    }
