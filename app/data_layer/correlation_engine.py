"""
Tier 6: Cross-Entity Correlation Matrix
=========================================
Computes rolling correlation matrices across all scored entities.
No new API calls needed — computed from existing scored history.

When USDC depegs, which protocols lose TVL in sync? When gas spikes,
which bridges see volume? These matrices answer those questions.

Schedule: Daily
"""

import json
import logging
import math
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def _pearson_correlation(x: list[float], y: list[float]) -> Optional[float]:
    """Compute Pearson correlation coefficient between two series."""
    n = min(len(x), len(y))
    if n < 5:
        return None

    x = x[:n]
    y = y[:n]

    mean_x = sum(x) / n
    mean_y = sum(y) / n

    cov = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n)) / n
    std_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x) / n)
    std_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y) / n)

    if std_x == 0 or std_y == 0:
        return None

    return round(cov / (std_x * std_y), 4)


def compute_sii_correlation_matrix(window_days: int = 30) -> dict:
    """
    Compute correlation matrix of SII score changes across stablecoins.
    Uses daily score history.
    """
    from app.database import fetch_all

    # Get all stablecoins with history
    rows = fetch_all(
        """SELECT DISTINCT stablecoin FROM score_history
           WHERE score_date >= CURRENT_DATE - %s
           ORDER BY stablecoin""",
        (window_days,),
    )
    if not rows or len(rows) < 2:
        return {"error": "insufficient data for correlation matrix"}

    entity_ids = [r["stablecoin"] for r in rows]

    # Build score time series per entity
    series: dict[str, list[float]] = {}
    for entity_id in entity_ids:
        history = fetch_all(
            """SELECT score_date, overall_score FROM score_history
               WHERE stablecoin = %s AND score_date >= CURRENT_DATE - %s
               ORDER BY score_date""",
            (entity_id, window_days),
        )
        if history and len(history) >= 5:
            series[entity_id] = [float(h["overall_score"]) for h in history]

    # Compute pairwise correlations
    valid_ids = list(series.keys())
    n = len(valid_ids)
    matrix = [[None] * n for _ in range(n)]

    for i in range(n):
        for j in range(n):
            if i == j:
                matrix[i][j] = 1.0
            elif j > i:
                corr = _pearson_correlation(series[valid_ids[i]], series[valid_ids[j]])
                matrix[i][j] = corr
                matrix[j][i] = corr

    return {
        "entity_ids": valid_ids,
        "matrix": matrix,
        "window_days": window_days,
    }


def compute_cross_index_correlation(window_days: int = 30) -> dict:
    """
    Compute cross-index correlation (SII vs PSI vs RPI scores over time).
    """
    from app.database import fetch_all

    # SII daily scores
    sii_scores = fetch_all(
        """SELECT score_date, AVG(overall_score) as avg_score
           FROM score_history
           WHERE score_date >= CURRENT_DATE - %s
           GROUP BY score_date ORDER BY score_date""",
        (window_days,),
    )

    # PSI daily scores
    psi_scores = fetch_all(
        """SELECT DATE(scored_at) as score_date, AVG(overall_score) as avg_score
           FROM psi_scores
           WHERE scored_at >= NOW() - INTERVAL '%s days'
           GROUP BY DATE(scored_at) ORDER BY score_date""",
        (window_days,),
    )

    if not sii_scores or not psi_scores:
        return {"error": "insufficient data"}

    # Align by date
    sii_by_date = {str(r["score_date"]): float(r["avg_score"]) for r in sii_scores}
    psi_by_date = {str(r["score_date"]): float(r["avg_score"]) for r in psi_scores}

    common_dates = sorted(set(sii_by_date.keys()) & set(psi_by_date.keys()))
    if len(common_dates) < 5:
        return {"error": "insufficient overlapping data"}

    sii_series = [sii_by_date[d] for d in common_dates]
    psi_series = [psi_by_date[d] for d in common_dates]

    corr = _pearson_correlation(sii_series, psi_series)

    return {
        "sii_psi_correlation": corr,
        "window_days": window_days,
        "data_points": len(common_dates),
    }


def _sanitize_matrix(matrix_data: list) -> list:
    """Replace NaN/Infinity with None in a nested correlation matrix."""
    sanitized = []
    for row in matrix_data:
        sanitized_row = []
        for val in row:
            if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                sanitized_row.append(None)
            else:
                sanitized_row.append(val)
        sanitized.append(sanitized_row)
    return sanitized


def _store_matrix(matrix_type: str, window_days: int, entity_ids: list, matrix_data: list):
    """Store correlation matrix to database (per-row transaction)."""
    from app.database import get_cursor

    sanitized_data = _sanitize_matrix(matrix_data)

    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO correlation_matrices
                   (matrix_type, window_days, entity_ids, matrix_data, computed_at)
                   VALUES (%s, %s, %s::jsonb, %s::jsonb, NOW())
                   ON CONFLICT (matrix_type, window_days, computed_at) DO UPDATE SET
                       entity_ids = EXCLUDED.entity_ids,
                       matrix_data = EXCLUDED.matrix_data""",
                (
                    matrix_type, window_days,
                    json.dumps(entity_ids),
                    json.dumps(sanitized_data),
                ),
            )
    except Exception as e:
        logger.error(
            "Failed to store correlation matrix %s (window=%d): %s",
            matrix_type, window_days, e,
        )


def run_correlation_computation() -> dict:
    """
    Compute and store all correlation matrices.
    Daily schedule, no API calls needed.
    """
    results = {}

    # SII 30-day correlation
    try:
        sii_30 = compute_sii_correlation_matrix(30)
        if "entity_ids" in sii_30:
            _store_matrix("sii_30d", 30, sii_30["entity_ids"], sii_30["matrix"])
            results["sii_30d"] = {
                "entities": len(sii_30["entity_ids"]),
                "computed": True,
            }
        else:
            results["sii_30d"] = sii_30
    except Exception as e:
        logger.warning(f"SII 30d correlation failed: {e}")
        results["sii_30d"] = {"error": str(e)}

    # SII 90-day correlation
    try:
        sii_90 = compute_sii_correlation_matrix(90)
        if "entity_ids" in sii_90:
            _store_matrix("sii_90d", 90, sii_90["entity_ids"], sii_90["matrix"])
            results["sii_90d"] = {
                "entities": len(sii_90["entity_ids"]),
                "computed": True,
            }
        else:
            results["sii_90d"] = sii_90
    except Exception as e:
        logger.warning(f"SII 90d correlation failed: {e}")
        results["sii_90d"] = {"error": str(e)}

    # Cross-index correlation
    try:
        cross = compute_cross_index_correlation(30)
        results["cross_index_30d"] = cross
    except Exception as e:
        logger.warning(f"Cross-index correlation failed: {e}")
        results["cross_index_30d"] = {"error": str(e)}

    logger.info(f"Correlation computation complete: {results}")
    return results
