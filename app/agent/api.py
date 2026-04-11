"""
Verification Agent — API Routes
==================================
REST endpoints for assessment events and daily pulses.
"""

import json
import logging
from datetime import date

from fastapi import FastAPI, Query, HTTPException

from app.database import fetch_all, fetch_one

logger = logging.getLogger(__name__)


def register_agent_routes(app: FastAPI) -> None:
    """Register assessment and pulse API routes on the FastAPI app."""

    @app.get("/api/assessments")
    async def get_assessments(
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
        severity: str | None = Query(None),
        trigger_type: str | None = Query(None),
    ):
        """Recent assessment events, paginated."""
        conditions = []
        params = []

        if severity:
            conditions.append("severity = %s")
            params.append(severity)
        if trigger_type:
            conditions.append("trigger_type = %s")
            params.append(trigger_type)

        where = ""
        if conditions:
            where = "WHERE " + " AND ".join(conditions)

        params.extend([limit, offset])

        rows = fetch_all(f"""
            SELECT id::text, created_at, wallet_address, chain,
                   trigger_type, trigger_detail,
                   wallet_risk_score,
                   wallet_risk_score_prev, concentration_hhi,
                   concentration_hhi_prev, coverage_ratio,
                   total_stablecoin_value, holdings_snapshot,
                   severity, broadcast, content_hash,
                   methodology_version, page_url
            FROM assessment_events
            {where}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """, tuple(params))

        count_row = fetch_one(f"""
            SELECT COUNT(*) AS total FROM assessment_events {where}
        """, tuple(params[:-2]) if params[:-2] else None)

        return {
            "assessments": [_serialize_assessment(r) for r in rows],
            "total": count_row["total"] if count_row else 0,
            "limit": limit,
            "offset": offset,
        }

    @app.get("/api/assessments/{assessment_id}")
    async def get_assessment(assessment_id: str):
        """Specific assessment event by UUID."""
        row = fetch_one("""
            SELECT id::text, created_at, wallet_address, chain,
                   trigger_type, trigger_detail,
                   wallet_risk_score,
                   wallet_risk_score_prev, concentration_hhi,
                   concentration_hhi_prev, coverage_ratio,
                   total_stablecoin_value, holdings_snapshot,
                   severity, broadcast, content_hash,
                   methodology_version, page_url,
                   onchain_tx, social_posted_at, onchain_posted_at
            FROM assessment_events
            WHERE id::text = %s
        """, (assessment_id,))
        if not row:
            raise HTTPException(status_code=404, detail="Assessment not found")
        return _serialize_assessment(row)

    @app.get("/api/assessments/wallet/{address}")
    async def get_wallet_assessments(
        address: str,
        limit: int = Query(50, ge=1, le=500),
    ):
        """Assessment history for a specific wallet."""
        rows = fetch_all("""
            SELECT id::text, created_at, wallet_address, chain,
                   trigger_type, trigger_detail,
                   wallet_risk_score,
                   wallet_risk_score_prev, concentration_hhi,
                   severity, broadcast, content_hash,
                   total_stablecoin_value
            FROM assessment_events
            WHERE wallet_address = %s
            ORDER BY created_at DESC
            LIMIT %s
        """, (address.lower(), limit))
        return {
            "wallet_address": address.lower(),
            "assessments": [_serialize_assessment(r) for r in rows],
            "count": len(rows),
        }

    @app.get("/api/pulse/latest")
    async def get_latest_pulse():
        """Most recent daily pulse summary."""
        row = fetch_one("""
            SELECT id, pulse_date, created_at, summary, page_url
            FROM daily_pulses
            ORDER BY pulse_date DESC LIMIT 1
        """)
        if not row:
            raise HTTPException(status_code=404, detail="No pulse data available yet")
        return _serialize_pulse(row)

    @app.get("/api/pulse/{pulse_date}")
    async def get_pulse(pulse_date: str):
        """Daily pulse summary for a specific date (YYYY-MM-DD)."""
        row = fetch_one("""
            SELECT id, pulse_date, created_at, summary, page_url
            FROM daily_pulses
            WHERE pulse_date = %s
        """, (pulse_date,))
        if not row:
            raise HTTPException(status_code=404, detail="No pulse for that date")
        return _serialize_pulse(row)

    logger.info("Agent API routes registered: /api/assessments, /api/pulse")


def _serialize_assessment(row: dict) -> dict:
    """Serialize an assessment row for JSON response."""
    result = dict(row)
    # Ensure JSON fields are parsed
    for key in ("trigger_detail", "holdings_snapshot"):
        val = result.get(key)
        if isinstance(val, str):
            try:
                result[key] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                pass
    # ISO format datetimes
    for key in ("created_at", "social_posted_at", "onchain_posted_at"):
        val = result.get(key)
        if val and hasattr(val, "isoformat"):
            result[key] = val.isoformat()
    return result


def _serialize_pulse(row: dict) -> dict:
    """Serialize a pulse row for JSON response."""
    result = dict(row)
    if isinstance(result.get("summary"), str):
        try:
            result["summary"] = json.loads(result["summary"])
        except (json.JSONDecodeError, TypeError):
            pass
    for key in ("pulse_date", "created_at"):
        val = result.get(key)
        if val and hasattr(val, "isoformat"):
            result[key] = val.isoformat()
    return result
