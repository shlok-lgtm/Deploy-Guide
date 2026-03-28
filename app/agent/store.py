"""
Verification Agent — Store
============================
Writes assessment events to the canonical database.
Every event is persisted, even silent ones.
"""

import json
import logging

from app.computation_attestation import compute_inputs_hash
from app.database import execute, fetch_one

logger = logging.getLogger(__name__)


def store_assessment(assessment: dict) -> str | None:
    """
    Insert assessment event into canonical database.
    Returns the UUID of the created event, or None if skipped.

    The only condition that prevents storage is a duplicate
    content_hash within the same hour (idempotency guard).
    """
    content_hash = assessment.get("content_hash")

    # Idempotency: skip if same content_hash exists in the last hour
    if content_hash:
        existing = fetch_one("""
            SELECT id FROM assessment_events
            WHERE content_hash = %s
            AND created_at > NOW() - INTERVAL '1 hour'
        """, (content_hash,))
        if existing:
            logger.debug(f"Duplicate assessment skipped (hash: {content_hash[:16]}...)")
            return None

    holdings_json = json.dumps(assessment.get("holdings_snapshot", []), default=str)
    trigger_json = json.dumps(assessment.get("trigger_detail", {}), default=str)

    # Compute inputs hash for computation attestation
    holdings_snapshot = assessment.get("holdings_snapshot", [])
    formula_ver = assessment.get("methodology_version", "wallet-v1.0.0")
    inputs_hash = None
    inputs_summary = None
    try:
        # Collect stablecoin scores for inputs hash
        component_scores_for_hash = {}
        for h in holdings_snapshot:
            sym = h.get("symbol", "")
            sii = h.get("sii_score")
            if sym and sii is not None:
                component_scores_for_hash[sym] = float(sii)

        inputs_hash, inputs_summary = compute_inputs_hash(
            component_scores=component_scores_for_hash,
            wallet_holdings=holdings_snapshot,
            formula_version=formula_ver,
        )
        inputs_summary_json = json.dumps(inputs_summary, default=str)
    except Exception:
        logger.debug("Could not compute inputs_hash, storing without it")
        inputs_summary_json = None

    row = fetch_one("""
        INSERT INTO assessment_events (
            wallet_address, chain, trigger_type, trigger_detail,
            wallet_risk_score, wallet_risk_grade,
            wallet_risk_score_prev, concentration_hhi,
            concentration_hhi_prev, coverage_ratio,
            total_stablecoin_value, holdings_snapshot,
            severity, broadcast, content_hash, methodology_version,
            inputs_hash, inputs_summary
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s
        ) RETURNING id::text
    """, (
        assessment["wallet_address"],
        assessment.get("chain", "ethereum"),
        assessment["trigger_type"],
        trigger_json,
        assessment.get("wallet_risk_score"),
        assessment.get("wallet_risk_grade"),
        assessment.get("wallet_risk_score_prev"),
        assessment.get("concentration_hhi"),
        assessment.get("concentration_hhi_prev"),
        assessment.get("coverage_ratio"),
        assessment.get("total_stablecoin_value"),
        holdings_json,
        assessment.get("severity", "silent"),
        assessment.get("broadcast", False),
        content_hash,
        assessment.get("methodology_version", "wallet-v1.0.0"),
        inputs_hash,
        inputs_summary_json,
    ))

    event_id = row["id"] if row else None
    if event_id:
        logger.info(
            f"Assessment stored: {event_id} | "
            f"wallet={assessment['wallet_address'][:10]}... | "
            f"trigger={assessment['trigger_type']} | "
            f"severity={assessment.get('severity')}"
        )

        # Store full input vector for computation attestation
        try:
            stablecoin_scores = {}
            for h in holdings_snapshot:
                symbol = h.get("symbol", "")
                if symbol:
                    score_row = fetch_one(
                        "SELECT overall_score, grade FROM scores s JOIN stablecoins st ON st.id = s.stablecoin_id WHERE UPPER(st.symbol) = UPPER(%s)",
                        (symbol,)
                    )
                    if score_row:
                        stablecoin_scores[symbol] = {
                            "score": float(score_row["overall_score"]) if score_row.get("overall_score") else None,
                            "grade": score_row.get("grade")
                        }

            execute("""
                INSERT INTO assessment_input_vectors
                    (assessment_id, wallet_address, holdings, stablecoin_scores, formula_version, inputs_hash)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (assessment_id) DO NOTHING
            """, (
                event_id,
                assessment["wallet_address"],
                json.dumps(holdings_snapshot, default=str),
                json.dumps(stablecoin_scores, default=str),
                formula_ver,
                inputs_hash,
            ))
        except Exception as e:
            logger.debug(f"Could not store input vector: {e}")

    return event_id
