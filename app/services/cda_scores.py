"""
CDA → SII bridge.
Reads latest vendor extractions and returns off-chain component scores
that the SII scoring pipeline can consume.

Called by the offline collector for each stablecoin.
If no CDA data exists, returns empty list — scoring continues with existing data.
"""

import asyncio
import logging
from datetime import datetime, timezone

from app.database import fetch_one, fetch_one_async, fetch_all_async, execute_async
from app.scoring import normalize_inverse_linear, normalize_linear, normalize_direct

logger = logging.getLogger(__name__)

# Auditor quality tiers (mirrors offline.py but adds Reducto-extracted auditor names)
BIG_FOUR = {"deloitte", "kpmg", "pwc", "ey", "ernst & young", "pricewaterhousecoopers"}
MAJOR_FIRMS = {"grant thornton", "bdo", "rsm", "mazars", "moore", "withumsmith+brown",
               "withum", "state street", "ankura", "bitgo trust company", "prescient assurance"}
CRYPTO_AUDITORS = {"armanino", "the network firm", "mha", "network firm", "various custodians"}


def _parse_date(s: str) -> datetime:
    """Parse a date string in various formats into a timezone-aware datetime."""
    from dateutil import parser as dateutil_parser
    dt = dateutil_parser.parse(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _unwrap_citations(obj):
    """Recursively unwrap Reducto citation-wrapped values.

    Reducto returns {"value": X, "citations": [...]} for each field.
    This flattens to just X, and recurses into nested dicts.
    """
    if isinstance(obj, dict):
        if "value" in obj and "citations" in obj:
            return _unwrap_citations(obj["value"])
        return {k: _unwrap_citations(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_unwrap_citations(item) for item in obj]
    return obj


async def get_cda_components(stablecoin_id: str) -> list[dict]:
    """
    Get off-chain component scores from CDA vendor extractions.

    Returns a list of component dicts compatible with the scoring pipeline:
    [{"component_id": ..., "category": ..., "raw_value": ..., "normalized_score": ..., "data_source": ...}]

    Returns empty list if no CDA data exists — scoring falls through to
    existing offline collectors.
    """
    # Get latest high-confidence PDF extraction for this asset
    # Map stablecoin_id (lowercase, e.g. "usdc") to symbol (uppercase, e.g. "USDC")
    latest = await fetch_one_async(
        """
        SELECT structured_data, confidence_score, extracted_at, extraction_method, source_type
        FROM cda_vendor_extractions
        WHERE LOWER(asset_symbol) = %s
          AND confidence_score > 0.3
          AND structured_data IS NOT NULL
        ORDER BY
            CASE WHEN source_type = 'pdf_attestation' THEN 0 ELSE 1 END,
            extracted_at DESC
        LIMIT 1
        """,
        (stablecoin_id.lower(),),
    )

    if not latest or not latest.get("structured_data"):
        return []

    data = latest["structured_data"]
    confidence = latest.get("confidence_score", 0.5)
    extracted_at = latest.get("extracted_at")
    method = latest.get("extraction_method", "unknown")

    # Only use high-quality extractions for scoring
    if confidence < 0.5:
        logger.debug(f"CDA data for {stablecoin_id} too low confidence ({confidence:.2f}), skipping")
        return []

    components = []
    source = f"cda_{method}"

    # Unwrap Reducto citation-wrapped values: {"value": X, "citations": [...]} → X
    data = _unwrap_citations(data)

    # 1. Attestation Freshness (category: transparency)
    att_date_str = data.get("attestation_date")
    if att_date_str:
        try:
            att_date = _parse_date(str(att_date_str))
            days_old = (datetime.now(timezone.utc) - att_date).days
            freshness_score = normalize_inverse_linear(days_old, 0, 180)
            components.append({
                "component_id": "attestation_freshness",
                "category": "transparency",
                "raw_value": days_old,
                "normalized_score": round(freshness_score, 2),
                "data_source": source,
            })
        except asyncio.CancelledError:
            raise
        except (ValueError, TypeError) as e:
            logger.warning(f"cda_scores: get_cda_components attestation_freshness parse failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="services_get_cda_components_attestation_freshness_parse_failure",
                    error_message=str(e)[:500],
                    cycle_phase="cda_scores",
                )
            except Exception:
                pass

    # 2. Reserve-to-Supply Ratio (category: transparency)
    reserves = data.get("total_reserves_usd")
    supply = data.get("total_supply")
    if reserves and supply and supply > 0:
        ratio = reserves / supply
        ratio_score = normalize_linear(ratio, 0.98, 1.02)
        components.append({
            "component_id": "reserve_to_supply_ratio",
            "category": "transparency",
            "raw_value": round(ratio, 4),
            "normalized_score": round(ratio_score, 2),
            "data_source": source,
        })

    # 3. Cash Equivalents Percentage (category: transparency)
    comp = data.get("reserve_composition", {})
    if comp and isinstance(comp, dict):
        safe_pct = sum(filter(None, [
            comp.get("cash_and_deposits_pct"),
            comp.get("us_treasury_bills_pct"),
            comp.get("reverse_repo_pct"),
            comp.get("money_market_funds_pct"),
        ]))
        if safe_pct > 0:
            components.append({
                "component_id": "cash_equivalents_pct",
                "category": "transparency",
                "raw_value": round(safe_pct, 2),
                "normalized_score": round(normalize_direct(min(safe_pct, 100)), 2),
                "data_source": source,
            })

    # 4. Auditor Quality (category: transparency)
    auditor = (data.get("auditor_name") or "").lower().strip()
    if auditor:
        if any(name in auditor for name in BIG_FOUR):
            auditor_score = 100
        elif any(name in auditor for name in MAJOR_FIRMS):
            auditor_score = 80
        elif any(name in auditor for name in CRYPTO_AUDITORS):
            auditor_score = 60
        else:
            auditor_score = 40
        components.append({
            "component_id": "auditor_quality",
            "category": "transparency",
            "raw_value": auditor_score,
            "normalized_score": auditor_score,
            "data_source": source,
        })

    if components:
        logger.info(
            f"CDA: {stablecoin_id} — {len(components)} components from {method} "
            f"(confidence: {confidence:.2f})"
        )

    return components


async def get_cda_components_typed(stablecoin_id: str, disclosure_type: str = None) -> list[dict]:
    """Type-aware CDA component extraction.
    Routes to the appropriate extraction logic based on disclosure type.
    Falls back to the original fiat-reserve extraction if type is unknown."""
    if not disclosure_type or disclosure_type == "fiat-reserve":
        return await get_cda_components(stablecoin_id)

    if disclosure_type in ("overcollateralized", "algorithmic"):
        return []

    latest = await fetch_one_async(
        """
        SELECT structured_data, confidence_score, extracted_at, extraction_method, source_type
        FROM cda_vendor_extractions
        WHERE LOWER(asset_symbol) = %s
          AND confidence_score > 0.3
          AND structured_data IS NOT NULL
        ORDER BY
            CASE WHEN source_type = 'pdf_attestation' THEN 0 ELSE 1 END,
            extracted_at DESC
        LIMIT 1
        """,
        (stablecoin_id.lower(),),
    )

    if not latest or not latest.get("structured_data"):
        return []

    data = _unwrap_citations(latest["structured_data"])
    confidence = latest.get("confidence_score", 0.5)
    method = latest.get("extraction_method", "unknown")

    if confidence < 0.5:
        return []

    components = []
    source = f"cda_{method}"

    if disclosure_type == "synthetic-derivative":
        components.extend(_extract_synthetic_components(data, source))
    elif disclosure_type == "rwa-tokenized":
        components.extend(_extract_rwa_components(data, source))

    # Attestation freshness (applies to all types)
    att_date_str = data.get("attestation_date")
    if att_date_str:
        try:
            att_date = _parse_date(str(att_date_str))
            days_old = (datetime.now(timezone.utc) - att_date).days
            freshness_score = normalize_inverse_linear(days_old, 0, 180)
            components.append({
                "component_id": "attestation_freshness",
                "category": "transparency",
                "raw_value": days_old,
                "normalized_score": round(freshness_score, 2),
                "data_source": source,
            })
        except asyncio.CancelledError:
            raise
        except (ValueError, TypeError) as e:
            logger.warning(f"cda_scores: get_cda_components_typed attestation_freshness parse failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="services_get_cda_components_typed_attestation_freshness_parse_failure",
                    error_message=str(e)[:500],
                    cycle_phase="cda_scores",
                )
            except Exception:
                pass

    if components:
        logger.info(
            f"CDA typed: {stablecoin_id} ({disclosure_type}) — {len(components)} components"
        )

    return components


def _extract_synthetic_components(data: dict, source: str) -> list[dict]:
    """Extract scoring components from synthetic/derivative attestation data."""
    components = []

    cr = data.get("collateral_ratio")
    if cr and isinstance(cr, (int, float)) and cr > 0:
        cr_score = normalize_linear(cr, 0.95, 1.10)
        components.append({
            "component_id": "synthetic_collateral_ratio",
            "category": "transparency",
            "raw_value": round(cr, 4),
            "normalized_score": round(cr_score, 2),
            "data_source": source,
        })

    custodians = data.get("custodians", [])
    if isinstance(custodians, list) and len(custodians) > 0:
        custodian_score = min(len(custodians) * 25, 100)
        components.append({
            "component_id": "custodian_diversification",
            "category": "transparency",
            "raw_value": len(custodians),
            "normalized_score": custodian_score,
            "data_source": source,
        })

    return components


def _extract_rwa_components(data: dict, source: str) -> list[dict]:
    """Extract scoring components from RWA/tokenized asset attestation data."""
    components = []

    nav = data.get("nav_per_token")
    if nav and isinstance(nav, (int, float)) and nav > 0:
        nav_score = normalize_linear(nav, 0.98, 1.02)
        components.append({
            "component_id": "nav_per_token",
            "category": "transparency",
            "raw_value": round(nav, 4),
            "normalized_score": round(nav_score, 2),
            "data_source": source,
        })

    return components
