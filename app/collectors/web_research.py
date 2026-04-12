"""
Web Research Collector (Parallel.ai Task API)
===============================================
Uses Parallel.ai's Task API to automate web research for governance and
operational components across protocols, bridges, and exchanges.

Components produced:
  - bridge_audit_research:         Audit count for bridge protocols
  - por_frequency_research:        PoR publication frequency for exchanges
  - compensation_transparency:     Governance compensation disclosure activity
  - meeting_cadence:               Regular governance meeting/call frequency
  - por_method_research:           PoR methodology quality for exchanges

Data source: Parallel.ai Task API (structured JSON output)
"""

import json
import logging
import re
from datetime import datetime, timezone

from app.database import execute, fetch_one
from app.services import parallel_client

logger = logging.getLogger(__name__)


# =============================================================================
# Validation helpers
# =============================================================================

def _validate_date(date_str: str) -> bool:
    """Check if a date string is parseable."""
    if not date_str or not isinstance(date_str, str):
        return False
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y/%m/%d", "%B %Y", "%b %Y"):
        try:
            datetime.strptime(date_str.strip(), fmt)
            return True
        except ValueError:
            continue
    return False


def _validate_url(url: str) -> bool:
    """Check if URL has valid format."""
    if not url or not isinstance(url, str):
        return False
    return url.startswith("http://") or url.startswith("https://")


def _validate_nonneg_int(val) -> bool:
    """Check if value is a non-negative integer."""
    try:
        return int(val) >= 0
    except (TypeError, ValueError):
        return False


def _safe_int(val, default: int = 0) -> int:
    """Safely convert to int."""
    try:
        return max(0, int(val))
    except (TypeError, ValueError):
        return default


def _safe_str(val, default: str = "") -> str:
    """Safely convert to string."""
    if val is None:
        return default
    return str(val).strip()


# =============================================================================
# Research functions
# =============================================================================

async def research_bridge_audits(bridge_name: str) -> dict:
    """
    Research public security audits for a bridge protocol.
    Returns {"audit_count": int, "audits": list, "score": float}.
    """
    fields = {
        "audits": "JSON array of audit objects with auditor, date, scope, url fields",
        "total_count": "Total number of public security audits found",
    }

    result = await parallel_client.task(
        question=f"Find all public security audits for the {bridge_name} bridge protocol. "
                 f"Include auditor name, date, scope of audit, and URL to the audit report.",
        fields=fields,
        processor="base",
    )

    if result.get("error"):
        logger.warning(f"Bridge audit research failed for {bridge_name}: {result['error']}")
        return {}

    # Extract structured data
    output = result.get("output", {})
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            output = {}

    audit_count = _safe_int(output.get("total_count", 0))

    # Validate audits list
    audits = output.get("audits", [])
    if isinstance(audits, list):
        validated = []
        for a in audits:
            if isinstance(a, dict):
                entry = {
                    "auditor": _safe_str(a.get("auditor")),
                    "date": _safe_str(a.get("date")),
                    "scope": _safe_str(a.get("scope")),
                    "url": _safe_str(a.get("url")),
                }
                if entry["auditor"]:
                    validated.append(entry)
                    if not _validate_date(entry["date"]):
                        logger.debug(f"Invalid audit date for {bridge_name}: {entry['date']}")
                    if entry["url"] and not _validate_url(entry["url"]):
                        logger.debug(f"Invalid audit URL for {bridge_name}: {entry['url']}")
        audits = validated
        audit_count = max(audit_count, len(audits))

    # Normalize: 5+ = 100, 3-4 = 80, 1-2 = 50, 0 = 10
    if audit_count >= 5:
        score = 100.0
    elif audit_count >= 3:
        score = 80.0
    elif audit_count >= 1:
        score = 50.0
    else:
        score = 10.0

    return {
        "audit_count": audit_count,
        "audits": audits,
        "score": score,
    }


async def research_por_frequency(exchange_name: str) -> dict:
    """
    Research proof-of-reserves publication frequency for an exchange.
    Returns {"last_published": str, "frequency_days": int, "score": float}.
    """
    fields = {
        "last_published": "Date when the exchange last published proof-of-reserves (YYYY-MM-DD format)",
        "frequency_days": "Average number of days between PoR publications",
        "url": "URL to the most recent PoR report or page",
        "methodology": "Brief description of PoR methodology used",
    }

    result = await parallel_client.task(
        question=f"When did {exchange_name} last publish their proof-of-reserves? "
                 f"What is the publication frequency? Include the URL and methodology.",
        fields=fields,
        processor="base",
    )

    if result.get("error"):
        logger.warning(f"PoR frequency research failed for {exchange_name}: {result['error']}")
        return {}

    output = result.get("output", {})
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            output = {}

    freq_days = _safe_int(output.get("frequency_days", 0))
    last_pub = _safe_str(output.get("last_published"))
    url = _safe_str(output.get("url"))
    methodology = _safe_str(output.get("methodology"))

    # Validate
    if last_pub and not _validate_date(last_pub):
        logger.debug(f"Invalid PoR date for {exchange_name}: {last_pub}")
    if url and not _validate_url(url):
        logger.debug(f"Invalid PoR URL for {exchange_name}: {url}")

    # Normalize: monthly (<=30d) = 100, quarterly (<=90d) = 70, semi-annual = 40, never = 10
    if freq_days > 0 and freq_days <= 30:
        score = 100.0
    elif freq_days > 0 and freq_days <= 90:
        score = 70.0
    elif freq_days > 0 and freq_days <= 180:
        score = 40.0
    elif freq_days > 0:
        score = 20.0
    else:
        score = 10.0

    return {
        "last_published": last_pub,
        "frequency_days": freq_days,
        "url": url,
        "methodology": methodology,
        "score": score,
    }


async def research_compensation_transparency(protocol_name: str) -> dict:
    """
    Research governance compensation transparency for a protocol.
    Returns {"post_count": int, "most_recent_date": str, "score": float}.
    """
    fields = {
        "post_count": "Number of governance forum posts about contributor compensation or grant spending in last 12 months",
        "most_recent_date": "Date of the most recent compensation-related post (YYYY-MM-DD format)",
        "forum_url": "URL to the governance forum",
    }

    result = await parallel_client.task(
        question=f"Search {protocol_name} governance forum for posts about contributor compensation, "
                 f"team pay, or grant spending transparency. Count how many such posts exist in the last 12 months.",
        fields=fields,
        processor="base",
    )

    if result.get("error"):
        logger.warning(f"Compensation research failed for {protocol_name}: {result['error']}")
        return {}

    output = result.get("output", {})
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            output = {}

    post_count = _safe_int(output.get("post_count", 0))
    recent_date = _safe_str(output.get("most_recent_date"))
    forum_url = _safe_str(output.get("forum_url"))

    if recent_date and not _validate_date(recent_date):
        logger.debug(f"Invalid compensation date for {protocol_name}: {recent_date}")
    if forum_url and not _validate_url(forum_url):
        logger.debug(f"Invalid forum URL for {protocol_name}: {forum_url}")

    # Normalize: 10+ = 100, 5-9 = 70, 1-4 = 40, 0 = 10
    if post_count >= 10:
        score = 100.0
    elif post_count >= 5:
        score = 70.0
    elif post_count >= 1:
        score = 40.0
    else:
        score = 10.0

    return {
        "post_count": post_count,
        "most_recent_date": recent_date,
        "forum_url": forum_url,
        "score": score,
    }


async def research_meeting_cadence(protocol_name: str) -> dict:
    """
    Research governance meeting/call cadence for a protocol.
    Returns {"has_regular_meetings": bool, "frequency": str, "score": float}.
    """
    fields = {
        "has_regular_meetings": "Whether the protocol holds regular governance calls or community meetings (true/false)",
        "frequency": "Meeting frequency: weekly, biweekly, monthly, irregular, or none",
        "most_recent_date": "Date of the most recent governance call or community meeting (YYYY-MM-DD format)",
        "url": "URL to meeting recordings, notes, or calendar",
    }

    result = await parallel_client.task(
        question=f"Does {protocol_name} hold regular governance calls, community meetings, "
                 f"or publish regular updates? What is the cadence?",
        fields=fields,
        processor="base",
    )

    if result.get("error"):
        logger.warning(f"Meeting cadence research failed for {protocol_name}: {result['error']}")
        return {}

    output = result.get("output", {})
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            output = {}

    has_meetings = str(output.get("has_regular_meetings", "false")).lower() in ("true", "yes", "1")
    frequency = _safe_str(output.get("frequency", "none")).lower()
    recent_date = _safe_str(output.get("most_recent_date"))
    url = _safe_str(output.get("url"))

    if recent_date and not _validate_date(recent_date):
        logger.debug(f"Invalid meeting date for {protocol_name}: {recent_date}")

    # Normalize: weekly = 100, biweekly = 85, monthly = 70, irregular = 40, none = 10
    freq_scores = {
        "weekly": 100.0,
        "biweekly": 85.0,
        "bi-weekly": 85.0,
        "monthly": 70.0,
        "quarterly": 50.0,
        "irregular": 40.0,
        "none": 10.0,
    }
    score = freq_scores.get(frequency, 40.0 if has_meetings else 10.0)

    return {
        "has_regular_meetings": has_meetings,
        "frequency": frequency,
        "most_recent_date": recent_date,
        "url": url,
        "score": score,
    }


async def research_por_method(exchange_name: str) -> dict:
    """
    Research proof-of-reserves methodology for an exchange.
    Returns {"method_type": str, "uses_merkle_tree": bool, "uses_zk_proof": bool, "score": float}.
    """
    fields = {
        "method_type": "Primary PoR methodology: merkle_tree, zk_proof, third_party_audit, self_reported, or none",
        "uses_merkle_tree": "Whether Merkle tree proof is used (true/false)",
        "uses_zk_proof": "Whether zero-knowledge proof is used (true/false)",
        "third_party_auditor": "Name of third-party auditor if any",
        "last_methodology_update": "Date of most recent methodology update (YYYY-MM-DD format)",
    }

    result = await parallel_client.task(
        question=f"Describe {exchange_name}'s current proof-of-reserves methodology. "
                 f"Do they use Merkle trees, zk-proofs, third-party auditors?",
        fields=fields,
        processor="base",
    )

    if result.get("error"):
        logger.warning(f"PoR method research failed for {exchange_name}: {result['error']}")
        return {}

    output = result.get("output", {})
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            output = {}

    method_type = _safe_str(output.get("method_type", "none")).lower()
    uses_merkle = str(output.get("uses_merkle_tree", "false")).lower() in ("true", "yes", "1")
    uses_zk = str(output.get("uses_zk_proof", "false")).lower() in ("true", "yes", "1")
    auditor = _safe_str(output.get("third_party_auditor"))
    last_update = _safe_str(output.get("last_methodology_update"))

    if last_update and not _validate_date(last_update):
        logger.debug(f"Invalid PoR method date for {exchange_name}: {last_update}")

    # Normalize: zk + third-party = 100, Merkle + third-party = 85, Merkle only = 60, self-reported = 30
    if uses_zk and auditor:
        score = 100.0
    elif uses_merkle and auditor:
        score = 85.0
    elif uses_zk:
        score = 75.0
    elif uses_merkle:
        score = 60.0
    elif auditor:
        score = 50.0
    elif method_type == "self_reported":
        score = 30.0
    else:
        score = 10.0

    return {
        "method_type": method_type,
        "uses_merkle_tree": uses_merkle,
        "uses_zk_proof": uses_zk,
        "third_party_auditor": auditor,
        "last_methodology_update": last_update,
        "score": score,
    }


# =============================================================================
# Storage
# =============================================================================

def _store_research_component(entity_type: str, entity_slug: str, component_id: str,
                              category: str, score: float, raw_data: dict):
    """Store a web research component result."""
    try:
        execute(
            """
            INSERT INTO generic_index_scores (index_id, entity_slug, entity_name,
                overall_score, category_scores, component_scores, raw_values,
                formula_version, confidence, scored_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'v1.0.0', 'standard', CURRENT_DATE)
            ON CONFLICT (index_id, entity_slug, scored_date)
            DO UPDATE SET
                overall_score = EXCLUDED.overall_score,
                component_scores = generic_index_scores.component_scores || EXCLUDED.component_scores,
                raw_values = generic_index_scores.raw_values || EXCLUDED.raw_values,
                computed_at = NOW()
            """,
            (
                f"web_research_{entity_type}",
                entity_slug, entity_slug, score,
                json.dumps({category: score}),
                json.dumps({component_id: score}),
                json.dumps({component_id: raw_data}),
            ),
        )
    except Exception as e:
        logger.warning(f"Failed to store research component {component_id} for {entity_slug}: {e}")


# =============================================================================
# Main runner
# =============================================================================

async def run_web_research_collection() -> list[dict]:
    """
    Run all web research tasks for scored entities.
    Called from worker slow cycle (every 3 hours or daily).
    Returns list of result dicts.
    """
    import os
    if not os.environ.get("PARALLEL_API_KEY"):
        logger.warning("PARALLEL_API_KEY not set, skipping web research collection")
        return []

    results = []

    # --- Bridge audits ---
    from app.index_definitions.bri_v01 import BRIDGE_ENTITIES
    for bridge in BRIDGE_ENTITIES:
        slug = bridge["slug"]
        name = bridge.get("name", slug)
        try:
            audit_data = await research_bridge_audits(name)
            if audit_data and audit_data.get("score") is not None:
                _store_research_component(
                    "bridge", slug, "bridge_audit_research",
                    "smart_contract_risk", audit_data["score"], audit_data,
                )
                results.append({
                    "entity_type": "bridge",
                    "entity_slug": slug,
                    "component": "bridge_audit_research",
                    "score": audit_data["score"],
                })
        except Exception as e:
            logger.warning(f"Bridge audit research failed for {slug}: {e}")

    # --- Exchange PoR frequency + methodology ---
    from app.index_definitions.cxri_v01 import CEX_ENTITIES
    for exchange in CEX_ENTITIES:
        slug = exchange["slug"]
        name = exchange.get("name", slug)
        try:
            por_freq = await research_por_frequency(name)
            if por_freq and por_freq.get("score") is not None:
                _store_research_component(
                    "exchange", slug, "por_frequency_research",
                    "reserve_proof_quality", por_freq["score"], por_freq,
                )
                results.append({
                    "entity_type": "exchange",
                    "entity_slug": slug,
                    "component": "por_frequency_research",
                    "score": por_freq["score"],
                })
        except Exception as e:
            logger.warning(f"PoR frequency research failed for {slug}: {e}")

        try:
            por_method = await research_por_method(name)
            if por_method and por_method.get("score") is not None:
                _store_research_component(
                    "exchange", slug, "por_method_research",
                    "reserve_proof_quality", por_method["score"], por_method,
                )
                results.append({
                    "entity_type": "exchange",
                    "entity_slug": slug,
                    "component": "por_method_research",
                    "score": por_method["score"],
                })
        except Exception as e:
            logger.warning(f"PoR method research failed for {slug}: {e}")

    # --- Protocol governance research ---
    from app.index_definitions.psi_v01 import TARGET_PROTOCOLS
    for slug in TARGET_PROTOCOLS:
        name = slug.replace("-", " ").title()
        try:
            comp_data = await research_compensation_transparency(name)
            if comp_data and comp_data.get("score") is not None:
                _store_research_component(
                    "protocol", slug, "compensation_transparency",
                    "governance", comp_data["score"], comp_data,
                )
                results.append({
                    "entity_type": "protocol",
                    "entity_slug": slug,
                    "component": "compensation_transparency",
                    "score": comp_data["score"],
                })
        except Exception as e:
            logger.warning(f"Compensation research failed for {slug}: {e}")

        try:
            meeting_data = await research_meeting_cadence(name)
            if meeting_data and meeting_data.get("score") is not None:
                _store_research_component(
                    "protocol", slug, "meeting_cadence",
                    "governance", meeting_data["score"], meeting_data,
                )
                results.append({
                    "entity_type": "protocol",
                    "entity_slug": slug,
                    "component": "meeting_cadence",
                    "score": meeting_data["score"],
                })
        except Exception as e:
            logger.warning(f"Meeting cadence research failed for {slug}: {e}")

    # Attest
    try:
        from app.state_attestation import attest_state
        scored = [r for r in results if "score" in r]
        if scored:
            attest_state("web_research", [
                {"type": r["entity_type"], "slug": r["entity_slug"],
                 "component": r["component"], "score": r["score"]}
                for r in scored
            ])
    except Exception:
        pass

    return results
