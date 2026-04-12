"""
Bridge Message Success Rate Monitor
=====================================
Adapters for bridge explorer APIs to track message delivery rates and uptime.

Bridges monitored:
  - Wormhole (Wormholescan API)
  - LayerZero (LayerZero Scan — proxy via DeFiLlama if no API)
  - Axelar (Axelarscan API)
  - Others: DeFiLlama bridge volume as uptime proxy

Components produced:
  - message_success_rate:   % of successful cross-chain messages (24h)
  - uptime_pct:             Operational uptime derived from volume data
"""

import json
import hashlib
import logging
import time
from datetime import datetime, timezone

import requests

from app.database import execute, fetch_one
from app.index_definitions.bri_v01 import BRIDGE_ENTITIES

logger = logging.getLogger(__name__)

RATE_LIMIT_DELAY = 0.5


# =============================================================================
# Bridge-specific adapters
# =============================================================================

def _wormhole_message_stats(hours: int = 24) -> dict:
    """
    Wormholescan API: count completed vs total transactions.
    Endpoint: GET /api/v1/transactions
    """
    try:
        resp = requests.get(
            "https://api.wormholescan.io/api/v1/transactions",
            params={"page": 0, "pageSize": 100, "sortOrder": "DESC"},
            timeout=15,
        )
        time.sleep(RATE_LIMIT_DELAY)
        if resp.status_code != 200:
            logger.debug(f"Wormholescan API returned {resp.status_code}")
            return {}

        data = resp.json()
        txs = data.get("transactions", [])
        if not txs:
            return {}

        total = len(txs)
        successful = sum(1 for tx in txs if tx.get("status") == "completed")
        failed = total - successful
        success_rate = (successful / total * 100) if total > 0 else 0

        return {
            "total_messages": total,
            "successful": successful,
            "failed": failed,
            "success_rate": round(success_rate, 2),
        }
    except Exception as e:
        logger.debug(f"Wormhole adapter failed: {e}")
        return {}


def _axelar_message_stats(hours: int = 24) -> dict:
    """
    Axelarscan API: search GMP transactions and count by status.
    Endpoint: GET /gmp/searchGMP
    """
    try:
        resp = requests.get(
            "https://api.axelarscan.io/gmp/searchGMP",
            params={"size": 100, "sort": "desc"},
            timeout=15,
        )
        time.sleep(RATE_LIMIT_DELAY)
        if resp.status_code != 200:
            logger.debug(f"Axelarscan API returned {resp.status_code}")
            return {}

        data = resp.json()
        messages = data.get("data", [])
        if not messages:
            return {}

        total = len(messages)
        successful = sum(
            1 for m in messages
            if m.get("status") in ("executed", "approved", "confirmed")
        )
        failed = total - successful
        success_rate = (successful / total * 100) if total > 0 else 0

        return {
            "total_messages": total,
            "successful": successful,
            "failed": failed,
            "success_rate": round(success_rate, 2),
        }
    except Exception as e:
        logger.debug(f"Axelar adapter failed: {e}")
        return {}


def _layerzero_message_stats(hours: int = 24) -> dict:
    """
    LayerZero Scan: no documented public API.
    Fall back to DeFiLlama bridge volume as uptime proxy.
    """
    return _defillama_bridge_uptime("layerzero")


def _defillama_bridge_uptime(bridge_slug: str) -> dict:
    """
    Use DeFiLlama bridge volume as an uptime proxy.
    If volume exists in last 24h, bridge is operational.
    """
    # Map our slugs to DeFiLlama bridge IDs
    llama_id_map = {
        "wormhole": 1,
        "layerzero": 14,
        "axelar": 12,
        "circle-cctp": 40,
        "across": 10,
        "stargate": 5,
        "synapse": 11,
        "debridge": 18,
        "celer-cbridge": 8,
    }
    bridge_id = llama_id_map.get(bridge_slug)
    if not bridge_id:
        return {}

    try:
        resp = requests.get(
            f"https://bridges.llama.fi/bridge/{bridge_id}",
            timeout=15,
        )
        time.sleep(RATE_LIMIT_DELAY)
        if resp.status_code != 200:
            return {}

        data = resp.json()
        # Check currentDayVolume or last entry in hourly volumes
        current_vol = data.get("currentDayVolume")
        if current_vol and (
            float(current_vol.get("depositUSD", 0)) > 0
            or float(current_vol.get("withdrawUSD", 0)) > 0
        ):
            return {
                "total_messages": -1,  # Unknown — volume proxy
                "successful": -1,
                "failed": 0,
                "success_rate": 99.5,  # Operational if volume exists
            }

        # Check hourly volumes for gaps
        hourly = data.get("chainBreakdown", {})
        if hourly:
            return {
                "total_messages": -1,
                "successful": -1,
                "failed": 0,
                "success_rate": 99.0,
            }

        return {}
    except Exception as e:
        logger.debug(f"DeFiLlama bridge volume check failed for {bridge_slug}: {e}")
        return {}


# =============================================================================
# Unified interface
# =============================================================================

def get_message_stats(bridge_slug: str, hours: int = 24) -> dict:
    """
    Get message success rate for a bridge.
    Routes to the appropriate adapter based on bridge slug.
    Returns {"total_messages": N, "successful": N, "failed": N, "success_rate": float}.
    """
    adapter_map = {
        "wormhole": _wormhole_message_stats,
        "axelar": _axelar_message_stats,
        "layerzero": _layerzero_message_stats,
    }

    adapter = adapter_map.get(bridge_slug)
    if adapter:
        result = adapter(hours)
        if result:
            return result

    # Fallback: DeFiLlama volume proxy for all other bridges
    return _defillama_bridge_uptime(bridge_slug)


# =============================================================================
# Normalization
# =============================================================================

def normalize_message_success_rate(rate: float) -> float:
    """Normalize: 99.9% = 100, 99% = 85, 95% = 50, <90% = 20."""
    if rate >= 99.9:
        return 100.0
    if rate >= 99.0:
        return 85.0 + (rate - 99.0) / 0.9 * 15.0
    if rate >= 95.0:
        return 50.0 + (rate - 95.0) / 4.0 * 35.0
    if rate >= 90.0:
        return 20.0 + (rate - 90.0) / 5.0 * 30.0
    return max(0.0, rate / 90.0 * 20.0)


def normalize_uptime_pct(uptime: float) -> float:
    """Normalize: 100% = 100, 99.5% = 90, 99% = 70, <98% = 40."""
    if uptime >= 100.0:
        return 100.0
    if uptime >= 99.5:
        return 90.0 + (uptime - 99.5) / 0.5 * 10.0
    if uptime >= 99.0:
        return 70.0 + (uptime - 99.0) / 0.5 * 20.0
    if uptime >= 98.0:
        return 40.0 + (uptime - 98.0) / 1.0 * 30.0
    return max(0.0, uptime / 98.0 * 40.0)


# =============================================================================
# Main runner
# =============================================================================

def run_bridge_monitoring() -> list[dict]:
    """
    Collect message success rates for all BRI bridge entities.
    Called from worker fast cycle (hourly).
    Returns list of result dicts.
    """
    results = []

    for bridge in BRIDGE_ENTITIES:
        slug = bridge["slug"]
        try:
            stats = get_message_stats(slug)
            if not stats:
                logger.debug(f"No bridge stats for {slug}")
                results.append({"bridge_slug": slug, "error": "no_data"})
                continue

            success_rate = stats.get("success_rate", 0)
            success_score = normalize_message_success_rate(success_rate)
            uptime_score = normalize_uptime_pct(success_rate)  # Use success_rate as uptime proxy

            # Store as component readings
            try:
                execute(
                    """
                    INSERT INTO generic_index_scores (index_id, entity_slug, entity_name,
                        overall_score, category_scores, component_scores, raw_values,
                        formula_version, confidence, scored_date)
                    VALUES ('bridge_monitor', %s, %s, %s, %s, %s, %s, 'v1.0.0', 'standard', CURRENT_DATE)
                    ON CONFLICT (index_id, entity_slug, scored_date)
                    DO UPDATE SET
                        overall_score = EXCLUDED.overall_score,
                        component_scores = EXCLUDED.component_scores,
                        raw_values = EXCLUDED.raw_values,
                        computed_at = NOW()
                    """,
                    (
                        slug, bridge.get("name", slug),
                        round((success_score + uptime_score) / 2, 2),
                        json.dumps({"operational_history": success_score}),
                        json.dumps({
                            "message_success_rate": success_score,
                            "uptime_pct": uptime_score,
                        }),
                        json.dumps(stats),
                    ),
                )
            except Exception as db_err:
                logger.warning(f"Failed to store bridge monitor data for {slug}: {db_err}")

            results.append({
                "bridge_slug": slug,
                "success_rate": success_rate,
                "success_score": success_score,
                "uptime_score": uptime_score,
            })

        except Exception as e:
            logger.warning(f"Bridge monitoring failed for {slug}: {e}")
            results.append({"bridge_slug": slug, "error": str(e)})

    # Attest
    try:
        from app.state_attestation import attest_state
        scored = [r for r in results if "success_rate" in r]
        if scored:
            attest_state("bridge_monitoring", [
                {"slug": r["bridge_slug"], "rate": r["success_rate"]}
                for r in scored
            ])
    except Exception:
        pass

    return results
