"""
RPI Revenue Collector
======================
Fetches protocol revenue from DeFiLlama for the spend_ratio component.
Reuses the same DeFiLlama client pattern from psi_collector.
"""

import asyncio
import logging
import time

import requests

from app.index_definitions.rpi_v2 import RPI_TARGET_PROTOCOLS

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://api.llama.fi"


def fetch_fees_data(slug: str) -> dict | None:
    """Fetch fee/revenue data from DeFiLlama. Same pattern as psi_collector."""
    time.sleep(1)  # rate limit
    try:
        resp = requests.get(f"{DEFILLAMA_BASE}/summary/fees/{slug}", timeout=45)
        if resp.status_code == 200:
            return resp.json()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"DeFiLlama fees fetch failed for {slug}: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="rpi_fetch_fees_data_request_failure",
                error_message=str(e)[:500],
                cycle_phase="rpi_revenue_collector",
            )
        except Exception:
            pass
    return None


def get_annualized_revenue(slug: str) -> float | None:
    """Get annualized revenue for a protocol from DeFiLlama.

    Returns annual revenue in USD, or None if unavailable.
    """
    fees = fetch_fees_data(slug)
    if not fees:
        return None

    # Try totalRevenue30d first, fall back to total30d * 0.3
    revenue_30d = fees.get("totalRevenue30d")
    if not revenue_30d:
        total_30d = fees.get("total30d")
        if total_30d:
            revenue_30d = total_30d * 0.3  # estimate if not available

    if revenue_30d and revenue_30d > 0:
        return revenue_30d * 12  # annualize

    return None


def get_all_revenues() -> dict[str, float]:
    """Fetch annualized revenue for all RPI target protocols.

    Returns dict of slug -> annualized_revenue_usd.
    """
    revenues = {}
    for slug in RPI_TARGET_PROTOCOLS:
        rev = get_annualized_revenue(slug)
        if rev is not None:
            revenues[slug] = rev
            logger.info(f"RPI revenue: {slug} = ${rev:,.0f}/yr")
        else:
            logger.debug(f"RPI revenue: {slug} — no data")
    return revenues
