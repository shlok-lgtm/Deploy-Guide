"""
Temporal Reconstruction Engine
===============================
Reconstructs SII scores at any historical date by assembling component
readings from live data, historical price reconstruction, and carry-forward
of semi-static components.

The scoring engine (app/scoring.py, app/worker.py) is NOT modified.
This layer feeds it inputs — it doesn't change how scoring works.
"""

import json
import logging
import statistics
from datetime import date, datetime, timezone, timedelta
from typing import Optional

from app.database import fetch_one, fetch_all, execute
from app.config import STABLECOIN_REGISTRY
from app.scoring import (
    COMPONENT_NORMALIZATIONS,
    FORMULA_VERSION,
    normalize_component,
)
from app.worker import compute_sii_from_components

logger = logging.getLogger(__name__)

# Components that can be reconstructed from historical_prices
PRICE_RECONSTRUCTIBLE = {
    "peg_current_deviation",
    "peg_24h_max_deviation",
    "peg_7d_stddev",
    "peg_30d_stability",
    "depeg_events_30d",
    "max_drawdown_30d",
    "market_cap",
    "volume_24h",
    "volume_mcap_ratio",
}

# Categories that are semi-static and can be carried forward
CARRYABLE_CATEGORIES = {
    "smart_contract", "transparency", "network", "governance",
    "reserves", "oracle",
}

# Max days a carried-forward reading remains useful
CARRY_MAX_DAYS = 365


def _get_coingecko_id(stablecoin_id: str) -> Optional[str]:
    """Resolve stablecoin_id to coingecko_id."""
    cfg = STABLECOIN_REGISTRY.get(stablecoin_id)
    if cfg:
        return cfg.get("coingecko_id")
    # Try DB for promoted coins
    row = fetch_one(
        "SELECT coingecko_id FROM stablecoins WHERE id = %s",
        (stablecoin_id,),
    )
    return row["coingecko_id"] if row else None


# =========================================================================
# Price-based component reconstruction
# =========================================================================

def _get_price_series(coingecko_id: str, target: date, lookback_days: int = 30) -> list[dict]:
    """Fetch daily prices from historical_prices for a window ending at target_date."""
    start = target - timedelta(days=lookback_days)
    rows = fetch_all(
        """
        SELECT "timestamp"::date AS d, price, market_cap, volume_24h
        FROM historical_prices
        WHERE coingecko_id = %s
          AND "timestamp"::date BETWEEN %s AND %s
        ORDER BY "timestamp"::date
        """,
        (coingecko_id, start.isoformat(), target.isoformat()),
    )
    return [
        {
            "date": r["d"],
            "price": float(r["price"]) if r["price"] else None,
            "market_cap": float(r["market_cap"]) if r["market_cap"] else None,
            "volume_24h": float(r["volume_24h"]) if r["volume_24h"] else None,
        }
        for r in rows
    ]


def _reconstruct_from_prices(
    coingecko_id: str, target: date, stablecoin_id: str,
) -> list[dict]:
    """Build synthetic component readings from historical price data."""
    series = _get_price_series(coingecko_id, target, lookback_days=30)
    if not series:
        logger.warning(f"  No price series for {coingecko_id} at {target}")
        return []

    prices = [p["price"] for p in series if p["price"] is not None]
    if not prices:
        logger.warning(f"  Price series empty (all null) for {coingecko_id} at {target}")
        return []
    logger.info(f"  Price series: {len(series)} points, {len(prices)} with prices")

    # Find the target date's data point (or closest)
    target_data = None
    for p in reversed(series):
        if p["date"] <= target:
            target_data = p
            break
    if not target_data:
        target_data = series[-1]

    components = []
    current_price = target_data["price"]
    target_date_str = target.isoformat()

    def _add(comp_id, raw_value):
        spec = COMPONENT_NORMALIZATIONS.get(comp_id)
        if not spec:
            return
        normalized = normalize_component(comp_id, raw_value)
        if normalized is None:
            return
        components.append({
            "stablecoin_id": stablecoin_id,
            "component_id": comp_id,
            "category": spec["category"],
            "raw_value": round(raw_value, 6),
            "normalized_score": normalized,
            "data_source": "reconstructed",
            "source_date": target_date_str,
            "days_offset": 0,
        })

    # peg_current_deviation: abs(1.0 - price) * 100
    _add("peg_current_deviation", abs(1.0 - current_price) * 100)

    # peg_24h_max_deviation: use 1-day window (target and day before)
    day_prices = [p["price"] for p in series if p["price"] and p["date"] >= target - timedelta(days=1)]
    if day_prices:
        max_dev = max(abs(1.0 - p) * 100 for p in day_prices)
        _add("peg_24h_max_deviation", max_dev)

    # peg_7d_stddev
    week_prices = [p["price"] for p in series if p["price"] and p["date"] >= target - timedelta(days=7)]
    if len(week_prices) >= 2:
        _add("peg_7d_stddev", statistics.stdev(week_prices))

    # peg_30d_stability: % of 30d prices within 0.5% of peg
    if len(prices) >= 7:
        within_band = sum(1 for p in prices if abs(1.0 - p) <= 0.005)
        _add("peg_30d_stability", (within_band / len(prices)) * 100)

    # depeg_events_30d: count of price readings > 2% from peg
    depeg_count = sum(1 for p in prices if abs(1.0 - p) > 0.02)
    _add("depeg_events_30d", depeg_count)

    # max_drawdown_30d: (max - min) / max * 100
    if len(prices) >= 2:
        max_p, min_p = max(prices), min(prices)
        if max_p > 0:
            _add("max_drawdown_30d", (max_p - min_p) / max_p * 100)

    # market_cap
    if target_data.get("market_cap"):
        _add("market_cap", target_data["market_cap"])

    # volume_24h
    if target_data.get("volume_24h"):
        _add("volume_24h", target_data["volume_24h"])

    # volume_mcap_ratio
    if target_data.get("volume_24h") and target_data.get("market_cap") and target_data["market_cap"] > 0:
        _add("volume_mcap_ratio", target_data["volume_24h"] / target_data["market_cap"])

    return components


# =========================================================================
# Live and carry-forward component loading
# =========================================================================

def _get_live_readings(stablecoin_id: str, target: date) -> list[dict]:
    """Fetch component readings that were actually collected on target_date."""
    rows = fetch_all(
        """
        SELECT component_id, category, raw_value, normalized_score, data_source, collected_at
        FROM component_readings
        WHERE stablecoin_id = %s
          AND (collected_at::date) = %s
        """,
        (stablecoin_id, target.isoformat()),
    )
    return [
        {
            "stablecoin_id": stablecoin_id,
            "component_id": r["component_id"],
            "category": r["category"],
            "raw_value": float(r["raw_value"]) if r["raw_value"] is not None else None,
            "normalized_score": float(r["normalized_score"]) if r["normalized_score"] is not None else None,
            "data_source": "live",
            "source_date": target.isoformat(),
            "days_offset": 0,
        }
        for r in rows
        if r["normalized_score"] is not None
    ]


def _get_nearest_reading(stablecoin_id: str, component_id: str, target: date) -> Optional[dict]:
    """Find the nearest reading (forward or backward) for a semi-static component."""
    row = fetch_one(
        """
        SELECT component_id, category, raw_value, normalized_score, data_source, collected_at::date AS d
        FROM component_readings
        WHERE stablecoin_id = %s AND component_id = %s AND normalized_score IS NOT NULL
        ORDER BY ABS(collected_at::date - %s::date)
        LIMIT 1
        """,
        (stablecoin_id, component_id, target.isoformat()),
    )
    if not row:
        return None

    days_off = abs((target - row["d"]).days)
    if days_off > CARRY_MAX_DAYS:
        return None

    return {
        "stablecoin_id": stablecoin_id,
        "component_id": row["component_id"],
        "category": row["category"],
        "raw_value": float(row["raw_value"]) if row["raw_value"] is not None else None,
        "normalized_score": float(row["normalized_score"]),
        "data_source": "carried",
        "source_date": row["d"].isoformat(),
        "days_offset": days_off,
    }


# =========================================================================
# Core reconstruction
# =========================================================================

def _compute_confidence(
    coverage_pct: float, max_carry_days: int,
    has_peg: bool,
) -> str:
    """Determine confidence level from coverage and data quality."""
    if coverage_pct >= 80 and max_carry_days <= 30:
        return "high"
    if coverage_pct >= 50 or (coverage_pct >= 40 and has_peg):
        return "medium"
    return "low"


def reconstruct_score_sync(
    stablecoin_id: str,
    target_date: date,
    formula_version: str = None,
) -> dict:
    """Reconstruct an SII score for a stablecoin at a historical date.

    Returns a full reconstruction dict with score, provenance, and component detail.
    Results are cached in temporal_reconstructions.
    """
    if formula_version is None:
        formula_version = FORMULA_VERSION

    # 1. Check cache (skip zero-coverage entries — those were cached before data existed)
    cached = fetch_one(
        """
        SELECT * FROM temporal_reconstructions
        WHERE stablecoin_id = %s AND target_date = %s AND formula_version = %s
          AND components_available > 0
        """,
        (stablecoin_id, target_date.isoformat(), formula_version),
    )
    if cached:
        return _format_cached(cached)

    # 2. Resolve CoinGecko ID
    coingecko_id = _get_coingecko_id(stablecoin_id)
    logger.info(f"Reconstruct {stablecoin_id} @ {target_date}: coingecko_id={coingecko_id}")

    # 3. Assemble components from all sources
    all_components = {}  # component_id -> reading dict

    # 3a. Live readings (highest priority)
    live = _get_live_readings(stablecoin_id, target_date)
    for comp in live:
        all_components[comp["component_id"]] = comp
    logger.info(f"  Live readings: {len(live)}")

    # 3b. Reconstructed from historical prices
    if coingecko_id:
        reconstructed = _reconstruct_from_prices(coingecko_id, target_date, stablecoin_id)
        logger.info(f"  Reconstructed from prices: {len(reconstructed)}")
        for comp in reconstructed:
            if comp["component_id"] not in all_components:
                all_components[comp["component_id"]] = comp
    else:
        logger.warning(f"  No coingecko_id for {stablecoin_id} — skipping price reconstruction")

    # 3c. Carry-forward for semi-static components
    for comp_id, spec in COMPONENT_NORMALIZATIONS.items():
        if comp_id in all_components:
            continue
        if spec["category"] in CARRYABLE_CATEGORIES:
            carried = _get_nearest_reading(stablecoin_id, comp_id, target_date)
            if carried:
                all_components[comp_id] = carried

    # 4. Score using the existing pure function
    components_list = list(all_components.values())
    score_result = compute_sii_from_components(components_list)

    # 5. Build provenance
    total = len(COMPONENT_NORMALIZATIONS)
    live_count = sum(1 for c in components_list if c["data_source"] == "live")
    recon_count = sum(1 for c in components_list if c["data_source"] == "reconstructed")
    carried_count = sum(1 for c in components_list if c["data_source"] == "carried")
    missing_count = total - len(components_list)
    available = len(components_list)
    coverage_pct = round((available / total) * 100, 1) if total else 0

    max_carry = max((c["days_offset"] for c in components_list if c["data_source"] == "carried"), default=0)
    peg_components = {c["component_id"] for c in components_list if c["category"] == "peg_stability"}
    has_peg = len(peg_components) >= 4

    confidence = _compute_confidence(coverage_pct, max_carry, has_peg)

    source_breakdown = {
        "live": live_count,
        "reconstructed": recon_count,
        "carried": carried_count,
        "missing": missing_count,
    }

    # 6. Cache
    try:
        execute(
            """
            INSERT INTO temporal_reconstructions
                (stablecoin_id, target_date, overall_score, grade,
                 peg_score, liquidity_score, mint_burn_score,
                 distribution_score, structural_score,
                 formula_version, components_total, components_available,
                 components_reconstructed, components_carried, components_missing,
                 coverage_pct, confidence, source_breakdown, component_detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stablecoin_id, target_date, formula_version)
            DO UPDATE SET
                overall_score = EXCLUDED.overall_score,
                grade = EXCLUDED.grade,
                peg_score = EXCLUDED.peg_score,
                liquidity_score = EXCLUDED.liquidity_score,
                mint_burn_score = EXCLUDED.mint_burn_score,
                distribution_score = EXCLUDED.distribution_score,
                structural_score = EXCLUDED.structural_score,
                components_available = EXCLUDED.components_available,
                components_reconstructed = EXCLUDED.components_reconstructed,
                components_carried = EXCLUDED.components_carried,
                components_missing = EXCLUDED.components_missing,
                coverage_pct = EXCLUDED.coverage_pct,
                confidence = EXCLUDED.confidence,
                source_breakdown = EXCLUDED.source_breakdown,
                component_detail = EXCLUDED.component_detail,
                reconstructed_at = NOW()
            """,
            (
                stablecoin_id,
                target_date.isoformat(),
                score_result["overall_score"],
                score_result["grade"],
                score_result["peg_score"],
                score_result["liquidity_score"],
                score_result["mint_burn_score"],
                score_result["distribution_score"],
                score_result["structural_score"],
                formula_version,
                total,
                available,
                recon_count,
                carried_count,
                missing_count,
                coverage_pct,
                confidence,
                json.dumps(source_breakdown),
                json.dumps(components_list, default=str),
            ),
        )
    except Exception as e:
        logger.warning(f"Failed to cache reconstruction for {stablecoin_id}/{target_date}: {e}")

    # 7. Return
    return {
        "stablecoin_id": stablecoin_id,
        "target_date": target_date.isoformat(),
        "overall_score": score_result["overall_score"],
        "category_scores": {
            "peg_stability": score_result["peg_score"],
            "liquidity_depth": score_result["liquidity_score"],
            "mint_burn_dynamics": score_result["mint_burn_score"],
            "holder_distribution": score_result["distribution_score"],
            "structural_risk_composite": score_result["structural_score"],
        },
        "structural_subscores": {
            "reserves_collateral": score_result.get("reserves_score", 0),
            "smart_contract_risk": score_result.get("contract_score", 0),
            "oracle_integrity": score_result.get("oracle_score", 0),
            "governance_operations": score_result.get("governance_score", 0),
            "network_chain_risk": score_result.get("network_score", 0),
        },
        "coverage": {
            "total": total,
            "available": available,
            "live": live_count,
            "reconstructed": recon_count,
            "carried": carried_count,
            "missing": missing_count,
            "coverage_pct": coverage_pct,
            "confidence": confidence,
        },
        "components": components_list,
        "formula_version": formula_version,
        "reconstructed_at": datetime.now(timezone.utc).isoformat(),
    }


def reconstruct_range_sync(
    stablecoin_id: str,
    from_date: date,
    to_date: date,
    formula_version: str = None,
) -> list[dict]:
    """Reconstruct scores for a date range. Max 365 days."""
    if (to_date - from_date).days > 365:
        to_date = from_date + timedelta(days=365)

    results = []
    current = from_date
    while current <= to_date:
        result = reconstruct_score_sync(stablecoin_id, current, formula_version)
        # Return slim version for range queries (no per-component detail)
        results.append({
            "target_date": result["target_date"],
            "overall_score": result["overall_score"],
            "category_scores": result["category_scores"],
            "coverage": result["coverage"],
        })
        current += timedelta(days=1)

    return results


def _format_cached(row) -> dict:
    """Format a cached temporal_reconstructions row into the API response shape."""
    components = row.get("component_detail") or []
    if isinstance(components, str):
        components = json.loads(components)

    source = row.get("source_breakdown") or {}
    if isinstance(source, str):
        source = json.loads(source)

    return {
        "stablecoin_id": row["stablecoin_id"],
        "target_date": row["target_date"].isoformat() if hasattr(row["target_date"], "isoformat") else str(row["target_date"]),
        "overall_score": float(row["overall_score"]) if row["overall_score"] else None,
        "category_scores": {
            "peg_stability": float(row["peg_score"]) if row["peg_score"] else 0,
            "liquidity_depth": float(row["liquidity_score"]) if row["liquidity_score"] else 0,
            "mint_burn_dynamics": float(row["mint_burn_score"]) if row["mint_burn_score"] else 0,
            "holder_distribution": float(row["distribution_score"]) if row["distribution_score"] else 0,
            "structural_risk_composite": float(row["structural_score"]) if row["structural_score"] else 0,
        },
        "coverage": {
            "total": row["components_total"],
            "available": row["components_available"],
            "live": source.get("live", 0),
            "reconstructed": row["components_reconstructed"],
            "carried": row["components_carried"],
            "missing": row["components_missing"],
            "coverage_pct": float(row["coverage_pct"]) if row["coverage_pct"] else 0,
            "confidence": row["confidence"],
        },
        "components": components,
        "formula_version": row["formula_version"],
        "reconstructed_at": row["reconstructed_at"].isoformat() if row.get("reconstructed_at") else None,
    }


# Named crisis events for the backtest endpoint
CRISIS_EVENTS = {
    "terra_collapse": {
        "name": "Terra/UST Collapse",
        "from": "2022-05-07",
        "to": "2022-05-15",
        "description": "UST depegged and collapsed, triggering broad stablecoin stress.",
    },
    "svb_crisis": {
        "name": "SVB Bank Run",
        "from": "2023-03-10",
        "to": "2023-03-15",
        "description": "Silicon Valley Bank collapsed. USDC briefly depegged due to $3.3B reserve exposure.",
    },
    "fdusd_delisting": {
        "name": "FDUSD Binance Delisting Scare",
        "from": "2025-04-01",
        "to": "2025-04-07",
        "description": "FDUSD faced delisting pressure after reserve transparency concerns.",
    },
    "mica_enforcement": {
        "name": "MiCA Enforcement",
        "from": "2024-12-28",
        "to": "2025-01-15",
        "description": "EU MiCA regulation enforcement began, affecting non-compliant stablecoins.",
    },
    "drift_exploit_2026": {
        "name": "Drift Protocol Exploit",
        "from": "2026-04-01",
        "to": "2026-04-07",
        "description": "~$270M drained from Drift Protocol vaults on Solana. DRIFT token -37%. USDC held by Drift affected. Deposits/withdrawals suspended.",
    },
}


# Async wrappers for backward compatibility
async def reconstruct_score(stablecoin_id: str, target_date: date, formula_version: str = None) -> dict:
    return reconstruct_score_sync(stablecoin_id, target_date, formula_version)

async def reconstruct_range(stablecoin_id: str, from_date: date, to_date: date, formula_version: str = None) -> list[dict]:
    return reconstruct_range_sync(stablecoin_id, from_date, to_date, formula_version)
