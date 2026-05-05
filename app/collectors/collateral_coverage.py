"""
Collateral Coverage Ratio & Market Listing Velocity
=====================================================
Prompt 2: Measures what percentage of a protocol's accepted collateral is scored/known.
Prompt 3: Tracks market listing velocity and flags anomalous additions.

Both share the same DeFiLlama pools data pipeline — no extra API calls.
"""

import json
import logging
from datetime import datetime, timezone, timedelta

from app.database import execute, fetch_all, fetch_one
from app.collectors.psi_collector import (
    fetch_protocol_pools,
    DEFILLAMA_PROJECT_MAP,
    _extract_stablecoin_symbols,
    _is_stablecoin_token,
    _get_sii_score_map,
    SII_SCORED_SYMBOLS,
)
from app.index_definitions.psi_v01 import TARGET_PROTOCOLS

logger = logging.getLogger(__name__)

# Known non-stablecoin assets that should not be penalized
KNOWN_MAJOR_ASSETS = {
    "ETH", "WETH", "BTC", "WBTC", "STETH", "WSTETH", "RETH", "CBETH",
    "SOL", "MSOL", "JITOSOL", "BNSOL",
    "MATIC", "WMATIC", "AVAX", "WAVAX", "BNB", "WBNB",
    "ARB", "OP", "BASE",
}


def _is_known_asset(symbol: str) -> bool:
    """Check if a symbol is a known major asset or SII-scored stablecoin."""
    sym = symbol.upper().strip()
    return sym in KNOWN_MAJOR_ASSETS or sym in SII_SCORED_SYMBOLS


def compute_collateral_coverage(pools: list, slug: str) -> dict:
    """Compute collateral coverage ratio for a protocol from pool data.

    Returns dict with total_tvl, scored_tvl, coverage_ratio, unscored_assets.
    """
    sii_map = _get_sii_score_map()
    total_tvl = 0.0
    known_tvl = 0.0
    unscored_assets = {}  # symbol -> tvl

    for pool in pools:
        project = pool.get("project", "")
        mapped_slug = DEFILLAMA_PROJECT_MAP.get(project)
        if mapped_slug != slug:
            continue

        tvl = pool.get("tvlUsd") or 0
        if tvl <= 0:
            continue

        pool_symbol = pool.get("symbol", "")
        symbols = _extract_stablecoin_symbols(pool_symbol)
        if not symbols:
            continue

        # Split TVL evenly among symbols in the pool
        per_symbol_tvl = tvl / len(symbols)
        total_tvl += tvl

        for sym in symbols:
            sym_upper = sym.upper().strip()
            if _is_known_asset(sym_upper) or _is_stablecoin_token(sym_upper, sym_upper):
                known_tvl += per_symbol_tvl
            else:
                unscored_assets[sym_upper] = unscored_assets.get(sym_upper, 0) + per_symbol_tvl

    coverage_ratio = (known_tvl / total_tvl * 100) if total_tvl > 0 else 100.0

    return {
        "total_collateral_tvl": total_tvl,
        "scored_collateral_tvl": known_tvl,
        "collateral_coverage_ratio": round(coverage_ratio, 2),
        "unscored_assets": unscored_assets,
    }


def normalize_coverage_ratio(ratio: float) -> float:
    """Normalize collateral coverage ratio to a 0-100 PSI component score.

    100% coverage = 100, 90% = 80, 80% = 60, 70% = 40, <60% = 20, <40% = 10
    """
    if ratio >= 100:
        return 100.0
    elif ratio >= 90:
        return 80.0
    elif ratio >= 80:
        return 60.0
    elif ratio >= 70:
        return 40.0
    elif ratio >= 60:
        return 20.0
    elif ratio >= 40:
        return 10.0
    else:
        return 10.0


def compute_market_snapshots(pools: list) -> list[dict]:
    """Compute market snapshots for all target protocols from pool data.

    Returns list of snapshot dicts with new_markets and removed_markets detected.
    """
    target_slugs = set(TARGET_PROTOCOLS)
    protocol_pools: dict[str, list[str]] = {}  # slug -> [pool_id, ...]

    for pool in pools:
        project = pool.get("project", "")
        slug = DEFILLAMA_PROJECT_MAP.get(project)
        if not slug or slug not in target_slugs:
            continue
        tvl = pool.get("tvlUsd") or 0
        if tvl <= 0:
            continue

        pool_id = pool.get("pool", pool.get("symbol", "unknown"))
        if slug not in protocol_pools:
            protocol_pools[slug] = []
        protocol_pools[slug].append(pool_id)

    results = []
    for slug, current_markets in protocol_pools.items():
        current_set = set(current_markets)

        # Get prior snapshot
        prior = fetch_one("""
            SELECT market_list FROM protocol_market_snapshots
            WHERE protocol_slug = %s
            ORDER BY snapshot_date DESC
            LIMIT 1
        """, (slug,))

        prior_set = set()
        if prior and prior["market_list"]:
            prior_list = prior["market_list"] if isinstance(prior["market_list"], list) else json.loads(prior["market_list"])
            prior_set = set(prior_list)

        new_markets = list(current_set - prior_set) if prior else []
        removed_markets = list(prior_set - current_set) if prior else []

        # Store snapshot
        try:
            execute("""
                INSERT INTO protocol_market_snapshots
                    (protocol_slug, market_count, market_list, new_markets, removed_markets)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (protocol_slug, snapshot_date)
                DO UPDATE SET
                    market_count = EXCLUDED.market_count,
                    market_list = EXCLUDED.market_list,
                    new_markets = EXCLUDED.new_markets,
                    removed_markets = EXCLUDED.removed_markets
            """, (
                slug,
                len(current_markets),
                json.dumps(sorted(current_markets)),
                json.dumps(new_markets) if new_markets else None,
                json.dumps(removed_markets) if removed_markets else None,
            ))
        except Exception as e:
            logger.error(f"Failed to store market snapshot for {slug}: {e}")

        # Auto-events for anomalous market additions
        if len(new_markets) > 3:
            _emit_market_event(slug, new_markets, "warning",
                               f"{slug}: {len(new_markets)} new markets added in a single day")

        # Check for unscored assets in new markets
        if new_markets:
            unscored_in_new = []
            for pool_id in new_markets:
                # Find the pool data
                for pool in pools:
                    if pool.get("pool") == pool_id or pool.get("symbol") == pool_id:
                        symbols = _extract_stablecoin_symbols(pool.get("symbol", ""))
                        for sym in symbols:
                            if not _is_known_asset(sym.upper()):
                                unscored_in_new.append(sym)
            if unscored_in_new:
                _emit_market_event(slug, new_markets, "critical",
                                   f"{slug}: new markets contain unscored assets: {', '.join(set(unscored_in_new))}")

        results.append({
            "protocol_slug": slug,
            "market_count": len(current_markets),
            "new_markets": new_markets,
            "removed_markets": removed_markets,
            "is_initial_snapshot": prior is None,
        })

    logger.info(f"Market snapshots: {len(results)} protocols, "
                f"{sum(len(r['new_markets']) for r in results)} new markets detected")
    return results


def _emit_market_event(slug: str, markets: list, severity: str, description: str):
    """Store a market velocity event in score_events."""
    try:
        execute("""
            INSERT INTO score_events (event_date, event_name, event_type,
                affected_stablecoins, description, severity)
            VALUES (CURRENT_DATE, %s, %s, %s, %s, %s)
        """, (
            f"Market listing anomaly: {slug}",
            "market_listing_anomaly",
            [slug],
            description,
            severity,
        ))
        logger.warning(f"MARKET EVENT: {description} (severity={severity})")
    except Exception as e:
        logger.error(f"Failed to store market event for {slug}: {e}")


def compute_market_listing_velocity(slug: str) -> float:
    """Compute market_listing_velocity PSI component (0-100).

    Base score 80.
    - New markets in last 7 days: each -5
    - New markets in last 24 hours: each -15
    - New market with unscored asset: additional -20 per market
    Floor at 10.
    """
    now = datetime.now(timezone.utc)
    seven_days_ago = (now - timedelta(days=7)).date()
    yesterday = (now - timedelta(days=1)).date()

    snapshots = fetch_all("""
        SELECT snapshot_date, new_markets
        FROM protocol_market_snapshots
        WHERE protocol_slug = %s AND snapshot_date >= %s
        ORDER BY snapshot_date DESC
    """, (slug, seven_days_ago))

    if not snapshots:
        return 80.0  # No data — base score

    score = 80.0
    for snap in snapshots:
        new_markets = snap.get("new_markets")
        if not new_markets:
            continue
        if isinstance(new_markets, str):
            new_markets = json.loads(new_markets)

        n = len(new_markets)
        if snap["snapshot_date"] >= yesterday:
            score -= n * 15  # Last 24h: harsh penalty
        else:
            score -= n * 5   # Last 7 days: moderate penalty

        # Check for unscored assets in new markets (by symbol parsing)
        for market_id in new_markets:
            symbols = _extract_stablecoin_symbols(str(market_id))
            for sym in symbols:
                if not _is_known_asset(sym.upper()):
                    score -= 20

    return max(score, 10.0)


def collect_coverage_and_markets():
    """Run both collateral coverage and market snapshot collection in a single pass.

    Called from the PSI scoring pipeline. Uses the same DeFiLlama pools data.
    Returns dict with coverage results per protocol and market snapshot results.
    """
    logger.info("Collecting collateral coverage and market snapshots...")
    pools = fetch_protocol_pools()
    if not pools:
        logger.warning("No pool data for coverage/market collection")
        return {"coverage": {}, "markets": []}

    # Collateral coverage per protocol
    coverage_results = {}
    for slug in TARGET_PROTOCOLS:
        try:
            coverage = compute_collateral_coverage(pools, slug)
            coverage_results[slug] = coverage

            ratio = coverage["collateral_coverage_ratio"]
            if ratio < 80:
                severity = "critical" if ratio < 50 else "warning"
                unscored = coverage["unscored_assets"]
                desc = (f"{slug}: collateral coverage ratio {ratio:.1f}% — "
                        f"unscored assets: {', '.join(f'{k} (${v:,.0f})' for k, v in sorted(unscored.items(), key=lambda x: -x[1])[:5])}")
                try:
                    execute("""
                        INSERT INTO score_events (event_date, event_name, event_type,
                            affected_stablecoins, description, severity)
                        VALUES (CURRENT_DATE, %s, %s, %s, %s, %s)
                    """, (
                        f"Low collateral coverage: {slug}",
                        "collateral_coverage_low",
                        [slug],
                        desc,
                        severity,
                    ))
                except Exception as e:
                    logger.warning(f"collect coverage and markets failed: {e}")
                    try:
                        from app.worker import _record_cycle_error
                        _record_cycle_error(
                            error_type="collectors_collect_coverage_and_markets_failure",
                            error_message=str(e)[:500],
                            cycle_phase="collateral_coverage",
                        )
                    except Exception:
                        pass
                    pass  # May conflict on same-day re-run

            logger.info(f"  {slug}: coverage {ratio:.1f}% "
                        f"(${coverage['scored_collateral_tvl']:,.0f} / ${coverage['total_collateral_tvl']:,.0f})")
        except Exception as e:
            logger.error(f"Failed to compute coverage for {slug}: {e}")

    # Market snapshots
    market_results = compute_market_snapshots(pools)

    return {"coverage": coverage_results, "markets": market_results}


def get_market_history(slug: str) -> dict:
    """Get market snapshot history for a protocol with diffs."""
    snapshots = fetch_all("""
        SELECT snapshot_date, market_count, market_list, new_markets, removed_markets
        FROM protocol_market_snapshots
        WHERE protocol_slug = %s
        ORDER BY snapshot_date DESC
        LIMIT 90
    """, (slug,))

    events = fetch_all("""
        SELECT event_date, event_name, event_type, description, severity
        FROM score_events
        WHERE event_type = 'market_listing_anomaly'
          AND %s = ANY(affected_stablecoins)
        ORDER BY event_date DESC
        LIMIT 50
    """, (slug,))

    return {
        "protocol_slug": slug,
        "snapshot_count": len(snapshots),
        "snapshots": [dict(s) for s in snapshots],
        "events": [dict(e) for e in events],
        "market_listing_velocity_score": compute_market_listing_velocity(slug),
    }
