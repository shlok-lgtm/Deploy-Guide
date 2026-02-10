"""
Basis Protocol - Worker
========================
Collects data from all sources, computes SII scores, writes to database.
Runs as a standalone process on a schedule.

Usage:
    python -m app.worker              # Run once
    python -m app.worker --loop       # Run continuously on schedule
    python -m app.worker --coin usdc  # Score a single coin
"""

import asyncio
import logging
import sys
import os
import time
from datetime import datetime, timezone, date
from typing import Optional

import httpx

# Add parent to path for module resolution
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import STABLECOIN_REGISTRY, COLLECTION_INTERVAL_MINUTES
from app.database import init_pool, close_pool, get_cursor, fetch_one
from app.scoring import (
    calculate_sii, calculate_structural_composite, score_to_grade,
    aggregate_legacy_to_v1, FORMULA_VERSION, SII_V1_WEIGHTS,
)
from app.collectors.coingecko import (
    collect_peg_components, collect_liquidity_components,
    collect_market_activity_components, fetch_current, extract_price_context,
)
from app.collectors.defillama import collect_defillama_components
from app.collectors.curve import collect_curve_components
from app.collectors.offline import (
    collect_transparency_components, collect_regulatory_components,
    collect_governance_components, collect_reserve_components,
    collect_network_components,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("worker")


# =============================================================================
# Core: Collect all components for one stablecoin
# =============================================================================

async def collect_all_components(
    client: httpx.AsyncClient, stablecoin_id: str
) -> list[dict]:
    """
    Collect all components from all sources for one stablecoin.
    Returns flat list of component dicts ready for DB insert.
    """
    cfg = STABLECOIN_REGISTRY.get(stablecoin_id)
    if not cfg:
        logger.error(f"Unknown stablecoin: {stablecoin_id}")
        return []
    
    cg_id = cfg["coingecko_id"]
    all_components = []
    
    # Async collectors (with timeouts)
    async def safe_collect(name, coro):
        try:
            result = await asyncio.wait_for(coro, timeout=20.0)
            return result
        except asyncio.TimeoutError:
            logger.warning(f"{name} timed out for {stablecoin_id}")
            return []
        except Exception as e:
            logger.error(f"{name} error for {stablecoin_id}: {e}")
            return []
    
    # Run API collectors in parallel
    results = await asyncio.gather(
        safe_collect("peg", collect_peg_components(client, cg_id, stablecoin_id)),
        safe_collect("liquidity", collect_liquidity_components(client, cg_id, stablecoin_id)),
        safe_collect("market", collect_market_activity_components(client, cg_id, stablecoin_id)),
        safe_collect("defillama", collect_defillama_components(client, cg_id, stablecoin_id)),
        safe_collect("curve", collect_curve_components(client, stablecoin_id)),
    )
    
    for result in results:
        if result:
            all_components.extend(result)
    
    # Offline collectors (synchronous, from config/scraped data)
    for collector in [
        collect_transparency_components,
        collect_regulatory_components,
        collect_governance_components,
        collect_reserve_components,
        collect_network_components,
    ]:
        try:
            offline = collector(stablecoin_id)
            all_components.extend(offline)
        except Exception as e:
            logger.error(f"Offline collector error for {stablecoin_id}: {e}")
    
    # Tag all components with stablecoin_id
    for comp in all_components:
        comp["stablecoin_id"] = stablecoin_id
    
    return all_components


# =============================================================================
# Core: Compute SII from collected components
# =============================================================================

def compute_sii_from_components(components: list[dict]) -> dict:
    """
    Given a flat list of component readings, compute the SII score.
    Returns dict with overall score, grade, category scores, structural breakdown.
    """
    # Group by category → average normalized scores
    category_scores: dict[str, list[float]] = {}
    for comp in components:
        cat = comp.get("category", "unknown")
        score = comp.get("normalized_score")
        if score is not None:
            category_scores.setdefault(cat, []).append(score)
    
    cat_avgs = {cat: sum(s) / len(s) for cat, s in category_scores.items()}
    
    # Map legacy categories to v1.0.0 structure
    v1_scores = aggregate_legacy_to_v1(cat_avgs)
    
    # Calculate final SII
    overall = calculate_sii(v1_scores)
    if overall is None:
        overall = 0.0
    
    # Extract structural subscores for storage
    from app.scoring import DB_TO_STRUCTURAL_MAPPING, STRUCTURAL_SUBWEIGHTS
    structural_buckets: dict[str, list[float]] = {}
    for legacy_cat, sub in DB_TO_STRUCTURAL_MAPPING.items():
        if legacy_cat in cat_avgs:
            structural_buckets.setdefault(sub, []).append(cat_avgs[legacy_cat])
    structural_subs = {
        sub: sum(s) / len(s)
        for sub, s in structural_buckets.items()
    }
    
    return {
        "overall_score": round(overall, 2),
        "grade": score_to_grade(overall),
        "peg_score": round(v1_scores.get("peg_stability") or 0, 2),
        "liquidity_score": round(v1_scores.get("liquidity_depth") or 0, 2),
        "mint_burn_score": round(v1_scores.get("mint_burn_dynamics") or 0, 2),
        "distribution_score": round(v1_scores.get("holder_distribution") or 0, 2),
        "structural_score": round(v1_scores.get("structural_risk_composite") or 0, 2),
        "reserves_score": round(structural_subs.get("reserves_collateral") or 0, 2),
        "contract_score": round(structural_subs.get("smart_contract_risk") or 0, 2),
        "oracle_score": round(structural_subs.get("oracle_integrity") or 0, 2),
        "governance_score": round(structural_subs.get("governance_operations") or 0, 2),
        "network_score": round(structural_subs.get("network_chain_risk") or 0, 2),
        "component_count": len(components),
        "formula_version": FORMULA_VERSION,
    }


# =============================================================================
# Core: Store results to database
# =============================================================================

def store_component_readings(components: list[dict]):
    """Insert component readings into component_readings table."""
    if not components:
        return
    
    with get_cursor() as cur:
        for comp in components:
            cur.execute("""
                INSERT INTO component_readings
                    (stablecoin_id, component_id, category, raw_value, normalized_score, data_source, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (stablecoin_id, component_id, immutable_date(collected_at))
                DO UPDATE SET
                    raw_value = EXCLUDED.raw_value,
                    normalized_score = EXCLUDED.normalized_score,
                    data_source = EXCLUDED.data_source,
                    collected_at = EXCLUDED.collected_at
            """, (
                comp["stablecoin_id"],
                comp["component_id"],
                comp["category"],
                comp.get("raw_value"),
                comp.get("normalized_score"),
                comp.get("data_source", "unknown"),
            ))


def store_score(stablecoin_id: str, score_data: dict, price_ctx: dict):
    """Upsert current score into scores table."""
    # Get previous score for change calculation
    prev = fetch_one(
        "SELECT overall_score FROM scores WHERE stablecoin_id = %s",
        (stablecoin_id,)
    )
    daily_change = None
    if prev and prev.get("overall_score"):
        daily_change = round(score_data["overall_score"] - float(prev["overall_score"]), 3)
    
    # Get 7-day-ago score for weekly change
    week_ago = fetch_one("""
        SELECT overall_score FROM score_history
        WHERE stablecoin = %s AND score_date <= CURRENT_DATE - 7
        ORDER BY score_date DESC LIMIT 1
    """, (stablecoin_id,))
    weekly_change = None
    if week_ago and week_ago.get("overall_score"):
        weekly_change = round(score_data["overall_score"] - float(week_ago["overall_score"]), 3)
    
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO scores (
                stablecoin_id, overall_score, grade,
                peg_score, liquidity_score, mint_burn_score, distribution_score, structural_score,
                reserves_score, contract_score, oracle_score, governance_score, network_score,
                component_count, formula_version, data_freshness_pct,
                current_price, market_cap, volume_24h,
                daily_change, weekly_change,
                computed_at, updated_at
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                NOW(), NOW()
            )
            ON CONFLICT (stablecoin_id) DO UPDATE SET
                overall_score = EXCLUDED.overall_score,
                grade = EXCLUDED.grade,
                peg_score = EXCLUDED.peg_score,
                liquidity_score = EXCLUDED.liquidity_score,
                mint_burn_score = EXCLUDED.mint_burn_score,
                distribution_score = EXCLUDED.distribution_score,
                structural_score = EXCLUDED.structural_score,
                reserves_score = EXCLUDED.reserves_score,
                contract_score = EXCLUDED.contract_score,
                oracle_score = EXCLUDED.oracle_score,
                governance_score = EXCLUDED.governance_score,
                network_score = EXCLUDED.network_score,
                component_count = EXCLUDED.component_count,
                formula_version = EXCLUDED.formula_version,
                data_freshness_pct = EXCLUDED.data_freshness_pct,
                current_price = EXCLUDED.current_price,
                market_cap = EXCLUDED.market_cap,
                volume_24h = EXCLUDED.volume_24h,
                daily_change = EXCLUDED.daily_change,
                weekly_change = EXCLUDED.weekly_change,
                computed_at = NOW(),
                updated_at = NOW()
        """, (
            stablecoin_id,
            score_data["overall_score"], score_data["grade"],
            score_data["peg_score"], score_data["liquidity_score"],
            score_data["mint_burn_score"], score_data["distribution_score"],
            score_data["structural_score"],
            score_data["reserves_score"], score_data["contract_score"],
            score_data["oracle_score"], score_data["governance_score"],
            score_data["network_score"],
            score_data["component_count"], score_data["formula_version"],
            round(score_data["component_count"] / 102 * 100, 1),  # freshness pct
            price_ctx.get("current_price"), price_ctx.get("market_cap"),
            price_ctx.get("volume_24h"),
            daily_change, weekly_change,
        ))


def store_history_snapshot(stablecoin_id: str, score_data: dict):
    """Insert daily snapshot into score_history (one per day per coin)."""
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO score_history (
                stablecoin, score_date, overall_score, grade,
                peg_score, liquidity_score, mint_burn_score, distribution_score, structural_score,
                reserves_score, contract_score, oracle_score, governance_score, network_score,
                component_count, formula_version, daily_change, created_at
            ) VALUES (
                %s, CURRENT_DATE, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, NOW()
            )
            ON CONFLICT (stablecoin, score_date) DO UPDATE SET
                overall_score = EXCLUDED.overall_score,
                grade = EXCLUDED.grade,
                peg_score = EXCLUDED.peg_score,
                liquidity_score = EXCLUDED.liquidity_score,
                mint_burn_score = EXCLUDED.mint_burn_score,
                distribution_score = EXCLUDED.distribution_score,
                structural_score = EXCLUDED.structural_score,
                reserves_score = EXCLUDED.reserves_score,
                contract_score = EXCLUDED.contract_score,
                oracle_score = EXCLUDED.oracle_score,
                governance_score = EXCLUDED.governance_score,
                network_score = EXCLUDED.network_score,
                component_count = EXCLUDED.component_count
        """, (
            stablecoin_id,
            score_data["overall_score"], score_data["grade"],
            score_data["peg_score"], score_data["liquidity_score"],
            score_data["mint_burn_score"], score_data["distribution_score"],
            score_data["structural_score"],
            score_data["reserves_score"], score_data["contract_score"],
            score_data["oracle_score"], score_data["governance_score"],
            score_data["network_score"],
            score_data["component_count"], score_data["formula_version"],
            None,  # daily_change calculated on read
        ))


def store_provenance(components: list[dict]):
    """Store component readings in data_provenance for audit trail."""
    if not components:
        return
    with get_cursor() as cur:
        for comp in components:
            cur.execute("""
                INSERT INTO data_provenance
                    (stablecoin_id, component_id, category, raw_value, normalized_score,
                     data_source, recorded_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """, (
                comp.get("stablecoin_id"),
                comp.get("component_id"),
                comp.get("category"),
                comp.get("raw_value"),
                comp.get("normalized_score"),
                comp.get("data_source"),
            ))


# =============================================================================
# Orchestrator: Score one stablecoin
# =============================================================================

async def score_stablecoin(client: httpx.AsyncClient, stablecoin_id: str) -> dict:
    """Full pipeline: collect → compute → store for one stablecoin."""
    cfg = STABLECOIN_REGISTRY.get(stablecoin_id)
    if not cfg:
        return {"error": f"Unknown stablecoin: {stablecoin_id}"}
    
    start = time.time()
    
    # 1. Collect all components
    components = await collect_all_components(client, stablecoin_id)
    
    if not components:
        logger.warning(f"No components collected for {stablecoin_id}")
        return {"error": "No data collected", "stablecoin": stablecoin_id}
    
    # 2. Compute SII score
    score_data = compute_sii_from_components(components)
    
    # 3. Get price context
    current = await fetch_current(client, cfg["coingecko_id"])
    price_ctx = extract_price_context(current) if current else {}
    
    # 4. Store everything
    store_component_readings(components)
    store_score(stablecoin_id, score_data, price_ctx)
    store_history_snapshot(stablecoin_id, score_data)
    store_provenance(components)
    
    elapsed = time.time() - start
    logger.info(
        f"{stablecoin_id}: {score_data['overall_score']} ({score_data['grade']}) "
        f"- {score_data['component_count']} components in {elapsed:.1f}s"
    )
    
    return {
        "stablecoin": stablecoin_id,
        "score": score_data["overall_score"],
        "grade": score_data["grade"],
        "components": score_data["component_count"],
        "elapsed": round(elapsed, 1),
    }


# =============================================================================
# Orchestrator: Score all stablecoins
# =============================================================================

async def run_scoring_cycle():
    """Score all stablecoins in the registry."""
    start = time.time()
    stablecoins = list(STABLECOIN_REGISTRY.keys())
    
    logger.info(f"Starting scoring cycle for {len(stablecoins)} stablecoins")
    
    results = []
    async with httpx.AsyncClient() as client:
        for sid in stablecoins:
            try:
                result = await score_stablecoin(client, sid)
                results.append(result)
                # Rate limit: pause between coins
                await asyncio.sleep(2.0)
            except Exception as e:
                logger.error(f"Failed to score {sid}: {e}")
                results.append({"stablecoin": sid, "error": str(e)})
    
    elapsed = time.time() - start
    successes = sum(1 for r in results if "score" in r)
    logger.info(
        f"Scoring cycle complete: {successes}/{len(stablecoins)} scored in {elapsed:.0f}s"
    )
    
    return {
        "results": results,
        "successes": successes,
        "total": len(stablecoins),
        "elapsed": round(elapsed, 1),
    }


# =============================================================================
# Entry point
# =============================================================================

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Basis Protocol Worker")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--coin", type=str, help="Score single coin")
    parser.add_argument("--interval", type=int, default=COLLECTION_INTERVAL_MINUTES, help="Minutes between cycles")
    args = parser.parse_args()
    
    init_pool()
    
    try:
        if args.coin:
            async with httpx.AsyncClient() as client:
                result = await score_stablecoin(client, args.coin)
                print(result)
        elif args.loop:
            logger.info(f"Starting worker loop (interval: {args.interval} min)")
            gov_cycle_counter = 0
            while True:
                await run_scoring_cycle()
                
                # Run governance crawl every 6 SII cycles (~6 hours)
                gov_cycle_counter += 1
                if gov_cycle_counter % 6 == 0:
                    try:
                        from app.governance import run_crawl as gov_crawl
                        logger.info("Running governance crawl...")
                        gov_crawl(since_days=7)
                    except Exception as e:
                        logger.warning(f"Governance crawl failed: {e}")
                
                logger.info(f"Sleeping {args.interval} minutes...")
                await asyncio.sleep(args.interval * 60)
        else:
            await run_scoring_cycle()
    finally:
        close_pool()


if __name__ == "__main__":
    asyncio.run(main())
