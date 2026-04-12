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
import json
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
from app.database import init_pool, close_pool, get_cursor, fetch_one, fetch_all, execute
from app.scoring import (
    calculate_sii, calculate_structural_composite,
    aggregate_legacy_to_v1, score_to_grade, FORMULA_VERSION, SII_V1_WEIGHTS,
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
from app.collectors.etherscan import collect_holder_distribution
from app.collectors.flows import collect_flows_components
from app.collectors.smart_contract import collect_smart_contract_components
from app.collectors.derived import collect_derived_components
from app.collectors.solana import collect_solana_components

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("worker")


# =============================================================================
# DB-driven registry helpers
# =============================================================================

def get_scoring_ids_from_db() -> list:
    """
    Read scoring-enabled stablecoin IDs from the database.
    Falls back to STABLECOIN_REGISTRY keys if DB query fails.
    """
    try:
        rows = fetch_all("SELECT id FROM stablecoins WHERE scoring_enabled = TRUE ORDER BY id")
        if rows:
            return [r["id"] for r in rows]
    except Exception as e:
        logger.warning(f"Could not read stablecoins from DB, using registry fallback: {e}")
    return list(STABLECOIN_REGISTRY.keys())


def get_stablecoin_config(stablecoin_id: str) -> dict:
    """
    Get config for a stablecoin: tries STABLECOIN_REGISTRY first,
    then falls back to the stablecoins DB table for promoted coins.
    Returns None if not found anywhere.
    """
    cfg = STABLECOIN_REGISTRY.get(stablecoin_id)
    if cfg:
        return cfg
    try:
        row = fetch_one(
            "SELECT name, symbol, coingecko_id, contract, decimals FROM stablecoins WHERE id = %s",
            (stablecoin_id,)
        )
        if row:
            return {
                "name": row["name"],
                "symbol": row["symbol"],
                "coingecko_id": row["coingecko_id"],
                "contract": row.get("contract"),
                "decimals": row.get("decimals", 18),
            }
    except Exception as e:
        logger.warning(f"Could not load config for {stablecoin_id} from DB: {e}")
    return None


def _mark_scoring_status(coingecko_id: str, status: str) -> None:
    """Update scoring_status in unscored_assets for a promoted coin."""
    try:
        with get_cursor() as cur:
            cur.execute(
                """
                UPDATE wallet_graph.unscored_assets
                SET scoring_status = %s, updated_at = NOW()
                WHERE coingecko_id = %s
                """,
                (status, coingecko_id),
            )
    except Exception as e:
        logger.warning(f"Could not update scoring_status for {coingecko_id}: {e}")


# =============================================================================
# Core: Collect all components for one stablecoin
# =============================================================================

async def _collect_actor_metrics(client, stablecoin_id):
    """Lazy wrapper for actor metrics collector."""
    from app.collectors.actor_metrics import collect_actor_metrics
    return await collect_actor_metrics(client, stablecoin_id)


async def collect_all_components(
    client: httpx.AsyncClient, stablecoin_id: str
) -> list[dict]:
    """
    Collect all components from all sources for one stablecoin.
    Returns flat list of component dicts ready for DB insert.
    """
    cfg = get_stablecoin_config(stablecoin_id)
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
        safe_collect("etherscan", collect_holder_distribution(client, stablecoin_id)),
        safe_collect("flows", collect_flows_components(client, stablecoin_id)),
        safe_collect("smart_contract", collect_smart_contract_components(client, stablecoin_id)),
        safe_collect("solana", collect_solana_components(client, stablecoin_id)),
        safe_collect("actor_metrics", _collect_actor_metrics(client, stablecoin_id)),
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
        collect_derived_components,
    ]:
        try:
            offline = collector(stablecoin_id)
            all_components.extend(offline)
        except Exception as e:
            logger.error(f"Offline collector error for {stablecoin_id}: {e}")

    # CDA vendor data (overlays/improves offline transparency + reserve components)
    try:
        from app.services.cda_scores import get_cda_components
        cda_components = get_cda_components(stablecoin_id)
        if cda_components:
            all_components.extend(cda_components)
    except Exception as e:
        logger.debug(f"CDA collector skipped for {stablecoin_id}: {e}")
    
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
    Returns dict with overall score, category scores, structural breakdown.
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
    
    rounded_overall = round(overall, 2)
    return {
        "overall_score": rounded_overall,
        "grade": score_to_grade(rounded_overall),
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
            round(score_data["component_count"] / 51 * 100, 1),  # freshness pct (51 collectible components)
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
            metadata = comp.get("metadata")
            metadata_json = json.dumps(metadata) if metadata else None
            cur.execute("""
                INSERT INTO data_provenance
                    (stablecoin_id, component_id, category, raw_value, normalized_score,
                     data_source, metadata, recorded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                comp.get("stablecoin_id"),
                comp.get("component_id"),
                comp.get("category"),
                comp.get("raw_value"),
                comp.get("normalized_score"),
                comp.get("data_source"),
                metadata_json,
            ))


def _store_component_batch_hash(entity_id: str, entity_type: str,
                                components: list[dict], score_data: dict):
    """Compute and store a batch hash of component readings for attestation."""
    import hashlib
    canonical_components = sorted([
        {"id": c.get("component_id", ""), "score": round(float(c.get("normalized_score") or 0), 4)}
        for c in components
    ], key=lambda x: x["id"])
    canonical = json.dumps(canonical_components, sort_keys=True, separators=(",", ":"))
    batch_hash = "0x" + hashlib.sha256(canonical.encode()).hexdigest()

    from app.database import execute as db_execute
    db_execute(
        """
        INSERT INTO component_batch_hashes
            (entity_type, entity_id, batch_hash, component_count, methodology_version)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (entity_type, entity_id, computed_at) DO UPDATE SET
            batch_hash = EXCLUDED.batch_hash,
            component_count = EXCLUDED.component_count
        """,
        (entity_type, entity_id, batch_hash, len(components),
         score_data.get("formula_version", "v1.0.0")),
    )


# =============================================================================
# Orchestrator: Score one stablecoin
# =============================================================================

async def score_stablecoin(client: httpx.AsyncClient, stablecoin_id: str) -> dict:
    """Full pipeline: collect → compute → store for one stablecoin."""
    cfg = get_stablecoin_config(stablecoin_id)
    if not cfg:
        return {"error": f"Unknown stablecoin: {stablecoin_id}"}
    
    start = time.time()
    
    # 1. Collect all components
    components = await collect_all_components(client, stablecoin_id)
    
    if not components:
        logger.warning(f"No components collected for {stablecoin_id}")
        return {"error": "No data collected", "stablecoin": stablecoin_id}

    # 1b. Category-completeness gate — every v1 category must have ≥1 component
    from app.scoring_engine import is_sii_category_complete_legacy
    is_complete, missing_cats = is_sii_category_complete_legacy(components)

    if not is_complete:
        # Store component readings for future attempts but skip scoring
        store_component_readings(components)
        elapsed = time.time() - start
        logger.info(
            f"{stablecoin_id}: SKIPPED (category-incomplete) — "
            f"missing: {', '.join(missing_cats)} — "
            f"{len(components)} components collected in {elapsed:.1f}s"
        )
        return {
            "stablecoin": stablecoin_id,
            "skipped": True,
            "reason": "category_incomplete",
            "missing_categories": missing_cats,
            "components": len(components),
            "elapsed": round(elapsed, 1),
        }

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

    # State attestation for component readings
    try:
        from app.state_attestation import attest_state
        attest_state("sii_components", [
            {"id": c.get("component_id", ""), "score": round(float(c.get("normalized_score") or 0), 4)}
            for c in components
        ], entity_id=stablecoin_id)
    except Exception as e:
        logger.debug(f"SII attestation skipped for {stablecoin_id}: {e}")

    elapsed = time.time() - start
    logger.info(
        f"{stablecoin_id}: {score_data['overall_score']} "
        f"- {score_data['component_count']} components in {elapsed:.1f}s"
    )

    return {
        "stablecoin": stablecoin_id,
        "score": score_data["overall_score"],
        "components": score_data["component_count"],
        "elapsed": round(elapsed, 1),
    }


# =============================================================================
# Orchestrator: Score all stablecoins
# =============================================================================

async def run_scoring_cycle():
    """Score all stablecoins enabled in the database (falls back to registry)."""
    start = time.time()
    stablecoins = get_scoring_ids_from_db()
    
    logger.info(f"Starting scoring cycle for {len(stablecoins)} stablecoins")
    
    results = []
    async with httpx.AsyncClient(timeout=30) as client:
        for sid in stablecoins:
            # For promoted coins (not in hardcoded registry), mark in_progress
            is_promoted = sid not in STABLECOIN_REGISTRY
            if is_promoted:
                cfg = get_stablecoin_config(sid)
                if cfg:
                    _mark_scoring_status(cfg["coingecko_id"], "in_progress")
            try:
                result = await asyncio.wait_for(
                    score_stablecoin(client, sid), timeout=120
                )
                results.append(result)
                # After success, mark scored for promoted coins
                if is_promoted and "score" in result:
                    cfg = get_stablecoin_config(sid)
                    if cfg:
                        _mark_scoring_status(cfg["coingecko_id"], "scored")
                # Rate limit: pause between coins
                await asyncio.sleep(2.0)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout scoring {sid} (>120s) — skipping")
                results.append({"stablecoin": sid, "error": "timeout_120s"})
            except Exception as e:
                logger.error(f"Failed to score {sid}: {e}")
                results.append({"stablecoin": sid, "error": str(e)})
    
    elapsed = time.time() - start
    successes = sum(1 for r in results if "score" in r)
    logger.info(
        f"Scoring cycle complete: {successes}/{len(stablecoins)} scored in {elapsed:.0f}s"
    )

    # PSI scoring — runs after SII, uses DeFiLlama (no explorer budget)
    try:
        from app.collectors.psi_collector import run_psi_scoring
        logger.info("Running PSI scoring cycle...")
        psi_results = run_psi_scoring()
        logger.info(f"PSI scoring complete: {len(psi_results)} protocols scored")
        # Attest PSI scores
        try:
            from app.state_attestation import attest_state
            if psi_results:
                attest_state("psi_components", [{"slug": r.get("protocol_slug", ""), "score": r.get("overall_score")} for r in psi_results if isinstance(r, dict)])
        except Exception as ae:
            logger.debug(f"PSI attestation skipped: {ae}")
    except Exception as e:
        logger.warning(f"PSI scoring failed: {e}")

    # PSI expansion pipeline — daily gate (discover → enrich → promote)
    try:
        from app.collectors.psi_collector import (
            collect_collateral_exposure,
            sync_collateral_to_backlog,
            discover_protocols,
            enrich_protocol_backlog,
            promote_eligible_protocols,
        )
        # Only run expansion once per day — check last run from DB
        last_expansion = fetch_one(
            "SELECT MAX(snapshot_date) AS latest FROM protocol_collateral_exposure"
        )
        last_date = last_expansion["latest"] if last_expansion else None
        hours_since = 25  # default: run if no prior record
        if last_date:
            from datetime import date
            days_diff = (date.today() - last_date).days if isinstance(last_date, date) else 1
            hours_since = days_diff * 24

        if hours_since >= 24:
            logger.info("Running PSI expansion pipeline...")
            collect_collateral_exposure()
            synced = sync_collateral_to_backlog()
            discovered = discover_protocols()
            enriched = enrich_protocol_backlog()
            promoted = promote_eligible_protocols()
            logger.info(
                f"PSI expansion: {synced} stablecoins synced, {discovered} discovered, "
                f"{enriched} enriched, {promoted} promoted"
            )
            try:
                from app.state_attestation import attest_state
                if discovered or promoted:
                    attest_state("psi_discoveries", [{"synced": synced, "discovered": discovered, "enriched": enriched, "promoted": promoted}])
            except Exception as ae:
                logger.debug(f"PSI discovery attestation skipped: {ae}")
        else:
            logger.info(f"PSI expansion skipped — last ran {hours_since:.0f}h ago")
    except Exception as e:
        logger.warning(f"PSI expansion pipeline failed: {e}")

    # -------------------------------------------------------------------------
    # RPI scoring — daily gate (governance data changes slowly)
    # -------------------------------------------------------------------------
    try:
        last_rpi = fetch_one(
            "SELECT MAX(computed_at) AS latest FROM rpi_scores"
        )
        rpi_age_hours = 25  # default: run if no prior record
        if last_rpi and last_rpi.get("latest"):
            latest = last_rpi["latest"]
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            rpi_age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

        if rpi_age_hours >= 24:
            logger.info("Running RPI scoring pipeline...")
            # Collect governance data first
            from app.rpi.snapshot_collector import collect_snapshot_proposals
            from app.rpi.tally_collector import collect_tally_proposals
            from app.rpi.parameter_collector import collect_parameter_changes
            collect_snapshot_proposals()
            collect_tally_proposals()
            collect_parameter_changes()

            # Phase 2A: Automate lens components
            try:
                from app.rpi.forum_scraper import scrape_all_forums, update_vendor_diversity_lens
                forum_results = scrape_all_forums(since_days=90)
                logger.info(f"RPI forum scraper: {sum(forum_results.values())} posts across {len(forum_results)} protocols")
                update_vendor_diversity_lens()
            except Exception as fa_err:
                logger.warning(f"RPI forum scraper failed: {fa_err}")

            try:
                from app.rpi.docs_scorer import score_all_docs
                score_all_docs()
            except Exception as ds_err:
                logger.warning(f"RPI docs scorer failed: {ds_err}")

            try:
                from app.rpi.incident_detector import run_incident_detection
                run_incident_detection()
            except Exception as id_err:
                logger.warning(f"RPI incident detection failed: {id_err}")

            # Phase 2B: Expansion pipeline (weekly gate)
            try:
                last_expansion = fetch_one(
                    "SELECT MAX(created_at) AS latest FROM rpi_protocol_config WHERE discovery_source != 'manual'"
                )
                expansion_age = 169  # default: run if no records
                if last_expansion and last_expansion.get("latest"):
                    exp_ts = last_expansion["latest"]
                    if exp_ts.tzinfo is None:
                        exp_ts = exp_ts.replace(tzinfo=timezone.utc)
                    expansion_age = (datetime.now(timezone.utc) - exp_ts).total_seconds() / 3600
                if expansion_age >= 168:  # weekly
                    from app.rpi.expansion import run_expansion_pipeline
                    run_expansion_pipeline()
            except Exception as exp_err:
                logger.warning(f"RPI expansion failed: {exp_err}")

            # Score all protocols
            from app.rpi.scorer import run_rpi_scoring
            rpi_results = run_rpi_scoring()
            logger.info(f"RPI scoring complete: {len(rpi_results)} protocols scored")

            # Attest RPI scores (14th domain)
            try:
                from app.state_attestation import attest_state
                if rpi_results:
                    attest_state("rpi_components", [
                        {"slug": r.get("protocol_slug", ""), "score": r.get("overall_score")}
                        for r in rpi_results if isinstance(r, dict)
                    ])
            except Exception as ae:
                logger.debug(f"RPI attestation skipped: {ae}")
        else:
            logger.info(f"RPI scoring skipped — last ran {rpi_age_hours:.0f}h ago")
    except Exception as e:
        logger.warning(f"RPI scoring pipeline failed: {e}")

    # -------------------------------------------------------------------------
    # CDA collection — daily gate via DB timestamp
    # -------------------------------------------------------------------------
    try:
        cda_interval_hours = int(os.environ.get("CDA_COLLECTION_INTERVAL_HOURS", "24"))
        last_cda = fetch_one(
            "SELECT MAX(extracted_at) AS latest FROM cda_vendor_extractions"
        )
        cda_age_hours = 25  # default: run if no prior record
        if last_cda and last_cda.get("latest"):
            latest = last_cda["latest"]
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            cda_age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

        if cda_age_hours >= cda_interval_hours:
            logger.info("Running CDA collection pipeline...")
            from app.services.cda_collector import run_collection
            await run_collection()
            logger.info("CDA collection complete")
        else:
            logger.info(f"CDA collection skipped — last ran {cda_age_hours:.1f}h ago")
    except Exception as e:
        logger.warning(f"CDA collection failed: {e}")

    # -------------------------------------------------------------------------
    # Wallet batch re-index — every cycle (500 stalest wallets)
    # -------------------------------------------------------------------------
    try:
        from app.indexer.pipeline import run_pipeline_batch
        logger.info("Running wallet batch re-index (500 stalest wallets)...")
        reindex_result = await run_pipeline_batch(batch_size=500)
        logger.info(
            f"Wallet re-index complete: {reindex_result.get('processed', 0)} processed, "
            f"{reindex_result.get('scored', 0)} scored, "
            f"{reindex_result.get('errors', 0)} errors, "
            f"{reindex_result.get('remaining', '?')} remaining"
        )
    except Exception as e:
        logger.warning(f"Wallet batch re-index failed: {e}")

    # -------------------------------------------------------------------------
    # Wallet expansion + profile rebuild — daily gate via DB timestamp
    # -------------------------------------------------------------------------
    try:
        last_expansion_row = fetch_one(
            "SELECT MAX(created_at) AS latest FROM wallet_graph.wallets WHERE created_at > NOW() - INTERVAL '48 hours'"
        )
        wallet_expansion_age = 25
        if last_expansion_row and last_expansion_row.get("latest"):
            latest = last_expansion_row["latest"]
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            wallet_expansion_age = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

        if wallet_expansion_age >= 24:
            # Wallet expansion — seed new addresses from under-covered stablecoins
            try:
                from app.indexer.expander import run_wallet_expansion
                logger.info("Running wallet expansion pipeline...")
                expansion_result = await run_wallet_expansion(max_etherscan_calls=50)
                logger.info(
                    f"Wallet expansion complete: {expansion_result.get('new_wallets_seeded', 0)} seeded, "
                    f"{expansion_result.get('etherscan_calls_used', 0)} Etherscan calls used"
                )
            except Exception as e:
                logger.warning(f"Wallet expansion failed: {e}")

            # Profile rebuild — cap at 2000 stalest wallets, 30-min timeout
            try:
                from app.indexer.profiles import rebuild_all_profiles
                logger.info("Rebuilding wallet profiles (max 2000, 30-min timeout)...")
                profile_result = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, rebuild_all_profiles, 2000),
                    timeout=1800,
                )
                logger.info(
                    f"Profile rebuild complete: {profile_result.get('built', 0)} built, "
                    f"{profile_result.get('errors', 0)} errors out of {profile_result.get('total', 0)} addresses"
                )
            except asyncio.TimeoutError:
                logger.warning("Profile rebuild hit 30-minute timeout — will continue next cycle")
            except Exception as e:
                logger.warning(f"Profile rebuild failed: {e}")
        else:
            logger.info(f"Wallet expansion skipped — last ran {wallet_expansion_age:.1f}h ago")
    except Exception as e:
        logger.warning(f"Wallet expansion gate check failed: {e}")

    # -------------------------------------------------------------------------
    # Verification agent cycle — every cycle
    # -------------------------------------------------------------------------
    try:
        from app.agent.watcher import run_agent_cycle
        result = run_agent_cycle()
        if result:
            logger.info(f"Agent cycle: {result.get('assessments', 0)} assessments")
    except Exception as e:
        logger.warning(f"Agent cycle error: {e}")

    # -------------------------------------------------------------------------
    # Treasury flow detection — every cycle, minimal API budget
    # -------------------------------------------------------------------------
    try:
        from app.collectors.treasury_flows import collect_treasury_events
        logger.info("Running treasury flow detection...")
        treasury_events = await collect_treasury_events()
        logger.info(f"Treasury flow detection: {len(treasury_events)} events")
    except Exception as e:
        logger.warning(f"Treasury flow detection failed: {e}")

    # -------------------------------------------------------------------------
    # Edge building — daily gate via edge_build_status table (~10h cycle)
    # -------------------------------------------------------------------------
    try:
        edge_ts_row = fetch_one(
            "SELECT EXTRACT(EPOCH FROM MAX(last_built_at)) AS ts FROM wallet_graph.edge_build_status"
        )
        edge_last_ts = float(edge_ts_row["ts"]) if edge_ts_row and edge_ts_row.get("ts") else 0
        edge_age_hours = (time.time() - edge_last_ts) / 3600

        if edge_age_hours >= 10:
            for edge_chain in ["ethereum", "base", "arbitrum", "solana"]:
                try:
                    from app.indexer.edges import run_edge_builder
                    logger.info(f"Running edge builder for {edge_chain} (top 100 unbuilt wallets by value, 15-min timeout)...")
                    edge_result = await asyncio.wait_for(
                        run_edge_builder(max_wallets=100, priority="value", chain=edge_chain),
                        timeout=900,
                    )
                    logger.info(
                        f"Edge builder ({edge_chain}) complete: {edge_result.get('wallets_processed', 0)} wallets, "
                        f"{edge_result.get('total_edges_created', 0)} edges"
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"Edge building for {edge_chain} hit 15-minute timeout — moving to next chain")
                except Exception as e:
                    logger.warning(f"Edge building failed for {edge_chain}: {e}")

            # Decay + prune after edge building
            try:
                from app.indexer.edges import decay_edges, prune_stale_edges
                decay_result = decay_edges()
                logger.info(f"Edge decay: {decay_result.get('edges_decayed', 0)} edges recalculated")
                prune_result = prune_stale_edges()
                logger.info(f"Edge prune: {prune_result.get('edges_archived', 0)} archived")
            except Exception as e:
                logger.warning(f"Edge decay/prune failed: {e}")
        else:
            logger.info(f"Edge building skipped — last ran {edge_age_hours:.1f}h ago")
    except Exception as e:
        logger.warning(f"Edge build gate check failed: {e}")

    # -------------------------------------------------------------------------
    # Health sweep + alerting — every cycle
    # -------------------------------------------------------------------------
    try:
        from app.ops.tools.health_checker import run_all_checks
        logger.info("Running health sweep...")
        health_results = run_all_checks()
        healthy_count = sum(1 for r in health_results if r.get("status") == "healthy")
        total_count = len(health_results)
        logger.info(f"Health sweep: {healthy_count}/{total_count} healthy")

        failures = [r for r in health_results if r.get("status") in ("degraded", "down")]
        if failures:
            try:
                from app.ops.tools.alerter import check_and_alert_health
                await check_and_alert_health(health_results)
                logger.info(f"Health alerts dispatched for {len(failures)} failing system(s)")
            except Exception as alert_err:
                logger.warning(f"Health alert dispatch failed: {alert_err}")
    except Exception as e:
        logger.warning(f"Health sweep failed: {e}")

    # -------------------------------------------------------------------------
    # Generate daily pulse after all scoring + indexing
    # -------------------------------------------------------------------------
    try:
        from app.pulse_generator import run_daily_pulse
        run_daily_pulse()
    except Exception as e:
        logger.warning(f"Daily pulse generation failed: {e}")

    # Run actor classification after pulse, before discovery
    try:
        from app.actor_classification import classify_all_active
        actor_result = classify_all_active()
        logger.info(
            f"Actor classification: {actor_result.get('classified', 0)} classified, "
            f"{actor_result.get('reclassified', 0)} reclassified"
        )
    except Exception as e:
        logger.warning(f"Actor classification failed: {e}")

    # Run discovery layer after actor classification
    try:
        from app.discovery import run_discovery_cycle
        run_discovery_cycle()
    except Exception as e:
        logger.warning(f"Discovery cycle failed: {e}")

    # Provenance attestation (13th domain)
    try:
        from app.state_attestation import attest_state
        from app.database import fetch_all
        prov_rows = fetch_all("SELECT source_domain, attestation_hash, proved_at FROM provenance_proofs WHERE proved_at > NOW() - INTERVAL '2 hours'")
        if prov_rows:
            attest_state("provenance", [dict(r) for r in prov_rows])
    except Exception as e:
        logger.debug(f"Provenance attestation skipped: {e}")

    # -------------------------------------------------------------------------
    # Static evidence collection — daily gate (24h)
    # Captures source page snapshots + content hashes for static components.
    # Runs after CDA collection since both are daily-gated evidence tasks.
    # -------------------------------------------------------------------------
    try:
        last_evidence = fetch_one(
            "SELECT MAX(captured_at) AS latest FROM static_evidence"
        )
        evidence_age_hours = 25  # default: run if no prior record
        if last_evidence and last_evidence.get("latest"):
            latest_ev = last_evidence["latest"]
            if latest_ev.tzinfo is None:
                latest_ev = latest_ev.replace(tzinfo=timezone.utc)
            evidence_age_hours = (datetime.now(timezone.utc) - latest_ev).total_seconds() / 3600

        if evidence_age_hours >= 24:
            logger.info("Running static evidence collection pipeline...")
            from app.collectors.static_evidence import run_static_evidence_collection
            result = run_static_evidence_collection()
            logger.info(
                f"Static evidence collection complete: "
                f"captured={result['captured']} skipped={result['skipped']} "
                f"stale={result['stale_detected']} errors={result['errors']}"
            )
        else:
            logger.info(f"Static evidence collection skipped — last ran {evidence_age_hours:.1f}h ago")
    except Exception as e:
        logger.warning(f"Static evidence collection failed: {e}")

    # -------------------------------------------------------------------------
    # Daily digest — send operational summary once per 24h
    # -------------------------------------------------------------------------
    try:
        last_digest = fetch_one(
            "SELECT MAX(sent_at) AS latest FROM ops_alert_log WHERE alert_type = 'daily_digest'"
        )
        digest_age_hours = 25
        if last_digest and last_digest.get("latest"):
            latest = last_digest["latest"]
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            digest_age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

        if digest_age_hours >= 24:
            logger.info("Assembling daily digest...")
            from app.ops.tools.health_checker import get_latest_health
            health = get_latest_health()
            healthy_cnt = sum(1 for r in health if r.get("status") == "healthy")
            total_cnt = len(health)
            failing = [r for r in health if r.get("status") in ("degraded", "down")]

            sii_row = fetch_one("SELECT COUNT(*) as cnt, MAX(calculated_at) as latest FROM scores")
            sii_count = sii_row["cnt"] if sii_row else 0
            sii_age = "?"
            if sii_row and sii_row.get("latest"):
                _sii_ts = sii_row["latest"]
                if _sii_ts.tzinfo is None:
                    _sii_ts = _sii_ts.replace(tzinfo=timezone.utc)
                sii_age = f"{(datetime.now(timezone.utc) - _sii_ts).total_seconds() / 3600:.1f}"

            psi_row = fetch_one("SELECT COUNT(*) as cnt, MAX(scored_at) as latest FROM psi_scores")
            psi_count = psi_row["cnt"] if psi_row else 0
            psi_age = "?"
            if psi_row and psi_row.get("latest"):
                _psi_ts = psi_row["latest"]
                if _psi_ts.tzinfo is None:
                    _psi_ts = _psi_ts.replace(tzinfo=timezone.utc)
                psi_age = f"{(datetime.now(timezone.utc) - _psi_ts).total_seconds() / 3600:.1f}"

            db_conns = fetch_one("SELECT count(*) as cnt FROM pg_stat_activity WHERE datname = current_database()")
            conn_count = db_conns["cnt"] if db_conns else "?"

            failures_summary = ""
            if failing:
                failures_summary = "\nFailing systems:\n" + "\n".join(
                    f"  - {f['system']}: {f.get('status', '?')}" for f in failing
                )

            msg = (
                f"Basis Daily Digest\n\n"
                f"Systems: {healthy_cnt}/{total_cnt} healthy\n"
                f"SII: {sii_count} coins scored, last: {sii_age}h ago\n"
                f"PSI: {psi_count} protocols scored, last: {psi_age}h ago\n"
                f"DB: {conn_count} active connections\n"
                f"{failures_summary if failing else 'All systems operational.'}"
            )

            from app.ops.tools.alerter import send_alert as _send_digest
            await _send_digest("daily_digest", msg)
            logger.info("Daily digest sent")
        else:
            logger.debug(f"Daily digest skipped — last sent {digest_age_hours:.1f}h ago")
    except Exception as e:
        logger.warning(f"Daily digest failed: {e}")

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

    # Seed email alert channel if not configured
    try:
        existing = fetch_one("SELECT id FROM ops_alert_config WHERE channel = 'email'")
        if not existing:
            execute(
                "INSERT INTO ops_alert_config (channel, config, alert_types, enabled) VALUES (%s, %s, %s, TRUE)",
                ("email", "{}",
                 ["health_failure", "engagement_response", "state_growth", "daily_digest", "service_restart"]),
            )
            logger.info("Email alert channel seeded in ops_alert_config")
        else:
            execute(
                "UPDATE ops_alert_config SET alert_types = %s, enabled = TRUE WHERE channel = 'email'",
                (["health_failure", "engagement_response", "state_growth", "daily_digest", "service_restart"],),
            )
    except Exception as e:
        logger.warning(f"Alert config seed skipped: {e}")

    # Startup notification
    try:
        from app.ops.tools.alerter import send_alert
        await send_alert("service_restart", "Worker started. Beginning first cycle.")
    except Exception as e:
        logger.warning(f"Worker startup alert failed: {e}")

    CYCLE_TIMEOUT = 45 * 60  # 45 minutes max per scoring cycle

    try:
        if args.coin:
            async with httpx.AsyncClient(timeout=30) as client:
                result = await score_stablecoin(client, args.coin)
                print(result)
        elif args.loop:
            logger.info(f"Starting worker loop (interval: {args.interval} min)")
            gov_cycle_counter = 0
            while True:
                try:
                    await asyncio.wait_for(run_scoring_cycle(), timeout=CYCLE_TIMEOUT)
                except asyncio.TimeoutError:
                    logger.error(f"Scoring cycle exceeded {CYCLE_TIMEOUT}s timeout — aborting cycle")
                
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
