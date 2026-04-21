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
from app.collectors.coingecko import fetch_current, extract_price_context

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("worker")

_current_cycle_stats = None


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

    all_components = []

    # Run all registered collectors via the registry
    from app.collectors.registry import run_all_collectors
    all_components = await run_all_collectors(
        client, stablecoin_id, cfg, _current_cycle_stats
    )

    # Blockscout shadow comparison (non-blocking, during evaluation period only)
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

    try:
        from app.utils.data_source_comparator import compare_contract_abi, compare_token_holder_count, is_comparison_active
        if is_comparison_active():
            contract = cfg.get("contract", "")
            if contract:
                await safe_collect("blockscout_abi_compare", compare_contract_abi(client, contract))
                await safe_collect("blockscout_holder_compare", compare_token_holder_count(client, contract))
    except Exception:
        pass  # Shadow comparison is non-critical

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
    _cg_fix_score = {"susd": "nusd", "spark": "spark-protocol"}
    current = await fetch_current(client, _cg_fix_score.get(cfg["coingecko_id"], cfg["coingecko_id"]))
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
# Cycle diagnostics — runs at startup and end of every fast cycle
# =============================================================================

def run_cycle_diagnostics():
    """Log stale types, provenance gaps, API budget, and usage stats."""
    # 1. Stale types
    try:
        _stale_thresholds = {
            "liquidity_depth": ("snapshot_at", 3),
            "exchange_snapshots": ("snapshot_at", 3),
            "entity_snapshots_hourly": ("snapshot_at", 3),
            "yield_snapshots": ("snapshot_at", 26),
            "governance_proposals": ("captured_at", 26),
            "peg_snapshots_5m": ("timestamp", 26),
            "mint_burn_events": ("collected_at", 26),
            "contract_surveillance": ("scanned_at", 170),
            "dex_pool_ohlcv": ("timestamp", 6),
            "market_chart_history": ("timestamp", 26),
            "scores": ("computed_at", 3),
            "psi_scores": ("computed_at", 26),
        }
        _stale_found = []
        for _tbl, (_col, _max_h) in _stale_thresholds.items():
            try:
                _row = fetch_one(f"SELECT MAX({_col}) as latest FROM {_tbl}")
                if _row and _row.get("latest"):
                    _lt = _row["latest"]
                    if _lt.tzinfo is None:
                        _lt = _lt.replace(tzinfo=timezone.utc)
                    _age = (datetime.now(timezone.utc) - _lt).total_seconds() / 3600
                    if _age > _max_h:
                        _stale_found.append(f"{_tbl} (age={_age:.1f}h, threshold={_max_h}h)")
                else:
                    _stale_found.append(f"{_tbl} (empty, threshold={_max_h}h)")
            except Exception as _e:
                _stale_found.append(f"{_tbl} (error: {_e})")
        if _stale_found:
            logger.error(f"[stale_diagnostic] stale={len(_stale_found)}: {', '.join(_stale_found)}")
        else:
            logger.error("[stale_diagnostic] all data types fresh")
    except Exception as _e:
        logger.error(f"[stale_diagnostic] failed: {_e}")

    # 2. Provenance gaps
    try:
        _configured = fetch_all("SELECT id, schedule FROM provenance_sources WHERE enabled = true")
        _active = fetch_all(
            "SELECT DISTINCT source_domain FROM provenance_proofs WHERE proved_at > NOW() - INTERVAL '24 hours'"
        )
        _hourly_ids = {r["id"] for r in (_configured or []) if r.get("schedule") == "hourly"}
        _weekly_ids = {r["id"] for r in (_configured or []) if r.get("schedule") == "weekly"}
        _act_ids = {r["source_domain"] for r in (_active or [])}
        _missing_hourly = sorted(_hourly_ids - _act_ids)
        _missing_weekly = sorted(_weekly_ids - _act_ids)
        _producing_hourly = len(_hourly_ids) - len(_missing_hourly)
        logger.error(
            f"[provenance_gap] hourly: {_producing_hourly}/{len(_hourly_ids)} producing, "
            f"weekly: {len(_weekly_ids)}, "
            f"missing_hourly={_missing_hourly}"
        )
        if _missing_weekly:
            logger.error(f"[provenance_gap] weekly sources (not expected in 24h): {_missing_weekly}")
        _extra = sorted(_act_ids - _hourly_ids - _weekly_ids)
        if _extra:
            logger.error(f"[provenance_gap] producing but not configured: {_extra}")
    except Exception as _e:
        logger.error(f"[provenance_gap] failed: {_e}")

    # 3. API budget — read from new tracker (in-memory) + DB fallback
    try:
        _limits = {"coingecko": 16_600, "etherscan": 200_000, "blockscout": 100_000}
        _parts = []
        # Try new tracker first
        try:
            from app.utils.api_tracker import tracker as _bt
            _budget = _bt.get_budget_summary()
            for _p, _today in sorted(_budget.items()):
                _lim = _limits.get(_p)
                if _lim:
                    _parts.append(f"{_p}={_today:,}/{_lim:,} ({round(_today/_lim*100)}%)")
                else:
                    _parts.append(f"{_p}={_today:,}")
        except Exception:
            pass
        # Fallback to DB
        if not _parts:
            try:
                _db_budget = fetch_all("""
                    SELECT provider, SUM(total_calls) as total
                    FROM api_usage_hourly
                    WHERE hour > NOW() - INTERVAL '24 hours'
                    GROUP BY provider ORDER BY total DESC
                """)
                for _r in (_db_budget or []):
                    _p, _today = _r["provider"], int(_r["total"])
                    _lim = _limits.get(_p)
                    if _lim:
                        _parts.append(f"{_p}={_today:,}/{_lim:,} ({round(_today/_lim*100)}%)")
                    else:
                        _parts.append(f"{_p}={_today:,}")
            except Exception:
                pass
        logger.error(f"[api_budget] {', '.join(_parts) if _parts else 'no calls tracked yet'}")
    except Exception as _e:
        logger.error(f"[api_budget] failed: {_e}")

    # 4. API usage table verification
    try:
        _hourly = fetch_one("SELECT COUNT(*) as cnt, MAX(hour) as latest FROM api_usage_hourly")
        _tracker = fetch_one("SELECT COUNT(*) as cnt, MAX(recorded_at) as latest FROM api_usage_tracker")
        logger.error(
            f"[api_usage_verify] hourly: {_hourly['cnt'] if _hourly else 0} rows, "
            f"latest={_hourly.get('latest') if _hourly else 'none'} | "
            f"tracker: {_tracker['cnt'] if _tracker else 0} rows, "
            f"latest={_tracker.get('latest') if _tracker else 'none'}"
        )
    except Exception as _e:
        logger.error(f"[api_usage_verify] failed: {_e}")


# =============================================================================
# Orchestrator: Fast cycle — critical scoring + lightweight tasks (<15 min)
# =============================================================================

async def run_fast_cycle():
    """Critical scoring + lightweight tasks. Must complete in <15 min."""
    fast_start = time.time()
    logger.info("=== Fast cycle start ===")

    global _current_cycle_stats
    from app.collectors.registry import CycleStats, sync_provenance_sources
    _current_cycle_stats = CycleStats()

    # Sync provenance source registry so new collectors get prover coverage
    try:
        sync_provenance_sources()
    except Exception as e:
        logger.warning(f"Provenance source sync failed (non-critical): {e}")

    # Seed any missing provenance sources from local config (idempotent)
    try:
        from app.data_layer.prover_source_registry import seed_from_local_config
        seed_from_local_config()
    except Exception as e:
        logger.debug(f"Provenance seed from local config skipped: {e}")

    # -------------------------------------------------------------------------
    # SII scoring — score all stablecoins
    # -------------------------------------------------------------------------
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

    sii_elapsed = time.time() - fast_start
    successes = sum(1 for r in results if "score" in r)
    logger.info(
        f"Scoring cycle complete: {successes}/{len(stablecoins)} scored in {sii_elapsed:.0f}s"
    )

    # -------------------------------------------------------------------------
    # PSI scoring — runs after SII, uses DeFiLlama (no explorer budget)
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # Morpho Blue isolated-market exposure — fills protocol_collateral_exposure
    # for Morpho (DeFiLlama yields API doesn't index isolated markets).
    # -------------------------------------------------------------------------
    try:
        from app.collectors.morpho_blue import run_morpho_blue_collection
        morpho_result = run_morpho_blue_collection()
        if morpho_result.get("enabled"):
            logger.info(
                f"Morpho Blue exposure: {morpho_result.get('exposure_rows', 0)} rows "
                f"({morpho_result.get('stablecoin_rows', 0)} stablecoin) "
                f"from {morpho_result.get('markets', 0)} markets"
            )
    except Exception as e:
        logger.warning(f"Morpho Blue collection failed: {e}")

    # -------------------------------------------------------------------------
    # Bridge monitoring — every cycle (lightweight HTTP checks)
    # -------------------------------------------------------------------------
    try:
        from app.collectors.bridge_monitors import run_bridge_monitoring
        logger.info("Running bridge monitoring...")
        bridge_monitor_results = run_bridge_monitoring()
        monitored = sum(1 for r in bridge_monitor_results if "success_rate" in r)
        logger.info(f"Bridge monitoring complete: {monitored}/{len(bridge_monitor_results)} bridges checked")
    except Exception as e:
        logger.warning(f"Bridge monitoring failed: {e}")

    # -------------------------------------------------------------------------
    # Exchange health checks — every cycle (lightweight HTTP pings)
    # -------------------------------------------------------------------------
    try:
        from app.collectors.exchange_health import run_exchange_health_monitoring
        logger.info("Running exchange health monitoring...")
        exchange_health_results = run_exchange_health_monitoring()
        healthy = sum(1 for r in exchange_health_results if r.get("is_healthy"))
        logger.info(f"Exchange health: {healthy}/{len(exchange_health_results)} healthy")
    except Exception as e:
        logger.warning(f"Exchange health monitoring failed: {e}")

    # -------------------------------------------------------------------------
    # Data layer — fetch + store directly in worker.py (proven pattern)
    # No collector store functions. worker.py owns the INSERT.
    # -------------------------------------------------------------------------
    logger.error("=== DATA LAYER START ===")

    import json as _dj, math as _dm
    from app.database import get_cursor as _dl_gc, fetch_all as _dl_fa, fetch_one as _dl_fo

    def _sn(v):
        if v is None: return None
        try:
            f = float(v)
            return None if (_dm.isnan(f) or _dm.isinf(f)) else f
        except (TypeError, ValueError): return None

    CG_KEY = os.environ.get("COINGECKO_API_KEY", "")
    CG_HDR = {"x-cg-pro-api-key": CG_KEY, "Accept": "application/json"} if CG_KEY else {"Accept": "application/json"}
    CG_BASE = "https://pro-api.coingecko.com/api/v3" if CG_KEY else "https://api.coingecko.com/api/v3"

    # ==== 1. ENTITY SNAPSHOTS — all scored entities ====
    try:
        _coins = _dl_fa("SELECT id, coingecko_id FROM stablecoins WHERE scoring_enabled = TRUE AND coingecko_id IS NOT NULL") or []
        _cg_fix = {"susd": "nusd", "spark": "spark-protocol"}
        _entities = [(r["id"], _cg_fix.get(r["coingecko_id"], r["coingecko_id"]), "stablecoin") for r in _coins]
        _psi = {"aave":"aave","compound-finance":"compound-governance-token","morpho":"morpho",
                "lido":"lido-dao","uniswap":"uniswap","curve-finance":"curve-dao-token",
                "convex-finance":"convex-finance","eigenlayer":"eigenlayer","sky":"maker",
                "spark":"spark-protocol","pendle":"pendle","ethena":"ethena"}
        for s,c in _psi.items(): _entities.append((s,c,"protocol_token"))

        _es_ok, _es_err = 0, 0
        async with httpx.AsyncClient(timeout=30) as _ec:
            for _eid, _cg, _et in _entities:
                try:
                    _r = await _ec.get(f"{CG_BASE}/coins/{_cg}",
                        params={"localization":"false","tickers":"false","market_data":"true",
                                "community_data":"false","developer_data":"false"}, headers=CG_HDR)
                    if _r.status_code != 200: continue
                    _md = _r.json().get("market_data",{})
                    with _dl_gc() as _c:
                        _c.execute("""INSERT INTO entity_snapshots_hourly
                            (entity_id,entity_type,market_cap,total_volume,price_usd,
                             price_change_24h,circulating_supply,total_supply,snapshot_at)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,NOW())""",
                            (_eid,_et,_sn(_md.get("market_cap",{}).get("usd")),
                             _sn(_md.get("total_volume",{}).get("usd")),
                             _sn(_md.get("current_price",{}).get("usd")),
                             _sn(_md.get("price_change_percentage_24h")),
                             _sn(_md.get("circulating_supply")),_sn(_md.get("total_supply"))))
                    _es_ok += 1
                except Exception as _e:
                    _es_err += 1
                    if _es_err <= 3: logger.error(f"entity fail {_eid}: {_e}")
                await asyncio.sleep(0.15)
        logger.error(f"=== ENTITIES: {_es_ok} ok, {_es_err} err, total={_dl_fo('SELECT COUNT(*) as c FROM entity_snapshots_hourly')} ===")
    except Exception as _e1: logger.error(f"=== ENTITIES FAILED: {_e1} ===")

    # ==== 2. EXCHANGE SNAPSHOTS ====
    try:
        _EX = ["binance","coinbase-exchange","okx","bybit_spot","kraken","kucoin","gate",
               "bitget","htx","crypto_com","mexc","bitfinex","bitstamp","gemini","lbank"]
        # CoinGecko exchange ID corrections (IDs that 404)
        _EX_FIX = {
            "coinbase-exchange": "gdax",   # CoinGecko still uses legacy 'gdax' for Coinbase
            "okx": "okex",                 # OKX listed as 'okex' on CoinGecko
            "htx": "huobi",                # HTX rebranded from Huobi, CG still uses 'huobi'
            "mexc": "mxc",                 # CoinGecko slug is 'mxc'
        }
        _ex_ok, _ex_err = 0, 0
        async with httpx.AsyncClient(timeout=30) as _xc:
            for _xid in _EX:
                _cg_xid = _EX_FIX.get(_xid, _xid)
                try:
                    _r = await _xc.get(f"{CG_BASE}/exchanges/{_cg_xid}", headers=CG_HDR)
                    if _r.status_code != 200: continue
                    _d = _r.json()
                    with _dl_gc() as _c:
                        _c.execute("""INSERT INTO exchange_snapshots
                            (exchange_id,name,trust_score,trust_score_rank,trade_volume_24h_btc,
                             year_established,country,trading_pairs,snapshot_at)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,NOW())""",
                            (_xid,_d.get("name"),_d.get("trust_score"),_d.get("trust_score_rank"),
                             _sn(_d.get("trade_volume_24h_btc")),_d.get("year_established"),
                             _d.get("country"),len(_d.get("tickers",[])) if _d.get("tickers") else None))
                    _ex_ok += 1
                except Exception as _e:
                    _ex_err += 1
                    if _ex_err <= 3: logger.error(f"exchange fail {_xid}: {_e}")
                await asyncio.sleep(0.15)
        logger.error(f"=== EXCHANGES: {_ex_ok} ok, {_ex_err} err, total={_dl_fo('SELECT COUNT(*) as c FROM exchange_snapshots')} ===")
    except Exception as _e2: logger.error(f"=== EXCHANGES FAILED: {_e2} ===")

    # ==== 3. YIELD SNAPSHOTS (DeFiLlama) ====
    try:
        async with httpx.AsyncClient(timeout=30) as _yc:
            _r = await _yc.get("https://yields.llama.fi/pools")
            _pools = _r.json().get("data",[]) if _r.status_code == 200 else []
        _rel = [p for p in _pools if (p.get("stablecoin") or any(
            s in (p.get("symbol","").upper()) for s in ["USDC","USDT","DAI","FRAX"]
        )) and (p.get("tvlUsd") or 0) >= 1_000_000][:200]
        _ys_start = time.time()
        _ys_rows = []
        for _p in _rel:
            _ys_rows.append((
                _p.get("pool",""), _p.get("project",""), _p.get("chain",""), _p.get("symbol",""),
                _sn(_p.get("apy")), _sn(_p.get("apyBase")), _sn(_p.get("apyReward")),
                _sn(_p.get("tvlUsd")), _p.get("stablecoin", False),
            ))
        _ys_ok = 0
        try:
            # Build single multi-row INSERT (one round-trip instead of 200)
            _ys_now = datetime.now(timezone.utc)
            _ys_rows_with_ts = [r + (_ys_now,) for r in _ys_rows]
            from psycopg2.extras import execute_values as _ev_ys
            with _dl_gc() as _c:
                _ev_ys(_c,
                    """INSERT INTO yield_snapshots
                       (pool_id,protocol,chain,asset,apy,apy_base,apy_reward,tvl_usd,stable_pool,snapshot_at)
                       VALUES %s
                       ON CONFLICT(pool_id,snapshot_at) DO UPDATE SET apy=EXCLUDED.apy,tvl_usd=EXCLUDED.tvl_usd""",
                    _ys_rows_with_ts, page_size=500,
                )
            _ys_ok = len(_ys_rows)
        except Exception as _yb:
            logger.error(f"yield batch failed, falling back to per-row: {_yb}")
            for _yr in _ys_rows:
                try:
                    with _dl_gc() as _c:
                        _c.execute(
                            """INSERT INTO yield_snapshots
                               (pool_id,protocol,chain,asset,apy,apy_base,apy_reward,tvl_usd,stable_pool,snapshot_at)
                               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                               ON CONFLICT(pool_id,snapshot_at) DO UPDATE SET apy=EXCLUDED.apy,tvl_usd=EXCLUDED.tvl_usd""",
                            _yr,
                        )
                    _ys_ok += 1
                except Exception:
                    pass
        logger.error(f"=== YIELDS: {_ys_ok}/{len(_ys_rows)} in {time.time()-_ys_start:.1f}s ===")
    except Exception as _e3: logger.error(f"=== YIELDS FAILED: {_e3} ===")

    # ==== 4. BRIDGE FLOWS — DEFERRED (constitution v9.3) ====
    # DeFiLlama paywalled all bridges endpoints (402/404) circa April 2026.
    # Deferred to Phase 2: direct contract monitoring using existing Etherscan quota.
    # Table schema retained for future backfill. See basis_protocol_v9_3_constitution_amendment.md.

    # ==== 5. PEG 5-MIN + MARKET CHART ====
    try:
        _pg_ok, _mc_ok = 0, 0
        _mc_start = time.time()
        _peg_coins = _dl_fa("SELECT id, coingecko_id FROM stablecoins WHERE scoring_enabled = TRUE AND coingecko_id IS NOT NULL") or []
        _cg_fix_mc = {"susd": "nusd", "spark": "spark-protocol"}
        async with httpx.AsyncClient(timeout=30) as _pc:
            for _sc in _peg_coins:
                _coin_start = time.time()
                _cg = _cg_fix_mc.get(_sc["coingecko_id"], _sc["coingecko_id"])
                try:
                    _r = await _pc.get(f"{CG_BASE}/coins/{_cg}/market_chart",
                        params={"vs_currency":"usd","days":1}, headers=CG_HDR)
                    if _r.status_code != 200:
                        await asyncio.sleep(0.15)
                        continue
                    from datetime import datetime as _pdt
                    from psycopg2.extras import execute_values as _ev
                    _peg_rows = []
                    _mc_rows = []
                    for _pt in _r.json().get("prices",[]):
                        _ts = _pdt.fromtimestamp(_pt[0]/1000, tz=timezone.utc)
                        _peg_rows.append((_sc["id"], _pt[1], _ts, round(abs(_pt[1]-1.0)*10000, 2)))
                        _mc_rows.append((_sc["coingecko_id"], _sc["id"], _ts, _pt[1]))
                    if _peg_rows:
                        try:
                            with _dl_gc() as _c:
                                _ev(_c,
                                    "INSERT INTO peg_snapshots_5m(stablecoin_id,price,timestamp,deviation_bps) "
                                    "VALUES %s ON CONFLICT DO NOTHING",
                                    _peg_rows, page_size=500,
                                )
                                _ev(_c,
                                    "INSERT INTO market_chart_history(coin_id,stablecoin_id,timestamp,price,granularity) "
                                    "VALUES %s ON CONFLICT DO NOTHING",
                                    [r + ('5min',) for r in _mc_rows], page_size=500,
                                )
                            _pg_ok += len(_peg_rows)
                            _mc_ok += len(_mc_rows)
                        except Exception as _ei:
                            logger.error(f"peg/mchart bulk insert fail {_sc['id']}: {_ei}")
                    logger.error(f"peg/mchart {_sc['id']}: {len(_peg_rows)} rows in {time.time()-_coin_start:.1f}s")
                except Exception as _e: logger.error(f"peg fail {_sc['id']}: {_e}")
                await asyncio.sleep(0.15)
        logger.error(f"=== PEG: {_pg_ok}, MCHART: {_mc_ok}, elapsed={time.time()-_mc_start:.1f}s ===")
    except Exception as _e5: logger.error(f"=== PEG FAILED: {_e5} ===")

    # ==== 6. LIQUIDITY DEPTH (CEX tickers) ====
    try:
        _liq_ok, _liq_err = 0, 0
        async with httpx.AsyncClient(timeout=30) as _lc:
            for _sc in (_peg_coins or [])[:10]:
                try:
                    _r = await _lc.get(f"{CG_BASE}/coins/{_sc['coingecko_id']}/tickers",
                        params={"include_exchange_logo":"false"}, headers=CG_HDR)
                    if _r.status_code != 200: continue
                    for _tk in _r.json().get("tickers",[])[:20]:
                        if (_tk.get("target","")).upper() not in ("USD","USDT","USDC","BUSD"): continue
                        try:
                            with _dl_gc() as _c:
                                _c.execute("""INSERT INTO liquidity_depth
                                    (asset_id,venue,venue_type,spread_bps,volume_24h,trust_score,snapshot_at)
                                    VALUES(%s,%s,'cex',%s,%s,%s,NOW())""",
                                    (_sc["id"],_tk.get("market",{}).get("identifier","?"),
                                     _sn((_tk.get("bid_ask_spread_percentage") or 0)*100),
                                     _sn(_tk.get("converted_volume",{}).get("usd")),
                                     _tk.get("trust_score")))
                            _liq_ok += 1
                        except Exception as _e:
                            _liq_err += 1
                except Exception: pass
                await asyncio.sleep(0.15)
        logger.error(f"=== LIQUIDITY: {_liq_ok} ok, {_liq_err} err, total={_dl_fo('SELECT COUNT(*) as c FROM liquidity_depth')} ===")
    except Exception as _e6: logger.error(f"=== LIQUIDITY FAILED: {_e6} ===")

    # ==== 7. MINT/BURN EVENTS (Etherscan tokentx, daily gate) ====
    try:
        _mb_last = _dl_fo("SELECT MAX(collected_at) as t FROM mint_burn_events")
        _mb_age = 25
        if _mb_last and _mb_last.get("t"):
            _mt = _mb_last["t"]
            if _mt.tzinfo is None: _mt = _mt.replace(tzinfo=timezone.utc)
            _mb_age = (datetime.now(timezone.utc) - _mt).total_seconds() / 3600
        if _mb_age >= 20:
            ETH_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
            _mb_ok, _mb_err = 0, 0
            _mb_coins = _dl_fa("SELECT id, contract FROM stablecoins WHERE scoring_enabled = TRUE AND contract IS NOT NULL") or []
            async with httpx.AsyncClient(timeout=15) as _mbc:
                for _sc in _mb_coins:
                    _contract = _sc.get("contract","")
                    if not _contract or not _contract.startswith("0x"): continue
                    try:
                        _r = await _mbc.get("https://api.etherscan.io/v2/api",
                            params={"chainid":1,"module":"account","action":"tokentx",
                                    "contractaddress":_contract,"page":1,"offset":100,
                                    "sort":"desc","apikey":ETH_KEY})
                        if _r.status_code != 200: continue
                        _txs = _r.json().get("result",[]) if _r.json().get("status")=="1" else []
                        for _tx in _txs:
                            _from = (_tx.get("from","")).lower()
                            _to = (_tx.get("to","")).lower()
                            if _from != "0x0000000000000000000000000000000000000000" and _to != "0x0000000000000000000000000000000000000000":
                                continue
                            _evt = "mint" if _from == "0x0000000000000000000000000000000000000000" else "burn"
                            try:
                                _raw_val = int(_tx.get("value","0"))
                                _dec = int(_tx.get("tokenDecimal","18"))
                                _amt = _raw_val / (10**_dec)
                            except: _amt = 0
                            if _amt < 1000: continue
                            _ts = None
                            if _tx.get("timeStamp"):
                                try: _ts = datetime.fromtimestamp(int(_tx["timeStamp"]), tz=timezone.utc)
                                except: pass
                            try:
                                with _dl_gc() as _c:
                                    _c.execute("""INSERT INTO mint_burn_events
                                        (stablecoin_id,chain,event_type,amount,tx_hash,block_number,
                                         from_address,to_address,timestamp,collected_at)
                                        VALUES(%s,'ethereum',%s,%s,%s,%s,%s,%s,%s,NOW())
                                        ON CONFLICT(chain,tx_hash,event_type) DO NOTHING""",
                                        (_sc["id"],_evt,_amt,_tx.get("hash",""),
                                         int(_tx.get("blockNumber",0)),_from,_to,_ts))
                                _mb_ok += 1
                            except Exception as _e:
                                _mb_err += 1
                                if _mb_err <= 3: logger.error(f"mintburn fail: {_e}")
                    except Exception as _e:
                        logger.error(f"mintburn fetch fail {_sc['id']}: {_e}")
                    await asyncio.sleep(0.15)
            logger.error(f"=== MINTBURN: {_mb_ok} ok, {_mb_err} err, coins={len(_mb_coins)}, total={_dl_fo('SELECT COUNT(*) as c FROM mint_burn_events')} ===")
        else:
            logger.error(f"=== MINTBURN: skipped (last {_mb_age:.0f}h ago) ===")
    except Exception as _e7: logger.error(f"=== MINTBURN FAILED: {_e7} ===")

    # ==== 8. PROTOCOL POOL WALLETS (Blockscout/Etherscan top holders, daily gate) ====
    try:
        _pw_last = _dl_fo("SELECT MAX(discovered_at) as t FROM protocol_pool_wallets")
        _pw_age = 25
        if _pw_last and _pw_last.get("t"):
            _pt = _pw_last["t"]
            if _pt.tzinfo is None: _pt = _pt.replace(tzinfo=timezone.utc)
            _pw_age = (datetime.now(timezone.utc) - _pt).total_seconds() / 3600
        if _pw_age >= 20:
            from app.collectors.protocol_adapters import get_all_receipt_tokens
            _registry = get_all_receipt_tokens()
            _pw_ok, _pw_err = 0, 0
            ETH_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
            async with httpx.AsyncClient(timeout=15) as _pwc:
                for (_proto, _sym, _chain), _rt in list(_registry.items())[:20]:
                    try:
                        _r = await _pwc.get("https://eth.blockscout.com/api",
                            params={"module":"token","action":"getTokenHolders",
                                    "contractaddress":_rt.contract,"page":1,"offset":50})
                        if _r.status_code != 200: continue
                        _holders = _r.json().get("result",[]) if _r.json().get("status")=="1" else []
                        for _h in _holders:
                            _addr = (_h.get("address") or _h.get("TokenHolderAddress","")).lower()
                            if not _addr or _addr == "0x0000000000000000000000000000000000000000": continue
                            try:
                                with _dl_gc() as _c:
                                    _c.execute("""INSERT INTO protocol_pool_wallets
                                        (protocol_slug,stablecoin_symbol,chain,wallet_address,
                                         pool_contract_address,discovered_at,last_seen)
                                        VALUES(%s,%s,%s,%s,%s,NOW(),NOW())
                                        ON CONFLICT(protocol_slug,stablecoin_symbol,chain,wallet_address)
                                        DO UPDATE SET last_seen=NOW()""",
                                        (_proto,_sym,_chain,_addr,_rt.contract.lower()))
                                _pw_ok += 1
                            except Exception as _e:
                                _pw_err += 1
                                if _pw_err <= 3: logger.error(f"pool_wallet fail: {_e}")
                    except Exception as _e:
                        logger.error(f"pool_wallet fetch fail {_proto}/{_sym}: {_e}")
                    await asyncio.sleep(0.25)
            logger.error(f"=== POOL_WALLETS: {_pw_ok} ok, {_pw_err} err ===")
        else:
            logger.error(f"=== POOL_WALLETS: skipped (last {_pw_age:.0f}h ago) ===")
    except Exception as _e8: logger.error(f"=== POOL_WALLETS FAILED: {_e8} ===")

    # ==== 9. GOVERNANCE VOTERS (Snapshot, daily gate) ====
    try:
        _gv_last = _dl_fo("SELECT MAX(collected_at) as t FROM governance_voters")
        _gv_age = 25
        if _gv_last and _gv_last.get("t"):
            _gt = _gv_last["t"]
            if _gt.tzinfo is None: _gt = _gt.replace(tzinfo=timezone.utc)
            _gv_age = (datetime.now(timezone.utc) - _gt).total_seconds() / 3600
        if _gv_age >= 20:
            _SPACES = ["aavedao.eth","lido-snapshot.eth","comp-vote.eth",
                        "uniswapgovernance.eth","curve.eth","morpho.eth"]
            _gv_ok, _gv_err = 0, 0
            async with httpx.AsyncClient(timeout=15) as _gvc:
                for _space in _SPACES:
                    _proto = _space.replace(".eth","").replace("-snapshot","").replace("-vote","")
                    try:
                        # Get latest proposal
                        _r = await _gvc.post("https://hub.snapshot.org/graphql", json={
                            "query": """query($space:String!){proposals(where:{space:$space},first:1,orderBy:"created",orderDirection:desc){id}}""",
                            "variables": {"space": _space}})
                        _props = _r.json().get("data",{}).get("proposals",[])
                        if not _props: continue
                        _pid = _props[0]["id"]
                        # Get top voters
                        _r2 = await _gvc.post("https://hub.snapshot.org/graphql", json={
                            "query": """query($proposal:String!){votes(where:{proposal:$proposal},first:100,orderBy:"vp",orderDirection:desc){voter vp choice created}}""",
                            "variables": {"proposal": _pid}})
                        _votes = _r2.json().get("data",{}).get("votes",[])
                        for _v in _votes:
                            try:
                                _vts = None
                                if _v.get("created"):
                                    try: _vts = datetime.fromtimestamp(_v["created"], tz=timezone.utc)
                                    except: pass
                                with _dl_gc() as _c:
                                    _c.execute("""INSERT INTO governance_voters
                                        (protocol,source,proposal_id,voter_address,voting_power,choice,created_at,collected_at)
                                        VALUES(%s,'snapshot',%s,%s,%s,%s,%s,NOW())
                                        ON CONFLICT(protocol,proposal_id,voter_address) DO UPDATE SET collected_at=NOW()""",
                                        (_proto,_pid,_v.get("voter",""),_sn(_v.get("vp")),
                                         _v.get("choice"),_vts))
                                _gv_ok += 1
                            except Exception as _e:
                                _gv_err += 1
                                if _gv_err <= 3: logger.error(f"gov_voter fail: {_e}")
                    except Exception as _e:
                        logger.error(f"gov_voter fetch fail {_space}: {_e}")
                    await asyncio.sleep(0.5)
            logger.error(f"=== GOV_VOTERS: {_gv_ok} ok, {_gv_err} err ===")
        else:
            logger.error(f"=== GOV_VOTERS: skipped (last {_gv_age:.0f}h ago) ===")
    except Exception as _e9: logger.error(f"=== GOV_VOTERS FAILED: {_e9} ===")

    # ==== 10. CONTRACT SURVEILLANCE (Etherscan source code, weekly gate) ====
    try:
        _cs_last = _dl_fo("SELECT MAX(scanned_at) as t FROM contract_surveillance")
        _cs_age = 170
        if _cs_last and _cs_last.get("t"):
            _ct = _cs_last["t"]
            if _ct.tzinfo is None: _ct = _ct.replace(tzinfo=timezone.utc)
            _cs_age = (datetime.now(timezone.utc) - _ct).total_seconds() / 3600
        if _cs_age >= 168:
            import hashlib as _hl
            ETH_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
            _cs_ok, _cs_err = 0, 0
            _cs_coins = _dl_fa("SELECT id, contract FROM stablecoins WHERE scoring_enabled = TRUE AND contract IS NOT NULL") or []
            async with httpx.AsyncClient(timeout=15) as _csc:
                for _sc in _cs_coins:
                    _addr = _sc.get("contract","")
                    if not _addr.startswith("0x"): continue
                    try:
                        _r = await _csc.get("https://api.etherscan.io/v2/api",
                            params={"chainid":1,"module":"contract","action":"getsourcecode",
                                    "address":_addr,"apikey":ETH_KEY})
                        if _r.status_code != 200: continue
                        _res = _r.json().get("result",[])
                        _src = _res[0] if isinstance(_res,list) and _res else {}
                        _code = _src.get("SourceCode","")
                        _hash = _hl.sha256(_code.encode()).hexdigest() if _code else None
                        with _dl_gc() as _c:
                            _c.execute("""INSERT INTO contract_surveillance
                                (entity_id,chain,contract_address,source_code_hash,
                                 is_upgradeable,has_admin_keys,scanned_at)
                                VALUES(%s,'ethereum',%s,%s,%s,%s,NOW())
                                ON CONFLICT(entity_id,chain,contract_address,scanned_at) DO NOTHING""",
                                (_sc["id"],_addr,_hash,
                                 "Proxy" in _code or "upgradeTo" in _code if _code else None,
                                 "onlyOwner" in _code or "onlyAdmin" in _code if _code else None))
                        _cs_ok += 1
                    except Exception as _e:
                        _cs_err += 1
                        if _cs_err <= 3: logger.error(f"contract_surv fail {_sc['id']}: {_e}")
                    await asyncio.sleep(0.15)
            logger.error(f"=== CONTRACT_SURV: {_cs_ok} ok, {_cs_err} err ===")
        else:
            logger.error(f"=== CONTRACT_SURV: skipped (last {_cs_age:.0f}h ago) ===")
    except Exception as _e10: logger.error(f"=== CONTRACT_SURV FAILED: {_e10} ===")

    logger.error("=== DATA LAYER END ===")
    await asyncio.sleep(5)

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
    # Generate daily pulse after scoring
    # -------------------------------------------------------------------------
    try:
        from app.pulse_generator import run_daily_pulse
        run_daily_pulse()
    except Exception as e:
        logger.warning(f"Daily pulse generation failed: {e}")

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

            sii_row = fetch_one("SELECT COUNT(*) as cnt, MAX(computed_at) as latest FROM scores")
            sii_count = sii_row["cnt"] if sii_row else 0
            sii_age = "?"
            if sii_row and sii_row.get("latest"):
                _sii_ts = sii_row["latest"]
                if _sii_ts.tzinfo is None:
                    _sii_ts = _sii_ts.replace(tzinfo=timezone.utc)
                sii_age = f"{(datetime.now(timezone.utc) - _sii_ts).total_seconds() / 3600:.1f}"

            psi_row = fetch_one("SELECT COUNT(*) as cnt, MAX(computed_at) as latest FROM psi_scores")
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

    # Pipeline 9: Parameter change check (lightweight — every cycle)
    try:
        from app.collectors.parameter_history import check_parameter_changes
        param_result = check_parameter_changes()
        if param_result.get("changes_detected", 0) > 0:
            logger.warning(f"Parameter changes detected: {param_result['changes_detected']}")
        elif param_result.get("parameters_checked", 0) > 0:
            logger.info(f"Parameter check: {param_result['parameters_checked']} params, no changes")
    except Exception as e:
        logger.error(f"Parameter change check failed: {e}")

    # Pipeline 10: Oracle deviation and latency behavioral record (every cycle)
    try:
        from app.collectors.oracle_behavior import collect_oracle_readings
        oracle_result = await collect_oracle_readings()
        logger.error(
            f"=== ORACLE: {oracle_result.get('oracles_read', 0)} read, "
            f"{oracle_result.get('readings_stored', 0)} stored, "
            f"{oracle_result.get('stress_events_detected', 0)} stress, "
            f"errors={oracle_result.get('errors', [])} ==="
        )
    except Exception as e:
        logger.error(f"Oracle behavior collector failed: {e}")

    # Log and persist collector performance stats
    if _current_cycle_stats:
        _current_cycle_stats.log_summary()
        _current_cycle_stats.store()
        _current_cycle_stats = None

    # Flush API trackers
    try:
        from app.api_usage_tracker import flush as _fast_flush
        _fast_flush()
    except Exception:
        pass
    try:
        from app.utils.api_tracker import tracker as _cycle_tracker
        _flushed = _cycle_tracker.flush()
        if _flushed:
            logger.error(f"[api_tracker] flushed {_flushed} rows to api_usage_hourly")
    except Exception:
        pass

    # Snapshot row counts for dashboard delta computation (pg_stat, instant)
    try:
        from app.data_layer.state_growth import snapshot_row_counts
        snapshot_row_counts()
    except Exception:
        pass

    # Gate status diagnostic — log whether expansion and edge gates are open
    try:
        from app.database import fetch_one as _gate_fo
        _edge_ts = _gate_fo("SELECT EXTRACT(EPOCH FROM MAX(last_built_at)) AS ts FROM wallet_graph.edge_build_status")
        _edge_last = float(_edge_ts["ts"]) if _edge_ts and _edge_ts.get("ts") else 0
        _edge_age_h = (time.time() - _edge_last) / 3600 if _edge_last > 0 else 999
        _wallet_max = _gate_fo("SELECT MAX(created_at) AS latest FROM wallet_graph.wallets WHERE created_at > NOW() - INTERVAL '48 hours'")
        _wallet_latest = _wallet_max.get("latest") if _wallet_max else None
        _wallet_age_h = 999
        if _wallet_latest:
            if _wallet_latest.tzinfo is None:
                _wallet_latest = _wallet_latest.replace(tzinfo=timezone.utc)
            _wallet_age_h = (datetime.now(timezone.utc) - _wallet_latest).total_seconds() / 3600
        logger.error(
            f"=== GATE STATUS: edge_age={_edge_age_h:.1f}h (open={_edge_age_h >= 10}), "
            f"wallet_expansion_age={_wallet_age_h:.1f}h (open={_wallet_age_h >= 24 or _wallet_latest is None}) ==="
        )
    except Exception as _ge:
        logger.error(f"=== GATE STATUS FAILED: {_ge} ===")

    # One-time diagnostic: data layer table row counts via pg_stat (instant, no scan)
    try:
        from app.database import fetch_all as _diag_fa
        _diag_rows = _diag_fa("""
            SELECT relname AS table_name, n_live_tup
            FROM pg_stat_user_tables
            WHERE relname IN (
                'entity_snapshots_hourly', 'liquidity_depth', 'yield_snapshots',
                'exchange_snapshots', 'bridge_flows', 'mint_burn_events',
                'peg_snapshots_5m', 'dex_pool_ohlcv', 'market_chart_history',
                'volatility_surfaces', 'correlation_matrices', 'contract_surveillance',
                'wallet_behavior_tags', 'protocol_pool_wallets', 'coherence_violations',
                'incident_events', 'oracle_price_readings', 'oracle_stress_events',
                'holder_clusters', 'concentration_snapshots',
                'protocol_parameter_changes', 'protocol_parameter_snapshots',
                'contract_upgrade_history'
            )
            ORDER BY n_live_tup DESC
        """)
        _diag_lines = [f"  {r['table_name']:35s} {r['n_live_tup']:>10,}" for r in (_diag_rows or [])]
        logger.error("=== DATA LAYER ROW COUNTS (pg_stat) ===\n" + "\n".join(_diag_lines))
    except Exception as _de:
        logger.error(f"=== DATA LAYER DIAGNOSTIC FAILED: {_de} ===")

    elapsed = time.time() - fast_start
    logger.error(f"=== Fast cycle complete in {elapsed:.0f}s ===")

    return {
        "results": results,
        "successes": successes,
        "total": len(stablecoins),
        "elapsed": round(elapsed, 1),
    }


# =============================================================================
# Orchestrator: Slow cycle — data enrichment tasks (up to 60 min)
# =============================================================================

async def run_slow_cycle():
    """Data enrichment tasks. Can take up to 60 min. Doesn't block scoring."""
    start = time.time()
    logger.info("=== Slow cycle start ===")

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
    # Circle 7 Index Scoring — LSTI, BRI, DOHI, VSRI, CXRI, TTI
    # Runs after PSI, uses DeFiLlama/CoinGecko (no extra explorer budget)
    # -------------------------------------------------------------------------
    try:
        from app.collectors.lst_collector import run_lsti_scoring
        logger.info("Running LSTI scoring cycle...")
        lsti_results = run_lsti_scoring()
        logger.info(f"LSTI scoring complete: {len(lsti_results)} LSTs scored")
    except Exception as e:
        logger.warning(f"LSTI scoring failed: {e}")

    try:
        from app.collectors.bridge_collector import run_bri_scoring
        logger.info("Running BRI scoring cycle...")
        bri_results = run_bri_scoring()
        logger.info(f"BRI scoring complete: {len(bri_results)} bridges scored")
    except Exception as e:
        logger.warning(f"BRI scoring failed: {e}")

    try:
        from app.collectors.vault_collector import run_vsri_scoring
        logger.info("Running VSRI scoring cycle...")
        vsri_results = run_vsri_scoring()
        logger.info(f"VSRI scoring complete: {len(vsri_results)} vaults scored")
    except Exception as e:
        logger.warning(f"VSRI scoring failed: {e}")

    try:
        from app.collectors.cex_collector import run_cxri_scoring
        logger.info("Running CXRI scoring cycle...")
        cxri_results = run_cxri_scoring()
        logger.info(f"CXRI scoring complete: {len(cxri_results)} exchanges scored")
    except Exception as e:
        logger.warning(f"CXRI scoring failed: {e}")

    try:
        from app.collectors.tti_collector import run_tti_scoring
        logger.info("Running TTI scoring cycle...")
        tti_results = run_tti_scoring()
        logger.info(f"TTI scoring complete: {len(tti_results)} treasury products scored")
    except Exception as e:
        logger.warning(f"TTI scoring failed: {e}")

    # -------------------------------------------------------------------------
    # On-chain governance reads — every slow cycle
    # Reads timelock delays, multisig configs, proxy patterns from contracts
    # -------------------------------------------------------------------------
    try:
        async with httpx.AsyncClient(timeout=30) as gov_client:
            from app.collectors.smart_contract import collect_governance_reads
            logger.info("Running on-chain governance reads...")
            gov_read_results = await collect_governance_reads(gov_client)
            logger.info(f"Governance reads complete: {len(gov_read_results)} components collected")
    except Exception as e:
        logger.warning(f"On-chain governance reads failed: {e}")

    # -------------------------------------------------------------------------
    # DEX pool data collection — 3-hour gate (pool data changes slowly)
    # -------------------------------------------------------------------------
    try:
        last_dex = fetch_one(
            "SELECT MAX(computed_at) AS latest FROM generic_index_scores WHERE index_id = 'dex_pool_data'"
        )
        dex_age_hours = 4  # default: run if no prior record
        if last_dex and last_dex.get("latest"):
            latest = last_dex["latest"]
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            dex_age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

        if dex_age_hours >= 3:
            from app.collectors.dex_pools import run_dex_pool_collection
            logger.info("Running DEX pool data collection...")
            dex_results = run_dex_pool_collection()
            scored = sum(1 for r in dex_results if "score" in r)
            logger.info(f"DEX pool collection complete: {scored} components across {len(dex_results)} entries")
        else:
            logger.info(f"DEX pool collection skipped — last ran {dex_age_hours:.1f}h ago")
    except Exception as e:
        logger.warning(f"DEX pool data collection failed: {e}")

    # -------------------------------------------------------------------------
    # Web research collection — daily gate (expensive Parallel API calls)
    # -------------------------------------------------------------------------
    try:
        last_research = fetch_one(
            "SELECT MAX(computed_at) AS latest FROM generic_index_scores WHERE index_id LIKE 'web_research_%'"
        )
        research_age_hours = 25
        if last_research and last_research.get("latest"):
            latest = last_research["latest"]
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            research_age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

        if research_age_hours >= 24:
            from app.collectors.web_research import run_web_research_collection
            logger.info("Running web research collection...")
            research_results = await run_web_research_collection()
            scored = sum(1 for r in research_results if "score" in r)
            logger.info(f"Web research complete: {scored} components collected")
        else:
            logger.info(f"Web research skipped — last ran {research_age_hours:.1f}h ago")
    except Exception as e:
        logger.warning(f"Web research collection failed: {e}")

    # -------------------------------------------------------------------------
    # Daily-gated: governance event collection (Snapshot/Tally)
    # -------------------------------------------------------------------------
    _gov_daily_gate_open = False
    try:
        last_gov_row = fetch_one(
            "SELECT MAX(created_at) AS latest FROM governance_events WHERE created_at > NOW() - INTERVAL '48 hours'"
        )
        gov_age_hours = 25
        if last_gov_row and last_gov_row.get("latest"):
            latest = last_gov_row["latest"]
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            gov_age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

        if gov_age_hours >= 24:
            _gov_daily_gate_open = True
            from app.collectors.governance_events import run_governance_event_collection
            logger.info("Running governance event collection...")
            gov_result = run_governance_event_collection()
            logger.info(f"Governance events: {gov_result.get('new_events', 0)} new across {gov_result.get('protocols_processed', 0)} protocols")
        else:
            logger.info(f"Governance events skipped — last ran {gov_age_hours:.1f}h ago")
    except Exception as e:
        logger.warning(f"Governance event collection failed: {e}")

    # -------------------------------------------------------------------------
    # Daily-gated: DOHI scoring (independent of governance event success)
    # -------------------------------------------------------------------------
    if _gov_daily_gate_open:
        try:
            from app.collectors.dao_collector import run_dohi_scoring
            logger.info("Running DOHI scoring cycle...")
            dohi_results = run_dohi_scoring()
            logger.info(f"DOHI scoring complete: {len(dohi_results)} DAOs scored")
        except Exception as e:
            logger.warning(f"DOHI scoring failed: {e}")

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
        current_wallet_count = fetch_one("SELECT COUNT(*) as cnt FROM wallet_graph.wallets")
        _wc = current_wallet_count["cnt"] if current_wallet_count else 0

        # Always run expansion — no gate. The expansion function itself is idempotent
        # and the graph needs to grow from 44K to 500K.
        from app.data_layer.wallet_expansion import run_wallet_graph_expansion
        logger.error(f"=== WALLET EXPANSION: started, target=10000, current={_wc} ===")
        expansion_result = await run_wallet_graph_expansion(
            target_new_wallets=10_000, max_etherscan_calls=5_000
        )
        new_wallets = expansion_result.get('new_wallets_seeded', 0)
        logger.error(f"=== WALLET EXPANSION: {new_wallets} new wallets discovered, result={expansion_result} ===")
    except Exception as e:
        logger.error(f"=== WALLET EXPANSION FAILED: {e} ===")

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
                        run_edge_builder(max_wallets=500, priority="value", chain=edge_chain),
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
    # Correlation matrices — daily, needs 30+ days of score history
    # -------------------------------------------------------------------------
    try:
        from app.data_layer.correlation_engine import run_correlation_computation
        logger.info("Running correlation computation...")
        corr_result = run_correlation_computation()
        logger.info(f"Correlation computation: {corr_result}")
    except Exception as e:
        logger.warning(f"Correlation computation failed: {e}")

    # -------------------------------------------------------------------------
    # Incident detection — every cycle, computed from existing data
    # -------------------------------------------------------------------------
    try:
        from app.data_layer.incident_detector import run_incident_detection
        logger.info("Running incident detection...")
        incident_result = run_incident_detection()
        total_incidents = sum(v for v in incident_result.values() if isinstance(v, int))
        logger.info(f"Incident detection: {total_incidents} incidents detected")
    except Exception as e:
        logger.warning(f"Incident detection failed: {e}")

    # -------------------------------------------------------------------------
    # DEX pool OHLCV — 6-hour gate, GeckoTerminal API
    # -------------------------------------------------------------------------
    try:
        ohlcv_last = fetch_one("SELECT MAX(timestamp) as t FROM dex_pool_ohlcv")
        ohlcv_age = 7
        if ohlcv_last and ohlcv_last.get("t"):
            _ot = ohlcv_last["t"]
            if hasattr(_ot, 'tzinfo') and _ot.tzinfo is None:
                _ot = _ot.replace(tzinfo=timezone.utc)
            if hasattr(_ot, 'timestamp'):
                ohlcv_age = (datetime.now(timezone.utc) - _ot).total_seconds() / 3600
        if ohlcv_age >= 6:
            from app.data_layer.ohlcv_collector import run_ohlcv_collection
            logger.info("Running DEX pool OHLCV collection...")
            ohlcv_result = await run_ohlcv_collection()
            logger.info(
                f"OHLCV collection: {ohlcv_result.get('records_stored', 0)} records, "
                f"{ohlcv_result.get('pools_found', 0)} pools"
            )
        else:
            logger.info(f"OHLCV collection skipped — last ran {ohlcv_age:.1f}h ago")
    except Exception as e:
        logger.warning(f"OHLCV collection failed: {e}")

    # -------------------------------------------------------------------------
    # Wallet behavior tagging — every cycle, computed from existing data
    # -------------------------------------------------------------------------
    try:
        from app.data_layer.wallet_behavior import run_behavioral_classification
        logger.info("Running wallet behavior classification...")
        behavior_result = run_behavioral_classification(batch_size=2000)
        tagged = behavior_result.get("wallets_classified", 0)
        skipped = behavior_result.get("skipped", 0)
        logger.error(
            f"=== WALLET BEHAVIOR: {tagged} wallets tagged, {skipped} skipped (insufficient history) ==="
        )
    except Exception as e:
        logger.warning(f"Wallet behavior classification failed: {e}")

    # =========================================================================
    # State-building pipelines — permanent historical record
    # These run daily and must never block other pipeline tasks.
    # =========================================================================

    # Pipeline 3: Contract upgrade detection (CRITICAL — run first)
    try:
        from app.collectors.contract_upgrades import collect_contract_upgrades
        logger.info("Running contract upgrade tracker...")
        upgrade_result = collect_contract_upgrades()
        logger.error(
            f"=== CONTRACT UPGRADES: {upgrade_result.get('entities_checked', 0)} checked, "
            f"{upgrade_result.get('upgrades_detected', 0)} upgrades, "
            f"{upgrade_result.get('first_captures', 0)} first captures ==="
        )
    except Exception as e:
        logger.error(f"Contract upgrade collector failed: {e}")

    # Pipeline 17: Rated validator performance (daily-gated internally)
    try:
        from app.collectors.rated_validators import collect_validator_performance
        validator_result = collect_validator_performance()
        logger.info(f"Validator performance: {validator_result}")
    except Exception as e:
        logger.error(f"Validator collector failed: {e}")

    # Pipeline 19: OpenSanctions screening (daily-gated internally)
    try:
        from app.collectors.sanctions_screening import run_sanctions_screening
        sanctions_result = run_sanctions_screening()
        logger.info(f"Sanctions screening: {sanctions_result}")
    except Exception as e:
        logger.error(f"Sanctions screening failed: {e}")

    # Pipeline 20: CourtListener enforcement history (weekly-gated internally)
    try:
        from app.collectors.enforcement_history import collect_enforcement_records
        enforcement_result = collect_enforcement_records()
        logger.info(f"Enforcement history: {enforcement_result}")
    except Exception as e:
        logger.error(f"Enforcement collector failed: {e}")

    # Pipeline 21: SEC EDGAR parent company financials (weekly-gated internally)
    try:
        from app.collectors.parent_company_financials import collect_parent_financials
        edgar_result = collect_parent_financials()
        logger.info(f"EDGAR financials: {edgar_result}")
    except Exception as e:
        logger.error(f"EDGAR collector failed: {e}")

    # Pipeline 16 (contagion archive) integrates directly into divergence
    # signal emission below — not a separate worker task.

    # Pipeline 8: Governance proposal corpus (daily-gated internally)
    try:
        from app.collectors.governance_proposals import collect_governance_proposals
        logger.info("Running governance proposal collector...")
        gov_result = await collect_governance_proposals()
        logger.info(f"Governance proposals: {gov_result}")
        if gov_result.get("edits_detected", 0) > 0:
            logger.warning(f"Governance body edits detected: {gov_result['edits_detected']}")
    except Exception as e:
        logger.error(f"Governance proposal collector failed: {e}")

    # Pipeline 6: Contract dependency graph (daily-gated via snapshot table)
    try:
        from app.collectors.contract_dependencies import collect_contract_dependencies
        logger.info("Running contract dependency collector...")
        dep_result = await collect_contract_dependencies()
        logger.info(f"Contract dependencies: {dep_result}")
        if dep_result.get("removed_dependencies", 0) > 0:
            logger.warning(f"Contract dependencies removed: {dep_result['removed_dependencies']}")
    except Exception as e:
        logger.error(f"Contract dependency collector failed: {e}")

    # Pipeline 9: Parameter history (full run with daily snapshots)
    try:
        from app.collectors.parameter_history import collect_parameter_history
        logger.info("Running parameter history collector...")
        param_full_result = await collect_parameter_history()
        logger.error(
            f"=== PARAMETERS: {param_full_result.get('protocols_checked', 0)} protocols, "
            f"{param_full_result.get('changes_detected', 0)} changes, "
            f"{param_full_result.get('snapshots_stored', 0)} snapshots ==="
        )
    except Exception as e:
        logger.error(f"Parameter history collector failed: {e}")

    # Pipeline 14: Graph-clustered holder concentration (daily, computationally heavy)
    try:
        from app.collectors.clustered_concentration import collect_clustered_concentration
        logger.info("Running clustered concentration analysis...")
        _conc_t0 = time.time()
        conc_result = await collect_clustered_concentration()
        _conc_elapsed = time.time() - _conc_t0
        logger.error(
            f"=== CONCENTRATION: {conc_result.get('stablecoins_analyzed', 0)} stablecoins, "
            f"{conc_result.get('clusters_computed', 0)} clusters, "
            f"{conc_result.get('snapshots_stored', 0)} snapshots ({_conc_elapsed:.0f}s) ==="
        )
    except Exception as e:
        logger.error(f"Clustered concentration failed: {e}")

    # -------------------------------------------------------------------------
    # Divergence detection — every cycle, store all signals
    # -------------------------------------------------------------------------
    try:
        from app.divergence import detect_all_divergences
        logger.info("Running divergence detection...")
        div_result = detect_all_divergences(store=True)
        div_summary = div_result.get("summary", {})
        logger.info(
            f"Divergence detection: {div_summary.get('total_signals', 0)} signals "
            f"({div_summary.get('critical', 0)} critical, {div_summary.get('alerts', 0)} alerts)"
        )
        try:
            from app.state_attestation import attest_state
            signals = div_result.get("divergence_signals", [])
            if signals:
                attest_state("divergence_signals", [
                    {"type": s.get("type"), "severity": s.get("severity")}
                    for s in signals
                ])
        except Exception as e:
            logger.warning(f"Divergence attestation failed: {e}")
    except Exception as e:
        logger.warning(f"Divergence detection failed: {e}")

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
    # Integrity checks — run and store for trending (every cycle)
    # -------------------------------------------------------------------------
    try:
        from app.integrity import check_all_and_store
        logger.info("Running integrity checks...")
        integrity_result = check_all_and_store()
        logger.info(f"Integrity: {integrity_result['status']} across {len(integrity_result['domains'])} domains")
    except Exception as e:
        logger.warning(f"Integrity check persistence failed: {e}")

    # -------------------------------------------------------------------------
    # Generate daily pulse after all scoring + indexing
    # -------------------------------------------------------------------------
    try:
        from app.pulse_generator import run_daily_pulse
        run_daily_pulse()
    except Exception as e:
        logger.warning(f"Daily pulse generation failed: {e}")

    # -------------------------------------------------------------------------
    # Daily report generation — persist report attestations for all scored entities
    # -------------------------------------------------------------------------
    try:
        last_report = fetch_one(
            "SELECT MAX(generated_at) AS latest FROM report_attestations WHERE generated_at > NOW() - INTERVAL '48 hours'"
        )
        report_age_hours = 25
        if last_report and last_report.get("latest"):
            latest = last_report["latest"]
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            report_age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

        if report_age_hours >= 24:
            from app.report import assemble_report_data
            logger.info("Generating daily report attestations...")
            report_count = 0

            # Stablecoin reports
            stablecoin_ids = get_scoring_ids_from_db()
            for sid in stablecoin_ids:
                try:
                    cfg = get_stablecoin_config(sid)
                    if cfg:
                        assemble_report_data("stablecoin", cfg["symbol"])
                        report_count += 1
                except Exception:
                    pass

            # Protocol reports
            try:
                from app.collectors.psi_collector import get_scoring_protocols
                for slug in get_scoring_protocols():
                    try:
                        assemble_report_data("protocol", slug)
                        report_count += 1
                    except Exception:
                        pass
            except Exception:
                pass

            logger.info(f"Report attestations: {report_count} reports generated")
        else:
            logger.info(f"Report generation skipped — last ran {report_age_hours:.1f}h ago")
    except Exception as e:
        logger.warning(f"Daily report generation failed: {e}")

    # -------------------------------------------------------------------------
    # Actor classification
    # -------------------------------------------------------------------------
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
    # PSI expansion pipeline — daily gate (discover → enrich → promote)
    # -------------------------------------------------------------------------
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
    # Pool wallet discovery — daily gate via DB timestamp
    # Discovers wallets in protocol stablecoin pools (e.g. Aave aUSDC holders)
    # and seeds them into the wallet graph for edge building + risk scoring.
    # -------------------------------------------------------------------------
    try:
        last_pool_wallet = fetch_one(
            "SELECT MAX(discovered_at) AS latest FROM protocol_pool_wallets"
        )
        pool_wallet_age_hours = 25  # default: run if no prior record
        if last_pool_wallet and last_pool_wallet.get("latest"):
            latest = last_pool_wallet["latest"]
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            pool_wallet_age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

        if pool_wallet_age_hours >= 24:
            from app.collectors.pool_wallet_collector import run_pool_wallet_collection
            logger.info("Running pool wallet discovery...")
            pool_result = await run_pool_wallet_collection()
            logger.info(
                f"Pool wallet discovery complete: {pool_result.get('pools_processed', 0)} pools, "
                f"{pool_result.get('wallets_discovered', 0)} holders, "
                f"{pool_result.get('wallets_seeded', 0)} new wallets seeded"
            )
        else:
            logger.info(f"Pool wallet discovery skipped — last ran {pool_wallet_age_hours:.1f}h ago")
    except Exception as e:
        logger.warning(f"Pool wallet discovery failed: {e}")

    # -------------------------------------------------------------------------
    # Daily coherence sweep — cross-domain state consistency
    # -------------------------------------------------------------------------
    try:
        last_coherence = fetch_one(
            "SELECT MAX(created_at) AS latest FROM coherence_reports"
        )
        coherence_age_hours = 25
        if last_coherence and last_coherence.get("latest"):
            _coh_ts = last_coherence["latest"]
            if _coh_ts.tzinfo is None:
                _coh_ts = _coh_ts.replace(tzinfo=timezone.utc)
            coherence_age_hours = (datetime.now(timezone.utc) - _coh_ts).total_seconds() / 3600

        if coherence_age_hours >= 24:
            from app.coherence import run_coherence_sweep
            logger.info("Running coherence sweep...")
            coh_report = run_coherence_sweep()
            logger.info(
                f"Coherence sweep: {coh_report['domains_checked']} domains, "
                f"{coh_report['issues_found']} issues"
            )
        else:
            logger.info(f"Coherence sweep skipped -- last ran {coherence_age_hours:.1f}h ago")
    except Exception as e:
        logger.warning(f"Coherence sweep failed: {e}")

    elapsed = time.time() - start
    logger.info(f"=== Slow cycle complete in {elapsed:.0f}s ===")


# =============================================================================
# Orchestrator: Parallel slow cycle via enrichment worker
# =============================================================================

async def run_slow_cycle_parallel():
    """
    Parallel slow cycle using the enrichment worker.
    All enrichment tasks run concurrently with shared rate limiting.
    This replaces the sequential run_slow_cycle when enabled.
    """
    start = time.time()
    logger.info("=== Parallel slow cycle start ===")

    try:
        from app.enrichment_worker import run_enrichment_pipeline
        result = await run_enrichment_pipeline()
        logger.error(
            f"=== ENRICHMENT PIPELINE COMPLETE: {result.get('succeeded', 0)}/{result.get('total_tasks', 0)} "
            f"tasks in {result.get('total_elapsed_s', 0)}s ==="
        )
    except Exception as e:
        logger.error(f"=== ENRICHMENT PIPELINE FAILED: {type(e).__name__}: {e} ===")
        import traceback as _tb
        logger.error(_tb.format_exc())
        await run_slow_cycle()
        return

    # Post-pipeline tasks that depend on enrichment results.
    # Each is wrapped in asyncio.wait_for(to_thread(...)) so a blocking
    # DB query cannot hang the entire slow cycle.
    POST_TASK_TIMEOUT = 300  # 5 min per task

    # Health sweep + alerting
    try:
        async def _health_sweep():
            from app.ops.tools.health_checker import run_all_checks
            logger.info("Running health sweep...")
            health_results = await asyncio.to_thread(run_all_checks)
            healthy_count = sum(1 for r in health_results if r.get("status") == "healthy")
            logger.info(f"Health sweep: {healthy_count}/{len(health_results)} healthy")
            failures = [r for r in health_results if r.get("status") in ("degraded", "down")]
            if failures:
                try:
                    from app.ops.tools.alerter import check_and_alert_health
                    await check_and_alert_health(health_results)
                except Exception as alert_err:
                    logger.warning(f"Health alert dispatch failed: {alert_err}")
        await asyncio.wait_for(_health_sweep(), timeout=POST_TASK_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("Health sweep timed out after %ds", POST_TASK_TIMEOUT)
    except Exception as e:
        logger.warning(f"Health sweep failed: {e}")

    # Integrity checks
    try:
        async def _integrity():
            from app.integrity import check_all_and_store
            result = await asyncio.to_thread(check_all_and_store)
            logger.info(f"Integrity: {result['status']} across {len(result['domains'])} domains")
        await asyncio.wait_for(_integrity(), timeout=POST_TASK_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("Integrity check timed out after %ds", POST_TASK_TIMEOUT)
    except Exception as e:
        logger.warning(f"Integrity check failed: {e}")

    # Actor classification
    try:
        async def _actors():
            from app.actor_classification import classify_all_active
            result = await asyncio.to_thread(classify_all_active)
            logger.info(f"Actor classification: {result.get('classified', 0)} classified")
        await asyncio.wait_for(_actors(), timeout=POST_TASK_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("Actor classification timed out after %ds", POST_TASK_TIMEOUT)
    except Exception as e:
        logger.warning(f"Actor classification failed: {e}")

    # Discovery cycle
    try:
        async def _discovery():
            from app.discovery import run_discovery_cycle
            await asyncio.to_thread(run_discovery_cycle)
        await asyncio.wait_for(_discovery(), timeout=POST_TASK_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("Discovery cycle timed out after %ds", POST_TASK_TIMEOUT)
    except Exception as e:
        logger.warning(f"Discovery cycle failed: {e}")

    # Coherence sweep
    try:
        async def _coherence():
            last_coherence = await asyncio.to_thread(
                fetch_one, "SELECT MAX(created_at) AS latest FROM coherence_reports"
            )
            coherence_age_hours = 25
            if last_coherence and last_coherence.get("latest"):
                _coh_ts = last_coherence["latest"]
                if _coh_ts.tzinfo is None:
                    _coh_ts = _coh_ts.replace(tzinfo=timezone.utc)
                coherence_age_hours = (datetime.now(timezone.utc) - _coh_ts).total_seconds() / 3600
            if coherence_age_hours >= 24:
                from app.coherence import run_coherence_sweep
                coh_report = await asyncio.to_thread(run_coherence_sweep)
                logger.info(
                    f"Coherence sweep: {coh_report['domains_checked']} domains, "
                    f"{coh_report['issues_found']} issues"
                )
            else:
                logger.info(f"Coherence sweep skipped -- last ran {coherence_age_hours:.1f}h ago")
        await asyncio.wait_for(_coherence(), timeout=POST_TASK_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("Coherence sweep timed out after %ds", POST_TASK_TIMEOUT)
    except Exception as e:
        logger.warning(f"Coherence sweep failed: {e}")

    # Wallet expansion — run every cycle, no gate
    try:
        async def _wallet_expansion():
            from app.data_layer.wallet_expansion import run_wallet_graph_expansion
            from app.database import fetch_one as _wfe
            _wc = _wfe("SELECT COUNT(*) as cnt FROM wallet_graph.wallets")
            _count = _wc["cnt"] if _wc else 0
            logger.error(f"=== WALLET EXPANSION: started, target=10000, current={_count} ===")
            result = await run_wallet_graph_expansion(target_new_wallets=10_000, max_etherscan_calls=5_000)
            logger.error(f"=== WALLET EXPANSION: {result.get('new_wallets_seeded', 0)} new wallets, result={result} ===")
        await asyncio.wait_for(_wallet_expansion(), timeout=POST_TASK_TIMEOUT)
    except asyncio.TimeoutError:
        logger.error("=== WALLET EXPANSION: timed out after %ds ===" % POST_TASK_TIMEOUT)
    except Exception as e:
        logger.error(f"=== WALLET EXPANSION FAILED: {e} ===")

    # Clustered concentration — daily
    try:
        async def _concentration():
            from app.collectors.clustered_concentration import collect_clustered_concentration
            result = await collect_clustered_concentration()
            logger.error(
                f"=== CONCENTRATION: {result.get('stablecoins_analyzed', 0)} stablecoins, "
                f"{result.get('clusters_computed', 0)} clusters, "
                f"{result.get('snapshots_stored', 0)} snapshots ==="
            )
        await asyncio.wait_for(_concentration(), timeout=POST_TASK_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("Concentration analysis timed out after %ds", POST_TASK_TIMEOUT)
    except Exception as e:
        logger.error(f"Concentration analysis failed: {e}")

    # Ensure oracle_registry table exists (migration may not have run)
    try:
        from app.database import execute as _exec_mig
        _exec_mig("""
            CREATE TABLE IF NOT EXISTS oracle_registry (
                id SERIAL PRIMARY KEY,
                oracle_address VARCHAR(42) NOT NULL,
                oracle_name VARCHAR(200) NOT NULL,
                oracle_provider VARCHAR(50) NOT NULL,
                chain VARCHAR(20) NOT NULL,
                asset_symbol VARCHAR(20) NOT NULL,
                quote_symbol VARCHAR(20) NOT NULL DEFAULT 'usd',
                decimals INTEGER NOT NULL DEFAULT 8,
                read_function VARCHAR(100),
                is_active BOOLEAN DEFAULT TRUE,
                entity_type VARCHAR(20),
                entity_slug VARCHAR(100),
                added_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (oracle_address, chain, asset_symbol)
            )
        """)
        _exec_mig("""
            CREATE TABLE IF NOT EXISTS oracle_price_readings (
                id SERIAL PRIMARY KEY,
                oracle_address VARCHAR(42) NOT NULL,
                oracle_name VARCHAR(200),
                oracle_provider VARCHAR(50),
                chain VARCHAR(20) NOT NULL,
                asset_symbol VARCHAR(20) NOT NULL,
                quote_symbol VARCHAR(20) NOT NULL DEFAULT 'usd',
                oracle_price DECIMAL(30,8) NOT NULL,
                oracle_price_raw VARCHAR(200),
                oracle_decimals INTEGER,
                cex_price DECIMAL(30,8),
                deviation_pct DECIMAL(10,6),
                deviation_abs DECIMAL(20,8),
                latency_seconds INTEGER,
                round_id VARCHAR(100),
                answer_timestamp TIMESTAMPTZ,
                recorded_at TIMESTAMPTZ DEFAULT NOW(),
                is_stress_event BOOLEAN DEFAULT FALSE,
                content_hash VARCHAR(66),
                attested_at TIMESTAMPTZ
            )
        """)
        _exec_mig("""
            CREATE TABLE IF NOT EXISTS oracle_stress_events (
                id SERIAL PRIMARY KEY,
                oracle_address VARCHAR(42) NOT NULL,
                oracle_name VARCHAR(200),
                asset_symbol VARCHAR(20) NOT NULL,
                chain VARCHAR(20) NOT NULL,
                event_type VARCHAR(50),
                event_start TIMESTAMPTZ NOT NULL,
                event_end TIMESTAMPTZ,
                duration_seconds INTEGER,
                max_deviation_pct DECIMAL(10,6),
                max_latency_seconds INTEGER,
                reading_count INTEGER DEFAULT 1,
                concurrent_sii_score DECIMAL(6,2),
                concurrent_psi_scores JSONB,
                affected_protocols JSONB,
                content_hash VARCHAR(66),
                attested_at TIMESTAMPTZ
            )
        """)
        # Seed oracle feeds if empty
        from app.database import fetch_one as _ofe
        _ocount = _ofe("SELECT COUNT(*) as cnt FROM oracle_registry")
        if _ocount and _ocount["cnt"] == 0:
            _exec_mig("""
                INSERT INTO oracle_registry
                    (oracle_address, oracle_name, oracle_provider, chain, asset_symbol, quote_symbol,
                     decimals, read_function, entity_type, entity_slug)
                VALUES
                    ('0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6', 'Chainlink USDC/USD', 'chainlink',
                     'ethereum', 'USDC', 'usd', 8, 'latestRoundData', 'stablecoin', 'usdc'),
                    ('0x3E7d1eAB13ad0104d2750B8863b489D65364e32D', 'Chainlink USDT/USD', 'chainlink',
                     'ethereum', 'USDT', 'usd', 8, 'latestRoundData', 'stablecoin', 'usdt'),
                    ('0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9', 'Chainlink DAI/USD', 'chainlink',
                     'ethereum', 'DAI', 'usd', 8, 'latestRoundData', 'stablecoin', 'dai'),
                    ('0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419', 'Chainlink ETH/USD', 'chainlink',
                     'ethereum', 'ETH', 'usd', 8, 'latestRoundData', NULL, NULL),
                    ('0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c', 'Chainlink BTC/USD', 'chainlink',
                     'ethereum', 'BTC', 'usd', 8, 'latestRoundData', NULL, NULL),
                    ('0x86392dC19c0b719886221c78AB11eb8Cf5c52812', 'Chainlink stETH/ETH', 'chainlink',
                     'ethereum', 'stETH', 'eth', 18, 'latestRoundData', 'stablecoin', 'steth')
                ON CONFLICT (oracle_address, chain, asset_symbol) DO NOTHING
            """)
            # Remove Pyth — pull-oracle ABI incompatible with Chainlink read path
            _exec_mig("DELETE FROM oracle_registry WHERE oracle_provider = 'pyth'")
            logger.error("=== ORACLE: seeded 6 oracle feeds into oracle_registry ===")
    except Exception as e:
        logger.warning(f"Oracle table creation/seeding failed (non-critical): {e}")

    # Mark non-attesting CDA issuers so they don't show as stale
    try:
        from app.database import execute as _cda_exec
        # USDD (TRON): minimal attestation practices, no standard reserve reports
        # DAI (MakerDAO/Sky): crypto-backed, uses on-chain collateral not attestation PDFs
        # FRAX (Frax Finance): algorithmic/hybrid, no standard attestation reports
        for _symbol, _method in [("USDD", "no_attestation"), ("DAI", "crypto_backed"), ("FRAX", "algorithmic")]:
            _cda_exec("""
                UPDATE cda_issuer_registry
                SET collection_method = %s, updated_at = NOW()
                WHERE UPPER(asset_symbol) = %s
                  AND collection_method NOT IN ('no_attestation', 'crypto_backed', 'algorithmic')
            """, (_method, _symbol))
    except Exception as e:
        logger.debug(f"CDA issuer method update skipped: {e}")

    # Contract surveillance re-scan — force if no scans in 24h
    try:
        from app.database import fetch_one as _csf
        _cs_latest = _csf("SELECT MAX(scanned_at) as latest FROM contract_surveillance")
        _cs_age = 999
        if _cs_latest and _cs_latest.get("latest"):
            _cslt = _cs_latest["latest"]
            if _cslt.tzinfo is None:
                _cslt = _cslt.replace(tzinfo=timezone.utc)
            _cs_age = (datetime.now(timezone.utc) - _cslt).total_seconds() / 3600
        if _cs_age >= 24:
            from app.data_layer.contract_surveillance import run_contract_surveillance
            logger.info("Running contract surveillance re-scan...")
            _cs_result = await run_contract_surveillance()
            logger.error(f"=== CONTRACT SURVEILLANCE: {_cs_result} ===")
    except Exception as e:
        logger.warning(f"Contract surveillance re-scan failed: {e}")

    # On-chain CDA verification — crypto-backed stablecoins (DAI, LUSD)
    try:
        from app.collectors.on_chain_cda import run_on_chain_cda_verification
        cda_result = await run_on_chain_cda_verification()
        logger.error(
            f"=== ON-CHAIN CDA: {cda_result.get('assets_read', 0)} read, "
            f"{cda_result.get('stored', 0)} stored ==="
        )
    except Exception as e:
        logger.warning(f"On-chain CDA verification failed: {e}")

    # Solana program monitoring — Drift, Jupiter, Raydium
    try:
        from app.collectors.solana_program_monitor import run_solana_program_monitoring
        sol_result = await run_solana_program_monitoring()
        logger.error(
            f"=== SOLANA PROGRAMS: {sol_result.get('programs_checked', 0)} checked, "
            f"{sol_result.get('upgrades_detected', 0)} upgrades, "
            f"{sol_result.get('immutable', 0)} immutable ==="
        )
    except Exception as e:
        logger.warning(f"Solana program monitoring failed: {e}")

    # Provenance health re-check (disabled sources)
    try:
        async def _provenance_recheck():
            from app.data_layer.prover_source_registry import run_provenance_health_recheck
            result = await run_provenance_health_recheck()
            logger.info(
                f"Provenance recheck: {result['checked']} checked, "
                f"{result['re_enabled']} re-enabled, {result['healed']} healed"
            )
        await asyncio.wait_for(_provenance_recheck(), timeout=POST_TASK_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("Provenance recheck timed out after %ds", POST_TASK_TIMEOUT)
    except Exception as e:
        logger.warning(f"Provenance recheck failed: {e}")

    # Track record: auto-log qualifying entries from this cycle's signals
    try:
        from app.track_record import detect_and_log_entries
        tr_result = detect_and_log_entries()
        logger.info(f"Track record: {tr_result.get('entries_logged', 0)} entries logged")
    except Exception as e:
        logger.error(f"track_record auto-log failed: {e}", exc_info=True)

    # Track record: evaluate pending followups (daily)
    try:
        from app.track_record_followups import evaluate_pending_followups
        fu_result = evaluate_pending_followups()
        logger.info(f"Track record followups: {fu_result.get('evaluated', 0)} evaluated")
    except Exception as e:
        logger.error(f"track_record followup eval failed: {e}", exc_info=True)

    # Flush API usage tracker
    try:
        from app.api_usage_tracker import flush
        flush()
    except Exception:
        pass

    elapsed = time.time() - start
    logger.info(f"=== Parallel slow cycle complete in {elapsed:.0f}s ===")


# =============================================================================
# Orchestrator: Full cycle wrapper (backward compat)
# =============================================================================

async def run_scoring_cycle():
    """Full cycle — used for single-run mode and backward compat."""
    result = await run_fast_cycle()
    logger.error("=== FAST CYCLE RETURNED, STARTING SLOW CYCLE ===")
    await run_slow_cycle_parallel()
    logger.error("=== SLOW CYCLE RETURNED, SCORING CYCLE COMPLETE ===")
    return result


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

    logger.error("[startup] worker main() entered — initializing pool")
    init_pool()
    logger.error("[startup] pool initialized — running schema fixes")

    # Wire API call tracking via httpx monkey-patch
    try:
        import httpx as _httpx_mod
        from app.utils.api_tracker import tracker as _api_tracker
        _orig_send = _httpx_mod.AsyncClient.send
        _patch_call_count = [0]
        async def _tracked_send(self, request, **kwargs):
            _t0 = time.monotonic()
            try:
                _resp = await _orig_send(self, request, **kwargs)
                _host = request.url.host or ""
                _provider = _host.split(".")[-2] if "." in _host else _host
                _api_tracker.record(_provider, str(request.url.path)[:100], _resp.status_code,
                                    int((time.monotonic() - _t0) * 1000))
                _patch_call_count[0] += 1
                if _patch_call_count[0] <= 3:
                    logger.error(f"[api_tracker] recorded: {_provider} {request.url.path} → {_resp.status_code}")
                return _resp
            except Exception as _te:
                _host = request.url.host or ""
                _provider = _host.split(".")[-2] if "." in _host else _host
                _api_tracker.record(_provider, str(request.url.path)[:100], 599,
                                    int((time.monotonic() - _t0) * 1000))
                _patch_call_count[0] += 1
                raise
        _httpx_mod.AsyncClient.send = _tracked_send
        logger.error("[startup] httpx monkey-patched for API tracking")
    except Exception as e:
        logger.error(f"[startup] httpx monkey-patch failed: {e}")

    # Ensure data layer tables exist (migration 058 may not have been fully applied)
    # Ensure data layer tables exist + column alignment
    _data_layer_creates = [
        "CREATE TABLE IF NOT EXISTS governance_voters (id BIGSERIAL PRIMARY KEY, protocol TEXT NOT NULL, source TEXT, proposal_id TEXT, voter_address TEXT NOT NULL, voting_power NUMERIC, choice INTEGER, created_at TIMESTAMPTZ, collected_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(protocol, proposal_id, voter_address))",
        "CREATE TABLE IF NOT EXISTS mint_burn_events (id BIGSERIAL PRIMARY KEY, stablecoin_id TEXT, chain TEXT NOT NULL DEFAULT 'ethereum', event_type TEXT NOT NULL, amount NUMERIC, tx_hash TEXT, block_number BIGINT, from_address TEXT, to_address TEXT, timestamp TIMESTAMPTZ, collected_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(chain, tx_hash, event_type))",
        "CREATE TABLE IF NOT EXISTS liquidity_depth (id BIGSERIAL PRIMARY KEY, asset_id TEXT NOT NULL, venue TEXT NOT NULL, chain TEXT NOT NULL DEFAULT 'ethereum', depth_usd_2pct NUMERIC, depth_usd_5pct NUMERIC, bid_depth NUMERIC, ask_depth NUMERIC, spread_bps NUMERIC, raw_data JSONB, snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(asset_id, venue, chain, snapshot_at))",
        "CREATE TABLE IF NOT EXISTS contract_surveillance (id SERIAL PRIMARY KEY, entity_id TEXT NOT NULL, chain TEXT NOT NULL, contract_address TEXT NOT NULL, has_admin_keys BOOLEAN, is_upgradeable BOOLEAN, has_pause_function BOOLEAN, has_blacklist BOOLEAN, timelock_hours NUMERIC, multisig_threshold TEXT, source_code_hash TEXT, analysis JSONB, scanned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(entity_id, chain, contract_address, scanned_at))",
        "CREATE TABLE IF NOT EXISTS protocol_parameter_changes (id SERIAL PRIMARY KEY, protocol_slug VARCHAR(100) NOT NULL, protocol_id INTEGER, parameter_name VARCHAR(200) NOT NULL, parameter_key VARCHAR(200) NOT NULL, asset_address VARCHAR(42), asset_symbol VARCHAR(20), contract_address VARCHAR(42) NOT NULL, chain VARCHAR(20) NOT NULL, previous_value DECIMAL(30,8), previous_value_raw VARCHAR(200), new_value DECIMAL(30,8), new_value_raw VARCHAR(200), value_unit VARCHAR(50), change_magnitude DECIMAL(10,4), change_direction VARCHAR(10), changed_at TIMESTAMPTZ NOT NULL, detected_at TIMESTAMPTZ DEFAULT NOW(), concurrent_sii_score DECIMAL(6,2), concurrent_psi_score DECIMAL(6,2), hours_since_last_sii_change DECIMAL(8,2), sii_trend_7d DECIMAL(6,2), change_context VARCHAR(100), content_hash VARCHAR(66), attested_at TIMESTAMPTZ)",
        "CREATE TABLE IF NOT EXISTS exchange_snapshots (id BIGSERIAL PRIMARY KEY, exchange_id TEXT NOT NULL, name TEXT, trust_score INTEGER, trust_score_rank INTEGER, trade_volume_24h_btc NUMERIC, year_established INTEGER, country TEXT, trading_pairs INTEGER, snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
        "CREATE TABLE IF NOT EXISTS yield_snapshots (id BIGSERIAL PRIMARY KEY, pool_id TEXT NOT NULL, protocol TEXT, chain TEXT, asset TEXT, apy NUMERIC, apy_base NUMERIC, apy_reward NUMERIC, tvl_usd NUMERIC, stable_pool BOOLEAN, snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(pool_id, snapshot_at))",
        "CREATE TABLE IF NOT EXISTS peg_snapshots_5m (id BIGSERIAL PRIMARY KEY, stablecoin_id TEXT NOT NULL, price NUMERIC, timestamp TIMESTAMPTZ NOT NULL, deviation_bps NUMERIC, UNIQUE(stablecoin_id, timestamp))",
        "CREATE TABLE IF NOT EXISTS entity_snapshots_hourly (id BIGSERIAL PRIMARY KEY, entity_id TEXT NOT NULL, entity_type TEXT, market_cap NUMERIC, total_volume NUMERIC, price_usd NUMERIC, price_change_24h NUMERIC, circulating_supply NUMERIC, total_supply NUMERIC, exchange_tickers_count INTEGER, developer_data JSONB, community_data JSONB, raw_data JSONB, snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
        "CREATE TABLE IF NOT EXISTS correlation_matrices (id BIGSERIAL PRIMARY KEY, matrix_type TEXT NOT NULL, window_days INTEGER NOT NULL, entity_ids JSONB, matrix_data JSONB, computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(matrix_type, window_days, computed_at))",
        "CREATE TABLE IF NOT EXISTS wallet_behavior_tags (id BIGSERIAL PRIMARY KEY, wallet_address TEXT NOT NULL, behavior_type TEXT NOT NULL, confidence NUMERIC, metrics JSONB, computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(wallet_address, behavior_type, computed_at))",
        "CREATE TABLE IF NOT EXISTS dex_pool_ohlcv (id BIGSERIAL PRIMARY KEY, pool_address TEXT NOT NULL, chain TEXT NOT NULL, dex TEXT, asset_id TEXT, timestamp TIMESTAMPTZ NOT NULL, open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC, trades_count INTEGER, UNIQUE(pool_address, chain, timestamp))",
        "CREATE TABLE IF NOT EXISTS volatility_surfaces (id BIGSERIAL PRIMARY KEY, asset_id TEXT NOT NULL, realized_vol_1d NUMERIC, realized_vol_7d NUMERIC, realized_vol_30d NUMERIC, realized_vol_90d NUMERIC, max_drawdown_7d NUMERIC, max_drawdown_30d NUMERIC, max_drawdown_90d NUMERIC, recovery_time_hours NUMERIC, raw_prices JSONB, computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(asset_id, computed_at))",
    ]
    _data_layer_alters = [
        "ALTER TABLE governance_voters ADD COLUMN IF NOT EXISTS source TEXT",
        "ALTER TABLE governance_voters ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ",
        "ALTER TABLE governance_voters ADD COLUMN IF NOT EXISTS collected_at TIMESTAMPTZ DEFAULT NOW()",
        "ALTER TABLE governance_voters ADD COLUMN IF NOT EXISTS voting_power NUMERIC",
        "ALTER TABLE governance_voters ADD COLUMN IF NOT EXISTS choice INTEGER",
        "ALTER TABLE mint_burn_events ADD COLUMN IF NOT EXISTS stablecoin_id TEXT",
        "ALTER TABLE mint_burn_events ADD COLUMN IF NOT EXISTS collected_at TIMESTAMPTZ DEFAULT NOW()",
        "ALTER TABLE mint_burn_events ADD COLUMN IF NOT EXISTS from_address TEXT",
        "ALTER TABLE mint_burn_events ADD COLUMN IF NOT EXISTS to_address TEXT",
        "ALTER TABLE mint_burn_events ADD COLUMN IF NOT EXISTS timestamp TIMESTAMPTZ",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS depth_usd_2pct NUMERIC",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS depth_usd_5pct NUMERIC",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS bid_depth NUMERIC",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS ask_depth NUMERIC",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS spread_bps NUMERIC",
        # liquidity_depth columns used by data_layer/liquidity_collector.py + worker fast cycle
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS venue_type TEXT",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS pool_address TEXT",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS bid_depth_1pct NUMERIC",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS ask_depth_1pct NUMERIC",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS bid_depth_2pct NUMERIC",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS ask_depth_2pct NUMERIC",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS volume_24h NUMERIC",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS trade_count_24h INTEGER",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS buy_sell_ratio NUMERIC",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS trust_score TEXT",
        "ALTER TABLE liquidity_depth ADD COLUMN IF NOT EXISTS liquidity_score NUMERIC",
        # dex_pool_ohlcv columns (safety — in case older CREATE TABLE was applied)
        "ALTER TABLE dex_pool_ohlcv ADD COLUMN IF NOT EXISTS pool_address TEXT",
        "ALTER TABLE dex_pool_ohlcv ADD COLUMN IF NOT EXISTS chain TEXT",
        "ALTER TABLE dex_pool_ohlcv ADD COLUMN IF NOT EXISTS dex TEXT",
        "ALTER TABLE dex_pool_ohlcv ADD COLUMN IF NOT EXISTS asset_id TEXT",
        "ALTER TABLE dex_pool_ohlcv ADD COLUMN IF NOT EXISTS timestamp TIMESTAMPTZ",
        "ALTER TABLE dex_pool_ohlcv ADD COLUMN IF NOT EXISTS open NUMERIC",
        "ALTER TABLE dex_pool_ohlcv ADD COLUMN IF NOT EXISTS high NUMERIC",
        "ALTER TABLE dex_pool_ohlcv ADD COLUMN IF NOT EXISTS low NUMERIC",
        "ALTER TABLE dex_pool_ohlcv ADD COLUMN IF NOT EXISTS close NUMERIC",
        "ALTER TABLE dex_pool_ohlcv ADD COLUMN IF NOT EXISTS volume NUMERIC",
        "ALTER TABLE dex_pool_ohlcv ADD COLUMN IF NOT EXISTS trades_count INTEGER",
    ]
    for _ddl in _data_layer_creates:
        try:
            execute(_ddl)
        except Exception as _de:
            logger.error(f"[startup] DDL failed: {str(_de)[:100]}")

    # Ensure incident_events table exists
    try:
        execute("""CREATE TABLE IF NOT EXISTS incident_events (
            id SERIAL PRIMARY KEY, entity_id TEXT NOT NULL, entity_type TEXT,
            incident_type TEXT NOT NULL, severity TEXT, title TEXT, description TEXT,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), ended_at TIMESTAMPTZ,
            detection_method TEXT DEFAULT 'automated', raw_data JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(entity_id, incident_type, started_at))""")
    except Exception as _ie:
        logger.error(f"[startup] incident_events DDL failed: {_ie}")

    # Ensure wallet_graph.wallets has a unique constraint on address
    try:
        execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_wallets_address_unique ON wallet_graph.wallets (address)")
    except Exception as _we:
        logger.error(f"[startup] wallets unique index failed: {_we}")

    # Ensure unique constraints exist for all ON CONFLICT targets
    _unique_indexes = [
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_gov_voters_unique ON governance_voters (protocol, proposal_id, voter_address)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_mint_burn_unique ON mint_burn_events (chain, tx_hash, event_type)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_liq_depth_unique ON liquidity_depth (asset_id, venue, chain, snapshot_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_exchange_snap_unique ON exchange_snapshots (exchange_id, snapshot_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_yield_snap_unique ON yield_snapshots (pool_id, snapshot_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_peg_5m_unique ON peg_snapshots_5m (stablecoin_id, timestamp)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_snap_unique ON entity_snapshots_hourly (entity_id, entity_type, snapshot_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_contract_surv_unique ON contract_surveillance (entity_id, chain, contract_address, scanned_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_corr_matrix_unique ON correlation_matrices (matrix_type, window_days, computed_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_wallet_behavior_unique ON wallet_behavior_tags (wallet_address, behavior_type, computed_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_unique ON dex_pool_ohlcv (pool_address, chain, timestamp)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_vol_surface_unique ON volatility_surfaces (asset_id, computed_at)",
    ]
    for _ui in _unique_indexes:
        try:
            execute(_ui)
        except Exception as _ue:
            logger.error(f"[startup] unique index failed: {_ui[:80]} — {_ue}")

    for _alt in _data_layer_alters:
        try:
            execute(_alt)
        except Exception as _ae:
            logger.error(f"[startup] ALTER failed: {_alt[:80]} — {_ae}")

    # Oracle stress_events + price_readings column fixes (migration 075 not applied)
    _oracle_alters = [
        "ALTER TABLE oracle_stress_events ADD COLUMN IF NOT EXISTS pre_stress_window_hours INTEGER DEFAULT 72",
        "ALTER TABLE oracle_stress_events ADD COLUMN IF NOT EXISTS pre_stress_readings_tagged INTEGER",
        "ALTER TABLE oracle_price_readings ADD COLUMN IF NOT EXISTS pre_stress_event_id BIGINT",
        # Backfill tracking columns (migration 077)
        "ALTER TABLE psi_scores ADD COLUMN IF NOT EXISTS backfilled BOOLEAN DEFAULT FALSE",
        "ALTER TABLE psi_scores ADD COLUMN IF NOT EXISTS backfill_source TEXT",
        "ALTER TABLE generic_index_scores ADD COLUMN IF NOT EXISTS backfilled BOOLEAN DEFAULT FALSE",
        "ALTER TABLE generic_index_scores ADD COLUMN IF NOT EXISTS backfill_source TEXT",
        "ALTER TABLE score_history ADD COLUMN IF NOT EXISTS backfilled BOOLEAN DEFAULT FALSE",
        "ALTER TABLE score_history ADD COLUMN IF NOT EXISTS backfill_source TEXT",
        "ALTER TABLE rpi_score_history ADD COLUMN IF NOT EXISTS backfilled BOOLEAN DEFAULT FALSE",
        "ALTER TABLE rpi_score_history ADD COLUMN IF NOT EXISTS backfill_source TEXT",
    ]
    for _alt in _oracle_alters:
        try:
            execute(_alt)
        except Exception as _ae:
            logger.error(f"[startup] oracle ALTER failed: {_ae}")

    # Ensure API usage tracking tables exist
    try:
        execute("""
            CREATE TABLE IF NOT EXISTS api_usage_tracker (
                id BIGSERIAL PRIMARY KEY,
                provider TEXT NOT NULL,
                endpoint TEXT,
                calls_count INTEGER DEFAULT 1,
                caller TEXT,
                response_status INTEGER,
                latency_ms INTEGER,
                recorded_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        execute("""
            CREATE TABLE IF NOT EXISTS api_usage_hourly (
                id SERIAL PRIMARY KEY,
                provider TEXT NOT NULL,
                hour TIMESTAMPTZ NOT NULL,
                total_calls INTEGER DEFAULT 0,
                success_calls INTEGER DEFAULT 0,
                error_calls INTEGER DEFAULT 0,
                avg_latency_ms INTEGER,
                p95_latency_ms INTEGER,
                callers JSONB,
                UNIQUE (provider, hour)
            )
        """)
    except Exception as e:
        logger.error(f"[startup] API usage table creation failed: {e}")

    # VACUUM ANALYZE churny tables — needs autocommit (can't run in transaction)
    try:
        import psycopg2 as _vac_pg
        _vac_url = os.environ.get("DATABASE_URL", "")
        if _vac_url:
            _vac_conn = _vac_pg.connect(_vac_url)
            _vac_conn.autocommit = True
            try:
                _vac_cur = _vac_conn.cursor()
                for _tbl in [
                    "wallet_graph.wallet_risk_scores",
                    "wallet_graph.wallet_holdings",
                    "component_readings",
                ]:
                    try:
                        _vac_cur.execute(f"VACUUM ANALYZE {_tbl}")
                    except Exception as _ve:
                        logger.error(f"[startup] VACUUM ANALYZE {_tbl} failed: {_ve}")
                        try:
                            _vac_cur.execute(f"ANALYZE {_tbl}")
                        except Exception:
                            pass
                # Regular ANALYZE for other key tables
                for _tbl in [
                    "score_history", "scores", "psi_scores",
                    "wallet_graph.wallets", "wallet_graph.wallet_edges",
                    "entity_snapshots_hourly", "data_provenance", "state_attestations",
                    "provenance_proofs", "assessment_events",
                ]:
                    try:
                        _vac_cur.execute(f"ANALYZE {_tbl}")
                    except Exception:
                        pass
                _vac_cur.close()
                logger.error("[startup] VACUUM ANALYZE complete")
            finally:
                _vac_conn.close()
    except Exception as e:
        logger.error(f"[startup] VACUUM ANALYZE skipped: {e}")

    # Create state_growth_snapshots table if needed
    try:
        execute("""
            CREATE TABLE IF NOT EXISTS state_growth_snapshots (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                row_count BIGINT NOT NULL,
                snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
                UNIQUE (table_name, snapshot_date)
            )
        """)
    except Exception:
        pass

    # Ensure oracle tables exist (migration 073 may not have been applied)
    try:
        execute("""CREATE TABLE IF NOT EXISTS oracle_registry (
            id SERIAL PRIMARY KEY,
            oracle_address VARCHAR(42) NOT NULL,
            oracle_name VARCHAR(200) NOT NULL,
            oracle_provider VARCHAR(50) NOT NULL,
            chain VARCHAR(20) NOT NULL,
            asset_symbol VARCHAR(20) NOT NULL,
            quote_symbol VARCHAR(20) NOT NULL DEFAULT 'usd',
            decimals INTEGER NOT NULL DEFAULT 8,
            read_function VARCHAR(100),
            is_active BOOLEAN DEFAULT TRUE,
            entity_type VARCHAR(20),
            entity_slug VARCHAR(100),
            added_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (oracle_address, chain, asset_symbol)
        )""")
        execute("""CREATE TABLE IF NOT EXISTS oracle_price_readings (
            id SERIAL PRIMARY KEY,
            oracle_address VARCHAR(42) NOT NULL,
            oracle_name VARCHAR(200),
            oracle_provider VARCHAR(50),
            chain VARCHAR(20) NOT NULL,
            asset_symbol VARCHAR(20) NOT NULL,
            quote_symbol VARCHAR(20) NOT NULL DEFAULT 'usd',
            oracle_price DECIMAL(30,8) NOT NULL,
            oracle_price_raw VARCHAR(200),
            oracle_decimals INTEGER,
            cex_price DECIMAL(30,8),
            deviation_pct DECIMAL(10,6),
            deviation_abs DECIMAL(20,8),
            latency_seconds INTEGER,
            round_id VARCHAR(100),
            answer_timestamp TIMESTAMPTZ,
            recorded_at TIMESTAMPTZ DEFAULT NOW(),
            is_stress_event BOOLEAN DEFAULT FALSE,
            content_hash VARCHAR(66),
            attested_at TIMESTAMPTZ
        )""")
        execute("""CREATE TABLE IF NOT EXISTS oracle_stress_events (
            id SERIAL PRIMARY KEY,
            oracle_address VARCHAR(42) NOT NULL,
            oracle_name VARCHAR(200),
            asset_symbol VARCHAR(20) NOT NULL,
            chain VARCHAR(20) NOT NULL,
            event_type VARCHAR(50),
            event_start TIMESTAMPTZ NOT NULL,
            event_end TIMESTAMPTZ,
            duration_seconds INTEGER,
            max_deviation_pct DECIMAL(10,6),
            max_latency_seconds INTEGER,
            reading_count INTEGER DEFAULT 1,
            concurrent_sii_score DECIMAL(6,2),
            concurrent_psi_scores JSONB,
            affected_protocols JSONB,
            content_hash VARCHAR(66),
            attested_at TIMESTAMPTZ
        )""")
        # Seed oracle feeds if empty
        oracle_count = fetch_one("SELECT COUNT(*) as cnt FROM oracle_registry")
        if oracle_count and oracle_count["cnt"] == 0:
            execute("""
                INSERT INTO oracle_registry
                    (oracle_address, oracle_name, oracle_provider, chain, asset_symbol, quote_symbol,
                     decimals, read_function, entity_type, entity_slug)
                VALUES
                    ('0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6', 'Chainlink USDC/USD', 'chainlink',
                     'ethereum', 'USDC', 'usd', 8, 'latestRoundData', 'stablecoin', 'usdc'),
                    ('0x3E7d1eAB13ad0104d2750B8863b489D65364e32D', 'Chainlink USDT/USD', 'chainlink',
                     'ethereum', 'USDT', 'usd', 8, 'latestRoundData', 'stablecoin', 'usdt'),
                    ('0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9', 'Chainlink DAI/USD', 'chainlink',
                     'ethereum', 'DAI', 'usd', 8, 'latestRoundData', 'stablecoin', 'dai'),
                    ('0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419', 'Chainlink ETH/USD', 'chainlink',
                     'ethereum', 'ETH', 'usd', 8, 'latestRoundData', NULL, NULL),
                    ('0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c', 'Chainlink BTC/USD', 'chainlink',
                     'ethereum', 'BTC', 'usd', 8, 'latestRoundData', NULL, NULL),
                    ('0x86392dC19c0b719886221c78AB11eb8Cf5c52812', 'Chainlink stETH/ETH', 'chainlink',
                     'ethereum', 'stETH', 'eth', 18, 'latestRoundData', 'stablecoin', 'steth')
                ON CONFLICT (oracle_address, chain, asset_symbol) DO NOTHING
            """)
            execute("DELETE FROM oracle_registry WHERE oracle_provider = 'pyth'")
            logger.info("Oracle registry seeded with 6 Chainlink feeds at startup")
    except Exception as e:
        logger.error(f"[startup] Oracle table creation failed: {e}")

    logger.error("[bridges] collector disabled — DeFiLlama paywalled all endpoints. See constitution v9.3 for deferral rationale.")

    # Schema introspection — log actual columns for tables with known drift
    _schema_tables = ["governance_proposals", "psi_scores", "scores", "oracle_registry"]
    for _st in _schema_tables:
        try:
            _cols = fetch_all(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s ORDER BY ordinal_position",
                (_st.split(".")[-1],),
            )
            _col_names = [r["column_name"] for r in (_cols or [])]
            logger.error(f"[schema] {_st}: {_col_names if _col_names else 'TABLE DOES NOT EXIST'}")
        except Exception as _se:
            logger.error(f"[schema] {_st}: introspection failed: {_se}")

    # Fix governance_proposals schema drift — add missing columns from migration 069
    for _col_sql in [
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS protocol_id INTEGER",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS proposal_source VARCHAR(50)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS body TEXT",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS body_hash VARCHAR(66)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS author_address VARCHAR(42)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS author_ens VARCHAR(200)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS state VARCHAR(50)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS vote_start TIMESTAMPTZ",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS vote_end TIMESTAMPTZ",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS scores_total DECIMAL(30,8)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS scores_for DECIMAL(30,8)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS scores_against DECIMAL(30,8)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS scores_abstain DECIMAL(30,8)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS quorum DECIMAL(30,8)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS choices JSONB",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS votes JSONB",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS ipfs_hash VARCHAR(100)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS discussion_url TEXT",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS captured_at TIMESTAMPTZ DEFAULT NOW()",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS body_changed BOOLEAN DEFAULT FALSE",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS first_capture_body_hash VARCHAR(66)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS content_hash VARCHAR(66)",
        "ALTER TABLE governance_proposals ADD COLUMN IF NOT EXISTS attested_at TIMESTAMPTZ",
    ]:
        try:
            execute(_col_sql)
        except Exception as _ae:
            logger.error(f"[schema_fix] ALTER failed: {_col_sql[:80]} — {_ae}")

    # Fix psi_scores — add scored_at alias if missing (code references scored_at but table has computed_at)
    try:
        execute("ALTER TABLE psi_scores ADD COLUMN IF NOT EXISTS scored_at TIMESTAMPTZ DEFAULT NOW()")
        logger.error("[schema_fix] psi_scores.scored_at column ensured")
    except Exception as _ae:
        logger.error(f"[schema_fix] psi_scores.scored_at failed: {_ae}")

    # Unique index for governance_proposals 069-style
    try:
        execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_gov_proposals_source_id
            ON governance_proposals (proposal_source, proposal_id)
        """)
    except Exception as _ae:
        logger.error(f"[schema_fix] governance_proposals unique index failed: {_ae}")

    # Log schema AFTER fixes
    for _st in ["governance_proposals", "psi_scores"]:
        try:
            _cols = fetch_all(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s ORDER BY ordinal_position",
                (_st,),
            )
            logger.error(f"[schema_after] {_st}: {[r['column_name'] for r in (_cols or [])]}")
        except Exception:
            pass

    # Startup diagnostic fires via the independent loop after 60s
    # Schema validation — catch all drift in one shot
    try:
        from app.db_schema_validator import validate_schemas
        validate_schemas()
    except Exception as e:
        logger.error(f"[schema_validator] failed to run: {e}")

    logger.error("[startup] schema fixes complete, diagnostics will fire in 60s via independent loop")

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

    FAST_CYCLE_TIMEOUT = 30 * 60   # 30 minutes max for fast cycle
    SLOW_CYCLE_TIMEOUT = 60 * 60   # 60 minutes max for slow cycle

    # Independent diagnostic loop — fires every 10 min regardless of cycle state
    async def _diagnostic_loop():
        await asyncio.sleep(60)  # wait 1 min for first cycle to start
        while True:
            try:
                run_cycle_diagnostics()
            except Exception as _dl_e:
                logger.error(f"[diagnostic_loop] failed: {_dl_e}")
            await asyncio.sleep(600)  # 10 minutes

    try:
        if args.coin:
            async with httpx.AsyncClient(timeout=30) as client:
                result = await score_stablecoin(client, args.coin)
                print(result)
        elif args.loop:
            logger.info(f"Starting worker loop (interval: {args.interval} min)")
            asyncio.create_task(_diagnostic_loop())
            cycle_counter = 0
            while True:
                # Fast cycle — runs every interval
                try:
                    await asyncio.wait_for(run_fast_cycle(), timeout=FAST_CYCLE_TIMEOUT)
                except asyncio.TimeoutError:
                    logger.error("Fast cycle exceeded 30-minute timeout")

                # Enrichment cycle — runs every cycle now that fast cycle is <20 min
                cycle_counter += 1
                try:
                    logger.error(f"=== Starting enrichment (cycle {cycle_counter}) ===")
                    await asyncio.wait_for(run_slow_cycle_parallel(), timeout=SLOW_CYCLE_TIMEOUT)
                except asyncio.TimeoutError:
                    logger.error("Enrichment exceeded 60-minute timeout")

                # Governance crawl — every 6th cycle (~6 hours)
                if cycle_counter % 6 == 0:
                    try:
                        from app.governance import run_crawl as gov_crawl
                        logger.info("Running governance crawl...")
                        gov_crawl(since_days=7)
                    except Exception as e:
                        logger.warning(f"Governance crawl failed: {e}")

                logger.info(f"Sleeping {args.interval} minutes...")
                await asyncio.sleep(args.interval * 60)
        else:
            # Single-run mode: run both cycles (backward compat)
            await run_scoring_cycle()
    finally:
        close_pool()


if __name__ == "__main__":
    asyncio.run(main())
