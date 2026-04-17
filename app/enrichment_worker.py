"""
Enrichment Worker — Parallel Data Collection
=============================================
Runs enrichment tasks concurrently without blocking the fast scoring cycle.
Uses asyncio task groups with per-task timeouts and shared rate limiting.

The fast cycle (SII scoring) runs on its own dedicated path and is NEVER
blocked by enrichment. Enrichment workers run concurrently — wallet balance
collection, DEX pool polling, Circle 7 scoring, entity discovery all run
in parallel.

Usage:
    # In the slow cycle:
    from app.enrichment_worker import run_enrichment_pipeline
    results = await run_enrichment_pipeline()
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Coroutine, Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentTask:
    """Definition of a single enrichment task."""
    name: str
    func: Callable[..., Coroutine[Any, Any, Any]]
    timeout_seconds: int = 900            # 15 min default
    gate_check: Optional[Callable[[], bool]] = None  # skip if returns False
    priority: int = 1                      # lower = higher priority
    group: str = "default"                 # for grouping in dashboard
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)


@dataclass
class TaskResult:
    """Result of executing an enrichment task."""
    name: str
    success: bool
    elapsed_seconds: float
    result: Any = None
    error: Optional[str] = None


class EnrichmentPipeline:
    """
    Manages concurrent execution of enrichment tasks.
    Groups tasks by dependency — independent tasks run in parallel,
    dependent tasks run sequentially within their group.
    """

    def __init__(self, max_concurrent: int = 6):
        self.max_concurrent = max_concurrent
        self._tasks: list[EnrichmentTask] = []
        self._results: list[TaskResult] = []

    def add(self, task: EnrichmentTask):
        """Register an enrichment task."""
        self._tasks.append(task)

    async def _execute_task(self, task: EnrichmentTask, semaphore: asyncio.Semaphore) -> TaskResult:
        """Execute a single task with timeout and error handling."""
        # Check gate
        if task.gate_check:
            try:
                if not task.gate_check():
                    logger.info(f"Enrichment [{task.name}] skipped (gate closed)")
                    return TaskResult(
                        name=task.name,
                        success=True,
                        elapsed_seconds=0,
                        result={"skipped": True, "reason": "gate_closed"},
                    )
            except Exception as e:
                logger.warning(f"Gate check failed for {task.name}: {e}")

        async with semaphore:
            start = time.time()
            try:
                logger.info(f"Enrichment [{task.name}] starting...")
                result = await asyncio.wait_for(
                    task.func(*task.args, **task.kwargs),
                    timeout=task.timeout_seconds,
                )
                elapsed = time.time() - start
                logger.info(f"Enrichment [{task.name}] complete in {elapsed:.1f}s")
                return TaskResult(
                    name=task.name,
                    success=True,
                    elapsed_seconds=elapsed,
                    result=result,
                )
            except asyncio.TimeoutError:
                elapsed = time.time() - start
                logger.warning(
                    f"Enrichment [{task.name}] timeout after {task.timeout_seconds}s"
                )
                return TaskResult(
                    name=task.name,
                    success=False,
                    elapsed_seconds=elapsed,
                    error=f"timeout_{task.timeout_seconds}s",
                )
            except Exception as e:
                elapsed = time.time() - start
                logger.error(f"Enrichment [{task.name}] failed after {elapsed:.1f}s: {e}")
                return TaskResult(
                    name=task.name,
                    success=False,
                    elapsed_seconds=elapsed,
                    error=str(e),
                )

    async def run(self) -> list[TaskResult]:
        """
        Execute all registered tasks with concurrency control.
        Tasks are sorted by priority, then run concurrently up to max_concurrent.
        """
        if not self._tasks:
            return []

        start = time.time()
        semaphore = asyncio.Semaphore(self.max_concurrent)

        # Sort by priority
        sorted_tasks = sorted(self._tasks, key=lambda t: t.priority)

        # Group by group name
        groups: dict[str, list[EnrichmentTask]] = {}
        for task in sorted_tasks:
            groups.setdefault(task.group, []).append(task)

        # Run all groups concurrently; within each group, tasks run concurrently
        all_coros = []
        for group_name, group_tasks in groups.items():
            for task in group_tasks:
                all_coros.append(self._execute_task(task, semaphore))

        self._results = await asyncio.gather(*all_coros, return_exceptions=False)

        elapsed = time.time() - start
        successes = sum(1 for r in self._results if r.success)
        failures = [r for r in self._results if not r.success]
        gated = [r for r in self._results if r.error and "gated" in str(r.error).lower()]
        logger.error(
            f"=== ENRICHMENT PIPELINE: {successes}/{len(self._results)} succeeded, "
            f"{len(failures)} failed, {len(gated)} gated, {elapsed:.0f}s ==="
        )
        if failures:
            for f in failures:
                if "gated" not in str(f.error or "").lower():
                    logger.error(f"  FAILED: {f.name} — {f.error}")

        return list(self._results)

    def get_results_summary(self) -> dict:
        """Return a summary of the last pipeline run."""
        if not self._results:
            return {"status": "not_run"}

        return {
            "total_tasks": len(self._results),
            "succeeded": sum(1 for r in self._results if r.success),
            "failed": sum(1 for r in self._results if not r.success),
            "total_elapsed_s": round(sum(r.elapsed_seconds for r in self._results), 1),
            "tasks": [
                {
                    "name": r.name,
                    "success": r.success,
                    "elapsed_s": round(r.elapsed_seconds, 1),
                    "error": r.error,
                }
                for r in self._results
            ],
        }


# =============================================================================
# Gate check helpers — reusable time-based gates
# =============================================================================

def make_db_gate(query: str, min_hours: float = 24) -> Callable[[], bool]:
    """
    Create a gate check that returns True if min_hours have passed
    since the latest timestamp returned by the query.
    """
    def check() -> bool:
        from app.database import fetch_one
        from datetime import datetime, timezone

        row = fetch_one(query)
        if not row:
            return True  # no prior record, run it

        # Try to get the first column value
        latest = None
        for v in row.values():
            if v is not None:
                latest = v
                break

        if latest is None:
            return True

        if hasattr(latest, 'tzinfo') and latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)

        if isinstance(latest, datetime):
            age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
            return age_hours >= min_hours

        return True

    return check


# =============================================================================
# Pre-built enrichment pipeline for the slow cycle
# =============================================================================

async def run_enrichment_pipeline() -> dict:
    """
    Build and run the full enrichment pipeline.
    Called from worker.py slow cycle.
    Returns summary of all task results.
    """
    pipeline = EnrichmentPipeline(max_concurrent=6)

    # ---- Circle 7 indices (all run concurrently) ----

    async def _run_lsti():
        from app.collectors.lst_collector import run_lsti_scoring
        return run_lsti_scoring()

    async def _run_bri():
        from app.collectors.bridge_collector import run_bri_scoring
        return run_bri_scoring()

    async def _run_vsri():
        from app.collectors.vault_collector import run_vsri_scoring
        return run_vsri_scoring()

    async def _run_cxri():
        from app.collectors.cex_collector import run_cxri_scoring
        return run_cxri_scoring()

    async def _run_tti():
        from app.collectors.tti_collector import run_tti_scoring
        return run_tti_scoring()

    async def _run_dohi():
        from app.collectors.dao_collector import run_dohi_scoring
        return run_dohi_scoring()

    pipeline.add(EnrichmentTask(
        name="lsti_scoring", func=_run_lsti,
        timeout_seconds=600, group="circle7", priority=2,
    ))
    pipeline.add(EnrichmentTask(
        name="bri_scoring", func=_run_bri,
        timeout_seconds=600, group="circle7", priority=2,
    ))
    pipeline.add(EnrichmentTask(
        name="vsri_scoring", func=_run_vsri,
        timeout_seconds=600, group="circle7", priority=2,
    ))
    pipeline.add(EnrichmentTask(
        name="cxri_scoring", func=_run_cxri,
        timeout_seconds=600, group="circle7", priority=2,
    ))
    pipeline.add(EnrichmentTask(
        name="tti_scoring", func=_run_tti,
        timeout_seconds=600, group="circle7", priority=2,
    ))
    pipeline.add(EnrichmentTask(
        name="dohi_scoring", func=_run_dohi,
        timeout_seconds=600, group="circle7", priority=2,
        gate_check=make_db_gate(
            "SELECT MAX(created_at) AS latest FROM governance_events WHERE created_at > NOW() - INTERVAL '48 hours'",
            min_hours=24,
        ),
    ))

    # ---- RPI pipeline ----

    async def _run_rpi():
        from app.rpi.snapshot_collector import collect_snapshot_proposals
        from app.rpi.tally_collector import collect_tally_proposals
        from app.rpi.parameter_collector import collect_parameter_changes
        collect_snapshot_proposals()
        collect_tally_proposals()
        collect_parameter_changes()

        try:
            from app.rpi.forum_scraper import scrape_all_forums, update_vendor_diversity_lens
            scrape_all_forums(since_days=90)
            update_vendor_diversity_lens()
        except Exception as e:
            logger.warning(f"RPI forum scraper failed: {e}")

        try:
            from app.rpi.docs_scorer import score_all_docs
            score_all_docs()
        except Exception as e:
            logger.warning(f"RPI docs scorer failed: {e}")

        try:
            from app.rpi.incident_detector import run_incident_detection
            run_incident_detection()
        except Exception as e:
            logger.warning(f"RPI incident detection failed: {e}")

        from app.rpi.scorer import run_rpi_scoring
        return run_rpi_scoring()

    pipeline.add(EnrichmentTask(
        name="rpi_scoring", func=_run_rpi,
        timeout_seconds=900, group="rpi", priority=2,
        gate_check=make_db_gate(
            "SELECT MAX(computed_at) AS latest FROM rpi_scores",
            min_hours=24,
        ),
    ))

    # ---- DEX pool collection ----

    async def _run_dex_pools():
        from app.collectors.dex_pools import run_dex_pool_collection
        return run_dex_pool_collection()

    pipeline.add(EnrichmentTask(
        name="dex_pool_collection", func=_run_dex_pools,
        timeout_seconds=600, group="data_collection", priority=3,
        gate_check=make_db_gate(
            "SELECT MAX(computed_at) AS latest FROM generic_index_scores WHERE index_id = 'dex_pool_data'",
            min_hours=3,
        ),
    ))

    # ---- On-chain governance reads ----

    async def _run_gov_reads():
        import httpx
        async with httpx.AsyncClient(timeout=30) as client:
            from app.collectors.smart_contract import collect_governance_reads
            return await collect_governance_reads(client)

    pipeline.add(EnrichmentTask(
        name="onchain_governance_reads", func=_run_gov_reads,
        timeout_seconds=300, group="data_collection", priority=3,
    ))

    # ---- Web research ----

    async def _run_web_research():
        from app.collectors.web_research import run_web_research_collection
        return await run_web_research_collection()

    pipeline.add(EnrichmentTask(
        name="web_research", func=_run_web_research,
        timeout_seconds=600, group="data_collection", priority=4,
        gate_check=make_db_gate(
            "SELECT MAX(computed_at) AS latest FROM generic_index_scores WHERE index_id LIKE 'web_research_%'",
            min_hours=24,
        ),
    ))

    # ---- Governance events ----

    async def _run_gov_events():
        from app.collectors.governance_events import run_governance_event_collection
        return run_governance_event_collection()

    pipeline.add(EnrichmentTask(
        name="governance_events", func=_run_gov_events,
        timeout_seconds=300, group="data_collection", priority=3,
        gate_check=make_db_gate(
            "SELECT MAX(created_at) AS latest FROM governance_events WHERE created_at > NOW() - INTERVAL '48 hours'",
            min_hours=24,
        ),
    ))

    # ---- Wallet batch re-index ----

    async def _run_wallet_reindex():
        from app.indexer.pipeline import run_pipeline_batch
        return await run_pipeline_batch(batch_size=5000)

    pipeline.add(EnrichmentTask(
        name="wallet_reindex", func=_run_wallet_reindex,
        timeout_seconds=900, group="wallet", priority=3,
    ))

    # ---- Wallet expansion + profiles ----

    async def _run_wallet_expansion():
        results = {}
        try:
            from app.data_layer.wallet_expansion import run_wallet_graph_expansion
            expansion_result = await run_wallet_graph_expansion(
                target_new_wallets=10_000, max_etherscan_calls=5_000
            )
            results["expansion"] = expansion_result
            logger.error(
                f"=== ENRICHMENT WALLET EXPANSION: "
                f"{expansion_result.get('new_wallets_seeded', 0)} new wallets ==="
            )
        except Exception as e:
            logger.error(f"Wallet expansion failed: {e}")
            results["expansion_error"] = str(e)

        try:
            from app.indexer.profiles import rebuild_all_profiles
            profile_result = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(None, rebuild_all_profiles, 2000),
                timeout=1800,
            )
            results["profiles"] = profile_result
        except asyncio.TimeoutError:
            results["profile_error"] = "30min timeout"
        except Exception as e:
            results["profile_error"] = str(e)

        return results

    pipeline.add(EnrichmentTask(
        name="wallet_expansion", func=_run_wallet_expansion,
        timeout_seconds=2400, group="wallet", priority=4,
        gate_check=make_db_gate(
            "SELECT MAX(created_at) AS latest FROM wallet_graph.wallets WHERE created_at > NOW() - INTERVAL '48 hours'",
            min_hours=24,
        ),
    ))

    # ---- CDA collection ----

    async def _run_cda():
        from app.services.cda_collector import run_collection
        return await run_collection()

    pipeline.add(EnrichmentTask(
        name="cda_collection", func=_run_cda,
        timeout_seconds=600, group="data_collection", priority=3,
        gate_check=make_db_gate(
            "SELECT MAX(extracted_at) AS latest FROM cda_vendor_extractions",
            min_hours=24,
        ),
    ))

    # ---- Treasury flows ----

    async def _run_treasury_flows():
        from app.collectors.treasury_flows import collect_treasury_events
        return await collect_treasury_events()

    pipeline.add(EnrichmentTask(
        name="treasury_flows", func=_run_treasury_flows,
        timeout_seconds=300, group="data_collection", priority=3,
    ))

    # ---- Edge building ----

    async def _run_edges():
        results = {}
        for chain in ["ethereum", "base", "arbitrum", "solana"]:
            try:
                from app.indexer.edges import run_edge_builder
                result = await asyncio.wait_for(
                    run_edge_builder(max_wallets=100, priority="value", chain=chain),
                    timeout=900,
                )
                results[chain] = result
            except asyncio.TimeoutError:
                results[chain] = {"error": "15min timeout"}
            except Exception as e:
                results[chain] = {"error": str(e)}

        # Decay + prune
        try:
            from app.indexer.edges import decay_edges, prune_stale_edges
            results["decay"] = decay_edges()
            results["prune"] = prune_stale_edges()
        except Exception as e:
            results["decay_error"] = str(e)

        return results

    pipeline.add(EnrichmentTask(
        name="edge_building", func=_run_edges,
        timeout_seconds=3600, group="wallet", priority=4,
        gate_check=make_db_gate(
            "SELECT MAX(last_built_at) AS latest FROM wallet_graph.edge_build_status",
            min_hours=10,
        ),
    ))

    # ---- Divergence detection ----

    async def _run_divergence():
        from app.divergence import detect_all_divergences
        return detect_all_divergences(store=True)

    pipeline.add(EnrichmentTask(
        name="divergence_detection", func=_run_divergence,
        timeout_seconds=300, group="analysis", priority=3,
    ))

    # ---- PSI expansion pipeline ----

    async def _run_psi_expansion():
        from app.collectors.psi_collector import (
            collect_collateral_exposure,
            sync_collateral_to_backlog,
            discover_protocols,
            enrich_protocol_backlog,
            promote_eligible_protocols,
        )
        collect_collateral_exposure()
        synced = sync_collateral_to_backlog()
        discovered = discover_protocols()
        enriched = enrich_protocol_backlog()
        promoted = promote_eligible_protocols()
        return {
            "synced": synced, "discovered": discovered,
            "enriched": enriched, "promoted": promoted,
        }

    pipeline.add(EnrichmentTask(
        name="psi_expansion", func=_run_psi_expansion,
        timeout_seconds=600, group="expansion", priority=4,
        gate_check=make_db_gate(
            "SELECT MAX(snapshot_date)::timestamptz AS latest FROM protocol_collateral_exposure",
            min_hours=24,
        ),
    ))

    # =========================================================================
    # Universal Data Layer collectors
    # =========================================================================

    # ---- Tier 1: Liquidity depth ----

    async def _run_liquidity_collection():
        from app.data_layer.liquidity_collector import run_liquidity_collection
        return await run_liquidity_collection()

    pipeline.add(EnrichmentTask(
        name="liquidity_depth", func=_run_liquidity_collection,
        timeout_seconds=900, group="data_layer", priority=1,
        # No gate — runs every slow cycle (~3h) for near-continuous liquidity data
    ))

    # ---- Tier 2: Yield data ----

    async def _run_yield_collection():
        from app.data_layer.yield_collector import run_yield_collection
        return await run_yield_collection()

    pipeline.add(EnrichmentTask(
        name="yield_data", func=_run_yield_collection,
        timeout_seconds=600, group="data_layer", priority=2,
        gate_check=make_db_gate(
            "SELECT MAX(snapshot_at) AS latest FROM yield_snapshots",
            min_hours=24,
        ),
    ))

    # ---- Tier 3: Governance activity ----

    async def _run_governance_collection():
        from app.data_layer.governance_collector import run_governance_collection
        return await run_governance_collection()

    pipeline.add(EnrichmentTask(
        name="governance_activity", func=_run_governance_collection,
        timeout_seconds=600, group="data_layer", priority=3,
        gate_check=make_db_gate(
            "SELECT MAX(captured_at) AS latest FROM governance_proposals",
            min_hours=24,
        ),
    ))

    # ---- Tier 4: Bridge flows ----

    async def _run_bridge_flows():
        # Use inline fetch — data_layer collector uses bridges.llama.fi which is paywalled
        import httpx as _bfx
        from app.database import get_cursor as _bf_gc
        import math
        def _sn(v):
            if v is None: return None
            try:
                f = float(v)
                return None if (math.isnan(f) or math.isinf(f)) else f
            except: return None

        async with _bfx.AsyncClient(timeout=30) as _bc:
            _r = await _bc.get("https://api.llama.fi/bridges", params={"includeChains": "true"})
            if _r.status_code == 402:
                _r = await _bc.get("https://api.llama.fi/v2/bridges")
            data = _r.json() if _r.status_code == 200 else {}
            _brs = data.get("bridges", data if isinstance(data, list) else [])

        ok, err = 0, 0
        for _b in sorted(_brs, key=lambda x: x.get("lastDayVolume", 0) or 0, reverse=True)[:20]:
            _bid = _b.get("id")
            if _bid is None: continue
            try:
                with _bf_gc() as _c:
                    _c.execute("""INSERT INTO bridge_flows
                        (bridge_id, bridge_name, source_chain, dest_chain, volume_usd, period, snapshot_at)
                        VALUES(%s, %s, 'all', 'all', %s, '24h', NOW())
                        ON CONFLICT DO NOTHING""",
                        (str(_bid), _b.get("displayName") or _b.get("name", ""), _sn(_b.get("lastDayVolume"))))
                ok += 1
            except Exception:
                err += 1
        logger.error(f"=== ENRICHMENT BRIDGES: {ok} ok, {err} err ===")
        return {"ok": ok, "err": err}

    pipeline.add(EnrichmentTask(
        name="bridge_flows", func=_run_bridge_flows,
        timeout_seconds=600, group="data_layer", priority=3,
        gate_check=make_db_gate(
            "SELECT MAX(snapshot_at) AS latest FROM bridge_flows",
            min_hours=24,
        ),
    ))

    # ---- Tier 5: Exchange data ----

    async def _run_exchange_collection():
        from app.data_layer.exchange_collector import run_exchange_collection
        return await run_exchange_collection()

    pipeline.add(EnrichmentTask(
        name="exchange_data", func=_run_exchange_collection,
        timeout_seconds=600, group="data_layer", priority=3,
        gate_check=make_db_gate(
            "SELECT MAX(snapshot_at) AS latest FROM exchange_snapshots",
            min_hours=1,
        ),
    ))

    # ---- Tier 6: Correlation matrices (computed, no API calls) ----

    async def _run_correlation():
        from app.data_layer.correlation_engine import run_correlation_computation
        return run_correlation_computation()

    pipeline.add(EnrichmentTask(
        name="correlation_matrices", func=_run_correlation,
        timeout_seconds=300, group="computed", priority=5,
        gate_check=make_db_gate(
            "SELECT MAX(computed_at) AS latest FROM correlation_matrices",
            min_hours=24,
        ),
    ))

    # ---- 5-minute peg monitoring + volatility surfaces ----

    async def _run_peg_monitoring():
        from app.data_layer.peg_monitor import run_peg_monitoring
        return await run_peg_monitoring()

    pipeline.add(EnrichmentTask(
        name="peg_5m_monitoring", func=_run_peg_monitoring,
        timeout_seconds=600, group="data_layer", priority=2,
        gate_check=make_db_gate(
            "SELECT MAX(timestamp) AS latest FROM peg_snapshots_5m",
            min_hours=20,
        ),
    ))

    # ---- GeckoTerminal OHLCV (pool-level candlestick data) ----

    async def _run_ohlcv():
        from app.data_layer.ohlcv_collector import run_ohlcv_collection
        return await run_ohlcv_collection()

    pipeline.add(EnrichmentTask(
        name="dex_pool_ohlcv", func=_run_ohlcv,
        timeout_seconds=900, group="data_layer", priority=3,
        gate_check=make_db_gate(
            "SELECT MAX(timestamp) AS latest FROM dex_pool_ohlcv",
            min_hours=3,
        ),
    ))

    # ---- Market chart historical backfill + volatility surfaces ----

    async def _run_market_chart_backfill():
        from app.data_layer.market_chart_backfill import run_market_chart_backfill
        return await run_market_chart_backfill(backfill_days=90)

    pipeline.add(EnrichmentTask(
        name="market_chart_backfill", func=_run_market_chart_backfill,
        timeout_seconds=600, group="data_layer", priority=3,
        gate_check=make_db_gate(
            "SELECT MAX(timestamp) AS latest FROM market_chart_history",
            min_hours=20,
        ),
    ))

    # ---- Extended 5-min pulls for Circle 7 entities ----

    async def _run_extended_5min():
        from app.data_layer.markets_collector import run_extended_5min_pulls
        return await run_extended_5min_pulls()

    pipeline.add(EnrichmentTask(
        name="circle7_5min_pulls", func=_run_extended_5min,
        timeout_seconds=600, group="data_layer", priority=3,
        gate_check=make_db_gate(
            "SELECT MAX(timestamp) AS latest FROM market_chart_history WHERE granularity = '5min' AND coin_id NOT IN (SELECT coingecko_id FROM stablecoins WHERE scoring_enabled = TRUE)",
            min_hours=20,
        ),
    ))

    # =========================================================================
    # Wave 4: Depth + precision collectors
    # =========================================================================

    # ---- Mint/burn event capture ----

    async def _run_mint_burn():
        from app.data_layer.mint_burn_collector import run_mint_burn_collection
        return await run_mint_burn_collection()

    pipeline.add(EnrichmentTask(
        name="mint_burn_events", func=_run_mint_burn,
        timeout_seconds=600, group="data_layer", priority=3,
        gate_check=make_db_gate(
            "SELECT MAX(collected_at) AS latest FROM mint_burn_events",
            min_hours=24,
        ),
    ))

    # ---- Blockscout holder discovery (deep pagination, 90K calls/day) ----

    async def _run_holder_discovery():
        from app.data_layer.holder_discovery import run_holder_discovery
        return await run_holder_discovery()

    pipeline.add(EnrichmentTask(
        name="holder_discovery", func=_run_holder_discovery,
        timeout_seconds=3600, group="growth", priority=3,
        gate_check=make_db_gate(
            "SELECT MAX(created_at) AS latest FROM wallet_graph.wallets WHERE source = 'holder_discovery'",
            min_hours=24,
        ),
    ))

    # ---- Contract surveillance (weekly) ----

    async def _run_contract_surveillance():
        from app.data_layer.contract_surveillance import run_contract_surveillance
        return await run_contract_surveillance()

    pipeline.add(EnrichmentTask(
        name="contract_surveillance", func=_run_contract_surveillance,
        timeout_seconds=600, group="data_layer", priority=5,
        gate_check=make_db_gate(
            "SELECT MAX(scanned_at) AS latest FROM contract_surveillance",
            min_hours=168,  # weekly
        ),
    ))

    # ---- Hourly entity snapshots ----

    async def _run_entity_snapshots():
        from app.data_layer.entity_snapshots import run_entity_snapshots
        return await run_entity_snapshots()

    pipeline.add(EnrichmentTask(
        name="entity_snapshots", func=_run_entity_snapshots,
        timeout_seconds=600, group="data_layer", priority=3,
        gate_check=make_db_gate(
            "SELECT MAX(snapshot_at) AS latest FROM entity_snapshots_hourly",
            min_hours=1,
        ),
    ))

    # ---- Behavioral wallet classification ----

    async def _run_wallet_behavior():
        from app.data_layer.wallet_behavior import run_behavioral_classification
        return run_behavioral_classification(batch_size=2000)

    pipeline.add(EnrichmentTask(
        name="wallet_behavior", func=_run_wallet_behavior,
        timeout_seconds=900, group="computed", priority=5,
        gate_check=make_db_gate(
            "SELECT MAX(computed_at) AS latest FROM wallet_behavior_tags",
            min_hours=24,
        ),
    ))

    # =========================================================================
    # Wave 3 gaps: Growth engine
    # =========================================================================

    # ---- Autonomous wallet graph expansion ----

    async def _run_wallet_graph_expansion():
        from app.data_layer.wallet_expansion import run_wallet_graph_expansion
        return await run_wallet_graph_expansion(target_new_wallets=10_000, max_etherscan_calls=190_000)

    pipeline.add(EnrichmentTask(
        name="wallet_graph_expansion", func=_run_wallet_graph_expansion,
        timeout_seconds=3600, group="growth", priority=4,
        gate_check=make_db_gate(
            "SELECT MAX(created_at) AS latest FROM wallet_graph.wallets WHERE source = 'graph_expansion'",
            min_hours=24,
        ),
    ))

    # ---- Entity auto-discovery (weekly) ----

    async def _run_entity_discovery():
        from app.data_layer.entity_discovery import run_entity_discovery
        return await run_entity_discovery()

    pipeline.add(EnrichmentTask(
        name="entity_discovery", func=_run_entity_discovery,
        timeout_seconds=300, group="growth", priority=5,
        gate_check=make_db_gate(
            "SELECT MAX(detected_at) AS latest FROM discovery_signals WHERE signal_type = 'entity_discovery'",
            min_hours=168,  # weekly
        ),
    ))

    # =========================================================================
    # Wave 5: Computed surfaces
    # =========================================================================

    # ---- Incident auto-detection ----

    async def _run_incident_detection():
        from app.data_layer.incident_detector import run_incident_detection
        return run_incident_detection()

    pipeline.add(EnrichmentTask(
        name="incident_detection", func=_run_incident_detection,
        timeout_seconds=120, group="computed", priority=4,
        gate_check=make_db_gate(
            "SELECT MAX(created_at) AS latest FROM incident_events WHERE detection_method = 'automated'",
            min_hours=12,
        ),
    ))

    # ---- Materialized compositions ----

    async def _run_materialized():
        from app.data_layer.materialized_compositions import run_materialized_compositions
        return run_materialized_compositions()

    pipeline.add(EnrichmentTask(
        name="materialized_compositions", func=_run_materialized,
        timeout_seconds=120, group="computed", priority=4,
    ))

    # ---- Provenance linking + catalog update ----

    async def _run_provenance_update():
        from app.data_layer.provenance_scaling import update_catalog_provenance, run_provenance_linking
        run_provenance_linking()
        return update_catalog_provenance()

    pipeline.add(EnrichmentTask(
        name="provenance_update", func=_run_provenance_update,
        timeout_seconds=60, group="housekeeping", priority=10,
    ))

    # ---- Data catalog update (end of pipeline) ----

    async def _run_catalog_update():
        from app.data_layer.catalog import update_catalog
        return update_catalog()

    pipeline.add(EnrichmentTask(
        name="data_catalog_update", func=_run_catalog_update,
        timeout_seconds=60, group="housekeeping", priority=10,
    ))

    # ---- Run the pipeline ----
    results = await pipeline.run()

    # Flush API usage tracker at end of pipeline
    try:
        from app.api_usage_tracker import flush
        flush()
    except Exception:
        pass

    return pipeline.get_results_summary()
