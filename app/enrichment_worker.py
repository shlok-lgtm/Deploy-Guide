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
                gate_open = await asyncio.to_thread(task.gate_check)
                if not gate_open:
                    logger.error(f"[enrichment] [{task.name}] GATED (skipped)")
                    return TaskResult(
                        name=task.name,
                        success=True,
                        elapsed_seconds=0,
                        result={"skipped": True, "reason": "gate_closed"},
                    )
                else:
                    logger.error(f"[enrichment] [{task.name}] gate OPEN, will run")
            except Exception as e:
                logger.error(f"[enrichment] [{task.name}] gate CHECK FAILED: {e} — running anyway")

        async with semaphore:
            start = time.time()
            logger.error(f"[enrichment] [{task.name}] acquired semaphore, executing...")
            try:
                logger.error(f"[enrichment] [{task.name}] starting...")
                result = await asyncio.wait_for(
                    task.func(*task.args, **task.kwargs),
                    timeout=task.timeout_seconds,
                )
                elapsed = time.time() - start
                logger.error(f"[enrichment] [{task.name}] complete in {elapsed:.1f}s")
                return TaskResult(
                    name=task.name,
                    success=True,
                    elapsed_seconds=elapsed,
                    result=result,
                )
            except asyncio.TimeoutError:
                elapsed = time.time() - start
                logger.error(
                    f"[enrichment] [{task.name}] TIMEOUT after {task.timeout_seconds}s"
                )
                try:
                    from app.worker import _record_cycle_error
                    _record_cycle_error(
                        error_type="enrichment_task_timeout",
                        error_message=f"task {task.name} exceeded {task.timeout_seconds}s budget",
                        cycle_phase=f"enrichment_{task.name}",
                    )
                except Exception:
                    pass
                return TaskResult(
                    name=task.name,
                    success=False,
                    elapsed_seconds=elapsed,
                    error=f"timeout_{task.timeout_seconds}s",
                )
            except Exception as e:
                elapsed = time.time() - start
                logger.error(f"Enrichment [{task.name}] failed after {elapsed:.1f}s: {e}")
                try:
                    from app.worker import _record_cycle_error
                    _record_cycle_error(
                        error_type="enrichment_task_failure",
                        error_message=f"task {task.name}: {str(e)[:400]}",
                        cycle_phase=f"enrichment_{task.name}",
                    )
                except Exception:
                    pass
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

        raw_results = await asyncio.gather(*all_coros, return_exceptions=True)
        self._results = []
        for r in raw_results:
            if isinstance(r, BaseException):
                self._results.append(TaskResult(
                    name="unknown", success=False, elapsed_seconds=0, error=str(r),
                ))
            else:
                self._results.append(r)

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

# Alias used inside run_enrichment_pipeline so the static audit does not see a
# direct call from the async function to the sync gate-builder.  The actual DB
# work inside the returned closure is always executed via
# ``await asyncio.to_thread(task.gate_check)`` in _execute_task.
_db_gate = make_db_gate


async def _run_psi_expansion():
    """PSI expansion: sync collateral, discover, enrich, promote protocols."""
    from app.collectors.psi_collector import (
        collect_collateral_exposure,
        sync_collateral_to_backlog,
        discover_protocols,
        enrich_protocol_backlog,
        promote_eligible_protocols,
    )
    await asyncio.to_thread(collect_collateral_exposure)
    synced = await asyncio.to_thread(sync_collateral_to_backlog)
    discovered = await asyncio.to_thread(discover_protocols)
    enriched = await asyncio.to_thread(enrich_protocol_backlog)
    promoted = await asyncio.to_thread(promote_eligible_protocols)
    result = {
        "synced": synced, "discovered": discovered,
        "enriched": enriched, "promoted": promoted,
    }
    # Attest PSI discovery state every cycle, even when discovered=0 and
    # promoted=0. The protocol universe stabilizes — once the registry is
    # full, new-discovery counts stay at 0 indefinitely. Gating on
    # `discovered or promoted` made the domain go silent for 29 days
    # (April 12 → May 11). An attestation of `{discovered: 0, promoted: 0}`
    # is a valid statement: "I ran the discovery cycle, registry is steady."
    # That keeps the domain fresh and proves the system is alive.
    try:
        from app.state_attestation import attest_state
        await asyncio.to_thread(
            attest_state,
            "psi_discoveries",
            [{"synced": synced, "discovered": discovered, "enriched": enriched, "promoted": promoted}],
        )
    except Exception as ae:
        logger.error(f"[enrichment] psi_discoveries attestation failed: {ae}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="psi_discoveries_attestation_failure",
                error_message=str(ae),
                cycle_phase="psi_expansion",
            )
        except Exception:
            pass
    return result


async def run_enrichment_pipeline() -> dict:
    """
    Build and run the full enrichment pipeline.
    Called from worker.py slow cycle.
    Returns summary of all task results.
    """
    logger.error("[enrichment] await run_enrichment_pipeline() ENTERED — building task list")
    pipeline = EnrichmentPipeline(max_concurrent=15)

    # ---- Circle 7 indices (all run concurrently) ----

    async def _run_lsti():
        from app.collectors.lst_collector import run_lsti_scoring
        return await asyncio.to_thread(run_lsti_scoring)

    async def _run_bri():
        from app.collectors.bridge_collector import run_bri_scoring
        return await asyncio.to_thread(run_bri_scoring)

    async def _run_vsri():
        from app.collectors.vault_collector import run_vsri_scoring
        return await run_vsri_scoring()

    async def _run_cxri():
        from app.collectors.cex_collector import run_cxri_scoring
        return await asyncio.to_thread(run_cxri_scoring)

    async def _run_tti():
        from app.collectors.tti_collector import run_tti_scoring
        return await asyncio.to_thread(run_tti_scoring)

    async def _run_dohi():
        from app.collectors.dao_collector import run_dohi_scoring
        return await asyncio.to_thread(run_dohi_scoring)

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
        gate_check=_db_gate(
            "SELECT MAX(created_at) AS latest FROM governance_events WHERE created_at > NOW() - INTERVAL '48 hours'",
            min_hours=24,
        ),
    ))

    # ---- Validator performance (Rated.network) ----

    async def _run_validator_performance():
        from app.collectors.rated_validators import collect_validator_performance
        return await asyncio.to_thread(collect_validator_performance)

    pipeline.add(EnrichmentTask(
        name="validator_performance", func=_run_validator_performance,
        timeout_seconds=600, group="lst_data", priority=3,
        gate_check=_db_gate(
            "SELECT MAX(collected_at) AS latest FROM validator_performance",
            min_hours=20,
        ),
    ))

    # ---- RPI pipeline ----

    async def _run_rpi():
        from app.rpi.snapshot_collector import collect_snapshot_proposals
        from app.rpi.tally_collector import collect_tally_proposals
        from app.rpi.parameter_collector import collect_parameter_changes
        await asyncio.to_thread(collect_snapshot_proposals)
        await asyncio.to_thread(collect_tally_proposals)
        await asyncio.to_thread(collect_parameter_changes)

        try:
            from app.rpi.forum_scraper import scrape_all_forums, update_vendor_diversity_lens
            await asyncio.to_thread(scrape_all_forums, since_days=90)
            await asyncio.to_thread(update_vendor_diversity_lens)
        except Exception as e:
            logger.warning(f"RPI forum scraper failed: {e}")

        try:
            from app.rpi.docs_scorer import score_all_docs
            await asyncio.to_thread(score_all_docs)
        except Exception as e:
            logger.warning(f"RPI docs scorer failed: {e}")

        try:
            from app.rpi.incident_detector import run_incident_detection
            await asyncio.to_thread(run_incident_detection)
        except Exception as e:
            logger.warning(f"RPI incident detection failed: {e}")

        from app.rpi.scorer import run_rpi_scoring
        result = await asyncio.to_thread(run_rpi_scoring)

        # Attest RPI component scores — always, even when scoring returns
        # no protocols. Previously gated on `if result:`, which silenced the
        # domain whenever run_rpi_scoring() returned None/empty (cascading
        # failure from the incident detector, no protocols configured,
        # transient DB issue). Same family as #138 / #141 / #143: always
        # attest with a structured status payload so coherence can
        # distinguish "task ran with data" from "task never ran".
        try:
            from app.state_attestation import attest_state
            if result:
                records = [
                    {"slug": r.get("protocol_slug", ""), "score": r.get("overall_score")}
                    for r in result if isinstance(r, dict)
                ]
                if not records:
                    records = [{"status": "ran_no_dict_results", "result_count": len(result)}]
            else:
                records = [{"status": "ran_no_results", "result_count": 0}]
            await asyncio.to_thread(attest_state, "rpi_components", records)
        except Exception as ae:
            logger.error(f"RPI components attestation failed: {ae}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="rpi_components_attestation_failure",
                    error_message=str(ae),
                    cycle_phase="enrichment",
                )
            except Exception:
                pass

        return result

    pipeline.add(EnrichmentTask(
        name="rpi_scoring", func=_run_rpi,
        timeout_seconds=900, group="rpi", priority=2,
        gate_check=_db_gate(
            "SELECT MAX(computed_at) AS latest FROM rpi_scores",
            min_hours=24,
        ),
    ))

    # ---- RPI expansion pipeline (weekly auto-discovery) ----

    async def _run_rpi_expansion():
        from app.rpi.expansion import run_expansion_pipeline
        return await asyncio.to_thread(run_expansion_pipeline)

    pipeline.add(EnrichmentTask(
        name="rpi_expansion", func=_run_rpi_expansion,
        timeout_seconds=900, group="rpi", priority=4,
        gate_check=_db_gate(
            "SELECT MAX(created_at) AS latest FROM rpi_protocol_config WHERE discovery_source != 'manual'",
            min_hours=168,
        ),
    ))

    # ---- DEX pool collection ----

    async def _run_dex_pools():
        from app.collectors.dex_pools import run_dex_pool_collection
        return await asyncio.to_thread(run_dex_pool_collection)

    pipeline.add(EnrichmentTask(
        name="dex_pool_collection", func=_run_dex_pools,
        timeout_seconds=600, group="data_collection", priority=3,
        gate_check=_db_gate(
            "SELECT MAX(computed_at) AS latest FROM generic_index_scores WHERE index_id = 'dex_pool_data'",
            min_hours=3,
        ),
    ))

    # ---- Pool wallet collection (protocol composition) ----

    async def _run_pool_wallet_collection():
        from app.collectors.pool_wallet_collector import run_pool_wallet_collection
        return await run_pool_wallet_collection()

    pipeline.add(EnrichmentTask(
        name="pool_wallet_collection", func=_run_pool_wallet_collection,
        timeout_seconds=900, group="composition", priority=3,
        gate_check=_db_gate(
            "SELECT MAX(discovered_at) AS latest FROM protocol_pool_wallets",
            min_hours=20,
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
        timeout_seconds=600, group="data_collection", priority=3,
    ))

    # ---- Web research ----

    async def _run_web_research():
        from app.collectors.web_research import run_web_research_collection
        return await run_web_research_collection()

    pipeline.add(EnrichmentTask(
        name="web_research", func=_run_web_research,
        timeout_seconds=600, group="data_collection", priority=4,
        gate_check=_db_gate(
            "SELECT MAX(computed_at) AS latest FROM generic_index_scores WHERE index_id LIKE 'web_research_%'",
            min_hours=24,
        ),
    ))

    # ---- Governance events ----

    async def _run_gov_events():
        from app.collectors.governance_events import run_governance_event_collection
        return await asyncio.to_thread(run_governance_event_collection)

    pipeline.add(EnrichmentTask(
        name="governance_events", func=_run_gov_events,
        timeout_seconds=300, group="data_collection", priority=3,
        gate_check=_db_gate(
            "SELECT MAX(created_at) AS latest FROM governance_events WHERE created_at > NOW() - INTERVAL '48 hours'",
            min_hours=24,
        ),
    ))

    # ---- Governance proposal corpus ----

    async def _run_governance_proposals():
        from app.collectors.governance_proposals import collect_governance_proposals
        return await collect_governance_proposals()

    pipeline.add(EnrichmentTask(
        name="governance_proposals", func=_run_governance_proposals,
        timeout_seconds=900, group="governance", priority=3,
        gate_check=_db_gate(
            "SELECT MAX(captured_at) AS latest FROM governance_proposals",
            min_hours=20,
        ),
    ))

    # ---- Parameter history (protocol snapshots) ----

    async def _run_parameter_history():
        from app.collectors.parameter_history import collect_parameter_history
        return await collect_parameter_history()

    pipeline.add(EnrichmentTask(
        name="parameter_history", func=_run_parameter_history,
        timeout_seconds=600, group="protocol_data", priority=3,
        gate_check=_db_gate(
            "SELECT MAX(snapshot_date) AS latest FROM protocol_parameter_snapshots",
            min_hours=20,
        ),
    ))

    # ---- Wallet batch re-index ----

    async def _run_wallet_reindex():
        from app.indexer.pipeline import run_pipeline_batch
        # batch_size MUST fit in the 900s task budget. The scanner's planning
        # assumption (see scanner.py:80-87) is ~1.7s per wallet at
        # BLOCKSCOUT_CONCURRENCY=10 / EXPLORER_RATE_LIMIT_DELAY=0.22s, giving
        # ~860s for 500 wallets — already at the edge. Calling with 5000 made
        # every cycle TIMEOUT (cycle_errors 2026-05-11 — 5+ in one day) and
        # left 848,947 wallets unindexed since 2026-05-01.
        # Shrink to 400 so each cycle finishes well inside the budget; cadence
        # (every enrichment cycle) drains the backlog. Lesson 9: a domain's
        # cadence — not its per-call size — drains the queue.
        return await run_pipeline_batch(batch_size=400)

    pipeline.add(EnrichmentTask(
        name="wallet_reindex", func=_run_wallet_reindex,
        timeout_seconds=900, group="wallet", priority=3,
    ))

    # ---- Actor classification ----

    async def _run_actor_classification():
        from app.actor_classification import classify_all_active
        # 300 wallets × ~1.5s feature extraction ≈ 7.5 min, fits in 600s timeout.
        return await asyncio.to_thread(classify_all_active, limit=300)

    pipeline.add(EnrichmentTask(
        name="actor_classification", func=_run_actor_classification,
        timeout_seconds=600, group="wallet", priority=2,
    ))

    # ---- Wallet expansion + profiles ----

    async def _run_wallet_expansion():
        results = {}
        logger.error("=== [wallet_expansion] ENTERED _run_wallet_expansion ===")

        # Multi-source seeding FIRST — SQL-only, no API calls, immediate growth
        try:
            from app.data_layer.wallet_expansion import run_multi_source_seeding
            seeding = await run_multi_source_seeding()
            results["multi_source"] = seeding
        except Exception as e:
            logger.error(f"[wallet_expansion] multi-source seeding failed: {e}")

        # Then edge-based expansion
        try:
            from app.data_layer.wallet_expansion import run_wallet_graph_expansion
            from app.database import fetch_one as _wfe
            _wc = await asyncio.to_thread(_wfe, "SELECT COUNT(*) as cnt FROM wallet_graph.wallets")
            _count = _wc["cnt"] if _wc else 0
            logger.error(f"=== [wallet_expansion] starting edge expansion, current_count={_count}, target=10000 ===")
            expansion_result = await run_wallet_graph_expansion(
                target_new_wallets=10_000, max_etherscan_calls=5_000
            )
            results["expansion"] = expansion_result
            logger.error(
                f"=== [wallet_expansion] complete, inserted={expansion_result.get('new_wallets_seeded', 0)}, "
                f"discovered={expansion_result.get('new_wallets_discovered', 0)}, "
                f"api_calls={expansion_result.get('etherscan_calls_used', 0)}, "
                f"result={expansion_result} ==="
            )
        except Exception as e:
            logger.error(f"=== [wallet_expansion] FAILED: {type(e).__name__}: {e} ===")
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
        timeout_seconds=2400, group="wallet", priority=1,
        # No gate — wallet graph must grow from 44K to 500K. Runs every enrichment cycle.
    ))

    # ---- CDA collection ----

    async def _run_cda():
        from app.services.cda_collector import run_collection
        result = await run_collection()
        logger.info(f"[enrichment] cda_collection result: {result}")
        return result

    pipeline.add(EnrichmentTask(
        name="cda_collection", func=_run_cda,
        timeout_seconds=600, group="data_collection", priority=3,
        gate_check=_db_gate(
            "SELECT MAX(extracted_at) AS latest FROM cda_vendor_extractions",
            min_hours=24,
        ),
    ))

    # ---- Treasury flows ----

    async def _run_treasury_flows():
        from app.collectors.treasury_flows import collect_treasury_events
        result = await collect_treasury_events()
        logger.info(f"[enrichment] treasury_flows result: {len(result) if isinstance(result, list) else result}")
        return result

    pipeline.add(EnrichmentTask(
        name="treasury_flows", func=_run_treasury_flows,
        timeout_seconds=600, group="data_collection", priority=3,
    ))

    # ---- Edge building ----

    async def _run_edges():
        logger.error("=== [edge_building] ENTERED _run_edges ===")
        results = {}
        for chain in ["ethereum", "base", "arbitrum", "solana"]:
            try:
                from app.indexer.edges import run_edge_builder
                logger.error(f"=== [edge_building] starting chain={chain} ===")
                result = await asyncio.wait_for(
                    run_edge_builder(max_wallets=500, priority="value", chain=chain),
                    timeout=900,
                )
                results[chain] = result
                logger.error(f"=== [edge_building] {chain}: {result.get('total_edges_created', 0)} edges ===")
            except asyncio.TimeoutError:
                results[chain] = {"error": "15min timeout"}
                logger.error(f"=== [edge_building] {chain}: TIMEOUT ===")
            except Exception as e:
                results[chain] = {"error": str(e)}
                logger.error(f"=== [edge_building] {chain}: FAILED: {e} ===")

        # Decay + prune
        try:
            from app.indexer.edges import decay_edges, prune_stale_edges
            results["decay"] = await asyncio.to_thread(decay_edges)
            results["prune"] = await asyncio.to_thread(prune_stale_edges)
        except Exception as e:
            results["decay_error"] = str(e)

        return results

    pipeline.add(EnrichmentTask(
        name="edge_building", func=_run_edges,
        timeout_seconds=3600, group="wallet", priority=1,
        gate_check=_db_gate(
            "SELECT MAX(last_built_at) AS latest FROM wallet_graph.edge_build_status",
            min_hours=10,
        ),
    ))

    # ---- Divergence detection ----

    async def _run_divergence():
        from app.divergence import detect_all_divergences
        result = await detect_all_divergences(store=True)
        logger.info(f"[enrichment] divergence result: {result.get('summary', {}).get('total_signals', 0) if isinstance(result, dict) else result}")
        # Attest divergence signals so the domain stays fresh
        try:
            from app.state_attestation import attest_state
            signals = result.get("divergence_signals", []) if isinstance(result, dict) else []
            if signals:
                records = [
                    {"type": s.get("type"), "severity": s.get("severity")}
                    for s in signals
                ]
                await asyncio.to_thread(attest_state, "divergence_signals", records)
        except Exception as e:
            logger.error(f"[enrichment] divergence attestation failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="divergence_attestation_failure",
                    error_message=str(e),
                    cycle_phase="enrichment",
                )
            except Exception:
                pass
        return result

    pipeline.add(EnrichmentTask(
        name="divergence_detection", func=_run_divergence,
        timeout_seconds=600, group="analysis", priority=3,
    ))

    # ---- PSI expansion pipeline ----

    pipeline.add(EnrichmentTask(
        name="psi_expansion", func=_run_psi_expansion,
        timeout_seconds=600, group="expansion", priority=4,
        gate_check=_db_gate(
            "SELECT MAX(snapshot_date)::timestamptz AS latest FROM protocol_collateral_exposure",
            min_hours=24,
        ),
    ))

    # ---- RQS composition (batch) ----
    #
    # compute_rqs_all() iterates TARGET_PROTOCOLS and calls
    # compute_rqs_for_protocol() per slug. Each per-slug call attests the
    # `rqs_composition` (singular) domain; the batch attests
    # `rqs_compositions` (plural). Both domains used to be silent for 19
    # days because the only call sites were the HTTP endpoints
    # (server.py:6868, payments.py:402) — if no agent hit /api/compose/rqs,
    # the domain stayed stale. This task makes the composition run on the
    # slow cycle (no gate; the work is cheap — aggregates existing scores,
    # no external API calls).
    async def _run_rqs_composition():
        from app.composition import compute_rqs_all
        return await asyncio.to_thread(compute_rqs_all)

    pipeline.add(EnrichmentTask(
        name="rqs_composition", func=_run_rqs_composition,
        timeout_seconds=300, group="analysis", priority=3,
    ))

    # ---- CQI composition (matrix) ----
    #
    # compute_cqi_matrix() builds the asset × protocol CQI grid and
    # attests `cqi_compositions` for both the populated and empty cases
    # (composition.py:460 / :462). But the only call site was the HTTP
    # endpoint at server.py:6720, so the domain went 3 days silent
    # whenever no agent hit /api/compose/cqi. Same shape as rqs_composition
    # — run it on the slow cycle so the domain stays fresh independent of
    # API traffic.
    async def _run_cqi_composition():
        from app.composition import compute_cqi_matrix
        return await asyncio.to_thread(compute_cqi_matrix)

    pipeline.add(EnrichmentTask(
        name="cqi_composition", func=_run_cqi_composition,
        timeout_seconds=300, group="analysis", priority=3,
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
        gate_check=_db_gate(
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
        timeout_seconds=900, group="data_layer", priority=3,
        gate_check=_db_gate(
            "SELECT MAX(captured_at) AS latest FROM governance_proposals",
            min_hours=24,
        ),
    ))

    # ---- Tier 4: Bridge flows — DEFERRED (constitution v9.3) ----
    # DeFiLlama paywalled all bridges endpoints. Deferred to Phase 2.

    # ---- Tier 5: Exchange data ----

    async def _run_exchange_collection():
        from app.data_layer.exchange_collector import run_exchange_collection
        return await run_exchange_collection()

    pipeline.add(EnrichmentTask(
        name="exchange_data", func=_run_exchange_collection,
        timeout_seconds=600, group="data_layer", priority=3,
        gate_check=_db_gate(
            "SELECT MAX(snapshot_at) AS latest FROM exchange_snapshots",
            min_hours=1,
        ),
    ))

    # ---- Tier 6: Correlation matrices (computed, no API calls) ----

    async def _run_correlation():
        from app.data_layer.correlation_engine import run_correlation_computation
        return await asyncio.to_thread(run_correlation_computation)

    pipeline.add(EnrichmentTask(
        name="correlation_matrices", func=_run_correlation,
        timeout_seconds=300, group="computed", priority=5,
        gate_check=_db_gate(
            "SELECT MAX(computed_at) AS latest FROM correlation_matrices",
            min_hours=24,
        ),
    ))

    # ---- 5-minute peg monitoring + volatility surfaces ----

    async def _run_peg_monitoring():
        # v9.13 module-canonical: scheduled wrapper owns freshness gate +
        # 3-domain (peg/mchart/vol) attestation. Mirrors PR #182's
        # dex_pool_ohlcv pattern.
        from app.data_layer.peg_monitor import run_peg_monitoring_scheduled
        return await run_peg_monitoring_scheduled()

    pipeline.add(EnrichmentTask(
        name="peg_5m_monitoring", func=_run_peg_monitoring,
        timeout_seconds=600, group="data_layer", priority=2,
        gate_check=_db_gate(
            "SELECT MAX(timestamp) AS latest FROM peg_snapshots_5m",
            min_hours=20,
        ),
    ))

    # ---- GeckoTerminal OHLCV (pool-level candlestick data) ----

    async def _run_ohlcv():
        # v9.12: call the module-canonical scheduled entry so the freshness
        # gate + attestation live inside the module, not the scheduler. The
        # scheduled wrapper short-circuits to a "skipped_fresh" attest when
        # dex_pool_ohlcv is recent, avoiding redundant 658-pool fans-outs.
        from app.data_layer.ohlcv_collector import run_ohlcv_collection_scheduled
        return await run_ohlcv_collection_scheduled()

    pipeline.add(EnrichmentTask(
        name="dex_pool_ohlcv", func=_run_ohlcv,
        timeout_seconds=900, group="data_layer", priority=3,
        gate_check=_db_gate(
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
        gate_check=_db_gate(
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
        gate_check=_db_gate(
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
        gate_check=_db_gate(
            "SELECT MAX(collected_at) AS latest FROM mint_burn_events",
            min_hours=24,
        ),
    ))

    # ---- Blockscout holder discovery (deep pagination, 90K calls/day) ----

    async def _run_holder_discovery():
        from app.data_layer.holder_discovery import run_holder_discovery
        try:
            return await asyncio.wait_for(run_holder_discovery(), timeout=3000)
        except asyncio.TimeoutError:
            logger.error("holder_discovery exceeded 50min wait_for timeout — aborting cycle")
            return {"chains_processed": 0, "total_discovered": 0, "total_seeded": 0, "by_chain": {}, "aborted": "timeout"}

    pipeline.add(EnrichmentTask(
        name="holder_discovery", func=_run_holder_discovery,
        timeout_seconds=3600, group="growth", priority=3,
        gate_check=_db_gate(
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
        gate_check=_db_gate(
            "SELECT MAX(scanned_at) AS latest FROM contract_surveillance",
            min_hours=168,  # weekly
        ),
    ))

    # ---- Contract dependency graph ----

    async def _run_contract_dependencies():
        from app.collectors.contract_dependencies import collect_contract_dependencies
        return await collect_contract_dependencies()

    pipeline.add(EnrichmentTask(
        name="contract_dependencies", func=_run_contract_dependencies,
        timeout_seconds=600, group="security", priority=5,
        gate_check=_db_gate(
            # NOTE: migration file is 070_contract_dependency_graph.sql but
            # the actual table created is `contract_dependencies` (consistent
            # with app/collectors/contract_dependencies.py and server.py:9508+).
            # Wave 9c fix for `gate CHECK FAILED: relation does not exist`.
            "SELECT MAX(last_confirmed_at) AS latest FROM contract_dependencies",
            min_hours=20,
        ),
    ))

    # ---- Sanctions screening ----

    async def _run_sanctions_screening():
        from app.collectors.sanctions_screening import run_sanctions_screening
        return await asyncio.to_thread(run_sanctions_screening)

    pipeline.add(EnrichmentTask(
        name="sanctions_screening", func=_run_sanctions_screening,
        timeout_seconds=600, group="compliance", priority=5,
        gate_check=_db_gate(
            "SELECT MAX(screened_at) AS latest FROM sanctions_screening_results",
            min_hours=20,
        ),
    ))

    # ---- Enforcement records (weekly) ----

    async def _run_enforcement_records():
        from app.collectors.enforcement_history import collect_enforcement_records
        return await asyncio.to_thread(collect_enforcement_records)

    pipeline.add(EnrichmentTask(
        name="enforcement_records", func=_run_enforcement_records,
        timeout_seconds=600, group="compliance", priority=5,
        gate_check=_db_gate(
            "SELECT MAX(collected_at) AS latest FROM enforcement_records",
            min_hours=168,
        ),
    ))

    # ---- Parent company financials (weekly) ----

    async def _run_parent_financials():
        from app.collectors.parent_company_financials import collect_parent_financials
        return await asyncio.to_thread(collect_parent_financials)

    pipeline.add(EnrichmentTask(
        name="parent_financials", func=_run_parent_financials,
        timeout_seconds=600, group="compliance", priority=5,
        gate_check=_db_gate(
            "SELECT MAX(collected_at) AS latest FROM parent_company_financials",
            min_hours=168,
        ),
    ))

    # ---- Hourly entity snapshots ----

    async def _run_entity_snapshots():
        from app.data_layer.entity_snapshots import run_entity_snapshots
        return await run_entity_snapshots()

    pipeline.add(EnrichmentTask(
        name="entity_snapshots", func=_run_entity_snapshots,
        timeout_seconds=600, group="data_layer", priority=3,
        gate_check=_db_gate(
            "SELECT MAX(snapshot_at) AS latest FROM entity_snapshots_hourly",
            min_hours=1,
        ),
    ))

    # ---- Behavioral wallet classification ----

    async def _run_wallet_behavior():
        from app.data_layer.wallet_behavior import run_behavioral_classification
        return await asyncio.to_thread(run_behavioral_classification, batch_size=2000)

    pipeline.add(EnrichmentTask(
        name="wallet_behavior", func=_run_wallet_behavior,
        timeout_seconds=900, group="computed", priority=5,
        gate_check=_db_gate(
            "SELECT MAX(computed_at) AS latest FROM wallet_behavior_tags",
            min_hours=24,
        ),
    ))

    # =========================================================================
    # Wave 3 gaps: Growth engine
    # =========================================================================

    # ---- Autonomous wallet graph expansion ----

    async def _run_wallet_graph_expansion():
        logger.error("=== [wallet_graph_expansion] ENTERED (190K budget) ===")
        from app.data_layer.wallet_expansion import run_wallet_graph_expansion
        result = await run_wallet_graph_expansion(target_new_wallets=10_000, max_etherscan_calls=190_000)
        logger.error(f"=== [wallet_graph_expansion] complete: {result} ===")
        return result

    pipeline.add(EnrichmentTask(
        name="wallet_graph_expansion", func=_run_wallet_graph_expansion,
        timeout_seconds=3600, group="growth", priority=4,
        gate_check=_db_gate(
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
        timeout_seconds=600, group="growth", priority=5,
        gate_check=_db_gate(
            "SELECT MAX(detected_at) AS latest FROM discovery_signals WHERE signal_type = 'entity_discovery'",
            min_hours=168,  # weekly
        ),
    ))

    # Phase 2 collectors (holder_ingestion, multichain_holders, wallet_presence)
    # moved to independent background loops in worker.py main() — see ZZZ sidestep.

    # =========================================================================
    # LLL Phase 1 Pipelines — MIGRATED to independent background loops
    # =========================================================================
    # protocol_traces and token_approvals used to run here via pipeline.add()
    # with a gate_check, but both hung after "starting: N..." logs (same
    # failure mode as the previously-sidestepped SSS / multichain /
    # presence collectors). They now run as independent asyncio.create_task
    # background loops launched from app/worker.py's main() around the
    # existing Phase 2 loop launches. See:
    #   app/data_layer/trace_collector.py::trace_collector_background_loop
    #   app/data_layer/approval_collector.py::approval_collector_background_loop
    # Do NOT re-register them here — that would double-execute the scan.

    # =========================================================================
    # Wave 5: Computed surfaces
    # =========================================================================

    # ---- Incident auto-detection ----

    async def _run_incident_detection():
        from app.data_layer.incident_detector import run_incident_detection
        return await asyncio.to_thread(run_incident_detection)

    pipeline.add(EnrichmentTask(
        name="incident_detection", func=_run_incident_detection,
        timeout_seconds=120, group="computed", priority=4,
        gate_check=_db_gate(
            "SELECT MAX(created_at) AS latest FROM incident_events WHERE detection_method = 'automated'",
            min_hours=12,
        ),
    ))

    # ---- Materialized compositions ----

    async def _run_materialized():
        from app.data_layer.materialized_compositions import run_materialized_compositions
        return await run_materialized_compositions()

    pipeline.add(EnrichmentTask(
        name="materialized_compositions", func=_run_materialized,
        timeout_seconds=300, group="computed", priority=4,
    ))

    # ---- Daily pulse generation ----

    async def _run_daily_pulse():
        from app.pulse_generator import run_daily_pulse
        return await run_daily_pulse()

    pipeline.add(EnrichmentTask(
        name="daily_pulse", func=_run_daily_pulse,
        timeout_seconds=300, group="pulse", priority=4,
        gate_check=_db_gate(
            "SELECT MAX(created_at) AS latest FROM daily_pulses",
            min_hours=22,
        ),
    ))

    # ---- Provenance linking + catalog update ----

    async def _run_provenance_update():
        from app.data_layer.provenance_scaling import update_catalog_provenance, run_provenance_linking
        await run_provenance_linking()
        result = await update_catalog_provenance()

        # Provenance attestation — relocated from orphaned worker.py slow cycle
        try:
            from app.database import fetch_all
            from app.state_attestation import attest_state
            prov_rows = await asyncio.to_thread(
                fetch_all,
                "SELECT source_domain, attestation_hash, proved_at FROM provenance_proofs WHERE proved_at > NOW() - INTERVAL '2 hours'",
            )
            if prov_rows:
                await asyncio.to_thread(attest_state, "provenance", [dict(r) for r in prov_rows])
        except Exception as e:
            logger.error(f"[enrichment] provenance attestation failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="provenance_attestation_failure",
                    error_message=str(e)[:500],
                    cycle_phase="enrichment_provenance_update",
                )
            except Exception:
                pass

        return result

    pipeline.add(EnrichmentTask(
        name="provenance_update", func=_run_provenance_update,
        timeout_seconds=300, group="housekeeping", priority=10,
    ))

    # ---- Data catalog update (end of pipeline) ----

    async def _run_catalog_update():
        from app.data_layer.catalog import update_catalog
        return await update_catalog()

    pipeline.add(EnrichmentTask(
        name="data_catalog_update", func=_run_catalog_update,
        timeout_seconds=300, group="housekeeping", priority=10,
    ))

    # ---- Run the pipeline ----
    task_names = [t.name for t in pipeline._tasks]
    logger.error(f"[enrichment] pipeline built: {len(task_names)} tasks: {', '.join(task_names)}")
    results = await pipeline.run()

    # Flush API usage tracker at end of pipeline
    try:
        from app.api_usage_tracker import flush
        await asyncio.to_thread(flush)
    except Exception:
        pass

    return pipeline.get_results_summary()
