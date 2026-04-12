"""
Collector Registry
==================
Register SII component collectors declaratively. Each collector is
auto-discovered, auto-instrumented (timing, error tracking, stats),
and run in parallel during scoring.

To add a new collector:
  1. Write the collector function in app/collectors/
  2. Add one line to _make_async_collectors() or _make_sync_collectors() below
  3. Done. No other files need editing.
"""

import asyncio
import time
import logging
from dataclasses import dataclass, field
from typing import Callable, Awaitable, Optional

logger = logging.getLogger(__name__)


@dataclass
class CollectorResult:
    name: str
    ok: bool
    components: list
    elapsed_ms: int
    error: Optional[str] = None
    timed_out: bool = False


@dataclass
class CycleStats:
    """Aggregated stats across all coins for one scoring cycle."""
    collectors: dict = field(default_factory=dict)
    # collector_name -> {ok, timeout, error, total_ms, components}

    def record(self, result: CollectorResult):
        s = self.collectors.setdefault(result.name, {
            "ok": 0, "timeout": 0, "error": 0, "total_ms": 0, "components": 0
        })
        if result.timed_out:
            s["timeout"] += 1
        elif not result.ok:
            s["error"] += 1
        else:
            s["ok"] += 1
            s["total_ms"] += result.elapsed_ms
            s["components"] += len(result.components)

    def log_summary(self):
        if not self.collectors:
            return
        logger.info("=== Collector Performance ===")
        for name, s in sorted(self.collectors.items()):
            total = s["ok"] + s["timeout"] + s["error"]
            avg_ms = s["total_ms"] // max(s["ok"], 1)
            logger.info(
                f"  {name}: {s['ok']}/{total} ok, "
                f"{s['timeout']} timeout, {s['error']} error, "
                f"avg {avg_ms}ms, {s['components']} components"
            )

    def store(self):
        """Persist to collector_cycle_stats table (best-effort)."""
        try:
            from app.database import execute
            for name, s in self.collectors.items():
                execute(
                    """INSERT INTO collector_cycle_stats
                       (collector_name, coins_ok, coins_timeout, coins_error,
                        avg_latency_ms, total_components)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (name, s["ok"], s["timeout"], s["error"],
                     s["total_ms"] // max(s["ok"], 1), s["components"])
                )
        except Exception as e:
            logger.debug(f"Collector stats storage failed: {e}")


# =========================================================================
# Collector definitions
# =========================================================================
# Each entry: (name, factory_function)
# factory_function(client, coingecko_id, stablecoin_id) -> coroutine
# For collectors that don't need all args, wrap them.

def _make_async_collectors():
    """Build the async collector list. Imports happen here to avoid
    circular imports at module level."""
    from app.collectors.coingecko import (
        collect_peg_components, collect_liquidity_components,
        collect_market_activity_components,
    )
    from app.collectors.defillama import collect_defillama_components
    from app.collectors.curve import collect_curve_components
    from app.collectors.etherscan import collect_holder_distribution
    from app.collectors.flows import collect_flows_components
    from app.collectors.smart_contract import collect_smart_contract_components
    from app.collectors.solana import collect_solana_components
    from app.collectors.actor_metrics import collect_actor_metrics

    return [
        # (name, callable(client, cg_id, stablecoin_id) -> coroutine)
        ("peg",            lambda c, cg, sid: collect_peg_components(c, cg, sid)),
        ("liquidity",      lambda c, cg, sid: collect_liquidity_components(c, cg, sid)),
        ("market",         lambda c, cg, sid: collect_market_activity_components(c, cg, sid)),
        ("defillama",      lambda c, cg, sid: collect_defillama_components(c, cg, sid)),
        ("curve",          lambda c, cg, sid: collect_curve_components(c, sid)),
        ("etherscan",      lambda c, cg, sid: collect_holder_distribution(c, sid)),
        ("flows",          lambda c, cg, sid: collect_flows_components(c, sid)),
        ("smart_contract", lambda c, cg, sid: collect_smart_contract_components(c, sid)),
        ("solana",         lambda c, cg, sid: collect_solana_components(c, sid)),
        ("actor_metrics",  lambda c, cg, sid: collect_actor_metrics(c, sid)),
        # To add a new collector, add one line here:
        # ("my_collector", lambda c, cg, sid: my_collect_fn(c, sid)),
    ]


def _make_sync_collectors():
    """Build the sync (offline) collector list."""
    from app.collectors.offline import (
        collect_transparency_components, collect_regulatory_components,
        collect_governance_components, collect_reserve_components,
        collect_network_components,
    )
    from app.collectors.derived import collect_derived_components

    return [
        # (name, callable(stablecoin_id) -> list[dict])
        ("transparency",  collect_transparency_components),
        ("regulatory",    collect_regulatory_components),
        ("governance",    collect_governance_components),
        ("reserves",      collect_reserve_components),
        ("network",       collect_network_components),
        ("derived",       collect_derived_components),
        # To add a new offline collector, add one line here:
        # ("my_offline", my_offline_fn),
    ]


# Singleton — built on first use
_async_collectors = None
_sync_collectors = None


def get_async_collectors():
    global _async_collectors
    if _async_collectors is None:
        _async_collectors = _make_async_collectors()
    return _async_collectors


def get_sync_collectors():
    global _sync_collectors
    if _sync_collectors is None:
        _sync_collectors = _make_sync_collectors()
    return _sync_collectors


# =========================================================================
# Runner
# =========================================================================

COLLECTOR_TIMEOUT = 20.0  # seconds per collector per coin


async def run_all_collectors(
    client, coingecko_id: str, stablecoin_id: str,
    cycle_stats: Optional[CycleStats] = None
) -> list[dict]:
    """
    Run all registered collectors for one stablecoin.
    Returns flat list of component dicts.
    Auto-instruments every collector with timing and error tracking.
    """
    all_components = []

    # Async collectors — parallel
    async def _run_one(name, factory):
        start = time.time()
        try:
            coro = factory(client, coingecko_id, stablecoin_id)
            result = await asyncio.wait_for(coro, timeout=COLLECTOR_TIMEOUT)
            elapsed = int((time.time() - start) * 1000)
            r = CollectorResult(name=name, ok=True, components=result or [], elapsed_ms=elapsed)
        except asyncio.TimeoutError:
            r = CollectorResult(name=name, ok=False, components=[], elapsed_ms=int(COLLECTOR_TIMEOUT * 1000), timed_out=True)
            logger.warning(f"{name} timed out for {stablecoin_id}")
        except Exception as e:
            elapsed = int((time.time() - start) * 1000)
            r = CollectorResult(name=name, ok=False, components=[], elapsed_ms=elapsed, error=str(e))
            logger.error(f"{name} error for {stablecoin_id}: {e}")

        if cycle_stats:
            cycle_stats.record(r)
        return r.components

    tasks = [_run_one(name, factory) for name, factory in get_async_collectors()]
    results = await asyncio.gather(*tasks)
    for result in results:
        if result:
            all_components.extend(result)

    # Sync collectors — sequential
    for name, fn in get_sync_collectors():
        start = time.time()
        try:
            components = fn(stablecoin_id)
            elapsed = int((time.time() - start) * 1000)
            if cycle_stats:
                cycle_stats.record(CollectorResult(name=name, ok=True, components=components or [], elapsed_ms=elapsed))
            if components:
                all_components.extend(components)
        except Exception as e:
            elapsed = int((time.time() - start) * 1000)
            if cycle_stats:
                cycle_stats.record(CollectorResult(name=name, ok=False, components=[], elapsed_ms=elapsed, error=str(e)))
            logger.error(f"Offline collector {name} error for {stablecoin_id}: {e}")

    # CDA overlay (special — reads from DB, not an API)
    try:
        from app.services.cda_scores import get_cda_components
        cda = get_cda_components(stablecoin_id)
        if cda:
            all_components.extend(cda)
    except Exception as e:
        logger.debug(f"CDA collector skipped for {stablecoin_id}: {e}")

    # Tag all components
    for comp in all_components:
        comp["stablecoin_id"] = stablecoin_id

    return all_components
