"""
Collector Registry
===================
Declarative registry for SII data collectors.
Auto-instrumentation: timing, error tracking, CycleStats persistence.

To add a new async collector:
    Add one entry to _make_async_collectors().

To add a new sync collector:
    Add one entry to _make_sync_collectors().
"""

import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone

from app.database import execute, get_cursor

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Provenance source auto-registration
# ---------------------------------------------------------------------------

def _infer_source_type(url: str) -> str:
    """Infer provenance source type from URL pattern."""
    if "api.github.com" in url or "raw.githubusercontent.com" in url:
        return "static_github"
    elif "etherscan.io" in url or "basescan.org" in url or "arbiscan.io" in url:
        return "etherscan_api"
    elif "coingecko.com" in url or "llama.fi" in url:
        return "live_api"
    elif "docs." in url or "documentation" in url:
        return "html_docs"
    else:
        return "protocol_api"


def register_provenance_source(
    source_id: str,
    entity: str,
    component: str,
    url: str,
    schedule: str = "hourly",
    source_type: str = None,
):
    """
    Register (or update) a provenance source in the DB.
    Called when a collector registers or when syncing from PROVENANCE_SOURCES.

    Uses ON CONFLICT DO UPDATE for the URL so endpoint changes propagate
    to the prover automatically.

    Non-critical — failures are logged but never block collector registration.
    """
    if source_type is None:
        source_type = _infer_source_type(url)
    try:
        execute(
            """INSERT INTO provenance_sources (id, entity, component, source_type, url, schedule)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (id) DO UPDATE SET
                   url = EXCLUDED.url,
                   source_type = EXCLUDED.source_type,
                   updated_at = now()""",
            (source_id, entity, component, source_type, url, schedule),
        )
    except Exception as e:
        logger.warning(f"Failed to register provenance source {source_id}: {e}")


def sync_provenance_sources():
    """
    Sync all known provenance sources from PROVENANCE_SOURCES dict into the DB.
    Called at the start of each scoring cycle so new collectors automatically
    get provenance coverage on the next prover run.
    """
    try:
        from app.data_layer.provenance_scaling import PROVENANCE_SOURCES
    except ImportError:
        logger.debug("provenance_scaling not available — skipping provenance sync")
        return 0

    synced = 0
    for source_id, src in PROVENANCE_SOURCES.items():
        provider = src.get("provider", "unknown")
        endpoint = src.get("endpoint", "")
        # Build a representative URL from provider + endpoint
        url = _build_url(provider, endpoint)
        component = source_id.replace(f"{provider}_", "", 1) if source_id.startswith(f"{provider}_") else source_id
        source_type = _infer_source_type(url)

        try:
            execute(
                """INSERT INTO provenance_sources (id, entity, component, source_type, url, schedule)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO UPDATE SET
                       url = EXCLUDED.url,
                       source_type = EXCLUDED.source_type,
                       updated_at = now()""",
                (source_id, provider, component, source_type, url, "hourly"),
            )
            synced += 1
        except Exception as e:
            logger.warning(f"Failed to sync provenance source {source_id}: {e}")

    if synced:
        logger.info(f"Synced {synced} provenance sources to DB")
    return synced


def _build_url(provider: str, endpoint: str) -> str:
    """Map provider + endpoint path to a full URL."""
    base_urls = {
        "coingecko": "https://pro-api.coingecko.com/api/v3",
        "defillama": "https://api.llama.fi",
        "etherscan": "https://api.etherscan.io/v2/api",
        "snapshot": "https://hub.snapshot.org",
        "blockscout": "https://eth.blockscout.com/api",
        "tally": "https://api.tally.xyz",
        "issuer_website": "dynamic:cda_source_urls",
    }
    base = base_urls.get(provider, "")
    if not base or base.startswith("dynamic:"):
        return base or endpoint
    return f"{base}{endpoint}"


# ---------------------------------------------------------------------------
# CycleStats — per-cycle performance tracking
# ---------------------------------------------------------------------------

class CycleStats:
    """Accumulates per-collector stats across all coins in a scoring cycle."""

    def __init__(self):
        self._data = defaultdict(lambda: {
            "ok": 0,
            "timeout": 0,
            "error": 0,
            "latencies": [],
            "components": 0,
        })

    def record_ok(self, name: str, latency_ms: int, component_count: int):
        entry = self._data[name]
        entry["ok"] += 1
        entry["latencies"].append(latency_ms)
        entry["components"] += component_count

    def record_timeout(self, name: str):
        self._data[name]["timeout"] += 1

    def record_error(self, name: str):
        self._data[name]["error"] += 1

    def log_summary(self):
        # Bumped from logger.info to logger.error so Railway surfaces this
        # prominently. Prior level meant the fast-cycle 30-min timeout
        # diagnosis was invisible to operators — exactly when we need it.
        if not self._data:
            logger.error("[cycle_stats] no collector data recorded")
            return
        logger.error("=== [cycle_stats] Collector cycle stats ===")
        # Sort by avg latency descending so slowest collectors surface first.
        rows = []
        for name, d in self._data.items():
            avg_ms = (
                int(sum(d["latencies"]) / len(d["latencies"]))
                if d["latencies"]
                else 0
            )
            max_ms = max(d["latencies"]) if d["latencies"] else 0
            rows.append((name, d, avg_ms, max_ms))
        rows.sort(key=lambda r: -r[2])
        for name, d, avg_ms, max_ms in rows:
            logger.error(
                f"  {name:30s}  ok={d['ok']:3d}  timeout={d['timeout']:2d}  "
                f"error={d['error']:2d}  avg={avg_ms:5d}ms  max={max_ms:5d}ms  "
                f"components={d['components']}"
            )

    def store(self):
        """Persist stats to collector_cycle_stats table."""
        if not self._data:
            return
        try:
            with get_cursor() as cur:
                for name, d in self._data.items():
                    avg_ms = (
                        int(sum(d["latencies"]) / len(d["latencies"]))
                        if d["latencies"]
                        else 0
                    )
                    cur.execute(
                        """
                        INSERT INTO collector_cycle_stats
                            (collector_name, coins_ok, coins_timeout, coins_error,
                             avg_latency_ms, total_components)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (name, d["ok"], d["timeout"], d["error"],
                         avg_ms, d["components"]),
                    )
        except Exception as e:
            logger.warning(f"Failed to store cycle stats: {e}")


# ---------------------------------------------------------------------------
# Collector descriptors
# ---------------------------------------------------------------------------

def _make_async_collectors():
    """
    Return list of (name, callable) for async collectors.
    Each callable has signature: (client, coingecko_id, stablecoin_id) -> list[dict].
    Collectors that only need (client, stablecoin_id) are wrapped to ignore cg_id.
    """
    from app.collectors.coingecko import (
        collect_peg_components,
        collect_liquidity_components,
        collect_market_activity_components,
    )
    from app.collectors.defillama import collect_defillama_components
    from app.collectors.curve import collect_curve_components
    from app.collectors.etherscan import collect_holder_distribution
    from app.collectors.flows import collect_flows_components
    from app.collectors.smart_contract import collect_smart_contract_components
    from app.collectors.solana import collect_solana_components
    from app.collectors.actor_metrics import collect_actor_metrics

    # Wrappers to normalise signature to (client, cg_id, stablecoin_id)
    async def _curve(client, _cg_id, sid):
        return await collect_curve_components(client, sid)

    async def _etherscan(client, _cg_id, sid):
        return await collect_holder_distribution(client, sid)

    async def _flows(client, _cg_id, sid):
        return await collect_flows_components(client, sid)

    async def _smart_contract(client, _cg_id, sid):
        return await collect_smart_contract_components(client, sid)

    async def _solana(client, _cg_id, sid):
        return await collect_solana_components(client, sid)

    async def _actor(client, _cg_id, sid):
        return await collect_actor_metrics(client, sid)

    return [
        ("peg", collect_peg_components),
        ("liquidity", collect_liquidity_components),
        ("market", collect_market_activity_components),
        ("defillama", collect_defillama_components),
        ("curve", _curve),
        ("etherscan", _etherscan),
        ("flows", _flows),
        ("smart_contract", _smart_contract),
        ("solana", _solana),
        ("actor_metrics", _actor),
    ]


def _make_sync_collectors():
    """
    Return list of (name, callable) for sync collectors.
    Each callable has signature: (stablecoin_id) -> list[dict].
    """
    from app.collectors.offline import (
        collect_transparency_components,
        collect_regulatory_components,
        collect_governance_components,
        collect_reserve_components,
        collect_network_components,
    )
    from app.collectors.derived import collect_derived_components

    return [
        ("transparency", collect_transparency_components),
        ("regulatory", collect_regulatory_components),
        ("governance", collect_governance_components),
        ("reserves", collect_reserve_components),
        ("network", collect_network_components),
        ("derived", collect_derived_components),
    ]


# ---------------------------------------------------------------------------
# run_all_collectors — drop-in replacement for the old inline gather+loop
# ---------------------------------------------------------------------------

async def run_all_collectors(
    client,
    stablecoin_id: str,
    cfg: dict,
    cycle_stats: CycleStats | None = None,
    timeout: float = 20.0,
) -> list[dict]:
    """
    Run every registered SII collector for one stablecoin.
    Returns flat list of component dicts (not yet tagged with stablecoin_id).
    """
    # CoinGecko ID corrections — map delisted/renamed IDs to current ones
    CG_ID_CORRECTIONS = {
        "susd": "nusd",              # Synthetix USD — CoinGecko lists as 'nusd'
        "spark": "spark-protocol",   # Spark Protocol — CoinGecko renamed to 'spark-protocol'
    }
    cg_id = CG_ID_CORRECTIONS.get(cfg["coingecko_id"], cfg["coingecko_id"])
    all_components: list[dict] = []

    # --- async collectors (parallel with per-collector timeout) ---
    async_collectors = _make_async_collectors()

    # Per-collector call that exceeds this fraction of its timeout is
    # logged as a slow call so fast-cycle 30-min timeouts can be
    # attributed to specific (collector, stablecoin) pairs without
    # re-running the cycle.
    SLOW_CALL_FRACTION = 0.75
    slow_threshold_ms = int(timeout * SLOW_CALL_FRACTION * 1000)

    async def _instrumented(name, coro):
        t0 = time.monotonic()
        try:
            result = await asyncio.wait_for(coro, timeout=timeout)
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            count = len(result) if result else 0
            if cycle_stats:
                cycle_stats.record_ok(name, elapsed_ms, count)
            if elapsed_ms >= slow_threshold_ms:
                logger.error(
                    f"[slow_collector] {name} took {elapsed_ms}ms for {stablecoin_id} "
                    f"(>={int(SLOW_CALL_FRACTION*100)}% of {int(timeout*1000)}ms timeout)"
                )
            return result or []
        except asyncio.TimeoutError:
            logger.warning(f"{name} timed out for {stablecoin_id}")
            if cycle_stats:
                cycle_stats.record_timeout(name)
            return []
        except Exception as e:
            logger.error(f"{name} error for {stablecoin_id}: {e}")
            if cycle_stats:
                cycle_stats.record_error(name)
            return []

    tasks = [
        _instrumented(name, fn(client, cg_id, stablecoin_id))
        for name, fn in async_collectors
    ]
    results = await asyncio.gather(*tasks)
    for result in results:
        if result:
            all_components.extend(result)

    # --- sync collectors (sequential, from config/scraped data) ---
    sync_collectors = _make_sync_collectors()
    for name, fn in sync_collectors:
        t0 = time.monotonic()
        try:
            result = fn(stablecoin_id)
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            count = len(result) if result else 0
            if cycle_stats:
                cycle_stats.record_ok(name, elapsed_ms, count)
            if result:
                all_components.extend(result)
        except Exception as e:
            logger.error(f"{name} error for {stablecoin_id}: {e}")
            if cycle_stats:
                cycle_stats.record_error(name)

    return all_components
