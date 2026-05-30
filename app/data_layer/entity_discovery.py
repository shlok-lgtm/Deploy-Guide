"""
Entity Auto-Discovery Engine
==============================
DeFiLlama tracks 5,000+ protocols. CoinGecko tracks 9M tokens.
Continuously scan both APIs, filter by category and TVL threshold,
and auto-promote into the appropriate Circle 7 index.

Category → Index mapping:
- Liquid staking → LSTI (>$10M TVL)
- Bridge → BRI (>$50M TVL)
- Exchange → CXRI (any with CoinGecko trust score)
- Vault/yield → VSRI (>$10M TVL)
- DAO → DOHI (>$50M TVL, has governance)
- Tokenized treasury → TTI (>$10M AUM)

Schedule: Weekly
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://api.llama.fi"

# Category to Circle 7 index mapping with TVL thresholds.
#
# PSI and SII discovery do NOT use protocol-category mapping — they use
# different DeFiLlama endpoints (pools + peggedAssets respectively) with
# their own gates. Phase 2 routes the existing PSI pipeline through
# the framework via `_run_psi_discovery_via_framework()` below so that
# `discovery_signals WHERE signal_type='entity_discovery' AND domain='psi'`
# advances on the same cycle the Circle 7 streams do, satisfying the
# OUTPUT_STREAM_CHECKS registry entry added in app/coherence.py.
# SII discovery (Phase 3) will plug in alongside via the same shape.
CATEGORY_INDEX_MAP = {
    # DeFiLlama categories → index
    "Liquid Staking": {"index": "lsti", "min_tvl": 10_000_000},
    "Bridge": {"index": "bri", "min_tvl": 50_000_000},
    "CEX": {"index": "cxri", "min_tvl": 0},
    "Yield Aggregator": {"index": "vsri", "min_tvl": 10_000_000},
    "Yield": {"index": "vsri", "min_tvl": 10_000_000},
    "Lending": {"index": "vsri", "min_tvl": 10_000_000},
    "CDP": {"index": "vsri", "min_tvl": 10_000_000},
    "RWA": {"index": "tti", "min_tvl": 10_000_000},
    "Staking": {"index": "lsti", "min_tvl": 10_000_000},
}


async def _fetch_protocols(client: httpx.AsyncClient) -> list[dict]:
    """Fetch all protocols from DeFiLlama."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("defillama")

    url = f"{DEFILLAMA_BASE}/protocols"
    start = time.time()
    try:
        resp = await client.get(url, timeout=30)
        latency = int((time.time() - start) * 1000)
        track_api_call("defillama", "/protocols", caller="entity_discovery",
                       status=resp.status_code, latency_ms=latency)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("defillama", "/protocols", caller="entity_discovery",
                       status=500, latency_ms=latency)
        logger.warning(f"DeFiLlama protocols fetch failed: {e}")
        return []


async def _get_existing_entities() -> set:
    """Get all entity slugs already tracked in Circle 7 indices."""
    from app.database import fetch_all, fetch_one_async, fetch_all_async, execute_async

    existing = set()
    try:
        rows = await fetch_all_async(
            """SELECT DISTINCT entity_slug FROM generic_index_scores
               WHERE index_id IN ('lsti', 'bri', 'vsri', 'cxri', 'tti', 'dohi')"""
        )
        if rows:
            existing.update(r["entity_slug"] for r in rows)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"[entity_discovery] generic_index_scores query failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="data_layer__get_existing_entities_index_scores_failure",
                error_message=str(e)[:500],
                cycle_phase="entity_discovery",
            )
        except Exception:
            pass

    # Also check manually configured entities
    try:
        rows = await fetch_all_async("SELECT DISTINCT protocol_slug FROM rpi_protocol_config WHERE enabled = TRUE")
        if rows:
            existing.update(r["protocol_slug"] for r in rows)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"[entity_discovery] rpi_protocol_config query failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="data_layer__get_existing_entities_rpi_config_failure",
                error_message=str(e)[:500],
                cycle_phase="entity_discovery",
            )
        except Exception:
            pass

    return existing


def _sanitize_float(val):
    """Return None if val is NaN or Infinity, else return val."""
    import math
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _store_discovered_entities(entities: list[dict]):
    """Store discovered entities to a discovery backlog (per-row transactions)."""
    if not entities:
        return

    from app.database import get_cursor

    stored = 0
    errors = 0

    for entity in entities:
        try:
            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO discovery_signals
                       (signal_type, domain, title, description, entities,
                        novelty_score, direction, magnitude, baseline,
                        detail, methodology_version)
                       VALUES ('entity_discovery', %s, %s, %s, %s,
                               %s, %s, %s, %s, %s, 'discovery-v0.1.0')""",
                    (
                        entity["target_index"],
                        f"New {entity['target_index'].upper()} candidate: {entity['name']}",
                        f"{entity['category']} protocol with ${_sanitize_float(entity['tvl']) or 0:,.0f} TVL",
                        json.dumps([entity["slug"]]),
                        0.3,  # 'notable' severity baseline
                        "new",
                        _sanitize_float(entity["tvl"]),
                        None,  # baseline TVL threshold varies per index
                        json.dumps({
                            "name": entity["name"],
                            "slug": entity["slug"],
                            "category": entity["category"],
                            "tvl_usd": _sanitize_float(entity["tvl"]),
                            "target_index": entity["target_index"],
                            "chains": entity.get("chains", []),
                        }),
                    ),
                )
            stored += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.error(
                    "Failed to store entity discovery for %s: %s",
                    entity.get("slug", "unknown"), e,
                )

    if errors:
        logger.error(
            "Entity discovery store: %d stored, %d errors", stored, errors,
        )
    logger.info(f"Stored {stored} entity discovery signals")


def _store_psi_discovery_signals(
    promoted_slugs: list[str],
    cycle_stats: dict,
) -> int:
    """Write entity_discovery rows for PSI promotions in the cycle.

    Phase 2: PSI is now visible alongside Circle 7 in discovery_signals.
    Without this, the OUTPUT_STREAM_CHECKS entry for entity_discovery:psi
    (cadence 168h) would query an empty stream and flag DEGRADED — the same
    blind-heartbeat shape #268 fixed for the Circle 7 indices.

    One signal_type='entity_discovery' row is written per promoted protocol
    in the cycle. The domain is 'psi'. Cycle stats are stored in `detail`
    so operators can see candidate/promotion counts at the per-cycle
    granularity the framework expects.

    Returns the number of signal rows written.
    """
    from app.database import get_cursor

    written = 0
    if not promoted_slugs:
        # Even with zero promotions, write one summary row so the stream
        # advances and the cadence registry doesn't flag DEGRADED purely
        # because no candidate cleared the gate this week. Distinguishable
        # via detail.cycle_status='no_promotions'.
        try:
            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO discovery_signals
                       (signal_type, domain, title, description, entities,
                        novelty_score, direction, magnitude, baseline,
                        detail, methodology_version)
                       VALUES ('entity_discovery', 'psi', %s, %s, %s,
                               %s, %s, %s, %s, %s, 'discovery-v0.1.0')""",
                    (
                        "PSI discovery cycle — no new promotions",
                        f"Considered {cycle_stats.get('discovered', 0)} candidates above $10M TVL; "
                        f"enriched {cycle_stats.get('enriched', 0)}; "
                        f"promoted {cycle_stats.get('promoted', 0)}.",
                        json.dumps([]),
                        0.1,
                        "stable",
                        0,
                        None,
                        json.dumps({**cycle_stats, "cycle_status": "no_promotions"}),
                    ),
                )
            written = 1
        except Exception as e:
            logger.warning(f"[psi-discovery] heartbeat write failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="psi_discovery_heartbeat_write_failure",
                    error_message=str(e)[:500],
                    cycle_phase="entity_discovery",
                )
            except Exception:
                pass
        return written

    for slug in promoted_slugs:
        try:
            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO discovery_signals
                       (signal_type, domain, title, description, entities,
                        novelty_score, direction, magnitude, baseline,
                        detail, methodology_version)
                       VALUES ('entity_discovery', 'psi', %s, %s, %s,
                               %s, %s, %s, %s, %s, 'discovery-v0.1.0')""",
                    (
                        f"New PSI candidate promoted: {slug}",
                        f"Protocol {slug} cleared the PSI promotion gate "
                        "(TVL >= $10M, category coverage complete).",
                        json.dumps([slug]),
                        0.3,
                        "new",
                        None,
                        None,
                        json.dumps({**cycle_stats,
                                    "promoted_slug": slug,
                                    "cycle_status": "promoted"}),
                    ),
                )
            written += 1
        except Exception as e:
            logger.warning(
                f"[psi-discovery] signal write failed for {slug}: {e}"
            )
    return written


async def _run_psi_discovery_via_framework() -> dict:
    """Run PSI's existing discover → enrich → promote chain and surface it
    through the entity_discovery framework.

    Phase 2 integration: psi_collector's pipeline at
    `app/collectors/psi_collector.py:1339-1816` is unchanged in behavior;
    this wrapper adds (a) per-domain visibility in `discovery_signals` and
    (b) framework-level logging consistent with the Circle 7 cycles.

    Returns a stats dict for caller composition.
    """
    from app.collectors.psi_collector import (
        discover_protocols,
        enrich_protocol_backlog,
        promote_eligible_protocols,
    )
    from app.database import fetch_all

    logger.error("[psi-discovery] cycle starting")
    discovered = await asyncio.to_thread(discover_protocols)
    enriched = await asyncio.to_thread(enrich_protocol_backlog)

    # Capture freshly-promoted slugs by diffing pre/post.
    promote_pre = await asyncio.to_thread(
        fetch_all,
        "SELECT slug FROM protocol_backlog WHERE enrichment_status = 'promoted'",
    )
    pre_set = {r["slug"] for r in (promote_pre or [])}
    promoted = await asyncio.to_thread(promote_eligible_protocols)
    promote_post = await asyncio.to_thread(
        fetch_all,
        "SELECT slug FROM protocol_backlog WHERE enrichment_status = 'promoted'",
    )
    new_promoted = sorted({r["slug"] for r in (promote_post or [])} - pre_set)

    cycle_stats = {
        "discovered": discovered,
        "enriched": enriched,
        "promoted": promoted,
        "newly_promoted_count": len(new_promoted),
    }
    signals_written = await asyncio.to_thread(
        _store_psi_discovery_signals, new_promoted, cycle_stats,
    )
    logger.error(
        f"[psi-discovery] cycle done: discovered={discovered} "
        f"enriched={enriched} promoted={promoted} "
        f"newly_promoted={new_promoted} signals_written={signals_written}"
    )

    return {**cycle_stats,
            "newly_promoted": new_promoted,
            "signals_written": signals_written}


async def run_entity_discovery() -> dict:
    """
    Full entity discovery cycle:
    1. Fetch all protocols from DeFiLlama
    2. Filter by category and TVL threshold
    3. Exclude already-tracked entities
    4. Emit discovery signals for new candidates
    5. Run PSI discovery via the same framework (Phase 2)

    Returns summary.
    """
    existing = await _get_existing_entities()
    logger.error(f"[discovery] starting: {len(existing)} existing entities tracked")

    async with httpx.AsyncClient(timeout=30) as client:
        protocols = await _fetch_protocols(client)

    if not protocols:
        logger.error("[discovery] ZERO protocols fetched from DeFiLlama — upstream may be down")
        return {"error": "no protocols fetched from DeFiLlama"}

    candidates = []
    by_index = {}

    for protocol in protocols:
        category = protocol.get("category", "")
        slug = protocol.get("slug", "")
        name = protocol.get("name", "")
        tvl = protocol.get("tvl") or 0

        if not slug or slug in existing:
            continue

        # Check if category maps to a Circle 7 index
        mapping = CATEGORY_INDEX_MAP.get(category)
        if not mapping:
            continue

        # Check TVL threshold
        if tvl < mapping["min_tvl"]:
            continue

        target_index = mapping["index"]
        candidate = {
            "slug": slug,
            "name": name,
            "category": category,
            "tvl": tvl,
            "target_index": target_index,
            "chains": protocol.get("chains", []),
        }
        candidates.append(candidate)
        by_index.setdefault(target_index, []).append(candidate)

    # Store discovery signals
    if candidates:
        await asyncio.to_thread(_store_discovered_entities, candidates)

    # Summary by index
    index_summary = {
        idx: len(entities) for idx, entities in by_index.items()
    }

    logger.error(
        f"[discovery] SUMMARY: sources_scanned={len(protocols)}, "
        f"already_tracked={len(existing)}, signals_found={len(candidates)}, "
        f"by_index={index_summary}"
    )

    # Phase 2: PSI's existing discover/enrich/promote chain runs on the
    # same cycle so its `entity_discovery:psi` stream advances on the same
    # 168h cadence the Circle 7 streams use. Failure here must not block
    # the Circle 7 summary already computed above.
    psi_stats: dict = {}
    try:
        psi_stats = await _run_psi_discovery_via_framework()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"[psi-discovery] cycle failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="psi_discovery_framework_cycle_failure",
                error_message=str(e)[:500],
                cycle_phase="entity_discovery",
            )
        except Exception:
            pass
        psi_stats = {"error": str(e)[:200]}

    return {
        "protocols_scanned": len(protocols),
        "already_tracked": len(existing),
        "new_candidates": len(candidates),
        "by_index": index_summary,
        "top_candidates": [
            {"name": c["name"], "index": c["target_index"], "tvl": c["tvl"]}
            for c in sorted(candidates, key=lambda x: x["tvl"], reverse=True)[:20]
        ],
        "psi_discovery": psi_stats,
    }
