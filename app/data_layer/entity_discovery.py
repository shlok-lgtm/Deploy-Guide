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

import json
import logging
import time
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://api.llama.fi"

# Category to Circle 7 index mapping with TVL thresholds
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


def _get_existing_entities() -> set:
    """Get all entity slugs already tracked in Circle 7 indices."""
    from app.database import fetch_all

    existing = set()
    try:
        rows = fetch_all(
            """SELECT DISTINCT entity_id FROM generic_index_scores
               WHERE index_id IN ('lsti', 'bri', 'vsri', 'cxri', 'tti', 'dohi')"""
        )
        if rows:
            existing.update(r["entity_id"] for r in rows)
    except Exception:
        pass

    # Also check manually configured entities
    try:
        rows = fetch_all("SELECT DISTINCT protocol_slug FROM rpi_protocol_config WHERE enabled = TRUE")
        if rows:
            existing.update(r["protocol_slug"] for r in rows)
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
                       (signal_type, domain, entity_id, severity, title, details, created_at)
                       VALUES ('entity_discovery', %s, %s, 'notable', %s, %s, NOW())
                       ON CONFLICT DO NOTHING""",
                    (
                        entity["target_index"],
                        entity["slug"],
                        f"New {entity['target_index'].upper()} candidate: {entity['name']}",
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


async def run_entity_discovery() -> dict:
    """
    Full entity discovery cycle:
    1. Fetch all protocols from DeFiLlama
    2. Filter by category and TVL threshold
    3. Exclude already-tracked entities
    4. Emit discovery signals for new candidates

    Returns summary.
    """
    existing = _get_existing_entities()

    async with httpx.AsyncClient(timeout=30) as client:
        protocols = await _fetch_protocols(client)

    if not protocols:
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
        _store_discovered_entities(candidates)

    # Summary by index
    index_summary = {
        idx: len(entities) for idx, entities in by_index.items()
    }

    logger.info(
        f"Entity discovery complete: {len(candidates)} new candidates "
        f"across {len(by_index)} indices from {len(protocols)} protocols scanned"
    )

    return {
        "protocols_scanned": len(protocols),
        "already_tracked": len(existing),
        "new_candidates": len(candidates),
        "by_index": index_summary,
        "top_candidates": [
            {"name": c["name"], "index": c["target_index"], "tvl": c["tvl"]}
            for c in sorted(candidates, key=lambda x: x["tvl"], reverse=True)[:20]
        ],
    }
