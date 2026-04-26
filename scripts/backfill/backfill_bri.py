"""
BRI Backfill — historical Bridge Integrity Index scores.
Sources: DeFiLlama Bridges API (volume, TVL, chain data).

For each bridge entity in BRIDGE_ENTITIES, fetches historical volume
and TVL data from DeFiLlama, computes rolling 30-day volume, and
writes raw_values into generic_index_scores with backfilled=TRUE.
"""
import asyncio
import json
import logging
import sys
import os
import time
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from scripts.backfill.base import init_db, log_run_start, log_run_complete, parse_args
from app.index_definitions.bri_v01 import BRIDGE_ENTITIES
from app.api_usage_tracker import track_api_call

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("backfill_bri")

INDEX_ID = "bri"
FORMULA_VERSION = "bri-v0.1.0-backfill"
BACKFILL_SOURCE = "defillama_bridges"
DEFILLAMA_BRIDGES_URL = "https://bridges.llama.fi/bridge"


async def fetch_bridge_data(client, defillama_id: str) -> dict | None:
    """Fetch bridge data from DeFiLlama Bridges API with rate-limit handling."""
    url = f"{DEFILLAMA_BRIDGES_URL}/{defillama_id}"
    t0 = time.time()
    try:
        resp = await client.get(url)
        latency_ms = int((time.time() - t0) * 1000)
        track_api_call(
            provider="defillama",
            endpoint=f"/bridge/{defillama_id}",
            caller="backfill_bri",
            status=resp.status_code,
            latency_ms=latency_ms,
        )
        if resp.status_code == 429:
            logger.warning(f"Rate limited on {defillama_id}, sleeping 10s")
            await asyncio.sleep(10)
            return None
        if resp.status_code != 200:
            logger.warning(f"DeFiLlama bridge {defillama_id}: HTTP {resp.status_code}")
            return None
        return resp.json()
    except Exception as e:
        latency_ms = int((time.time() - t0) * 1000)
        track_api_call(
            provider="defillama",
            endpoint=f"/bridge/{defillama_id}",
            caller="backfill_bri",
            status=0,
            latency_ms=latency_ms,
        )
        logger.error(f"DeFiLlama bridge {defillama_id} request failed: {e}")
        return None


def build_volume_timeseries(data: dict) -> dict:
    """
    Parse DeFiLlama bridge data into a date-keyed volume timeseries.
    Returns {date_str: daily_volume_usd}.
    """
    volumes = {}
    # DeFiLlama bridges API returns volume data in chainBreakdown or
    # currentDayVolume-style arrays depending on version. Parse both forms.
    chain_breakdown = data.get("chainBreakdown", {})
    for chain_name, chain_data in chain_breakdown.items():
        for direction in ("deposits", "withdrawals"):
            entries = chain_data.get(direction, {}).get("txs", [])
            if not entries:
                entries = chain_data.get(direction, [])
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                ts = entry.get("date")
                vol = entry.get("usdValue", 0) or entry.get("totalUsd", 0) or 0
                if ts is None:
                    continue
                try:
                    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                    date_key = dt.strftime("%Y-%m-%d")
                except (ValueError, OSError):
                    continue
                volumes[date_key] = volumes.get(date_key, 0) + float(vol)
    return volumes


def build_tvl_timeseries(data: dict) -> dict:
    """
    Parse historical TVL from DeFiLlama bridge data.
    Returns {date_str: tvl_usd}.
    """
    tvl_map = {}
    chain_breakdown = data.get("chainBreakdown", {})
    # Aggregate TVL across chains if available
    for chain_name, chain_data in chain_breakdown.items():
        for direction in ("deposits", "withdrawals"):
            tokens = chain_data.get(direction, {}).get("tokens", [])
            for entry in tokens:
                if not isinstance(entry, dict):
                    continue
                ts = entry.get("date")
                tvl = entry.get("usdValue", 0) or 0
                if ts is None:
                    continue
                try:
                    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                    date_key = dt.strftime("%Y-%m-%d")
                except (ValueError, OSError):
                    continue
                tvl_map[date_key] = tvl_map.get(date_key, 0) + float(tvl)
    return tvl_map


def compute_rolling_30d(volumes: dict, target_date: str) -> float:
    """Compute rolling 30-day volume sum ending on target_date."""
    from datetime import date as date_cls
    target = datetime.strptime(target_date, "%Y-%m-%d").date()
    total = 0.0
    for i in range(30):
        d = (target - timedelta(days=i)).strftime("%Y-%m-%d")
        total += volumes.get(d, 0)
    return total


async def backfill_entity(entity: dict, days_back: int = 365):
    """Backfill a single bridge entity from DeFiLlama data."""
    import httpx
    from app.database import execute

    slug = entity["slug"]
    name = entity["name"]
    defillama_id = entity.get("defillama_id")
    if not defillama_id:
        logger.warning(f"BRI backfill {slug}: no defillama_id, skipping")
        return 0, 0

    run_id = log_run_start(INDEX_ID, slug, BACKFILL_SOURCE)
    rows_written = 0
    rows_failed = 0

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_back)

    logger.info(f"BRI backfill {slug}: {start_date.date()} -> {end_date.date()}")

    async with httpx.AsyncClient(timeout=30) as client:
        data = await fetch_bridge_data(client, defillama_id)

    if data is None:
        log_run_complete(run_id, 0, 0, "fetch_failed")
        return 0, 0

    # Parse volume and TVL timeseries
    volumes = build_volume_timeseries(data)
    tvl_map = build_tvl_timeseries(data)

    # Extract chain count from the data
    chains = data.get("chains", [])
    supported_chains = len(chains) if chains else 0

    # Current day volume for reference
    current_day_volume = data.get("currentDayVolume", 0) or 0

    if not volumes and not tvl_map:
        logger.info(f"BRI backfill {slug}: no historical volume/TVL data found")
        # Still write rows with chain count and static data
        if supported_chains == 0:
            log_run_complete(run_id, 0, 0, "no_data")
            return 0, 0

    # Generate daily rows for the date range
    current = start_date
    while current <= end_date:
        score_date = current.date()
        date_key = score_date.strftime("%Y-%m-%d")

        daily_volume = volumes.get(date_key, 0)
        daily_tvl = tvl_map.get(date_key, 0)
        volume_30d = compute_rolling_30d(volumes, date_key)

        # Compute volume/TVL ratio
        volume_tvl_ratio = 0.0
        if daily_tvl > 0:
            volume_tvl_ratio = round(daily_volume / daily_tvl, 4)

        # Build raw_values with available components
        raw_values = {
            "bridge_tvl": daily_tvl,
            "daily_volume": daily_volume,
            "volume_30d": volume_30d,
            "volume_tvl_ratio": volume_tvl_ratio,
            "supported_chains": supported_chains,
        }

        # Add total_value_transferred as cumulative sum up to this date
        cumulative = sum(
            v for d, v in volumes.items() if d <= date_key
        )
        if cumulative > 0:
            raw_values["total_value_transferred"] = cumulative

        raw_json = json.dumps(raw_values)

        try:
            execute(
                """
                INSERT INTO generic_index_scores
                    (index_id, entity_slug, entity_name, overall_score,
                     raw_values, formula_version, scored_date,
                     backfilled, backfill_source)
                VALUES (%s, %s, %s, NULL, %s, %s, %s, TRUE, %s)
                ON CONFLICT (index_id, entity_slug, scored_date) DO UPDATE
                SET raw_values = EXCLUDED.raw_values,
                    formula_version = EXCLUDED.formula_version,
                    backfill_source = EXCLUDED.backfill_source
                """,
                (INDEX_ID, slug, name, raw_json, FORMULA_VERSION,
                 score_date, BACKFILL_SOURCE),
            )
            rows_written += 1
        except Exception as e:
            rows_failed += 1
            if rows_failed <= 3:
                logger.warning(f"BRI backfill row {slug}/{score_date}: {e}")

        current += timedelta(days=1)

    logger.info(f"BRI backfill {slug}: {rows_written} written, {rows_failed} failed")
    log_run_complete(run_id, rows_written, rows_failed)
    return rows_written, rows_failed


async def main():
    args = parse_args()
    init_db()

    entities = BRIDGE_ENTITIES
    if args.limit > 0:
        entities = entities[: args.limit]

    total_written = 0
    total_failed = 0

    for i, entity in enumerate(entities):
        written, failed = await backfill_entity(entity, days_back=args.days_back)
        total_written += written
        total_failed += failed

        # Log progress every 10 entities
        if (i + 1) % 10 == 0 or (i + 1) == len(entities):
            logger.info(
                f"BRI progress: {i + 1}/{len(entities)} entities, "
                f"{total_written} total rows written, {total_failed} failed"
            )

        # Rate limit between API calls
        await asyncio.sleep(0.5)

    logger.info(
        f"BRI backfill complete: {len(entities)} entities, "
        f"{total_written} rows written, {total_failed} failed"
    )


if __name__ == "__main__":
    asyncio.run(main())
