"""
DOHI Backfill — historical DAO Operational Health Index scores.
Sources: Snapshot GraphQL (proposals, voters), Tally GraphQL (delegates).

For each DAO entity in DAO_ENTITIES, fetches historical governance
proposals from Snapshot, computes trailing 90-day activity metrics,
and writes raw_values into generic_index_scores with backfilled=TRUE.
Optionally supplements with Tally data if TALLY_API_KEY is set.
"""
import asyncio
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from scripts.backfill.base import init_db, log_run_start, log_run_complete, parse_args
from app.index_definitions.dohi_v01 import DAO_ENTITIES
from app.api_usage_tracker import track_api_call

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("backfill_dohi")

INDEX_ID = "dohi"
FORMULA_VERSION = "dohi-v0.1.0-backfill"
BACKFILL_SOURCE = "snapshot"
SNAPSHOT_GRAPHQL_URL = "https://hub.snapshot.org/graphql"
TALLY_GRAPHQL_URL = "https://api.tally.xyz/query"


async def fetch_snapshot_proposals(
    client, space: str, start_ts: int
) -> list[dict]:
    """
    Fetch all proposals from Snapshot for a given space since start_ts.
    Pages through results in batches of 1000.
    """
    all_proposals = []
    created_gte = start_ts

    while True:
        query = """
        query($space: String!, $created_gte: Int!) {
            proposals(
                where: {space: $space, created_gte: $created_gte}
                first: 1000
                orderBy: "created"
                orderDirection: asc
            ) {
                id
                title
                state
                scores_total
                votes
                created
            }
        }
        """
        variables = {"space": space, "created_gte": created_gte}

        t0 = time.time()
        try:
            resp = await client.post(
                SNAPSHOT_GRAPHQL_URL,
                json={"query": query, "variables": variables},
            )
            latency_ms = int((time.time() - t0) * 1000)
            track_api_call(
                provider="snapshot",
                endpoint=f"/graphql/proposals/{space}",
                caller="backfill_dohi",
                status=resp.status_code,
                latency_ms=latency_ms,
            )

            if resp.status_code == 429:
                logger.warning(f"Snapshot rate limited for {space}, sleeping 10s")
                await asyncio.sleep(10)
                continue

            if resp.status_code != 200:
                logger.warning(f"Snapshot {space}: HTTP {resp.status_code}")
                break

            data = resp.json()
            proposals = data.get("data", {}).get("proposals", [])

            if not proposals:
                break

            all_proposals.extend(proposals)

            # If we got a full page, continue pagination
            if len(proposals) < 1000:
                break

            # Move the cursor past the last proposal's created time
            last_created = proposals[-1].get("created", 0)
            if last_created <= created_gte:
                break
            created_gte = last_created

            await asyncio.sleep(0.5)

        except Exception as e:
            latency_ms = int((time.time() - t0) * 1000)
            track_api_call(
                provider="snapshot",
                endpoint=f"/graphql/proposals/{space}",
                caller="backfill_dohi",
                status=0,
                latency_ms=latency_ms,
            )
            logger.error(f"Snapshot {space} request failed: {e}")
            break

    return all_proposals


async def fetch_tally_delegates(client, tally_org: str, api_key: str) -> int | None:
    """
    Fetch delegate count from Tally GraphQL API.
    Returns delegate count or None on failure.
    """
    query = """
    query($orgSlug: String!) {
        organization(slug: $orgSlug) {
            delegatesCount
        }
    }
    """
    variables = {"orgSlug": tally_org}
    headers = {"Api-Key": api_key}

    t0 = time.time()
    try:
        resp = await client.post(
            TALLY_GRAPHQL_URL,
            json={"query": query, "variables": variables},
            headers=headers,
        )
        latency_ms = int((time.time() - t0) * 1000)
        track_api_call(
            provider="tally",
            endpoint=f"/query/delegates/{tally_org}",
            caller="backfill_dohi",
            status=resp.status_code,
            latency_ms=latency_ms,
        )

        if resp.status_code == 429:
            logger.warning(f"Tally rate limited for {tally_org}, sleeping 10s")
            await asyncio.sleep(10)
            return None

        if resp.status_code != 200:
            logger.warning(f"Tally {tally_org}: HTTP {resp.status_code}")
            return None

        data = resp.json()
        org = data.get("data", {}).get("organization", {})
        return org.get("delegatesCount")
    except Exception as e:
        latency_ms = int((time.time() - t0) * 1000)
        track_api_call(
            provider="tally",
            endpoint=f"/query/delegates/{tally_org}",
            caller="backfill_dohi",
            status=0,
            latency_ms=latency_ms,
        )
        logger.error(f"Tally {tally_org} request failed: {e}")
        return None


def compute_daily_metrics(
    proposals: list[dict], target_date: datetime, trailing_days: int = 90
) -> dict:
    """
    Compute governance metrics for a specific date using a trailing window.

    Returns dict with:
        proposal_frequency_90d: count of proposals in window
        voter_participation_rate: avg votes per proposal (raw count)
        quorum_achievement_rate: % of closed proposals that reached quorum (approximated)
        proposal_pass_rate: % of closed proposals with state 'closed' (passed)
        unique_voter_estimate: estimated unique voters (sum of votes across proposals)
    """
    window_start = target_date - timedelta(days=trailing_days)
    window_start_ts = int(window_start.timestamp())
    target_ts = int(target_date.timestamp())

    window_proposals = [
        p for p in proposals
        if window_start_ts <= p.get("created", 0) <= target_ts
    ]

    proposal_count = len(window_proposals)
    if proposal_count == 0:
        return {
            "proposal_frequency_90d": 0,
            "voter_participation_rate": 0,
            "quorum_achievement_rate": 0,
            "proposal_pass_rate": 0,
        }

    # Votes and participation
    total_votes = sum(p.get("votes", 0) for p in window_proposals)
    avg_votes_per_proposal = total_votes / proposal_count if proposal_count > 0 else 0

    # Closed (finalized) proposals
    closed = [p for p in window_proposals if p.get("state") == "closed"]
    closed_count = len(closed)

    # Proposal pass rate: closed proposals as fraction of total
    # (Snapshot "closed" = executed/passed; "pending"/"active" = ongoing)
    pass_rate = 0.0
    if proposal_count > 0:
        pass_rate = round((closed_count / proposal_count) * 100, 1)

    # Quorum approximation: proposals with scores_total > 0
    quorum_achieved = sum(
        1 for p in closed if (p.get("scores_total") or 0) > 0
    )
    quorum_rate = 0.0
    if closed_count > 0:
        quorum_rate = round((quorum_achieved / closed_count) * 100, 1)

    return {
        "proposal_frequency_90d": proposal_count,
        "voter_participation_rate": round(avg_votes_per_proposal, 1),
        "quorum_achievement_rate": quorum_rate,
        "proposal_pass_rate": pass_rate,
    }


async def backfill_entity(entity: dict, days_back: int = 365):
    """Backfill a single DAO entity from Snapshot (and optionally Tally) data."""
    import httpx
    from app.database import execute

    slug = entity["slug"]
    name = entity["name"]
    snapshot_space = entity.get("snapshot_space")
    tally_org = entity.get("tally_org")

    if not snapshot_space:
        logger.warning(f"DOHI backfill {slug}: no snapshot_space, skipping")
        return 0, 0

    source = BACKFILL_SOURCE
    if tally_org:
        source = "snapshot+tally"

    run_id = log_run_start(INDEX_ID, slug, source)
    rows_written = 0
    rows_failed = 0

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_back)
    # Fetch proposals starting 90 days before our window for trailing metrics
    fetch_start = start_date - timedelta(days=90)
    fetch_start_ts = int(fetch_start.timestamp())

    logger.info(f"DOHI backfill {slug}: {start_date.date()} -> {end_date.date()}")

    async with httpx.AsyncClient(timeout=30) as client:
        # Fetch Snapshot proposals
        proposals = await fetch_snapshot_proposals(
            client, snapshot_space, fetch_start_ts
        )
        await asyncio.sleep(0.5)

        # Optionally fetch Tally delegate count
        delegate_count = None
        tally_api_key = os.environ.get("TALLY_API_KEY")
        if tally_org and tally_api_key:
            delegate_count = await fetch_tally_delegates(
                client, tally_org, tally_api_key
            )
            await asyncio.sleep(0.5)
        elif tally_org:
            logger.info(
                f"DOHI backfill {slug}: TALLY_API_KEY not set, "
                f"skipping Tally data for {tally_org}"
            )

    if not proposals:
        logger.info(f"DOHI backfill {slug}: no proposals found on Snapshot")
        # Still complete the run — this is valid (DAO may be inactive)
        log_run_complete(run_id, 0, 0, "no_proposals")
        return 0, 0

    logger.info(f"DOHI backfill {slug}: fetched {len(proposals)} proposals")

    # Generate daily rows
    current = start_date
    while current <= end_date:
        score_date = current.date()

        metrics = compute_daily_metrics(proposals, current, trailing_days=90)

        raw_values = {
            "proposal_frequency_90d": metrics["proposal_frequency_90d"],
            "voter_participation_rate": metrics["voter_participation_rate"],
            "quorum_achievement_rate": metrics["quorum_achievement_rate"],
            "proposal_pass_rate": metrics["proposal_pass_rate"],
        }

        # Add delegate_count if available (static — Tally gives current snapshot)
        if delegate_count is not None:
            raw_values["delegate_count"] = delegate_count

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
                 score_date, source),
            )
            rows_written += 1
        except Exception as e:
            rows_failed += 1
            if rows_failed <= 3:
                logger.warning(f"DOHI backfill row {slug}/{score_date}: {e}")

        current += timedelta(days=1)

    logger.info(f"DOHI backfill {slug}: {rows_written} written, {rows_failed} failed")
    log_run_complete(run_id, rows_written, rows_failed)
    return rows_written, rows_failed


async def main():
    args = parse_args()
    init_db()

    entities = [e for e in DAO_ENTITIES if e.get("snapshot_space")]
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
                f"DOHI progress: {i + 1}/{len(entities)} entities, "
                f"{total_written} total rows written, {total_failed} failed"
            )

        # Rate limit between API calls
        await asyncio.sleep(0.5)

    logger.info(
        f"DOHI backfill complete: {len(entities)} entities, "
        f"{total_written} rows written, {total_failed} failed"
    )


if __name__ == "__main__":
    asyncio.run(main())
