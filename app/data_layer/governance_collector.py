"""
Tier 3: Governance Activity Collector
======================================
Expands dao_collector.py to store raw proposal and vote data,
not just aggregated signals.

Sources:
- Snapshot API: proposals, votes, spaces for all scored protocols
- Tally API: on-chain governance

Schedule: Daily
"""

import json
import logging
import math
import time
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

SNAPSHOT_GRAPHQL = "https://hub.snapshot.org/graphql"
TALLY_GRAPHQL = "https://api.tally.xyz/query"

# All protocol spaces we track
SNAPSHOT_SPACES = [
    "aavedao.eth", "lido-snapshot.eth", "comp-vote.eth",
    "uniswapgovernance.eth", "curve.eth", "cvx.eth",
    "morpho.eth", "makerdao.eth", "eigenlayer-community.eth",
    "frax.eth", "pendle.eth", "rocketpool-dao.eth",
    "safe.eth", "arbitrumfoundation.eth", "skyecosystem.eth",
]

TALLY_ORGS = [
    "compound", "uniswap", "aave", "arbitrum",
]


async def collect_snapshot_proposals_full(
    client: httpx.AsyncClient,
    space: str,
    since_days: int = 90,
) -> list[dict]:
    """Fetch full proposal data from Snapshot including vote details."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("snapshot")

    query = """
    query Proposals($space: String!, $first: Int!, $skip: Int!) {
        proposals(
            where: { space: $space, created_gte: %d }
            first: $first
            skip: $skip
            orderBy: "created"
            orderDirection: desc
        ) {
            id
            title
            body
            choices
            start
            end
            state
            author
            scores
            scores_total
            votes
            quorum
            type
            created
        }
    }
    """ % int((datetime.now(timezone.utc).timestamp()) - since_days * 86400)

    start = time.time()
    try:
        resp = await client.post(
            SNAPSHOT_GRAPHQL,
            json={
                "query": query,
                "variables": {"space": space, "first": 100, "skip": 0},
            },
            timeout=15,
        )
        latency = int((time.time() - start) * 1000)
        track_api_call("snapshot", f"/proposals/{space}", caller="governance_collector",
                       status=resp.status_code, latency_ms=latency)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", {}).get("proposals", [])
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("snapshot", f"/proposals/{space}", caller="governance_collector",
                       status=500, latency_ms=latency)
        logger.warning(f"Snapshot proposals fetch failed for {space}: {e}")
        return []


async def collect_snapshot_voters(
    client: httpx.AsyncClient,
    proposal_id: str,
    space: str,
) -> list[dict]:
    """Fetch voter details for a specific proposal."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("snapshot")

    query = """
    query Votes($proposal: String!, $first: Int!) {
        votes(
            where: { proposal: $proposal }
            first: $first
            orderBy: "vp"
            orderDirection: desc
        ) {
            voter
            vp
            choice
            created
        }
    }
    """

    start = time.time()
    try:
        resp = await client.post(
            SNAPSHOT_GRAPHQL,
            json={
                "query": query,
                "variables": {"proposal": proposal_id, "first": 1000},
            },
            timeout=15,
        )
        latency = int((time.time() - start) * 1000)
        track_api_call("snapshot", f"/votes/{space}", caller="governance_collector",
                       status=resp.status_code, latency_ms=latency)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", {}).get("votes", [])
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("snapshot", f"/votes/{space}", caller="governance_collector",
                       status=500, latency_ms=latency)
        logger.debug(f"Snapshot voters fetch failed for {proposal_id}: {e}")
        return []


async def collect_tally_proposals_full(
    client: httpx.AsyncClient,
    org_slug: str,
) -> list[dict]:
    """Fetch on-chain proposals from Tally."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("tally")

    query = """
    query Proposals($slug: String!) {
        proposals(
            governorSlug: $slug
            pagination: { limit: 50, offset: 0 }
            sort: { sortBy: START_BLOCK, isDescending: true }
        ) {
            id
            title
            description
            status
            proposer { address }
            voteStats { votesCount support percent }
            quorum
            createdAt
            startTime
            endTime
        }
    }
    """

    start = time.time()
    try:
        resp = await client.post(
            TALLY_GRAPHQL,
            json={"query": query, "variables": {"slug": org_slug}},
            timeout=15,
        )
        latency = int((time.time() - start) * 1000)
        track_api_call("tally", f"/proposals/{org_slug}", caller="governance_collector",
                       status=resp.status_code, latency_ms=latency)

        if resp.status_code != 200:
            return []

        resp.raise_for_status()
        data = resp.json()
        return data.get("data", {}).get("proposals", [])
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("tally", f"/proposals/{org_slug}", caller="governance_collector",
                       status=500, latency_ms=latency)
        logger.warning(f"Tally proposals fetch failed for {org_slug}: {e}")
        return []


def _store_proposals(proposals: list[dict]):
    """Store governance proposals to DB. Per-row error handling — one bad row doesn't kill the batch."""
    if not proposals:
        return

    from app.database import get_cursor

    def _safe_num(v):
        if v is None:
            return None
        try:
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except (TypeError, ValueError):
            return None

    stored = 0
    errors = 0
    for p in proposals:
        try:
            scores_json = json.dumps(p.get("scores")) if p.get("scores") else None
            raw_json = json.dumps(p.get("raw_data")) if p.get("raw_data") else None

            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO governance_proposals
                       (protocol, source, proposal_id, title, state, author,
                        created_at, start_at, end_at,
                        votes_for, votes_against, votes_abstain,
                        voter_count, quorum_reached, scores, raw_data,
                        collected_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
                       ON CONFLICT (protocol, source, proposal_id) DO UPDATE SET
                           state = EXCLUDED.state,
                           votes_for = EXCLUDED.votes_for,
                           votes_against = EXCLUDED.votes_against,
                           voter_count = EXCLUDED.voter_count,
                           quorum_reached = EXCLUDED.quorum_reached,
                           scores = EXCLUDED.scores,
                           collected_at = NOW()""",
                    (
                        p["protocol"], p["source"], p["proposal_id"],
                        p.get("title"), p.get("state"), p.get("author"),
                        p.get("created_at"), p.get("start_at"), p.get("end_at"),
                        _safe_num(p.get("votes_for")), _safe_num(p.get("votes_against")),
                        _safe_num(p.get("votes_abstain")),
                        p.get("voter_count"), p.get("quorum_reached"),
                        scores_json, raw_json,
                    ),
                )
            stored += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.error(f"governance_proposals row FAILED: proposal_id={p.get('proposal_id')}: {type(e).__name__}: {e}")

    logger.error(f"governance_proposals: {stored} stored, {errors} errors out of {len(proposals)}")


def _store_voters(voters: list[dict]):
    """Store governance voters to DB. Per-row error handling — one bad row doesn't kill the batch."""
    if not voters:
        return

    from app.database import get_cursor

    def _safe_num(v):
        if v is None:
            return None
        try:
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except (TypeError, ValueError):
            return None

    stored = 0
    errors = 0
    for v in voters:
        try:
            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO governance_voters
                       (protocol, proposal_id, voter_address, voting_power, choice, created_at, collected_at)
                       VALUES (%s, %s, %s, %s, %s, %s, NOW())
                       ON CONFLICT (protocol, proposal_id, voter_address) DO UPDATE SET
                           voting_power = EXCLUDED.voting_power,
                           collected_at = NOW()""",
                    (
                        v["protocol"], v["proposal_id"], v["voter_address"],
                        _safe_num(v.get("voting_power")), v.get("choice"), v.get("created_at"),
                    ),
                )
            stored += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.error(f"governance_voters row FAILED: voter={v.get('voter_address')}: {type(e).__name__}: {e}")

    logger.error(f"governance_voters: {stored} stored, {errors} errors out of {len(voters)}")


async def run_governance_collection() -> dict:
    """
    Full governance collection cycle:
    1. Fetch proposals from all Snapshot spaces
    2. Fetch top voter data for recent active proposals
    3. Fetch on-chain proposals from Tally
    4. Store all data

    Returns summary.
    """
    total_proposals = 0
    total_voters = 0
    spaces_processed = 0

    async with httpx.AsyncClient(timeout=30) as client:
        # Snapshot proposals
        for space in SNAPSHOT_SPACES:
            try:
                proposals = await collect_snapshot_proposals_full(client, space, since_days=90)
                if not proposals:
                    continue

                # Extract protocol name from space
                protocol = space.replace(".eth", "").replace("-snapshot", "").replace("-vote", "")

                # Transform to storage format
                proposal_records = []
                for p in proposals:
                    scores = p.get("scores", [])
                    votes_for = scores[0] if len(scores) > 0 else None
                    votes_against = scores[1] if len(scores) > 1 else None
                    votes_abstain = scores[2] if len(scores) > 2 else None

                    quorum = p.get("quorum")
                    scores_total = p.get("scores_total", 0)
                    quorum_reached = scores_total >= quorum if quorum and scores_total else None

                    proposal_records.append({
                        "protocol": protocol,
                        "source": "snapshot",
                        "proposal_id": p["id"],
                        "title": p.get("title"),
                        "state": p.get("state"),
                        "author": p.get("author"),
                        "created_at": datetime.fromtimestamp(p["created"], tz=timezone.utc) if p.get("created") else None,
                        "start_at": datetime.fromtimestamp(p["start"], tz=timezone.utc) if p.get("start") else None,
                        "end_at": datetime.fromtimestamp(p["end"], tz=timezone.utc) if p.get("end") else None,
                        "votes_for": votes_for,
                        "votes_against": votes_against,
                        "votes_abstain": votes_abstain,
                        "voter_count": p.get("votes"),
                        "quorum_reached": quorum_reached,
                        "scores": scores,
                        "raw_data": {
                            "choices": p.get("choices"),
                            "type": p.get("type"),
                            "quorum": quorum,
                            "scores_total": scores_total,
                        },
                    })

                _store_proposals(proposal_records)
                total_proposals += len(proposal_records)

                # Fetch voters for recent active proposals (top 3 by recency)
                active = [p for p in proposals if p.get("state") in ("active", "closed")]
                for prop in active[:3]:
                    try:
                        voters = await collect_snapshot_voters(client, prop["id"], space)
                        if voters:
                            voter_records = [
                                {
                                    "protocol": protocol,
                                    "proposal_id": prop["id"],
                                    "voter_address": v["voter"],
                                    "voting_power": v.get("vp"),
                                    "choice": v.get("choice"),
                                    "created_at": datetime.fromtimestamp(v["created"], tz=timezone.utc) if v.get("created") else None,
                                }
                                for v in voters
                            ]
                            _store_voters(voter_records)
                            total_voters += len(voter_records)
                    except Exception as e:
                        logger.debug(f"Voter collection failed for {prop['id']}: {e}")

                spaces_processed += 1

            except Exception as e:
                logger.warning(f"Snapshot collection failed for {space}: {e}")

        # Tally on-chain proposals
        tally_proposals = 0
        for org in TALLY_ORGS:
            try:
                proposals = await collect_tally_proposals_full(client, org)
                if not proposals:
                    continue

                proposal_records = []
                for p in proposals:
                    proposer = p.get("proposer", {})
                    vote_stats = p.get("voteStats", [])

                    votes_for = None
                    votes_against = None
                    votes_abstain = None
                    for vs in vote_stats:
                        support = vs.get("support")
                        if support == "FOR":
                            votes_for = vs.get("votesCount")
                        elif support == "AGAINST":
                            votes_against = vs.get("votesCount")
                        elif support == "ABSTAIN":
                            votes_abstain = vs.get("votesCount")

                    proposal_records.append({
                        "protocol": org,
                        "source": "tally",
                        "proposal_id": p.get("id", ""),
                        "title": p.get("title"),
                        "state": p.get("status"),
                        "author": proposer.get("address") if proposer else None,
                        "created_at": p.get("createdAt"),
                        "start_at": p.get("startTime"),
                        "end_at": p.get("endTime"),
                        "votes_for": votes_for,
                        "votes_against": votes_against,
                        "votes_abstain": votes_abstain,
                        "voter_count": None,
                        "quorum_reached": None,
                        "scores": None,
                        "raw_data": {"quorum": p.get("quorum")},
                    })

                _store_proposals(proposal_records)
                tally_proposals += len(proposal_records)
                total_proposals += len(proposal_records)

            except Exception as e:
                logger.warning(f"Tally collection failed for {org}: {e}")

    # Provenance: attest and link
    try:
        from app.data_layer.provenance_scaling import attest_data_batch, link_batch_to_proof
        if total_proposals > 0:
            attest_data_batch("governance_proposals", [{"proposals": total_proposals, "voters": total_voters}])
            link_batch_to_proof("governance_proposals", "governance_proposals")
            link_batch_to_proof("governance_voters", "governance_voters")
    except Exception as e:
        logger.debug(f"Governance provenance failed: {e}")

    logger.info(
        f"Governance collection complete: {total_proposals} proposals, "
        f"{total_voters} voters from {spaces_processed} Snapshot spaces + Tally"
    )

    return {
        "total_proposals": total_proposals,
        "total_voters": total_voters,
        "snapshot_spaces_processed": spaces_processed,
        "tally_proposals": tally_proposals,
    }
