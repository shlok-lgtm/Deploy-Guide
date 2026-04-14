"""
Governance Proposal Corpus Collector (Pipeline 8)
====================================================
Captures every governance proposal from scored protocols at publication time.
Proposals get deleted, edited, and migrated — text must be captured immediately.

Sources: Snapshot GraphQL API (free) and Tally GraphQL API (free basic).
Runs daily in the slow cycle.  Never raises — all errors logged and skipped.
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timezone, timedelta

import httpx

from app.database import fetch_all, fetch_one, execute

logger = logging.getLogger(__name__)

SNAPSHOT_API = "https://hub.snapshot.org/graphql"
TALLY_API = "https://api.tally.xyz/query"

# Protocol → governance source mapping
PROTOCOL_GOVERNANCE_SOURCES = {
    "aave": [
        {"type": "snapshot", "space": "aave.eth"},
        {"type": "tally", "governor_id": "eip155:1:0xEC568fffba86c094cf06b22134B23074DFE2252c"},
    ],
    "compound-finance": [
        {"type": "snapshot", "space": "comp-vote.eth"},
        {"type": "tally", "governor_id": "eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529"},
    ],
    "uniswap": [
        {"type": "snapshot", "space": "uniswapgovernance.eth"},
        {"type": "tally", "governor_id": "eip155:1:0x408ED6354d4973f66138C91495F2f2FCbd8724C3"},
    ],
    "sky": [
        {"type": "snapshot", "space": "makerdao.eth"},
    ],
    "lido": [
        {"type": "snapshot", "space": "lido-snapshot.eth"},
        {"type": "snapshot", "space": "lido-vote.eth"},
    ],
    "curve-finance": [
        {"type": "snapshot", "space": "curve.eth"},
    ],
    "balancer": [
        {"type": "snapshot", "space": "balancer.eth"},
    ],
    "frax": [
        {"type": "snapshot", "space": "frax.eth"},
    ],
    "morpho": [
        {"type": "snapshot", "space": "morpho.eth"},
    ],
    "spark": [
        {"type": "snapshot", "space": "sdai.eth"},
    ],
}

SNAPSHOT_QUERY = """
query GetProposals($space: String!, $skip: Int!) {
  proposals(
    first: 100
    skip: $skip
    where: { space: $space }
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
    ipfs
    discussion
    scores
    scores_total
    quorum
    votes
    space {
      id
    }
  }
}
"""

TALLY_QUERY = """
query GetProposals($governorId: AccountID!) {
  proposals(
    chainId: "eip155:1"
    governors: [$governorId]
    pagination: { limit: 50, offset: 0 }
    sort: { sortBy: CREATED_AT, isDescending: true }
  ) {
    nodes {
      id
      title
      description
      status {
        active
        executed
        canceled
      }
      createdAt
      voteStats {
        votes
        weight
        support
        percent
      }
      proposer {
        address
        ens
      }
      start {
        timestamp
      }
      end {
        timestamp
      }
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Snapshot client
# ---------------------------------------------------------------------------

def _fetch_snapshot_proposals(space: str, protocol_slug: str) -> list[dict]:
    """Fetch proposals from Snapshot GraphQL API. Paginate up to 500."""
    all_proposals = []
    skip = 0
    max_proposals = 500

    # Check last captured timestamp for this space to limit backfill
    last_captured = fetch_one(
        """SELECT MAX(captured_at) AS latest FROM governance_proposals
           WHERE protocol_slug = %s AND proposal_source = 'snapshot'""",
        (protocol_slug,),
    )
    # On ongoing runs, only fetch recent proposals
    is_backfill = not (last_captured and last_captured.get("latest"))

    while skip < max_proposals:
        try:
            resp = httpx.post(
                SNAPSHOT_API,
                json={
                    "query": SNAPSHOT_QUERY,
                    "variables": {"space": space, "skip": skip},
                },
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            if resp.status_code == 429:
                logger.warning("Snapshot API rate limited, backing off 30s")
                time.sleep(30)
                resp = httpx.post(
                    SNAPSHOT_API,
                    json={
                        "query": SNAPSHOT_QUERY,
                        "variables": {"space": space, "skip": skip},
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
            if resp.status_code != 200:
                logger.warning(f"Snapshot returned {resp.status_code} for {space}")
                break

            data = resp.json()
            proposals = data.get("data", {}).get("proposals", [])
            if not proposals:
                break

            for p in proposals:
                # On ongoing runs, skip old proposals (24h buffer)
                if not is_backfill and last_captured.get("latest"):
                    created_ts = p.get("start", 0)
                    if created_ts:
                        created_dt = datetime.fromtimestamp(created_ts, tz=timezone.utc)
                        cutoff = last_captured["latest"] - timedelta(hours=24)
                        if cutoff.tzinfo is None:
                            cutoff = cutoff.replace(tzinfo=timezone.utc)
                        if created_dt < cutoff:
                            # Older than cutoff, stop pagination
                            return all_proposals

                scores = p.get("scores") or []
                scores_for = scores[0] if len(scores) > 0 else 0
                scores_against = scores[1] if len(scores) > 1 else 0
                scores_abstain = scores[2] if len(scores) > 2 else 0

                all_proposals.append({
                    "source": "snapshot",
                    "proposal_id": p.get("id", ""),
                    "title": p.get("title", ""),
                    "body": p.get("body", ""),
                    "author_address": (p.get("author") or "")[:42],
                    "author_ens": None,
                    "state": p.get("state", ""),
                    "vote_start": datetime.fromtimestamp(p["start"], tz=timezone.utc) if p.get("start") else None,
                    "vote_end": datetime.fromtimestamp(p["end"], tz=timezone.utc) if p.get("end") else None,
                    "scores_total": p.get("scores_total", 0),
                    "scores_for": scores_for,
                    "scores_against": scores_against,
                    "scores_abstain": scores_abstain,
                    "quorum": p.get("quorum", 0),
                    "choices": p.get("choices"),
                    "votes_count": p.get("votes", 0),
                    "ipfs_hash": p.get("ipfs", ""),
                    "discussion_url": p.get("discussion", ""),
                })

            if len(proposals) < 100:
                break
            skip += 100
            time.sleep(0.5)

        except Exception as e:
            logger.warning(f"Snapshot fetch failed for {space} at skip={skip}: {e}")
            break

    return all_proposals


# ---------------------------------------------------------------------------
# Tally client
# ---------------------------------------------------------------------------

def _fetch_tally_proposals(governor_id: str, protocol_slug: str) -> list[dict]:
    """Fetch proposals from Tally GraphQL API."""
    all_proposals = []
    try:
        resp = httpx.post(
            TALLY_API,
            json={
                "query": TALLY_QUERY,
                "variables": {"governorId": governor_id},
            },
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code == 429:
            logger.warning("Tally API rate limited, backing off 30s")
            time.sleep(30)
            resp = httpx.post(
                TALLY_API,
                json={
                    "query": TALLY_QUERY,
                    "variables": {"governorId": governor_id},
                },
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
        if resp.status_code != 200:
            logger.warning(f"Tally returned {resp.status_code} for {governor_id}")
            return all_proposals

        data = resp.json()
        nodes = data.get("data", {}).get("proposals", {}).get("nodes", [])

        for p in nodes:
            # Parse vote stats
            vote_stats = p.get("voteStats") or []
            scores_for = 0
            scores_against = 0
            scores_abstain = 0
            total_votes = 0
            for vs in vote_stats:
                support = vs.get("support", "")
                weight = float(vs.get("weight", 0) or 0)
                votes = int(vs.get("votes", 0) or 0)
                total_votes += votes
                if support == "FOR":
                    scores_for = weight
                elif support == "AGAINST":
                    scores_against = weight
                elif support == "ABSTAIN":
                    scores_abstain = weight

            # Parse state
            status = p.get("status") or {}
            if status.get("executed"):
                state = "executed"
            elif status.get("canceled"):
                state = "cancelled"
            elif status.get("active"):
                state = "active"
            else:
                state = "closed"

            proposer = p.get("proposer") or {}
            start_block = p.get("start") or {}
            end_block = p.get("end") or {}

            all_proposals.append({
                "source": "tally",
                "proposal_id": str(p.get("id", "")),
                "title": p.get("title", ""),
                "body": p.get("description", ""),
                "author_address": (proposer.get("address") or "")[:42],
                "author_ens": proposer.get("ens"),
                "state": state,
                "vote_start": datetime.fromtimestamp(int(start_block["timestamp"]), tz=timezone.utc) if start_block.get("timestamp") else None,
                "vote_end": datetime.fromtimestamp(int(end_block["timestamp"]), tz=timezone.utc) if end_block.get("timestamp") else None,
                "scores_total": scores_for + scores_against + scores_abstain,
                "scores_for": scores_for,
                "scores_against": scores_against,
                "scores_abstain": scores_abstain,
                "quorum": 0,
                "choices": None,
                "votes_count": total_votes,
                "ipfs_hash": None,
                "discussion_url": None,
            })

    except Exception as e:
        logger.warning(f"Tally fetch failed for {governor_id}: {e}")

    return all_proposals


# ---------------------------------------------------------------------------
# Upsert logic
# ---------------------------------------------------------------------------

def _upsert_proposal(proposal: dict, protocol_id: int, protocol_slug: str, results: dict):
    """Insert or update a single governance proposal."""
    body = proposal.get("body") or ""
    body_hash = "0x" + hashlib.sha256(body.encode()).hexdigest()
    source = proposal["source"]
    prop_id = proposal["proposal_id"]
    state = proposal.get("state", "")

    content_data = f"{prop_id}{source}{body_hash}{state}"
    content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

    # Build votes summary
    scores_total = float(proposal.get("scores_total") or 0)
    scores_for = float(proposal.get("scores_for") or 0)
    scores_against = float(proposal.get("scores_against") or 0)
    scores_abstain = float(proposal.get("scores_abstain") or 0)
    votes_count = int(proposal.get("votes_count") or 0)

    votes_summary = None
    if scores_total > 0:
        votes_summary = json.dumps({
            "for_pct": round(scores_for / scores_total * 100, 2),
            "against_pct": round(scores_against / scores_total * 100, 2),
            "abstain_pct": round(scores_abstain / scores_total * 100, 2),
            "voter_count": votes_count,
        })

    existing = fetch_one(
        """SELECT id, first_capture_body_hash FROM governance_proposals
           WHERE proposal_source = %s AND proposal_id = %s""",
        (source, prop_id),
    )

    if not existing:
        # New proposal — INSERT
        execute(
            """INSERT INTO governance_proposals
                (protocol_slug, protocol_id, proposal_id, proposal_source,
                 title, body, body_hash, author_address, author_ens,
                 state, vote_start, vote_end,
                 scores_total, scores_for, scores_against, scores_abstain,
                 quorum, choices, votes, ipfs_hash, discussion_url,
                 first_capture_body_hash, content_hash, attested_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
               ON CONFLICT (proposal_source, proposal_id) DO NOTHING""",
            (
                protocol_slug, protocol_id, prop_id, source,
                proposal.get("title"), body, body_hash,
                proposal.get("author_address"), proposal.get("author_ens"),
                state, proposal.get("vote_start"), proposal.get("vote_end"),
                scores_total, scores_for, scores_against, scores_abstain,
                proposal.get("quorum"),
                json.dumps(proposal.get("choices")) if proposal.get("choices") else None,
                votes_summary,
                proposal.get("ipfs_hash"), proposal.get("discussion_url"),
                body_hash, content_hash,
            ),
        )

        try:
            from app.state_attestation import attest_state
            attest_state("governance_proposals", [{
                "proposal_id": prop_id,
                "source": source,
                "protocol_slug": protocol_slug,
                "body_hash": body_hash,
            }], str(protocol_id))
        except Exception:
            pass

        results["proposals_captured"] += 1
    else:
        # Existing proposal — check for body edit, update state/scores
        first_hash = existing.get("first_capture_body_hash")
        body_changed = first_hash and body_hash != first_hash

        if body_changed:
            logger.warning(
                f"GOVERNANCE BODY EDIT DETECTED: {protocol_slug} "
                f"proposal {prop_id} (source={source})"
            )
            results["edits_detected"] += 1

        # Record snapshot for change tracking
        execute(
            """INSERT INTO governance_proposal_snapshots
                (proposal_db_id, body_hash, state, scores_total)
               VALUES (%s, %s, %s, %s)""",
            (existing["id"], body_hash, state, scores_total),
        )

        # Update current state
        execute(
            """UPDATE governance_proposals
               SET state = %s, scores_total = %s, scores_for = %s,
                   scores_against = %s, scores_abstain = %s,
                   votes = %s, body_changed = %s
               WHERE id = %s""",
            (
                state, scores_total, scores_for,
                scores_against, scores_abstain,
                votes_summary, body_changed,
                existing["id"],
            ),
        )
        results["proposals_updated"] += 1


# ---------------------------------------------------------------------------
# Main collector
# ---------------------------------------------------------------------------

async def collect_governance_proposals() -> dict:
    """
    Fetch and store governance proposals from all scored protocols.
    Returns summary dict.
    """
    results = {
        "protocols_checked": 0,
        "proposals_captured": 0,
        "proposals_updated": 0,
        "edits_detected": 0,
        "errors": [],
    }

    for protocol_slug, sources in PROTOCOL_GOVERNANCE_SOURCES.items():
        # Look up protocol_id — try psi_scores first (most protocols),
        # fall back to rpi_protocol_config
        proto_row = fetch_one(
            "SELECT DISTINCT protocol_slug FROM psi_scores WHERE protocol_slug = %s LIMIT 1",
            (protocol_slug,),
        )
        protocol_id = 0  # protocol_id is informational, not a hard FK

        for source_cfg in sources:
            try:
                source_type = source_cfg["type"]

                if source_type == "snapshot":
                    proposals = _fetch_snapshot_proposals(
                        source_cfg["space"], protocol_slug
                    )
                    time.sleep(0.5)
                elif source_type == "tally":
                    proposals = _fetch_tally_proposals(
                        source_cfg["governor_id"], protocol_slug
                    )
                    time.sleep(1)
                else:
                    continue

                for proposal in proposals:
                    try:
                        _upsert_proposal(proposal, protocol_id, protocol_slug, results)
                    except Exception as e:
                        logger.debug(f"Failed to upsert proposal: {e}")

            except Exception as e:
                error_msg = f"{protocol_slug}/{source_cfg.get('type')}: {e}"
                results["errors"].append(error_msg)
                logger.error(f"Governance proposal collection failed: {error_msg}")

        results["protocols_checked"] += 1

    logger.info(
        f"Governance proposals: protocols={results['protocols_checked']} "
        f"captured={results['proposals_captured']} "
        f"updated={results['proposals_updated']} "
        f"edits={results['edits_detected']} "
        f"errors={len(results['errors'])}"
    )
    return results
