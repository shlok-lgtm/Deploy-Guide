"""
Governance monitor — tracks Snapshot proposals and Tally on-chain governance
for target DAOs. Filters for stablecoin-relevant proposals, stores in
ops_governance_proposals and ops_target_content for analysis.
"""
import logging
import httpx
from datetime import datetime, timezone
from app.database import fetch_one, fetch_all, execute
from app.data_source_registry import register_data_source

logger = logging.getLogger(__name__)

SNAPSHOT_GRAPHQL = "https://hub.snapshot.org/graphql"

# Map targets to their Snapshot space IDs
SNAPSHOT_SPACES = {
    "Aave governance": ["aave.eth"],
    "CoW DAO": ["cow.eth"],
    "Lido Earn": ["lido-snapshot.eth"],
    "ENS governance": ["ens.eth"],
    "Arbitrum DAO": ["arbitrumfoundation.eth"],
    "Optimism Collective": ["opcollective.eth"],
    "Uniswap governance": ["uniswapgovernance.eth"],
    "Curve": ["curve.eth"],
    "MakerDAO / Sky": ["makerdao-srd.eth"],
    "Ethena": ["ethena.eth"],
}

# Tally org slugs for on-chain governance
TALLY_ORGS = {
    "Aave governance": ["aave"],
    "Compound / GFX Labs": ["compound"],
    "Arbitrum DAO": ["arbitrum"],
    "Uniswap governance": ["uniswap"],
    "ENS governance": ["ens"],
    "Optimism Collective": ["optimism"],
}

TALLY_API = "https://api.tally.xyz/query"

# Stablecoin keywords for relevance detection
STABLECOIN_KEYWORDS = [
    "stablecoin", "usdc", "usdt", "dai", "frax", "pyusd", "fdusd", "tusd",
    "usdd", "usde", "usd1", "peg", "depeg", "collateral", "reserves",
    "attestation", "backing", "mint", "redeem", "stable asset",
    "risk parameter", "risk assessment", "asset listing", "onboard",
]


async def scan_snapshot(target_id: int = None, days_back: int = 14) -> dict:
    """
    Scan Snapshot for recent proposals from target DAOs.
    If target_id given, scan only that target's spaces.
    Otherwise scan all configured targets.
    """
    if target_id:
        target = fetch_one("SELECT id, name FROM ops_targets WHERE id = %s", (target_id,))
        if not target or target["name"] not in SNAPSHOT_SPACES:
            return {"scanned": 0, "new_proposals": 0, "message": "Target has no Snapshot spaces configured"}
        targets_to_scan = {target["name"]: {"id": target["id"], "spaces": SNAPSHOT_SPACES[target["name"]]}}
    else:
        # Scan all configured targets
        targets_to_scan = {}
        for target_name, spaces in SNAPSHOT_SPACES.items():
            row = fetch_one("SELECT id FROM ops_targets WHERE name = %s", (target_name,))
            if row:
                targets_to_scan[target_name] = {"id": row["id"], "spaces": spaces}

    total_new = 0
    total_scanned = 0
    errors = []

    for target_name, info in targets_to_scan.items():
        for space_id in info["spaces"]:
            try:
                new = await _fetch_snapshot_proposals(info["id"], space_id, days_back)
                total_new += new
                total_scanned += 1
            except Exception as e:
                errors.append({"space": space_id, "error": str(e)})
                logger.error(f"Snapshot scan failed for {space_id}: {e}")

    return {
        "scanned": total_scanned,
        "new_proposals": total_new,
        "errors": errors[:5] if errors else [],
    }


async def _fetch_snapshot_proposals(target_id: int, space_id: str, days_back: int = 14) -> int:
    """Fetch proposals from a Snapshot space via GraphQL."""
    query = """
    query Proposals($space: String!, $first: Int!, $orderBy: String!) {
        proposals(
            where: { space: $space },
            first: $first,
            orderBy: $orderBy,
            orderDirection: desc
        ) {
            id
            title
            body
            state
            type
            choices
            scores
            votes
            start
            end
            author
            space { id name }
        }
    }
    """

    register_data_source("hub.snapshot.org", "/graphql", "governance_monitor",
                         method="POST", prove=False, prove_frequency="daily",
                         description="Snapshot proposals for governance monitoring",
                         notes="POST/GraphQL — TLSNotary POST support unverified")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            SNAPSHOT_GRAPHQL,
            json={
                "query": query,
                "variables": {
                    "space": space_id,
                    "first": 20,
                    "orderBy": "created",
                },
            },
        )
        resp.raise_for_status()
        data = resp.json()

    proposals = data.get("data", {}).get("proposals", [])
    if not proposals:
        return 0

    new_count = 0
    for prop in proposals:
        proposal_id = prop.get("id", "")

        # Skip if already tracked
        existing = fetch_one(
            "SELECT id FROM ops_governance_proposals WHERE platform = 'snapshot' AND proposal_id = %s",
            (proposal_id,),
        )
        if existing:
            continue

        title = prop.get("title", "")
        body = prop.get("body", "")
        state = prop.get("state", "")
        choices = prop.get("choices", [])
        scores = prop.get("scores", [])

        # Check stablecoin relevance
        text_lower = (title + " " + body).lower()
        relevant, matched = _check_stablecoin_relevance(text_lower)

        # Convert timestamps
        start_ts = datetime.fromtimestamp(prop["start"], tz=timezone.utc) if prop.get("start") else None
        end_ts = datetime.fromtimestamp(prop["end"], tz=timezone.utc) if prop.get("end") else None

        execute(
            """INSERT INTO ops_governance_proposals
               (target_id, platform, proposal_id, space_or_org, title, body, state,
                vote_type, choices, scores, votes_count, start_at, end_at, author,
                stablecoin_relevant, relevant_coins, relevance_notes)
               VALUES (%s, 'snapshot', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (platform, proposal_id) DO NOTHING""",
            (
                target_id, proposal_id, space_id, title, body[:10000], state,
                prop.get("type"), choices, [float(s) for s in scores] if scores else None,
                prop.get("votes", 0), start_ts, end_ts, prop.get("author"),
                relevant, matched if matched else None,
                f"Matched keywords: {', '.join(matched)}" if matched else None,
            ),
        )

        # If stablecoin-relevant, also store in ops_target_content for analysis pipeline
        if relevant:
            source_url = f"https://snapshot.org/#/{space_id}/proposal/{proposal_id}"
            execute(
                """INSERT INTO ops_target_content
                   (target_id, source_url, source_type, title, content, scraped_at)
                   VALUES (%s, %s, 'snapshot_vote', %s, %s, %s)
                   ON CONFLICT (source_url) DO NOTHING""",
                (target_id, source_url, title, body[:10000], datetime.now(timezone.utc)),
            )

        new_count += 1

    return new_count


async def scan_tally(target_id: int = None) -> dict:
    """
    Scan Tally for recent on-chain governance proposals.
    Tally API requires an API key — if not available, falls back to
    Parallel Search for governance proposal discovery.
    """
    import os
    tally_key = os.getenv("TALLY_API_KEY")

    if target_id:
        target = fetch_one("SELECT id, name FROM ops_targets WHERE id = %s", (target_id,))
        if not target or target["name"] not in TALLY_ORGS:
            return {"scanned": 0, "new_proposals": 0, "message": "Target has no Tally orgs configured"}
        targets_to_scan = {target["name"]: {"id": target["id"], "orgs": TALLY_ORGS[target["name"]]}}
    else:
        targets_to_scan = {}
        for target_name, orgs in TALLY_ORGS.items():
            row = fetch_one("SELECT id FROM ops_targets WHERE name = %s", (target_name,))
            if row:
                targets_to_scan[target_name] = {"id": row["id"], "orgs": orgs}

    if not tally_key:
        # Fallback: use Parallel Search to find governance proposals
        return await _scan_tally_via_search(targets_to_scan)

    total_new = 0
    total_scanned = 0
    errors = []

    for target_name, info in targets_to_scan.items():
        for org_slug in info["orgs"]:
            try:
                new = await _fetch_tally_proposals(info["id"], org_slug, tally_key)
                total_new += new
                total_scanned += 1
            except Exception as e:
                errors.append({"org": org_slug, "error": str(e)})
                logger.error(f"Tally scan failed for {org_slug}: {e}")

    return {"scanned": total_scanned, "new_proposals": total_new, "errors": errors[:5]}


async def _fetch_tally_proposals(target_id: int, org_slug: str, api_key: str) -> int:
    """Fetch proposals from Tally GraphQL API."""
    query = """
    query GovernorProposals($slug: String!) {
        organization(slug: $slug) {
            governorIds
            name
        }
        proposals(
            organizationSlug: $slug,
            first: 20,
            sort: { field: START_BLOCK, order: DESC }
        ) {
            nodes {
                id
                onchainId
                title
                description
                status
                voteStats {
                    type
                    votesCount
                    votersCount
                    percent
                }
                governor { name }
                start { timestamp }
                end { timestamp }
                proposer { address }
            }
        }
    }
    """

    register_data_source("api.tally.xyz", "/query", "governance_monitor",
                         method="POST", prove=False, prove_frequency="daily",
                         description="Tally on-chain proposals for governance monitoring",
                         notes="POST/GraphQL — TLSNotary POST support unverified")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            TALLY_API,
            headers={"Api-Key": api_key, "Content-Type": "application/json"},
            json={"query": query, "variables": {"slug": org_slug}},
        )
        resp.raise_for_status()
        data = resp.json()

    proposals = data.get("data", {}).get("proposals", {}).get("nodes", [])
    new_count = 0

    for prop in proposals:
        proposal_id = prop.get("id", "")

        existing = fetch_one(
            "SELECT id FROM ops_governance_proposals WHERE platform = 'tally' AND proposal_id = %s",
            (proposal_id,),
        )
        if existing:
            continue

        title = prop.get("title", "")
        description = prop.get("description", "")
        status = prop.get("status", "")

        text_lower = (title + " " + description).lower()
        relevant, matched = _check_stablecoin_relevance(text_lower)

        start_ts = None
        end_ts = None
        if prop.get("start", {}).get("timestamp"):
            start_ts = datetime.fromisoformat(prop["start"]["timestamp"].replace("Z", "+00:00"))
        if prop.get("end", {}).get("timestamp"):
            end_ts = datetime.fromisoformat(prop["end"]["timestamp"].replace("Z", "+00:00"))

        vote_stats = prop.get("voteStats", [])
        scores = [vs.get("percent", 0) for vs in vote_stats] if vote_stats else None
        votes_count = sum(vs.get("votersCount", 0) for vs in vote_stats) if vote_stats else 0

        execute(
            """INSERT INTO ops_governance_proposals
               (target_id, platform, proposal_id, space_or_org, title, body, state,
                scores, votes_count, start_at, end_at, author,
                stablecoin_relevant, relevant_coins, relevance_notes)
               VALUES (%s, 'tally', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (platform, proposal_id) DO NOTHING""",
            (
                target_id, proposal_id, org_slug, title, description[:10000], status,
                scores, votes_count, start_ts, end_ts,
                prop.get("proposer", {}).get("address"),
                relevant, matched if matched else None,
                f"Matched keywords: {', '.join(matched)}" if matched else None,
            ),
        )

        if relevant:
            source_url = f"https://www.tally.xyz/gov/{org_slug}/proposal/{prop.get('onchainId', proposal_id)}"
            execute(
                """INSERT INTO ops_target_content
                   (target_id, source_url, source_type, title, content, scraped_at)
                   VALUES (%s, %s, 'governance_proposal', %s, %s, %s)
                   ON CONFLICT (source_url) DO NOTHING""",
                (target_id, source_url, title, description[:10000], datetime.now(timezone.utc)),
            )

        new_count += 1

    return new_count


async def _scan_tally_via_search(targets_to_scan: dict) -> dict:
    """Fallback: use Parallel Search to find governance proposals when no Tally API key."""
    from app.services import parallel_client

    total_new = 0
    total_scanned = 0

    for target_name, info in targets_to_scan.items():
        for org_slug in info["orgs"]:
            try:
                query = f"site:tally.xyz/gov/{org_slug} proposal stablecoin OR collateral OR USDC OR USDT OR DAI"
                result = await parallel_client.search(query, num_results=10)

                results_data = result.get("results", result.get("search_results", []))
                for item in results_data if isinstance(results_data, list) else []:
                    if isinstance(item, dict):
                        url = item.get("url") or item.get("link", "")
                        title = item.get("title", "")
                        snippet = item.get("snippet") or item.get("excerpt", "")

                        if "tally.xyz" not in url.lower():
                            continue

                        existing = fetch_one("SELECT id FROM ops_target_content WHERE source_url = %s", (url,))
                        if existing:
                            continue

                        execute(
                            """INSERT INTO ops_target_content
                               (target_id, source_url, source_type, title, content, scraped_at)
                               VALUES (%s, %s, 'governance_proposal', %s, %s, %s)
                               ON CONFLICT (source_url) DO NOTHING""",
                            (info["id"], url, title, snippet, datetime.now(timezone.utc)),
                        )
                        total_new += 1

                total_scanned += 1
            except Exception as e:
                logger.error(f"Tally search fallback failed for {org_slug}: {e}")

    return {
        "scanned": total_scanned,
        "new_proposals": total_new,
        "method": "parallel_search_fallback",
    }


async def scan_all_governance(target_id: int = None, days_back: int = 14) -> dict:
    """Run both Snapshot and Tally scans, return combined results."""
    snapshot_result = await scan_snapshot(target_id=target_id, days_back=days_back)
    tally_result = await scan_tally(target_id=target_id)

    return {
        "snapshot": snapshot_result,
        "tally": tally_result,
        "total_new": snapshot_result.get("new_proposals", 0) + tally_result.get("new_proposals", 0),
    }


def get_recent_proposals(limit: int = 30, stablecoin_only: bool = False, target_id: int = None) -> list:
    """Get recent governance proposals."""
    conditions = []
    params = []

    if stablecoin_only:
        conditions.append("g.stablecoin_relevant = TRUE")
    if target_id:
        conditions.append("g.target_id = %s")
        params.append(target_id)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    params.append(limit)

    return fetch_all(
        f"""SELECT g.*, t.name as target_name
            FROM ops_governance_proposals g
            JOIN ops_targets t ON g.target_id = t.id
            {where}
            ORDER BY g.fetched_at DESC LIMIT %s""",
        params,
    ) or []


def _check_stablecoin_relevance(text: str) -> tuple:
    """Check if text contains stablecoin-relevant keywords. Returns (relevant, matched_keywords)."""
    matched = [kw for kw in STABLECOIN_KEYWORDS if kw in text]
    return (len(matched) > 0, matched)
