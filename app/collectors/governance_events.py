"""
Governance Event Collector — RPI Delta
=======================================
Ingests governance events from Snapshot GraphQL and Tally REST APIs.
Stores timestamped proposals with contributor attribution tags.

Serves two purposes:
1. Attribution queries — correlate governance events with PSI score trajectories
2. DOHI governance activity components — proposal frequency, voter participation

Event types:
  parameter_change, vendor_engagement, vendor_departure, security_incident,
  upgrade_proposal, treasury_proposal, emergency_action

This is NOT a new index. It enriches existing PSI scored state with
governance event metadata for service provider attribution queries.
"""

import logging
import time
import re
from datetime import datetime, timezone, timedelta

import requests

from app.database import execute, fetch_all, fetch_one

logger = logging.getLogger(__name__)

# =============================================================================
# Contributor keyword mapping — simple keyword-in-text tagging
# =============================================================================

CONTRIBUTOR_KEYWORDS = {
    "chaos labs": "chaos-labs",
    "chaos-labs": "chaos-labs",
    "chaoslabs": "chaos-labs",
    "gauntlet": "gauntlet",
    "bgd labs": "bgd-labs",
    "bgd-labs": "bgd-labs",
    "aci": "aci",
    "aave chan initiative": "aci",
    "aave-chan": "aci",
    "llama risk": "llama-risk",
    "llamarisk": "llama-risk",
    "steakhouse": "steakhouse-financial",
    "steakhouse financial": "steakhouse-financial",
    "karpatkey": "karpatkey",
    "tokenlogic": "tokenlogic",
    "warden finance": "warden-finance",
    "aave companies": "aave-companies",
    "flipside": "flipside",
    "messari": "messari",
    "deco": "deco",
    "block analitica": "block-analitica",
    "phoenix labs": "phoenix-labs",
    "ba labs": "ba-labs",
    "risk dao": "risk-dao",
}

# =============================================================================
# Snapshot GraphQL API
# =============================================================================

SNAPSHOT_GQL_URL = "https://hub.snapshot.org/graphql"

# Protocol slug -> Snapshot space ID (shared with psi_collector.SNAPSHOT_SPACES)
SNAPSHOT_SPACES = {
    "aave": "aavedao.eth",
    "lido": "lido-snapshot.eth",
    "compound-finance": "comp-vote.eth",
    "uniswap": "uniswapgovernance.eth",
    "curve-finance": "curve.eth",
    "convex-finance": "cvx.eth",
    "arbitrum-dao": "arbitrumfoundation.eth",
    "optimism": "opcollective.eth",
    "ens-dao": "ens.eth",
    "gitcoin-dao": "gitcoindao.eth",
    "safe-dao": "safe.eth",
    "balancer": "balancer.eth",
    "sushi": "sushigov.eth",
    "gmx": "gmx.eth",
}


def _tag_contributor(title: str, body: str) -> str | None:
    """Extract contributor tag from proposal title/body via keyword matching."""
    text = f"{title or ''} {body or ''}".lower()
    for keyword, tag in CONTRIBUTOR_KEYWORDS.items():
        if keyword in text:
            return tag
    return None


def _classify_event_type(title: str, body: str) -> str:
    """Classify governance event type from proposal text."""
    text = f"{title or ''} {body or ''}".lower()
    if any(kw in text for kw in ["parameter", "risk param", "interest rate", "ltv", "collateral factor"]):
        return "parameter_change"
    if any(kw in text for kw in ["vendor", "service provider", "engagement", "onboard"]):
        return "vendor_engagement"
    if any(kw in text for kw in ["offboard", "sunset", "terminate", "departure"]):
        return "vendor_departure"
    if any(kw in text for kw in ["security", "incident", "exploit", "emergency", "pause"]):
        return "security_incident"
    if any(kw in text for kw in ["upgrade", "migration", "v2", "v3", "v4", "implementation"]):
        return "upgrade_proposal"
    if any(kw in text for kw in ["treasury", "budget", "funding", "grant", "compensation"]):
        return "treasury_proposal"
    if any(kw in text for kw in ["emergency", "guardian", "fast-track"]):
        return "emergency_action"
    return "governance_proposal"


def fetch_snapshot_events(space_id: str, since_days: int = 90) -> list[dict]:
    """Fetch proposals from Snapshot GraphQL API for a given space."""
    since_ts = int((datetime.now(timezone.utc) - timedelta(days=since_days)).timestamp())

    query = """
    query($space: String!, $created_gte: Int!) {
      proposals(
        first: 1000,
        where: {space: $space, created_gte: $created_gte},
        orderBy: "created",
        orderDirection: desc
      ) {
        id
        title
        body
        state
        created
        end
        choices
        scores_total
        votes
        author
      }
    }
    """
    try:
        resp = requests.post(
            SNAPSHOT_GQL_URL,
            json={
                "query": query,
                "variables": {"space": space_id, "created_gte": since_ts},
            },
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("data", {}).get("proposals", [])
    except Exception as e:
        logger.warning(f"Snapshot fetch failed for {space_id}: {e}")
    return []


def _map_snapshot_state(state: str) -> str:
    """Map Snapshot proposal state to our outcome format."""
    mapping = {
        "closed": "executed",
        "active": "active",
        "pending": "pending",
    }
    return mapping.get(state, state)


# =============================================================================
# Tally REST API
# =============================================================================

TALLY_API_BASE = "https://api.tally.xyz/query"

# Protocol slug -> Tally organization slug
TALLY_ORGS = {
    "aave": "aave",
    "compound-finance": "compound",
    "uniswap": "uniswap",
    "arbitrum-dao": "arbitrum",
    "optimism": "optimism",
    "ens-dao": "ens",
}


def fetch_tally_events(org_slug: str, since_days: int = 90) -> list[dict]:
    """Fetch on-chain governance proposals from Tally API."""
    since_ts = int((datetime.now(timezone.utc) - timedelta(days=since_days)).timestamp())

    # Tally GraphQL query for proposals
    query = """
    query Proposals($input: ProposalsInput!) {
      proposals(input: $input) {
        nodes {
          id
          title
          description
          statusChanges {
            type
            txHash
          }
          block {
            timestamp
          }
          voteStats {
            votes
            weight
            support
          }
          governor {
            name
          }
        }
      }
    }
    """
    try:
        resp = requests.post(
            TALLY_API_BASE,
            json={
                "query": query,
                "variables": {
                    "input": {
                        "filters": {"organizationId": org_slug},
                        "page": {"limit": 100},
                        "sort": {"sortBy": "id", "isDescending": True},
                    }
                },
            },
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            nodes = data.get("data", {}).get("proposals", {}).get("nodes", [])
            # Filter to proposals within the time window
            filtered = []
            for node in nodes:
                block = node.get("block", {})
                ts = block.get("timestamp") if block else None
                if ts:
                    try:
                        proposal_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        if proposal_time.timestamp() >= since_ts:
                            filtered.append(node)
                    except (ValueError, TypeError):
                        filtered.append(node)
                else:
                    filtered.append(node)
            return filtered
    except Exception as e:
        logger.debug(f"Tally fetch failed for {org_slug}: {e}")
    return []


def _map_tally_outcome(status_changes: list) -> str:
    """Determine outcome from Tally status changes."""
    if not status_changes:
        return "unknown"
    types = [s.get("type", "").lower() for s in status_changes]
    if "executed" in types:
        return "executed"
    if "defeated" in types or "failed" in types:
        return "defeated"
    if "canceled" in types or "cancelled" in types:
        return "cancelled"
    if "queued" in types:
        return "passed"
    return "active"


# =============================================================================
# Storage
# =============================================================================

def store_governance_event(event: dict) -> bool:
    """Store a single governance event. Returns True if new row inserted."""
    try:
        # Check for duplicate via source + source_id
        existing = fetch_one(
            "SELECT id FROM governance_events WHERE source = %s AND source_id = %s",
            (event["source"], event["source_id"]),
        )
        if existing:
            return False

        execute("""
            INSERT INTO governance_events
                (protocol_slug, event_type, event_timestamp, title, description,
                 outcome, contributor_tag, source, source_id, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            event["protocol_slug"],
            event["event_type"],
            event["event_timestamp"],
            event.get("title"),
            event.get("description"),
            event.get("outcome"),
            event.get("contributor_tag"),
            event["source"],
            event["source_id"],
            event.get("metadata"),
        ))
        return True
    except Exception as e:
        logger.warning(f"Failed to store governance event: {e}")
        return False


# =============================================================================
# Main collection functions
# =============================================================================

def collect_snapshot_events(protocol_slug: str, space_id: str, since_days: int = 90) -> list[dict]:
    """Collect and store Snapshot governance events for one protocol."""
    proposals = fetch_snapshot_events(space_id, since_days)
    events = []

    for p in proposals:
        title = p.get("title", "")
        body = (p.get("body") or "")[:2000]  # truncate for storage
        created = p.get("created", 0)

        event = {
            "protocol_slug": protocol_slug,
            "event_type": _classify_event_type(title, body),
            "event_timestamp": datetime.fromtimestamp(created, tz=timezone.utc).isoformat(),
            "title": title[:500],
            "description": body[:1000],
            "outcome": _map_snapshot_state(p.get("state", "")),
            "contributor_tag": _tag_contributor(title, body),
            "source": "snapshot",
            "source_id": p.get("id", ""),
            "metadata": None,  # Skip full body to keep DB lean
        }
        events.append(event)

    return events


def collect_tally_events(protocol_slug: str, org_slug: str, since_days: int = 90) -> list[dict]:
    """Collect and store Tally on-chain governance events for one protocol."""
    proposals = fetch_tally_events(org_slug, since_days)
    events = []

    for p in proposals:
        title = p.get("title", "")
        desc = (p.get("description") or "")[:2000]
        block = p.get("block", {})
        ts = block.get("timestamp") if block else None

        if ts:
            try:
                event_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                event_time = datetime.now(timezone.utc)
        else:
            event_time = datetime.now(timezone.utc)

        status_changes = p.get("statusChanges", [])

        event = {
            "protocol_slug": protocol_slug,
            "event_type": _classify_event_type(title, desc),
            "event_timestamp": event_time.isoformat(),
            "title": title[:500],
            "description": desc[:1000],
            "outcome": _map_tally_outcome(status_changes),
            "contributor_tag": _tag_contributor(title, desc),
            "source": "tally",
            "source_id": str(p.get("id", "")),
            "metadata": None,
        }
        events.append(event)

    return events


def run_governance_event_collection() -> dict:
    """
    Collect governance events from all configured protocols.
    Returns summary dict.
    """
    total_new = 0
    total_skipped = 0
    protocols_processed = []

    # Snapshot events
    for slug, space_id in SNAPSHOT_SPACES.items():
        try:
            events = collect_snapshot_events(slug, space_id)
            new = 0
            for ev in events:
                if store_governance_event(ev):
                    new += 1
            total_new += new
            total_skipped += len(events) - new
            protocols_processed.append(slug)
            logger.info(f"Governance events ({slug}/snapshot): {new} new, {len(events) - new} existing")
            time.sleep(0.5)  # rate limit Snapshot
        except Exception as e:
            logger.warning(f"Snapshot collection failed for {slug}: {e}")

    # Tally events
    for slug, org_slug in TALLY_ORGS.items():
        try:
            events = collect_tally_events(slug, org_slug)
            new = 0
            for ev in events:
                if store_governance_event(ev):
                    new += 1
            total_new += new
            total_skipped += len(events) - new
            if slug not in protocols_processed:
                protocols_processed.append(slug)
            logger.info(f"Governance events ({slug}/tally): {new} new, {len(events) - new} existing")
            time.sleep(0.5)  # rate limit Tally
        except Exception as e:
            logger.warning(f"Tally collection failed for {slug}: {e}")

    # Attest governance events
    try:
        from app.state_attestation import attest_state
        attest_state("governance_events", [
            {"protocol": slug, "new_events": total_new}
            for slug in protocols_processed
        ])
    except Exception:
        pass

    return {
        "protocols_processed": len(protocols_processed),
        "new_events": total_new,
        "skipped_duplicates": total_skipped,
    }


# =============================================================================
# Attribution query helpers
# =============================================================================

def get_attribution_by_protocol(protocol_slug: str, period_days: int = 90) -> dict:
    """
    Get PSI score trajectory overlaid with governance events for a protocol.
    Returns timeline of scores + events for the period.
    """
    # PSI score trajectory
    scores = fetch_all("""
        SELECT overall_score, scored_date, computed_at
        FROM psi_scores
        WHERE protocol_slug = %s AND scored_date >= CURRENT_DATE - %s
        ORDER BY scored_date ASC
    """, (protocol_slug, period_days))

    # Governance events in the period
    events = fetch_all("""
        SELECT event_type, event_timestamp, title, outcome, contributor_tag, source
        FROM governance_events
        WHERE protocol_slug = %s
          AND event_timestamp >= NOW() - INTERVAL '%s days'
        ORDER BY event_timestamp ASC
    """, (protocol_slug, period_days))

    # Per-contributor score deltas: PSI at event time vs PSI 30 days later
    contributor_deltas = {}
    for ev in events:
        tag = ev.get("contributor_tag")
        if not tag:
            continue

        ev_time = ev["event_timestamp"]
        # Find PSI score closest to event time
        score_at_event = fetch_one("""
            SELECT overall_score FROM psi_scores
            WHERE protocol_slug = %s AND scored_date <= %s::date
            ORDER BY scored_date DESC LIMIT 1
        """, (protocol_slug, ev_time))

        # Find PSI score 30 days after event
        score_after = fetch_one("""
            SELECT overall_score FROM psi_scores
            WHERE protocol_slug = %s AND scored_date <= (%s::date + 30)
            ORDER BY scored_date DESC LIMIT 1
        """, (protocol_slug, ev_time))

        if score_at_event and score_after:
            delta = float(score_after["overall_score"]) - float(score_at_event["overall_score"])
            contributor_deltas.setdefault(tag, []).append({
                "event_title": ev.get("title"),
                "event_date": str(ev_time),
                "psi_at_event": float(score_at_event["overall_score"]),
                "psi_after_30d": float(score_after["overall_score"]),
                "delta": round(delta, 2),
            })

    return {
        "protocol": protocol_slug,
        "period_days": period_days,
        "score_trajectory": [
            {"date": str(s["scored_date"]), "score": float(s["overall_score"])}
            for s in scores
        ],
        "events": [
            {
                "type": e["event_type"],
                "timestamp": str(e["event_timestamp"]),
                "title": e["title"],
                "outcome": e["outcome"],
                "contributor": e["contributor_tag"],
                "source": e["source"],
            }
            for e in events
        ],
        "contributor_deltas": contributor_deltas,
    }


def get_attribution_by_contributor(contributor_tag: str) -> dict:
    """
    Get all protocols a contributor has been involved with and
    average PSI trajectories after their engagement/departure events.
    """
    events = fetch_all("""
        SELECT protocol_slug, event_type, event_timestamp, title, outcome
        FROM governance_events
        WHERE contributor_tag = %s
        ORDER BY event_timestamp DESC
    """, (contributor_tag,))

    protocols = {}
    for ev in events:
        slug = ev["protocol_slug"]
        protocols.setdefault(slug, []).append({
            "type": ev["event_type"],
            "timestamp": str(ev["event_timestamp"]),
            "title": ev["title"],
            "outcome": ev["outcome"],
        })

    # Average PSI trajectory after engagement events
    engagement_deltas = []
    departure_deltas = []

    for ev in events:
        slug = ev["protocol_slug"]
        ev_time = ev["event_timestamp"]

        score_at = fetch_one("""
            SELECT overall_score FROM psi_scores
            WHERE protocol_slug = %s AND scored_date <= %s::date
            ORDER BY scored_date DESC LIMIT 1
        """, (slug, ev_time))

        score_after = fetch_one("""
            SELECT overall_score FROM psi_scores
            WHERE protocol_slug = %s AND scored_date <= (%s::date + 30)
            ORDER BY scored_date DESC LIMIT 1
        """, (slug, ev_time))

        if score_at and score_after:
            delta = float(score_after["overall_score"]) - float(score_at["overall_score"])
            if ev["event_type"] == "vendor_departure":
                departure_deltas.append(delta)
            else:
                engagement_deltas.append(delta)

    return {
        "contributor": contributor_tag,
        "protocols": protocols,
        "total_events": len(events),
        "avg_psi_delta_after_engagement": round(
            sum(engagement_deltas) / len(engagement_deltas), 2
        ) if engagement_deltas else None,
        "avg_psi_delta_after_departure": round(
            sum(departure_deltas) / len(departure_deltas), 2
        ) if departure_deltas else None,
    }
