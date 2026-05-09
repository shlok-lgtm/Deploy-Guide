"""
RPI Tally Collector
====================
Scrapes on-chain governance proposals from Tally GraphQL API.
Covers protocols with on-chain governance (Compound, Uniswap, Aave).
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import requests

from app.database import execute

logger = logging.getLogger(__name__)

TALLY_API = "https://api.tally.xyz/query"

# Tally organization IDs for protocols with on-chain governance
# These are Tally's internal slugs for their governance pages
TALLY_ORGS = {
    "compound-finance": "compound",
    "uniswap": "uniswap",
    "aave": "aave",
}

# Risk-related keywords (same as snapshot collector)
RISK_KEYWORDS = [
    "risk", "security", "audit", "vulnerability", "exploit", "hack",
    "incident", "parameter", "collateral", "liquidation", "oracle",
    "vendor", "gauntlet", "chaos", "llamarisk", "immunefi",
    "bug bounty", "insurance", "cap", "threshold", "borrow rate",
    "supply cap", "debt ceiling", "ltv", "bad debt", "recovery",
    "compensation", "budget", "risk manager",
]


def _classify_risk(title: str, description: str) -> tuple[bool, list[str]]:
    text = f"{title} {description}".lower()
    matched = [kw for kw in RISK_KEYWORDS if kw in text]
    return len(matched) > 0, matched


def fetch_tally_proposals(org_slug: str, since_days: int = 90) -> list[dict]:
    """Fetch proposals from Tally for a given organization."""
    if not org_slug:
        return []

    # Tally's API requires an API key for some operations, but the public
    # proposals query works without authentication for basic fields.
    query = """
    query GovernanceProposals($input: ProposalsInput!) {
      proposals(input: $input) {
        nodes {
          id
          title
          description
          statusChanges {
            type
          }
          voteStats {
            type
            votesCount
            votersCount
            percent
          }
          createdAt
          block {
            number
            timestamp
          }
        }
      }
    }
    """

    variables = {
        "input": {
            "governorSlugs": [org_slug],
            "sort": {"sortBy": "BLOCK_NUMBER", "isDescending": True},
            "page": {"limit": 50},
        }
    }

    time.sleep(1)  # rate limit
    try:
        resp = requests.post(
            TALLY_API,
            json={"query": query, "variables": variables},
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            nodes = data.get("data", {}).get("proposals", {}).get("nodes", [])
            return nodes
        else:
            logger.debug(f"Tally returned {resp.status_code} for {org_slug}")
    except Exception as e:
        logger.warning(f"Tally fetch failed for {org_slug}: {e}")
    return []


def collect_tally_proposals():
    """Collect on-chain governance proposals from Tally for RPI protocols."""
    total_stored = 0

    for protocol_slug, tally_org in TALLY_ORGS.items():
        proposals = fetch_tally_proposals(tally_org)
        logger.info(f"RPI Tally: {protocol_slug} ({tally_org}) — {len(proposals)} proposals")

        for prop in proposals:
            title = prop.get("title", "")
            description = prop.get("description", "")
            body_excerpt = description[:500] if description else ""

            is_risk, risk_kws = _classify_risk(title, body_excerpt)

            # Extract vote stats
            vote_for = None
            vote_against = None
            vote_abstain = None
            total_votes = 0
            for vs in prop.get("voteStats", []):
                vtype = vs.get("type", "").upper()
                count = vs.get("votesCount", 0) or 0
                if "FOR" in vtype:
                    vote_for = float(count)
                elif "AGAINST" in vtype:
                    vote_against = float(count)
                elif "ABSTAIN" in vtype:
                    vote_abstain = float(count)
                total_votes += int(count)

            # Get latest status
            status_changes = prop.get("statusChanges", [])
            state = status_changes[-1].get("type") if status_changes else None

            # Block timestamp for created_at
            block_ts = prop.get("block", {}).get("timestamp")
            created_at = None
            if block_ts:
                try:
                    created_at = datetime.fromisoformat(str(block_ts).replace("Z", "+00:00"))
                except asyncio.CancelledError:
                    raise
                except (ValueError, TypeError) as e:
                    logger.warning(f"Tally block timestamp parse failed: {e}")
                    try:
                        from app.worker import _record_cycle_error
                        _record_cycle_error(
                            error_type="rpi_collect_tally_proposals_block_ts_parse_failure",
                            error_message=str(e)[:500],
                            cycle_phase="rpi_tally_collector",
                        )
                    except Exception:
                        pass
            if not created_at and prop.get("createdAt"):
                try:
                    created_at = datetime.fromisoformat(str(prop["createdAt"]).replace("Z", "+00:00"))
                except asyncio.CancelledError:
                    raise
                except (ValueError, TypeError) as e:
                    logger.warning(f"Tally createdAt parse failed: {e}")
                    try:
                        from app.worker import _record_cycle_error
                        _record_cycle_error(
                            error_type="rpi_collect_tally_proposals_created_at_parse_failure",
                            error_message=str(e)[:500],
                            cycle_phase="rpi_tally_collector",
                        )
                    except Exception:
                        pass

            proposal_id = str(prop.get("id", ""))

            try:
                execute("""
                    INSERT INTO governance_proposals
                        (protocol_slug, proposal_id, source, title, body_excerpt,
                         is_risk_related, risk_keywords, budget_amount_usd,
                         vote_for, vote_against, vote_abstain,
                         quorum_reached, participation_rate,
                         proposal_state, created_at, scraped_at)
                    VALUES (%s, %s, 'tally', %s, %s,
                            %s, %s, NULL,
                            %s, %s, %s,
                            NULL, NULL,
                            %s, %s, NOW())
                    ON CONFLICT (protocol_slug, proposal_id, source) DO UPDATE SET
                        is_risk_related = EXCLUDED.is_risk_related,
                        risk_keywords = EXCLUDED.risk_keywords,
                        vote_for = EXCLUDED.vote_for,
                        vote_against = EXCLUDED.vote_against,
                        vote_abstain = EXCLUDED.vote_abstain,
                        proposal_state = EXCLUDED.proposal_state,
                        scraped_at = NOW()
                """, (
                    protocol_slug, proposal_id, title, body_excerpt,
                    is_risk, risk_kws,
                    vote_for, vote_against, vote_abstain,
                    state, created_at,
                ))
                total_stored += 1
            except Exception as e:
                logger.warning(f"Failed to store Tally proposal {proposal_id} for {protocol_slug}: {e}")

    logger.info(f"RPI Tally collector: {total_stored} proposals stored/updated")
    return total_stored
