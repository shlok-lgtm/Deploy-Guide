"""
DAO Operational Health Index Collector
=======================================
Extends PSI's governance category into a standalone surface.
Imports PSI governance component values where available and adds
DAO-specific signals from Snapshot/Tally.

Data sources:
- Snapshot GraphQL API: proposals, votes, spaces
- Tally API: on-chain governance, delegates, voting power
- DeFiLlama: treasury data (already integrated)
- PSI governance components: imported for entities with both scores

The Snapshot/Tally data collected here serves BOTH DOHI scoring
AND governance event tagging (Prompt 7 / governance_events.py).
"""

import json
import hashlib
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta

import requests

from app.database import execute, fetch_all, fetch_one
from app.index_definitions.dohi_v01 import DOHI_V01_DEFINITION, DAO_ENTITIES
from app.scoring_engine import score_entity
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

SNAPSHOT_GQL_URL = "https://hub.snapshot.org/graphql"
DEFILLAMA_BASE = "https://api.llama.fi"
ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"

# =============================================================================
# Static config for manually assessed components
# =============================================================================

DAO_STATIC_CONFIG = {
    "aave-dao": {
        "active_contributor_count": 40, "key_personnel_diversity": 80,
        "legal_entity_status": 70, "multisig_config": 85,
        "treasury_runway_months": 36,
        "dao_timelock_hours": 48, "emergency_capability": 80,
        "guardian_authority": 85, "dao_upgrade_mechanism": 85, "dao_audit_cadence": 80,
        "public_reporting_frequency": 75, "financial_disclosure": 70,
        "compensation_transparency": 65, "meeting_cadence": 70,
    },
    "lido-dao": {
        "active_contributor_count": 30, "key_personnel_diversity": 75,
        "legal_entity_status": 65, "multisig_config": 80,
        "treasury_runway_months": 24,
        "dao_timelock_hours": 24, "emergency_capability": 75,
        "guardian_authority": 75, "dao_upgrade_mechanism": 70, "dao_audit_cadence": 75,
        "public_reporting_frequency": 70, "financial_disclosure": 65,
        "compensation_transparency": 60, "meeting_cadence": 65,
    },
    "compound-dao": {
        "active_contributor_count": 15, "key_personnel_diversity": 65,
        "legal_entity_status": 60, "multisig_config": 80,
        "treasury_runway_months": 48,
        "dao_timelock_hours": 48, "emergency_capability": 70,
        "guardian_authority": 70, "dao_upgrade_mechanism": 80, "dao_audit_cadence": 70,
        "public_reporting_frequency": 60, "financial_disclosure": 55,
        "compensation_transparency": 50, "meeting_cadence": 55,
    },
    "uniswap-dao": {
        "active_contributor_count": 20, "key_personnel_diversity": 70,
        "legal_entity_status": 70, "multisig_config": 75,
        "treasury_runway_months": 60,
        "dao_timelock_hours": 48, "emergency_capability": 65,
        "guardian_authority": 65, "dao_upgrade_mechanism": 75, "dao_audit_cadence": 65,
        "public_reporting_frequency": 60, "financial_disclosure": 50,
        "compensation_transparency": 50, "meeting_cadence": 50,
    },
    "arbitrum-dao": {
        "active_contributor_count": 25, "key_personnel_diversity": 70,
        "legal_entity_status": 80, "multisig_config": 80,
        "treasury_runway_months": 48,
        "dao_timelock_hours": 72, "emergency_capability": 75,
        "guardian_authority": 80, "dao_upgrade_mechanism": 75, "dao_audit_cadence": 70,
        "public_reporting_frequency": 70, "financial_disclosure": 65,
        "compensation_transparency": 65, "meeting_cadence": 65,
    },
}


# =============================================================================
# Snapshot data collection
# =============================================================================

def fetch_snapshot_governance_data(space_id: str) -> dict:
    """Fetch governance activity data from Snapshot for DOHI scoring."""
    raw = {}
    since_ts = int((datetime.now(timezone.utc) - timedelta(days=90)).timestamp())

    # Fetch proposals with vote data
    query = """
    query($space: String!, $created_gte: Int!) {
      proposals(
        first: 1000,
        where: {space: $space, created_gte: $created_gte},
        orderBy: "created",
        orderDirection: desc
      ) {
        id
        state
        scores_total
        votes
        quorum
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
            proposals = data.get("data", {}).get("proposals", [])

            raw["proposal_frequency_90d"] = len(proposals)

            if proposals:
                # Voter participation (average votes per proposal)
                total_votes = sum(p.get("votes", 0) for p in proposals)
                raw["voter_participation_rate"] = total_votes / len(proposals) if proposals else 0

                # Quorum achievement rate
                quorum_met = sum(
                    1 for p in proposals
                    if p.get("quorum") and p.get("scores_total", 0) >= p["quorum"]
                )
                raw["quorum_achievement_rate"] = (quorum_met / len(proposals)) * 100 if proposals else 0

                # Proposal pass rate
                closed = [p for p in proposals if p.get("state") == "closed"]
                if closed:
                    # Approximate: a proposal "passed" if it closed (Snapshot doesn't have explicit pass/fail)
                    raw["proposal_pass_rate"] = 75.0  # Conservative default

    except Exception as e:
        logger.debug(f"Snapshot governance data failed for {space_id}: {e}")

    time.sleep(0.5)

    # Fetch voter concentration (top voters across recent proposals)
    try:
        voter_query = """
        query($space: String!, $created_gte: Int!) {
          votes(
            first: 1000,
            where: {space: $space, created_gte: $created_gte},
            orderBy: "vp",
            orderDirection: desc
          ) {
            voter
            vp
          }
        }
        """
        resp = requests.post(
            SNAPSHOT_GQL_URL,
            json={
                "query": voter_query,
                "variables": {"space": space_id, "created_gte": since_ts},
            },
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            votes = data.get("data", {}).get("votes", [])

            if votes:
                # Aggregate voting power by voter
                voter_vp = {}
                for v in votes:
                    voter = v.get("voter", "")
                    vp = v.get("vp", 0)
                    voter_vp[voter] = voter_vp.get(voter, 0) + vp

                sorted_voters = sorted(voter_vp.values(), reverse=True)
                total_vp = sum(sorted_voters)

                if total_vp > 0:
                    # Top 10 voter share
                    top10_vp = sum(sorted_voters[:10])
                    raw["top10_voter_share"] = (top10_vp / total_vp) * 100

                    # Voting power Gini coefficient (simplified)
                    n = len(sorted_voters)
                    if n > 1:
                        cumulative = sum((2 * (i + 1) - n - 1) * sorted_voters[i] for i in range(n))
                        raw["voting_power_gini"] = cumulative / (n * total_vp) if total_vp > 0 else 0.5

                    # Delegate count approximation (unique voters)
                    raw["delegate_count"] = len(voter_vp)

    except Exception as e:
        logger.debug(f"Snapshot voter data failed for {space_id}: {e}")

    time.sleep(0.5)
    return raw


# =============================================================================
# DeFiLlama treasury data
# =============================================================================

def fetch_dao_treasury(protocol_slug: str) -> dict:
    """Fetch treasury data for a DAO from DeFiLlama."""
    raw = {}
    if not protocol_slug:
        return raw

    try:
        time.sleep(1)
        resp = requests.get(f"{DEFILLAMA_BASE}/treasury/{protocol_slug}", timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            chain_tvls = data.get("chainTvls", {})
            total = 0
            stablecoin_total = 0

            for chain_name, chain_data in chain_tvls.items():
                if isinstance(chain_data, dict):
                    tvl_list = chain_data.get("tvl", [])
                    if tvl_list:
                        last = tvl_list[-1]
                        if isinstance(last, dict):
                            total += last.get("totalLiquidityUSD", 0)

                    # Token breakdown for diversification
                    tokens_list = chain_data.get("tokens", [])
                    if tokens_list:
                        latest_tokens = tokens_list[-1].get("tokens", {}) if tokens_list else {}
                        for token_name, usd_value in latest_tokens.items():
                            if isinstance(usd_value, (int, float)):
                                sym = token_name.upper()
                                if any(s in sym for s in ["USDC", "USDT", "DAI", "FRAX", "USD"]):
                                    stablecoin_total += usd_value

            if total > 0:
                raw["treasury_size_usd"] = total
                raw["treasury_diversification"] = (stablecoin_total / total) * 100
    except Exception as e:
        logger.debug(f"Treasury fetch failed for {protocol_slug}: {e}")
    return raw


# =============================================================================
# PSI governance component import
# =============================================================================

def import_psi_governance_components(protocol_slug: str) -> dict:
    """Import existing PSI governance component values for this entity."""
    raw = {}
    if not protocol_slug:
        return raw

    try:
        row = fetch_one("""
            SELECT raw_values FROM psi_scores
            WHERE protocol_slug = %s
            ORDER BY computed_at DESC LIMIT 1
        """, (protocol_slug,))
        if row and row.get("raw_values"):
            psi_raw = json.loads(row["raw_values"]) if isinstance(row["raw_values"], str) else row["raw_values"]
            # Import governance-relevant PSI values
            if "governance_token_holders" in psi_raw:
                raw["delegate_count"] = raw.get("delegate_count") or psi_raw["governance_token_holders"]
            if "governance_proposals_90d" in psi_raw:
                raw["proposal_frequency_90d"] = raw.get("proposal_frequency_90d") or psi_raw["governance_proposals_90d"]
    except Exception as e:
        logger.debug(f"PSI governance import failed for {protocol_slug}: {e}")
    return raw


# =============================================================================
# Phase 1: Live data automation for static components
# =============================================================================

def _automate_dao_treasury_runway(entity: dict, static: dict) -> dict:
    """Compute treasury_runway_months from DeFiLlama treasury + fees data."""
    automated = {}
    protocol_slug = entity.get("protocol_slug")
    if not protocol_slug:
        return automated

    try:
        from app.collectors.defillama import fetch_defillama_treasury, fetch_defillama_fees

        treasury = fetch_defillama_treasury(protocol_slug)
        fees = fetch_defillama_fees(protocol_slug)

        total_usd = treasury.get("total_usd", 0)
        monthly_fees = (fees.get("total_30d_fees", 0) or 0)

        if total_usd > 0 and monthly_fees > 0:
            # Runway = treasury / monthly expenses
            # Rough assumption: DAO spends ~50-100% of fees on operations
            monthly_expenses = monthly_fees * 0.75  # conservative estimate
            if monthly_expenses > 0:
                runway_months = total_usd / monthly_expenses
                static_runway = static.get("treasury_runway_months", 12)
                automated["treasury_runway_months"] = max(runway_months, static_runway)
                logger.info(f"DAO treasury runway {entity['slug']}: {runway_months:.1f} months")
    except Exception as e:
        logger.warning(f"DAO treasury runway automation failed for {entity['slug']}: {e}")

    return automated


def _automate_dao_active_contributors(entity: dict, static: dict, snapshot_data: dict) -> dict:
    """Derive active_contributor_count from Snapshot/Tally governance data.

    Count unique proposal authors + unique voters with >3 votes in 90 days.
    """
    automated = {}
    space_id = entity.get("snapshot_space")
    if not space_id:
        return automated

    since_ts = int((datetime.now(timezone.utc) - timedelta(days=90)).timestamp())

    try:
        # Fetch proposal authors
        query = """
        query($space: String!, $created_gte: Int!) {
          proposals(
            first: 1000,
            where: {space: $space, created_gte: $created_gte}
          ) {
            author
          }
        }
        """
        resp = requests.post(
            SNAPSHOT_GQL_URL,
            json={"query": query, "variables": {"space": space_id, "created_gte": since_ts}},
            timeout=15,
        )
        unique_authors = set()
        if resp.status_code == 200:
            proposals = resp.json().get("data", {}).get("proposals", [])
            unique_authors = set(p.get("author", "") for p in proposals if p.get("author"))

        time.sleep(0.5)

        # Count voters with >3 votes (active participants)
        voter_query = """
        query($space: String!, $created_gte: Int!) {
          votes(
            first: 1000,
            where: {space: $space, created_gte: $created_gte}
          ) {
            voter
          }
        }
        """
        resp2 = requests.post(
            SNAPSHOT_GQL_URL,
            json={"query": voter_query, "variables": {"space": space_id, "created_gte": since_ts}},
            timeout=15,
        )
        active_voters = set()
        if resp2.status_code == 200:
            votes = resp2.json().get("data", {}).get("votes", [])
            voter_counts: dict[str, int] = {}
            for v in votes:
                voter = v.get("voter", "")
                voter_counts[voter] = voter_counts.get(voter, 0) + 1
            active_voters = set(v for v, c in voter_counts.items() if c >= 3)

        contributor_count = len(unique_authors | active_voters)
        if contributor_count > 0:
            static_count = static.get("active_contributor_count", 0)
            automated["active_contributor_count"] = max(contributor_count, static_count)
            logger.info(f"DAO active contributors {entity['slug']}: {contributor_count}")

        time.sleep(0.5)
    except Exception as e:
        logger.warning(f"DAO contributor count failed for {entity['slug']}: {e}")

    return automated


def _automate_dao_multisig(entity: dict, static: dict) -> dict:
    """Read Safe multisig config from Etherscan contract reads.

    getOwners() returns signer count, getThreshold() returns threshold.
    Score = (threshold/signers) * 100 normalized.
    """
    automated = {}
    multisig_contract = entity.get("multisig_contract")
    if not multisig_contract:
        return automated

    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        return automated

    try:
        # Read getOwners() - returns array of addresses
        time.sleep(0.15)
        # Safe ABI: getOwners() returns address[]
        # getOwners selector: 0xa0e67e2b
        resp = requests.get(ETHERSCAN_V2_BASE, params={
            "chainid": 1,
            "module": "proxy",
            "action": "eth_call",
            "to": multisig_contract,
            "data": "0xa0e67e2b",
            "tag": "latest",
            "apikey": api_key,
        }, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            result_hex = data.get("result", "0x")
            if result_hex and len(result_hex) > 66:
                # Parse ABI-encoded array: count 20-byte addresses
                # Skip first 64 chars (offset) + 64 chars (length)
                hex_data = result_hex[2:]  # strip 0x
                if len(hex_data) >= 128:
                    count_hex = hex_data[64:128]
                    owner_count = int(count_hex, 16)

                    # Read getThreshold() — selector: 0xe75235b8
                    time.sleep(0.15)
                    resp2 = requests.get(ETHERSCAN_V2_BASE, params={
                        "chainid": 1,
                        "module": "proxy",
                        "action": "eth_call",
                        "to": multisig_contract,
                        "data": "0xe75235b8",
                        "tag": "latest",
                        "apikey": api_key,
                    }, timeout=15)
                    if resp2.status_code == 200:
                        data2 = resp2.json()
                        threshold_hex = data2.get("result", "0x0")
                        threshold = int(threshold_hex, 16) if threshold_hex else 0

                        if owner_count > 0 and threshold > 0:
                            # Score: threshold/owners ratio normalized to 0-100
                            # 1/1 = 100, 3/5 = 60, 2/5 = 40, etc.
                            # But also factor in total signers: more = better
                            ratio_score = (threshold / owner_count) * 100
                            # Bonus for more signers: +5 per signer beyond 3
                            signer_bonus = min(20, max(0, (owner_count - 3) * 5))
                            config_score = min(100, ratio_score + signer_bonus)

                            static_config = static.get("multisig_config", 50)
                            automated["multisig_config"] = max(config_score, static_config)
                            logger.info(
                                f"DAO multisig {entity['slug']}: {threshold}/{owner_count} "
                                f"= {config_score:.0f}"
                            )
    except Exception as e:
        logger.debug(f"DAO multisig read failed for {entity['slug']}: {e}")

    return automated


def _automate_dao_timelock(entity: dict, static: dict) -> dict:
    """Read Timelock delay from Etherscan contract call.

    delay() returns seconds. Convert to hours.
    """
    automated = {}
    timelock_contract = entity.get("timelock_contract")
    if not timelock_contract:
        return automated

    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        return automated

    try:
        time.sleep(0.15)
        # Common timelock selectors: delay() = 0x6a42b8f8, getMinDelay() = 0xf3e73875
        for selector in ["0x6a42b8f8", "0xf3e73875"]:
            resp = requests.get(ETHERSCAN_V2_BASE, params={
                "chainid": 1,
                "module": "proxy",
                "action": "eth_call",
                "to": timelock_contract,
                "data": selector,
                "tag": "latest",
                "apikey": api_key,
            }, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                result_hex = data.get("result", "0x0")
                if result_hex and result_hex != "0x" and len(result_hex) > 2:
                    delay_seconds = int(result_hex, 16)
                    if delay_seconds > 0:
                        delay_hours = delay_seconds / 3600
                        static_hours = static.get("dao_timelock_hours", 0)
                        automated["dao_timelock_hours"] = max(delay_hours, static_hours)
                        logger.info(f"DAO timelock {entity['slug']}: {delay_hours:.1f}h")
                        break
            time.sleep(0.15)
    except Exception as e:
        logger.debug(f"DAO timelock read failed for {entity['slug']}: {e}")

    return automated


def _automate_dao_transparency(entity: dict, static: dict) -> dict:
    """Automate financial_disclosure and public_reporting_frequency from forum data.

    Uses existing governance_forum_posts table data (populated by RPI forum scraper).
    """
    automated = {}
    protocol_slug = entity.get("protocol_slug")
    if not protocol_slug:
        return automated

    try:
        # Count forum posts with financial report keywords in last 365 days
        report_keywords = ['financial report', 'treasury report', 'quarterly',
                           'monthly update', 'financial update', 'budget report',
                           'spending report', 'treasury update']

        # Check governance_forum_posts table for report-like posts
        row = fetch_one("""
            SELECT COUNT(*) AS report_count
            FROM governance_forum_posts
            WHERE protocol_slug = %s
              AND collected_at >= NOW() - INTERVAL '365 days'
              AND (
                  LOWER(title) SIMILAR TO %s
                  OR LOWER(raw_text) SIMILAR TO %s
              )
        """, (
            protocol_slug,
            '%(' + '|'.join(report_keywords) + ')%',
            '%(' + '|'.join(report_keywords) + ')%',
        ))

        if row and row.get("report_count"):
            report_count = int(row["report_count"])

            # financial_disclosure: 0-100 based on report frequency
            # 0 reports = 20, 1-2 = 40, 3-6 = 60, 7-12 = 80, 12+ = 90
            if report_count >= 12:
                fd_score = 90
            elif report_count >= 7:
                fd_score = 80
            elif report_count >= 3:
                fd_score = 60
            elif report_count >= 1:
                fd_score = 40
            else:
                fd_score = 20

            static_fd = static.get("financial_disclosure", 20)
            automated["financial_disclosure"] = max(fd_score, static_fd)

            # public_reporting_frequency: reports per quarter
            # 0 per Q = 20, 1 = 40, 2 = 60, 3+ = 80
            per_quarter = report_count / 4  # ~4 quarters in a year
            if per_quarter >= 3:
                prf_score = 80
            elif per_quarter >= 2:
                prf_score = 60
            elif per_quarter >= 1:
                prf_score = 40
            else:
                prf_score = 20

            static_prf = static.get("public_reporting_frequency", 20)
            automated["public_reporting_frequency"] = max(prf_score, static_prf)
    except Exception as e:
        logger.debug(f"DAO transparency automation failed for {entity['slug']}: {e}")

    return automated


# =============================================================================
# Phase 3D: DOHI Audit Cadence Scoring
# Scrapes audit aggregator pages to count audits and check recency
# =============================================================================

# Audit docs/security page URL patterns per protocol
DAO_AUDIT_PAGES = {
    "aave-dao": [
        "https://docs.aave.com/developers/guides/security",
        "https://github.com/aave/aave-v3-core/tree/master/audits",
    ],
    "lido-dao": [
        "https://docs.lido.fi/security/audits",
        "https://github.com/lidofinance/audits",
    ],
    "compound-dao": [
        "https://docs.compound.finance/security",
        "https://github.com/compound-finance/compound-protocol/tree/master/audits",
    ],
    "uniswap-dao": [
        "https://docs.uniswap.org/contracts/v3/reference/deployments",
        "https://github.com/Uniswap/v3-core/tree/main/audits",
    ],
    "arbitrum-dao": [
        "https://docs.arbitrum.io/audit-reports",
    ],
}

# Known auditor names for detection
KNOWN_AUDITORS = [
    "openzeppelin", "trail of bits", "certora", "consensys diligence",
    "sigma prime", "spearbit", "sherlock", "code4rena", "cantina",
    "quantstamp", "halborn", "ottersec", "mixbytes", "chainsecurity",
    "peckshield", "slowmist", "dedaub", "statemind",
]

# Cache audit scoring (30 day TTL — audits change very slowly)
_audit_cache: dict[str, tuple[float, dict]] = {}
_AUDIT_CACHE_TTL = 2592000  # 30 days


def _automate_dao_audit_cadence(entity: dict, static: dict) -> dict:
    """Score dao_audit_cadence from audit aggregator page scraping.

    Fetches protocol security/audit docs pages, counts unique audit mentions,
    and checks recency. Score = count_score * 0.5 + recency_score * 0.5.

    Cached for 30 days.
    """
    automated = {}
    slug = entity["slug"]

    # Check cache
    cached = _audit_cache.get(slug)
    if cached and (time.time() - cached[0]) < _AUDIT_CACHE_TTL:
        return cached[1]

    audit_urls = DAO_AUDIT_PAGES.get(slug, [])
    if not audit_urls:
        return automated

    # Also check if there's a docs URL from the protocol itself
    protocol_slug = entity.get("protocol_slug")
    if protocol_slug:
        # Try standard patterns
        audit_urls = audit_urls + [
            f"https://docs.{protocol_slug.replace('-', '')}.com/security/audits",
        ]

    found_auditors = set()
    found_years = set()

    # Try Parallel Search first — finds audit reports across the web
    try:
        import asyncio
        from app.services import parallel_client

        protocol_name = entity.get("name", slug)
        search_query = f"{protocol_name} smart contract security audit report"

        async def _search():
            return await parallel_client.search(search_query, num_results=5)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, _search())
                search_result = future.result(timeout=40)
        else:
            search_result = asyncio.run(_search())

        if search_result and "error" not in search_result:
            results_list = search_result.get("results", [])
            for sr in results_list:
                snippet = (sr.get("snippet") or sr.get("title") or "").lower()
                for auditor in KNOWN_AUDITORS:
                    if auditor in snippet:
                        found_auditors.add(auditor)
                year_matches = re.findall(r'20(?:2[0-6]|1[0-9])', snippet)
                for y in year_matches:
                    found_years.add(int(y))
            if found_auditors:
                logger.info(f"DAO audit Parallel Search {slug}: found {found_auditors}")
    except Exception as e:
        logger.debug(f"DAO audit Parallel Search failed for {slug}: {e}")

    # Also check known audit pages directly (supplements search results)
    for url in audit_urls:
        try:
            time.sleep(1)
            resp = requests.get(url, timeout=15, allow_redirects=True)
            if resp.status_code != 200:
                continue

            text = resp.text.lower()

            # Detect auditors mentioned
            for auditor in KNOWN_AUDITORS:
                if auditor in text:
                    found_auditors.add(auditor)

            # Detect years (audit dates)
            year_matches = re.findall(r'20(?:2[0-6]|1[0-9])', text)
            for y in year_matches:
                found_years.add(int(y))

        except Exception as e:
            logger.debug(f"DAO audit page fetch failed for {url}: {e}")
            continue

    if not found_auditors:
        _audit_cache[slug] = (time.time(), automated)
        return automated

    audit_count = len(found_auditors)

    # Count score: 0 audits = 0, 1 = 40, 2-3 = 70, 4+ = 90
    if audit_count == 0:
        count_score = 0
    elif audit_count == 1:
        count_score = 40
    elif audit_count <= 3:
        count_score = 70
    else:
        count_score = 90

    # Recency score: most recent audit year
    current_year = datetime.now().year
    if found_years:
        most_recent_year = max(found_years)
        years_ago = current_year - most_recent_year
        if years_ago <= 0:
            recency_score = 90
        elif years_ago <= 1:
            recency_score = 70
        elif years_ago <= 2:
            recency_score = 50
        else:
            recency_score = 20
    else:
        recency_score = 30  # auditors found but no dates

    cadence_score = (count_score * 0.5 + recency_score * 0.5)
    static_cadence = static.get("dao_audit_cadence", 0)
    automated["dao_audit_cadence"] = max(cadence_score, static_cadence)

    logger.info(
        f"DAO audit cadence {slug}: {audit_count} auditors "
        f"({', '.join(sorted(found_auditors)[:3])}), "
        f"years={sorted(found_years)[-3:] if found_years else '?'} → {cadence_score:.0f}"
    )

    _audit_cache[slug] = (time.time(), automated)
    return automated


# =============================================================================
# Score and store
# =============================================================================

def score_dao(entity: dict) -> dict | None:
    """Score a single DAO entity."""
    slug = entity["slug"]
    logger.info(f"Scoring DAO: {slug}")

    raw_values = {}

    # Import PSI governance components first
    protocol_slug = entity.get("protocol_slug")
    if protocol_slug:
        psi_data = import_psi_governance_components(protocol_slug)
        raw_values.update(psi_data)

    # Snapshot governance data
    space_id = entity.get("snapshot_space")
    if space_id:
        snapshot_data = fetch_snapshot_governance_data(space_id)
        # Snapshot data overrides PSI imports (fresher data)
        raw_values.update(snapshot_data)

    # Treasury data
    if protocol_slug:
        treasury_data = fetch_dao_treasury(protocol_slug)
        raw_values.update(treasury_data)

    # Static config components (applied first, then overridden by live data)
    static = DAO_STATIC_CONFIG.get(slug, {})
    raw_values.update(static)

    # --- Phase 1 automation: replace static with live data ---
    # Treasury runway from DeFiLlama treasury + fees
    runway_automated = _automate_dao_treasury_runway(entity, static)
    raw_values.update(runway_automated)

    # Active contributor count from Snapshot
    contributor_automated = _automate_dao_active_contributors(entity, static, raw_values)
    raw_values.update(contributor_automated)

    # Multisig config from on-chain Safe reads
    multisig_automated = _automate_dao_multisig(entity, static)
    raw_values.update(multisig_automated)

    # Timelock hours from on-chain timelock contract
    timelock_automated = _automate_dao_timelock(entity, static)
    raw_values.update(timelock_automated)

    # Financial disclosure + reporting frequency from forum data
    transparency_automated = _automate_dao_transparency(entity, static)
    raw_values.update(transparency_automated)

    # --- Phase 3D: Audit cadence from aggregator scraping ---
    audit_automated = _automate_dao_audit_cadence(entity, static)
    raw_values.update(audit_automated)

    if not raw_values:
        logger.warning(f"No data collected for DAO {slug}")
        return None

    result = score_entity(DOHI_V01_DEFINITION, raw_values)
    result["entity_slug"] = slug
    result["entity_name"] = entity["name"]
    result["raw_values"] = raw_values

    return result


def store_dao_score(result: dict) -> None:
    """Store a DAO score in the generic_index_scores table."""
    slug = result["entity_slug"]
    raw_for_storage = {k: v for k, v in result["raw_values"].items() if not k.startswith("_")}
    raw_canonical = json.dumps(raw_for_storage, sort_keys=True, default=str)
    inputs_hash = "0x" + hashlib.sha256(raw_canonical.encode()).hexdigest()

    execute("""
        INSERT INTO generic_index_scores
            (index_id, entity_slug, entity_name, overall_score,
             category_scores, component_scores, raw_values,
             formula_version, inputs_hash, confidence, confidence_tag,
             component_coverage, components_populated, components_total, missing_categories)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (index_id, entity_slug, scored_date)
        DO UPDATE SET
            entity_name = EXCLUDED.entity_name,
            overall_score = EXCLUDED.overall_score,
            category_scores = EXCLUDED.category_scores,
            component_scores = EXCLUDED.component_scores,
            raw_values = EXCLUDED.raw_values,
            inputs_hash = EXCLUDED.inputs_hash,
            confidence = EXCLUDED.confidence,
            confidence_tag = EXCLUDED.confidence_tag,
            component_coverage = EXCLUDED.component_coverage,
            components_populated = EXCLUDED.components_populated,
            components_total = EXCLUDED.components_total,
            missing_categories = EXCLUDED.missing_categories,
            computed_at = NOW()
    """, (
        "dohi", slug, result["entity_name"], result["overall_score"],
        json.dumps(result["category_scores"]),
        json.dumps(result["component_scores"]),
        json.dumps(raw_for_storage, default=str),
        result["version"], inputs_hash,
        result.get("confidence", "limited"),
        result.get("confidence_tag"),
        result.get("component_coverage"),
        result.get("components_populated"),
        result.get("components_total"),
        json.dumps(result.get("missing_categories") or []),
    ))


def run_dohi_scoring() -> list[dict]:
    """Score all DAO entities. Called from worker."""
    results = []
    for entity in DAO_ENTITIES:
        try:
            result = score_dao(entity)
            if result:
                store_dao_score(result)
                results.append(result)
                logger.info(
                    f"  {result['entity_name']}: {result['overall_score']} "
                    f"({result['components_available']}/{result['components_total']} components)"
                )
        except Exception as e:
            logger.warning(f"DOHI scoring failed for {entity['slug']}: {e}")

    # Attest DOHI scores
    try:
        from app.state_attestation import attest_state
        if results:
            attest_state("dohi_components", [
                {"slug": r["entity_slug"], "score": r["overall_score"]}
                for r in results
            ])
    except Exception as e:
        logger.warning(f"DOHI attestation failed: {e}")

    return results
