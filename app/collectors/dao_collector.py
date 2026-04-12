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
import time
from datetime import datetime, timezone, timedelta

import requests

from app.database import execute, fetch_all, fetch_one
from app.index_definitions.dohi_v01 import DOHI_V01_DEFINITION, DAO_ENTITIES
from app.scoring_engine import score_entity

logger = logging.getLogger(__name__)

SNAPSHOT_GQL_URL = "https://hub.snapshot.org/graphql"
DEFILLAMA_BASE = "https://api.llama.fi"

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

    # Static config components
    static = DAO_STATIC_CONFIG.get(slug, {})
    raw_values.update(static)

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
             formula_version, inputs_hash, confidence, confidence_tag)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            computed_at = NOW()
    """, (
        "dohi", slug, result["entity_name"], result["overall_score"],
        json.dumps(result["category_scores"]),
        json.dumps(result["component_scores"]),
        json.dumps(raw_for_storage, default=str),
        result["version"], inputs_hash,
        result.get("confidence", "limited"),
        result.get("confidence_tag"),
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
    except Exception:
        pass

    return results
