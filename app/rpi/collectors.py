"""
RPI Data Collectors
====================
Collects governance proposals, parameter changes, and revenue data
for Risk Posture Index scoring.

Collectors:
  - Snapshot: governance proposals via GraphQL
  - Tally: on-chain governance proposals via GraphQL
  - Parameter tracker: on-chain parameter changes via Etherscan
  - Revenue: protocol revenue from DeFiLlama
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta

import requests

from app.database import execute, fetch_all, fetch_one

logger = logging.getLogger(__name__)

ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "")

# =============================================================================
# Snapshot Space IDs for each protocol
# =============================================================================

SNAPSHOT_SPACES = {
    "aave": "aave.eth",
    "lido": "lido-snapshot.eth",
    "eigenlayer": "eigenlayer.eth",
    "sky": "sky.eth",
    "compound-finance": "comp-vote.eth",
    "uniswap": "uniswapgovernance.eth",
    "curve-finance": "curve.eth",
    "morpho": "morpho.eth",
    "spark": "spark.eth",
    "convex-finance": "cvx.eth",
    "drift": "driftgov.eth",
    "jupiter-perpetual-exchange": "jup.eth",
    "raydium": "raydium.eth",
}

# =============================================================================
# Tally organization IDs (on-chain governance)
# =============================================================================

TALLY_ORGS = {
    "compound-finance": "compound",
    "uniswap": "uniswap",
    "aave": "aave",
}

TALLY_API_URL = "https://api.tally.xyz/query"
TALLY_API_KEY = os.environ.get("TALLY_API_KEY", "")

# =============================================================================
# Risk-related keywords for proposal classification
# =============================================================================

RISK_KEYWORDS = [
    "risk", "audit", "security", "vulnerability", "exploit", "incident",
    "parameter", "collateral", "liquidation", "oracle", "interest rate",
    "ltv", "loan-to-value", "reserve factor", "borrow cap", "supply cap",
    "debt ceiling", "spending", "budget", "compensation", "grant",
    "vendor", "service provider", "risk manager", "gauntlet", "chaos",
    "llamarisk", "openzeppelin", "certora", "trail of bits",
    "insurance", "bad debt", "recovery", "remediation",
    "safety module", "slashing", "penalty",
]

# Budget extraction patterns
_BUDGET_PATTERNS = [
    re.compile(r"\$\s*([\d,.]+)\s*[Mm](?:illion)?", re.IGNORECASE),
    re.compile(r"([\d,.]+)\s*[Mm](?:illion)?\s*(?:USD|USDC|USDT|DAI)", re.IGNORECASE),
    re.compile(r"\$\s*([\d,.]+)\s*[Kk]", re.IGNORECASE),
    re.compile(r"([\d,.]+)\s*(?:USD|USDC|USDT|DAI)", re.IGNORECASE),
]

# =============================================================================
# Protocol governance/admin contracts for parameter tracking
# =============================================================================

PROTOCOL_CONTRACTS = {
    "aave": {
        "chain": "ethereum",
        "contracts": [
            {
                "name": "PoolConfigurator",
                "address": "0x64b761D848206f447Fe2dd461b0c635Ec39EbB27",
                "functions": [
                    "setReserveBorrowing",
                    "configureReserveAsCollateral",
                    "setReserveFactor",
                    "setBorrowCap",
                    "setSupplyCap",
                    "setReserveFreeze",
                    "setReservePause",
                    "setLiquidationProtocolFee",
                    "setEModeCategory",
                    "setAssetEModeCategory",
                    "setDebtCeiling",
                ],
            },
        ],
    },
    "compound-finance": {
        "chain": "ethereum",
        "contracts": [
            {
                "name": "Configurator",
                "address": "0x316f9708bB98af7dA9c68C1C3b5e79039cD336E3",
                "functions": [
                    "setBaseBorrowMin",
                    "setBorrowPerYearInterestRateBase",
                    "setBorrowPerYearInterestRateSlopeLow",
                    "setBorrowPerYearInterestRateSlopeHigh",
                    "setSupplyPerYearInterestRateBase",
                    "setStoreFrontPriceFactor",
                    "setBaseTrackingBorrowSpeed",
                    "setBaseTrackingSupplySpeed",
                ],
            },
        ],
    },
    "uniswap": {
        "chain": "ethereum",
        "contracts": [
            {
                "name": "GovernorBravo",
                "address": "0x408ED6354d4973f66138C91495F2f2FCbd8724C3",
                "functions": ["execute"],
            },
        ],
    },
}


def _classify_risk_proposal(title: str, body: str) -> tuple[bool, list[str]]:
    """Check if a proposal is risk-related. Returns (is_risk, matched_keywords)."""
    text = f"{title} {body}".lower()
    matched = [kw for kw in RISK_KEYWORDS if kw.lower() in text]
    return (len(matched) >= 1, matched)


def _extract_budget(title: str, body: str) -> tuple[float | None, str | None]:
    """Extract budget amount from proposal text. Returns (amount_usd, currency)."""
    text = f"{title} {body}"
    for pattern in _BUDGET_PATTERNS:
        match = pattern.search(text)
        if match:
            raw = match.group(1).replace(",", "")
            try:
                amount = float(raw)
                # Check if the pattern was for millions or thousands
                if "m" in match.group(0).lower() and "million" not in match.group(0).lower():
                    amount *= 1_000_000
                elif "million" in match.group(0).lower():
                    amount *= 1_000_000
                elif "k" in match.group(0).lower():
                    amount *= 1_000
                return (amount, "USD")
            except ValueError:
                continue
    return (None, None)


# =============================================================================
# Snapshot Collector
# =============================================================================

SNAPSHOT_GRAPHQL = "https://hub.snapshot.org/graphql"


def collect_snapshot_proposals(protocol_slug: str, days: int = 180) -> list[dict]:
    """Scrape governance proposals from Snapshot for a protocol."""
    space = SNAPSHOT_SPACES.get(protocol_slug)
    if not space:
        logger.debug(f"No Snapshot space mapping for {protocol_slug}")
        return []

    since_ts = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

    query = """
    query Proposals($space: String!, $created_gte: Int!) {
      proposals(
        where: { space: $space, created_gte: $created_gte }
        orderBy: "created"
        orderDirection: desc
        first: 100
      ) {
        id
        title
        body
        state
        scores_total
        scores
        choices
        votes
        quorum
        created
        start
        end
      }
    }
    """

    try:
        resp = requests.post(
            SNAPSHOT_GRAPHQL,
            json={
                "query": query,
                "variables": {"space": space, "created_gte": since_ts},
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        proposals = data.get("data", {}).get("proposals", [])
    except Exception as e:
        logger.warning(f"Snapshot fetch failed for {protocol_slug} ({space}): {e}")
        return []

    stored = []
    for p in proposals:
        title = p.get("title", "")
        body = p.get("body", "")[:5000]  # truncate long bodies
        is_risk, keywords = _classify_risk_proposal(title, body)
        budget_amount, budget_currency = _extract_budget(title, body)

        scores = p.get("scores") or []
        choices = p.get("choices") or []
        votes_for = scores[0] if len(scores) > 0 else 0
        votes_against = scores[1] if len(scores) > 1 else 0
        votes_abstain = scores[2] if len(scores) > 2 else 0
        scores_total = p.get("scores_total", 0) or 0
        quorum = p.get("quorum", 0) or 0

        participation_rate = None
        quorum_reached = None
        if quorum > 0:
            quorum_reached = scores_total >= quorum
            participation_rate = min(scores_total / quorum * 100, 100) if quorum > 0 else None

        record = {
            "protocol_slug": protocol_slug,
            "proposal_id": p["id"],
            "source": "snapshot",
            "title": title,
            "body": body,
            "state": p.get("state", ""),
            "is_risk_related": is_risk,
            "risk_keywords": keywords,
            "budget_amount": budget_amount,
            "budget_currency": budget_currency,
            "votes_for": votes_for,
            "votes_against": votes_against,
            "votes_abstain": votes_abstain,
            "voter_count": p.get("votes", 0) or 0,
            "participation_rate": participation_rate,
            "quorum_reached": quorum_reached,
            "created_at": datetime.fromtimestamp(p.get("created", 0), tz=timezone.utc).isoformat() if p.get("created") else None,
            "start_at": datetime.fromtimestamp(p.get("start", 0), tz=timezone.utc).isoformat() if p.get("start") else None,
            "end_at": datetime.fromtimestamp(p.get("end", 0), tz=timezone.utc).isoformat() if p.get("end") else None,
        }

        try:
            execute("""
                INSERT INTO governance_proposals (
                    protocol_slug, proposal_id, source, title, body, state,
                    is_risk_related, risk_keywords, budget_amount, budget_currency,
                    votes_for, votes_against, votes_abstain, voter_count,
                    participation_rate, quorum_reached,
                    created_at, start_at, end_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (protocol_slug, proposal_id, source) DO UPDATE SET
                    state = EXCLUDED.state,
                    votes_for = EXCLUDED.votes_for,
                    votes_against = EXCLUDED.votes_against,
                    votes_abstain = EXCLUDED.votes_abstain,
                    voter_count = EXCLUDED.voter_count,
                    participation_rate = EXCLUDED.participation_rate,
                    quorum_reached = EXCLUDED.quorum_reached,
                    scraped_at = NOW()
            """, (
                record["protocol_slug"], record["proposal_id"], record["source"],
                record["title"], record["body"], record["state"],
                record["is_risk_related"], record["risk_keywords"],
                record["budget_amount"], record["budget_currency"],
                record["votes_for"], record["votes_against"], record["votes_abstain"],
                record["voter_count"], record["participation_rate"], record["quorum_reached"],
                record["created_at"], record["start_at"], record["end_at"],
            ))
            stored.append(record)
        except Exception as e:
            logger.debug(f"Failed to store proposal {p.get('id')}: {e}")

    logger.info(f"Snapshot: {protocol_slug} — {len(stored)} proposals stored ({sum(1 for s in stored if s['is_risk_related'])} risk-related)")
    return stored


# =============================================================================
# Tally Collector (on-chain governance)
# =============================================================================

def collect_tally_proposals(protocol_slug: str, days: int = 180) -> list[dict]:
    """Scrape on-chain governance proposals from Tally."""
    org = TALLY_ORGS.get(protocol_slug)
    if not org:
        return []

    if not TALLY_API_KEY:
        logger.debug("TALLY_API_KEY not set — skipping Tally collection")
        return []

    after_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    query = """
    query Proposals($input: ProposalsInput!) {
      proposals(input: $input) {
        nodes {
          id
          title
          description
          statusChanges { type }
          voteStats {
            type
            votesCount
            votersCount
            percent
          }
          createdAt
          start { timestamp }
          end { timestamp }
        }
      }
    }
    """

    headers = {"Api-Key": TALLY_API_KEY}

    try:
        resp = requests.post(
            TALLY_API_URL,
            json={
                "query": query,
                "variables": {
                    "input": {
                        "organizationSlug": org,
                        "sort": {"sortBy": "CREATED_AT", "isDescending": True},
                        "filters": {"afterDate": after_date},
                        "page": {"limit": 50},
                    }
                },
            },
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        proposals = data.get("data", {}).get("proposals", {}).get("nodes", [])
    except Exception as e:
        logger.warning(f"Tally fetch failed for {protocol_slug} ({org}): {e}")
        return []

    stored = []
    for p in proposals:
        title = p.get("title", "")
        body = (p.get("description") or "")[:5000]
        is_risk, keywords = _classify_risk_proposal(title, body)
        budget_amount, budget_currency = _extract_budget(title, body)

        vote_stats = p.get("voteStats") or []
        votes_for = 0
        votes_against = 0
        votes_abstain = 0
        voter_count = 0
        for vs in vote_stats:
            vtype = (vs.get("type") or "").lower()
            count = vs.get("votersCount", 0) or 0
            votes = vs.get("votesCount", 0) or 0
            voter_count += count
            if vtype == "for":
                votes_for = votes
            elif vtype == "against":
                votes_against = votes
            elif vtype == "abstain":
                votes_abstain = votes

        statuses = p.get("statusChanges") or []
        state = statuses[-1]["type"].lower() if statuses else "unknown"

        created_at = p.get("createdAt")
        start_ts = (p.get("start") or {}).get("timestamp")
        end_ts = (p.get("end") or {}).get("timestamp")

        record = {
            "protocol_slug": protocol_slug,
            "proposal_id": str(p["id"]),
            "source": "tally",
            "title": title,
            "body": body,
            "state": state,
            "is_risk_related": is_risk,
            "risk_keywords": keywords,
            "budget_amount": budget_amount,
            "budget_currency": budget_currency,
            "votes_for": votes_for,
            "votes_against": votes_against,
            "votes_abstain": votes_abstain,
            "voter_count": voter_count,
            "participation_rate": None,
            "quorum_reached": None,
            "created_at": created_at,
            "start_at": start_ts,
            "end_at": end_ts,
        }

        try:
            execute("""
                INSERT INTO governance_proposals (
                    protocol_slug, proposal_id, source, title, body, state,
                    is_risk_related, risk_keywords, budget_amount, budget_currency,
                    votes_for, votes_against, votes_abstain, voter_count,
                    participation_rate, quorum_reached,
                    created_at, start_at, end_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (protocol_slug, proposal_id, source) DO UPDATE SET
                    state = EXCLUDED.state,
                    votes_for = EXCLUDED.votes_for,
                    votes_against = EXCLUDED.votes_against,
                    votes_abstain = EXCLUDED.votes_abstain,
                    voter_count = EXCLUDED.voter_count,
                    scraped_at = NOW()
            """, (
                record["protocol_slug"], record["proposal_id"], record["source"],
                record["title"], record["body"], record["state"],
                record["is_risk_related"], record["risk_keywords"],
                record["budget_amount"], record["budget_currency"],
                record["votes_for"], record["votes_against"], record["votes_abstain"],
                record["voter_count"], record["participation_rate"], record["quorum_reached"],
                record["created_at"], record["start_at"], record["end_at"],
            ))
            stored.append(record)
        except Exception as e:
            logger.debug(f"Failed to store Tally proposal {p.get('id')}: {e}")

    logger.info(f"Tally: {protocol_slug} — {len(stored)} proposals stored")
    return stored


# =============================================================================
# Parameter Change Tracker (Etherscan)
# =============================================================================

ETHERSCAN_API = "https://api.etherscan.io/api"


def collect_parameter_changes(protocol_slug: str, days: int = 90) -> list[dict]:
    """Track on-chain parameter changes for a protocol via Etherscan."""
    config = PROTOCOL_CONTRACTS.get(protocol_slug)
    if not config:
        return []

    if not ETHERSCAN_API_KEY:
        logger.debug("ETHERSCAN_API_KEY not set — skipping parameter tracking")
        return []

    stored = []
    for contract in config.get("contracts", []):
        address = contract["address"]
        name = contract["name"]
        functions = contract.get("functions", [])

        try:
            # Fetch recent transactions to this contract
            resp = requests.get(
                ETHERSCAN_API,
                params={
                    "module": "account",
                    "action": "txlist",
                    "address": address,
                    "startblock": 0,
                    "endblock": 99999999,
                    "page": 1,
                    "offset": 200,
                    "sort": "desc",
                    "apikey": ETHERSCAN_API_KEY,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            txs = data.get("result", [])
            if not isinstance(txs, list):
                logger.debug(f"Etherscan returned non-list for {protocol_slug}/{name}")
                continue
        except Exception as e:
            logger.warning(f"Etherscan fetch failed for {protocol_slug}/{name}: {e}")
            continue

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        for tx in txs:
            try:
                ts = int(tx.get("timeStamp", 0))
                if ts == 0:
                    continue
                tx_time = datetime.fromtimestamp(ts, tz=timezone.utc)
                if tx_time < cutoff:
                    continue

                # Check if this is a parameter change (successful tx with matching function)
                if tx.get("isError") == "1" or tx.get("txreceipt_status") == "0":
                    continue

                method_id = tx.get("methodId", "")
                func_name = tx.get("functionName", "")

                # Match against known parameter functions
                matched_func = None
                for f in functions:
                    if f.lower() in func_name.lower():
                        matched_func = f
                        break

                if not matched_func and not func_name:
                    continue

                param_type = _infer_parameter_type(matched_func or func_name)

                record = {
                    "protocol_slug": protocol_slug,
                    "tx_hash": tx.get("hash"),
                    "block_number": int(tx.get("blockNumber", 0)),
                    "parameter_type": param_type,
                    "parameter_name": matched_func or func_name.split("(")[0] if func_name else method_id,
                    "old_value": None,  # Would need decode of input data
                    "new_value": None,
                    "contract_address": address,
                    "function_signature": func_name or method_id,
                    "chain": config.get("chain", "ethereum"),
                    "changed_at": tx_time.isoformat(),
                }

                execute("""
                    INSERT INTO parameter_changes (
                        protocol_slug, tx_hash, block_number, parameter_type,
                        parameter_name, old_value, new_value, contract_address,
                        function_signature, chain, changed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (protocol_slug, tx_hash) WHERE tx_hash IS NOT NULL DO NOTHING
                """, (
                    record["protocol_slug"], record["tx_hash"], record["block_number"],
                    record["parameter_type"], record["parameter_name"],
                    record["old_value"], record["new_value"],
                    record["contract_address"], record["function_signature"],
                    record["chain"], record["changed_at"],
                ))
                stored.append(record)
            except Exception as e:
                logger.debug(f"Failed to process tx {tx.get('hash', '?')}: {e}")

        # Rate limit between contract calls
        time.sleep(0.25)

    logger.info(f"Parameters: {protocol_slug} — {len(stored)} changes tracked")
    return stored


def _infer_parameter_type(func_name: str) -> str:
    """Infer parameter type from function name."""
    fn = func_name.lower()
    if any(kw in fn for kw in ["borrow", "interest", "rate"]):
        return "interest_rate"
    if any(kw in fn for kw in ["collateral", "ltv", "liquidation"]):
        return "collateral_factor"
    if any(kw in fn for kw in ["cap", "ceiling"]):
        return "supply_cap"
    if any(kw in fn for kw in ["reserve", "factor"]):
        return "reserve_factor"
    if any(kw in fn for kw in ["emode", "e-mode"]):
        return "emode"
    if any(kw in fn for kw in ["freeze", "pause"]):
        return "circuit_breaker"
    return "other"


# =============================================================================
# Revenue Collector (DeFiLlama — reuses same pattern as PSI)
# =============================================================================

DEFILLAMA_FEES = "https://api.llama.fi/summary/fees"


def collect_protocol_revenue(protocol_slug: str) -> dict | None:
    """Fetch protocol revenue from DeFiLlama for spend_ratio calculation."""
    try:
        resp = requests.get(f"{DEFILLAMA_FEES}/{protocol_slug}", timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Extract 30d revenue
        revenue_30d = None
        total_data_chart = data.get("totalDataChart", [])
        if total_data_chart and len(total_data_chart) >= 30:
            recent_30 = total_data_chart[-30:]
            revenue_30d = sum(d[1] for d in recent_30 if len(d) > 1 and d[1])

        # Also try the direct summary fields
        if revenue_30d is None:
            revenue_30d = data.get("total30d")

        # Annualize
        annual_revenue = revenue_30d * 12 if revenue_30d else None

        return {
            "protocol_slug": protocol_slug,
            "revenue_30d": revenue_30d,
            "annual_revenue": annual_revenue,
        }
    except Exception as e:
        logger.warning(f"DeFiLlama revenue fetch failed for {protocol_slug}: {e}")
        return None


# =============================================================================
# Orchestrator: collect all RPI data for all protocols
# =============================================================================

def collect_all_rpi_data(protocols: list[str] | None = None) -> dict:
    """Run all RPI collectors for the given protocol list."""
    from app.index_definitions.rpi_v01 import TARGET_PROTOCOLS
    slugs = protocols or TARGET_PROTOCOLS

    results = {
        "snapshot_proposals": 0,
        "tally_proposals": 0,
        "parameter_changes": 0,
        "revenue_fetched": 0,
        "protocols_processed": 0,
    }

    for slug in slugs:
        logger.info(f"RPI collection: {slug}")

        # Snapshot governance proposals
        try:
            snap = collect_snapshot_proposals(slug)
            results["snapshot_proposals"] += len(snap)
        except Exception as e:
            logger.warning(f"Snapshot collection failed for {slug}: {e}")

        # Tally on-chain proposals
        try:
            tally = collect_tally_proposals(slug)
            results["tally_proposals"] += len(tally)
        except Exception as e:
            logger.warning(f"Tally collection failed for {slug}: {e}")

        # Parameter changes
        try:
            params = collect_parameter_changes(slug)
            results["parameter_changes"] += len(params)
        except Exception as e:
            logger.warning(f"Parameter tracking failed for {slug}: {e}")

        # Revenue data
        try:
            rev = collect_protocol_revenue(slug)
            if rev:
                results["revenue_fetched"] += 1
        except Exception as e:
            logger.warning(f"Revenue collection failed for {slug}: {e}")

        results["protocols_processed"] += 1

        # Rate limit between protocols
        time.sleep(1.0)

    logger.info(f"RPI collection complete: {results}")
    return results
