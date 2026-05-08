"""
RPI Seed Data
===============
Initial values for RPI components. Base components are seeded with
source_type='seed' and will be replaced when automated collectors run.
Lens components are manually assessed and stored with source_type='manual'.

Data sources:
- spend_ratio: public governance budget proposals (annualized)
- incident_severity: public post-mortem disclosures
- vendor_diversity: public governance records
- recovery_ratio: post-mortem disclosures
- external_scoring: Basis API integration status
- documentation_depth: manual assessment of public risk docs
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from app.database import execute, fetch_one
from app.index_definitions.rpi_v2 import RPI_TARGET_PROTOCOLS

logger = logging.getLogger(__name__)


# =============================================================================
# Base component seed values
# =============================================================================

# spend_ratio: risk budget as % of annualized revenue
# Sources: governance proposals, forum posts, budget renewal votes
BASE_SPEND_RATIO = {
    "aave": 3.5,         # ~$5M risk budget / $142M revenue
    "lido": 2.0,         # ~$4M security budget / ~$200M revenue
    "eigenlayer": 1.0,   # early-stage, minimal public budget
    "sky": 4.0,          # ~$8M risk/security / ~$200M revenue (historically)
    "compound-finance": 3.0,  # Gauntlet renewal ~$2M / ~$65M revenue
    "uniswap": 1.5,      # ~$3M security grants / ~$200M fees
    "curve-finance": 2.5, # ~$2M audits + risk / ~$80M revenue
    "morpho": 1.0,       # newer protocol, limited public budget info
    "spark": 2.0,        # sub-DAO of Sky, inherits risk framework
    "convex-finance": 1.5, # ~$1M audits / ~$65M revenue
    "drift": 1.0,        # Solana protocol, limited public budget data
    "jupiter-perpetual-exchange": 1.0,  # limited public budget data
    "raydium": 0.5,      # limited public budget data
}

# parameter_velocity: estimated monthly parameter changes (seed values)
# Will be replaced by automated etherscan tracking
BASE_PARAMETER_VELOCITY = {
    "aave": 6,            # active Gauntlet/LlamaRisk management
    "lido": 2,            # fewer parameter changes (staking protocol)
    "eigenlayer": 3,      # moderate activity
    "sky": 5,             # frequent stability fee/collateral adjustments
    "compound-finance": 8, # active Gauntlet management
    "uniswap": 1,         # governance-only changes (AMM has few params)
    "curve-finance": 4,   # pool parameter adjustments
    "morpho": 5,          # market-level parameter changes
    "spark": 4,           # inherits Sky's parameter activity
    "convex-finance": 2,  # fewer direct parameter changes
    "drift": 3,           # Solana parameter updates
    "jupiter-perpetual-exchange": 2,  # limited parameter surface
    "raydium": 2,         # limited parameter surface
}

# parameter_recency: days since last parameter change (seed values)
BASE_PARAMETER_RECENCY = {
    "aave": 5,
    "lido": 14,
    "eigenlayer": 10,
    "sky": 7,
    "compound-finance": 3,
    "uniswap": 30,
    "curve-finance": 7,
    "morpho": 5,
    "spark": 7,
    "convex-finance": 14,
    "drift": 10,
    "jupiter-perpetual-exchange": 14,
    "raydium": 21,
}

# governance_health: average voter participation % (seed values)
BASE_GOVERNANCE_HEALTH = {
    "aave": 12.0,         # moderate Snapshot participation
    "lido": 8.0,          # Snapshot voting
    "eigenlayer": 5.0,    # early-stage governance
    "sky": 25.0,          # historically high participation (MKR governance)
    "compound-finance": 10.0,  # on-chain Governor Bravo
    "uniswap": 6.0,       # low relative participation
    "curve-finance": 15.0, # veCRV gauge voting
    "morpho": 8.0,        # newer governance
    "spark": 20.0,        # inherits Sky's participation patterns
    "convex-finance": 10.0, # vlCVX voting
    "drift": 5.0,         # Realms governance (Solana)
    "jupiter-perpetual-exchange": 7.0,  # JUP DAO
    "raydium": 3.0,       # limited governance activity
}


# =============================================================================
# Risk incidents (seeded with reviewed=true)
# =============================================================================

RISK_INCIDENTS = [
    {
        "protocol_slug": "aave",
        "incident_date": "2026-03-15",
        "title": "CAPO Oracle Misconfiguration — Erroneous Liquidations",
        "description": "A misconfigured CAPO (Correlated Asset Price Oracle) parameter led to $26.9M in erroneous liquidations across multiple markets. The oracle's growth cap was set too aggressively, causing price feeds to lag during a rapid market recovery.",
        "severity": "critical",
        "funds_at_risk_usd": 26900000,
        "funds_recovered_usd": 0,
        "reviewed": True,
        "source_url": "https://governance.aave.com/",
    },
    {
        "protocol_slug": "compound-finance",
        "incident_date": "2026-02-20",
        "title": "deUSD/sdeUSD Collateral Collapse",
        "description": "deUSD and sdeUSD collateral experienced rapid devaluation, exposing $15.6M in undercollateralized positions. Gauntlet's monitoring detected the issue and Compound governance executed emergency parameter freezes. $12M was recovered (78%).",
        "severity": "major",
        "funds_at_risk_usd": 15600000,
        "funds_recovered_usd": 12000000,
        "reviewed": True,
        "source_url": "https://compound.finance/governance",
    },
    {
        "protocol_slug": "curve-finance",
        "incident_date": "2025-07-30",
        "title": "Vyper Compiler Reentrancy Exploit (residual effects)",
        "description": "While the primary exploit occurred in July 2023, residual effects from the Vyper reentrancy vulnerability continued to affect pool confidence. Curve completed all pool migrations and audits by Q3 2025.",
        "severity": "moderate",
        "funds_at_risk_usd": 5000000,
        "funds_recovered_usd": 4500000,
        "reviewed": True,
        "source_url": "https://curve.fi/#/ethereum/pools",
    },
    {
        "protocol_slug": "drift",
        "incident_date": "2026-04-01",
        "title": "Bad Debt Accumulation from Leveraged Positions",
        "description": "Accumulated bad debt of approximately $270M from leveraged perpetual positions during a period of extreme market volatility on Solana.",
        "severity": "critical",
        "funds_at_risk_usd": 270000000,
        "funds_recovered_usd": 0,
        "reviewed": True,
        "source_url": "https://drift.trade",
    },
]


# =============================================================================
# Lens component seed values (manually assessed)
# =============================================================================

# vendor_diversity: count of active risk management vendors
LENS_VENDOR_DIVERSITY = {
    "aave": 1,            # LlamaRisk only (Chaos Labs departed April 2026)
    "lido": 2,            # Multiple auditors + risk committee
    "eigenlayer": 1,      # Single risk vendor
    "sky": 2,             # BA Labs + internal risk team
    "compound-finance": 2, # Gauntlet (renewed through Sept 2026) + OpenZeppelin
    "uniswap": 1,         # Security alliance membership
    "curve-finance": 1,   # Internal + community audits
    "morpho": 1,          # Single risk framework provider
    "spark": 2,           # Inherits Sky's vendors + own risk team
    "convex-finance": 1,  # Relies on Curve's security + own audits
    "drift": 1,           # Single security auditor
    "jupiter-perpetual-exchange": 1,  # OtterSec
    "raydium": 1,         # Limited public vendor info
}

# recovery_ratio: % of funds recovered after incidents (None = no incidents)
LENS_RECOVERY_RATIO = {
    "aave": 0.0,          # CAPO incident — no recovery (liquidations were protocol-functioning)
    "lido": None,         # no significant incidents
    "eigenlayer": None,   # no significant incidents
    "sky": None,          # no recent incidents
    "compound-finance": 78.0,  # $12M / $15.6M recovered
    "uniswap": None,      # no significant incidents
    "curve-finance": 90.0, # most funds recovered from Vyper exploit
    "morpho": None,       # no significant incidents
    "spark": None,        # no significant incidents
    "convex-finance": None, # no significant incidents
    "drift": 0.0,         # $270M bad debt — no recovery yet
    "jupiter-perpetual-exchange": None,  # no significant incidents
    "raydium": None,      # no significant incidents
}

# external_scoring: depth of external scoring integration
# 0 = none, 40 = references, 70 = API integration, 100 = bound in decisions
LENS_EXTERNAL_SCORING = {
    "aave": 0,
    "lido": 0,
    "eigenlayer": 0,
    "sky": 0,
    "compound-finance": 0,
    "uniswap": 0,
    "curve-finance": 0,
    "morpho": 0,
    "spark": 0,
    "convex-finance": 0,
    "drift": 0,
    "jupiter-perpetual-exchange": 0,
    "raydium": 0,
}

# documentation_depth: 0-100, 20pts per criterion:
# 1. Risk framework published
# 2. Parameter methodology documented
# 3. Incident response process documented
# 4. Counterparty/collateral risk policies published
# 5. Audit history and scope documented
LENS_DOCUMENTATION_DEPTH = {
    "aave": 80,           # Excellent docs: risk framework, parameter methodology, audit history. Missing formal incident response.
    "lido": 60,           # Good docs: risk framework published, audit history. Missing parameter methodology and incident response.
    "eigenlayer": 40,     # Risk framework exists, audit history. Other docs sparse.
    "sky": 80,            # Comprehensive: risk framework, collateral policies, audit history. Strong historical docs.
    "compound-finance": 60, # Parameter methodology (Gauntlet), audit history. Limited incident response docs.
    "uniswap": 40,        # Audit history strong. Risk framework and parameter docs limited (AMM design).
    "curve-finance": 60,  # Technical docs strong, audit history. Risk framework implicit in design.
    "morpho": 60,         # Good technical docs, risk framework emerging. Audit history documented.
    "spark": 60,          # Inherits Sky's docs framework. Own parameter docs growing.
    "convex-finance": 40, # Audit history documented. Risk framework relies on Curve's.
    "drift": 40,          # Technical docs available. Risk framework docs limited.
    "jupiter-perpetual-exchange": 40,  # Basic docs, audit history. Risk framework emerging.
    "raydium": 20,        # Minimal risk documentation. Basic audit info only.
}


# =============================================================================
# Seed data insertion
# =============================================================================

def _normalize_vendor_diversity(count: int) -> float:
    if count <= 0:
        return 0.0
    if count == 1:
        return 30.0
    if count == 2:
        return 60.0
    return 80.0


def _normalize_recovery_ratio(pct: float | None) -> float:
    if pct is None:
        return 100.0
    if pct >= 90:
        return 100.0
    if pct >= 70:
        return 80.0
    if pct >= 50:
        return 60.0
    if pct >= 30:
        return 40.0
    return 0.0


def seed_rpi_data():
    """Insert seed data for RPI components and incidents."""
    seeded = 0

    # Seed base components
    for slug in RPI_TARGET_PROTOCOLS:
        # spend_ratio
        if slug in BASE_SPEND_RATIO:
            execute("""
                INSERT INTO rpi_components
                    (protocol_slug, component_id, component_type, raw_value,
                     normalized_score, source_type, data_source, collected_at)
                VALUES (%s, 'spend_ratio', 'base', %s, %s, 'seed', 'governance_proposals', NOW())
            """, (slug, BASE_SPEND_RATIO[slug], min(BASE_SPEND_RATIO[slug] / 8.0 * 100, 100)))
            seeded += 1

        # parameter_velocity
        if slug in BASE_PARAMETER_VELOCITY:
            vel = BASE_PARAMETER_VELOCITY[slug]
            score = 0 if vel <= 0 else (50 if vel <= 3 else (80 if vel <= 8 else 100))
            execute("""
                INSERT INTO rpi_components
                    (protocol_slug, component_id, component_type, raw_value,
                     normalized_score, source_type, data_source, collected_at)
                VALUES (%s, 'parameter_velocity', 'base', %s, %s, 'seed', 'etherscan', NOW())
            """, (slug, vel, score))
            seeded += 1

        # parameter_recency
        if slug in BASE_PARAMETER_RECENCY:
            days = BASE_PARAMETER_RECENCY[slug]
            if days <= 7:
                score = 100
            elif days <= 14:
                score = 80
            elif days <= 30:
                score = 60
            elif days <= 60:
                score = 40
            elif days <= 90:
                score = 20
            else:
                score = 0
            execute("""
                INSERT INTO rpi_components
                    (protocol_slug, component_id, component_type, raw_value,
                     normalized_score, source_type, data_source, collected_at)
                VALUES (%s, 'parameter_recency', 'base', %s, %s, 'seed', 'etherscan', NOW())
            """, (slug, days, score))
            seeded += 1

        # governance_health
        if slug in BASE_GOVERNANCE_HEALTH:
            pct = BASE_GOVERNANCE_HEALTH[slug]
            if pct >= 30:
                score = 100
            elif pct >= 20:
                score = 80
            elif pct >= 10:
                score = 60
            elif pct >= 5:
                score = 40
            else:
                score = 0
            execute("""
                INSERT INTO rpi_components
                    (protocol_slug, component_id, component_type, raw_value,
                     normalized_score, source_type, data_source, collected_at)
                VALUES (%s, 'governance_health', 'base', %s, %s, 'seed', 'governance_proposals', NOW())
            """, (slug, pct, score))
            seeded += 1

    # Seed risk incidents
    for incident in RISK_INCIDENTS:
        try:
            execute("""
                INSERT INTO risk_incidents
                    (protocol_slug, incident_date, title, description,
                     severity, funds_at_risk_usd, funds_recovered_usd,
                     reviewed, source_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                incident["protocol_slug"],
                incident["incident_date"],
                incident["title"],
                incident["description"],
                incident["severity"],
                incident["funds_at_risk_usd"],
                incident["funds_recovered_usd"],
                incident["reviewed"],
                incident["source_url"],
            ))
            seeded += 1
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Failed to seed incident: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="rpi_seed_rpi_data_incident_insert_failure",
                    error_message=str(e)[:500],
                    cycle_phase="rpi_seed_data",
                )
            except Exception:
                pass

    # Seed lens components
    for slug in RPI_TARGET_PROTOCOLS:
        # vendor_diversity (risk_organization lens)
        if slug in LENS_VENDOR_DIVERSITY:
            count = LENS_VENDOR_DIVERSITY[slug]
            score = _normalize_vendor_diversity(count)
            execute("""
                INSERT INTO rpi_components
                    (protocol_slug, component_id, component_type, lens_id,
                     raw_value, normalized_score, source_type, data_source, collected_at)
                VALUES (%s, 'vendor_diversity', 'lens', 'risk_organization',
                        %s, %s, 'manual', 'governance_records', NOW())
            """, (slug, count, score))
            seeded += 1

        # recovery_ratio (risk_organization lens)
        if slug in LENS_RECOVERY_RATIO:
            pct = LENS_RECOVERY_RATIO[slug]
            score = _normalize_recovery_ratio(pct)
            execute("""
                INSERT INTO rpi_components
                    (protocol_slug, component_id, component_type, lens_id,
                     raw_value, normalized_score, source_type, data_source, collected_at)
                VALUES (%s, 'recovery_ratio', 'lens', 'risk_organization',
                        %s, %s, 'manual', 'risk_incidents', NOW())
            """, (slug, pct, score))
            seeded += 1

        # external_scoring (risk_infrastructure lens)
        if slug in LENS_EXTERNAL_SCORING:
            val = LENS_EXTERNAL_SCORING[slug]
            execute("""
                INSERT INTO rpi_components
                    (protocol_slug, component_id, component_type, lens_id,
                     raw_value, normalized_score, source_type, data_source, collected_at)
                VALUES (%s, 'external_scoring', 'lens', 'risk_infrastructure',
                        %s, %s, 'manual', 'api_logs', NOW())
            """, (slug, val, float(val)))
            seeded += 1

        # documentation_depth (risk_transparency lens)
        if slug in LENS_DOCUMENTATION_DEPTH:
            val = LENS_DOCUMENTATION_DEPTH[slug]
            execute("""
                INSERT INTO rpi_components
                    (protocol_slug, component_id, component_type, lens_id,
                     raw_value, normalized_score, source_type, data_source, collected_at)
                VALUES (%s, 'documentation_depth', 'lens', 'risk_transparency',
                        %s, %s, 'manual', 'manual_assessment', NOW())
            """, (slug, val, float(val)))
            seeded += 1

    logger.info(f"RPI seed data: {seeded} records inserted")
    return seeded
