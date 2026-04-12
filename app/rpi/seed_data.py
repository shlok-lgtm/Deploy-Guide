"""
RPI Seed Data
==============
Initial manual values for RPI components that can't be fully automated yet.
These are stored in rpi_components and used as fallback when live data
is unavailable.

Scoring rubric:
  documentation_depth (0-100, 20pts per criterion):
    - Risk framework published
    - Parameter methodology documented
    - Incident response process documented
    - Counterparty/collateral risk policies published
    - Audit history and scope documented

  vendor_diversity: normalize_vendor_diversity(count, has_external)
    - 0→0, 1→30, 2→60, 3+→80, 3+ with external→100

  external_scoring:
    - 0 (none)→0, 1 (references)→40, 2 (API integration)→70, 3 (bound in decisions)→100

  spend_ratio: percentage of revenue spent on risk management

  incident_severity & recovery_ratio: from known incident records

  governance_health: estimated participation rates
"""

import logging

from app.database import execute
from app.rpi.scorer import (
    normalize_vendor_diversity,
    normalize_external_scoring,
    normalize_spend_ratio,
    normalize_governance_health,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Manual component seed values
# =============================================================================

SEED_DATA = {
    # =========================================================================
    # Aave
    # =========================================================================
    "aave": {
        "documentation_depth": {
            "raw_value": 80,
            "normalized_score": 80.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": True,       # 20pts — Aave Risk Framework by LlamaRisk
                "parameter_methodology": True,           # 20pts — Documented parameter update process
                "incident_response": True,               # 20pts — Post-CAPO incident response documented
                "collateral_risk_policies": True,        # 20pts — Collateral onboarding framework published
                "audit_history_documented": False,        # 0pts  — Audit reports exist but no centralized index
            },
        },
        "vendor_diversity": {
            "raw_value": 1,
            "normalized_score": normalize_vendor_diversity(1, False),  # 30
            "source": "manual_audit",
            "metadata": {
                "vendors": ["LlamaRisk"],
                "note": "Chaos Labs departed April 2026. LlamaRisk sole risk vendor.",
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),  # 0
            "source": "manual_audit",
            "metadata": {"level": "none", "note": "No external scoring integration"},
        },
        "spend_ratio": {
            "raw_value": 3.5,
            "normalized_score": normalize_spend_ratio(3.5),  # ~43.75
            "source": "governance_budget",
            "metadata": {
                "risk_budget_usd": 5_000_000,
                "annual_revenue_usd": 142_000_000,
                "note": "~$5M risk budget / $142M revenue = 3.5%",
            },
        },
        "governance_health": {
            "raw_value": 15,
            "normalized_score": normalize_governance_health(15),  # 60
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 15},
        },
    },

    # =========================================================================
    # Compound
    # =========================================================================
    "compound-finance": {
        "documentation_depth": {
            "raw_value": 60,
            "normalized_score": 60.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": True,       # 20pts
                "parameter_methodology": True,           # 20pts
                "incident_response": True,               # 20pts — deUSD incident response documented
                "collateral_risk_policies": False,       # 0pts
                "audit_history_documented": False,        # 0pts
            },
        },
        "vendor_diversity": {
            "raw_value": 2,
            "normalized_score": normalize_vendor_diversity(2, False),  # 60
            "source": "manual_audit",
            "metadata": {
                "vendors": ["Gauntlet", "OpenZeppelin"],
                "note": "Gauntlet renewed through Sept 2026. OpenZeppelin for audits.",
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),
            "source": "manual_audit",
            "metadata": {"level": "none"},
        },
        "spend_ratio": {
            "raw_value": 4.0,
            "normalized_score": normalize_spend_ratio(4.0),  # 50
            "source": "governance_budget",
            "metadata": {
                "note": "Gauntlet renewal + operational security budget ~4% of revenue",
            },
        },
        "governance_health": {
            "raw_value": 12,
            "normalized_score": normalize_governance_health(12),  # 60
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 12},
        },
    },

    # =========================================================================
    # Lido
    # =========================================================================
    "lido": {
        "documentation_depth": {
            "raw_value": 80,
            "normalized_score": 80.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": True,       # 20pts
                "parameter_methodology": True,           # 20pts
                "incident_response": True,               # 20pts
                "collateral_risk_policies": True,        # 20pts
                "audit_history_documented": False,        # 0pts
            },
        },
        "vendor_diversity": {
            "raw_value": 2,
            "normalized_score": normalize_vendor_diversity(2, False),  # 60
            "source": "manual_audit",
            "metadata": {
                "vendors": ["Ackee Blockchain", "MixBytes"],
                "note": "Multiple audit firms, node operator risk managed internally",
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),
            "source": "manual_audit",
            "metadata": {"level": "none"},
        },
        "governance_health": {
            "raw_value": 20,
            "normalized_score": normalize_governance_health(20),  # 80
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 20},
        },
    },

    # =========================================================================
    # EigenLayer
    # =========================================================================
    "eigenlayer": {
        "documentation_depth": {
            "raw_value": 40,
            "normalized_score": 40.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": True,       # 20pts
                "parameter_methodology": False,          # 0pts — new, still evolving
                "incident_response": False,              # 0pts
                "collateral_risk_policies": True,        # 20pts — restaking risks documented
                "audit_history_documented": False,        # 0pts
            },
        },
        "vendor_diversity": {
            "raw_value": 1,
            "normalized_score": normalize_vendor_diversity(1, False),  # 30
            "source": "manual_audit",
            "metadata": {
                "vendors": ["Trail of Bits"],
                "note": "Primarily Trail of Bits for security audits",
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),
            "source": "manual_audit",
            "metadata": {"level": "none"},
        },
        "governance_health": {
            "raw_value": 8,
            "normalized_score": normalize_governance_health(8),  # 40
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 8},
        },
    },

    # =========================================================================
    # Sky (formerly MakerDAO)
    # =========================================================================
    "sky": {
        "documentation_depth": {
            "raw_value": 80,
            "normalized_score": 80.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": True,       # 20pts
                "parameter_methodology": True,           # 20pts
                "incident_response": True,               # 20pts
                "collateral_risk_policies": True,        # 20pts
                "audit_history_documented": False,        # 0pts
            },
        },
        "vendor_diversity": {
            "raw_value": 3,
            "normalized_score": normalize_vendor_diversity(3, False),  # 80
            "source": "manual_audit",
            "metadata": {
                "vendors": ["BA Labs", "Dewiz", "Steakhouse Financial"],
                "note": "Multiple risk contributors in Sky ecosystem",
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),
            "source": "manual_audit",
            "metadata": {"level": "none"},
        },
        "governance_health": {
            "raw_value": 10,
            "normalized_score": normalize_governance_health(10),  # 60
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 10},
        },
    },

    # =========================================================================
    # Uniswap
    # =========================================================================
    "uniswap": {
        "documentation_depth": {
            "raw_value": 40,
            "normalized_score": 40.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": False,      # 0pts — DEX, limited risk framework
                "parameter_methodology": True,           # 20pts — Fee tier documentation
                "incident_response": False,              # 0pts
                "collateral_risk_policies": False,       # 0pts — N/A for DEX
                "audit_history_documented": True,        # 20pts — Audit reports published
            },
        },
        "vendor_diversity": {
            "raw_value": 2,
            "normalized_score": normalize_vendor_diversity(2, False),  # 60
            "source": "manual_audit",
            "metadata": {
                "vendors": ["Trail of Bits", "OpenZeppelin"],
                "note": "Multiple audit firms for protocol security",
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),
            "source": "manual_audit",
            "metadata": {"level": "none"},
        },
        "governance_health": {
            "raw_value": 5,
            "normalized_score": normalize_governance_health(5),  # 40
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 5},
        },
    },

    # =========================================================================
    # Curve Finance
    # =========================================================================
    "curve-finance": {
        "documentation_depth": {
            "raw_value": 60,
            "normalized_score": 60.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": True,       # 20pts
                "parameter_methodology": True,           # 20pts
                "incident_response": True,               # 20pts — Post-Vyper exploit docs
                "collateral_risk_policies": False,       # 0pts
                "audit_history_documented": False,        # 0pts
            },
        },
        "vendor_diversity": {
            "raw_value": 2,
            "normalized_score": normalize_vendor_diversity(2, False),  # 60
            "source": "manual_audit",
            "metadata": {
                "vendors": ["MixBytes", "Trail of Bits"],
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),
            "source": "manual_audit",
            "metadata": {"level": "none"},
        },
        "governance_health": {
            "raw_value": 25,
            "normalized_score": normalize_governance_health(25),  # 80
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 25, "note": "veCRV governance active"},
        },
    },

    # =========================================================================
    # Morpho
    # =========================================================================
    "morpho": {
        "documentation_depth": {
            "raw_value": 60,
            "normalized_score": 60.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": True,       # 20pts
                "parameter_methodology": True,           # 20pts
                "incident_response": False,              # 0pts
                "collateral_risk_policies": True,        # 20pts
                "audit_history_documented": False,        # 0pts
            },
        },
        "vendor_diversity": {
            "raw_value": 1,
            "normalized_score": normalize_vendor_diversity(1, False),  # 30
            "source": "manual_audit",
            "metadata": {
                "vendors": ["Spearbit"],
                "note": "Spearbit primary security partner",
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),
            "source": "manual_audit",
            "metadata": {"level": "none"},
        },
        "governance_health": {
            "raw_value": 8,
            "normalized_score": normalize_governance_health(8),  # 40
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 8},
        },
    },

    # =========================================================================
    # Spark
    # =========================================================================
    "spark": {
        "documentation_depth": {
            "raw_value": 60,
            "normalized_score": 60.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": True,       # 20pts — Inherits Sky framework
                "parameter_methodology": True,           # 20pts
                "incident_response": True,               # 20pts — Sky ecosystem response
                "collateral_risk_policies": False,       # 0pts
                "audit_history_documented": False,        # 0pts
            },
        },
        "vendor_diversity": {
            "raw_value": 2,
            "normalized_score": normalize_vendor_diversity(2, False),  # 60
            "source": "manual_audit",
            "metadata": {
                "vendors": ["BA Labs", "Cantina"],
                "note": "Shares risk infrastructure with Sky",
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),
            "source": "manual_audit",
            "metadata": {"level": "none"},
        },
        "governance_health": {
            "raw_value": 10,
            "normalized_score": normalize_governance_health(10),  # 60
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 10},
        },
    },

    # =========================================================================
    # Convex Finance
    # =========================================================================
    "convex-finance": {
        "documentation_depth": {
            "raw_value": 40,
            "normalized_score": 40.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": False,      # 0pts
                "parameter_methodology": True,           # 20pts
                "incident_response": False,              # 0pts
                "collateral_risk_policies": False,       # 0pts
                "audit_history_documented": True,        # 20pts
            },
        },
        "vendor_diversity": {
            "raw_value": 1,
            "normalized_score": normalize_vendor_diversity(1, False),  # 30
            "source": "manual_audit",
            "metadata": {
                "vendors": ["MixBytes"],
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),
            "source": "manual_audit",
            "metadata": {"level": "none"},
        },
        "governance_health": {
            "raw_value": 15,
            "normalized_score": normalize_governance_health(15),  # 60
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 15},
        },
    },

    # =========================================================================
    # Drift (Solana)
    # =========================================================================
    "drift": {
        "documentation_depth": {
            "raw_value": 40,
            "normalized_score": 40.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": True,       # 20pts
                "parameter_methodology": False,          # 0pts
                "incident_response": False,              # 0pts
                "collateral_risk_policies": True,        # 20pts
                "audit_history_documented": False,        # 0pts
            },
        },
        "vendor_diversity": {
            "raw_value": 1,
            "normalized_score": normalize_vendor_diversity(1, False),  # 30
            "source": "manual_audit",
            "metadata": {
                "vendors": ["OtterSec"],
                "note": "OtterSec for Solana program audits",
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),
            "source": "manual_audit",
            "metadata": {"level": "none"},
        },
        "governance_health": {
            "raw_value": 5,
            "normalized_score": normalize_governance_health(5),  # 40
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 5},
        },
    },

    # =========================================================================
    # Jupiter Perpetual Exchange (Solana)
    # =========================================================================
    "jupiter-perpetual-exchange": {
        "documentation_depth": {
            "raw_value": 40,
            "normalized_score": 40.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": True,       # 20pts
                "parameter_methodology": False,          # 0pts
                "incident_response": False,              # 0pts
                "collateral_risk_policies": True,        # 20pts
                "audit_history_documented": False,        # 0pts
            },
        },
        "vendor_diversity": {
            "raw_value": 1,
            "normalized_score": normalize_vendor_diversity(1, False),  # 30
            "source": "manual_audit",
            "metadata": {
                "vendors": ["OtterSec"],
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),
            "source": "manual_audit",
            "metadata": {"level": "none"},
        },
        "governance_health": {
            "raw_value": 35,
            "normalized_score": normalize_governance_health(35),  # 100
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 35, "note": "JUP DAO very active"},
        },
    },

    # =========================================================================
    # Raydium (Solana)
    # =========================================================================
    "raydium": {
        "documentation_depth": {
            "raw_value": 20,
            "normalized_score": 20.0,
            "source": "manual_audit",
            "metadata": {
                "risk_framework_published": False,      # 0pts
                "parameter_methodology": True,           # 20pts
                "incident_response": False,              # 0pts
                "collateral_risk_policies": False,       # 0pts
                "audit_history_documented": False,        # 0pts
            },
        },
        "vendor_diversity": {
            "raw_value": 1,
            "normalized_score": normalize_vendor_diversity(1, False),  # 30
            "source": "manual_audit",
            "metadata": {
                "vendors": ["MadShield"],
            },
        },
        "external_scoring": {
            "raw_value": 0,
            "normalized_score": normalize_external_scoring(0),
            "source": "manual_audit",
            "metadata": {"level": "none"},
        },
        "governance_health": {
            "raw_value": 3,
            "normalized_score": normalize_governance_health(3),  # 0
            "source": "snapshot_estimate",
            "metadata": {"estimated_participation_pct": 3},
        },
    },
}


# =============================================================================
# Known risk incidents
# =============================================================================

INCIDENT_SEED = [
    {
        "protocol_slug": "aave",
        "incident_date": "2026-03-15",
        "title": "CAPO Oracle Misconfiguration — Erroneous Liquidations",
        "description": "A misconfigured CAPO oracle price cap led to $26.9M in erroneous liquidations across Aave V3 markets.",
        "severity": "major",
        "severity_weight": 3.0,
        "funds_at_risk_usd": 26_900_000,
        "funds_recovered_usd": 0,
        "recovery_ratio": 0.0,
        "root_cause": "oracle_misconfiguration",
        "source_url": "https://governance.aave.com/",
    },
    {
        "protocol_slug": "compound-finance",
        "incident_date": "2026-02-10",
        "title": "deUSD/sdeUSD Collateral Collapse",
        "description": "deUSD and sdeUSD collateral backing collapsed, exposing Compound to $15.6M in potential bad debt. $12M recovered through liquidations.",
        "severity": "major",
        "severity_weight": 3.0,
        "funds_at_risk_usd": 15_600_000,
        "funds_recovered_usd": 12_000_000,
        "recovery_ratio": 0.769,
        "root_cause": "collateral_depegging",
        "source_url": "https://compound.finance/governance/proposals/",
    },
    {
        "protocol_slug": "curve-finance",
        "incident_date": "2023-07-30",
        "title": "Vyper Compiler Reentrancy Exploit",
        "description": "A Vyper compiler bug enabled reentrancy attacks on several Curve pools, draining ~$70M. Significant funds recovered through white hat efforts.",
        "severity": "critical",
        "severity_weight": 5.0,
        "funds_at_risk_usd": 70_000_000,
        "funds_recovered_usd": 52_000_000,
        "recovery_ratio": 0.743,
        "root_cause": "compiler_vulnerability",
        "source_url": "https://hackmd.io/@LlamaRisk/BJzSKHNjn",
    },
    {
        "protocol_slug": "eigenlayer",
        "incident_date": "2024-10-04",
        "title": "EIGEN Token Transfer Exploit",
        "description": "An attacker exploited the EIGEN token claiming flow, stealing 1.67M EIGEN tokens ($5.7M). Approximately half recovered.",
        "severity": "moderate",
        "severity_weight": 1.0,
        "funds_at_risk_usd": 5_700_000,
        "funds_recovered_usd": 2_800_000,
        "recovery_ratio": 0.491,
        "root_cause": "token_claim_exploit",
        "source_url": "https://blog.eigenlayer.xyz/",
    },
]


# =============================================================================
# Seed insertion function
# =============================================================================

def insert_seed_data():
    """Insert all seed data into rpi_components and risk_incidents tables.

    Uses INSERT ... ON CONFLICT to be idempotent.
    """
    import json

    component_count = 0
    incident_count = 0

    # Insert component seed values
    for slug, components in SEED_DATA.items():
        for component_id, data in components.items():
            try:
                execute("""
                    INSERT INTO rpi_components (
                        protocol_slug, component_id, raw_value, normalized_score,
                        source, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (protocol_slug, component_id, (collected_at::date))
                    DO UPDATE SET
                        raw_value = EXCLUDED.raw_value,
                        normalized_score = EXCLUDED.normalized_score,
                        source = EXCLUDED.source,
                        metadata = EXCLUDED.metadata
                """, (
                    slug, component_id,
                    data["raw_value"], data["normalized_score"],
                    data.get("source", "manual"),
                    json.dumps(data.get("metadata", {})),
                ))
                component_count += 1
            except Exception as e:
                logger.warning(f"Failed to seed {slug}/{component_id}: {e}")

    # Insert incident records
    for inc in INCIDENT_SEED:
        try:
            execute("""
                INSERT INTO risk_incidents (
                    protocol_slug, incident_date, title, description,
                    severity, severity_weight,
                    funds_at_risk_usd, funds_recovered_usd, recovery_ratio,
                    root_cause, source_url
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                inc["protocol_slug"], inc["incident_date"], inc["title"],
                inc["description"], inc["severity"], inc["severity_weight"],
                inc["funds_at_risk_usd"], inc["funds_recovered_usd"],
                inc["recovery_ratio"], inc["root_cause"], inc["source_url"],
            ))
            incident_count += 1
        except Exception as e:
            logger.warning(f"Failed to seed incident for {inc['protocol_slug']}: {e}")

    logger.info(f"RPI seed data: {component_count} component values, {incident_count} incidents inserted")
    return {"components": component_count, "incidents": incident_count}
