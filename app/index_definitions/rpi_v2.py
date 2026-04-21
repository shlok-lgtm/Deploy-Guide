"""
RPI v2.0.0 — Risk Posture Index
=================================
Measures how well a protocol manages risk: governance spending,
parameter changes, vendor relationships, incident history.

RPI has a base/lens architecture. The base score uses only 5 automated,
ungameable components. Four additional components live in lenses —
optional overlays that consumers can apply.

Base score is always computed and stored.
Lensed score is computed on-the-fly when requested via ?lens= query param.

Aggregation: remains on RPI's bespoke normalization (not dispatched
through the aggregation registry — RPI uses its own custom path in
app/rpi/scorer.py::score_rpi_base). Section B of the aggregation
impact analysis report flagged RPI as having a methodology-
discrimination problem — 10+ protocols produce identical base scores
because the 5 automated components saturate on a crowded mid-range.
Swapping the aggregation formula would obscure this rather than solve
it; the fix is methodology review (adding discriminating components or
re-weighting), not a formula migration. Deferred until that review.
See docs/methodology/aggregation_impact_analysis.md.
"""

from app.index_definitions.psi_v01 import TARGET_PROTOCOLS

RPI_V2_DEFINITION = {
    "index_id": "rpi",
    "version": "v2.0.0",
    "name": "Risk Posture Index",
    "description": "Measures how well a protocol manages risk — governance spending, parameter changes, vendor relationships, incident history",
    "entity_type": "protocol",
    "categories": {
        "economics": {"name": "Economics", "weight": 0.20},
        "operations": {"name": "Operations", "weight": 0.40},
        "history": {"name": "History", "weight": 0.20},
        "governance": {"name": "Governance", "weight": 0.20},
    },
    "components": {
        "spend_ratio": {
            "name": "Risk Spend Ratio",
            "category": "economics",
            "weight": 1.0,
            "normalization": {"function": "linear", "params": {"min_val": 0.0, "max_val": 8.0}},
            "data_source": "governance_proposals",
        },
        "parameter_velocity": {
            "name": "Parameter Change Velocity",
            "category": "operations",
            "weight": 0.625,  # 0.25 / 0.40 = 0.625
            "normalization": {"function": "log", "params": {"thresholds": {0: 0, 1: 50, 4: 80, 9: 100}}},
            "data_source": "etherscan",
        },
        "parameter_recency": {
            "name": "Parameter Change Recency",
            "category": "operations",
            "weight": 0.375,  # 0.15 / 0.40 = 0.375
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 90}},
            "data_source": "etherscan",
        },
        "incident_severity": {
            "name": "Incident Severity Score",
            "category": "history",
            "weight": 1.0,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "risk_incidents",
        },
        "governance_health": {
            "name": "Governance Participation Health",
            "category": "governance",
            "weight": 1.0,
            "normalization": {"function": "log", "params": {"thresholds": {5: 40, 10: 60, 20: 80, 30: 100}}},
            "data_source": "governance_proposals",
        },
    },
}

# Lens definitions — optional overlays that consumers can apply
RPI_LENSES = {
    "risk_organization": {
        "name": "Risk Organization",
        "description": "Vendor diversity and incident recovery capability",
        "components": {
            "vendor_diversity": {
                "name": "Risk Vendor Diversity",
                "weight": 0.56,  # 0.10 / (0.10 + 0.08)
                "normalization": {"function": "log", "params": {"thresholds": {0: 0, 1: 30, 2: 60, 3: 80}}},
                "data_source": "manual",
            },
            "recovery_ratio": {
                "name": "Incident Recovery Ratio",
                "weight": 0.44,  # 0.08 / (0.10 + 0.08)
                "normalization": {"function": "linear", "params": {"min_val": 0.0, "max_val": 90.0}},
                "data_source": "risk_incidents",
            },
        },
    },
    "risk_infrastructure": {
        "name": "Risk Infrastructure",
        "description": "External scoring integration depth",
        "components": {
            "external_scoring": {
                "name": "External Scoring Integration",
                "weight": 1.0,
                "normalization": {"function": "direct", "params": {}},
                "data_source": "manual",
            },
        },
    },
    "risk_transparency": {
        "name": "Risk Transparency",
        "description": "Quality and depth of public risk documentation",
        "components": {
            "documentation_depth": {
                "name": "Risk Documentation Depth",
                "weight": 1.0,
                "normalization": {"function": "direct", "params": {}},
                "data_source": "manual",
            },
        },
    },
}

# Lens blend factor: when lenses are applied,
# RPI_lensed = (1 - LENS_BLEND) * base + LENS_BLEND * lens_weighted_avg
LENS_BLEND = 0.30

# RPI scores the same 13 PSI-scored protocols
RPI_TARGET_PROTOCOLS = list(TARGET_PROTOCOLS)
