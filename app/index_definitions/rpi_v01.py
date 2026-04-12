"""
RPI v1.0.0 — Risk Posture Index
=================================
Measures how well a protocol manages risk: governance spending,
parameter changes, vendor relationships, incident history.
Uses the same scoring engine as SII and PSI with different categories,
weights, components, and data sources.

RPI is Primitive #22.
"""

RPI_V01_DEFINITION = {
    "index_id": "rpi",
    "version": "v1.0.0",
    "name": "Risk Posture Index",
    "description": "Measures how well a protocol manages risk — governance spending, parameter tuning, vendor diversity, and incident response",
    "entity_type": "protocol",
    "categories": {
        "economics": {"name": "Risk Economics", "weight": 0.15},
        "organization": {"name": "Risk Organization", "weight": 0.10},
        "operations": {"name": "Risk Operations", "weight": 0.25},
        "history": {"name": "Incident History", "weight": 0.25},
        "infrastructure": {"name": "Risk Infrastructure", "weight": 0.10},
        "transparency": {"name": "Transparency", "weight": 0.05},
        "governance": {"name": "Governance Health", "weight": 0.10},
    },
    "components": {
        "spend_ratio": {
            "name": "Risk Spend Ratio",
            "category": "economics",
            "weight": 1.0,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "governance_proposals",
        },
        "vendor_diversity": {
            "name": "Vendor Diversity",
            "category": "organization",
            "weight": 1.0,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "manual",
        },
        "parameter_velocity": {
            "name": "Parameter Change Velocity",
            "category": "operations",
            "weight": 0.60,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "etherscan",
        },
        "parameter_recency": {
            "name": "Parameter Change Recency",
            "category": "operations",
            "weight": 0.40,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "etherscan",
        },
        "incident_severity": {
            "name": "Incident Severity Score",
            "category": "history",
            "weight": 0.60,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "curated",
        },
        "recovery_ratio": {
            "name": "Recovery Ratio",
            "category": "history",
            "weight": 0.40,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "curated",
        },
        "external_scoring": {
            "name": "External Scoring Integration",
            "category": "infrastructure",
            "weight": 1.0,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "manual",
        },
        "documentation_depth": {
            "name": "Documentation Depth",
            "category": "transparency",
            "weight": 1.0,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "manual",
        },
        "governance_health": {
            "name": "Governance Health",
            "category": "governance",
            "weight": 1.0,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "snapshot",
        },
    },
}

# RPI scores the same 13 protocols as PSI
TARGET_PROTOCOLS = [
    "aave", "lido", "eigenlayer", "sky", "compound-finance",
    "uniswap", "curve-finance", "morpho", "spark", "convex-finance",
    "drift", "jupiter-perpetual-exchange", "raydium",
]
