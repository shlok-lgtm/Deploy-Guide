"""
Methodology version registry for SII, PSI, and Wallet risk scoring.
Provides version metadata, governance rules, and changelog.
"""

METHODOLOGY_VERSIONS = {
    "current": "v1.1.0",
    "versions": [
        {
            "version": "v1.0.0",
            "released": "2025-12-28",
            "status": "superseded",
            "formula": "SII = 0.30*Peg + 0.25*Liquidity + 0.15*MintBurn + 0.10*Distribution + 0.20*Structural",
            "changelog": "Initial public release"
        },
        {
            "version": "v1.1.0",
            "released": "2026-04-21",
            "status": "current",
            "formula": "SII = 0.30*Peg + 0.25*Liquidity + 0.15*MintBurn + 0.10*Distribution + 0.20*Structural",
            "aggregation": {"formula": "coverage_weighted", "params": {"min_coverage": 0.0}},
            "changelog": (
                "Aggregation migrated from legacy SII v1.0.0 renormalization to "
                "coverage_weighted with min_coverage=0.0. Weights, categories, and "
                "components unchanged. Justified by docs/methodology/"
                "aggregation_impact_analysis.md — partial-coverage categories now "
                "contribute in proportion to their populated-weight fraction, so "
                "well-populated categories are no longer silently over-weighted "
                "relative to their peers. Production scoring path wiring is a "
                "follow-up; this release carries only the declaration change."
            )
        }
    ],
    "governance": {
        "change_protocol": "Announced 30 days in advance. Versioned. Timestamped. Retroactively reproducible.",
        "comment_period_days": 30,
        "deprecation_notice_days": 90
    }
}

PSI_METHODOLOGY_VERSIONS = {
    "current": "psi-v0.3.0",
    "versions": [
        {
            "version": "psi-v0.1.0",
            "released": "2026-03-15",
            "status": "superseded",
            "components": 24,
            "categories": 6,
            "security_components": ["audit_count", "audit_recency_days", "protocol_admin_key_risk"],
            "security_weights": {"audit_count": 0.40, "audit_recency_days": 0.30, "protocol_admin_key_risk": 0.30},
            "formula": "PSI = 0.25*BalanceSheet + 0.20*Revenue + 0.20*Liquidity + 0.15*Security + 0.10*Governance + 0.10*TokenHealth",
            "changelog": "Initial PSI release. 24 components across 6 categories scoring 13 DeFi protocols."
        },
        {
            "version": "psi-v0.2.0",
            "released": "2026-04-03",
            "status": "superseded",
            "components": 27,
            "categories": 6,
            "security_components": [
                "audit_count", "audit_recency_days", "protocol_admin_key_risk",
                "governance_stability", "collateral_coverage_ratio", "market_listing_velocity"
            ],
            "security_weights": {
                "audit_count": 0.25, "audit_recency_days": 0.15, "protocol_admin_key_risk": 0.15,
                "governance_stability": 0.15, "collateral_coverage_ratio": 0.15, "market_listing_velocity": 0.15
            },
            "formula": "PSI = 0.25*BalanceSheet + 0.20*Revenue + 0.20*Liquidity + 0.15*Security + 0.10*Governance + 0.10*TokenHealth",
            "changelog": (
                "V0.2.0: Added governance_stability, collateral_coverage_ratio, market_listing_velocity "
                "to Security category. Motivated by Drift Protocol exploit analysis (April 1, 2026). "
                "Security category expanded from 3 to 6 components with rebalanced weights. "
                "Category-level weights unchanged. 27 total components (was 24)."
            )
        },
        {
            "version": "psi-v0.3.0",
            "released": "2026-04-21",
            "status": "current",
            "components": 27,
            "categories": 6,
            "formula": "PSI = 0.25*BalanceSheet + 0.20*Revenue + 0.20*Liquidity + 0.15*Security + 0.10*Governance + 0.10*TokenHealth",
            "aggregation": {"formula": "coverage_weighted", "params": {"min_coverage": 0.60}},
            "changelog": (
                "Aggregation migrated from legacy_renormalize to coverage_weighted "
                "with min_coverage=0.60. Protocols below 60% component coverage now "
                "have overall_score withheld; above the gate, categories contribute "
                "in proportion to their populated-weight fraction. Weights, "
                "categories, and components unchanged. Justified by "
                "docs/methodology/aggregation_impact_analysis.md — the coverage "
                "distribution across the PSI roster supports 0.60 as the floor."
            )
        }
    ],
    "governance": {
        "change_protocol": "Versioned. Timestamped. Prior versions reproducible via temporal reconstruction.",
        "comment_period_days": 7,
        "deprecation_notice_days": 30
    }
}

WALLET_METHODOLOGY_VERSIONS = {
    "current": "wallet-v1.0.0",
    "versions": [
        {
            "version": "wallet-v1.0.0",
            "released": "2026-03-01",
            "status": "current",
            "formula": "Value-weighted average SII across wallet holdings",
            "changelog": "Initial wallet risk scoring"
        }
    ]
}
