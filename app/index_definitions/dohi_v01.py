"""
DOHI v0.1.0 — DAO Operational Health Index
============================================
Extends PSI's governance category into a standalone surface.
Measures governance activity, concentration, operational continuity,
treasury management, security posture, and transparency.

Aggregation: remains on legacy_renormalize (no `aggregation` block →
default). Section A of the aggregation impact analysis report shows
DOHI coverage is still too sparse across the governance-activity and
transparency categories for a formula migration to produce comparable
results. Deferred until DAO governance collectors catch up. See
docs/methodology/aggregation_impact_analysis.md.
"""

DOHI_V01_DEFINITION = {
    "index_id": "dohi",
    "version": "v0.1.0",
    "name": "DAO Operational Health Index",
    "description": "Governance and operational health scoring for DAOs",
    "entity_type": "dao",
    "categories": {
        "governance_activity": {"name": "Governance Activity", "weight": 0.20},
        "governance_concentration": {"name": "Governance Concentration", "weight": 0.20},
        "operational_continuity": {"name": "Operational Continuity", "weight": 0.15},
        "treasury_management": {"name": "Treasury Management", "weight": 0.15},
        "security_posture": {"name": "Security Posture", "weight": 0.15},
        "transparency": {"name": "Transparency", "weight": 0.15},
    },
    "components": {
        # --- Governance Activity (20%) ---
        "proposal_frequency_90d": {
            "name": "Proposal Frequency (90d)",
            "category": "governance_activity",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {1: 10, 3: 30, 5: 50, 10: 70, 20: 85, 50: 100}}},
            "data_source": "snapshot",
        },
        "voter_participation_rate": {
            "name": "Average Voter Participation Rate (%)",
            "category": "governance_activity",
            "weight": 0.25,
            "normalization": {"function": "linear", "params": {"min_val": 1, "max_val": 30}},
            "data_source": "snapshot",
        },
        "quorum_achievement_rate": {
            "name": "Quorum Achievement Rate (%)",
            "category": "governance_activity",
            "weight": 0.20,
            "normalization": {"function": "linear", "params": {"min_val": 50, "max_val": 100}},
            "data_source": "snapshot",
        },
        "proposal_pass_rate": {
            "name": "Proposal Pass Rate (%)",
            "category": "governance_activity",
            "weight": 0.15,
            "normalization": {"function": "centered", "params": {"center": 75, "tolerance": 25, "extreme": 50}},
            "data_source": "snapshot",
        },
        "delegate_count": {
            "name": "Active Delegate Count",
            "category": "governance_activity",
            "weight": 0.15,
            "normalization": {"function": "log", "params": {"thresholds": {10: 20, 50: 40, 100: 60, 500: 80, 1000: 100}}},
            "data_source": "tally",
        },

        # --- Governance Concentration (20%) ---
        "top10_voter_share": {
            "name": "Top 10 Voter Share (%)",
            "category": "governance_concentration",
            "weight": 0.30,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 20, "threshold": 90}},
            "data_source": "snapshot",
        },
        "delegate_concentration_hhi": {
            "name": "Delegate Concentration (1 - HHI)",
            "category": "governance_concentration",
            "weight": 0.25,
            "normalization": {"function": "linear", "params": {"min_val": 0.0, "max_val": 1.0}},
            "data_source": "tally",
        },
        "voting_power_gini": {
            "name": "Voting Power Gini Coefficient",
            "category": "governance_concentration",
            "weight": 0.25,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0.3, "threshold": 0.95}},
            "data_source": "tally",
        },
        "min_coalition_pct": {
            "name": "Minimum Coalition to Pass (%)",
            "category": "governance_concentration",
            "weight": 0.20,
            "normalization": {"function": "linear", "params": {"min_val": 1, "max_val": 30}},
            "data_source": "calculated",
        },

        # --- Operational Continuity (15%) ---
        "active_contributor_count": {
            "name": "Active Contributor Count",
            "category": "operational_continuity",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {5: 20, 10: 40, 25: 60, 50: 80, 100: 100}}},
            "data_source": "config",
        },
        "key_personnel_diversity": {
            "name": "Key Personnel Diversity Score",
            "category": "operational_continuity",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "legal_entity_status": {
            "name": "Legal Entity Status Score",
            "category": "operational_continuity",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "multisig_config": {
            "name": "Multisig Configuration Score",
            "category": "operational_continuity",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Treasury Management (15%) ---
        "treasury_size_usd": {
            "name": "Treasury Size (USD)",
            "category": "treasury_management",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {1000000: 20, 10000000: 40, 100000000: 70, 1000000000: 100}}},
            "data_source": "defillama",
        },
        "treasury_runway_months": {
            "name": "Treasury Runway (months)",
            "category": "treasury_management",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {3: 20, 6: 40, 12: 60, 24: 80, 48: 100}}},
            "data_source": "config",
        },
        "treasury_diversification": {
            "name": "Treasury Diversification (stablecoin %)",
            "category": "treasury_management",
            "weight": 0.25,
            "normalization": {"function": "centered", "params": {"center": 40, "tolerance": 20, "extreme": 40}},
            "data_source": "defillama",
        },
        "treasury_growth_trend": {
            "name": "Treasury Growth Trend (%)",
            "category": "treasury_management",
            "weight": 0.25,
            "normalization": {"function": "centered", "params": {"center": 0, "tolerance": 10, "extreme": 50}},
            "data_source": "defillama",
        },

        # --- Security Posture (15%) ---
        "dao_timelock_hours": {
            "name": "Timelock Duration (hours)",
            "category": "security_posture",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {1: 20, 6: 40, 24: 60, 48: 80, 168: 100}}},
            "data_source": "config",
        },
        "emergency_capability": {
            "name": "Emergency Action Capability Score",
            "category": "security_posture",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "guardian_authority": {
            "name": "Guardian/Pause Authority Score",
            "category": "security_posture",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "dao_upgrade_mechanism": {
            "name": "Upgrade Mechanism Score",
            "category": "security_posture",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "dao_audit_cadence": {
            "name": "Audit Cadence Score",
            "category": "security_posture",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Transparency (15%) ---
        "public_reporting_frequency": {
            "name": "Public Reporting Frequency Score",
            "category": "transparency",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "financial_disclosure": {
            "name": "Financial Disclosure Score",
            "category": "transparency",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "compensation_transparency": {
            "name": "Compensation Transparency Score",
            "category": "transparency",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "meeting_cadence": {
            "name": "Meeting/Call Cadence Score",
            "category": "transparency",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
    },
}

DAO_ENTITIES = [
    # PSI-scored protocols with governance tokens
    {"slug": "aave-dao", "name": "Aave DAO", "protocol_slug": "aave", "snapshot_space": "aavedao.eth", "tally_org": "aave"},
    {"slug": "lido-dao", "name": "Lido DAO", "protocol_slug": "lido", "snapshot_space": "lido-snapshot.eth"},
    {"slug": "compound-dao", "name": "Compound DAO", "protocol_slug": "compound-finance", "snapshot_space": "comp-vote.eth", "tally_org": "compound"},
    {"slug": "curve-dao", "name": "Curve DAO", "protocol_slug": "curve-finance", "snapshot_space": "curve.eth"},
    {"slug": "convex-dao", "name": "Convex DAO", "protocol_slug": "convex-finance", "snapshot_space": "cvx.eth"},
    # Standalone DAOs
    {"slug": "uniswap-dao", "name": "Uniswap DAO", "protocol_slug": "uniswap", "snapshot_space": "uniswapgovernance.eth", "tally_org": "uniswap"},
    {"slug": "ens-dao", "name": "ENS DAO", "snapshot_space": "ens.eth", "tally_org": "ens"},
    {"slug": "arbitrum-dao", "name": "Arbitrum DAO", "snapshot_space": "arbitrumfoundation.eth", "tally_org": "arbitrum"},
    {"slug": "optimism-dao", "name": "Optimism Collective", "snapshot_space": "opcollective.eth", "tally_org": "optimism"},
    {"slug": "gitcoin-dao", "name": "Gitcoin DAO", "snapshot_space": "gitcoindao.eth"},
    {"slug": "safe-dao", "name": "Safe DAO", "snapshot_space": "safe.eth"},
    {"slug": "maker-dao", "name": "MakerDAO (Sky)", "protocol_slug": "sky"},
]
