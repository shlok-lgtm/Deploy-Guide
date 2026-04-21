"""
BRI v0.2.0 — Bridge Integrity Index
=====================================
Measures security, operational history, liquidity, smart contract risk,
decentralization, and economic security for cross-chain bridges.

v0.2.0 — promoted from accruing to scored; aggregation migrated from
legacy_renormalize to coverage_withheld with coverage_threshold=0.70.
Weights, categories, and components unchanged. Threshold justified by
Section A of the aggregation impact analysis report — the BRI coverage
distribution is mature enough that 0.70 marks a meaningful quality gate
without withholding the bulk of the roster. See
docs/methodology/aggregation_impact_analysis.md and
docs/methodology/bri_changelog.md.
"""

BRI_V01_DEFINITION = {
    "index_id": "bri",
    "version": "v0.2.0",
    "name": "Bridge Integrity Index",
    "description": "Risk scoring for cross-chain bridge protocols",
    "entity_type": "bridge",
    "aggregation": {
        "formula": "coverage_withheld",
        "params": {"coverage_threshold": 0.70},
    },
    "categories": {
        "security_architecture": {"name": "Security Architecture", "weight": 0.25},
        "operational_history": {"name": "Operational History", "weight": 0.20},
        "liquidity_throughput": {"name": "Liquidity & Throughput", "weight": 0.20},
        "smart_contract_risk": {"name": "Smart Contract Risk", "weight": 0.15},
        "decentralization": {"name": "Decentralization", "weight": 0.10},
        "economic_security": {"name": "Economic Security", "weight": 0.10},
    },
    "components": {
        # --- Security Architecture (25%) ---
        "verification_mechanism": {
            "name": "Verification Mechanism Score",
            "category": "security_architecture",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "guardian_count": {
            "name": "Guardian/Relayer Count",
            "category": "security_architecture",
            "weight": 0.20,
            "normalization": {"function": "log", "params": {"thresholds": {3: 20, 5: 40, 10: 60, 15: 80, 19: 100}}},
            "data_source": "config",
        },
        "guardian_diversity": {
            "name": "Guardian Diversity (1 - HHI)",
            "category": "security_architecture",
            "weight": 0.15,
            "normalization": {"function": "linear", "params": {"min_val": 0.0, "max_val": 1.0}},
            "data_source": "config",
        },
        "bridge_upgrade_mechanism": {
            "name": "Upgrade Mechanism Score",
            "category": "security_architecture",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "bridge_timelock": {
            "name": "Timelock Duration (hours)",
            "category": "security_architecture",
            "weight": 0.10,
            "normalization": {"function": "log", "params": {"thresholds": {1: 20, 6: 40, 24: 60, 48: 80, 168: 100}}},
            "data_source": "config",
        },
        "bridge_audit_count": {
            "name": "Audit Count",
            "category": "security_architecture",
            "weight": 0.15,
            "normalization": {"function": "log", "params": {"thresholds": {1: 30, 2: 50, 3: 70, 5: 85, 10: 100}}},
            "data_source": "config",
        },

        # --- Operational History (20%) ---
        "total_value_transferred": {
            "name": "Total Value Transferred (USD)",
            "category": "operational_history",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {100000000: 10, 1000000000: 30, 10000000000: 60, 50000000000: 80, 100000000000: 100}}},
            "data_source": "defillama",
        },
        "uptime_pct": {
            "name": "Uptime Percentage (%)",
            "category": "operational_history",
            "weight": 0.20,
            "normalization": {"function": "linear", "params": {"min_val": 95, "max_val": 100}},
            "data_source": "config",
        },
        "message_success_rate": {
            "name": "Message Delivery Success Rate (%)",
            "category": "operational_history",
            "weight": 0.20,
            "normalization": {"function": "linear", "params": {"min_val": 95, "max_val": 100}},
            "data_source": "config",
        },
        "incident_history": {
            "name": "Exploit/Incident History Score",
            "category": "operational_history",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "time_since_incident_days": {
            "name": "Days Since Last Incident",
            "category": "operational_history",
            "weight": 0.15,
            "normalization": {"function": "log", "params": {"thresholds": {30: 10, 90: 30, 180: 50, 365: 70, 730: 90, 1000: 100}}},
            "data_source": "config",
        },

        # --- Liquidity & Throughput (20%) ---
        "bridge_tvl": {
            "name": "Bridge TVL (USD)",
            "category": "liquidity_throughput",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {1000000: 10, 10000000: 30, 100000000: 50, 500000000: 70, 1000000000: 85, 5000000000: 100}}},
            "data_source": "defillama",
        },
        "daily_volume": {
            "name": "Daily Bridge Volume (USD)",
            "category": "liquidity_throughput",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {100000: 10, 1000000: 30, 10000000: 50, 100000000: 70, 1000000000: 100}}},
            "data_source": "defillama",
        },
        "volume_tvl_ratio": {
            "name": "Volume / TVL Ratio",
            "category": "liquidity_throughput",
            "weight": 0.15,
            "normalization": {"function": "linear", "params": {"min_val": 0.01, "max_val": 1.0}},
            "data_source": "calculated",
        },
        "supported_chains": {
            "name": "Supported Chain Count",
            "category": "liquidity_throughput",
            "weight": 0.20,
            "normalization": {"function": "log", "params": {"thresholds": {2: 20, 5: 40, 10: 60, 20: 80, 50: 100}}},
            "data_source": "defillama",
        },
        "token_coverage": {
            "name": "Supported Token Count",
            "category": "liquidity_throughput",
            "weight": 0.15,
            "normalization": {"function": "log", "params": {"thresholds": {5: 20, 10: 40, 20: 60, 50: 80, 100: 100}}},
            "data_source": "config",
        },

        # --- Smart Contract Risk (15%) ---
        "bridge_formal_verification": {
            "name": "Formal Verification Status",
            "category": "smart_contract_risk",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "bug_bounty_size": {
            "name": "Bug Bounty Size (USD)",
            "category": "smart_contract_risk",
            "weight": 0.20,
            "normalization": {"function": "log", "params": {"thresholds": {50000: 20, 250000: 40, 1000000: 60, 5000000: 80, 10000000: 100}}},
            "data_source": "config",
        },
        "contract_age_days": {
            "name": "Contract Age (days)",
            "category": "smart_contract_risk",
            "weight": 0.20,
            "normalization": {"function": "log", "params": {"thresholds": {30: 10, 90: 30, 180: 50, 365: 70, 730: 90, 1000: 100}}},
            "data_source": "config",
        },
        "bridge_dependency_risk": {
            "name": "Dependency Risk Score",
            "category": "smart_contract_risk",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "code_complexity": {
            "name": "Code Complexity Score",
            "category": "smart_contract_risk",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Decentralization (10%) ---
        "operator_geographic_diversity": {
            "name": "Geographic Diversity Score",
            "category": "decentralization",
            "weight": 0.30,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "validator_rotation": {
            "name": "Validator Set Rotation Score",
            "category": "decentralization",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "bridge_governance_mechanism": {
            "name": "Governance Mechanism Score",
            "category": "decentralization",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "token_holder_concentration": {
            "name": "Token Holder Concentration (%)",
            "category": "decentralization",
            "weight": 0.20,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 10, "threshold": 80}},
            "data_source": "etherscan",
        },

        # --- Economic Security (10%) ---
        "cost_to_attack": {
            "name": "Estimated Cost to Attack (USD)",
            "category": "economic_security",
            "weight": 0.35,
            "normalization": {"function": "log", "params": {"thresholds": {1000000: 10, 10000000: 30, 100000000: 60, 1000000000: 80, 10000000000: 100}}},
            "data_source": "config",
        },
        "slashing_mechanism": {
            "name": "Slashing Mechanism Score",
            "category": "economic_security",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "bridge_insurance": {
            "name": "Insurance / Coverage Score",
            "category": "economic_security",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "restaking_security": {
            "name": "Restaking Security Score",
            "category": "economic_security",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
    },
}

BRIDGE_ENTITIES = [
    {"slug": "wormhole", "name": "Wormhole", "defillama_id": "wormhole", "token_contract": "0xB0fFa8000886e57F86dd5264b987B9a091e82984"},  # W token
    {"slug": "layerzero", "name": "LayerZero", "defillama_id": "layerzero", "token_contract": "0x6985884C4392D348587B19cb9eAAf157F13271cd"},  # ZRO token
    {"slug": "axelar", "name": "Axelar", "defillama_id": "axelar", "token_contract": "0x467719aD09025FcC6cF6F8311755809d45a5E5f3"},  # AXL token
    {"slug": "circle-cctp", "name": "Circle CCTP", "defillama_id": "circle-cctp"},
    {"slug": "across", "name": "Across Protocol", "defillama_id": "across"},
    {"slug": "stargate", "name": "Stargate", "defillama_id": "stargate"},
    {"slug": "synapse", "name": "Synapse", "defillama_id": "synapse"},
    {"slug": "debridge", "name": "deBridge", "defillama_id": "debridge"},
    {"slug": "celer-cbridge", "name": "Celer cBridge", "defillama_id": "celer"},
]
