"""
VSRI v0.1.0 — Vault/Yield Strategy Risk Index
================================================
Measures strategy transparency, performance, liquidity risk,
smart contract risk, underlying asset quality, and operational risk
for DeFi vault/yield products.
"""

VSRI_V01_DEFINITION = {
    "index_id": "vsri",
    "version": "v0.1.0",
    "name": "Vault/Yield Strategy Risk Index",
    "description": "Risk scoring for DeFi vault and yield strategy products",
    "entity_type": "vault",
    "categories": {
        "strategy_transparency": {"name": "Strategy Transparency", "weight": 0.20},
        "performance_volatility": {"name": "Performance & Volatility", "weight": 0.20},
        "liquidity_risk": {"name": "Liquidity Risk", "weight": 0.15},
        "smart_contract_risk": {"name": "Smart Contract Risk", "weight": 0.15},
        "underlying_quality": {"name": "Underlying Asset Quality", "weight": 0.15},
        "operational_risk": {"name": "Operational Risk", "weight": 0.15},
    },
    "components": {
        # --- Strategy Transparency (20%) ---
        "strategy_description_avail": {
            "name": "Strategy Description Availability",
            "category": "strategy_transparency",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "strategy_code_public": {
            "name": "Strategy Code Public/Audited",
            "category": "strategy_transparency",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "parameter_visibility": {
            "name": "Parameter Visibility Score",
            "category": "strategy_transparency",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "rebalance_logic_documented": {
            "name": "Rebalance Logic Documented",
            "category": "strategy_transparency",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "risk_disclosure": {
            "name": "Risk Disclosure Score",
            "category": "strategy_transparency",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Performance & Volatility (20%) ---
        "apy_7d": {
            "name": "7-Day APY (%)",
            "category": "performance_volatility",
            "weight": 0.20,
            "normalization": {"function": "linear", "params": {"min_val": 0, "max_val": 20}},
            "data_source": "defillama",
        },
        "apy_30d": {
            "name": "30-Day APY (%)",
            "category": "performance_volatility",
            "weight": 0.25,
            "normalization": {"function": "linear", "params": {"min_val": 0, "max_val": 15}},
            "data_source": "defillama",
        },
        "apy_volatility": {
            "name": "APY Volatility (std dev)",
            "category": "performance_volatility",
            "weight": 0.20,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 10}},
            "data_source": "defillama",
        },
        "max_drawdown": {
            "name": "Maximum Drawdown (%)",
            "category": "performance_volatility",
            "weight": 0.20,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 20}},
            "data_source": "defillama",
        },
        "il_exposure": {
            "name": "Impermanent Loss Exposure Score",
            "category": "performance_volatility",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Liquidity Risk (15%) ---
        "vault_tvl": {
            "name": "Vault TVL (USD)",
            "category": "liquidity_risk",
            "weight": 0.30,
            "normalization": {"function": "log", "params": {"thresholds": {100000: 10, 1000000: 30, 10000000: 50, 100000000: 70, 1000000000: 100}}},
            "data_source": "defillama",
        },
        "withdrawal_delay": {
            "name": "Withdrawal Queue/Delay Score",
            "category": "liquidity_risk",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "deposit_concentration": {
            "name": "Top Depositor Concentration (%)",
            "category": "liquidity_risk",
            "weight": 0.25,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 10, "threshold": 80}},
            "data_source": "etherscan",
        },
        "position_liquidity": {
            "name": "Underlying Position Liquidity Score",
            "category": "liquidity_risk",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Smart Contract Risk (15%) ---
        "vault_audit_status": {
            "name": "Audit Status Score",
            "category": "smart_contract_risk",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {1: 30, 2: 50, 3: 70, 5: 85, 10: 100}}},
            "data_source": "config",
        },
        "vault_contract_age_days": {
            "name": "Contract Age (days)",
            "category": "smart_contract_risk",
            "weight": 0.20,
            "normalization": {"function": "log", "params": {"thresholds": {30: 10, 90: 30, 180: 50, 365: 70, 730: 100}}},
            "data_source": "config",
        },
        "vault_upgrade_mechanism": {
            "name": "Upgrade Mechanism Score",
            "category": "smart_contract_risk",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "dependency_chain_depth": {
            "name": "Dependency Chain Depth Score",
            "category": "smart_contract_risk",
            "weight": 0.20,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 1, "threshold": 5}},
            "data_source": "config",
        },
        "composability_risk": {
            "name": "Composability Risk Score",
            "category": "smart_contract_risk",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Underlying Asset Quality (15%) — reads from existing CQI ---
        "underlying_sii_score": {
            "name": "Underlying Asset SII Score",
            "category": "underlying_quality",
            "weight": 0.35,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "cqi_lookup",
        },
        "underlying_psi_score": {
            "name": "Underlying Protocol PSI Score",
            "category": "underlying_quality",
            "weight": 0.30,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "cqi_lookup",
        },
        "collateral_diversity": {
            "name": "Collateral Type Diversity Score",
            "category": "underlying_quality",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "correlation_risk": {
            "name": "Correlation Risk Score",
            "category": "underlying_quality",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Operational Risk (15%) ---
        "curator_track_record": {
            "name": "Curator/Strategist Track Record",
            "category": "operational_risk",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "rebalance_frequency": {
            "name": "Rebalance Frequency Score",
            "category": "operational_risk",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "strategy_change_history": {
            "name": "Historical Strategy Changes",
            "category": "operational_risk",
            "weight": 0.20,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 10}},
            "data_source": "config",
        },
        "vault_incident_history": {
            "name": "Incident History Score",
            "category": "operational_risk",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "fee_transparency": {
            "name": "Fee Transparency Score",
            "category": "operational_risk",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
    },
}

VAULT_ENTITIES = [
    # Yearn Finance
    {"slug": "yearn-usdc", "name": "Yearn USDC Vault", "protocol": "yearn-finance", "pool_id": None},
    {"slug": "yearn-dai", "name": "Yearn DAI Vault", "protocol": "yearn-finance", "pool_id": None},
    {"slug": "yearn-eth", "name": "Yearn ETH Vault", "protocol": "yearn-finance", "pool_id": None},
    # Morpho
    {"slug": "morpho-usdc-aave", "name": "Morpho USDC (Aave)", "protocol": "morpho", "pool_id": None},
    {"slug": "morpho-eth-aave", "name": "Morpho ETH (Aave)", "protocol": "morpho", "pool_id": None},
    # Beefy Finance
    {"slug": "beefy-usdc-eth", "name": "Beefy USDC-ETH", "protocol": "beefy-finance", "pool_id": None},
    {"slug": "beefy-usdt-usdc", "name": "Beefy USDT-USDC", "protocol": "beefy-finance", "pool_id": None},
    # Pendle
    {"slug": "pendle-steth-dec25", "name": "Pendle stETH Dec 2025", "protocol": "pendle", "pool_id": None},
    {"slug": "pendle-eeth-dec25", "name": "Pendle eETH Dec 2025", "protocol": "pendle", "pool_id": None},
    # Sommelier
    {"slug": "sommelier-turbo-steth", "name": "Sommelier Turbo stETH", "protocol": "sommelier", "pool_id": None},
]
