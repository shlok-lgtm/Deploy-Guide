"""
LSTI v0.1.0 — Liquid Staking Token Integrity Index
====================================================
Measures peg stability, liquidity, validator/operator health,
distribution, smart contract risk, and withdrawal behavior for LSTs.

LSTs behave like stablecoins pegged to ETH rather than USD.
Reuses SII component structure where it maps.
"""

LSTI_V01_DEFINITION = {
    "index_id": "lsti",
    "version": "v0.1.0",
    "name": "Liquid Staking Token Integrity Index",
    "description": "Risk scoring for liquid staking tokens pegged to ETH",
    "entity_type": "lst",
    "categories": {
        "peg_stability": {"name": "Peg Stability (ETH)", "weight": 0.30},
        "liquidity": {"name": "Liquidity", "weight": 0.25},
        "validator_operator": {"name": "Validator/Operator", "weight": 0.15},
        "distribution": {"name": "Distribution", "weight": 0.10},
        "smart_contract": {"name": "Smart Contract", "weight": 0.10},
        "network_withdrawal": {"name": "Network/Withdrawal", "weight": 0.10},
    },
    "components": {
        # --- Peg Stability (30%) ---
        "eth_peg_deviation": {
            "name": "ETH Peg Deviation (%)",
            "category": "peg_stability",
            "weight": 0.30,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 5}},
            "data_source": "coingecko",
        },
        "peg_volatility_7d": {
            "name": "7d Peg Volatility (%)",
            "category": "peg_stability",
            "weight": 0.20,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 3}},
            "data_source": "coingecko",
        },
        "peg_volatility_30d": {
            "name": "30d Peg Volatility (%)",
            "category": "peg_stability",
            "weight": 0.15,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 5}},
            "data_source": "coingecko",
        },
        "dex_cex_spread": {
            "name": "DEX vs CEX Price Spread (%)",
            "category": "peg_stability",
            "weight": 0.15,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 2}},
            "data_source": "coingecko",
        },
        "exchange_price_variance": {
            "name": "Cross-Exchange Price Variance (%)",
            "category": "peg_stability",
            "weight": 0.20,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 3}},
            "data_source": "coingecko",
        },

        # --- Liquidity (25%) ---
        "market_cap": {
            "name": "Market Cap (USD)",
            "category": "liquidity",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {100000000: 20, 500000000: 40, 1000000000: 60, 5000000000: 80, 10000000000: 100}}},
            "data_source": "coingecko",
        },
        "dex_pool_depth": {
            "name": "DEX Pool Depth (USD)",
            "category": "liquidity",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {1000000: 10, 10000000: 30, 100000000: 60, 500000000: 80, 1000000000: 100}}},
            "data_source": "defillama",
        },
        "volume_cap_ratio": {
            "name": "Volume / Market Cap Ratio",
            "category": "liquidity",
            "weight": 0.20,
            "normalization": {"function": "linear", "params": {"min_val": 0.001, "max_val": 0.10}},
            "data_source": "coingecko",
        },
        "slippage_1m": {
            "name": "Slippage at $1M Swap (%)",
            "category": "liquidity",
            "weight": 0.15,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 5}},
            "data_source": "defillama",
        },
        "cross_chain_liquidity": {
            "name": "Cross-Chain Liquidity Count",
            "category": "liquidity",
            "weight": 0.15,
            "normalization": {"function": "log", "params": {"thresholds": {1: 20, 2: 40, 3: 60, 5: 80, 10: 100}}},
            "data_source": "defillama",
        },

        # --- Validator/Operator (15%) ---
        "validator_count": {
            "name": "Active Validator Count",
            "category": "validator_operator",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {10: 10, 50: 30, 200: 50, 1000: 70, 5000: 85, 10000: 100}}},
            "data_source": "beacon_chain",
        },
        "operator_diversity_hhi": {
            "name": "Operator Diversity (1 - HHI)",
            "category": "validator_operator",
            "weight": 0.25,
            "normalization": {"function": "linear", "params": {"min_val": 0.0, "max_val": 1.0}},
            "data_source": "rated_network",
        },
        "slashing_history": {
            "name": "Slashing Event Count (365d)",
            "category": "validator_operator",
            "weight": 0.20,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 10}},
            "data_source": "beacon_chain",
        },
        "attestation_rate": {
            "name": "Attestation Effectiveness (%)",
            "category": "validator_operator",
            "weight": 0.20,
            "normalization": {"function": "linear", "params": {"min_val": 90, "max_val": 100}},
            "data_source": "rated_network",
        },
        "slashing_insurance": {
            "name": "Slashing Insurance Coverage",
            "category": "validator_operator",
            "weight": 0.10,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Distribution (10%) ---
        "top_holder_concentration": {
            "name": "Top 10 Holder Share (%)",
            "category": "distribution",
            "weight": 0.30,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 10, "threshold": 80}},
            "data_source": "etherscan",
        },
        "holder_gini": {
            "name": "Holder Gini Coefficient",
            "category": "distribution",
            "weight": 0.25,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0.3, "threshold": 0.95}},
            "data_source": "etherscan",
        },
        "defi_protocol_share": {
            "name": "DeFi Protocol Holdings (%)",
            "category": "distribution",
            "weight": 0.25,
            "normalization": {"function": "linear", "params": {"min_val": 5, "max_val": 60}},
            "data_source": "defillama",
        },
        "exchange_concentration": {
            "name": "Exchange Concentration (%)",
            "category": "distribution",
            "weight": 0.20,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 5, "threshold": 50}},
            "data_source": "etherscan",
        },

        # --- Smart Contract (10%) ---
        "audit_status": {
            "name": "Audit Count",
            "category": "smart_contract",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {1: 30, 2: 50, 3: 70, 5: 85, 10: 100}}},
            "data_source": "config",
        },
        "upgradeability_risk": {
            "name": "Upgradeability Risk Score",
            "category": "smart_contract",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "admin_key_risk": {
            "name": "Admin Key Risk Score",
            "category": "smart_contract",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "withdrawal_queue_impl": {
            "name": "Withdrawal Queue Implementation Quality",
            "category": "smart_contract",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "exploit_history_lst": {
            "name": "Exploit History Score",
            "category": "smart_contract",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Network/Withdrawal (10%) ---
        "withdrawal_queue_length": {
            "name": "Withdrawal Queue Length (ETH)",
            "category": "network_withdrawal",
            "weight": 0.25,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 100000}},
            "data_source": "beacon_chain",
        },
        "avg_withdrawal_time": {
            "name": "Average Withdrawal Time (hours)",
            "category": "network_withdrawal",
            "weight": 0.25,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 168}},
            "data_source": "protocol_api",
        },
        "withdrawal_success_rate": {
            "name": "Withdrawal Success Rate (%)",
            "category": "network_withdrawal",
            "weight": 0.25,
            "normalization": {"function": "linear", "params": {"min_val": 90, "max_val": 100}},
            "data_source": "protocol_api",
        },
        "beacon_chain_dependency": {
            "name": "Beacon Chain Dependency Score",
            "category": "network_withdrawal",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "mev_exposure": {
            "name": "MEV Exposure Score",
            "category": "network_withdrawal",
            "weight": 0.10,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
    },
}

# Target LST entities to score
LST_ENTITIES = [
    {"slug": "lido-steth", "name": "Lido stETH", "symbol": "stETH", "coingecko_id": "staked-ether", "protocol": "Lido", "contract": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"},
    {"slug": "lido-wsteth", "name": "Lido wstETH", "symbol": "wstETH", "coingecko_id": "wrapped-steth", "protocol": "Lido", "contract": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"},
    {"slug": "rocket-pool-reth", "name": "Rocket Pool rETH", "symbol": "rETH", "coingecko_id": "rocket-pool-eth", "protocol": "Rocket Pool", "contract": "0xae78736Cd615f374D3085123A210448E74Fc6393"},
    {"slug": "coinbase-cbeth", "name": "Coinbase cbETH", "symbol": "cbETH", "coingecko_id": "coinbase-wrapped-staked-eth", "protocol": "Coinbase", "contract": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49BBa"},
    {"slug": "frax-sfrxeth", "name": "Frax sfrxETH", "symbol": "sfrxETH", "coingecko_id": "staked-frax-ether", "protocol": "Frax", "contract": "0xac3E018457B222d93114458476f3E3416Abbe38F"},
    {"slug": "mantle-meth", "name": "Mantle mETH", "symbol": "mETH", "coingecko_id": "mantle-staked-ether", "protocol": "Mantle", "contract": "0xd5F7838F5C461fefF7FE49ea5ebaF7728bB0ADfa"},
    {"slug": "swell-sweth", "name": "Swell swETH", "symbol": "swETH", "coingecko_id": "sweth", "protocol": "Swell", "contract": "0xf951E335afb289353dc249e82926178EaC7DEd78"},
    {"slug": "etherfi-eeth", "name": "EtherFi eETH", "symbol": "eETH", "coingecko_id": "ether-fi-staked-eth", "protocol": "EtherFi", "contract": "0x35fA164735182de50811E8e2E824cFb9B6118ac2"},
    {"slug": "etherfi-weeth", "name": "EtherFi weETH", "symbol": "weETH", "coingecko_id": "wrapped-eeth", "protocol": "EtherFi", "contract": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee"},
    {"slug": "kelp-rseth", "name": "Kelp rsETH", "symbol": "rsETH", "coingecko_id": "kelp-dao-restaked-eth", "protocol": "Kelp", "contract": "0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7"},
]
