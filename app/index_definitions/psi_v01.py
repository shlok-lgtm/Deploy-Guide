"""
PSI v0.1.0 — Protocol Solvency Index
======================================
Measures financial health and operational resilience of DeFi protocols.
Uses the same scoring engine as SII with different categories, weights,
components, and data sources.
"""

PSI_V01_DEFINITION = {
    "index_id": "psi",
    "version": "v0.1.0",
    "name": "Protocol Solvency Index",
    "description": "Measures financial health and operational resilience of DeFi protocols",
    "entity_type": "protocol",
    "categories": {
        "balance_sheet": {"name": "Balance Sheet & Reserves", "weight": 0.25},
        "revenue": {"name": "Revenue & Sustainability", "weight": 0.20},
        "liquidity": {"name": "Liquidity & Utilization", "weight": 0.20},
        "security": {"name": "Security & Audit", "weight": 0.15},
        "governance": {"name": "Governance & Decentralization", "weight": 0.10},
        "token_health": {"name": "Token Health", "weight": 0.10},
    },
    "components": {
        "tvl": {
            "name": "Total Value Locked",
            "category": "balance_sheet",
            "weight": 0.30,
            "normalization": {"function": "log", "params": {"thresholds": {1000000: 10, 10000000: 30, 100000000: 50, 1000000000: 70, 5000000000: 85, 10000000000: 100}}},
            "data_source": "defillama"
        },
        "tvl_7d_change": {
            "name": "TVL 7-Day Change (%)",
            "category": "balance_sheet",
            "weight": 0.15,
            "normalization": {"function": "centered", "params": {"center": 0, "tolerance": 5, "extreme": 30}},
            "data_source": "defillama"
        },
        "tvl_30d_change": {
            "name": "TVL 30-Day Change (%)",
            "category": "balance_sheet",
            "weight": 0.15,
            "normalization": {"function": "centered", "params": {"center": 0, "tolerance": 10, "extreme": 50}},
            "data_source": "defillama"
        },
        "chain_count": {
            "name": "Multi-Chain Deployment Count",
            "category": "balance_sheet",
            "weight": 0.15,
            "normalization": {"function": "log", "params": {"thresholds": {1: 20, 2: 40, 3: 60, 5: 80, 10: 100}}},
            "data_source": "defillama"
        },
        "tvl_concentration": {
            "name": "Chain Concentration (top chain %)",
            "category": "balance_sheet",
            "weight": 0.10,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 30, "threshold": 100}},
            "data_source": "defillama"
        },
        "treasury_total_usd": {
            "name": "Protocol Treasury (USD)",
            "category": "balance_sheet",
            "weight": 0.10,
            "normalization": {"function": "log", "params": {"thresholds": {1000000: 20, 10000000: 40, 100000000: 70, 1000000000: 100}}},
            "data_source": "defillama_treasury"
        },
        "bad_debt_ratio": {
            "name": "Bad Debt / TVL Ratio (%)",
            "category": "balance_sheet",
            "weight": 0.05,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 5}},
            "data_source": "config"
        },
        "fees_30d": {
            "name": "Protocol Fees (30d USD)",
            "category": "revenue",
            "weight": 0.30,
            "normalization": {"function": "log", "params": {"thresholds": {10000: 10, 100000: 30, 1000000: 60, 10000000: 80, 100000000: 100}}},
            "data_source": "defillama_fees"
        },
        "revenue_30d": {
            "name": "Protocol Revenue (30d USD)",
            "category": "revenue",
            "weight": 0.35,
            "normalization": {"function": "log", "params": {"thresholds": {10000: 10, 100000: 30, 1000000: 60, 10000000: 80, 100000000: 100}}},
            "data_source": "defillama_fees"
        },
        "fees_tvl_ratio": {
            "name": "Fees / TVL Ratio (annualized)",
            "category": "revenue",
            "weight": 0.20,
            "normalization": {"function": "linear", "params": {"min_val": 0.001, "max_val": 0.10}},
            "data_source": "calculated"
        },
        "fees_tvl_efficiency": {
            "name": "Revenue / TVL Efficiency (annualized)",
            "category": "revenue",
            "weight": 0.15,
            "normalization": {"function": "linear", "params": {"min_val": 0.001, "max_val": 0.05}},
            "data_source": "calculated"
        },
        "protocol_dex_tvl": {
            "name": "Protocol Liquidity (TVL)",
            "category": "liquidity",
            "weight": 0.40,
            "normalization": {"function": "log", "params": {"thresholds": {1000000: 10, 10000000: 30, 100000000: 50, 1000000000: 70, 5000000000: 85, 10000000000: 100}}},
            "data_source": "defillama"
        },
        "utilization_rate": {
            "name": "Utilization Rate (%)",
            "category": "liquidity",
            "weight": 0.35,
            "normalization": {"function": "centered", "params": {"center": 55, "tolerance": 15, "extreme": 45}},
            "data_source": "defillama"
        },
        "pool_depth": {
            "name": "Pool / Market Depth",
            "category": "liquidity",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {3: 20, 5: 40, 10: 60, 20: 80, 50: 100}}},
            "data_source": "defillama"
        },
        "audit_count": {
            "name": "Number of Security Audits",
            "category": "security",
            "weight": 0.40,
            "normalization": {"function": "log", "params": {"thresholds": {1: 30, 2: 50, 3: 70, 5: 85, 10: 100}}},
            "data_source": "defillama"
        },
        "audit_recency_days": {
            "name": "Days Since Last Audit",
            "category": "security",
            "weight": 0.30,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 730}},
            "data_source": "defillama"
        },
        "protocol_admin_key_risk": {
            "name": "Admin Key Risk Assessment",
            "category": "security",
            "weight": 0.30,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config"
        },
        "governance_token_holders": {
            "name": "Governance Token Holder Count",
            "category": "governance",
            "weight": 0.50,
            "normalization": {"function": "log", "params": {"thresholds": {100: 10, 1000: 30, 10000: 50, 100000: 80, 1000000: 100}}},
            "data_source": "coingecko"
        },
        "governance_proposals_90d": {
            "name": "Governance Proposals (90 days)",
            "category": "governance",
            "weight": 0.50,
            "normalization": {"function": "log", "params": {"thresholds": {1: 20, 3: 40, 5: 60, 10: 80, 20: 100}}},
            "data_source": "manual"
        },
        "token_mcap": {
            "name": "Governance Token Market Cap",
            "category": "token_health",
            "weight": 0.40,
            "normalization": {"function": "log", "params": {"thresholds": {1000000: 10, 10000000: 30, 100000000: 50, 1000000000: 70, 10000000000: 100}}},
            "data_source": "coingecko"
        },
        "token_volume_24h": {
            "name": "Token 24h Trading Volume",
            "category": "token_health",
            "weight": 0.15,
            "normalization": {"function": "log", "params": {"thresholds": {100000: 10, 1000000: 30, 10000000: 60, 100000000: 80, 1000000000: 100}}},
            "data_source": "coingecko"
        },
        "mcap_tvl_ratio": {
            "name": "Market Cap / TVL Ratio",
            "category": "token_health",
            "weight": 0.15,
            "normalization": {"function": "centered", "params": {"center": 1.0, "tolerance": 0.5, "extreme": 5.0}},
            "data_source": "calculated"
        },
        "token_price_volatility_30d": {
            "name": "Token 30-Day Price Volatility (%)",
            "category": "token_health",
            "weight": 0.15,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 50}},
            "data_source": "coingecko"
        },
        "token_liquidity_depth": {
            "name": "Token Volume / Market Cap Ratio",
            "category": "token_health",
            "weight": 0.15,
            "normalization": {"function": "linear", "params": {"min_val": 0.01, "max_val": 0.15}},
            "data_source": "coingecko"
        },
    },
}

TARGET_PROTOCOLS = [
    "aave", "lido", "eigenlayer", "sky", "compound-finance",
    "uniswap", "curve-finance", "morpho", "spark", "convex-finance",
    "drift", "jupiter-perpetual-exchange", "raydium",
]
