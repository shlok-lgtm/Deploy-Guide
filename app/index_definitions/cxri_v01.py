"""
CXRI v0.1.0 — CEX Reserve Integrity Index
============================================
Measures reserve proof quality, composition, regulatory status,
operational track record, transparency, and on-chain signals
for centralized exchanges.
"""

CXRI_V01_DEFINITION = {
    "index_id": "cxri",
    "version": "v0.1.0",
    "name": "CEX Reserve Integrity Index",
    "description": "Reserve integrity and operational risk scoring for centralized exchanges",
    "entity_type": "cex",
    "categories": {
        "reserve_proof_quality": {"name": "Reserve Proof Quality", "weight": 0.25},
        "reserve_composition": {"name": "Reserve Composition", "weight": 0.20},
        "regulatory_status": {"name": "Regulatory Status", "weight": 0.15},
        "operational_track_record": {"name": "Operational Track Record", "weight": 0.15},
        "cex_transparency": {"name": "Transparency", "weight": 0.15},
        "onchain_signals": {"name": "On-Chain Signals", "weight": 0.10},
    },
    "components": {
        # --- Reserve Proof Quality (25%) ---
        "por_method": {
            "name": "Proof-of-Reserves Method Score",
            "category": "reserve_proof_quality",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "por_frequency": {
            "name": "PoR Proof Frequency Score",
            "category": "reserve_proof_quality",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "por_recency_days": {
            "name": "PoR Recency (days since last)",
            "category": "reserve_proof_quality",
            "weight": 0.15,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 180}},
            "data_source": "config",
        },
        "auditor_reputation": {
            "name": "Auditor Reputation Score",
            "category": "reserve_proof_quality",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "liabilities_included": {
            "name": "Liabilities Inclusion Score",
            "category": "reserve_proof_quality",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "negative_balance_detection": {
            "name": "Negative Balance Detection Score",
            "category": "reserve_proof_quality",
            "weight": 0.10,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Reserve Composition (20%) ---
        "reserve_asset_diversity": {
            "name": "Asset Diversity Score",
            "category": "reserve_composition",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "defillama",
        },
        "stablecoin_reserve_pct": {
            "name": "Stablecoin % of Reserves",
            "category": "reserve_composition",
            "weight": 0.20,
            "normalization": {"function": "linear", "params": {"min_val": 5, "max_val": 40}},
            "data_source": "defillama",
        },
        "native_token_pct": {
            "name": "Native Token % (Self-Collateralization Risk)",
            "category": "reserve_composition",
            "weight": 0.25,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 30}},
            "data_source": "defillama",
        },
        "quality_asset_pct": {
            "name": "BTC+ETH % (Quality Assets)",
            "category": "reserve_composition",
            "weight": 0.20,
            "normalization": {"function": "linear", "params": {"min_val": 20, "max_val": 80}},
            "data_source": "defillama",
        },
        "unlabeled_asset_pct": {
            "name": "Unknown/Unlabeled Asset %",
            "category": "reserve_composition",
            "weight": 0.15,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 20}},
            "data_source": "defillama",
        },

        # --- Regulatory Status (15%) ---
        "license_count": {
            "name": "CASP/VASP License Count",
            "category": "regulatory_status",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {1: 20, 3: 40, 5: 60, 10: 80, 20: 100}}},
            "data_source": "config",
        },
        "mica_status": {
            "name": "MiCA Compliance Status",
            "category": "regulatory_status",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "us_licensing": {
            "name": "US State Licensing Score",
            "category": "regulatory_status",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "enforcement_history": {
            "name": "Enforcement Action History Score",
            "category": "regulatory_status",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "jurisdiction_quality": {
            "name": "Jurisdiction Quality Score",
            "category": "regulatory_status",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Operational Track Record (15%) ---
        "years_in_operation": {
            "name": "Years in Operation",
            "category": "operational_track_record",
            "weight": 0.25,
            "normalization": {"function": "log", "params": {"thresholds": {1: 20, 2: 40, 3: 60, 5: 80, 10: 100}}},
            "data_source": "config",
        },
        "withdrawal_freeze_count": {
            "name": "Historical Withdrawal Freeze Events",
            "category": "operational_track_record",
            "weight": 0.25,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 5}},
            "data_source": "config",
        },
        "security_breach_count": {
            "name": "Known Security Breaches",
            "category": "operational_track_record",
            "weight": 0.25,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 5}},
            "data_source": "config",
        },
        "insurance_coverage": {
            "name": "Insurance Coverage Score",
            "category": "operational_track_record",
            "weight": 0.15,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "fund_segregation": {
            "name": "Customer Fund Segregation Score",
            "category": "operational_track_record",
            "weight": 0.10,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- Transparency (15%) ---
        "public_audit_reports": {
            "name": "Public Audit Reports Score",
            "category": "cex_transparency",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "realtime_reserve_dashboard": {
            "name": "Real-Time Reserve Dashboard Score",
            "category": "cex_transparency",
            "weight": 0.25,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "api_availability": {
            "name": "API Availability Score",
            "category": "cex_transparency",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },
        "corporate_disclosure": {
            "name": "Corporate Structure Disclosure Score",
            "category": "cex_transparency",
            "weight": 0.30,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "config",
        },

        # --- On-Chain Signals (10%) ---
        "known_wallet_balance": {
            "name": "Known Wallet Total Balance (USD)",
            "category": "onchain_signals",
            "weight": 0.30,
            "normalization": {"function": "log", "params": {"thresholds": {100000000: 10, 1000000000: 30, 5000000000: 50, 10000000000: 70, 50000000000: 100}}},
            "data_source": "etherscan",
        },
        "hot_cold_ratio": {
            "name": "Hot/Cold Wallet Ratio Score",
            "category": "onchain_signals",
            "weight": 0.25,
            "normalization": {"function": "centered", "params": {"center": 10, "tolerance": 5, "extreme": 30}},
            "data_source": "etherscan",
        },
        "large_withdrawal_zscore": {
            "name": "Large Withdrawal Z-Score",
            "category": "onchain_signals",
            "weight": 0.25,
            "normalization": {"function": "inverse_linear", "params": {"perfect": 0, "threshold": 3}},
            "data_source": "etherscan",
        },
        "unusual_outflow_score": {
            "name": "Unusual Outflow Detection Score",
            "category": "onchain_signals",
            "weight": 0.20,
            "normalization": {"function": "direct", "params": {}},
            "data_source": "derived",
        },
    },
}

CEX_ENTITIES = [
    {"slug": "binance", "name": "Binance", "coingecko_id": "binance"},
    {"slug": "okx", "name": "OKX", "coingecko_id": "okx"},
    {"slug": "bybit", "name": "Bybit", "coingecko_id": "bybit"},
    {"slug": "bitget", "name": "Bitget", "coingecko_id": "bitget"},
    {"slug": "kraken", "name": "Kraken", "coingecko_id": "kraken"},
    {"slug": "coinbase", "name": "Coinbase", "coingecko_id": "gdax"},
    {"slug": "gate-io", "name": "Gate.io", "coingecko_id": "gate"},
    {"slug": "kucoin", "name": "KuCoin", "coingecko_id": "kucoin"},
]
