"""
Basis Protocol - Configuration
Single source of truth for stablecoin registry, API keys, and environment settings.
"""

import os
from typing import Dict, Any, Optional


# =============================================================================
# Environment
# =============================================================================

DATABASE_URL = os.environ.get("DATABASE_URL", "")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
ALCHEMY_API_KEY = os.environ.get("ALCHEMY_API_KEY", "")

# API settings
API_HOST = os.environ.get("API_HOST", "0.0.0.0")
API_PORT = int(os.environ.get("API_PORT", "8000"))
_cors_raw = os.environ.get("CORS_ORIGINS", "*")
CORS_ORIGINS = ["*"] if _cors_raw.strip() == "*" else [s.strip() for s in _cors_raw.split(",")]

# Worker settings
COLLECTION_INTERVAL_MINUTES = int(os.environ.get("COLLECTION_INTERVAL", "60"))
SCORING_INTERVAL_MINUTES = int(os.environ.get("SCORING_INTERVAL", "60"))


# =============================================================================
# Stablecoin Registry (scoring-enabled only)
# =============================================================================

STABLECOIN_REGISTRY: Dict[str, Dict[str, Any]] = {
    "usdc": {
        "name": "USD Coin",
        "symbol": "USDC",
        "issuer": "Circle",
        "coingecko_id": "usd-coin",
        "contract": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "decimals": 6,
        "attestation": {
            "auditor": "Deloitte",
            "frequency": "monthly",
            "frequency_days": 35,
            "transparency_url": "https://www.circle.com/en/transparency",
        },
    },
    "usdt": {
        "name": "Tether",
        "symbol": "USDT",
        "issuer": "Tether",
        "coingecko_id": "tether",
        "contract": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "decimals": 6,
        "attestation": {
            "auditor": "BDO Italia",
            "frequency": "quarterly",
            "frequency_days": 100,
            "transparency_url": "https://tether.to/en/transparency/",
        },
    },
    "dai": {
        "name": "Dai",
        "symbol": "DAI",
        "issuer": "MakerDAO",
        "coingecko_id": "dai",
        "contract": "0x6b175474e89094c44da98b954eedeac495271d0f",
        "decimals": 18,
        "attestation": {
            "auditor": "N/A (on-chain)",
            "frequency": "real-time",
            "frequency_days": 1,
            "transparency_url": "https://daistats.com/",
        },
    },
    "frax": {
        "name": "Frax",
        "symbol": "FRAX",
        "issuer": "Frax Finance",
        "coingecko_id": "frax",
        "contract": "0x853d955acef822db058eb8505911ed77f175b99e",
        "decimals": 18,
        "attestation": {
            "auditor": "N/A (algorithmic)",
            "frequency": "real-time",
            "frequency_days": 1,
            "transparency_url": "https://facts.frax.finance/",
        },
    },
    "pyusd": {
        "name": "PayPal USD",
        "symbol": "PYUSD",
        "issuer": "Paxos",
        "coingecko_id": "paypal-usd",
        "contract": "0x6c3ea9036406852006290770bedfcaba0e23a0e8",
        "decimals": 6,
        "attestation": {
            "auditor": "WithumSmith+Brown",
            "frequency": "monthly",
            "frequency_days": 35,
            "transparency_url": "https://www.paxos.com/pyusd-transparency/",
        },
    },
    "fdusd": {
        "name": "First Digital USD",
        "symbol": "FDUSD",
        "issuer": "First Digital",
        "coingecko_id": "first-digital-usd",
        "contract": "0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409",
        "decimals": 18,
        "attestation": {
            "auditor": "Prescient Assurance",
            "frequency": "monthly",
            "frequency_days": 35,
            "transparency_url": "https://www.firstdigitallabs.com/transparency",
        },
    },
    "tusd": {
        "name": "TrueUSD",
        "symbol": "TUSD",
        "issuer": "Archblock",
        "coingecko_id": "true-usd",
        "contract": "0x0000000000085d4780B73119b644AE5ecd22b376",
        "decimals": 18,
        "attestation": {
            "auditor": "The Network Firm",
            "frequency": "monthly",
            "frequency_days": 35,
            "transparency_url": "https://real-time-attest.trustexplorer.io/truecurrencies",
        },
    },
    "usdd": {
        "name": "USDD",
        "symbol": "USDD",
        "issuer": "TRON DAO",
        "coingecko_id": "usdd",
        "contract": "0x0C10bF8FcB7Bf5412187A595ab97a3609160b5c6",
        "decimals": 18,
        "attestation": {
            "auditor": "N/A",
            "frequency": "quarterly",
            "frequency_days": 100,
            "transparency_url": "https://usdd.io/#/",
        },
    },
    "usde": {
        "name": "Ethena USDe",
        "symbol": "USDe",
        "issuer": "Ethena Labs",
        "coingecko_id": "ethena-usde",
        "contract": "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
        "decimals": 18,
        "attestation": {
            "auditor": "Various Custodians",
            "frequency": "weekly",
            "frequency_days": 7,
            "transparency_url": "https://docs.ethena.fi/resources/custodian-attestations",
        },
    },
}


def get_scoring_ids() -> list[str]:
    """Get list of stablecoin IDs that have scoring enabled."""
    return list(STABLECOIN_REGISTRY.keys())


def get_coingecko_id(stablecoin_id: str) -> Optional[str]:
    """Get CoinGecko ID for a stablecoin."""
    cfg = STABLECOIN_REGISTRY.get(stablecoin_id)
    return cfg["coingecko_id"] if cfg else None


def get_contract(stablecoin_id: str) -> Optional[str]:
    """Get Ethereum contract address for a stablecoin."""
    cfg = STABLECOIN_REGISTRY.get(stablecoin_id)
    return cfg.get("contract") if cfg else None
