"""
Wallet Indexer — Configuration
==============================
Known stablecoin contracts: scored (from SII registry) and common unscored.
Size tier and coverage quality thresholds.
"""

import os

from app.config import STABLECOIN_REGISTRY

# =============================================================================
# Scored stablecoins — built dynamically from the SII registry
# =============================================================================
# Map: lowercased contract address → { stablecoin_id, symbol, decimals }

SCORED_CONTRACTS = {}
for sid, cfg in STABLECOIN_REGISTRY.items():
    contract = cfg.get("contract", "")
    if contract:
        SCORED_CONTRACTS[contract.lower()] = {
            "stablecoin_id": sid,
            "symbol": cfg["symbol"],
            "decimals": cfg.get("decimals", 18),
            "name": cfg["name"],
        }

# =============================================================================
# Common unscored stablecoins — tracked in backlog
# =============================================================================

UNSCORED_CONTRACTS = {
    "0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f": {
        "symbol": "GHO", "name": "GHO", "decimals": 18,
        "coingecko_id": "gho",
    },
    "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e": {
        "symbol": "crvUSD", "name": "Curve USD", "decimals": 18,
        "coingecko_id": "crvusd",
    },
    "0x5f98805a4e8be255a32880fdec7f6728c6568ba0": {
        "symbol": "LUSD", "name": "Liquity USD", "decimals": 18,
        "coingecko_id": "liquity-usd",
    },
    "0x57ab1ec28d129707052df4df418d58a2d46d5f51": {
        "symbol": "sUSD", "name": "Synthetix USD", "decimals": 18,
        "coingecko_id": "nusd",
    },
    "0x865377367054516e17014ccded1e7d814edc9ce4": {
        "symbol": "DOLA", "name": "Dola USD", "decimals": 18,
        "coingecko_id": "dola-usd",
    },
    "0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3": {
        "symbol": "MIM", "name": "Magic Internet Money", "decimals": 18,
        "coingecko_id": "magic-internet-money",
    },
    "0xdb25f211ab05b1c97d595516f45d248390d6bfa5": {
        "symbol": "EURS", "name": "STASIS EURO", "decimals": 2,
        "coingecko_id": "stasis-eurs",
    },
    "0x8e870d67f660d95d5be530380d0ec0bd388289e1": {
        "symbol": "USDP", "name": "Pax Dollar", "decimals": 18,
        "coingecko_id": "paxos-standard",
    },
    "0x056fd409e1d7a124bd7017459dfea2f387b6d5cd": {
        "symbol": "GUSD", "name": "Gemini Dollar", "decimals": 2,
        "coingecko_id": "gemini-dollar",
    },
    "0x03ab458634910aad20ef5f1c8ee96f1d6ac54919": {
        "symbol": "RAI", "name": "Rai Reflex Index", "decimals": 18,
        "coingecko_id": "rai",
    },
}

# Combined lookup: all known stablecoin contracts (lowercased)
ALL_KNOWN_CONTRACTS = {**SCORED_CONTRACTS, **UNSCORED_CONTRACTS}

# =============================================================================
# Thresholds
# =============================================================================

SIZE_TIER_THRESHOLDS = [
    (10_000_000, "whale"),
    (100_000, "institutional"),
    (0, "retail"),
]

COVERAGE_QUALITY_THRESHOLDS = [
    (0.0, "full"),
    (10.0, "high"),
    (40.0, "partial"),
]
# anything > 40% → "low"

FORMULA_VERSION = "wallet-v1.0.0"

BLOCK_EXPLORER_PROVIDER = os.environ.get("BLOCK_EXPLORER_PROVIDER", "blockscout").lower()

if BLOCK_EXPLORER_PROVIDER == "etherscan":
    EXPLORER_RATE_LIMIT_DELAY = 0.11  # ~9 req/sec (Etherscan Standard: 10/sec)
else:
    EXPLORER_RATE_LIMIT_DELAY = 0.22  # ~4.5 req/sec (Blockscout Free: 5/sec)
    # Bump to 0.07 (~14 req/sec) if on Blockscout Builder ($49/mo, 15 RPS)


# =============================================================================
# Multi-chain configuration — Blockscout instances + stablecoin contracts
# =============================================================================
# Ethereum contracts come from get_all_known_contracts() (dynamic from DB).
# L2 contracts are hardcoded until the stablecoins table gains a chain column.
# TODO: query L2 contracts from DB once stablecoins table supports multi-chain

CHAIN_CONFIGS = {
    "ethereum": {
        "explorer_base": "https://eth.blockscout.com/api",
        "chain_id": 1,
        # stablecoin_contracts populated dynamically via get_chain_contracts()
    },
    "base": {
        "explorer_base": "https://base.blockscout.com/api",
        "chain_id": 8453,
        "stablecoin_contracts": {
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": {"symbol": "USDC", "decimals": 6, "stablecoin_id": "usdc"},
            "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca": {"symbol": "USDbC", "decimals": 6, "stablecoin_id": "usdc"},
            "0x50c5725949a6f0c72e6c4a641f24049a917db0cb": {"symbol": "DAI", "decimals": 18, "stablecoin_id": "dai"},
        },
    },
    "arbitrum": {
        "explorer_base": "https://arbitrum.blockscout.com/api",
        "chain_id": 42161,
        "stablecoin_contracts": {
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831": {"symbol": "USDC", "decimals": 6, "stablecoin_id": "usdc"},
            "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": {"symbol": "USDC.e", "decimals": 6, "stablecoin_id": "usdc"},
            "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": {"symbol": "USDT", "decimals": 6, "stablecoin_id": "usdt"},
            "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": {"symbol": "DAI", "decimals": 18, "stablecoin_id": "dai"},
        },
    },
}

SUPPORTED_CHAINS = list(CHAIN_CONFIGS.keys())


def get_chain_contracts(chain: str) -> dict:
    """
    Get stablecoin contracts for a specific chain.
    Ethereum: dynamic from DB. L2s: static config.
    """
    if chain == "ethereum":
        scored, all_known = get_all_known_contracts()
        return scored
    cfg = CHAIN_CONFIGS.get(chain, {})
    return cfg.get("stablecoin_contracts", {})


def get_all_known_contracts() -> tuple[dict, dict]:
    """
    Build the contract registry at runtime from the database.

    Queries the stablecoins table so newly promoted coins (not in the static
    STABLECOIN_REGISTRY) are included in wallet balance scans.

    Returns:
        (scored_contracts, all_known_contracts) — same shape as the module-level
        SCORED_CONTRACTS and ALL_KNOWN_CONTRACTS dicts. Falls back to the static
        dicts if the DB query fails.
    """
    try:
        from app.database import fetch_all
        rows = fetch_all(
            "SELECT id, symbol, name, contract, decimals "
            "FROM stablecoins WHERE contract IS NOT NULL AND contract != ''"
        )
        db_scored: dict = {}
        for row in rows:
            contract = (row.get("contract") or "").lower().strip()
            if not contract:
                continue
            db_scored[contract] = {
                "stablecoin_id": row["id"],
                "symbol": row.get("symbol", "???"),
                "decimals": row.get("decimals") or 18,
                "name": row.get("name", ""),
            }
        # Merge: DB-scored coins take precedence; unscored are always static
        all_contracts = {**UNSCORED_CONTRACTS, **db_scored}
        return db_scored, all_contracts
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(
            f"get_all_known_contracts DB query failed, using static fallback: {e}"
        )
        return SCORED_CONTRACTS, ALL_KNOWN_CONTRACTS


def classify_size_tier(total_value: float) -> str:
    """Classify wallet by total stablecoin value."""
    for threshold, tier in SIZE_TIER_THRESHOLDS:
        if total_value >= threshold:
            return tier
    return "retail"


def classify_coverage(unscored_pct: float) -> str:
    """Classify coverage quality by unscored percentage."""
    for threshold, quality in COVERAGE_QUALITY_THRESHOLDS:
        if unscored_pct <= threshold:
            return quality
    return "low"
