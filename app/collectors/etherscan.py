"""
Etherscan Collector
====================
Fetches holder distribution data for stablecoins using Etherscan V2 API.
Uses tokenbalance endpoint with a curated registry of labeled major holder
addresses (exchanges, DeFi protocols, bridges, treasuries) to calculate:

  - top_10_concentration: % of supply held by top 10 known addresses
  - unique_holders:       estimated unique holder count
  - exchange_concentration: % of supply held by known exchange addresses

All addresses are labeled for meaningful provenance/audit data.
"""

import os
import asyncio
import logging
import json
from typing import Optional

import httpx

from app.config import STABLECOIN_REGISTRY

logger = logging.getLogger(__name__)

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
RATE_LIMIT_DELAY = 0.22  # ~4.5 req/sec (conservative under 5/sec Pro limit)


# =============================================================================
# Known Major Holder Addresses (Ethereum Mainnet)
# =============================================================================
# Each entry: (address, label, category)
# Categories: "exchange", "defi", "bridge", "treasury", "custodian", "other"

KNOWN_HOLDERS = [
    # --- Exchanges ---
    ("0x28C6c06298d514Db089934071355E5743bf21d60", "Binance Hot Wallet", "exchange"),
    ("0x21a31Ee1afC51d94C2eFcCAa2092aD1028285549", "Binance Cold Wallet", "exchange"),
    ("0xDFd5293D8e347dFe59E90eFd55b2956a1343963d", "Binance Deposit", "exchange"),
    ("0xF977814e90dA44bFA03b6295A0616a897441aceC", "Binance 8", "exchange"),
    ("0x503828976D22510aad0201ac7EC88293211D23Da", "Coinbase Prime", "exchange"),
    ("0x71660c4005BA85c37ccec55d0C4493E66Fe775d3", "Coinbase Commerce", "exchange"),
    ("0xA9D1e08C7793af67e9d92fe308d5697FB81d3E43", "Coinbase 10", "exchange"),
    ("0x2910543Af39abA0Cd09dBb2D50200b3E800A63D2", "Kraken Hot Wallet", "exchange"),
    ("0xAe2D4617c862309A3d75A0fFB358c7a5009c673F", "Kraken 10", "exchange"),
    ("0x0D0707963952f2fBA59dD06f2b425ace40b492Fe", "Gate.io", "exchange"),
    ("0x1AB4973a48dc892Cd9971ECE8e01DcC7688f8F23", "Bybit Hot Wallet", "exchange"),
    ("0xf89d7b9c864f589bbF53a82105107622B35EaA40", "Bybit Cold Wallet", "exchange"),
    ("0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503", "Binance 14", "exchange"),
    ("0x56Eddb7aa87536c09CCc2793473599fD21A8b17F", "Binance 17", "exchange"),
    ("0x6cC5F688a315f3dC28A7781717a9A798a59fDA7b", "OKX Hot Wallet", "exchange"),
    ("0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8", "Binance 7", "exchange"),
    ("0x40ec5B33f54e0E8A33A975908C5BA1c14e5BbbDf", "Polygon Bridge (Exchange-like)", "exchange"),

    # --- DeFi Protocols ---
    ("0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419", "Chainlink ETH/USD Feed", "defi"),
    ("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2", "Aave V3 Pool", "defi"),
    ("0xA17581A9E3356d9A858b789D68B4d866e593aE94", "Compound V3 cUSDCv3", "defi"),
    ("0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7", "Curve 3pool", "defi"),
    ("0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643", "Compound V2 cDAI", "defi"),
    ("0x60FaAe176336dAb62e284Fe19B885B095d29fB7F", "Aave V3 aEthDAI", "defi"),
    ("0x39AA39c021dfbaE8faC545936693aC917d5E7563", "Compound V2 cUSDC", "defi"),
    ("0x8c3527F6f6e9e3be8BdAFd5f20c5c9e31C6e7c2e", "Sky/Maker PSM USDC", "defi"),

    # --- Bridges ---
    ("0x40ec5B33f54e0E8A33A975908C5BA1c14e5BbbDf", "Polygon PoS Bridge", "bridge"),
    ("0x99C9fc46f92E8a1c0deC1b1747d010903E884bE1", "Optimism Gateway", "bridge"),
    ("0x4Dbd4fc535Ac27206064B68FfCf827b0A60BAB3f", "Arbitrum Delayed Inbox", "bridge"),
    ("0xa3A7B6F88361F48403514059F1F16C8E78d60EeC", "Arbitrum ERC20 Gateway", "bridge"),
    ("0x467194771dAe2967Aef3ECbEDD3Bf9a310C76C65", "Optimism DAI Bridge", "bridge"),
    ("0x3154Cf16ccdb4C6d922629664174b904d80F2C35", "Base Bridge", "bridge"),

    # --- Treasury / Issuer ---
    ("0x55FE002aefF02F77364de339a1292923A15844B8", "Circle Reserve (USDC)", "treasury"),
    ("0x5754284f345afc66a98fbB0a0Afe71e0F007B949", "Tether Treasury", "treasury"),
    ("0x36928500Bc1dCd7af6a2B4008875CC336b927D57", "MakerDAO Pause Proxy", "treasury"),
    ("0xBE8E3e3618f7474F8cB1d074A26afFef007E98FB", "MakerDAO DSR", "treasury"),

    # --- Custodians ---
    ("0x0A59649758aa4d66E25f08Dd01271e891fe52199", "USDC Master Minter (Circle)", "custodian"),
    ("0xC6CDE7C39eB2f0F0095F41570af89eFC2C1Ea828", "Paxos Treasury (PYUSD)", "custodian"),
]

# Remove duplicates (keep first occurrence)
_seen_addrs = set()
UNIQUE_HOLDERS = []
for addr, label, cat in KNOWN_HOLDERS:
    lower = addr.lower()
    if lower not in _seen_addrs:
        _seen_addrs.add(lower)
        UNIQUE_HOLDERS.append((addr, label, cat))
KNOWN_HOLDERS = UNIQUE_HOLDERS

# Estimated unique holder counts per stablecoin (from public block explorers)
# Updated periodically — these serve as fallback when tokenholdercount API is unavailable
ESTIMATED_HOLDER_COUNTS = {
    "usdc": 2_400_000,
    "usdt": 6_200_000,
    "dai":  680_000,
    "frax": 42_000,
    "pyusd": 85_000,
    "fdusd": 95_000,
    "tusd": 52_000,
    "usdd": 28_000,
    "usde": 120_000,
}


# =============================================================================
# API: Fetch token balance for a single address
# =============================================================================

async def fetch_token_balance(
    client: httpx.AsyncClient,
    contract_address: str,
    holder_address: str,
    api_key: str,
) -> Optional[int]:
    """Fetch ERC-20 token balance for one address via Etherscan V2 API."""
    try:
        resp = await client.get(
            ETHERSCAN_V2_BASE,
            params={
                "chainid": 1,
                "module": "account",
                "action": "tokenbalance",
                "contractaddress": contract_address,
                "address": holder_address,
                "tag": "latest",
                "apikey": api_key,
            },
            timeout=10.0,
        )
        data = resp.json()
        if data.get("status") == "1":
            return int(data["result"])
        else:
            msg = data.get("result", "")
            if "Max rate limit" in str(msg):
                logger.warning("Etherscan rate limit hit, backing off")
                await asyncio.sleep(1.0)
            return None
    except Exception as e:
        logger.debug(f"Etherscan balance fetch error for {holder_address[:10]}...: {e}")
        return None


# =============================================================================
# Collector: Holder Distribution Components
# =============================================================================

async def collect_holder_distribution(
    client: httpx.AsyncClient,
    stablecoin_id: str,
) -> list[dict]:
    """
    Collect holder distribution components for one stablecoin.

    Queries Etherscan tokenbalance for each known major holder address,
    then computes concentration metrics.

    Returns list of component dicts with labeled metadata for provenance.
    """
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        logger.warning("ETHERSCAN_API_KEY not set — skipping holder distribution")
        return []

    cfg = STABLECOIN_REGISTRY.get(stablecoin_id)
    if not cfg:
        return []

    contract = cfg.get("contract", "")
    if not contract:
        logger.warning(f"No contract address for {stablecoin_id}")
        return []

    decimals = cfg.get("decimals", 18)

    # Fetch balances for all known holder addresses
    holder_balances = []
    exchange_balances = []

    for address, label, category in KNOWN_HOLDERS:
        balance_raw = await fetch_token_balance(client, contract, address, api_key)
        await asyncio.sleep(RATE_LIMIT_DELAY)

        if balance_raw is None or balance_raw == 0:
            continue

        balance = balance_raw / (10 ** decimals)
        entry = {
            "address": address,
            "label": label,
            "category": category,
            "balance": balance,
            "balance_raw": str(balance_raw),
        }
        holder_balances.append(entry)

        if category == "exchange":
            exchange_balances.append(entry)

    if not holder_balances:
        logger.info(f"No holder balances found for {stablecoin_id}")
        return []

    # Sort by balance descending
    holder_balances.sort(key=lambda x: x["balance"], reverse=True)
    exchange_balances.sort(key=lambda x: x["balance"], reverse=True)

    # Get total supply from CoinGecko market data (already fetched by coingecko collector)
    # Use market_cap as proxy: market_cap ≈ total_supply * price ≈ total_supply for stablecoins
    total_supply = _estimate_total_supply(stablecoin_id, holder_balances)

    # --- Component 1: Top 10 Concentration ---
    top_10 = holder_balances[:10]
    top_10_total = sum(h["balance"] for h in top_10)
    top_10_pct = (top_10_total / total_supply * 100) if total_supply > 0 else 0

    top_10_labels = [
        {"label": h["label"], "category": h["category"],
         "address": h["address"], "balance": round(h["balance"], 2),
         "pct_of_supply": round(h["balance"] / total_supply * 100, 4) if total_supply > 0 else 0}
        for h in top_10
    ]

    # Normalize: lower concentration = higher score
    # Perfect: 10% concentration → 100; Threshold: 80% → 0
    from app.scoring import normalize_inverse_linear
    top_10_score = normalize_inverse_linear(top_10_pct, 10, 80)

    # --- Component 2: Unique Holders ---
    holder_count = ESTIMATED_HOLDER_COUNTS.get(stablecoin_id, 50_000)

    from app.scoring import normalize_log
    holders_score = normalize_log(
        holder_count,
        thresholds={1000: 20, 10000: 40, 100000: 60, 1000000: 80, 10000000: 100}
    )

    # --- Component 3: Exchange Concentration ---
    exchange_total = sum(h["balance"] for h in exchange_balances)
    exchange_pct = (exchange_total / total_supply * 100) if total_supply > 0 else 0

    exchange_labels = [
        {"label": h["label"], "address": h["address"],
         "balance": round(h["balance"], 2),
         "pct_of_supply": round(h["balance"] / total_supply * 100, 4) if total_supply > 0 else 0}
        for h in exchange_balances[:10]
    ]

    from app.scoring import normalize_centered
    exchange_score = normalize_centered(exchange_pct, center=30, tolerance=15, extreme=40)

    # Build components
    components = [
        {
            "component_id": "top_10_concentration",
            "category": "holder_distribution",
            "raw_value": round(top_10_pct, 4),
            "normalized_score": round(top_10_score, 2),
            "data_source": "etherscan",
            "metadata": {
                "description": f"Top 10 known holders control {top_10_pct:.2f}% of supply",
                "total_supply_estimate": round(total_supply, 2),
                "top_10_total_balance": round(top_10_total, 2),
                "holders": top_10_labels,
                "contract": contract,
                "addresses_queried": len(KNOWN_HOLDERS),
                "addresses_with_balance": len(holder_balances),
            },
        },
        {
            "component_id": "unique_holders",
            "category": "holder_distribution",
            "raw_value": holder_count,
            "normalized_score": round(holders_score, 2),
            "data_source": "etherscan",
            "metadata": {
                "description": f"Estimated {holder_count:,} unique holders",
                "source": "block explorer estimate (tokenholdercount API Pro required for live data)",
                "note": "Updated periodically from public block explorer data",
            },
        },
        {
            "component_id": "exchange_concentration",
            "category": "holder_distribution",
            "raw_value": round(exchange_pct, 4),
            "normalized_score": round(exchange_score, 2),
            "data_source": "etherscan",
            "metadata": {
                "description": f"Known exchanges hold {exchange_pct:.2f}% of supply",
                "exchange_total_balance": round(exchange_total, 2),
                "exchanges": exchange_labels,
            },
        },
    ]

    logger.info(
        f"Etherscan {stablecoin_id}: top10={top_10_pct:.1f}% "
        f"exchanges={exchange_pct:.1f}% holders≈{holder_count:,} "
        f"({len(holder_balances)} addresses with balance)"
    )

    return components


def _estimate_total_supply(stablecoin_id: str, holder_balances: list[dict]) -> float:
    """
    Estimate total circulating supply for a stablecoin.
    Uses database market_cap if available (≈ supply for stablecoins pegged to $1),
    otherwise falls back to known supply estimates.
    """
    try:
        from app.database import fetch_one
        row = fetch_one(
            "SELECT market_cap FROM scores WHERE stablecoin_id = %s",
            (stablecoin_id,)
        )
        if row and row.get("market_cap"):
            return float(row["market_cap"])
    except Exception:
        pass

    SUPPLY_ESTIMATES = {
        "usdc": 45_000_000_000,
        "usdt": 140_000_000_000,
        "dai": 3_500_000_000,
        "frax": 650_000_000,
        "pyusd": 800_000_000,
        "fdusd": 2_000_000_000,
        "tusd": 500_000_000,
        "usdd": 750_000_000,
        "usde": 6_000_000_000,
    }
    return SUPPLY_ESTIMATES.get(stablecoin_id, 1_000_000_000)
