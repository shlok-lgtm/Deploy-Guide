"""
Holder Analysis — Shared Etherscan Token Holder Distribution
==============================================================
Shared utility for analyzing ERC-20 token holder distribution.
Called by lst_collector, tti_collector, bridge_collector, cex_collector.

Reuses the same KNOWN_HOLDERS registry and Etherscan API pattern from
app/collectors/etherscan.py but works for any ERC-20 contract address.

Results are cached with 24h TTL to stay within Etherscan rate limits.
"""

import asyncio
import logging
import math
import os
import time
from datetime import datetime, timezone

import httpx

from app.database import fetch_one
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
RATE_LIMIT_DELAY = 0.15  # conservative for shared usage

# In-memory cache: {contract_address_lower: (timestamp, result_dict)}
_holder_cache: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 86400  # 24 hours


# =============================================================================
# Known holder addresses (reused from etherscan.py, extended for LST/TTI)
# =============================================================================

KNOWN_ADDRESSES = [
    # Exchanges
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
    ("0x6cC5F688a315f3dC28A7781717a9A798a59fDA7b", "OKX Hot Wallet", "exchange"),
    ("0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8", "Binance 7", "exchange"),

    # DeFi protocols (common LST/token holders)
    ("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2", "Aave V3 Pool", "defi"),
    ("0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "Lido wstETH", "defi"),
    ("0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84", "Lido stETH", "defi"),
    ("0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7", "Curve 3pool", "defi"),
    ("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022", "Lido Staking Contract", "defi"),
    ("0xC36442b4a4522E871399CD717aBDD847Ab11FE88", "Uniswap V3 NonfungiblePositionManager", "defi"),
    ("0x1111111254EEB25477B68fb85Ed929f73A960582", "1inch V5 Router", "defi"),
    ("0x93D199263632a4EF4Bb438F1feB99e57b4b5f0BD", "Eigenlayer Strategy Manager", "defi"),
    ("0x858646372CC42E1A627fcE94aa7A7033e7CF075A", "Eigenlayer StrategyManager", "defi"),
    ("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D", "Uniswap V2 Router", "defi"),
    ("0xA17581A9E3356d9A858b789D68B4d866e593aE94", "Compound V3 cUSDCv3", "defi"),
    ("0x39AA39c021dfbaE8faC545936693aC917d5E7563", "Compound V2 cUSDC", "defi"),
    ("0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419", "Chainlink ETH/USD Feed", "defi"),
    ("0x60FaAe176336dAb62e284Fe19B885B095d29fB7F", "Aave V3 aEthDAI", "defi"),

    # Bridges
    ("0x99C9fc46f92E8a1c0deC1b1747d010903E884bE1", "Optimism Gateway", "bridge"),
    ("0x4Dbd4fc535Ac27206064B68FfCf827b0A60BAB3f", "Arbitrum Delayed Inbox", "bridge"),
    ("0xa3A7B6F88361F48403514059F1F16C8E78d60EeC", "Arbitrum ERC20 Gateway", "bridge"),
    ("0x3154Cf16ccdb4C6d922629664174b904d80F2C35", "Base Bridge", "bridge"),
    ("0x40ec5B33f54e0E8A33A975908C5BA1c14e5BbbDf", "Polygon PoS Bridge", "bridge"),
]

# De-duplicate
_seen = set()
_UNIQUE_ADDRESSES = []
for _addr, _label, _cat in KNOWN_ADDRESSES:
    _lower = _addr.lower()
    if _lower not in _seen:
        _seen.add(_lower)
        _UNIQUE_ADDRESSES.append((_addr, _label, _cat))
KNOWN_ADDRESSES = _UNIQUE_ADDRESSES


# =============================================================================
# Core: Fetch holder data for any ERC-20 token
# =============================================================================

async def _fetch_balance(
    client: httpx.AsyncClient,
    contract: str,
    holder: str,
    api_key: str,
) -> int | None:
    """Fetch ERC-20 balance for one address."""
    try:
        resp = await client.get(
            ETHERSCAN_V2_BASE,
            params={
                "chainid": 1,
                "module": "account",
                "action": "tokenbalance",
                "contractaddress": contract,
                "address": holder,
                "tag": "latest",
                "apikey": api_key,
            },
            timeout=10,
        )
        data = resp.json()
        if data.get("status") == "1":
            return int(data["result"])
        if "Max rate limit" in str(data.get("result", "")):
            await asyncio.sleep(1.0)
        return None
    except Exception:
        return None


async def _fetch_holder_count(
    client: httpx.AsyncClient,
    contract: str,
    api_key: str,
) -> int | None:
    """Fetch unique holder count via tokenholdercount endpoint."""
    try:
        resp = await client.get(
            ETHERSCAN_V2_BASE,
            params={
                "chainid": 1,
                "module": "token",
                "action": "tokenholdercount",
                "contractaddress": contract,
                "apikey": api_key,
            },
            timeout=10,
        )
        data = resp.json()
        if data.get("status") == "1":
            return int(data["result"])
        return None
    except Exception:
        return None


def _compute_gini(balances: list[float]) -> float:
    """Compute Gini coefficient from a list of balances."""
    if not balances or len(balances) < 2:
        return 0.5
    sorted_b = sorted(balances)
    n = len(sorted_b)
    total = sum(sorted_b)
    if total <= 0:
        return 0.5
    cumulative = sum((2 * (i + 1) - n - 1) * sorted_b[i] for i in range(n))
    return cumulative / (n * total)


def _estimate_supply(contract: str, cg_market_cap: float | None) -> float:
    """Estimate total supply. Uses CoinGecko market cap if available."""
    if cg_market_cap and cg_market_cap > 0:
        return cg_market_cap
    return 1_000_000_000  # fallback 1B


async def analyze_holders(
    contract_address: str,
    decimals: int = 18,
    market_cap: float | None = None,
) -> dict:
    """
    Analyze token holder distribution for any ERC-20 contract.

    Returns:
        {
            "holder_count": int | None,
            "top_10_pct": float,
            "gini": float,
            "defi_pct": float,
            "exchange_pct": float,
            "hhi": float,
            "defi_protocol_count": int,
            "balances_found": int,
        }

    Results cached for 24h per contract address.
    """
    contract_lower = contract_address.lower()

    # Check cache
    cached = _holder_cache.get(contract_lower)
    if cached and (time.time() - cached[0]) < _CACHE_TTL:
        return cached[1]

    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        return _empty_result()

    total_supply = _estimate_supply(contract_address, market_cap)

    async with httpx.AsyncClient(timeout=15) as client:
        # Fetch holder count
        holder_count = await _fetch_holder_count(client, contract_address, api_key)
        await asyncio.sleep(RATE_LIMIT_DELAY)

        # Fetch balances for known addresses
        balances = []
        exchange_total = 0.0
        defi_total = 0.0
        defi_contracts = set()

        for addr, label, category in KNOWN_ADDRESSES:
            raw = await _fetch_balance(client, contract_address, addr, api_key)
            await asyncio.sleep(RATE_LIMIT_DELAY)

            if raw is None or raw == 0:
                continue

            amount = raw / (10 ** decimals)
            balances.append({"amount": amount, "category": category, "label": label})

            if category == "exchange":
                exchange_total += amount
            elif category == "defi":
                defi_total += amount
                defi_contracts.add(label)

    if not balances:
        result = _empty_result()
        result["holder_count"] = holder_count
        _holder_cache[contract_lower] = (time.time(), result)
        return result

    # Sort descending
    balances.sort(key=lambda x: x["amount"], reverse=True)
    amounts = [b["amount"] for b in balances]

    # Top 10 concentration
    top_10_total = sum(amounts[:10])
    top_10_pct = (top_10_total / total_supply * 100) if total_supply > 0 else 0

    # Gini coefficient
    gini = _compute_gini(amounts)

    # DeFi protocol share
    defi_pct = (defi_total / total_supply * 100) if total_supply > 0 else 0

    # Exchange concentration
    exchange_pct = (exchange_total / total_supply * 100) if total_supply > 0 else 0

    # HHI (Herfindahl-Hirschman Index) from top holders
    shares = [(a / total_supply) for a in amounts if total_supply > 0]
    hhi = sum(s ** 2 for s in shares) if shares else 0

    result = {
        "holder_count": holder_count,
        "top_10_pct": round(top_10_pct, 4),
        "gini": round(gini, 4),
        "defi_pct": round(defi_pct, 4),
        "exchange_pct": round(exchange_pct, 4),
        "hhi": round(hhi, 6),
        "defi_protocol_count": len(defi_contracts),
        "balances_found": len(balances),
    }

    _holder_cache[contract_lower] = (time.time(), result)
    logger.info(
        f"Holder analysis {contract_address[:10]}...: "
        f"top10={top_10_pct:.1f}% exchange={exchange_pct:.1f}% "
        f"defi={defi_pct:.1f}% gini={gini:.3f} "
        f"holders={holder_count or '?'} ({len(balances)} addresses with balance)"
    )
    return result


def _empty_result() -> dict:
    """Return empty holder analysis result."""
    return {
        "holder_count": None,
        "top_10_pct": 0,
        "gini": 0.5,
        "defi_pct": 0,
        "exchange_pct": 0,
        "hhi": 0,
        "defi_protocol_count": 0,
        "balances_found": 0,
    }


def get_cached_holders(contract_address: str) -> dict | None:
    """Return cached holder data if available and fresh, else None."""
    contract_lower = contract_address.lower()
    cached = _holder_cache.get(contract_lower)
    if cached and (time.time() - cached[0]) < _CACHE_TTL:
        return cached[1]
    return None


def analyze_holders_sync(
    contract_address: str,
    decimals: int = 18,
    market_cap: float | None = None,
) -> dict:
    """Synchronous wrapper around analyze_holders().

    Runs the async function in a new event loop or reuses an existing one.
    Safe to call from synchronous collector code.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # We're inside an existing async loop (worker context).
        # Run in a new thread to avoid blocking.
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                asyncio.run,
                analyze_holders(contract_address, decimals, market_cap),
            )
            return future.result(timeout=120)
    else:
        return asyncio.run(analyze_holders(contract_address, decimals, market_cap))
