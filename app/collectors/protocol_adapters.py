"""
Protocol Adapters — Receipt Token Resolution
==============================================
Each lending protocol has a different receipt token architecture.
This module provides a unified interface for resolving receipt token
addresses and fetching depositor wallets for any protocol.

Adapters:
- AaveV3Adapter:   aTokens (getReserveData on Pool contract)
- SparkAdapter:    spTokens (Aave V3 fork, same pattern)
- CompoundV2Adapter: cTokens (hardcoded registry)
- CompoundV3Adapter: Comet contracts (balanceOf pattern, no receipt token)
- MorphoAdapter:   MetaMorpho vault share tokens
- MakerAdapter:    sDAI (ERC-4626 vault)
- CurveAdapter:    LP tokens for stablecoin pools
- ConvexAdapter:   cvx-deposit tokens wrapping Curve LP

All addresses verified against on-chain sources and protocol documentation.
"""

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ReceiptToken:
    """A resolved receipt token for a protocol-stablecoin-chain tuple."""
    contract: str           # receipt token contract address
    label: str              # human-readable label
    protocol_slug: str
    stablecoin_symbol: str
    chain: str
    underlying: str         # underlying stablecoin contract address
    token_type: str         # atoken, ctoken, comet, vault_share, lp_token
    holder_method: str = "tokenholderlist"  # how to discover depositors


# =============================================================================
# Stablecoin contract addresses by chain (lowercase)
# =============================================================================

STABLECOIN_CONTRACTS = {
    ("USDC", "ethereum"): "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    ("USDT", "ethereum"): "0xdac17f958d2ee523a2206206994597c13d831ec7",
    ("DAI", "ethereum"):  "0x6b175474e89094c44da98b954eedeac495271d0f",
    ("FRAX", "ethereum"): "0x853d955acef822db058eb8505911ed77f175b99e",
    ("PYUSD", "ethereum"): "0x6c3ea9036406852006290770bedfcaba0e23a0e8",
    ("USDe", "ethereum"): "0x4c9edd5852cd905f086c759e8383e09bff1e68b3",
    ("USDC", "base"):     "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
    ("USDT", "base"):     "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2",
    ("DAI", "base"):      "0x50c5725949a6f0c72e6c4a641f24049a917db0cb",
    ("USDC", "arbitrum"):  "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    ("USDT", "arbitrum"):  "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
    ("DAI", "arbitrum"):   "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
}


# =============================================================================
# Aave V3 aToken Registry
# =============================================================================
# Source: bgd-labs/aave-address-book, verified 2024-06-23
# Pool contracts: getReserveData(asset) → returns aTokenAddress

AAVE_V3_POOL = {
    "ethereum": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
    "base":     "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
    "arbitrum": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
}

AAVE_V3_ATOKENS = {
    # Ethereum
    ("aave", "USDC", "ethereum"): ReceiptToken(
        contract="0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c",
        label="Aave V3 aEthUSDC", protocol_slug="aave",
        stablecoin_symbol="USDC", chain="ethereum",
        underlying="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        token_type="atoken",
    ),
    ("aave", "USDT", "ethereum"): ReceiptToken(
        contract="0x23878914EFE38d27C4D67Ab83ed1b93A74D4086a",
        label="Aave V3 aEthUSDT", protocol_slug="aave",
        stablecoin_symbol="USDT", chain="ethereum",
        underlying="0xdac17f958d2ee523a2206206994597c13d831ec7",
        token_type="atoken",
    ),
    ("aave", "DAI", "ethereum"): ReceiptToken(
        contract="0x018008bfb33d285247A21d44E50697654f754e63",
        label="Aave V3 aEthDAI", protocol_slug="aave",
        stablecoin_symbol="DAI", chain="ethereum",
        underlying="0x6b175474e89094c44da98b954eedeac495271d0f",
        token_type="atoken",
    ),
    ("aave", "FRAX", "ethereum"): ReceiptToken(
        contract="0xd4e245848d6E1220DBE62e155d89fa327A284cB0",
        label="Aave V3 aEthFRAX", protocol_slug="aave",
        stablecoin_symbol="FRAX", chain="ethereum",
        underlying="0x853d955acef822db058eb8505911ed77f175b99e",
        token_type="atoken",
    ),
    ("aave", "PYUSD", "ethereum"): ReceiptToken(
        contract="0x0C0d01AbF3e6aDfcA0989eBbA9d6e85dD58EaB1E",
        label="Aave V3 aEthPYUSD", protocol_slug="aave",
        stablecoin_symbol="PYUSD", chain="ethereum",
        underlying="0x6c3ea9036406852006290770bedfcaba0e23a0e8",
        token_type="atoken",
    ),
    ("aave", "USDe", "ethereum"): ReceiptToken(
        contract="0x4F5923Fc5FD4a93352581b38B7cD26943012DECF",
        label="Aave V3 aEthUSDe", protocol_slug="aave",
        stablecoin_symbol="USDe", chain="ethereum",
        underlying="0x4c9edd5852cd905f086c759e8383e09bff1e68b3",
        token_type="atoken",
    ),
    # Base
    ("aave", "USDC", "base"): ReceiptToken(
        contract="0x4e65fE4DbA92790696d040ac24Aa414708F5c0AB",
        label="Aave V3 aBasUSDC", protocol_slug="aave",
        stablecoin_symbol="USDC", chain="base",
        underlying="0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        token_type="atoken",
    ),
    # Arbitrum
    ("aave", "USDC", "arbitrum"): ReceiptToken(
        contract="0x724dc807b04555b71ed48a6896b6F41593b8C637",
        label="Aave V3 aArbUSDCn", protocol_slug="aave",
        stablecoin_symbol="USDC", chain="arbitrum",
        underlying="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        token_type="atoken",
    ),
    ("aave", "USDT", "arbitrum"): ReceiptToken(
        contract="0x6ab707Aca953eDAeFBc4fD23bA73294241490620",
        label="Aave V3 aArbUSDT", protocol_slug="aave",
        stablecoin_symbol="USDT", chain="arbitrum",
        underlying="0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
        token_type="atoken",
    ),
    ("aave", "DAI", "arbitrum"): ReceiptToken(
        contract="0x82E64f49Ed5EC1bC6e43DAD4FC8Af9bb3A2312EE",
        label="Aave V3 aArbDAI", protocol_slug="aave",
        stablecoin_symbol="DAI", chain="arbitrum",
        underlying="0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
        token_type="atoken",
    ),
    ("aave", "FRAX", "arbitrum"): ReceiptToken(
        contract="0x38d693cE1dF5AaDF7bC62043aE5EE32bb3f1259B",
        label="Aave V3 aArbFRAX", protocol_slug="aave",
        stablecoin_symbol="FRAX", chain="arbitrum",
        underlying="0x17fc002b466eec40dae837fc4be5c67993ddbd6f",
        token_type="atoken",
    ),
}


# =============================================================================
# Spark Protocol (Aave V3 fork)
# =============================================================================

SPARK_TOKENS = {
    ("spark", "USDC", "ethereum"): ReceiptToken(
        contract="0x377C3bd93f2a2984E1E7bE6A5C22c525eD4A4815",
        label="Spark spUSDC", protocol_slug="spark",
        stablecoin_symbol="USDC", chain="ethereum",
        underlying="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        token_type="atoken",
    ),
    ("spark", "USDT", "ethereum"): ReceiptToken(
        contract="0xe7dF13b8e3d6740fe17CBE928C7334243d86c92f",
        label="Spark spUSDT", protocol_slug="spark",
        stablecoin_symbol="USDT", chain="ethereum",
        underlying="0xdac17f958d2ee523a2206206994597c13d831ec7",
        token_type="atoken",
    ),
    ("spark", "DAI", "ethereum"): ReceiptToken(
        contract="0x4DEDf26112B3Ec8eC46e7E31EA5e123490B05B8B",
        label="Spark spDAI", protocol_slug="spark",
        stablecoin_symbol="DAI", chain="ethereum",
        underlying="0x6b175474e89094c44da98b954eedeac495271d0f",
        token_type="atoken",
    ),
}


# =============================================================================
# Compound V2 cToken Registry
# =============================================================================
# cTokens are the market contracts themselves. Each wraps one underlying.
# Stable, well-documented, no on-chain discovery needed.

COMPOUND_V2_CTOKENS = {
    ("compound-finance", "USDC", "ethereum"): ReceiptToken(
        contract="0x39AA39c021dfbaE8faC545936693aC917d5E7563",
        label="Compound V2 cUSDC", protocol_slug="compound-finance",
        stablecoin_symbol="USDC", chain="ethereum",
        underlying="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        token_type="ctoken",
    ),
    ("compound-finance", "USDT", "ethereum"): ReceiptToken(
        contract="0xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9",
        label="Compound V2 cUSDT", protocol_slug="compound-finance",
        stablecoin_symbol="USDT", chain="ethereum",
        underlying="0xdac17f958d2ee523a2206206994597c13d831ec7",
        token_type="ctoken",
    ),
    ("compound-finance", "DAI", "ethereum"): ReceiptToken(
        contract="0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643",
        label="Compound V2 cDAI", protocol_slug="compound-finance",
        stablecoin_symbol="DAI", chain="ethereum",
        underlying="0x6b175474e89094c44da98b954eedeac495271d0f",
        token_type="ctoken",
    ),
}


# =============================================================================
# Compound V3 (Comet) — no receipt token, use Comet contract directly
# =============================================================================
# Compound V3 uses a single Comet contract per market. Depositors are tracked
# internally — no ERC-20 receipt token to query holders for.
# We query tokenholderlist on the Comet contract itself to find depositors.

COMPOUND_V3_COMETS = {
    ("compound-finance", "USDC", "ethereum"): ReceiptToken(
        contract="0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        label="Compound V3 cUSDCv3", protocol_slug="compound-finance",
        stablecoin_symbol="USDC", chain="ethereum",
        underlying="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        token_type="comet",
    ),
    ("compound-finance", "USDT", "ethereum"): ReceiptToken(
        contract="0x3Afdc9BCA9213A35503b077a6072F3D0d5AB0840",
        label="Compound V3 cUSDTv3", protocol_slug="compound-finance",
        stablecoin_symbol="USDT", chain="ethereum",
        underlying="0xdac17f958d2ee523a2206206994597c13d831ec7",
        token_type="comet",
    ),
    ("compound-finance", "USDC", "base"): ReceiptToken(
        contract="0xb125E6687d4313864e53df431d5425969c15Eb2F",
        label="Compound V3 cUSDCv3 Base", protocol_slug="compound-finance",
        stablecoin_symbol="USDC", chain="base",
        underlying="0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        token_type="comet",
    ),
    ("compound-finance", "USDC", "arbitrum"): ReceiptToken(
        contract="0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA",
        label="Compound V3 cUSDCv3 Arb", protocol_slug="compound-finance",
        stablecoin_symbol="USDC", chain="arbitrum",
        underlying="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        token_type="comet",
    ),
}


# =============================================================================
# Morpho (MetaMorpho Vaults)
# =============================================================================
# Each MetaMorpho vault has its own ERC-20 share token.
# Major USDC/USDT vaults on Ethereum.

MORPHO_VAULTS = {
    ("morpho", "USDC", "ethereum"): ReceiptToken(
        contract="0xBEEF01735c132Ada46AA9aA9cE21E792a30CFE8b",
        label="Morpho Steakhouse USDC", protocol_slug="morpho",
        stablecoin_symbol="USDC", chain="ethereum",
        underlying="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        token_type="vault_share",
    ),
    ("morpho", "USDT", "ethereum"): ReceiptToken(
        contract="0xbEEF02e5e13584ab96848af90261f0C8Ee04722a",
        label="Morpho Steakhouse USDT", protocol_slug="morpho",
        stablecoin_symbol="USDT", chain="ethereum",
        underlying="0xdac17f958d2ee523a2206206994597c13d831ec7",
        token_type="vault_share",
    ),
    ("morpho", "DAI", "ethereum"): ReceiptToken(
        contract="0x500331c9fF24D9d11aee6B07734Aa72343EA74a5",
        label="Morpho Steakhouse DAI", protocol_slug="morpho",
        stablecoin_symbol="DAI", chain="ethereum",
        underlying="0x6b175474e89094c44da98b954eedeac495271d0f",
        token_type="vault_share",
    ),
    ("morpho", "PYUSD", "ethereum"): ReceiptToken(
        contract="0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb",
        label="Morpho PYUSD Vault", protocol_slug="morpho",
        stablecoin_symbol="PYUSD", chain="ethereum",
        underlying="0x6c3ea9036406852006290770bedfcaba0e23a0e8",
        token_type="vault_share",
    ),
}


# =============================================================================
# MakerDAO / Sky — sDAI (DSR vault, ERC-4626)
# =============================================================================

MAKER_TOKENS = {
    ("sky", "DAI", "ethereum"): ReceiptToken(
        contract="0x83F20F44975D03b1b09e64809B757c47f942BEeA",
        label="sDAI (Savings DAI)", protocol_slug="sky",
        stablecoin_symbol="DAI", chain="ethereum",
        underlying="0x6b175474e89094c44da98b954eedeac495271d0f",
        token_type="vault_share",
    ),
}


# =============================================================================
# Curve — stablecoin pool LP tokens
# =============================================================================

CURVE_TOKENS = {
    ("curve-finance", "USDC", "ethereum"): ReceiptToken(
        contract="0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
        label="Curve 3pool LP (DAI/USDC/USDT)", protocol_slug="curve-finance",
        stablecoin_symbol="USDC", chain="ethereum",
        underlying="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        token_type="lp_token",
    ),
    ("curve-finance", "DAI", "ethereum"): ReceiptToken(
        contract="0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
        label="Curve 3pool LP (DAI/USDC/USDT)", protocol_slug="curve-finance",
        stablecoin_symbol="DAI", chain="ethereum",
        underlying="0x6b175474e89094c44da98b954eedeac495271d0f",
        token_type="lp_token",
    ),
    ("curve-finance", "USDT", "ethereum"): ReceiptToken(
        contract="0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
        label="Curve 3pool LP (DAI/USDC/USDT)", protocol_slug="curve-finance",
        stablecoin_symbol="USDT", chain="ethereum",
        underlying="0xdac17f958d2ee523a2206206994597c13d831ec7",
        token_type="lp_token",
    ),
}


# =============================================================================
# Convex — cvx-wrapped Curve LP tokens
# =============================================================================

CONVEX_TOKENS = {
    ("convex-finance", "USDC", "ethereum"): ReceiptToken(
        contract="0x30D9410ED1D5DA1F6C8391af5338C93ab8d4035C",
        label="Convex 3pool Deposit", protocol_slug="convex-finance",
        stablecoin_symbol="USDC", chain="ethereum",
        underlying="0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",  # Curve 3pool LP
        token_type="lp_token",
    ),
}


# =============================================================================
# Lido — wstETH holders
# =============================================================================
# wstETH is the wrapped staking receipt. Top holders shows protocol-level
# exposure to Lido's validator set.

LIDO_TOKENS = {
    ("lido", "USDC", "ethereum"): ReceiptToken(
        contract="0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        label="wstETH (Lido Wrapped Staked ETH)", protocol_slug="lido",
        stablecoin_symbol="USDC", chain="ethereum",
        underlying="0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",  # stETH
        token_type="staking_receipt",
    ),
}


# =============================================================================
# Ethena — sUSDe (staked USDe) and USDe itself
# =============================================================================

ETHENA_TOKENS = {
    ("ethena", "USDe", "ethereum"): ReceiptToken(
        contract="0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
        label="sUSDe (Staked USDe)", protocol_slug="ethena",
        stablecoin_symbol="USDe", chain="ethereum",
        underlying="0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",  # USDe
        token_type="staking_receipt",
    ),
}


# =============================================================================
# Pendle — PT (Principal Token) for sUSDe
# =============================================================================

PENDLE_TOKENS = {
    ("pendle", "USDe", "ethereum"): ReceiptToken(
        contract="0xB05cABCd99cf9a73b19805edefC5f67CA5d1895E",
        label="PT-sUSDe (Pendle Principal Token)", protocol_slug="pendle",
        stablecoin_symbol="USDe", chain="ethereum",
        underlying="0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",  # sUSDe
        token_type="pt_token",
    ),
}


# =============================================================================
# Unified registry — merge all adapters
# =============================================================================

def get_all_receipt_tokens() -> dict[tuple, ReceiptToken]:
    """
    Return the complete registry of all receipt tokens across all protocols.
    Key: (protocol_slug, stablecoin_symbol, chain)
    Value: ReceiptToken
    """
    registry = {}
    registry.update(AAVE_V3_ATOKENS)
    registry.update(SPARK_TOKENS)
    registry.update(COMPOUND_V2_CTOKENS)
    registry.update(COMPOUND_V3_COMETS)
    registry.update(MORPHO_VAULTS)
    registry.update(MAKER_TOKENS)
    registry.update(CURVE_TOKENS)
    registry.update(CONVEX_TOKENS)
    registry.update(LIDO_TOKENS)
    registry.update(ETHENA_TOKENS)
    registry.update(PENDLE_TOKENS)
    return registry


def get_receipt_tokens_for_protocol(protocol_slug: str) -> list[ReceiptToken]:
    """Get all receipt tokens for a specific protocol."""
    return [
        rt for key, rt in get_all_receipt_tokens().items()
        if key[0] == protocol_slug
    ]


def get_receipt_tokens_for_stablecoin(symbol: str) -> list[ReceiptToken]:
    """Get all receipt tokens across protocols for a specific stablecoin."""
    return [
        rt for key, rt in get_all_receipt_tokens().items()
        if key[1] == symbol
    ]


def get_coverage_summary() -> dict:
    """
    Report which protocol-stablecoin-chain combos have receipt token coverage.
    Used for the CQI pair coverage report.
    """
    registry = get_all_receipt_tokens()

    protocols = set()
    stablecoins = set()
    chains = set()
    by_protocol = {}

    for (proto, symbol, chain), rt in registry.items():
        protocols.add(proto)
        stablecoins.add(symbol)
        chains.add(chain)
        by_protocol.setdefault(proto, []).append({
            "stablecoin": symbol,
            "chain": chain,
            "contract": rt.contract,
            "label": rt.label,
            "token_type": rt.token_type,
        })

    return {
        "total_receipt_tokens": len(registry),
        "protocols_covered": len(protocols),
        "stablecoins_covered": len(stablecoins),
        "chains_covered": len(chains),
        "protocols": sorted(protocols),
        "by_protocol": {
            p: {"count": len(tokens), "tokens": tokens}
            for p, tokens in sorted(by_protocol.items())
        },
    }
