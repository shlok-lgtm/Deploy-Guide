"""
Static Provenance Registry
============================
Centralized registry mapping every static component value to its source URL,
capture timestamp, and evidence metadata. This is the source-of-truth for
static component provenance — every manually assessed value declares where
it came from so the evidence pipeline can capture, screenshot, and attest it.

Usage:
    from app.collectors.static_provenance_registry import get_value, REGISTRY

    # Backward-compatible: returns the plain value whether the config
    # uses old format (plain number) or new provenance format (dict).
    score = get_value(some_config, "usdc")

    # Access full provenance metadata:
    entry = REGISTRY["sii"]["usdc"]["admin_key_risk"]
    url   = entry["source_url"]
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

CAPTURED_AT = "2026-04-12T00:00:00Z"


def _p(value, source_url, source_section=None, captured_at=CAPTURED_AT):
    """Shorthand to build a provenance-annotated value entry."""
    return {
        "value": value,
        "source_url": source_url,
        "source_section": source_section,
        "captured_at": captured_at,
        "evidence_hash": None,  # populated by evidence pipeline
    }


# ---------------------------------------------------------------------------
# Value extraction helper — backward compatible
# ---------------------------------------------------------------------------

def get_value(config_entry, key=None):
    """
    Extract the plain value from a config entry that may be either:
      - old format:  42          (plain scalar)
      - new format:  {"value": 42, "source_url": "...", ...}

    If *key* is given, config_entry is treated as a dict and we look up
    config_entry[key] first.
    """
    if key is not None:
        config_entry = config_entry[key]

    if isinstance(config_entry, dict) and "value" in config_entry:
        return config_entry["value"]
    return config_entry


def get_provenance(config_entry, key=None):
    """
    Return the full provenance dict for a config entry, or None if it's
    a plain value (old format).
    """
    if key is not None:
        config_entry = config_entry[key]

    if isinstance(config_entry, dict) and "source_url" in config_entry:
        return config_entry
    return None


# =============================================================================
# REGISTRY — provenance metadata for every static component across all indices
# =============================================================================
#
# Structure:  REGISTRY[index_id][entity_slug][component_name] = _p(value, url, section)
#
# This does NOT replace the config dicts in each collector. Instead, collectors
# keep their own dicts (for import locality) but this registry is the canonical
# source for the evidence pipeline to iterate over all static components.
# =============================================================================

REGISTRY = {
    # -----------------------------------------------------------------
    # SII — Stablecoin Integrity Index
    # -----------------------------------------------------------------
    "sii": {
        # -- smart_contract.py: ADMIN_KEY_RISK --
        "usdc": {
            "admin_key_risk": _p(80, "https://etherscan.io/address/0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48#readProxyContract", "Admin / Owner functions"),
            "bug_bounty_active": _p(True, "https://immunefi.com/bug-bounty/circle/", "Bounty Program"),
            "bug_bounty_max_payout": _p(250_000, "https://immunefi.com/bug-bounty/circle/", "Max Payout"),
            "pausable": _p(True, "https://etherscan.io/address/0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48#readProxyContract", "pause() function in ABI"),
            "blacklist": _p(True, "https://etherscan.io/address/0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48#readProxyContract", "blacklist() function in ABI"),
            "reserve_ratio": _p(1.01, "https://www.circle.com/en/transparency", "USDC Reserves"),
            "cash_pct": _p(95, "https://www.circle.com/en/transparency", "Reserve Composition"),
            "auditor": _p("deloitte", "https://www.circle.com/en/transparency", "Independent Auditor"),
            "attestation_frequency_days": _p(30, "https://www.circle.com/en/transparency", "Monthly Attestation"),
            "regulatory_status": _p(90, "https://www.circle.com/en/legal/licenses", "Licenses"),
        },
        "usdt": {
            "admin_key_risk": _p(40, "https://etherscan.io/address/0xdac17f958d2ee523a2206206994597c13d831ec7#code", "Admin structure"),
            "bug_bounty_active": _p(False, "https://tether.to/en/", "No public bug bounty"),
            "bug_bounty_max_payout": _p(0, "https://tether.to/en/", "N/A"),
            "pausable": _p(True, "https://etherscan.io/address/0xdac17f958d2ee523a2206206994597c13d831ec7#code", "pause() in ABI"),
            "blacklist": _p(True, "https://etherscan.io/address/0xdac17f958d2ee523a2206206994597c13d831ec7#code", "addBlackList() in ABI"),
            "reserve_ratio": _p(1.00, "https://tether.to/en/transparency/", "Reserves Report"),
            "cash_pct": _p(80, "https://tether.to/en/transparency/", "Reserve Composition"),
            "auditor": _p("bdo italia", "https://tether.to/en/transparency/", "Independent Auditor"),
            "attestation_frequency_days": _p(90, "https://tether.to/en/transparency/", "Quarterly Attestation"),
            "regulatory_status": _p(55, "https://tether.to/en/", "Regulatory Status"),
        },
        "dai": {
            "admin_key_risk": _p(90, "https://etherscan.io/address/0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2#code", "MakerDAO DSChief governance"),
            "bug_bounty_active": _p(True, "https://immunefi.com/bug-bounty/makerdao/", "Bounty Program"),
            "bug_bounty_max_payout": _p(10_000_000, "https://immunefi.com/bug-bounty/makerdao/", "Max Payout"),
            "pausable": _p(False, "https://etherscan.io/address/0x6b175474e89094c44da98b954eedeac495271d0f#code", "No pause function"),
            "blacklist": _p(False, "https://etherscan.io/address/0x6b175474e89094c44da98b954eedeac495271d0f#code", "No blacklist function"),
            "reserve_ratio": _p(1.50, "https://daistats.com/", "Collateral Ratio"),
            "cash_pct": _p(40, "https://daistats.com/", "Real-World Assets"),
            "auditor": _p("n/a (on-chain)", "https://makerburn.com/", "On-chain verifiable"),
            "regulatory_status": _p(55, "https://makerdao.com/", "Decentralized - N/A"),
        },
        "frax": {
            "admin_key_risk": _p(75, "https://docs.frax.finance/smart-contracts/frax", "Multisig + veFXS governance"),
            "bug_bounty_active": _p(True, "https://immunefi.com/bug-bounty/fraxfinance/", "Bounty Program"),
            "bug_bounty_max_payout": _p(500_000, "https://immunefi.com/bug-bounty/fraxfinance/", "Max Payout"),
            "pausable": _p(False, "https://etherscan.io/address/0x853d955acef822db058eb8505911ed77f175b99e#code", "No pause function"),
            "blacklist": _p(False, "https://etherscan.io/address/0x853d955acef822db058eb8505911ed77f175b99e#code", "No blacklist function"),
            "reserve_ratio": _p(1.00, "https://facts.frax.finance/", "Collateral Ratio"),
            "cash_pct": _p(70, "https://facts.frax.finance/", "Reserve Composition"),
            "regulatory_status": _p(50, "https://frax.finance/", "DeFi protocol"),
        },
        "pyusd": {
            "admin_key_risk": _p(70, "https://etherscan.io/address/0x6c3ea9036406852006290770BEdFcAbA0e23A0e8#readProxyContract", "PayPal / Paxos admin structure"),
            "bug_bounty_active": _p(False, "https://paxos.com/pyusd/", "No public bounty"),
            "bug_bounty_max_payout": _p(0, "https://paxos.com/pyusd/", "N/A"),
            "pausable": _p(True, "https://etherscan.io/address/0x6c3ea9036406852006290770BEdFcAbA0e23A0e8#readProxyContract", "pause() in ABI"),
            "blacklist": _p(True, "https://etherscan.io/address/0x6c3ea9036406852006290770BEdFcAbA0e23A0e8#readProxyContract", "blacklist in ABI"),
            "reserve_ratio": _p(1.00, "https://paxos.com/pyusd-transparency/", "Reserve Attestation"),
            "cash_pct": _p(95, "https://paxos.com/pyusd-transparency/", "Reserve Composition"),
            "auditor": _p("withum", "https://paxos.com/pyusd-transparency/", "Independent Auditor"),
            "regulatory_status": _p(85, "https://paxos.com/regulatory-oversight/", "NYDFS Regulated"),
        },
        "fdusd": {
            "admin_key_risk": _p(50, "https://etherscan.io/address/0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409#readProxyContract", "First Digital admin"),
            "bug_bounty_active": _p(False, "https://firstdigitallabs.com/", "No public bounty"),
            "bug_bounty_max_payout": _p(0, "https://firstdigitallabs.com/", "N/A"),
            "pausable": _p(True, "https://etherscan.io/address/0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409#readProxyContract", "pause() in ABI"),
            "blacklist": _p(True, "https://etherscan.io/address/0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409#readProxyContract", "blacklist in ABI"),
            "reserve_ratio": _p(1.00, "https://firstdigitallabs.com/transparency", "Reserve Attestation"),
            "cash_pct": _p(90, "https://firstdigitallabs.com/transparency", "Reserve Composition"),
            "regulatory_status": _p(60, "https://firstdigitallabs.com/", "Hong Kong TCSP"),
        },
        "tusd": {
            "admin_key_risk": _p(35, "https://etherscan.io/address/0x0000000000085d4780B73119b644AE5ecd22b376#readProxyContract", "Centralized, ownership disputes"),
            "bug_bounty_active": _p(False, "https://tusd.io/", "No public bounty"),
            "bug_bounty_max_payout": _p(0, "https://tusd.io/", "N/A"),
            "pausable": _p(True, "https://etherscan.io/address/0x0000000000085d4780B73119b644AE5ecd22b376#readProxyContract", "pause() in ABI"),
            "blacklist": _p(True, "https://etherscan.io/address/0x0000000000085d4780B73119b644AE5ecd22b376#readProxyContract", "blacklist in ABI"),
            "reserve_ratio": _p(1.00, "https://tusd.io/transparency", "Reserve Attestation"),
            "cash_pct": _p(85, "https://tusd.io/transparency", "Reserve Composition"),
            "exploit_history": _p(1, "https://rekt.news/trueusd-rekt/", "Reserve backing disputes"),
            "regulatory_status": _p(55, "https://tusd.io/", "Various state licenses"),
        },
        "usdd": {
            "admin_key_risk": _p(30, "https://usdd.io/", "TRON DAO Reserve centralized"),
            "bug_bounty_active": _p(False, "https://usdd.io/", "No public bounty"),
            "bug_bounty_max_payout": _p(0, "https://usdd.io/", "N/A"),
            "pausable": _p(False, "https://tronscan.org/#/contract/TPYmHEhy5n8TCEfYGqW2rPxsghSfzghPDn/code", "No pause function"),
            "blacklist": _p(True, "https://tronscan.org/#/contract/TPYmHEhy5n8TCEfYGqW2rPxsghSfzghPDn/code", "blacklist in ABI"),
            "reserve_ratio": _p(3.00, "https://usdd.io/#/collateral", "Over-collateralized reserve"),
            "cash_pct": _p(10, "https://usdd.io/#/collateral", "Crypto collateral"),
            "exploit_history": _p(1, "https://rekt.news/", "Sustained depeg below $0.97"),
            "regulatory_status": _p(40, "https://usdd.io/", "Limited regulation"),
        },
        "usde": {
            "admin_key_risk": _p(65, "https://etherscan.io/address/0x4c9EDD5852cd905f086C759E8383e09bff1E68B3#readProxyContract", "Ethena multisig governance"),
            "bug_bounty_active": _p(True, "https://immunefi.com/bug-bounty/ethena/", "Bounty Program"),
            "bug_bounty_max_payout": _p(250_000, "https://immunefi.com/bug-bounty/ethena/", "Max Payout"),
            "pausable": _p(False, "https://etherscan.io/address/0x4c9EDD5852cd905f086C759E8383e09bff1E68B3#readProxyContract", "No pause function"),
            "blacklist": _p(False, "https://etherscan.io/address/0x4c9EDD5852cd905f086C759E8383e09bff1E68B3#readProxyContract", "No blacklist function"),
            "reserve_ratio": _p(1.01, "https://app.ethena.fi/dashboards/transparency", "Delta-neutral backing"),
            "cash_pct": _p(0, "https://app.ethena.fi/dashboards/transparency", "Crypto derivatives backing"),
            "regulatory_status": _p(45, "https://ethena.fi/", "Newer protocol"),
        },
        "usd1": {
            "admin_key_risk": _p(45, "https://etherscan.io/address/0x5EBB3f2feaA15271101a927869B3A56837e73056#readProxyContract", "World Liberty Financial admin"),
            "bug_bounty_active": _p(False, "https://worldlibertyfinancial.com/", "No public bounty"),
            "bug_bounty_max_payout": _p(0, "https://worldlibertyfinancial.com/", "N/A"),
            "pausable": _p(True, "https://etherscan.io/address/0x5EBB3f2feaA15271101a927869B3A56837e73056#readProxyContract", "pause() in ABI"),
            "blacklist": _p(True, "https://etherscan.io/address/0x5EBB3f2feaA15271101a927869B3A56837e73056#readProxyContract", "blacklist in ABI"),
            "reserve_ratio": _p(1.00, "https://worldlibertyfinancial.com/", "Reserve backing"),
            "cash_pct": _p(90, "https://worldlibertyfinancial.com/", "Reserve Composition"),
            "regulatory_status": _p(55, "https://worldlibertyfinancial.com/", "Newer issuer"),
        },
    },

    # -----------------------------------------------------------------
    # PSI — Protocol Solvency Index
    # -----------------------------------------------------------------
    "psi": {
        "aave": {
            "protocol_admin_key_risk": _p(90, "https://docs.aave.com/governance/", "Aave Governance V3"),
            "bad_debt": _p(0, "https://community.aave.com/", "No known bad debt"),
            "governance_token_gecko_id": _p("aave", "https://www.coingecko.com/en/coins/aave", "CoinGecko listing"),
            "snapshot_space": _p("aavedao.eth", "https://snapshot.org/#/aavedao.eth", "Snapshot governance"),
        },
        "lido": {
            "protocol_admin_key_risk": _p(85, "https://docs.lido.fi/guides/lido-dao/", "LidoDAO governance + multisig"),
            "bad_debt": _p(0, "https://lido.fi/", "No known bad debt"),
            "governance_token_gecko_id": _p("lido-dao", "https://www.coingecko.com/en/coins/lido-dao", "CoinGecko listing"),
            "snapshot_space": _p("lido-snapshot.eth", "https://snapshot.org/#/lido-snapshot.eth", "Snapshot governance"),
        },
        "eigenlayer": {
            "protocol_admin_key_risk": _p(60, "https://docs.eigenlayer.xyz/", "Early stage, team-controlled"),
            "bad_debt": _p(0, "https://eigenlayer.xyz/", "No known bad debt"),
            "governance_token_gecko_id": _p("eigenlayer", "https://www.coingecko.com/en/coins/eigenlayer", "CoinGecko listing"),
        },
        "sky": {
            "protocol_admin_key_risk": _p(90, "https://docs.sky.money/", "MakerDAO DSChief on-chain governance"),
            "bad_debt": _p(0, "https://makerburn.com/", "Historical bad debt resolved"),
            "governance_token_gecko_id": _p("maker", "https://www.coingecko.com/en/coins/maker", "CoinGecko listing (MKR)"),
            "onchain_governance_proposals_90d": _p(15, "https://vote.makerdao.com/", "Executive votes + polls"),
        },
        "compound-finance": {
            "protocol_admin_key_risk": _p(85, "https://docs.compound.finance/v2/governance/", "Compound Governor Bravo"),
            "bad_debt": _p(0, "https://compound.finance/", "No known bad debt"),
            "governance_token_gecko_id": _p("compound-governance-token", "https://www.coingecko.com/en/coins/compound", "CoinGecko listing"),
            "snapshot_space": _p("comp-vote.eth", "https://snapshot.org/#/comp-vote.eth", "Snapshot governance"),
        },
        "uniswap": {
            "protocol_admin_key_risk": _p(85, "https://docs.uniswap.org/contracts/v3/reference/governance/governance", "Uniswap on-chain governance"),
            "bad_debt": _p(0, "https://uniswap.org/", "No known bad debt"),
            "governance_token_gecko_id": _p("uniswap", "https://www.coingecko.com/en/coins/uniswap", "CoinGecko listing"),
            "snapshot_space": _p("uniswapgovernance.eth", "https://snapshot.org/#/uniswapgovernance.eth", "Snapshot governance"),
        },
        "curve-finance": {
            "protocol_admin_key_risk": _p(85, "https://resources.curve.fi/governance/understanding-governance/", "veCRV governance"),
            "bad_debt": _p(0, "https://curve.fi/", "No known bad debt"),
            "governance_token_gecko_id": _p("curve-dao-token", "https://www.coingecko.com/en/coins/curve-dao-token", "CoinGecko listing"),
            "snapshot_space": _p("curve.eth", "https://snapshot.org/#/curve.eth", "Snapshot governance"),
        },
        "morpho": {
            "protocol_admin_key_risk": _p(65, "https://docs.morpho.org/governance/overview/", "Newer protocol, multisig governance"),
            "bad_debt": _p(0, "https://morpho.org/", "No known bad debt"),
            "governance_token_gecko_id": _p("morpho", "https://www.coingecko.com/en/coins/morpho", "CoinGecko listing"),
        },
        "spark": {
            "protocol_admin_key_risk": _p(70, "https://docs.spark.fi/governance/overview", "Sub-DAO of MakerDAO"),
            "bad_debt": _p(0, "https://spark.fi/", "No known bad debt"),
        },
        "convex-finance": {
            "protocol_admin_key_risk": _p(75, "https://docs.convexfinance.com/convexfinance/general-information/governance", "Multisig + veCVX governance"),
            "bad_debt": _p(0, "https://convexfinance.com/", "No known bad debt"),
            "governance_token_gecko_id": _p("convex-finance", "https://www.coingecko.com/en/coins/convex-finance", "CoinGecko listing"),
            "snapshot_space": _p("cvx.eth", "https://snapshot.org/#/cvx.eth", "Snapshot governance"),
        },
        "drift": {
            "protocol_admin_key_risk": _p(50, "https://docs.drift.trade/", "Solana program — upgrade authority unknown"),
            "bad_debt": _p({"amount": 270000000, "since": "2026-04-01"}, "https://docs.drift.trade/insurance-fund/", "Insurance fund event"),
            "governance_token_gecko_id": _p("drift-protocol", "https://www.coingecko.com/en/coins/drift-protocol", "CoinGecko listing"),
        },
        "jupiter-perpetual-exchange": {
            "protocol_admin_key_risk": _p(55, "https://station.jup.ag/docs/", "Solana program — JUP DAO governance active"),
            "bad_debt": _p(0, "https://jup.ag/", "No known bad debt"),
            "governance_token_gecko_id": _p("jupiter-exchange-solana", "https://www.coingecko.com/en/coins/jupiter", "CoinGecko listing"),
        },
        "raydium": {
            "protocol_admin_key_risk": _p(50, "https://docs.raydium.io/", "Solana program — upgrade authority unknown"),
            "bad_debt": _p(0, "https://raydium.io/", "No known bad debt"),
            "governance_token_gecko_id": _p("raydium", "https://www.coingecko.com/en/coins/raydium", "CoinGecko listing"),
        },
    },
}


def iter_all_static_components():
    """
    Yield (index_id, entity_slug, component_name, entry) for every
    provenance-annotated static component in the registry.
    """
    for index_id, entities in REGISTRY.items():
        for entity_slug, components in entities.items():
            for component_name, entry in components.items():
                if isinstance(entry, dict) and "source_url" in entry:
                    yield index_id, entity_slug, component_name, entry


def count_static_components():
    """Return total count of registered static components."""
    return sum(1 for _ in iter_all_static_components())
