"""
Smart Contract & Security Collector
=====================================
Produces components for the smart_contract and security categories:

  - contract_verified:          Etherscan getabi succeeds → source is verified
  - pausability:                ABI contains pause/unpause functions
  - blacklist_capability:       ABI contains blacklist/freeze functions
  - admin_key_risk:             Admin structure analysis (multisig, EOA, governance)
  - bug_bounty_score:           Active bug bounty program (Immunefi etc.)
  - exploit_history:            Past security incidents

Data sources: Etherscan getabi + static config for bounty/exploit history.
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timezone

import httpx

from app.database import fetch_one

logger = logging.getLogger(__name__)

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
RATE_LIMIT_DELAY = 0.15

# ============================================================================
# Static security config — updated manually as conditions change
# ============================================================================

# Admin key risk: assessed from on-chain analysis of each token's admin structure.
# Score meaning: 0=EOA with no timelock, 50=2-of-3 multisig, 80=3-of-5+, 95=governance
ADMIN_KEY_RISK = {
    "usdc":   80,   # Circle: 3-of-6 multisig + timelock (FiatTokenV2)
    "usdt":   40,   # Tether: centralized issuer, limited multisig info
    "dai":    90,   # MakerDAO: on-chain governance (DSChief), timelock
    "frax":   75,   # Frax: multisig + governance (veFXS)
    "pyusd":  70,   # PayPal: corporate issuer, multisig admin
    "fdusd":  50,   # First Digital: centralized issuer, 2-of-3 multisig
    "tusd":   35,   # TrueUSD: centralized, ownership disputes history
    "usdd":   30,   # TRON DAO Reserve: centralized reserve management
    "usde":   65,   # Ethena: multisig governance, newer protocol
    "usd1":   45,   # World Liberty Financial: newer, limited governance info
    "gho":    85,   # Aave: on-chain governance (Aave Gov V3)
    "crvusd": 85,   # Curve: on-chain governance (veCRV)
    "dola":   80,   # Inverse Finance: on-chain governance
    "usdp":   60,   # Paxos: regulated issuer, corporate multisig
}

# Bug bounty programs
BUG_BOUNTY = {
    "usdc":   {"active": True, "max_payout": 250_000, "platform": "Immunefi"},
    "usdt":   {"active": False, "max_payout": 0},
    "dai":    {"active": True, "max_payout": 10_000_000, "platform": "Immunefi"},
    "frax":   {"active": True, "max_payout": 500_000, "platform": "Immunefi"},
    "pyusd":  {"active": False, "max_payout": 0},
    "fdusd":  {"active": False, "max_payout": 0},
    "tusd":   {"active": False, "max_payout": 0},
    "usdd":   {"active": False, "max_payout": 0},
    "usde":   {"active": True, "max_payout": 250_000, "platform": "Immunefi"},
    "usd1":   {"active": False, "max_payout": 0},
    "gho":    {"active": True, "max_payout": 15_000_000, "platform": "Immunefi"},
    "crvusd": {"active": True, "max_payout": 250_000, "platform": "Immunefi"},
    "dola":   {"active": True, "max_payout": 500_000, "platform": "Immunefi"},
    "usdp":   {"active": False, "max_payout": 0},
}

# Exploit history — from DeFiLlama hacks + known incidents
# date is approximate, amount in USD
EXPLOIT_HISTORY = {
    "tusd":   [{"date": "2023-10-01", "amount": 0, "desc": "Reserve backing disputes, depegged"}],
    "usdd":   [{"date": "2023-06-15", "amount": 0, "desc": "Sustained depeg below $0.97"}],
    "dola":   [{"date": "2022-06-16", "amount": 1_200_000, "desc": "Inverse Finance oracle manipulation"}],
    # USDC, USDT, DAI, FRAX, PYUSD, FDUSD, USDE, USD1, GHO, CRVUSD, USDP — no direct exploits
}

# Pausability and blacklist capability per token (from ABI analysis)
ABI_FEATURES = {
    "usdc":   {"pausable": True, "blacklist": True},
    "usdt":   {"pausable": True, "blacklist": True},
    "dai":    {"pausable": False, "blacklist": False},
    "frax":   {"pausable": False, "blacklist": False},
    "pyusd":  {"pausable": True, "blacklist": True},
    "fdusd":  {"pausable": True, "blacklist": True},
    "tusd":   {"pausable": True, "blacklist": True},
    "usdd":   {"pausable": False, "blacklist": True},
    "usde":   {"pausable": False, "blacklist": False},
    "usd1":   {"pausable": True, "blacklist": True},
    "gho":    {"pausable": False, "blacklist": False},
    "crvusd": {"pausable": False, "blacklist": False},
    "dola":   {"pausable": False, "blacklist": False},
    "usdp":   {"pausable": True, "blacklist": True},
}


# ============================================================================
# Live ABI verification check
# ============================================================================

async def _check_contract_verified(
    client: httpx.AsyncClient, contract: str, api_key: str
) -> bool:
    """Check if contract source is verified on Etherscan."""
    try:
        resp = await client.get(ETHERSCAN_V2_BASE, params={
            "chainid": 1,
            "module": "contract",
            "action": "getabi",
            "address": contract,
            "apikey": api_key,
        }, timeout=20)
        data = resp.json()
        return data.get("status") == "1" and data.get("result", "").startswith("[")
    except Exception as e:
        logger.warning(f"ABI check failed for {contract}: {e}")
        return False


# ============================================================================
# Normalization helpers
# ============================================================================

def _score_bug_bounty(stablecoin_id: str) -> float:
    """Score bug bounty program. Active with >$100K = 100, <$100K = 70, none = 20."""
    info = BUG_BOUNTY.get(stablecoin_id, {})
    if not info.get("active"):
        return 20.0
    if info.get("max_payout", 0) >= 100_000:
        return 100.0
    return 70.0


def _score_exploit_history(stablecoin_id: str) -> float:
    """Score exploit history. No exploits = 100, >1yr ago = 60, <1yr = 20, <90d = 0."""
    exploits = EXPLOIT_HISTORY.get(stablecoin_id, [])
    if not exploits:
        return 100.0

    now = datetime.now(timezone.utc)
    most_recent = None
    for exp in exploits:
        try:
            d = datetime.strptime(exp["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if most_recent is None or d > most_recent:
                most_recent = d
        except (ValueError, KeyError):
            continue

    if most_recent is None:
        return 100.0

    days_ago = (now - most_recent).days
    if days_ago < 90:
        return 0.0
    if days_ago < 365:
        return 20.0
    return 60.0


def _score_pausability(stablecoin_id: str) -> float:
    """Pausable = 60 (tradeoff), not pausable = 100 (decentralized)."""
    features = ABI_FEATURES.get(stablecoin_id, {})
    return 60.0 if features.get("pausable") else 100.0


def _score_blacklist(stablecoin_id: str) -> float:
    """Blacklist capability = 60, no blacklist = 100."""
    features = ABI_FEATURES.get(stablecoin_id, {})
    return 60.0 if features.get("blacklist") else 100.0


# ============================================================================
# Main collector
# ============================================================================

async def collect_smart_contract_components(
    client: httpx.AsyncClient, stablecoin_id: str
) -> list[dict]:
    """
    Collect smart contract risk components for one stablecoin.
    Returns list of component dicts ready for DB insert.
    """
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")

    # Get contract address
    row = fetch_one(
        "SELECT contract FROM stablecoins WHERE id = %s", (stablecoin_id,)
    )
    contract = row.get("contract", "") if row else ""

    components = []

    # 1. contract_verified — live API check
    if contract and api_key:
        verified = await _check_contract_verified(client, contract, api_key)
        await asyncio.sleep(RATE_LIMIT_DELAY)
        components.append({
            "component_id": "contract_verified",
            "category": "smart_contract",
            "raw_value": 1 if verified else 0,
            "normalized_score": 100.0 if verified else 0.0,
            "data_source": "etherscan",
        })
    else:
        components.append({
            "component_id": "contract_verified",
            "category": "smart_contract",
            "raw_value": 0,
            "normalized_score": 0.0,
            "data_source": "static",
        })

    # 2. pausability — from ABI feature config
    pause_score = _score_pausability(stablecoin_id)
    components.append({
        "component_id": "pausability",
        "category": "smart_contract",
        "raw_value": 1 if ABI_FEATURES.get(stablecoin_id, {}).get("pausable") else 0,
        "normalized_score": pause_score,
        "data_source": "config",
    })

    # 3. blacklist_capability
    bl_score = _score_blacklist(stablecoin_id)
    components.append({
        "component_id": "blacklist_capability",
        "category": "smart_contract",
        "raw_value": 1 if ABI_FEATURES.get(stablecoin_id, {}).get("blacklist") else 0,
        "normalized_score": bl_score,
        "data_source": "config",
    })

    # 4. admin_key_risk — from static analysis config
    admin_score = float(ADMIN_KEY_RISK.get(stablecoin_id, 10))
    components.append({
        "component_id": "admin_key_risk",
        "category": "smart_contract",
        "raw_value": admin_score,
        "normalized_score": admin_score,
        "data_source": "config",
    })

    # 5. bug_bounty_score
    bounty_score = _score_bug_bounty(stablecoin_id)
    bounty_raw = BUG_BOUNTY.get(stablecoin_id, {}).get("max_payout", 0)
    components.append({
        "component_id": "bug_bounty_score",
        "category": "smart_contract",
        "raw_value": bounty_raw,
        "normalized_score": bounty_score,
        "data_source": "config",
    })

    # 6. exploit_history
    exploit_score = _score_exploit_history(stablecoin_id)
    exploits = EXPLOIT_HISTORY.get(stablecoin_id, [])
    components.append({
        "component_id": "exploit_history",
        "category": "smart_contract",
        "raw_value": len(exploits),
        "normalized_score": exploit_score,
        "data_source": "config",
    })

    return components
