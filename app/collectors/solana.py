"""
Solana-specific data collector using Helius API.
Provides supplementary SII components for stablecoins with Solana SPL tokens:
holder distribution, supply data, and mint/burn flow detection.

Free tier: 1M credits/month, 10 RPS.
Gracefully returns empty data if HELIUS_API_KEY is not set.
"""

import os
import logging

import httpx

logger = logging.getLogger(__name__)

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_API_URL = "https://api.helius.xyz"

# Solana SPL mint addresses for SII-scored stablecoins
SOLANA_STABLECOIN_MINTS = {
    "usdc": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "usdt": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
}

RATE_LIMIT_DELAY = 0.15  # seconds between Helius calls


async def get_solana_token_supply(client: httpx.AsyncClient, mint_address: str) -> dict:
    """Get current token supply via Solana RPC."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenSupply",
        "params": [mint_address],
    }
    try:
        resp = await client.post(HELIUS_RPC_URL, json=payload, timeout=30)
        data = resp.json()
        result = data.get("result", {}).get("value", {})
        return {
            "supply": float(result.get("uiAmount", 0)),
            "decimals": result.get("decimals", 0),
            "raw_amount": result.get("amount", "0"),
        }
    except Exception as e:
        logger.error(f"Solana getTokenSupply error for {mint_address}: {e}")
        return {}


async def get_solana_largest_holders(client: httpx.AsyncClient, mint_address: str) -> dict:
    """Get top 20 token holders via Solana RPC."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [mint_address],
    }
    try:
        resp = await client.post(HELIUS_RPC_URL, json=payload, timeout=30)
        data = resp.json()
        accounts = data.get("result", {}).get("value", [])

        holders = []
        for a in accounts:
            amt = float(a.get("uiAmount", 0)) if a.get("uiAmount") is not None else 0
            holders.append({
                "address": a.get("address", ""),
                "amount": amt,
            })

        total_top20 = sum(h["amount"] for h in holders)
        largest = holders[0]["amount"] if holders else 0

        return {
            "top_holders": holders,
            "top_20_total": total_top20,
            "largest_holder_amount": largest,
            "holder_count": len(holders),
        }
    except Exception as e:
        logger.error(f"Solana getTokenLargestAccounts error for {mint_address}: {e}")
        return {}


async def get_solana_token_transfers(
    client: httpx.AsyncClient, mint_address: str, limit: int = 100
) -> list:
    """Get recent parsed token transfers via Helius."""
    url = f"{HELIUS_API_URL}/v0/addresses/{mint_address}/transactions"
    params = {
        "api-key": HELIUS_API_KEY,
        "type": "TRANSFER",
        "limit": limit,
    }
    try:
        resp = await client.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            logger.error(f"Helius transfers error: {resp.status_code}")
            return []
        return resp.json()
    except Exception as e:
        logger.error(f"Helius transfers error for {mint_address}: {e}")
        return []


async def get_solana_program_info(client: httpx.AsyncClient, program_id: str) -> dict:
    """Check Solana program upgrade authority and executable status."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [program_id, {"encoding": "jsonParsed"}],
    }
    try:
        resp = await client.post(HELIUS_RPC_URL, json=payload, timeout=30)
        data = resp.json()
        account = data.get("result", {}).get("value", {})
        if not account:
            return {"executable": False, "admin_key_risk": 50}

        return {
            "executable": account.get("executable", False),
            "owner": account.get("owner", ""),
            "lamports": account.get("lamports", 0),
            "admin_key_risk": 50,  # default unknown for Solana programs
        }
    except Exception as e:
        logger.error(f"Solana getAccountInfo error for {program_id}: {e}")
        return {"executable": False, "admin_key_risk": 50}


# =============================================================================
# Main SII collector entry point
# =============================================================================

async def collect_solana_components(
    client: httpx.AsyncClient, stablecoin_id: str
) -> list[dict]:
    """
    Collect Solana-specific SII components for a stablecoin.
    Returns list of component dicts matching the standard format.
    Only runs for stablecoins with known Solana SPL mint addresses.
    """
    mint = SOLANA_STABLECOIN_MINTS.get(stablecoin_id)
    if not mint:
        return []

    if not HELIUS_API_KEY:
        logger.debug(f"HELIUS_API_KEY not set — skipping Solana collection for {stablecoin_id}")
        return []

    components = []
    import asyncio

    # --- 1. Supply data ---
    try:
        supply = await get_solana_token_supply(client, mint)
        if supply.get("supply"):
            components.append({
                "component_id": "solana_spl_supply",
                "category": "distribution",
                "raw_value": supply["supply"],
                "normalized_score": None,  # informational, not scored directly
                "data_source": "helius",
                "metadata": {"chain": "solana", "mint": mint, "decimals": supply.get("decimals")},
            })
    except Exception as e:
        logger.error(f"Solana supply error for {stablecoin_id}: {e}")
    await asyncio.sleep(RATE_LIMIT_DELAY)

    # --- 2. Top holder concentration ---
    try:
        holders = await get_solana_largest_holders(client, mint)
        if holders.get("top_20_total") and holders.get("top_holders"):
            supply_val = components[0]["raw_value"] if components and components[0].get("raw_value") else None

            top20_total = holders["top_20_total"]
            largest = holders["largest_holder_amount"]

            # Concentration: top-20 as % of known supply
            if supply_val and supply_val > 0:
                top20_pct = (top20_total / supply_val) * 100
            else:
                top20_pct = 0

            # Score: lower concentration = better (inverted)
            # >80% = 10, 60-80% = 30, 40-60% = 50, 20-40% = 70, <20% = 90
            if top20_pct >= 80:
                conc_score = 10.0
            elif top20_pct >= 60:
                conc_score = 30.0
            elif top20_pct >= 40:
                conc_score = 50.0
            elif top20_pct >= 20:
                conc_score = 70.0
            else:
                conc_score = 90.0

            components.append({
                "component_id": "solana_holder_concentration",
                "category": "distribution",
                "raw_value": round(top20_pct, 2),
                "normalized_score": conc_score,
                "data_source": "helius",
                "metadata": {
                    "chain": "solana",
                    "top_20_total": top20_total,
                    "largest_holder": largest,
                    "holder_count_sampled": holders.get("holder_count", 0),
                },
            })
    except Exception as e:
        logger.error(f"Solana holders error for {stablecoin_id}: {e}")
    await asyncio.sleep(RATE_LIMIT_DELAY)

    # --- 3. Mint/burn flow detection ---
    try:
        transfers = await get_solana_token_transfers(client, mint, limit=100)
        if transfers:
            mint_count = 0
            burn_count = 0
            transfer_count = len(transfers)

            system_addrs = {None, "", "11111111111111111111111111111111"}
            for tx in transfers:
                for tt in tx.get("tokenTransfers", []):
                    if tt.get("fromUserAccount") in system_addrs:
                        mint_count += 1
                    if tt.get("toUserAccount") in system_addrs:
                        burn_count += 1

            total_flow = mint_count + burn_count
            if total_flow > 0:
                ratio = mint_count / total_flow
            else:
                ratio = 0.5  # balanced if no activity

            # Score: closer to 0.5 = balanced = better
            balance_score = max(0, 100 - abs(ratio - 0.5) * 200)

            components.append({
                "component_id": "solana_mint_burn_ratio",
                "category": "flows",
                "raw_value": round(ratio, 4),
                "normalized_score": round(balance_score, 2),
                "data_source": "helius",
                "metadata": {
                    "chain": "solana",
                    "recent_mints": mint_count,
                    "recent_burns": burn_count,
                    "transfer_count": transfer_count,
                },
            })
    except Exception as e:
        logger.error(f"Solana transfers error for {stablecoin_id}: {e}")

    if components:
        logger.info(f"Solana collector: {len(components)} components for {stablecoin_id}")

    return components
