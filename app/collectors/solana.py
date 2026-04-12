"""
Solana-specific data collector using Helius API.
Provides supplementary SII components for stablecoins with Solana SPL tokens:
supply, holder distribution, mint/burn flows, freeze/mint authority, velocity,
and unusual minting detection.

Also provides on-chain vault balance reading for Drift collateral exposure.

Free tier: 1M credits/month, 10 RPS.
Gracefully returns empty data if HELIUS_API_KEY is not set.
"""

import asyncio
import os
import logging

import httpx

from app.data_source_registry import register_data_source

logger = logging.getLogger(__name__)

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_API_URL = "https://api.helius.xyz"

# Solana SPL mint addresses for SII-scored stablecoins
SOLANA_STABLECOIN_MINTS = {
    "usdc": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "usdt": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
}

# Drift v2 program ID on Solana mainnet
DRIFT_PROGRAM_ID = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"

# SPL Token program ID
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

RATE_LIMIT_DELAY = 0.15  # seconds between Helius calls

# Estimated unique Solana SPL token account counts (from Solscan/Solana FM)
# Updated periodically — serves as fallback since full enumeration is too expensive
SOLANA_HOLDER_ESTIMATES = {
    "usdc": 3_800_000,   # USDC is the dominant Solana stablecoin
    "usdt": 1_200_000,
}

# =============================================================================
# Static security config for Solana stablecoins
# Mirrors app/collectors/smart_contract.py approach for EVM
# =============================================================================

# Program verification status — checked against OtterSec Verified / Solana Verify
SOLANA_VERIFIED_PROGRAMS = {
    "usdc": True,    # Circle's SPL token — standard SPL Token program (verified)
    "usdt": True,    # Tether SPL token — standard SPL Token program (verified)
}

# Bug bounty programs (Solana-specific or cross-chain coverage)
SOLANA_BUG_BOUNTY = {
    "usdc": {"active": True, "max_payout": 250_000, "platform": "Immunefi", "note": "Cross-chain program covers Solana"},
    "usdt": {"active": False, "max_payout": 0},
}

# Exploit history on Solana (separate from EVM incidents)
SOLANA_EXPLOIT_HISTORY = {
    # USDC and USDT on Solana: no direct Solana-specific exploits
}

# Pausability on Solana: mapped from freeze authority capability
# If the token has a freeze authority, the issuer can freeze individual accounts (≈ pause)
# This is detected live from on-chain data in section 4, but we need a static fallback
SOLANA_PAUSABILITY = {
    "usdc": True,    # Circle has freeze authority on Solana USDC
    "usdt": True,    # Tether has freeze authority on Solana USDT
}


# =============================================================================
# RPC helpers
# =============================================================================

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
    """
    Get top token holders via Solana RPC (getTokenLargestAccounts).
    Falls back to Helius DAS getTokenAccounts for large tokens where
    the standard RPC errors out due to too many accounts.
    """
    # --- Try standard RPC first ---
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [mint_address],
    }
    try:
        resp = await client.post(HELIUS_RPC_URL, json=payload, timeout=30)
        data = resp.json()

        if "error" not in data:
            accounts = data.get("result", {}).get("value", [])
            if accounts:
                holders = []
                for a in accounts:
                    amt = float(a.get("uiAmount", 0)) if a.get("uiAmount") is not None else 0
                    holders.append({"address": a.get("address", ""), "amount": amt})

                total_top20 = sum(h["amount"] for h in holders)
                largest = holders[0]["amount"] if holders else 0
                return {
                    "top_holders": holders,
                    "top_20_total": total_top20,
                    "largest_holder_amount": largest,
                    "holder_count": len(holders),
                }

        # --- Fallback: Helius DAS getTokenAccounts (handles large tokens) ---
        logger.info(f"getTokenLargestAccounts failed for {mint_address[:12]}…, falling back to DAS")
        return await _get_holders_via_das(client, mint_address)

    except Exception as e:
        logger.error(f"Solana getTokenLargestAccounts error for {mint_address}: {e}")
        # Try DAS as last resort
        try:
            return await _get_holders_via_das(client, mint_address)
        except Exception as e2:
            logger.error(f"Solana DAS holder fallback also failed: {e2}")
            return {}


async def _get_holders_via_das(client: httpx.AsyncClient, mint_address: str) -> dict:
    """
    Fetch token holders via Helius DAS getTokenAccounts API.
    Retrieves up to 100 accounts, sorts by balance descending, returns top 20.
    Also fetches total supply via getAsset for concentration calculations.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccounts",
        "params": {
            "mint": mint_address,
            "limit": 100,
            "options": {"showZeroBalance": False},
        },
    }
    resp = await client.post(HELIUS_RPC_URL, json=payload, timeout=30)
    data = resp.json()
    items = data.get("result", {}).get("token_accounts", [])

    if not items:
        return {}

    # Get decimals from getAsset for proper amount conversion
    asset_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAsset",
        "params": {"id": mint_address},
    }
    await asyncio.sleep(RATE_LIMIT_DELAY)
    asset_resp = await client.post(HELIUS_RPC_URL, json=asset_payload, timeout=30)
    asset_data = asset_resp.json()
    decimals = asset_data.get("result", {}).get("token_info", {}).get("decimals", 6)

    # Convert raw amounts and sort descending
    holders = []
    for item in items:
        raw_amount = item.get("amount", 0)
        ui_amount = raw_amount / (10 ** decimals) if decimals > 0 else raw_amount
        holders.append({
            "address": item.get("owner", item.get("address", "")),
            "amount": ui_amount,
        })

    holders.sort(key=lambda h: h["amount"], reverse=True)
    top20 = holders[:20]

    total_top20 = sum(h["amount"] for h in top20)
    largest = top20[0]["amount"] if top20 else 0

    return {
        "top_holders": top20,
        "top_20_total": total_top20,
        "largest_holder_amount": largest,
        "holder_count": len(holders),
    }


async def get_solana_token_transfers(
    client: httpx.AsyncClient, mint_address: str, limit: int = 100
) -> list:
    """Get recent parsed token transfers via Helius."""
    register_data_source("api.helius.xyz", "/v0/addresses/{address}/transactions", "sii_solana_collector",
                         description="Solana token transfers for SII flow analysis",
                         params_template={"type": "TRANSFER"})
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


async def get_solana_mint_info(client: httpx.AsyncClient, mint_address: str) -> dict:
    """
    Read SPL token mint account for freeze authority + mint authority.
    jsonParsed encoding returns structured data for SPL token mints.

    - freezeAuthority: if set, issuer can freeze any token account (= blacklist/pause)
    - mintAuthority: if set, issuer can mint new tokens (= supply control)
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [mint_address, {"encoding": "jsonParsed"}],
    }
    try:
        resp = await client.post(HELIUS_RPC_URL, json=payload, timeout=30)
        data = resp.json()
        account = data.get("result", {}).get("value", {})
        if not account:
            return {}

        parsed = account.get("data", {}).get("parsed", {})
        info = parsed.get("info", {})

        decimals = info.get("decimals", 0)
        return {
            "freeze_authority": info.get("freezeAuthority"),
            "mint_authority": info.get("mintAuthority"),
            "has_freeze": info.get("freezeAuthority") is not None,
            "has_mint_authority": info.get("mintAuthority") is not None,
            "supply": int(info.get("supply", "0")) / (10 ** decimals) if decimals > 0 else 0,
            "decimals": decimals,
        }
    except Exception as e:
        logger.error(f"Solana mint info error for {mint_address}: {e}")
        return {}


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
# Main SII collector entry point — 9 components
# =============================================================================

async def collect_solana_components(
    client: httpx.AsyncClient, stablecoin_id: str
) -> list[dict]:
    """
    Collect Solana-specific SII components for a stablecoin.
    Returns list of component dicts matching the standard format.
    Only runs for stablecoins with known Solana SPL mint addresses.

    Components produced (up to 9):
      1. solana_spl_supply           — total SPL token supply
      2. solana_holder_concentration — top-20 holder % of supply
      3. solana_mint_burn_ratio      — mint/(mint+burn) ratio
      4. solana_freeze_authority     — freeze authority present?
      5. solana_mint_authority       — mint authority present?
      6. solana_daily_mint_volume    — USD value of recent mints
      7. solana_daily_burn_volume    — USD value of recent burns
      8. solana_supply_change_velocity — abs(net flow / supply) %
      9. solana_unusual_minting      — deviation from expected mint rate
    """
    mint = SOLANA_STABLECOIN_MINTS.get(stablecoin_id)
    if not mint:
        return []

    if not HELIUS_API_KEY:
        logger.debug(f"HELIUS_API_KEY not set — skipping Solana collection for {stablecoin_id}")
        return []

    register_data_source("mainnet.helius-rpc.com", "/rpc/getTokenSupply", "sii_solana_collector",
                         method="POST", prove=False,
                         description="Solana token supply for SII scoring",
                         notes="POST/JSON-RPC — TLSNotary POST support unverified")
    register_data_source("mainnet.helius-rpc.com", "/rpc/getTokenLargestAccounts", "sii_solana_collector",
                         method="POST", prove=False,
                         description="Solana top holders for SII distribution scoring",
                         notes="POST/JSON-RPC — TLSNotary POST support unverified")

    components = []
    supply_val = None  # populated by section 1, used by later sections

    # --- 1. Supply data ---
    try:
        supply = await get_solana_token_supply(client, mint)
        if supply.get("supply"):
            supply_val = supply["supply"]
            components.append({
                "component_id": "solana_spl_supply",
                "category": "distribution",
                "raw_value": supply_val,
                "normalized_score": None,  # informational, not scored directly
                "data_source": "helius",
                "metadata": {"chain": "solana", "mint": mint, "decimals": supply.get("decimals")},
            })
    except Exception as e:
        logger.error(f"Solana supply error for {stablecoin_id}: {e}")
    await asyncio.sleep(RATE_LIMIT_DELAY)

    # --- 2. Top holder concentration (top-20 and top-10) ---
    try:
        holders = await get_solana_largest_holders(client, mint)
        if holders.get("top_20_total") and holders.get("top_holders"):
            top20_total = holders["top_20_total"]
            largest = holders["largest_holder_amount"]
            top_holders = holders["top_holders"]

            if supply_val and supply_val > 0:
                top20_pct = (top20_total / supply_val) * 100
            else:
                top20_pct = 0

            # Score: lower concentration = better (inverted)
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

            # 2b. Top-10 holder concentration (maps to EVM top_10_concentration)
            top10 = top_holders[:10]
            top10_total = sum(h["amount"] for h in top10)
            if supply_val and supply_val > 0:
                top10_pct = (top10_total / supply_val) * 100
            else:
                top10_pct = 0

            from app.scoring import normalize_inverse_linear
            top10_score = normalize_inverse_linear(top10_pct, 10, 80)

            components.append({
                "component_id": "solana_top_10_concentration",
                "category": "holder_distribution",
                "raw_value": round(top10_pct, 4),
                "normalized_score": round(top10_score, 2),
                "data_source": "helius",
                "metadata": {
                    "chain": "solana",
                    "top_10_total": top10_total,
                    "description": f"Top 10 Solana holders control {top10_pct:.2f}% of SPL supply",
                },
            })

            # 2c. Unique holder count (static estimate — full enumeration too expensive)
            holder_count = SOLANA_HOLDER_ESTIMATES.get(stablecoin_id, 50_000)
            from app.scoring import normalize_log
            holders_score = normalize_log(
                holder_count,
                thresholds={1000: 20, 10000: 40, 100000: 60, 1000000: 80, 10000000: 100},
            )
            components.append({
                "component_id": "solana_unique_holders",
                "category": "holder_distribution",
                "raw_value": holder_count,
                "normalized_score": round(holders_score, 2),
                "data_source": "helius",
                "metadata": {
                    "chain": "solana",
                    "source": "static_estimate",
                    "description": f"{holder_count:,} estimated unique Solana token accounts",
                },
            })
    except Exception as e:
        logger.error(f"Solana holders error for {stablecoin_id}: {e}")
    await asyncio.sleep(RATE_LIMIT_DELAY)

    # --- 3. Mint/burn flow detection + USD volumes + velocity + unusual minting ---
    try:
        transfers = await get_solana_token_transfers(client, mint, limit=100)
        if transfers:
            mint_count = 0
            burn_count = 0
            transfer_count = len(transfers)
            mint_volume_usd = 0.0
            burn_volume_usd = 0.0

            system_addrs = {None, "", "11111111111111111111111111111111"}
            for tx in transfers:
                for tt in tx.get("tokenTransfers", []):
                    amount = tt.get("tokenAmount", 0)
                    if not isinstance(amount, (int, float)):
                        amount = 0

                    if tt.get("fromUserAccount") in system_addrs:
                        mint_count += 1
                        mint_volume_usd += amount
                    if tt.get("toUserAccount") in system_addrs:
                        burn_count += 1
                        burn_volume_usd += amount

            # 3a. Mint/burn ratio
            total_flow = mint_count + burn_count
            ratio = mint_count / total_flow if total_flow > 0 else 0.5
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

            # 3b. USD-denominated mint volume
            components.append({
                "component_id": "solana_daily_mint_volume",
                "category": "flows",
                "raw_value": round(mint_volume_usd, 2),
                "normalized_score": None,  # informational
                "data_source": "helius",
                "metadata": {"chain": "solana", "note": "Approximate USD (stablecoin 1:1)"},
            })

            # 3c. USD-denominated burn volume
            components.append({
                "component_id": "solana_daily_burn_volume",
                "category": "flows",
                "raw_value": round(burn_volume_usd, 2),
                "normalized_score": None,  # informational
                "data_source": "helius",
                "metadata": {"chain": "solana", "note": "Approximate USD (stablecoin 1:1)"},
            })

            # 3d. Supply change velocity
            if supply_val and supply_val > 0:
                net_flow = mint_volume_usd - burn_volume_usd
                velocity_pct = abs(net_flow / supply_val) * 100

                if velocity_pct < 0.1:
                    vel_score = 95.0
                elif velocity_pct < 0.5:
                    vel_score = 80.0
                elif velocity_pct < 1.0:
                    vel_score = 65.0
                elif velocity_pct < 3.0:
                    vel_score = 40.0
                else:
                    vel_score = 20.0

                components.append({
                    "component_id": "solana_supply_change_velocity",
                    "category": "flows",
                    "raw_value": round(velocity_pct, 4),
                    "normalized_score": vel_score,
                    "data_source": "helius",
                    "metadata": {"chain": "solana", "net_flow_usd": round(net_flow, 2)},
                })

            # 3e. Unusual minting detection (z-score proxy)
            expected_mint_rate = 0.05  # baseline: 5% of txns are mints
            actual_mint_rate = mint_count / max(transfer_count, 1)
            deviation = abs(actual_mint_rate - expected_mint_rate) / max(expected_mint_rate, 0.01)

            if deviation < 1.0:
                unusual_score = 95.0
            elif deviation < 2.0:
                unusual_score = 75.0
            elif deviation < 3.0:
                unusual_score = 50.0
            else:
                unusual_score = 20.0

            components.append({
                "component_id": "solana_unusual_minting",
                "category": "flows",
                "raw_value": round(deviation, 2),
                "normalized_score": unusual_score,
                "data_source": "helius",
                "metadata": {
                    "chain": "solana",
                    "actual_mint_rate": round(actual_mint_rate, 4),
                    "expected_rate": expected_mint_rate,
                    "interpretation": "z-score proxy — deviation from expected mint rate in recent transactions",
                },
            })
    except Exception as e:
        logger.error(f"Solana transfers error for {stablecoin_id}: {e}")
    await asyncio.sleep(RATE_LIMIT_DELAY)

    # --- 4. Mint account authorities (freeze + mint) ---
    try:
        mint_info = await get_solana_mint_info(client, mint)
        if mint_info:
            has_freeze = mint_info.get("has_freeze", False)
            components.append({
                "component_id": "solana_freeze_authority",
                "category": "smart_contract",
                "raw_value": 1 if has_freeze else 0,
                "normalized_score": 40.0 if has_freeze else 90.0,
                "data_source": "helius",
                "metadata": {
                    "chain": "solana",
                    "freeze_authority": mint_info.get("freeze_authority"),
                    "interpretation": "Issuer can freeze individual token accounts" if has_freeze else "No freeze capability",
                },
            })

            has_mint = mint_info.get("has_mint_authority", False)
            components.append({
                "component_id": "solana_mint_authority",
                "category": "smart_contract",
                "raw_value": 1 if has_mint else 0,
                "normalized_score": 50.0 if has_mint else 85.0,
                "data_source": "helius",
                "metadata": {
                    "chain": "solana",
                    "mint_authority": mint_info.get("mint_authority"),
                    "interpretation": "Issuer can mint new tokens" if has_mint else "Mint authority revoked — fixed supply",
                },
            })

            # 4c. Admin key risk — composite of freeze + mint authority
            # Both present = more centralized (lower score), neither = decentralized
            if has_freeze and has_mint:
                admin_score = 40.0   # centralized issuer with full control
            elif has_mint:
                admin_score = 55.0   # can mint but can't freeze
            elif has_freeze:
                admin_score = 60.0   # can freeze but fixed supply
            else:
                admin_score = 90.0   # fully decentralized

            components.append({
                "component_id": "solana_admin_key_risk",
                "category": "smart_contract",
                "raw_value": admin_score,
                "normalized_score": admin_score,
                "data_source": "helius",
                "metadata": {
                    "chain": "solana",
                    "has_freeze": has_freeze,
                    "has_mint": has_mint,
                    "interpretation": "Composite admin risk from on-chain authority analysis",
                },
            })

            # 4d. Pausability — derived from freeze authority (Solana equivalent of EVM pause)
            pause_score = 60.0 if has_freeze else 100.0
            components.append({
                "component_id": "solana_pausability",
                "category": "smart_contract",
                "raw_value": 1 if has_freeze else 0,
                "normalized_score": pause_score,
                "data_source": "helius",
                "metadata": {
                    "chain": "solana",
                    "interpretation": "Freeze authority = can pause individual accounts" if has_freeze else "No freeze capability — not pausable",
                },
            })
    except Exception as e:
        logger.error(f"Solana mint authority error for {stablecoin_id}: {e}")

    # --- 5. Static smart contract components (mirrors EVM smart_contract.py) ---
    try:
        # 5a. Contract verified (program verification status)
        verified = SOLANA_VERIFIED_PROGRAMS.get(stablecoin_id, False)
        components.append({
            "component_id": "solana_contract_verified",
            "category": "smart_contract",
            "raw_value": 1 if verified else 0,
            "normalized_score": 100.0 if verified else 0.0,
            "data_source": "config",
            "metadata": {
                "chain": "solana",
                "interpretation": "SPL Token program — verified" if verified else "Verification status unknown",
            },
        })

        # 5b. Bug bounty score
        bounty_info = SOLANA_BUG_BOUNTY.get(stablecoin_id, {})
        if bounty_info.get("active"):
            if bounty_info.get("max_payout", 0) >= 100_000:
                bounty_score = 100.0
            else:
                bounty_score = 70.0
        else:
            bounty_score = 20.0

        components.append({
            "component_id": "solana_bug_bounty",
            "category": "smart_contract",
            "raw_value": bounty_info.get("max_payout", 0),
            "normalized_score": bounty_score,
            "data_source": "config",
            "metadata": {
                "chain": "solana",
                "active": bounty_info.get("active", False),
                "platform": bounty_info.get("platform"),
                "note": bounty_info.get("note"),
            },
        })

        # 5c. Exploit history
        exploits = SOLANA_EXPLOIT_HISTORY.get(stablecoin_id, [])
        if not exploits:
            exploit_score = 100.0
        else:
            from datetime import datetime, timezone
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
                exploit_score = 100.0
            else:
                days_ago = (now - most_recent).days
                if days_ago < 90:
                    exploit_score = 0.0
                elif days_ago < 365:
                    exploit_score = 20.0
                else:
                    exploit_score = 60.0

        components.append({
            "component_id": "solana_exploit_history",
            "category": "smart_contract",
            "raw_value": len(exploits),
            "normalized_score": exploit_score,
            "data_source": "config",
            "metadata": {
                "chain": "solana",
                "exploit_count": len(exploits),
                "interpretation": "No known Solana-specific exploits" if not exploits else f"{len(exploits)} incident(s)",
            },
        })
    except Exception as e:
        logger.error(f"Solana static smart contract error for {stablecoin_id}: {e}")

    if components:
        logger.info(f"Solana collector: {len(components)} components for {stablecoin_id}")

    return components


# =============================================================================
# Drift vault balance reader
# =============================================================================

async def get_drift_vault_balances(client: httpx.AsyncClient) -> list[dict]:
    """
    Read token balances of Drift's program-owned token accounts.
    Uses getTokenAccountsByOwner to find all SPL token accounts owned by Drift.
    Returns list of {mint, symbol, balance, is_stablecoin, usd_value_approx}.
    """
    if not HELIUS_API_KEY:
        return []

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccountsByOwner",
        "params": [
            DRIFT_PROGRAM_ID,
            {"programId": TOKEN_PROGRAM_ID},
            {"encoding": "jsonParsed"},
        ],
    }

    try:
        resp = await client.post(HELIUS_RPC_URL, json=payload, timeout=30)
        data = resp.json()
        accounts = data.get("result", {}).get("value", [])

        # Reverse lookup: mint address -> symbol
        mint_to_symbol = {v: k.upper() for k, v in SOLANA_STABLECOIN_MINTS.items()}

        balances = []
        for acct in accounts:
            parsed = acct.get("account", {}).get("data", {}).get("parsed", {})
            info = parsed.get("info", {})
            acct_mint = info.get("mint", "")
            token_amount = info.get("tokenAmount", {})
            ui_amount = token_amount.get("uiAmount")

            if ui_amount and ui_amount > 0:
                symbol = mint_to_symbol.get(acct_mint, acct_mint[:8] + "...")
                is_stable = acct_mint in mint_to_symbol
                balances.append({
                    "mint": acct_mint,
                    "symbol": symbol,
                    "balance": ui_amount,
                    "is_stablecoin": is_stable,
                    "usd_value_approx": ui_amount if is_stable else None,
                })

        balances.sort(key=lambda x: x["balance"], reverse=True)
        return balances

    except Exception as e:
        logger.error(f"Drift vault balance error: {e}")
        return []


# =============================================================================
# POST-RAISE: Solana Wallet Graph
# =============================================================================
# The EVM wallet graph builds transfer edges from Etherscan/Blockscout data.
# Solana equivalent requires:
#   - Helius Wallet API: GET /v1/wallet/{address}/transfers for transfer history
#   - Helius parsed transactions: token transfers with sender/receiver
#   - Edge builder adapter: Solana txn model (instructions) differs from EVM (tx)
#   - Cross-chain merging: wallet_profiles table already supports multi-chain
#     addresses — a Solana wallet can be linked to an EVM wallet for the same entity
#   - Estimated cost: Helius Developer plan ($49/mo, 10M credits) sufficient for
#     initial wallet graph with ~10K wallets
#   - Estimated effort: 1-2 Claude Code sessions for the adapter, plus ongoing
#     indexing costs
#
# Priority: After seed close. The wallet graph is the biggest moat —
# every day of edge accumulation is unreplicable.
# =============================================================================
