"""
Solana Program Monitoring
===========================
Monitor Drift, Jupiter, Raydium program accounts for upgrade events.

Uses getAccountInfo on each program address to read:
- programdata account → upgrade_authority, last_deploy_slot
- If last_deploy_slot changes between cycles → emit contract upgrade event
- If upgrade_authority is null → mark as immutable, skip future checks

Stores snapshots in contract_surveillance with entity_type context.
"""

import json
import logging
import os
import struct
from datetime import datetime, timezone

import httpx

from app.database import execute, fetch_one, get_cursor
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
SOLANA_RPC = (
    f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    if HELIUS_API_KEY
    else "https://api.mainnet-beta.solana.com"
)

# Solana program addresses (mainnet)
SOLANA_PROGRAMS = {
    "drift": {
        "program_id": "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH",
        "name": "Drift Protocol",
    },
    "jupiter-perpetual-exchange": {
        "program_id": "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
        "name": "Jupiter Exchange",
    },
    "raydium": {
        "program_id": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        "name": "Raydium AMM",
    },
}

# BPF Upgradeable Loader program ID
BPF_LOADER_UPGRADEABLE = "BPFLoaderUpgradeab1e11111111111111111111111"

# ProgramData account header layout:
# 4 bytes: account type (3 = ProgramData)
# 8 bytes: slot last deployed
# 1 byte: has upgrade authority (0 = none, 1 = some)
# 32 bytes: upgrade authority pubkey (if present)
PROGRAMDATA_HEADER_SIZE = 45


async def _rpc_call(client: httpx.AsyncClient, method: str, params: list) -> dict:
    """Make a Solana JSON-RPC call."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }
    resp = await client.post(SOLANA_RPC, json=payload, timeout=15)
    return resp.json()


async def _get_program_info(client: httpx.AsyncClient, program_id: str) -> dict | None:
    """
    Get program account info and resolve programdata account.
    Returns: {programdata_address, upgrade_authority, last_deploy_slot, is_immutable}
    """
    # Get program account
    result = await _rpc_call(client, "getAccountInfo", [
        program_id,
        {"encoding": "base64"},
    ])

    account = result.get("result", {}).get("value")
    if not account:
        return None

    owner = account.get("owner", "")
    if owner != BPF_LOADER_UPGRADEABLE:
        # Not an upgradeable program
        return {
            "program_id": program_id,
            "is_immutable": True,
            "upgrade_authority": None,
            "last_deploy_slot": None,
            "owner": owner,
        }

    # Parse program account data to get programdata address
    import base64
    data = base64.b64decode(account["data"][0])
    if len(data) < 36:
        return None

    # Program account: 4 bytes type + 32 bytes programdata address
    programdata_address_bytes = data[4:36]
    programdata_address = _encode_base58(programdata_address_bytes)

    # Get programdata account
    pd_result = await _rpc_call(client, "getAccountInfo", [
        programdata_address,
        {"encoding": "base64"},
    ])

    pd_account = pd_result.get("result", {}).get("value")
    if not pd_account:
        return None

    pd_data = base64.b64decode(pd_account["data"][0])
    if len(pd_data) < PROGRAMDATA_HEADER_SIZE:
        return None

    # Parse programdata header
    # Bytes 0-3: account type (should be 3)
    # Bytes 4-11: last deploy slot (u64 LE)
    # Byte 12: has_authority (0 or 1)
    # Bytes 13-44: authority pubkey (32 bytes, if has_authority == 1)
    last_deploy_slot = struct.unpack_from("<Q", pd_data, 4)[0]
    has_authority = pd_data[12]

    upgrade_authority = None
    if has_authority == 1 and len(pd_data) >= 45:
        authority_bytes = pd_data[13:45]
        upgrade_authority = _encode_base58(authority_bytes)

    return {
        "program_id": program_id,
        "programdata_address": programdata_address,
        "upgrade_authority": upgrade_authority,
        "last_deploy_slot": last_deploy_slot,
        "is_immutable": has_authority == 0,
    }


def _encode_base58(data: bytes) -> str:
    """Encode bytes to base58 (Solana address encoding)."""
    ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    n = int.from_bytes(data, "big")
    result = bytearray()
    while n > 0:
        n, r = divmod(n, 58)
        result.append(ALPHABET[r])
    # Count leading zeros
    for byte in data:
        if byte == 0:
            result.append(ALPHABET[0])
        else:
            break
    return bytes(reversed(result)).decode("ascii")


def _store_program_snapshot(slug: str, info: dict):
    """Store Solana program snapshot in contract_surveillance."""
    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO contract_surveillance
                   (entity_id, chain, contract_address,
                    has_admin_keys, is_upgradeable, has_pause_function,
                    has_blacklist, timelock_hours, multisig_threshold,
                    source_code_hash, analysis, scanned_at)
                   VALUES (%s, 'solana', %s, %s, %s, FALSE, FALSE, NULL, NULL, %s, %s, NOW())
                   ON CONFLICT (entity_id, chain, contract_address, scanned_at)
                   DO UPDATE SET
                       source_code_hash = EXCLUDED.source_code_hash,
                       analysis = EXCLUDED.analysis""",
                (
                    slug,
                    info["program_id"],
                    info.get("upgrade_authority") is not None,  # has_admin_keys
                    not info.get("is_immutable", False),  # is_upgradeable
                    str(info.get("last_deploy_slot", "")),  # use deploy slot as "hash"
                    json.dumps({
                        "program_type": "solana_program",
                        "upgrade_authority": info.get("upgrade_authority"),
                        "last_deploy_slot": info.get("last_deploy_slot"),
                        "is_immutable": info.get("is_immutable", False),
                        "programdata_address": info.get("programdata_address"),
                    }),
                ),
            )
    except Exception as e:
        logger.warning(f"Failed to store Solana program snapshot for {slug}: {e}")


def _check_for_upgrade(slug: str, info: dict) -> bool:
    """Check if last_deploy_slot changed since previous scan. Returns True if upgrade detected."""
    try:
        prev = fetch_one(
            """SELECT source_code_hash FROM contract_surveillance
               WHERE entity_id = %s AND chain = 'solana'
               ORDER BY scanned_at DESC LIMIT 1""",
            (slug,),
        )
        if not prev:
            return False  # First scan, no comparison
        prev_slot = prev.get("source_code_hash", "")
        curr_slot = str(info.get("last_deploy_slot", ""))
        if prev_slot and curr_slot and prev_slot != curr_slot:
            logger.warning(
                f"SOLANA PROGRAM UPGRADE DETECTED: {slug} "
                f"deploy slot changed {prev_slot} → {curr_slot}"
            )
            # Record in contract_upgrade_history
            try:
                execute(
                    """INSERT INTO contract_upgrade_history
                       (entity_type, entity_id, entity_symbol, contract_address, chain,
                        previous_bytecode_hash, current_bytecode_hash, upgrade_detected_at)
                       VALUES ('protocol', 0, %s, %s, 'solana', %s, %s, NOW())""",
                    (slug, info["program_id"], prev_slot, curr_slot),
                )
            except Exception:
                pass
            return True
    except Exception:
        pass
    return False


async def run_solana_program_monitoring() -> dict:
    """
    Monitor Solana program accounts for upgrade events.
    Returns summary dict.
    """
    results = {
        "programs_checked": 0,
        "upgrades_detected": 0,
        "immutable": 0,
        "errors": 0,
    }

    async with httpx.AsyncClient(timeout=20) as client:
        for slug, config in SOLANA_PROGRAMS.items():
            try:
                info = await _get_program_info(client, config["program_id"])
                if not info:
                    results["errors"] += 1
                    logger.warning(f"Failed to read Solana program: {slug}")
                    continue

                results["programs_checked"] += 1

                if info.get("is_immutable"):
                    results["immutable"] += 1
                    logger.debug(f"Solana program {slug} is immutable — skipping future checks")
                    _store_program_snapshot(slug, info)
                    continue

                # Check for upgrade
                if _check_for_upgrade(slug, info):
                    results["upgrades_detected"] += 1

                _store_program_snapshot(slug, info)

            except Exception as e:
                results["errors"] += 1
                logger.warning(f"Solana program monitoring failed for {slug}: {e}")

    logger.info(f"Solana program monitoring: {results}")
    return results
