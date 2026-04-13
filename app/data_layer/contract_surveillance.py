"""
Continuous Contract Surveillance
=================================
Etherscan contract source code for every scored entity across all chains.
Parse for admin keys, upgradeability, pause functions, blacklist, timelock,
multisig. Store analysis. Re-scan weekly. Diff results. Emit discovery
signals on contract changes.

Sources:
- Etherscan V2: getsourcecode endpoint

Schedule: Weekly (diff against previous scan)
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"

CHAIN_IDS = {
    "ethereum": 1,
    "base": 8453,
    "arbitrum": 42161,
}

# Patterns to detect in source code
SECURITY_PATTERNS = {
    "has_pause_function": [
        r"function\s+pause\s*\(",
        r"function\s+_pause\s*\(",
        r"whenNotPaused",
        r"Pausable",
    ],
    "has_blacklist": [
        r"function\s+blacklist\s*\(",
        r"function\s+addBlacklist\s*\(",
        r"function\s+freeze\s*\(",
        r"isBlacklisted",
        r"_blacklisted",
    ],
    "is_upgradeable": [
        r"upgradeTo\s*\(",
        r"upgradeToAndCall\s*\(",
        r"TransparentUpgradeableProxy",
        r"UUPSUpgradeable",
        r"ERC1967Proxy",
    ],
    "has_admin_keys": [
        r"function\s+setAdmin\s*\(",
        r"function\s+transferOwnership\s*\(",
        r"onlyOwner",
        r"onlyAdmin",
        r"DEFAULT_ADMIN_ROLE",
    ],
    "has_timelock": [
        r"TimelockController",
        r"timelock",
        r"delay\s*>=",
        r"minimumDelay",
    ],
    "has_multisig": [
        r"GnosisSafe",
        r"Safe\s*\{",
        r"threshold",
        r"getOwners",
        r"execTransaction",
    ],
    "has_oracle": [
        r"AggregatorV3Interface",
        r"latestRoundData",
        r"priceFeed",
        r"getPrice",
    ],
    "has_mint_authority": [
        r"function\s+mint\s*\(",
        r"_mint\s*\(",
        r"onlyMinter",
        r"MINTER_ROLE",
    ],
}


async def _fetch_source_code(
    client: httpx.AsyncClient,
    contract: str,
    chain: str = "ethereum",
) -> dict:
    """Fetch verified source code from Etherscan V2."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("etherscan")

    chain_id = CHAIN_IDS.get(chain, 1)
    params = {
        "chainid": chain_id,
        "module": "contract",
        "action": "getsourcecode",
        "address": contract,
        "apikey": ETHERSCAN_API_KEY,
    }

    start = time.time()
    try:
        resp = await client.get(ETHERSCAN_V2_BASE, params=params, timeout=15)
        latency = int((time.time() - start) * 1000)
        track_api_call("etherscan", "/getsourcecode", caller="contract_surveillance",
                       status=resp.status_code, latency_ms=latency)

        if resp.status_code == 429 or "Max rate limit" in resp.text:
            rate_limiter.report_429("etherscan")
            return {}

        resp.raise_for_status()
        rate_limiter.report_success("etherscan")
        data = resp.json()

        if data.get("status") == "1" and data.get("result"):
            return data["result"][0] if isinstance(data["result"], list) else data["result"]
        return {}
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("etherscan", "/getsourcecode", caller="contract_surveillance",
                       status=500, latency_ms=latency)
        logger.warning(f"Source code fetch failed for {contract} on {chain}: {e}")
        return {}


def _analyze_source(source_code: str) -> dict:
    """Analyze source code for security-relevant patterns."""
    import re

    analysis = {}
    for pattern_name, regexes in SECURITY_PATTERNS.items():
        found = False
        for regex in regexes:
            if re.search(regex, source_code, re.IGNORECASE):
                found = True
                break
        analysis[pattern_name] = found

    return analysis


def _hash_source(source_code: str) -> str:
    """SHA256 hash of source code for change detection."""
    return hashlib.sha256(source_code.encode("utf-8")).hexdigest()


def _sanitize_float(val):
    """Return None if val is NaN or Infinity, else return val."""
    import math
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _store_surveillance_result(result: dict):
    """Store contract surveillance result (per-row transaction)."""
    from app.database import get_cursor

    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO contract_surveillance
                   (entity_id, chain, contract_address,
                    has_admin_keys, is_upgradeable, has_pause_function,
                    has_blacklist, timelock_hours, multisig_threshold,
                    source_code_hash, analysis, scanned_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                   ON CONFLICT (entity_id, chain, contract_address, scanned_at)
                   DO UPDATE SET
                       source_code_hash = EXCLUDED.source_code_hash,
                       analysis = EXCLUDED.analysis""",
                (
                    result["entity_id"], result["chain"], result["contract_address"],
                    result.get("has_admin_keys"), result.get("is_upgradeable"),
                    result.get("has_pause_function"), result.get("has_blacklist"),
                    _sanitize_float(result.get("timelock_hours")),
                    _sanitize_float(result.get("multisig_threshold")),
                    result.get("source_code_hash"),
                    json.dumps(result.get("analysis")) if result.get("analysis") else None,
                ),
            )
    except Exception as e:
        logger.error(
            "Failed to store surveillance result for %s on %s: %s",
            result.get("entity_id"), result.get("chain"), e,
        )


def _detect_changes(entity_id: str, chain: str, contract: str, new_hash: str) -> bool:
    """Check if source code has changed since last scan."""
    from app.database import fetch_one

    prev = fetch_one(
        """SELECT source_code_hash FROM contract_surveillance
           WHERE entity_id = %s AND chain = %s AND contract_address = %s
           ORDER BY scanned_at DESC LIMIT 1""",
        (entity_id, chain, contract),
    )

    if prev and prev.get("source_code_hash"):
        return prev["source_code_hash"] != new_hash
    return False  # First scan, no change to report


async def run_contract_surveillance() -> dict:
    """
    Full contract surveillance cycle.
    Scan all scored entity contracts, analyze for security patterns,
    detect changes from previous scan.

    Returns summary + any change signals.
    """
    from app.database import fetch_all

    # Get contracts to scan — stablecoins (all chains)
    from app.data_layer.liquidity_collector import STABLECOIN_CONTRACTS_BY_CHAIN

    contracts_to_scan = []

    # Stablecoin contracts on all chains
    stablecoins = fetch_all(
        """SELECT id, symbol, contract FROM stablecoins
           WHERE scoring_enabled = TRUE AND contract IS NOT NULL"""
    )
    if stablecoins:
        for sc in stablecoins:
            symbol = sc.get("symbol", "").upper()
            # Ethereum main contract
            contract = sc.get("contract", "")
            if contract and contract.startswith("0x"):
                contracts_to_scan.append({
                    "entity_id": sc["id"],
                    "chain": "ethereum",
                    "contract_address": contract,
                })
            # Multi-chain contracts
            for chain in ["base", "arbitrum"]:
                chain_addr = STABLECOIN_CONTRACTS_BY_CHAIN.get(chain, {}).get(symbol)
                if chain_addr:
                    contracts_to_scan.append({
                        "entity_id": sc["id"],
                        "chain": chain,
                        "contract_address": chain_addr,
                    })

    # Protocol core contracts from contract registry
    try:
        import json as _json
        import os
        registry_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "config", "contract_registry.json"
        )
        if os.path.exists(registry_path):
            with open(registry_path) as f:
                registry = _json.load(f)
            for slug, contracts in registry.get("protocols", {}).items():
                core = contracts.get("core_contract")
                if core and core.get("address"):
                    contracts_to_scan.append({
                        "entity_id": slug,
                        "chain": core.get("chain", "ethereum"),
                        "contract_address": core["address"],
                    })
                timelock = contracts.get("governance_timelock")
                if timelock and timelock.get("address"):
                    contracts_to_scan.append({
                        "entity_id": f"{slug}_timelock",
                        "chain": timelock.get("chain", "ethereum"),
                        "contract_address": timelock["address"],
                    })
    except Exception as e:
        logger.debug(f"Contract registry load failed: {e}")

    if not contracts_to_scan:
        return {"error": "no contracts to scan"}

    total_scanned = 0
    changes_detected = []

    async with httpx.AsyncClient(timeout=30) as client:
        for entry in contracts_to_scan:
            try:
                source_data = await _fetch_source_code(
                    client, entry["contract_address"], entry["chain"]
                )

                if not source_data:
                    continue

                source_code = source_data.get("SourceCode", "")
                if not source_code:
                    continue

                # Analyze
                analysis = _analyze_source(source_code)
                source_hash = _hash_source(source_code)

                # Check for changes
                changed = _detect_changes(
                    entry["entity_id"], entry["chain"],
                    entry["contract_address"], source_hash
                )

                if changed:
                    changes_detected.append({
                        "entity_id": entry["entity_id"],
                        "chain": entry["chain"],
                        "contract": entry["contract_address"],
                    })

                # Store result
                result = {
                    "entity_id": entry["entity_id"],
                    "chain": entry["chain"],
                    "contract_address": entry["contract_address"],
                    "has_admin_keys": analysis.get("has_admin_keys", False),
                    "is_upgradeable": analysis.get("is_upgradeable", False),
                    "has_pause_function": analysis.get("has_pause_function", False),
                    "has_blacklist": analysis.get("has_blacklist", False),
                    "timelock_hours": None,
                    "multisig_threshold": None,
                    "source_code_hash": source_hash,
                    "analysis": {
                        **analysis,
                        "compiler_version": source_data.get("CompilerVersion"),
                        "contract_name": source_data.get("ContractName"),
                        "optimization_used": source_data.get("OptimizationUsed"),
                        "runs": source_data.get("Runs"),
                        "license": source_data.get("LicenseType"),
                    },
                }
                _store_surveillance_result(result)
                total_scanned += 1

            except Exception as e:
                logger.warning(
                    f"Contract surveillance failed for {entry['entity_id']}: {e}"
                )

    # Emit discovery signals for changes
    if changes_detected:
        try:
            from app.database import execute as db_execute
            for change in changes_detected:
                db_execute(
                    """INSERT INTO discovery_signals
                       (signal_type, domain, entity_id, severity, title, details, created_at)
                       VALUES ('contract_change', 'sii', %s, 'alert', %s, %s, NOW())""",
                    (
                        change["entity_id"],
                        f"Contract source code changed: {change['entity_id']}",
                        json.dumps(change),
                    ),
                )
        except Exception as e:
            logger.debug(f"Contract change signal failed: {e}")

    # Provenance
    try:
        from app.data_layer.provenance_scaling import attest_data_batch, link_batch_to_proof
        if total_scanned > 0:
            attest_data_batch("contract_surveillance", [{"scanned": total_scanned, "changes": len(changes_detected)}])
            link_batch_to_proof("contract_surveillance", "contract_surveillance")
    except Exception as e:
        logger.debug(f"Contract surveillance provenance failed: {e}")

    logger.info(
        f"Contract surveillance complete: {total_scanned} contracts scanned, "
        f"{len(changes_detected)} changes detected"
    )

    return {
        "contracts_scanned": total_scanned,
        "changes_detected": len(changes_detected),
        "changes": changes_detected,
    }
