"""
Contract Upgrade Delta Tracker (Pipeline 3)
=============================================
Every time a scored contract's bytecode changes, capture and store the
before/after as permanent attested state.  Historical vulnerability deltas
that cannot be reconstructed after the fact.

Runs daily in the slow cycle.  Never raises — all errors logged and skipped.
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from app.database import fetch_all, fetch_one, execute

logger = logging.getLogger(__name__)

# EIP-1967 implementation storage slot
EIP1967_IMPLEMENTATION_SLOT = (
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
)


# ---------------------------------------------------------------------------
# RPC helpers (reuse pattern from smart_contract.py)
# ---------------------------------------------------------------------------

def _get_rpc_url(chain: str = "ethereum") -> str:
    alchemy_key = os.environ.get("ALCHEMY_API_KEY", "")
    if not alchemy_key:
        return ""
    chain_map = {
        "ethereum": "eth-mainnet",
        "arbitrum": "arb-mainnet",
        "optimism": "opt-mainnet",
        "base": "base-mainnet",
        "polygon": "polygon-mainnet",
    }
    network = chain_map.get(chain, "eth-mainnet")
    return f"https://{network}.g.alchemy.com/v2/{alchemy_key}"


def _get_etherscan_bytecode(address: str) -> str | None:
    """Fallback: fetch bytecode via Etherscan for Ethereum mainnet."""
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        return None
    try:
        resp = httpx.get(
            "https://api.etherscan.io/v2/api",
            params={
                "module": "proxy",
                "action": "eth_getCode",
                "address": address,
                "tag": "latest",
                "apikey": api_key,
            },
            timeout=15,
        )
        data = resp.json()
        result = data.get("result", "0x")
        if result and result != "0x":
            return result
    except Exception as e:
        logger.debug(f"Etherscan bytecode fetch failed for {address}: {e}")
    return None


def _rpc_get_code(rpc_url: str, address: str) -> str | None:
    """Fetch bytecode via eth_getCode RPC call."""
    if not rpc_url:
        return None
    try:
        resp = httpx.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getCode",
                "params": [address, "latest"],
            },
            timeout=15,
        )
        data = resp.json()
        result = data.get("result", "0x")
        if result and result != "0x":
            return result
    except Exception as e:
        logger.debug(f"RPC eth_getCode failed for {address}: {e}")
    return None


def _rpc_get_storage_at(rpc_url: str, address: str, slot: str) -> str:
    """Read a storage slot via RPC."""
    if not rpc_url:
        return "0x" + "00" * 32
    try:
        resp = httpx.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getStorageAt",
                "params": [address, slot, "latest"],
            },
            timeout=15,
        )
        data = resp.json()
        return data.get("result", "0x" + "00" * 32)
    except Exception as e:
        logger.debug(f"RPC eth_getStorageAt failed for {address}: {e}")
        return "0x" + "00" * 32


def _hash_bytecode(bytecode: str) -> str:
    """SHA-256 hash of bytecode, returned as 0x-prefixed hex."""
    return "0x" + hashlib.sha256(bytecode.encode()).hexdigest()


def _resolve_implementation(rpc_url: str, proxy_address: str) -> str | None:
    """Resolve EIP-1967 implementation address from storage slot."""
    raw = _rpc_get_storage_at(rpc_url, proxy_address, EIP1967_IMPLEMENTATION_SLOT)
    if raw and raw != "0x" + "00" * 32 and len(raw) >= 42:
        addr = "0x" + raw[-40:]
        if addr != "0x" + "0" * 40:
            return addr
    return None


# ---------------------------------------------------------------------------
# Build contract list from scored entities
# ---------------------------------------------------------------------------

def _load_contract_registry() -> dict:
    registry_path = Path(__file__).parent.parent / "config" / "contract_registry.json"
    try:
        with open(registry_path) as f:
            return json.load(f)
    except Exception as e:
        logger.debug(f"Could not load contract registry: {e}")
        return {"protocols": {}, "bridges": {}}


def _build_contract_targets() -> list[dict]:
    """
    Build list of contract targets from:
      1. stablecoins table (token contracts)
      2. contract_registry.json (protocol + bridge contracts)
    Returns list of {entity_type, entity_id, entity_symbol, contract_address, chain}.
    """
    targets = []

    # Stablecoin token contracts from DB
    try:
        rows = fetch_all(
            "SELECT id, symbol, contract FROM stablecoins WHERE contract IS NOT NULL AND contract != ''"
        )
        for row in rows or []:
            addr = row.get("contract", "")
            if addr and len(addr) >= 40:
                targets.append({
                    "entity_type": "stablecoin",
                    "entity_id": row["id"],
                    "entity_symbol": row.get("symbol", ""),
                    "contract_address": addr.lower(),
                    "chain": "ethereum",
                })
    except Exception as e:
        logger.warning(f"Failed to load stablecoin contracts: {e}")

    # Protocol + bridge contracts from registry
    registry = _load_contract_registry()

    # Protocol contracts
    for slug, cfg in registry.get("protocols", {}).items():
        # Look up entity_id from DB
        proto_row = fetch_one(
            "SELECT id FROM psi_scores WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1",
            (slug,),
        )
        entity_id = proto_row["id"] if proto_row else 0

        for contract_key in ("governance_timelock", "multisig", "core_contract"):
            contract_cfg = cfg.get(contract_key)
            if contract_cfg and contract_cfg.get("address"):
                targets.append({
                    "entity_type": "protocol",
                    "entity_id": entity_id,
                    "entity_symbol": slug,
                    "contract_address": contract_cfg["address"].lower(),
                    "chain": contract_cfg.get("chain", "ethereum"),
                })

    # Bridge contracts
    for slug, cfg in registry.get("bridges", {}).items():
        for contract_key in ("guardian_contract", "timelock"):
            contract_cfg = cfg.get(contract_key)
            if contract_cfg and contract_cfg.get("address"):
                targets.append({
                    "entity_type": "bridge",
                    "entity_id": 0,
                    "entity_symbol": slug,
                    "contract_address": contract_cfg["address"].lower(),
                    "chain": contract_cfg.get("chain", "ethereum"),
                })

    return targets


# ---------------------------------------------------------------------------
# Main collector
# ---------------------------------------------------------------------------

def _detect_change(last_snapshot: dict, current_hash: str, impl_address: str | None, impl_bytecode_hash: str | None) -> bool:
    """
    Determine whether a contract has changed since last snapshot.

    For proxy contracts (USDC, etc.): the proxy bytecode itself is a thin
    delegatecall stub that NEVER changes on upgrade.  What changes is the
    implementation address and/or the implementation bytecode.  So we must
    compare implementation_address and impl bytecode hash, not just the
    proxy bytecode hash.

    For non-proxy contracts: compare the direct bytecode hash.
    """
    prev_hash = last_snapshot.get("bytecode_hash")
    prev_impl = last_snapshot.get("implementation_address")

    # Direct bytecode change (covers non-proxy contracts, or rare proxy stub changes)
    if prev_hash != current_hash:
        return True

    # Proxy-specific: implementation address changed (the core upgrade signal)
    if impl_address and prev_impl and impl_address.lower() != prev_impl.lower():
        return True

    # Proxy-specific: previously not a proxy, now resolves to implementation
    if impl_address and not prev_impl:
        return True

    return False


def _record_upgrade(
    target: dict,
    address: str,
    chain: str,
    last_snapshot: dict,
    current_hash: str,
    impl_address: str | None,
    impl_bytecode_hash: str | None,
    is_proxy: bool,
) -> None:
    """Insert new snapshot + upgrade record + attestation."""
    now = datetime.now(timezone.utc)

    # For proxy contracts, the "meaningful" hash is the implementation bytecode.
    # Store impl hash as current_bytecode_hash so the upgrade record captures
    # the actual code change, not the unchanging proxy stub.
    effective_hash = impl_bytecode_hash if (is_proxy and impl_bytecode_hash) else current_hash
    prev_effective = last_snapshot.get("bytecode_hash")

    content_data = (
        f"{target['entity_id']}{address}{chain}"
        f"{effective_hash}{now.isoformat()}"
    )
    content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

    # Insert new snapshot (keyed on proxy bytecode hash — the on-chain identity)
    execute(
        """INSERT INTO contract_bytecode_snapshots
            (contract_address, chain, bytecode_hash, implementation_address,
             is_proxy, is_verified, captured_at)
           VALUES (%s, %s, %s, %s, %s, FALSE, NOW())
           ON CONFLICT (contract_address, chain, bytecode_hash) DO UPDATE
               SET implementation_address = EXCLUDED.implementation_address,
                   captured_at = NOW()""",
        (address, chain, current_hash, impl_address, is_proxy),
    )

    # Insert upgrade record
    execute(
        """INSERT INTO contract_upgrade_history
            (entity_type, entity_id, entity_symbol, contract_address, chain,
             previous_bytecode_hash, current_bytecode_hash,
             previous_implementation, current_implementation,
             slither_queued, content_hash, attested_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, NOW())""",
        (
            target["entity_type"],
            target["entity_id"],
            target["entity_symbol"],
            address,
            chain,
            prev_effective,
            effective_hash,
            last_snapshot.get("implementation_address"),
            impl_address,
            content_hash,
        ),
    )

    # Attest
    try:
        from app.state_attestation import attest_state
        attest_state("contract_upgrades", [{
            "entity_id": target["entity_id"],
            "contract_address": address,
            "chain": chain,
            "current_bytecode_hash": effective_hash,
            "previous_implementation": last_snapshot.get("implementation_address"),
            "current_implementation": impl_address,
            "upgrade_detected_at": now.isoformat(),
        }], str(target["entity_id"]))
    except Exception as ae:
        logger.debug(f"Contract upgrade attestation failed: {ae}")

    logger.warning(
        f"CONTRACT UPGRADE DETECTED: {target['entity_symbol']} "
        f"on {chain} ({address})"
        + (f" impl {last_snapshot.get('implementation_address')} -> {impl_address}" if is_proxy else "")
    )


def collect_contract_upgrades(entity_filter: str | None = None) -> dict:
    """
    Scan all scored entity contracts for bytecode changes.

    Args:
        entity_filter: If set, only process targets whose entity_symbol
                       matches (case-insensitive).  Useful for testing
                       against a single entity like 'usdc'.

    Returns summary: {entities_checked, upgrades_detected, first_captures, errors}.
    """
    targets = _build_contract_targets()
    if entity_filter:
        targets = [t for t in targets if t["entity_symbol"].lower() == entity_filter.lower()]
    if not targets:
        logger.info("Contract upgrade tracker: no targets found")
        return {"entities_checked": 0, "upgrades_detected": 0, "first_captures": 0}

    entities_checked = 0
    upgrades_detected = 0
    first_captures = 0
    errors = 0

    for target in targets:
        try:
            address = target["contract_address"]
            chain = target["chain"]
            rpc_url = _get_rpc_url(chain)

            # Fetch current bytecode
            bytecode = _rpc_get_code(rpc_url, address)
            if not bytecode and chain == "ethereum":
                bytecode = _get_etherscan_bytecode(address)

            # Look up most recent snapshot (need this even if bytecode is empty
            # to detect self-destruction of previously-live contracts)
            last_snapshot = fetch_one(
                """SELECT bytecode_hash, implementation_address
                   FROM contract_bytecode_snapshots
                   WHERE contract_address = %s AND chain = %s
                   ORDER BY captured_at DESC LIMIT 1""",
                (address, chain),
            )

            # No bytecode returned — either EOA, self-destructed, or RPC failure
            if not bytecode:
                if last_snapshot:
                    # Previously had bytecode, now gone — flag as destruction
                    logger.warning(
                        f"CONTRACT BYTECODE GONE: {target['entity_symbol']} "
                        f"on {chain} ({address}) — previously had bytecode, "
                        f"now eth_getCode returns 0x (self-destructed or RPC error)"
                    )
                else:
                    logger.debug(f"No bytecode for {address} on {chain}, skipping")
                entities_checked += 1
                continue

            current_hash = _hash_bytecode(bytecode)
            entities_checked += 1

            # Check for proxy and resolve implementation
            impl_address = _resolve_implementation(rpc_url, address)
            is_proxy = impl_address is not None
            impl_bytecode_hash = None
            if impl_address:
                impl_bytecode = _rpc_get_code(rpc_url, impl_address)
                if impl_bytecode:
                    impl_bytecode_hash = _hash_bytecode(impl_bytecode)

            if not last_snapshot:
                # First capture — insert snapshot, no upgrade record
                execute(
                    """INSERT INTO contract_bytecode_snapshots
                        (contract_address, chain, bytecode_hash, implementation_address,
                         is_proxy, is_verified, captured_at)
                       VALUES (%s, %s, %s, %s, %s, FALSE, NOW())
                       ON CONFLICT (contract_address, chain, bytecode_hash) DO NOTHING""",
                    (address, chain, current_hash, impl_address, is_proxy),
                )
                first_captures += 1
                if is_proxy:
                    logger.info(
                        f"First capture (proxy): {target['entity_symbol']} on {chain} "
                        f"impl={impl_address}"
                    )

            elif _detect_change(last_snapshot, current_hash, impl_address, impl_bytecode_hash):
                # Upgrade detected
                _record_upgrade(
                    target, address, chain, last_snapshot,
                    current_hash, impl_address, impl_bytecode_hash, is_proxy,
                )
                upgrades_detected += 1

            else:
                # No change — update captured_at and current impl on latest snapshot
                execute(
                    """UPDATE contract_bytecode_snapshots
                       SET captured_at = NOW(), implementation_address = %s
                       WHERE contract_address = %s AND chain = %s AND bytecode_hash = %s""",
                    (impl_address, address, chain, current_hash),
                )

            # Rate limit: small sleep between RPC calls
            time.sleep(0.3)

        except Exception as e:
            logger.debug(f"Contract upgrade check failed for {target.get('contract_address')}: {e}")
            errors += 1

    summary = {
        "entities_checked": entities_checked,
        "upgrades_detected": upgrades_detected,
        "first_captures": first_captures,
        "errors": errors,
    }
    logger.info(
        f"Contract upgrade tracker: checked={entities_checked} "
        f"upgrades={upgrades_detected} first_captures={first_captures} errors={errors}"
    )
    return summary
