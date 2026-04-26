"""
Contract Dependency Graph Collector (Pipeline 6)
===================================================
Maps which external contracts each scored protocol calls, versioned over time.
Pre-exploit dependency graph is the critical forensic record.

Uses Etherscan internal transactions + known patterns for dependency detection.
Runs daily in the slow cycle.  Never raises — all errors logged and skipped.
"""

import hashlib
import json
import logging
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path

import httpx

from app.database import fetch_all, fetch_one, execute
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

ETHERSCAN_API = "https://api.etherscan.io/v2/api"

# Known oracle/governance dependencies per protocol (supplementary)
KNOWN_DEPENDENCIES = {
    "aave": [
        {"address": "0x54586be62e3c3580375ae3723c145253060ca0c2", "label": "Aave Oracle", "type": "oracle"},
        {"address": "0x47fb2585d2c56fe188d0e6ec628a38b74fceeedf", "label": "Chainlink ETH/USD", "type": "oracle"},
        {"address": "0xec568fffba86c094cf06b22134b23074dfe2252c", "label": "Aave Governor", "type": "governance"},
    ],
    "compound-finance": [
        {"address": "0x6d2299c48a8dd07a872fdd0f8233924872ad1071", "label": "Compound Oracle", "type": "oracle"},
    ],
}


def _classify_by_label(label: str | None) -> str:
    """Classify a dependency by its Etherscan label."""
    if not label:
        return "unknown"
    lbl = label.lower()
    if any(k in lbl for k in ("oracle", "price", "feed", "chainlink", "pyth")):
        return "oracle"
    if any(k in lbl for k in ("token", "erc20", "usdc", "usdt", "dai")):
        return "token"
    if any(k in lbl for k in ("governor", "governance", "timelock")):
        return "governance"
    if any(k in lbl for k in ("proxy", "implementation", "impl")):
        return "proxy_impl"
    if any(k in lbl for k in ("library", "lib", "math", "safe")):
        return "library"
    return "unknown"


def _get_etherscan_key() -> str:
    return os.environ.get("ETHERSCAN_API_KEY", "")


def _fetch_internal_txs(contract_address: str) -> list[dict]:
    """Fetch recent internal transactions from Etherscan for dependency detection."""
    api_key = _get_etherscan_key()
    if not api_key:
        return []
    try:
        resp = httpx.get(
            ETHERSCAN_API,
            params={
                "module": "account",
                "action": "txlistinternal",
                "address": contract_address,
                "startblock": 0,
                "endblock": 99999999,
                "sort": "desc",
                "page": 1,
                "offset": 200,
                "apikey": api_key,
            },
            timeout=20,
        )
        if resp.status_code == 429:
            logger.warning("Etherscan rate limited, backing off 10s")
            time.sleep(10)
            resp = httpx.get(
                ETHERSCAN_API,
                params={
                    "module": "account",
                    "action": "txlistinternal",
                    "address": contract_address,
                    "startblock": 0,
                    "endblock": 99999999,
                    "sort": "desc",
                    "page": 1,
                    "offset": 200,
                    "apikey": api_key,
                },
                timeout=20,
            )
        if resp.status_code != 200:
            return []
        data = resp.json()
        if data.get("status") != "1":
            return []
        return data.get("result", [])
    except Exception as e:
        logger.debug(f"Etherscan internal txs failed for {contract_address}: {e}")
        return []


def _fetch_etherscan_label(address: str) -> str | None:
    """Try to get an Etherscan label for an address via the getaddressinfo proxy."""
    # Etherscan doesn't have a public label API, but we can check if
    # the address is a verified contract and use the contract name
    api_key = _get_etherscan_key()
    if not api_key:
        return None
    try:
        resp = httpx.get(
            ETHERSCAN_API,
            params={
                "module": "contract",
                "action": "getsourcecode",
                "address": address,
                "apikey": api_key,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        results = data.get("result", [])
        if results and isinstance(results, list) and results[0].get("ContractName"):
            return results[0]["ContractName"]
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Entity loading
# ---------------------------------------------------------------------------

def _load_contract_registry() -> dict:
    registry_path = Path(__file__).parent.parent / "config" / "contract_registry.json"
    try:
        with open(registry_path) as f:
            return json.load(f)
    except Exception:
        return {"protocols": {}, "bridges": {}}


def _load_scored_entities_with_contracts() -> list[dict]:
    """Load all scored entities with contract addresses."""
    entities = []

    # Stablecoins from DB
    try:
        rows = fetch_all(
            "SELECT id, symbol, contract FROM stablecoins WHERE contract IS NOT NULL AND contract != ''"
        )
        for row in rows or []:
            entities.append({
                "entity_type": "stablecoin",
                "entity_id": row["id"],
                "entity_slug": row["symbol"].lower(),
                "contracts": [{"address": row["contract"].lower(), "chain": "ethereum"}],
            })
    except Exception as e:
        logger.warning(f"Failed to load stablecoin contracts: {e}")

    # Protocols from contract_registry.json
    registry = _load_contract_registry()
    for slug, cfg in registry.get("protocols", {}).items():
        contracts = []
        for key in ("governance_timelock", "multisig", "core_contract"):
            contract_cfg = cfg.get(key)
            if contract_cfg and contract_cfg.get("address"):
                contracts.append({
                    "address": contract_cfg["address"].lower(),
                    "chain": contract_cfg.get("chain", "ethereum"),
                })
        if contracts:
            entities.append({
                "entity_type": "protocol",
                "entity_id": 0,
                "entity_slug": slug,
                "contracts": contracts,
            })

    # Bridges from contract_registry.json
    for slug, cfg in registry.get("bridges", {}).items():
        contracts = []
        for key in ("guardian_contract", "timelock"):
            contract_cfg = cfg.get(key)
            if contract_cfg and contract_cfg.get("address"):
                contracts.append({
                    "address": contract_cfg["address"].lower(),
                    "chain": contract_cfg.get("chain", "ethereum"),
                })
        if contracts:
            entities.append({
                "entity_type": "bridge",
                "entity_id": 0,
                "entity_slug": slug,
                "contracts": contracts,
            })

    return entities


# ---------------------------------------------------------------------------
# Dependency detection
# ---------------------------------------------------------------------------

def _detect_dependencies(entity: dict) -> list[dict]:
    """
    Detect contract dependencies via:
    1. Etherscan internal transactions (primary)
    2. Known patterns (supplementary)
    """
    dependencies = {}  # keyed by (address, chain) to deduplicate
    slug = entity["entity_slug"]

    for contract_info in entity["contracts"]:
        source_addr = contract_info["address"]
        chain = contract_info["chain"]

        if chain != "ethereum":
            continue  # Etherscan API only covers mainnet

        # Method 1: Internal transactions
        internal_txs = _fetch_internal_txs(source_addr)
        time.sleep(0.25)  # Rate limit: 4 req/s

        for tx in internal_txs:
            to_addr = (tx.get("to") or "").lower()
            if not to_addr or to_addr == source_addr:
                continue

            # Skip EOAs (internal txs to EOAs are transfers, not dependencies)
            # A rough heuristic: if contractAddress is empty, it's likely an EOA
            # But internal txs always have contract context, so just collect unique targets
            key = (to_addr, chain)
            if key not in dependencies:
                dependencies[key] = {
                    "depends_on_address": to_addr,
                    "depends_on_chain": chain,
                    "call_type": tx.get("type", "call").lower(),
                    "detected_via": "etherscan_internal_txs",
                    "depends_on_label": None,
                    "depends_on_type": "unknown",
                    "source_contract": source_addr,
                }

    # Method 2: Known patterns
    known = KNOWN_DEPENDENCIES.get(slug, [])
    for dep in known:
        addr = dep["address"].lower()
        key = (addr, "ethereum")
        if key not in dependencies:
            dependencies[key] = {
                "depends_on_address": addr,
                "depends_on_chain": "ethereum",
                "call_type": "call",
                "detected_via": "known_pattern",
                "depends_on_label": dep.get("label"),
                "depends_on_type": dep.get("type", "unknown"),
                "source_contract": entity["contracts"][0]["address"] if entity["contracts"] else "",
            }
        else:
            # Enrich existing detection with known label
            dependencies[key]["depends_on_label"] = dep.get("label")
            dependencies[key]["depends_on_type"] = dep.get("type", "unknown")

    # Enrich unlabeled dependencies with Etherscan contract names
    # (rate-limited, only for first 20 to stay under budget)
    unlabeled = [d for d in dependencies.values() if not d.get("depends_on_label")]
    for dep in unlabeled[:20]:
        try:
            label = _fetch_etherscan_label(dep["depends_on_address"])
            time.sleep(0.25)
            if label:
                dep["depends_on_label"] = label
                dep["depends_on_type"] = _classify_by_label(label)
        except Exception:
            pass

    return list(dependencies.values())


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def _reconcile_dependencies(entity: dict, current_deps: list[dict], results: dict):
    """Reconcile current dependencies against stored state."""
    entity_type = entity["entity_type"]
    entity_id = entity["entity_id"]

    # Load stored active dependencies
    stored = fetch_all(
        """SELECT depends_on_address, depends_on_chain
           FROM contract_dependencies
           WHERE entity_type = %s AND entity_id = %s AND removed_at IS NULL""",
        (entity_type, entity_id),
    ) or []

    stored_set = {(r["depends_on_address"], r["depends_on_chain"]) for r in stored}
    current_set = {(d["depends_on_address"], d["depends_on_chain"]) for d in current_deps}

    # New dependencies
    for dep in current_deps:
        key = (dep["depends_on_address"], dep["depends_on_chain"])
        content_data = f"{entity_id}{dep['depends_on_address']}{dep['depends_on_chain']}"
        content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

        execute(
            """INSERT INTO contract_dependencies
                (entity_type, entity_id, entity_slug, source_contract, source_chain,
                 depends_on_address, depends_on_chain, depends_on_label,
                 depends_on_type, call_type, detected_via,
                 content_hash, attested_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
               ON CONFLICT (source_contract, source_chain, depends_on_address, depends_on_chain)
               DO UPDATE SET last_confirmed_at = NOW(), removed_at = NULL,
                            depends_on_label = COALESCE(EXCLUDED.depends_on_label, contract_dependencies.depends_on_label),
                            depends_on_type = CASE WHEN EXCLUDED.depends_on_type != 'unknown'
                                              THEN EXCLUDED.depends_on_type
                                              ELSE contract_dependencies.depends_on_type END""",
            (
                entity_type, entity_id, entity["entity_slug"],
                dep["source_contract"], dep["depends_on_chain"],
                dep["depends_on_address"], dep["depends_on_chain"],
                dep.get("depends_on_label"), dep.get("depends_on_type", "unknown"),
                dep.get("call_type", "call"), dep.get("detected_via", "unknown"),
                content_hash,
            ),
        )

        if key not in stored_set:
            results["new_dependencies"] += 1
            try:
                from app.state_attestation import attest_state
                attest_state("contract_dependencies", [{
                    "entity_slug": entity["entity_slug"],
                    "depends_on": dep["depends_on_address"],
                    "chain": dep["depends_on_chain"],
                }], str(entity_id))
            except Exception:
                pass

        results["dependencies_found"] += 1

    # Removed dependencies
    removed = stored_set - current_set
    for addr, chain in removed:
        logger.warning(
            f"DEPENDENCY REMOVED: {entity['entity_slug']} no longer calls "
            f"{addr} on {chain}"
        )
        execute(
            """UPDATE contract_dependencies SET removed_at = NOW()
               WHERE entity_type = %s AND entity_id = %s
                 AND depends_on_address = %s AND depends_on_chain = %s""",
            (entity_type, entity_id, addr, chain),
        )
        results["removed_dependencies"] += 1


def _store_daily_snapshot(entity: dict, current_deps: list[dict], results: dict):
    """Store a point-in-time snapshot of the dependency graph."""
    today = date.today()
    entity_type = entity["entity_type"]
    entity_id = entity["entity_id"]

    # Check if today's snapshot exists
    existing = fetch_one(
        """SELECT id FROM dependency_graph_snapshots
           WHERE entity_type = %s AND entity_id = %s AND snapshot_date = %s""",
        (entity_type, entity_id, today),
    )
    if existing:
        return

    dep_addresses = sorted([d["depends_on_address"] for d in current_deps])

    # Build bytecode hash map from contract_bytecode_snapshots if available
    dep_hashes = {}
    for dep in current_deps:
        row = fetch_one(
            """SELECT bytecode_hash FROM contract_bytecode_snapshots
               WHERE contract_address = %s AND chain = %s
               ORDER BY captured_at DESC LIMIT 1""",
            (dep["depends_on_address"], dep["depends_on_chain"]),
        )
        if row and row.get("bytecode_hash"):
            dep_hashes[dep["depends_on_address"]] = row["bytecode_hash"]

    content_data = f"{entity_id}{today.isoformat()}{json.dumps(dep_addresses, sort_keys=True)}"
    content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

    execute(
        """INSERT INTO dependency_graph_snapshots
            (entity_type, entity_id, entity_slug, snapshot_date,
             dependency_count, dependency_addresses, dependency_hashes,
             content_hash, attested_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
           ON CONFLICT (entity_type, entity_id, snapshot_date) DO NOTHING""",
        (
            entity_type, entity_id, entity["entity_slug"], today,
            len(dep_addresses),
            json.dumps(dep_addresses),
            json.dumps(dep_hashes) if dep_hashes else None,
            content_hash,
        ),
    )

    try:
        from app.state_attestation import attest_state
        attest_state("contract_dependencies_snapshot", [{
            "entity_slug": entity["entity_slug"],
            "snapshot_date": today.isoformat(),
            "dependency_count": len(dep_addresses),
        }], str(entity_id))
    except Exception:
        pass

    results["snapshots_stored"] += 1


# ---------------------------------------------------------------------------
# Main collector
# ---------------------------------------------------------------------------

async def collect_contract_dependencies() -> dict:
    """
    Map contract dependencies for all scored entities.
    Returns summary dict.
    """
    results = {
        "entities_analyzed": 0,
        "dependencies_found": 0,
        "new_dependencies": 0,
        "removed_dependencies": 0,
        "snapshots_stored": 0,
        "errors": [],
    }

    entities = _load_scored_entities_with_contracts()
    if not entities:
        logger.info("Contract dependencies: no scored entities with contracts")
        return results

    for entity in entities:
        try:
            current_deps = _detect_dependencies(entity)
            _reconcile_dependencies(entity, current_deps, results)
            _store_daily_snapshot(entity, current_deps, results)
            results["entities_analyzed"] += 1
        except Exception as e:
            error_msg = f"{entity['entity_slug']}: {e}"
            results["errors"].append(error_msg)
            logger.error(f"Contract dependency analysis failed: {error_msg}")

    logger.info(
        f"Contract dependencies: entities={results['entities_analyzed']} "
        f"deps={results['dependencies_found']} "
        f"new={results['new_dependencies']} "
        f"removed={results['removed_dependencies']} "
        f"snapshots={results['snapshots_stored']} "
        f"errors={len(results['errors'])}"
    )
    return results
