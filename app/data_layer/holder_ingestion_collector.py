"""
Phase 2 Sprint 1 — Wallet Holder Ingestion
=============================================
Scans top holders of every scored entity (stablecoins, protocols, LSTs,
governance tokens) via Etherscan PRO + Blockscout fallback. Promotes
new addresses to wallet_graph.wallets.

Target: grow wallet_graph from 46K to 250K+ in first run.
Budget: ~100 Etherscan calls per weekly sweep (0.05% of 200K/day plan).
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import httpx

from app.database import fetch_all, fetch_one, get_cursor

logger = logging.getLogger(__name__)

USD_THRESHOLD = 10_000
MAX_CALLS_PER_RUN = 500
ETHERSCAN_BASE = "https://api.etherscan.io/api"

# Well-known governance token contracts (Ethereum mainnet)
GOVERNANCE_TOKENS = {
    "aave-dao": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
    "lido-dao": "0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32",
    "compound-dao": "0xc00e94Cb662C3520282E6f5717214004A7f26888",
    "curve-dao": "0xD533a949740bb3306d119CC777fa900bA034cd52",
    "convex-dao": "0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B",
    "uniswap-dao": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
    "ens-dao": "0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72",
    "arbitrum-dao": "0xB50721BCf8d664c30412Cfbc6cf7a15145234ad1",
    "optimism-dao": "0x4200000000000000000000000000000000000042",
    "gitcoin-dao": "0xDe30da39c46104798bB5aA3fe8B9e0e1F348163F",
    "safe-dao": "0x5aFE3855358E112B5647B952709E6165e1c1eEEe",
    "maker-dao": "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2",
}


def _build_entity_specs() -> list[dict]:
    """Build the full list of (entity_type, entity_id, contract, chain) to scan."""
    specs = []

    # 1. Stablecoins
    try:
        rows = fetch_all(
            "SELECT id, symbol, contract FROM stablecoins "
            "WHERE contract IS NOT NULL AND scoring_enabled = TRUE"
        ) or []
        for r in rows:
            contract = r.get("contract")
            if contract and len(contract) == 42:
                specs.append({
                    "entity_type": "stablecoin",
                    "entity_id": r["id"],
                    "contract": contract.lower(),
                    "chain": "ethereum",
                })
    except Exception as e:
        logger.error(f"[holder_ingestion] stablecoins spec build failed: {e}")

    # 2. PSI protocols — use core_contract from registry
    try:
        import json as _j
        registry_path = os.path.join(os.path.dirname(__file__), "..", "config", "contract_registry.json")
        if os.path.exists(registry_path):
            with open(registry_path) as f:
                registry = _j.load(f)
            for slug, data in registry.get("protocols", {}).items():
                core = data.get("core_contract")
                if core and core.get("address"):
                    specs.append({
                        "entity_type": "protocol",
                        "entity_id": slug,
                        "contract": core["address"].lower(),
                        "chain": core.get("chain", "ethereum"),
                    })
    except Exception as e:
        logger.error(f"[holder_ingestion] protocol spec build failed: {e}")

    # 3. LSTs
    try:
        from app.index_definitions.lsti_v01 import LST_ENTITIES
        for ent in LST_ENTITIES:
            contract = ent.get("contract")
            if contract:
                specs.append({
                    "entity_type": "lst",
                    "entity_id": ent["slug"],
                    "contract": contract.lower(),
                    "chain": "ethereum",
                })
    except Exception as e:
        logger.error(f"[holder_ingestion] LST spec build failed: {e}")

    # 4. Governance tokens
    for slug, contract in GOVERNANCE_TOKENS.items():
        specs.append({
            "entity_type": "governance",
            "entity_id": slug,
            "contract": contract.lower(),
            "chain": "ethereum",
        })

    # Deduplicate by contract (same contract can appear in multiple entity types)
    seen = set()
    deduped = []
    for s in specs:
        key = (s["contract"], s["chain"])
        if key not in seen:
            seen.add(key)
            deduped.append(s)

    return deduped


async def _fetch_holders_etherscan(
    client: httpx.AsyncClient,
    contract: str,
    api_key: str,
    page: int = 1,
    offset: int = 10000,
) -> list[dict]:
    """Fetch top holders from Etherscan PRO API."""
    from app.shared_rate_limiter import rate_limiter
    await rate_limiter.acquire("etherscan")

    resp = await client.get(ETHERSCAN_BASE, params={
        "module": "token",
        "action": "tokenholderlist",
        "contractaddress": contract,
        "page": page,
        "offset": offset,
        "apikey": api_key,
    }, timeout=30)

    data = resp.json()
    if data.get("status") != "1":
        msg = data.get("message", "unknown error")
        if "NOTOK" in str(msg) or "No data" in str(msg):
            return []
        raise Exception(f"Etherscan holder list: {msg}")

    return data.get("result", [])


async def _fetch_holders_blockscout(
    client: httpx.AsyncClient,
    contract: str,
    chain: str = "ethereum",
) -> list[dict]:
    """Fallback: fetch holders from Blockscout v2."""
    from app.shared_rate_limiter import rate_limiter
    await rate_limiter.acquire("blockscout")

    hosts = {
        "ethereum": "eth.blockscout.com",
        "base": "base.blockscout.com",
        "arbitrum": "arbitrum.blockscout.com",
    }
    host = hosts.get(chain, hosts["ethereum"])

    resp = await client.get(
        f"https://{host}/api/v2/tokens/{contract}/holders",
        params={"limit": 50},
        timeout=30,
    )
    if resp.status_code != 200:
        return []

    items = resp.json().get("items", [])
    return [
        {
            "TokenHolderAddress": item.get("address", {}).get("hash", ""),
            "TokenHolderQuantity": item.get("value", "0"),
        }
        for item in items
    ]


def _parse_holder_balance(raw_qty: str, decimals: int = 18) -> float:
    try:
        return float(int(raw_qty)) / (10 ** decimals)
    except (ValueError, OverflowError):
        return 0.0


async def run_holder_ingestion() -> dict:
    """Main entry: scan all scored entities, ingest holders, promote to wallet_graph."""
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        logger.error("[holder_ingestion] no ETHERSCAN_API_KEY — cannot run")
        return {"error": "no api key"}

    specs = _build_entity_specs()
    logger.error(f"[holder_ingestion] starting: {len(specs)} entities to scan")

    if not specs:
        return {"entities": 0}

    stats = defaultdict(lambda: {"scanned": 0, "holders_found": 0, "new_wallets": 0, "errors": 0})
    total_calls = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for i, spec in enumerate(specs):
            if total_calls >= MAX_CALLS_PER_RUN:
                logger.error(f"[holder_ingestion] ABORTED: {total_calls} calls exceeded {MAX_CALLS_PER_RUN} budget")
                break

            etype = spec["entity_type"]
            eid = spec["entity_id"]
            contract = spec["contract"]
            chain = spec["chain"]

            try:
                # Try Etherscan first (ethereum only), Blockscout fallback
                holders = []
                if chain == "ethereum":
                    holders = await _fetch_holders_etherscan(client, contract, api_key)
                    total_calls += 1
                if not holders:
                    holders = await _fetch_holders_blockscout(client, contract, chain)
                    total_calls += 1

                # Filter by USD threshold (approximate: stablecoins ≈ $1, ETH-based ≈ $3000)
                # For simplicity, use raw balance as USD proxy for stablecoins, skip USD calc for others
                filtered = []
                for rank, h in enumerate(holders, 1):
                    addr = (h.get("TokenHolderAddress") or "").lower()
                    if not addr or not addr.startswith("0x") or len(addr) != 42:
                        continue

                    raw_qty = h.get("TokenHolderQuantity", "0")
                    # Stablecoins: 6 decimals (USDC/USDT) or 18 (DAI)
                    decimals = 6 if etype == "stablecoin" else 18
                    balance = _parse_holder_balance(raw_qty, decimals)
                    balance_usd = balance if etype == "stablecoin" else balance * 3000 if etype == "lst" else balance

                    if balance_usd < USD_THRESHOLD:
                        continue

                    filtered.append({
                        "address": addr,
                        "balance_raw": float(raw_qty) if raw_qty else 0,
                        "balance_usd": balance_usd,
                        "rank": rank,
                    })

                stats[etype]["scanned"] += 1
                stats[etype]["holders_found"] += len(filtered)

                # Warn on unusually high count
                if len(filtered) > 50000:
                    logger.error(
                        f"[holder_ingestion] WARN: {eid} has {len(filtered)} holders above threshold — "
                        f"check contract address"
                    )

                # Bulk insert to wallet_holder_discovery
                inserted = 0
                for h in filtered:
                    try:
                        with get_cursor() as cur:
                            cur.execute("""
                                INSERT INTO wallet_holder_discovery
                                    (wallet_address, entity_type, entity_id, entity_contract,
                                     chain, balance_raw, balance_usd, rank_in_entity, source)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (wallet_address, entity_id, entity_contract, chain)
                                DO UPDATE SET balance_raw = EXCLUDED.balance_raw,
                                             balance_usd = EXCLUDED.balance_usd,
                                             rank_in_entity = EXCLUDED.rank_in_entity,
                                             discovered_at = NOW()
                            """, (
                                h["address"], etype, eid, contract, chain,
                                h["balance_raw"], h["balance_usd"], h["rank"],
                                "etherscan_pro" if chain == "ethereum" else "blockscout",
                            ))
                        inserted += 1
                    except Exception as e:
                        if inserted == 0:
                            logger.error(f"[holder_ingestion] insert failed for {eid}: {e}")
                        break

                # Promote new addresses to wallet_graph.wallets
                new_count = 0
                addresses = [h["address"] for h in filtered]
                if addresses:
                    try:
                        source_label = f"holder_scan:{etype}:{eid}"
                        with get_cursor() as cur:
                            from psycopg2.extras import execute_values
                            execute_values(cur, """
                                INSERT INTO wallet_graph.wallets (address, source, created_at)
                                VALUES %s
                                ON CONFLICT (address) DO NOTHING
                            """, [(a, source_label, datetime.now(timezone.utc)) for a in addresses],
                                page_size=1000)
                            new_count = cur.rowcount
                    except Exception as e:
                        logger.error(f"[holder_ingestion] wallet promotion failed for {eid}: {e}")

                stats[etype]["new_wallets"] += new_count

                if (i + 1) % 10 == 0:
                    logger.error(
                        f"[holder_ingestion] progress: {i + 1}/{len(specs)} entities, "
                        f"calls={total_calls}"
                    )

            except Exception as e:
                stats[etype]["errors"] += 1
                logger.error(f"[holder_ingestion] {eid}: FAIL {e}")

            await asyncio.sleep(0.5)

    # Attestation
    try:
        from app.data_layer.provenance_scaling import attest_data_batch
        total_new = sum(s["new_wallets"] for s in stats.values())
        if total_new > 0:
            attest_data_batch("wallet_holder_discovery", [dict(stats)])
    except Exception:
        pass

    # SUMMARY
    for etype, s in sorted(stats.items()):
        logger.error(
            f"[holder_ingestion] {etype}: scanned={s['scanned']} "
            f"holders={s['holders_found']} new_wallets={s['new_wallets']} "
            f"errors={s['errors']}"
        )

    total_new = sum(s["new_wallets"] for s in stats.values())
    total_holders = sum(s["holders_found"] for s in stats.values())
    logger.error(
        f"[holder_ingestion] TOTAL: entities={len(specs)}, holders_found={total_holders}, "
        f"new_wallets={total_new}, etherscan_calls={total_calls}"
    )

    return {
        "entities_scanned": len(specs),
        "by_type": {k: dict(v) for k, v in stats.items()},
        "total_new_wallets": total_new,
        "total_holders_found": total_holders,
        "etherscan_calls": total_calls,
    }
