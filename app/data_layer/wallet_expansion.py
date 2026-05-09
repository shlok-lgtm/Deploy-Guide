"""
Autonomous Wallet Graph Expansion — Pipelined
===============================================
Producer-consumer pipeline: fetch at 4.9/s, parse + insert in parallel.
Zero dead time between API calls.

Strategy: crawl outward from edge wallets (fewest connections, highest value),
discover counterparties via Etherscan tokentx, auto-seed into graph.

Uses EtherscanPipeline for ~26% throughput increase over sequential.
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timezone

import httpx

from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


async def run_wallet_graph_expansion(
    target_new_wallets: int = 10_000,
    max_etherscan_calls: int = 250_000,
) -> dict:
    """
    Expand the wallet graph using producer-consumer pipeline.

    Producer: fetches tokentx from Etherscan at rate-limited speed.
    Consumer: extracts counterparty addresses, batches inserts.
    """
    from app.database import (
        fetch_all, fetch_one, get_cursor,
        fetch_all_async, fetch_one_async,
    )
    from app.data_layer.async_pipeline import EtherscanPipeline

    ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
    if not ETHERSCAN_API_KEY:
        logger.error("[wallet_expansion] ETHERSCAN_API_KEY not set — cannot expand")
        return {"error": "ETHERSCAN_API_KEY not set"}

    # 1. Find edge wallets: high value, few connections
    logger.error("[wallet_expansion] querying edge wallets (value >= $100K, sorted by fewest edges)")
    edge_wallets = await fetch_all_async(
        """SELECT w.address, r.total_stablecoin_value,
                  COALESCE(e.edge_count, 0) as edge_count
           FROM wallet_graph.wallets w
           JOIN wallet_graph.wallet_risk_scores r ON w.address = r.wallet_address
           LEFT JOIN (
               SELECT from_address as address, COUNT(*) as edge_count
               FROM wallet_graph.wallet_edges
               GROUP BY from_address
           ) e ON w.address = e.address
           WHERE r.total_stablecoin_value >= 100000
           ORDER BY COALESCE(e.edge_count, 0) ASC, r.total_stablecoin_value DESC
           LIMIT %s""",
        (target_new_wallets,),
    )

    if not edge_wallets:
        logger.error("[wallet_expansion] ZERO edge wallets found — no wallets have $100K+ value with risk scores")
        # Diagnostic: how many wallets have risk scores at all?
        _scored = await fetch_one_async("SELECT COUNT(*) as cnt FROM wallet_graph.wallet_risk_scores WHERE total_stablecoin_value > 0")
        _total = await fetch_one_async("SELECT COUNT(*) as cnt FROM wallet_graph.wallets")
        logger.error(
            f"[wallet_expansion] DEBUG: total_wallets={_total['cnt'] if _total else 0}, "
            f"scored_with_value={_scored['cnt'] if _scored else 0}"
        )
        # Try lower threshold
        edge_wallets = await fetch_all_async(
            """SELECT w.address, COALESCE(r.total_stablecoin_value, 0) as total_stablecoin_value,
                      0 as edge_count
               FROM wallet_graph.wallets w
               LEFT JOIN wallet_graph.wallet_risk_scores r ON w.address = r.wallet_address
               ORDER BY r.total_stablecoin_value DESC NULLS LAST
               LIMIT %s""",
            (min(target_new_wallets, 500),),
        )
        logger.error(f"[wallet_expansion] fallback: found {len(edge_wallets)} wallets (any value)")
        if not edge_wallets:
            return {"error": "no wallets found even with fallback"}

    logger.error(
        f"[wallet_expansion] found {len(edge_wallets)} edge wallets "
        f"(top value: ${edge_wallets[0].get('total_stablecoin_value', 0):,.0f}, "
        f"edges: {edge_wallets[0].get('edge_count', 0)})"
    )

    # Pre-load existing addresses for dedup
    existing = await fetch_all_async("SELECT address FROM wallet_graph.wallets")
    existing_set = set(r["address"].lower() for r in existing) if existing else set()
    logger.error(f"[wallet_expansion] existing wallets for dedup: {len(existing_set)}")
    discovered_addresses = set()
    _api_errors = 0
    _api_empty = 0
    _api_ok = 0

    # Early diagnostic: edge wallets are already in the graph (they must be — that's where we got them).
    # New wallets come from their COUNTERPARTIES in token transfers.
    # If all counterparties are already tracked, discovered=0 and the graph is closed.
    edge_addrs = {w["address"].lower() for w in edge_wallets}
    overlap = edge_addrs & existing_set
    logger.error(
        f"[wallet_expansion] DEDUP: edge_wallets={len(edge_wallets)}, "
        f"existing={len(existing_set)}, edge_in_existing={len(overlap)}/{len(edge_addrs)} "
        f"(new wallets come from counterparties, not edge wallets themselves)"
    )

    # 2. Define producer function (fetch tokentx)
    async def fetch_tokentx(client: httpx.AsyncClient, wallet: dict) -> dict:
        nonlocal _api_errors
        _t0 = time.monotonic()
        _status = None
        try:
            resp = await client.get(
                ETHERSCAN_V2_BASE,
                params={
                    "chainid": 1,
                    "module": "account",
                    "action": "tokentx",
                    "address": wallet["address"],
                    "startblock": 0,
                    "endblock": 99999999,
                    "page": 1,
                    "offset": 50,
                    "sort": "desc",
                    "apikey": ETHERSCAN_API_KEY,
                },
                timeout=15,
            )
            _status = resp.status_code
        except Exception:
            _status = 0
            raise
        finally:
            try:
                track_api_call(provider="etherscan", endpoint="account/tokentx", caller="data_layer.wallet_expansion", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[wallet_expansion] track_api_call failed: {e}")
                try:
                    from app.worker import _record_cycle_error
                    _record_cycle_error(
                        error_type="data_layer_fetch_tokentx_track_api_call_failure",
                        error_message=str(e)[:500],
                        cycle_phase="wallet_expansion",
                    )
                except Exception:
                    pass
        if resp.status_code == 429 or "Max rate limit" in resp.text:
            _api_errors += 1
            raise httpx.HTTPStatusError(
                "Rate limited", request=resp.request, response=resp
            )
        resp.raise_for_status()
        return resp.json()

    # 3. Define consumer function (parse + discover)
    async def process_transfers(data: dict, wallet: dict):
        nonlocal _api_empty, _api_ok
        if data.get("status") != "1":
            _api_empty += 1
            return

        transfers = data.get("result", [])
        if not transfers:
            _api_empty += 1
            return

        _api_ok += 1
        address_lower = wallet["address"].lower()

        for tx in transfers:
            for addr_field in ["from", "to"]:
                counterparty = (tx.get(addr_field) or "").lower()
                if (
                    counterparty
                    and counterparty != address_lower
                    and counterparty != ZERO_ADDRESS
                    and counterparty not in existing_set
                    and counterparty not in discovered_addresses
                    and counterparty.startswith("0x")
                    and len(counterparty) == 42
                ):
                    discovered_addresses.add(counterparty)

    # 4. Run the pipeline
    logger.error(f"[wallet_expansion] starting pipeline: {len(edge_wallets)} wallets, max_calls={max_etherscan_calls}")
    pipeline = EtherscanPipeline(
        provider="etherscan",
        caller="wallet_expansion",
        max_calls=max_etherscan_calls,
        queue_size=100,
        consumer_count=2,
    )

    stats = await pipeline.run(
        items=edge_wallets,
        fetch_fn=fetch_tokentx,
        process_fn=process_transfers,
    )

    logger.error(
        f"[wallet_expansion] pipeline done: processed={stats.items_processed}, "
        f"fetched={stats.items_fetched}, api_ok={_api_ok}, api_empty={_api_empty}, "
        f"api_errors={_api_errors}, discovered={len(discovered_addresses)}"
    )
    if discovered_addresses:
        sample = list(discovered_addresses)[:5]
        logger.error(f"[wallet_expansion] sample new addresses: {sample}")
    elif _api_ok > 0:
        logger.error(
            f"[wallet_expansion] ZERO new addresses despite {_api_ok} successful API calls — "
            f"all counterparties already in graph ({len(existing_set)} existing)"
        )

    # 5. Batch insert discovered wallets
    new_wallets_seeded = 0
    already_existed = 0
    insert_errors = 0
    if discovered_addresses:
        batch = list(discovered_addresses)[:target_new_wallets]
        logger.error(f"[wallet_expansion] inserting {len(batch)} discovered addresses")
        for addr in batch:
            try:
                def _inner_one(_addr=addr):
                    with get_cursor() as cur:
                        cur.execute(
                            """INSERT INTO wallet_graph.wallets (address, source, created_at)
                               VALUES (%s, 'graph_expansion', NOW())
                               ON CONFLICT (address) DO NOTHING""",
                            (_addr,),
                        )
                        return cur.rowcount
                _rowcount = await asyncio.to_thread(_inner_one)
                if _rowcount > 0:
                    new_wallets_seeded += 1
                else:
                    already_existed += 1
            except Exception as e:
                insert_errors += 1
                if insert_errors <= 3:
                    logger.error(f"[wallet_expansion] insert failed: {addr}: {e}")
        logger.error(
            f"[wallet_expansion] insert results: new={new_wallets_seeded}, "
            f"already_existed={already_existed}, errors={insert_errors} "
            f"(batch={len(batch)})"
        )
    else:
        logger.error("[wallet_expansion] ZERO new addresses discovered — all counterparties already in graph")

    # Stats
    try:
        total_wallets = await fetch_one_async("SELECT COUNT(*) as cnt FROM wallet_graph.wallets")
        total_count = total_wallets["cnt"] if total_wallets else 0
    except Exception:
        total_count = "unknown"

    result = {
        "edge_wallets_processed": stats.items_processed,
        "new_wallets_discovered": len(discovered_addresses),
        "new_wallets_seeded": new_wallets_seeded,
        "insert_errors": insert_errors,
        "etherscan_calls_used": stats.items_fetched,
        "api_ok": _api_ok,
        "api_empty": _api_empty,
        "api_errors": _api_errors,
        "total_graph_size": total_count,
        "pipeline": stats.to_dict(),
    }

    logger.error(
        f"[wallet_expansion] SUMMARY: edge_wallets={len(edge_wallets)}, "
        f"discovered={len(discovered_addresses)}, inserted={new_wallets_seeded}, "
        f"insert_errors={insert_errors}, graph_size={total_count}"
    )

    return result


async def run_multi_source_seeding() -> dict:
    """
    Seed wallets from non-edge sources. Bulk INSERT with overlap diagnostics.
    """
    from app.database import (
        fetch_one, fetch_all, get_cursor,
        fetch_one_async, fetch_all_async,
    )

    results = {"sources": {}, "total_new": 0}

    # Source 1: Governance voters
    try:
        src_count = await fetch_one_async("SELECT COUNT(DISTINCT voter_address) as cnt FROM governance_voters WHERE voter_address IS NOT NULL")
        overlap = await fetch_one_async("""
            SELECT COUNT(DISTINCT gv.voter_address) as cnt
            FROM governance_voters gv
            JOIN wallet_graph.wallets w ON LOWER(gv.voter_address) = LOWER(w.address)
        """)
        src_n = src_count["cnt"] if src_count else 0
        ovl_n = overlap["cnt"] if overlap else 0
        logger.error(f"[wallet_seeding] governance_voters: source={src_n}, overlap={ovl_n}, expected_new={src_n - ovl_n}")
        def _inner_gv():
            with get_cursor() as cur:
                cur.execute("""
                    INSERT INTO wallet_graph.wallets (address, source, created_at)
                    SELECT DISTINCT LOWER(voter_address), 'governance_voter', NOW()
                    FROM governance_voters
                    WHERE voter_address IS NOT NULL
                      AND LOWER(voter_address) NOT IN (SELECT LOWER(address) FROM wallet_graph.wallets)
                    ON CONFLICT (address) DO NOTHING
                """)
                return cur.rowcount
        count = await asyncio.to_thread(_inner_gv)
        results["sources"]["governance_voters"] = count
        results["total_new"] += count
        logger.error(f"[wallet_seeding] governance_voters: inserted={count}")
    except Exception as e:
        logger.error(f"[wallet_seeding] governance_voters failed: {e}")

    # Source 2: Mint/burn originators
    try:
        src_count = await fetch_one_async("""
            SELECT COUNT(DISTINCT from_address) as cnt FROM mint_burn_events
            WHERE from_address IS NOT NULL AND from_address != '0x0000000000000000000000000000000000000000'
        """)
        overlap = await fetch_one_async("""
            SELECT COUNT(DISTINCT mb.from_address) as cnt
            FROM mint_burn_events mb
            JOIN wallet_graph.wallets w ON LOWER(mb.from_address) = LOWER(w.address)
            WHERE mb.from_address != '0x0000000000000000000000000000000000000000'
        """)
        src_n = src_count["cnt"] if src_count else 0
        ovl_n = overlap["cnt"] if overlap else 0
        logger.error(f"[wallet_seeding] mint_burn: source={src_n}, overlap={ovl_n}, expected_new={src_n - ovl_n}")
        def _inner_mb():
            with get_cursor() as cur:
                cur.execute("""
                    INSERT INTO wallet_graph.wallets (address, source, created_at)
                    SELECT DISTINCT LOWER(from_address), 'mint_burn', NOW()
                    FROM mint_burn_events
                    WHERE from_address IS NOT NULL
                      AND from_address != '0x0000000000000000000000000000000000000000'
                      AND LOWER(from_address) NOT IN (SELECT LOWER(address) FROM wallet_graph.wallets)
                    ON CONFLICT (address) DO NOTHING
                """)
                return cur.rowcount
        count = await asyncio.to_thread(_inner_mb)
        results["sources"]["mint_burn"] = count
        results["total_new"] += count
        logger.error(f"[wallet_seeding] mint_burn: inserted={count}")
    except Exception as e:
        logger.error(f"[wallet_seeding] mint_burn failed: {e}")

    # Source 3: Protocol pool wallets
    try:
        src_count = await fetch_one_async("SELECT COUNT(DISTINCT wallet_address) as cnt FROM protocol_pool_wallets")
        overlap = await fetch_one_async("""
            SELECT COUNT(DISTINCT pw.wallet_address) as cnt
            FROM protocol_pool_wallets pw
            JOIN wallet_graph.wallets w ON LOWER(pw.wallet_address) = LOWER(w.address)
        """)
        src_n = src_count["cnt"] if src_count else 0
        ovl_n = overlap["cnt"] if overlap else 0
        logger.error(f"[wallet_seeding] pool_wallets: source={src_n}, overlap={ovl_n}, expected_new={src_n - ovl_n}")
        def _inner_pw():
            with get_cursor() as cur:
                cur.execute("""
                    INSERT INTO wallet_graph.wallets (address, source, created_at)
                    SELECT DISTINCT LOWER(wallet_address), 'pool_wallet', NOW()
                    FROM protocol_pool_wallets
                    WHERE LOWER(wallet_address) NOT IN (SELECT LOWER(address) FROM wallet_graph.wallets)
                    ON CONFLICT (address) DO NOTHING
                """)
                return cur.rowcount
        count = await asyncio.to_thread(_inner_pw)
        results["sources"]["pool_wallets"] = count
        results["total_new"] += count
        logger.error(f"[wallet_seeding] pool_wallets: inserted={count}")
    except Exception as e:
        logger.error(f"[wallet_seeding] pool_wallets failed: {e}")

    # Source 4: Top stablecoin holders (Etherscan — uses API calls)
    try:
        api_key = os.environ.get("ETHERSCAN_API_KEY", "")
        if api_key:
            stablecoins = await fetch_all_async(
                "SELECT id, contract FROM stablecoins WHERE scoring_enabled = TRUE AND contract IS NOT NULL LIMIT 5"
            )
            holder_count = 0
            async with httpx.AsyncClient(timeout=15) as client:
                for sc in (stablecoins or []):
                    contract = sc.get("contract", "")
                    if not contract or not contract.startswith("0x"):
                        continue
                    try:
                        _t0 = time.monotonic()
                        _status = None
                        try:
                            resp = await client.get(
                                ETHERSCAN_V2_BASE,
                                params={
                                    "chainid": 1, "module": "token", "action": "tokenholderlist",
                                    "contractaddress": contract, "page": 1, "offset": 50,
                                    "apikey": api_key,
                                },
                            )
                            _status = resp.status_code
                        except Exception:
                            _status = 0
                            raise
                        finally:
                            try:
                                track_api_call(provider="etherscan", endpoint="token/tokenholderlist", caller="data_layer.wallet_expansion", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                            except asyncio.CancelledError:
                                raise
                            except Exception as e:
                                logger.warning(f"[wallet_expansion] track_api_call failed: {e}")
                                try:
                                    from app.worker import _record_cycle_error
                                    _record_cycle_error(
                                        error_type="data_layer_run_multi_source_seeding_track_api_call_failure",
                                        error_message=str(e)[:500],
                                        cycle_phase="wallet_expansion",
                                    )
                                except Exception:
                                    pass
                        if resp.status_code != 200:
                            continue
                        data = resp.json()
                        holders = data.get("result", []) if data.get("status") == "1" else []
                        for h in holders:
                            addr = (h.get("TokenHolderAddress") or "").lower()
                            if addr and addr.startswith("0x") and len(addr) == 42:
                                try:
                                    def _inner_th(_addr=addr):
                                        with get_cursor() as cur:
                                            cur.execute(
                                                "INSERT INTO wallet_graph.wallets (address, source, created_at) VALUES (%s, 'top_holder', NOW()) ON CONFLICT DO NOTHING",
                                                (_addr,),
                                            )
                                            return cur.rowcount
                                    _rowcount = await asyncio.to_thread(_inner_th)
                                    if _rowcount > 0:
                                        holder_count += 1
                                except asyncio.CancelledError:
                                    raise
                                except Exception as e:
                                    logger.warning(f"[wallet_expansion] top_holder insert failed: {e}")
                                    try:
                                        from app.worker import _record_cycle_error
                                        _record_cycle_error(
                                            error_type="data_layer_run_multi_source_seeding_top_holder_insert_failure",
                                            error_message=str(e)[:500],
                                            cycle_phase="wallet_expansion",
                                        )
                                    except Exception:
                                        pass
                        await asyncio.sleep(0.2)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.warning(f"[wallet_expansion] top_holders contract loop failed: {e}")
                        try:
                            from app.worker import _record_cycle_error
                            _record_cycle_error(
                                error_type="data_layer_run_multi_source_seeding_top_holders_loop_failure",
                                error_message=str(e)[:500],
                                cycle_phase="wallet_expansion",
                            )
                        except Exception:
                            pass
            results["sources"]["top_holders"] = holder_count
            results["total_new"] += holder_count
            logger.error(f"[wallet_seeding] top_holders: inserted={holder_count}")
    except Exception as e:
        logger.error(f"[wallet_seeding] top_holders failed: {e}")

    logger.error(f"[wallet_seeding] SUMMARY: {results['total_new']} total new — {results['sources']}")
    return results
