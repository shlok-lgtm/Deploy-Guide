"""
Wallet Indexer — Edge Builder
==============================
Derives wallet-to-wallet transfer edges from ERC-20 stablecoin token transfer
histories (tokentx). Stores edges in wallet_graph.wallet_edges with weight
signals (transfer count, total value, recency).

Uses the same Blockscout/Etherscan API as scanner.py. Admin-triggered only.
"""

import os
import math
import asyncio
import logging
import json
from datetime import datetime, timezone

import httpx
import psycopg2

from app.database import (
    fetch_all, fetch_one, execute,
    fetch_one_async, fetch_all_async, execute_async,
)
from app.indexer.config import (
    BLOCK_EXPLORER_PROVIDER,
    EXPLORER_RATE_LIMIT_DELAY,
    get_all_known_contracts,
    get_chain_contracts,
    CHAIN_CONFIGS,
    SUPPORTED_CHAINS,
)

logger = logging.getLogger(__name__)

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

if BLOCK_EXPLORER_PROVIDER == "etherscan":
    EXPLORER_BASE = "https://api.etherscan.io/v2/api"
else:
    EXPLORER_BASE = "https://api.blockscout.com/v2/api"

_EXPLORER_CHAIN_KEY = "chainid" if BLOCK_EXPLORER_PROVIDER == "etherscan" else "chain_id"


class _FetchResult:
    """Sentinel for _fetch_tokentx_page outcomes."""
    __slots__ = ("transfers", "error_type", "error_detail")

    def __init__(self, transfers=None, error_type=None, error_detail=None):
        self.transfers = transfers
        self.error_type = error_type
        self.error_detail = error_detail

    @property
    def ok(self):
        return self.error_type is None

    @property
    def transient(self):
        return self.error_type in ("explorer_timeout", "explorer_network_error", "explorer_server_error")


def _record_explorer_error(error_type: str, detail: str, chain: str = "ethereum"):
    """Write explorer failure to cycle_errors for V9.11 Layer 2 visibility."""
    try:
        from app.worker import _record_cycle_error
        _record_cycle_error(
            error_type=error_type,
            error_message=detail[:500],
            cycle_phase=f"edge_builder:{chain}",
        )
    except Exception:
        pass


async def _fetch_tokentx_page(
    client: httpx.AsyncClient,
    wallet_address: str,
    api_key: str,
    page: int = 1,
    offset: int = 100,
    explorer_base: str = None,
    chain_id: int = 1,
) -> _FetchResult:
    """Fetch one page of ERC-20 token transfer events for a wallet.

    Returns _FetchResult with .transfers (list) on success, or
    .error_type + .error_detail on failure. Callers use .ok and
    .transient to decide retry vs escalation.
    """
    base_url = explorer_base or EXPLORER_BASE
    try:
        resp = await client.get(
            base_url,
            params={
                "chain_id" if "blockscout" in base_url else "chainid": chain_id,
                "module": "account",
                "action": "tokentx",
                "address": wallet_address,
                "page": page,
                "offset": offset,
                "sort": "desc",
                "apikey": api_key,
            },
            timeout=15.0,
        )
    except httpx.TimeoutException:
        detail = f"Timeout fetching tokentx for {wallet_address[:10]}… page={page}"
        logger.warning(detail)
        await asyncio.to_thread(_record_explorer_error, "explorer_timeout", detail)
        return _FetchResult(error_type="explorer_timeout", error_detail=detail)
    except httpx.NetworkError as e:
        detail = f"Network error fetching tokentx for {wallet_address[:10]}…: {e}"
        logger.warning(detail)
        await asyncio.to_thread(_record_explorer_error, "explorer_network_error", detail)
        return _FetchResult(error_type="explorer_network_error", error_detail=detail)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        detail = f"Unexpected error fetching tokentx for {wallet_address[:10]}…: {type(e).__name__}: {e}"
        logger.error(detail)
        await asyncio.to_thread(_record_explorer_error, "explorer_unknown_error", detail)
        return _FetchResult(error_type="explorer_unknown_error", error_detail=detail)

    if resp.status_code >= 500:
        detail = f"Explorer returned {resp.status_code} for {wallet_address[:10]}…"
        logger.warning(detail)
        await asyncio.to_thread(_record_explorer_error, "explorer_server_error", detail)
        return _FetchResult(error_type="explorer_server_error", error_detail=detail)

    if resp.status_code in (401, 403):
        detail = f"Explorer auth failure ({resp.status_code}) for {wallet_address[:10]}…"
        logger.error(detail)
        await asyncio.to_thread(_record_explorer_error, "explorer_auth_failure", detail)
        return _FetchResult(error_type="explorer_auth_failure", error_detail=detail)

    if resp.status_code == 429:
        detail = "Explorer rate limit (HTTP 429)"
        logger.warning(detail)
        await asyncio.to_thread(_record_explorer_error, "explorer_rate_limit", detail)
        await asyncio.sleep(2.0)
        return _FetchResult(error_type="explorer_rate_limit", error_detail=detail)

    try:
        data = resp.json()
    except Exception as e:
        detail = f"Malformed JSON from explorer for {wallet_address[:10]}…: {e}"
        logger.error(detail)
        await asyncio.to_thread(_record_explorer_error, "explorer_malformed_response", detail)
        return _FetchResult(error_type="explorer_malformed_response", error_detail=detail)

    if data.get("status") == "1" and isinstance(data.get("result"), list):
        return _FetchResult(transfers=data["result"])

    msg = data.get("result", "")
    if "Max rate limit" in str(msg):
        detail = f"Explorer rate limit (in-body) for {wallet_address[:10]}…"
        logger.warning(detail)
        await asyncio.to_thread(_record_explorer_error, "explorer_rate_limit", detail)
        await asyncio.sleep(2.0)
        return _FetchResult(error_type="explorer_rate_limit", error_detail=detail)

    return _FetchResult(transfers=[])


def _compute_weight(total_value_usd: float, transfer_count: int, last_transfer_at: datetime) -> float:
    """Compute edge weight from value, frequency, and recency."""
    now = datetime.now(timezone.utc)
    if last_transfer_at.tzinfo is None:
        last_transfer_at = last_transfer_at.replace(tzinfo=timezone.utc)
    days_since = (now - last_transfer_at).days
    recency = max(0.1, 1.0 - (days_since / 365))
    return math.log10(1 + total_value_usd) * transfer_count * recency


async def build_edges_for_wallet(
    client: httpx.AsyncClient,
    wallet_address: str,
    api_key: str,
    max_pages: int = 10,
    chain: str = "ethereum",
) -> dict:
    """
    Fetch token transfer history for a wallet and upsert stablecoin transfer
    edges into wallet_graph.wallet_edges.
    """
    chain_cfg = CHAIN_CONFIGS.get(chain, CHAIN_CONFIGS["ethereum"])
    explorer_base = chain_cfg["explorer_base"]
    chain_id = chain_cfg.get("chain_id", 1)
    scored_contracts = await asyncio.to_thread(get_chain_contracts, chain)
    wallet_lower = wallet_address.lower()

    # Accumulate edges: (from, to) -> {count, total_value, first_ts, last_ts, tokens}
    edge_map: dict[tuple[str, str], dict] = {}
    total_transfers = 0
    pages_fetched = 0
    first_page_failed = False
    consecutive_transient = 0

    for page in range(1, max_pages + 1):
        result = await _fetch_tokentx_page(
            client, wallet_lower, api_key, page=page,
            explorer_base=explorer_base, chain_id=chain_id,
        )
        await asyncio.sleep(EXPLORER_RATE_LIMIT_DELAY)

        if not result.ok:
            if page == 1:
                first_page_failed = True
            if result.transient:
                consecutive_transient += 1
                if consecutive_transient < 3:
                    continue
            break

        consecutive_transient = 0
        transfers = result.transfers
        pages_fetched += 1

        if not transfers:
            break

        for tx in transfers:
            contract_addr = (tx.get("contractAddress") or "").lower()
            if contract_addr not in scored_contracts:
                continue

            from_addr = (tx.get("from") or "").lower()
            to_addr = (tx.get("to") or "").lower()

            if from_addr == ZERO_ADDRESS or to_addr == ZERO_ADDRESS:
                continue
            if not from_addr or not to_addr:
                continue

            token_info = scored_contracts[contract_addr]
            decimals = token_info.get("decimals", 18)
            symbol = token_info.get("symbol", "???")

            try:
                raw_value = int(tx.get("value", "0"))
            except (ValueError, TypeError):
                continue
            value_usd = raw_value / (10 ** decimals)

            ts_raw = tx.get("timeStamp")
            try:
                ts = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc) if ts_raw else datetime.now(timezone.utc)
            except (ValueError, TypeError):
                ts = datetime.now(timezone.utc)

            edge_key = (from_addr, to_addr)
            if edge_key not in edge_map:
                edge_map[edge_key] = {
                    "count": 0,
                    "total_value": 0.0,
                    "first_ts": ts,
                    "last_ts": ts,
                    "tokens": {},
                }

            edge = edge_map[edge_key]
            edge["count"] += 1
            edge["total_value"] += value_usd
            edge["first_ts"] = min(edge["first_ts"], ts)
            edge["last_ts"] = max(edge["last_ts"], ts)

            if symbol not in edge["tokens"]:
                edge["tokens"][symbol] = {"count": 0, "value": 0.0}
            edge["tokens"][symbol]["count"] += 1
            edge["tokens"][symbol]["value"] += value_usd

            total_transfers += 1

        if len(transfers) < 100:
            break

    # Upsert edges
    edges_upserted = 0
    for (from_addr, to_addr), edge in edge_map.items():
        weight = _compute_weight(edge["total_value"], edge["count"], edge["last_ts"])
        tokens_json = json.dumps(edge["tokens"])

        await execute_async(
            """
            INSERT INTO wallet_graph.wallet_edges
                (from_address, to_address, chain, transfer_count, total_value_usd,
                 first_transfer_at, last_transfer_at, tokens_transferred, weight, edge_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'shared_holder')
            ON CONFLICT (from_address, to_address, chain, edge_type) DO UPDATE SET
                transfer_count = wallet_graph.wallet_edges.transfer_count + EXCLUDED.transfer_count,
                total_value_usd = wallet_graph.wallet_edges.total_value_usd + EXCLUDED.total_value_usd,
                first_transfer_at = LEAST(wallet_graph.wallet_edges.first_transfer_at, EXCLUDED.first_transfer_at),
                last_transfer_at = GREATEST(wallet_graph.wallet_edges.last_transfer_at, EXCLUDED.last_transfer_at),
                tokens_transferred = wallet_graph.wallet_edges.tokens_transferred || EXCLUDED.tokens_transferred,
                weight = EXCLUDED.weight,
                updated_at = NOW()
            """,
            (from_addr, to_addr, chain, edge["count"], edge["total_value"],
             edge["first_ts"], edge["last_ts"], tokens_json, weight),
        )
        edges_upserted += 1

    # Update build status — only mark complete if API responded
    if first_page_failed:
        await execute_async(
            """
            INSERT INTO wallet_graph.edge_build_status
                (wallet_address, chain, build_attempted_at, transfers_processed, edges_created, pages_fetched, status)
            VALUES (%s, %s, NOW(), 0, 0, 0, 'api_failure')
            ON CONFLICT (wallet_address, chain) DO UPDATE SET
                build_attempted_at = NOW(),
                status = 'api_failure'
            """,
            (wallet_lower, chain),
        )
    else:
        await execute_async(
            """
            INSERT INTO wallet_graph.edge_build_status
                (wallet_address, chain, last_built_at, build_attempted_at, transfers_processed, edges_created, pages_fetched, status)
            VALUES (%s, %s, NOW(), NOW(), %s, %s, %s, 'complete')
            ON CONFLICT (wallet_address, chain) DO UPDATE SET
                last_built_at = NOW(),
                build_attempted_at = NOW(),
                transfers_processed = EXCLUDED.transfers_processed,
                edges_created = EXCLUDED.edges_created,
                pages_fetched = EXCLUDED.pages_fetched,
                status = 'complete'
            """,
            (wallet_lower, chain, total_transfers, edges_upserted, pages_fetched),
        )

    return {
        "transfers_processed": total_transfers,
        "edges_upserted": edges_upserted,
        "pages_fetched": pages_fetched,
        "api_failure": first_page_failed,
    }


async def run_edge_builder(
    max_wallets: int = 100,
    max_pages_per_wallet: int = 10,
    priority: str = "value",
    chain: str = "ethereum",
) -> dict:
    """
    Batch edge builder. Queries wallets needing edge building and processes them.
    Delegates to Solana adapter for chain='solana'.
    """
    # Solana uses a different data source (Helius, not Blockscout/Etherscan)
    if chain == "solana":
        from app.indexer.solana_edges import run_solana_edge_builder
        return await run_solana_edge_builder(
            max_wallets=max_wallets,
            max_pages_per_wallet=max_pages_per_wallet,
        )

    order_clause = "w.total_stablecoin_value DESC NULLS LAST"
    if priority == "unbuilt":
        order_clause = "w.created_at ASC"

    # Include wallets that haven't been built in 7+ days (re-scan for new transfers)
    wallets = await fetch_all_async(
        f"""
        SELECT w.address, w.total_stablecoin_value
        FROM wallet_graph.wallets w
        LEFT JOIN wallet_graph.edge_build_status e
            ON w.address = e.wallet_address AND e.chain = %s
        WHERE e.wallet_address IS NULL
           OR e.status = 'pending'
           OR e.last_built_at < NOW() - INTERVAL '7 days'
        ORDER BY {order_clause}
        LIMIT %s
        """,
        (chain, max_wallets),
    )

    # Count how many are fresh vs stale
    unbuilt = await fetch_one_async(
        "SELECT COUNT(*) as cnt FROM wallet_graph.wallets w "
        "LEFT JOIN wallet_graph.edge_build_status e ON w.address = e.wallet_address AND e.chain = %s "
        "WHERE e.wallet_address IS NULL", (chain,)
    )
    stale = await fetch_one_async(
        "SELECT COUNT(*) as cnt FROM wallet_graph.edge_build_status "
        "WHERE chain = %s AND last_built_at < NOW() - INTERVAL '7 days'", (chain,)
    )
    logger.error(
        f"[edge_builder] {chain}: {len(wallets)} candidates "
        f"(unbuilt={unbuilt['cnt'] if unbuilt else 0}, stale_7d={stale['cnt'] if stale else 0})"
    )

    if not wallets:
        logger.error(f"[edge_builder] {chain}: no wallets need edge building")
        return {"chain": chain, "wallets_processed": 0, "total_edges_created": 0, "total_transfers": 0}

    api_key = os.environ.get("ETHERSCAN_API_KEY", "") if chain == "ethereum" else ""
    total_edges = 0
    total_transfers = 0
    wallets_processed = 0

    _edge_client = httpx.AsyncClient(
        timeout=30, follow_redirects=True,
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    )
    try:
        for i, w in enumerate(wallets):
            if i < 5 or i % 50 == 0:
                logger.error(f"[edge_builder] wallet {w['address'][:12]}... start ({i}/{len(wallets)})")
            try:
                result = await build_edges_for_wallet(
                    _edge_client, w["address"], api_key,
                    max_pages=max_pages_per_wallet,
                    chain=chain,
                )
                total_edges += result["edges_upserted"]
                total_transfers += result["transfers_processed"]
                wallets_processed += 1

                if i < 5 or (i + 1) % 50 == 0:
                    logger.error(
                        f"[edge_builder] wallet {w['address'][:12]}... done: "
                        f"edges={result['edges_upserted']}, txs={result['transfers_processed']}, "
                        f"pages={result['pages_fetched']} | running_total: {total_edges} edges, {wallets_processed} wallets"
                    )
            except Exception as e:
                logger.error(f"[edge_builder] wallet {w['address'][:12]}... FAILED: {type(e).__name__}: {e}")
                await execute_async(
                    """
                    INSERT INTO wallet_graph.edge_build_status (wallet_address, chain, status)
                    VALUES (%s, %s, 'pending')
                    ON CONFLICT (wallet_address, chain) DO UPDATE SET status = 'pending'
                    """,
                    (w["address"], chain),
                )
    finally:
        await _edge_client.aclose()

    logger.error(
        f"[edge_builder] {chain}: examined {wallets_processed} wallets, "
        f"new_edges={total_edges}, transfers={total_transfers}"
    )

    # Attestation moved to run_edge_builder_scheduled wrapper (v9.12 #209).
    # See app/data_layer/exchange_collector.py::run_exchange_collection_scheduled
    # for the #198 pattern precedent. Schedulers MUST call the wrapper, NOT
    # this function directly, so attestation fires once per scheduled call.

    return {
        "chain": chain,
        "wallets_processed": wallets_processed,
        "total_edges_created": total_edges,
        "total_transfers": total_transfers,
    }


# =============================================================================
# Scheduled wrapper — v9.12 module-canonical entry per #198 / #209
# =============================================================================

# Freshness gate: matches the pre-v9.12 main.py:471 10h cadence (Q4 doc).
_EDGE_BUILDER_FRESHNESS_HOURS = 10


async def run_edge_builder_scheduled(chain: str, cycle_ts=None) -> dict:
    """Module-canonical scheduler entry for edge building (v9.12 #209).

    Per-chain wrapper. Returns a status dict regardless of branch:
      - {"status": "skipped_fresh", "chain": chain, "table_age_hours": X}
      - {"status": "ran", "chain": chain, ...run_edge_builder() result}
      - {"status": "error", "chain": chain, "error": str}

    Attestation ALWAYS fires in the `ran` branch (Bug A fix from #209 doc:
    the prior `total_edges > 0` gate rarely opened because ON CONFLICT
    DO UPDATE inflates edges_upserted with steady-state UPDATEs).

    `chain` lives INSIDE the payload dict (Bug B fix). The attest_state
    call passes NO 3rd positional, so entity_id stays NULL — matching
    consumer convention (coherence.py:159 filters `entity_id IS NULL`;
    report.py:327 + pulse_generator.py:260 call get_latest_attestation
    without entity_id, which filters IS NULL via state_attestation.py:84).

    Q5 (true inserted count via `RETURNING (xmax = 0)`) is deferred.
    """
    from app.state_attestation import attest_state

    # Per-chain freshness check via wallet_graph.wallet_edges MAX(updated_at).
    # Preserves the pre-v9.12 main.py:471 10h cadence (Q4 unanswered in doc).
    table_age_hours: float = float(_EDGE_BUILDER_FRESHNESS_HOURS)
    try:
        latest = await fetch_one_async(
            "SELECT MAX(updated_at) AS t FROM wallet_graph.wallet_edges WHERE chain = %s",
            (chain,),
        )
        if latest and latest.get("t"):
            _t = latest["t"]
            if hasattr(_t, "tzinfo") and _t.tzinfo is None:
                _t = _t.replace(tzinfo=timezone.utc)
            table_age_hours = (
                datetime.now(timezone.utc) - _t
            ).total_seconds() / 3600
    except Exception as e:
        logger.warning(f"[edge_builder_scheduled] {chain} freshness check failed: {e}")

    if table_age_hours < _EDGE_BUILDER_FRESHNESS_HOURS:
        payload = {
            "status": "skipped_fresh",
            "chain": chain,
            "table_age_hours": round(table_age_hours, 2),
        }
        try:
            await asyncio.to_thread(attest_state, "edges", [payload])
        except Exception as e:
            logger.warning(f"[edge_builder_scheduled] {chain} skipped-fresh attest failed: {e}")
        return payload

    try:
        result = await run_edge_builder(max_wallets=200, priority="value", chain=chain)
        payload = {
            "status": "ran",
            "chain": chain,
            "wallets_processed": result.get("wallets_processed", 0),
            "edges_upserted": result.get("total_edges_created", 0),
            "transfers_processed": result.get("total_transfers", 0),
            "table_age_hours": round(table_age_hours, 2),
        }
        # Bug A fix: attest ALWAYS in `ran` branch.
        # Bug B fix: NO 3rd positional — entity_id stays NULL.
        try:
            await asyncio.to_thread(attest_state, "edges", [payload])
        except Exception as e:
            logger.warning(f"[edge_builder_scheduled] {chain} ran-attest failed: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="indexer_run_edge_builder_attestation_failure",
                    error_message=str(e)[:500],
                    cycle_phase="wallet_edges",
                )
            except Exception:
                pass

        # v9.12 F3 (#224): coherence assertion. After a `ran` cycle attests,
        # the latest state_attestations.cycle_timestamp for domain='edges'
        # and wallet_graph.edge_build_status.MAX(last_built_at) should be
        # close together (build_edges_for_wallet upserts last_built_at=NOW(),
        # then we attest immediately). A wide gap means one of two things:
        #   (a) some other scheduler is writing edge_build_status without
        #       calling the wrapper (i.e. another bare run_edge_builder
        #       caller we missed), OR
        #   (b) attestation succeeded but edge_build_status didn't advance
        #       (no wallets processed this cycle).
        # Either case is a coherence violation worth surfacing.
        try:
            await _assert_edges_coherence(chain)
        except Exception as e:
            logger.warning(f"[edge_builder_scheduled] {chain} coherence check raised: {e}")
        return payload
    except Exception as e:
        logger.warning(f"[edge_builder_scheduled] {chain} run failed: {e}")
        payload = {"status": "error", "chain": chain, "error": str(e)[:200]}
        try:
            await asyncio.to_thread(attest_state, "edges", [payload])
        except Exception:
            pass
        return payload


# =============================================================================
# Coherence assertion — v9.12 F3 (#224)
# =============================================================================

# Tolerance: the wrapper attests within seconds of the final upsert in
# build_edges_for_wallet, so anything beyond 5 minutes is suspicious.
_EDGES_COHERENCE_TOLERANCE_SECONDS = 300


async def _assert_edges_coherence(chain: str) -> None:
    """Coherence check: latest state_attestations.cycle_timestamp for
    domain='edges' must be within 5 minutes of MAX(last_built_at) in
    wallet_graph.edge_build_status. Mismatch implies a sibling scheduler
    is bypassing the wrapper or attestation/upsert went out of sync.

    Records a cycle_error with cycle_phase='edges_coherence' on mismatch.
    Never raises.
    """
    try:
        attest_row = await fetch_one_async(
            """
            SELECT cycle_timestamp
            FROM state_attestations
            WHERE domain = 'edges' AND entity_id IS NULL
            ORDER BY cycle_timestamp DESC
            LIMIT 1
            """
        )
        build_row = await fetch_one_async(
            "SELECT MAX(last_built_at) AS latest FROM wallet_graph.edge_build_status"
        )
    except Exception as e:
        logger.warning(f"[edges_coherence] {chain} query failed: {e}")
        return

    attest_ts = attest_row.get("cycle_timestamp") if attest_row else None
    build_ts = build_row.get("latest") if build_row else None

    if attest_ts is None or build_ts is None:
        # Either side empty — not a coherence violation per se, but worth
        # noting once at info level. The post-deploy halt criteria is
        # explicitly checking that attest_ts becomes non-null within 24h.
        logger.info(
            f"[edges_coherence] {chain} skipped: "
            f"attest_ts={attest_ts!r} build_ts={build_ts!r}"
        )
        return

    # Normalize to aware UTC for safe subtraction.
    if hasattr(attest_ts, "tzinfo") and attest_ts.tzinfo is None:
        attest_ts = attest_ts.replace(tzinfo=timezone.utc)
    if hasattr(build_ts, "tzinfo") and build_ts.tzinfo is None:
        build_ts = build_ts.replace(tzinfo=timezone.utc)

    delta = abs((attest_ts - build_ts).total_seconds())
    if delta > _EDGES_COHERENCE_TOLERANCE_SECONDS:
        msg = (
            f"edges coherence drift: chain={chain} "
            f"attest_ts={attest_ts.isoformat()} "
            f"build_ts={build_ts.isoformat()} "
            f"delta_seconds={delta:.0f} "
            f"tolerance={_EDGES_COHERENCE_TOLERANCE_SECONDS}"
        )
        logger.warning(f"[edges_coherence] {msg}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="edges_coherence_drift",
                error_message=msg[:500],
                cycle_phase="edges_coherence",
            )
        except Exception as e:
            logger.warning(f"[edges_coherence] {chain} cycle_error record failed: {e}")
    else:
        logger.info(
            f"[edges_coherence] {chain} OK: delta_seconds={delta:.0f} "
            f"(tolerance={_EDGES_COHERENCE_TOLERANCE_SECONDS})"
        )


# =============================================================================
# Edge Decay — recalculate weights with time-decay multiplier
# =============================================================================

def decay_edges() -> dict:
    """
    Recalculate edge weights using a time-decay multiplier.

    decay_factor = max(0.1, 1.0 - (days_since_last_transfer / 180))
    new_weight = log10(total_value_usd + 1) * ln(transfer_count + 1) * decay_factor

    Skips edges with last_transfer_at within the last day (fresh edges).
    """
    result = execute(
        """
        UPDATE wallet_graph.wallet_edges
        SET
            weight = GREATEST(0.01,
                log(total_value_usd + 1)
                * ln(transfer_count + 1)
                * GREATEST(0.1, 1.0 - (EXTRACT(EPOCH FROM (NOW() - last_transfer_at)) / 86400.0 / 180.0))
            ),
            updated_at = NOW()
        WHERE last_transfer_at < NOW() - INTERVAL '1 day'
          AND last_transfer_at IS NOT NULL
        """,
    )

    # Count how many were updated
    count_row = fetch_one(
        """
        SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges
        WHERE updated_at > NOW() - INTERVAL '10 seconds'
        """
    )
    updated = count_row["cnt"] if count_row else 0
    logger.info(f"Edge decay: {updated} edges recalculated")
    return {"edges_decayed": updated}


# =============================================================================
# Edge Pruning — archive edges older than 180 days
# =============================================================================

def prune_stale_edges() -> dict:
    """
    Move edges with last_transfer_at older than 180 days to archive table.
    Returns count of edges archived.
    """
    # Count before archiving
    count_row = fetch_one(
        """
        SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges
        WHERE last_transfer_at < NOW() - INTERVAL '180 days'
        """
    )
    to_archive = count_row["cnt"] if count_row else 0

    if to_archive == 0:
        logger.info("Edge pruning: no stale edges to archive")
        return {"edges_archived": 0, "edges_remaining": 0}

    # Archive: copy to archive table
    execute(
        """
        INSERT INTO wallet_graph.wallet_edges_archive
            (id, from_address, to_address, transfer_count, total_value_usd,
             first_transfer_at, last_transfer_at, weight, tokens_transferred,
             created_at, updated_at)
        SELECT id, from_address, to_address, transfer_count, total_value_usd,
               first_transfer_at, last_transfer_at, weight, tokens_transferred,
               created_at, updated_at
        FROM wallet_graph.wallet_edges
        WHERE last_transfer_at < NOW() - INTERVAL '180 days'
        ON CONFLICT DO NOTHING
        """,
    )

    # Delete from live table
    execute(
        """
        DELETE FROM wallet_graph.wallet_edges
        WHERE last_transfer_at < NOW() - INTERVAL '180 days'
        """,
    )

    remaining_row = fetch_one("SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges")
    remaining = remaining_row["cnt"] if remaining_row else 0

    logger.info(f"Edge pruning: {to_archive} edges archived, {remaining} remaining")
    return {"edges_archived": to_archive, "edges_remaining": remaining}


# =============================================================================
# Sprint 3 background loop — high-throughput edge building
# =============================================================================

EDGE_BUILDER_BATCH_SIZE = 2000
EDGE_BUILDER_ETHERSCAN_CAP = 120_000


async def _get_etherscan_24h_usage() -> int:
    try:
        row = await fetch_one_async("""
            SELECT SUM(total_calls) AS total FROM api_usage_hourly
            WHERE provider = 'etherscan' AND hour > NOW() - INTERVAL '24 hours'
        """)
        return int(row["total"]) if row and row.get("total") else 0
    except Exception:
        return 0


async def edge_builder_background_loop():
    """Independent background loop for Sprint 3 edge graph density."""
    logger.error("[edge_builder_bg] background loop started")
    await asyncio.sleep(240)  # stagger behind other Phase 2 loops
    consecutive_db_failures = 0

    while True:
        try:
            logger.error("[edge_builder_bg] loop tick")

            usage = await _get_etherscan_24h_usage()
            if usage > EDGE_BUILDER_ETHERSCAN_CAP:
                logger.error(f"[edge_builder_bg] PAUSED: Etherscan 24h usage {usage:,}/{EDGE_BUILDER_ETHERSCAN_CAP:,}")
                await asyncio.sleep(3600)
                continue

            # Check how many wallets are scannable
            scannable = await fetch_one_async("""
                SELECT COUNT(*) AS cnt FROM wallet_graph.wallets w
                LEFT JOIN wallet_graph.edge_build_status e
                    ON w.address = e.wallet_address AND e.chain = 'ethereum'
                WHERE e.wallet_address IS NULL
                   OR e.last_built_at < NOW() - INTERVAL '24 hours'
            """)
            scannable_count = int(scannable["cnt"]) if scannable else 0

            if scannable_count == 0:
                logger.error("[edge_builder_bg] no wallets need scanning, sleeping 1h")
                await asyncio.sleep(3600)
                continue

            batch = min(EDGE_BUILDER_BATCH_SIZE, scannable_count)
            logger.error(f"[edge_builder_bg] {scannable_count} wallets need scanning, running batch of {batch}")

            # v9.12 F3 (#224): hoist sibling scheduler to module-canonical wrapper.
            # The wrapper's freshness gate (10h via MAX(updated_at) on wallet_edges)
            # may short-circuit to skipped_fresh; that's fine — the loop already
            # tolerates idle cycles. The win is that state_attestations for
            # domain='edges' now fires from whichever sibling wins the race.
            #
            # NOTE: the batch-size hint (EDGE_BUILDER_BATCH_SIZE=2000) is dropped
            # in favor of the wrapper's internal max_wallets=200. The loop's
            # `scannable` guard is preserved upstream so we still bail when
            # there's nothing to do.
            result = await run_edge_builder_scheduled("ethereum")

            logger.error(
                f"[edge_builder_bg] BATCH SUMMARY: "
                f"status={result.get('status', 'unknown')}, "
                f"wallets={result.get('wallets_processed', 0)}, "
                f"new_edges={result.get('edges_upserted', 0)}, "
                f"transfers={result.get('transfers_processed', 0)}"
            )

            # Short sleep before next batch — continuous while there are wallets to scan
            await asyncio.sleep(300)
            consecutive_db_failures = 0

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            consecutive_db_failures += 1
            if consecutive_db_failures >= 10:
                logger.critical(f"[edge_builder_bg] {consecutive_db_failures} consecutive DB failures — exiting")
                raise SystemExit(1)
            elif consecutive_db_failures >= 3:
                logger.error(f"[edge_builder_bg] DB failure #{consecutive_db_failures}: {e}")
            else:
                logger.warning(f"[edge_builder_bg] DB failure (will retry): {e}")
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"[edge_builder_bg] ERROR: {type(e).__name__}: {e}")
            await asyncio.sleep(600)
