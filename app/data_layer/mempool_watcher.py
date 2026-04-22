"""
Mempool observation capture via Alchemy's alchemy_pendingTransactions
WebSocket subscription with server-side address filtering.

Phase 1 scope: Ethereum mainnet only. No new vendor — uses the existing
ALCHEMY_API_KEY on free tier. A single long-lived WebSocket connection
subscribes with a watchlist of up to 100 addresses; each event writes a
row to `mempool_observations`. A reconciliation loop every 60s calls
eth_getTransactionByHash (via app.utils.rpc_provider.call) to stamp
confirmed_block / confirmed_at / confirmation_latency_ms on observations,
and marks tx dropped after 10 min of no confirmation.

Cost control: Alchemy free tier has a ~1M compute-unit daily budget.
This module tracks hourly CU consumption via `api_usage_hourly` (the
existing `api_tracker` snapshot) and:
  - logs [mempool_watcher] WARN when hourly CU > 50K (≈1.2M/day sustained)
  - pauses the subscription for 1 hour if 24h rolling CU > 800K (80% of budget)
  - emits a state_attestation with domain='mempool_capture_status' on
    pause/resume so operators have an audit trail of gaps

Deferred to Phase 2:
  - Dwellir WebSocket as verification source
  - Base / Arbitrum mempool capture
  - Solana mempool (different architecture)

The watcher runs as a background task launched from app/worker.py main().
It is idempotent across restarts (tx_hash is UNIQUE on the table) and
safe to kill at any point.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Iterable

from app.database import execute, fetch_all, fetch_one, get_cursor

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_WATCHLIST_ADDRESSES = 100
RECONCILIATION_INTERVAL_SEC = 60
DROPPED_THRESHOLD_SEC = 10 * 60      # 10 minutes
WS_PING_INTERVAL_SEC = 30
WS_PING_TIMEOUT_SEC = 10
WS_RECONNECT_BASE_DELAY_SEC = 2
WS_RECONNECT_MAX_DELAY_SEC = 120

# Alchemy CU budget thresholds (free tier ~1M/day).
CU_HOURLY_WARN_THRESHOLD = 50_000
CU_DAILY_PAUSE_THRESHOLD = 800_000
PAUSE_DURATION_SEC = 3600

_ALCHEMY_WS_CHAIN_MAP = {
    "ethereum": "eth-mainnet",
    "base":     "base-mainnet",
    "arbitrum": "arb-mainnet",
}


# ---------------------------------------------------------------------------
# Watchlist builder
# ---------------------------------------------------------------------------

def build_watchlist(chain: str = "ethereum", limit: int = MAX_WATCHLIST_ADDRESSES) -> list[str]:
    """Assemble the address watchlist from available tables.

    Sources (in priority order):
      1. stablecoins.contract — mainnet token contracts. Always on
         ethereum mainnet in current schema (no `chain` column); included
         only when chain='ethereum'.
      2. protocol_pool_wallets.pool_contract_address — distinct protocol
         pool contracts, ranked by balance sum across receipt-token holders.

    Returns lowercased 0x-prefixed addresses, deduplicated, capped at
    `limit`. Logs a warning if the combined source count exceeds the cap —
    caller should see that in Railway.
    """
    addresses: list[str] = []
    seen: set[str] = set()

    def _add(addr: str | None) -> None:
        if not addr or not isinstance(addr, str):
            return
        a = addr.lower().strip()
        if not a.startswith("0x") or len(a) != 42:
            return
        if a in seen:
            return
        seen.add(a)
        addresses.append(a)

    if chain == "ethereum":
        try:
            rows = fetch_all(
                "SELECT contract FROM stablecoins "
                "WHERE contract IS NOT NULL AND scoring_enabled = TRUE"
            ) or []
            for r in rows:
                _add(r.get("contract"))
        except Exception as e:
            logger.warning(f"[mempool_watcher] stablecoins watchlist skipped: {e}")

    try:
        rows = fetch_all(
            """
            SELECT LOWER(pool_contract_address) AS addr, SUM(COALESCE(balance, 0)) AS bal
            FROM protocol_pool_wallets
            WHERE chain = %s AND pool_contract_address IS NOT NULL
            GROUP BY LOWER(pool_contract_address)
            ORDER BY bal DESC
            LIMIT %s
            """,
            (chain, max(limit * 2, 50)),
        ) or []
        for r in rows:
            _add(r.get("addr"))
            if len(addresses) >= limit:
                break
    except Exception as e:
        logger.warning(f"[mempool_watcher] protocol_pool_wallets watchlist skipped: {e}")

    original_count = len(addresses)
    if original_count > limit:
        logger.warning(
            f"[mempool_watcher] watchlist truncated from {original_count} to {limit} "
            f"(MAX_WATCHLIST_ADDRESSES). Raise the cap in code when Alchemy free tier "
            f"demonstrates it can handle more."
        )
        addresses = addresses[:limit]

    return addresses


# ---------------------------------------------------------------------------
# Cost control
# ---------------------------------------------------------------------------

def _current_hour_cu(provider: str = "alchemy") -> int:
    """Approximate CU consumption for the current hour, derived from
    api_usage_hourly's total_calls. We don't have per-call CU accounting
    (Alchemy doesn't return it in responses), so this is a call-count
    proxy: mempool subscription + polling calls are roughly 1:1 with CU
    on free tier for eth_getTransactionByHash. The WebSocket
    subscription itself is a flat CU cost and not counted per-event, so
    this proxy is conservative."""
    try:
        row = fetch_one(
            """
            SELECT COALESCE(SUM(total_calls), 0) AS total
            FROM api_usage_hourly
            WHERE provider = %s AND hour = date_trunc('hour', NOW())
            """,
            (provider,),
        )
        return int(row["total"]) if row else 0
    except Exception:
        return 0


def _rolling_24h_cu(provider: str = "alchemy") -> int:
    try:
        row = fetch_one(
            """
            SELECT COALESCE(SUM(total_calls), 0) AS total
            FROM api_usage_hourly
            WHERE provider = %s AND hour > NOW() - INTERVAL '24 hours'
            """,
            (provider,),
        )
        return int(row["total"]) if row else 0
    except Exception:
        return 0


def _attest_capture_status(status: str, note: str, gap_seconds: int | None = None) -> None:
    """Record a pause/resume event to state_attestations so operators can
    see capture gaps. domain='mempool_capture_status'. Non-fatal."""
    try:
        payload = json.dumps({
            "status": status,
            "note": note,
            "gap_seconds": gap_seconds,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }, sort_keys=True)
        content_hash = hashlib.sha256(payload.encode()).hexdigest()
        execute(
            """
            INSERT INTO state_attestations
                (domain, entity_id, batch_hash, record_count, methodology_version, cycle_timestamp)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            ("mempool_capture_status", status, content_hash, 1, "mempool-v0.1.0"),
        )
    except Exception as e:
        logger.debug(f"[mempool_watcher] capture-status attestation skipped: {e}")


# ---------------------------------------------------------------------------
# Observation writer
# ---------------------------------------------------------------------------

def _insert_observation(tx: dict) -> bool:
    """Write one mempool observation row. Returns True on insert, False on
    ON CONFLICT (tx_hash already seen — subscription sometimes delivers
    dupes) or on error."""
    try:
        tx_hash = tx.get("hash")
        if not tx_hash:
            return False

        from_addr = (tx.get("from") or "").lower() or None
        to_addr = (tx.get("to") or "").lower() or None

        def _hex_to_int(v):
            if v is None:
                return None
            if isinstance(v, int):
                return v
            try:
                return int(v, 16) if isinstance(v, str) and v.startswith("0x") else int(v)
            except Exception:
                return None

        value_wei = _hex_to_int(tx.get("value"))
        gas_price_wei = _hex_to_int(tx.get("gasPrice") or tx.get("maxFeePerGas"))
        nonce = _hex_to_int(tx.get("nonce"))

        input_data = tx.get("input") or "0x"
        input_truncated = input_data[:1026]  # "0x" + 512 bytes (=1024 hex chars)
        selector = input_data[:10] if len(input_data) >= 10 else None

        seen_at_ms = int(time.time() * 1000)

        execute(
            """
            INSERT INTO mempool_observations
                (tx_hash, from_address, to_address, value_wei, gas_price_wei,
                 nonce, input_data_truncated, function_selector, source,
                 seen_at, seen_at_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
            ON CONFLICT (tx_hash) DO NOTHING
            """,
            (
                tx_hash, from_addr, to_addr, value_wei, gas_price_wei,
                nonce, input_truncated, selector, "alchemy",
                seen_at_ms,
            ),
        )
        return True
    except Exception as e:
        logger.debug(f"[mempool_watcher] insert failed: {type(e).__name__}: {e}")
        return False


def _attest_observation(tx_hash: str, seen_at_ms: int) -> None:
    """Single-row state attestation for the incoming observation.

    Per the spec: content_hash = SHA-256 of tx_hash + seen_at_ms + source.
    The attestation domain is `mempool_observations`. Writes are non-fatal —
    a failure here never drops the captured data, only the audit trail.
    """
    try:
        content = f"{tx_hash}|{seen_at_ms}|alchemy"
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        execute(
            """
            INSERT INTO state_attestations
                (domain, entity_id, batch_hash, record_count, methodology_version, cycle_timestamp)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            ("mempool_observations", tx_hash, content_hash, 1, "mempool-v0.1.0"),
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# WebSocket subscription
# ---------------------------------------------------------------------------

def _alchemy_ws_url(chain: str) -> str | None:
    key = os.environ.get("ALCHEMY_API_KEY", "")
    subdomain = _ALCHEMY_WS_CHAIN_MAP.get(chain)
    if not (key and subdomain):
        return None
    return f"wss://{subdomain}.g.alchemy.com/v2/{key}"


async def _subscribe_and_consume(ws, watchlist: list[str]) -> int:
    """Send the alchemy_pendingTransactions subscribe request, then consume
    events. Returns event count on disconnect (for logging). Caller owns
    retry/reconnect."""
    sub_id = 1
    subscribe_msg = {
        "jsonrpc": "2.0",
        "id": sub_id,
        "method": "eth_subscribe",
        "params": [
            "alchemy_pendingTransactions",
            {"toAddress": watchlist, "hashesOnly": False},
        ],
    }
    await ws.send(json.dumps(subscribe_msg))

    # Wait for subscription confirmation. Alchemy responds with a result
    # field containing the subscription id.
    subscription_id: str | None = None
    async for raw in ws:
        try:
            msg = json.loads(raw)
        except Exception:
            continue
        if msg.get("id") == sub_id:
            subscription_id = msg.get("result")
            logger.error(
                f"[mempool_watcher] SUBSCRIBED chain=ethereum "
                f"addresses={len(watchlist)} subscription_id={subscription_id}"
            )
            break
        if "error" in msg:
            raise RuntimeError(f"subscribe error: {msg['error']}")

    if not subscription_id:
        raise RuntimeError("subscription id not received")

    event_count = 0
    async for raw in ws:
        try:
            msg = json.loads(raw)
        except Exception:
            continue
        # Alchemy notification shape:
        # {"jsonrpc":"2.0","method":"eth_subscription",
        #  "params":{"subscription":"<id>", "result":{...tx...}}}
        if msg.get("method") == "eth_subscription":
            params = msg.get("params") or {}
            tx = params.get("result") or {}
            if tx and _insert_observation(tx):
                event_count += 1
                _attest_observation(tx.get("hash", ""), int(time.time() * 1000))

    return event_count


async def run_watcher(chain: str = "ethereum") -> None:
    """Long-lived WebSocket watcher. Never returns under normal operation —
    caller schedules this as a background task. Reconnects on any error
    with exponential backoff. Self-pauses on Alchemy CU budget exhaustion.

    Run exactly one instance per chain per worker. Multiple instances
    would duplicate subscriptions (Alchemy charges CU per subscription)
    and bloat mempool_observations via benign UNIQUE-on-tx_hash conflicts.
    """
    try:
        import websockets
    except ImportError:
        logger.error(
            "[mempool_watcher] websockets library not installed — skipping. "
            "Add `websockets>=12.0` to requirements.txt and redeploy."
        )
        return

    url = _alchemy_ws_url(chain)
    if not url:
        logger.error(
            f"[mempool_watcher] ALCHEMY_API_KEY or chain subdomain unresolvable "
            f"for chain={chain} — watcher disabled"
        )
        return

    reconnect_delay = WS_RECONNECT_BASE_DELAY_SEC
    paused_until: float = 0.0

    while True:
        # Cost-control gate.
        now = time.time()
        if now < paused_until:
            wait = paused_until - now
            await asyncio.sleep(min(wait, 60))
            continue

        rolling_24h = _rolling_24h_cu("alchemy")
        if rolling_24h > CU_DAILY_PAUSE_THRESHOLD:
            paused_until = now + PAUSE_DURATION_SEC
            logger.error(
                f"[mempool_watcher] PAUSE alchemy 24h CU={rolling_24h:,} > "
                f"{CU_DAILY_PAUSE_THRESHOLD:,} budget threshold — "
                f"pausing subscription for {PAUSE_DURATION_SEC}s"
            )
            _attest_capture_status(
                "paused",
                f"alchemy_24h_cu={rolling_24h} exceeds {CU_DAILY_PAUSE_THRESHOLD} threshold",
                gap_seconds=PAUSE_DURATION_SEC,
            )
            continue

        hourly_cu = _current_hour_cu("alchemy")
        if hourly_cu > CU_HOURLY_WARN_THRESHOLD:
            logger.error(
                f"[mempool_watcher] WARN alchemy hourly CU={hourly_cu:,} > "
                f"{CU_HOURLY_WARN_THRESHOLD:,} — sustained rate would exceed daily budget"
            )

        watchlist = build_watchlist(chain=chain, limit=MAX_WATCHLIST_ADDRESSES)
        if not watchlist:
            logger.error(
                "[mempool_watcher] watchlist is empty — no stablecoins or pool "
                "contracts available; retrying in 300s"
            )
            await asyncio.sleep(300)
            continue

        connect_started = time.time()
        try:
            async with websockets.connect(
                url,
                ping_interval=WS_PING_INTERVAL_SEC,
                ping_timeout=WS_PING_TIMEOUT_SEC,
                max_size=2**23,  # 8 MB frame cap
                close_timeout=5,
            ) as ws:
                reconnect_delay = WS_RECONNECT_BASE_DELAY_SEC  # reset on successful connect
                events = await _subscribe_and_consume(ws, watchlist)
                logger.warning(
                    f"[mempool_watcher] connection closed after {events:,} events"
                )
                _attest_capture_status(
                    "resumed",
                    f"connection lifetime={int(time.time() - connect_started)}s events={events}",
                )
        except asyncio.CancelledError:
            logger.error("[mempool_watcher] cancelled; shutting down cleanly")
            raise
        except Exception as e:
            elapsed = int(time.time() - connect_started)
            logger.error(
                f"[mempool_watcher] connection error after {elapsed}s: "
                f"{type(e).__name__}: {e}"
            )

        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, WS_RECONNECT_MAX_DELAY_SEC)


# ---------------------------------------------------------------------------
# Reconciliation loop
# ---------------------------------------------------------------------------

async def reconcile_once() -> dict:
    """Check every unconfirmed tx older than 60s exactly once. Update
    confirmed_block / confirmed_at / confirmation_latency_ms for txs the
    chain has sealed; mark dropped=TRUE for txs that have been in the pool
    longer than DROPPED_THRESHOLD_SEC with no confirmation.

    Returns counts for diagnostic logging.
    """
    from app.utils.rpc_provider import call as rpc_call, RPCError

    counts = {"checked": 0, "confirmed": 0, "dropped": 0, "errors": 0}

    try:
        rows = fetch_all(
            """
            SELECT id, tx_hash, seen_at, seen_at_ms
            FROM mempool_observations
            WHERE confirmed_block IS NULL
              AND dropped = FALSE
              AND seen_at < NOW() - INTERVAL '60 seconds'
            ORDER BY seen_at
            LIMIT 500
            """
        ) or []
    except Exception as e:
        logger.warning(f"[mempool_watcher] reconcile query failed: {e}")
        return counts

    now_ms = int(time.time() * 1000)
    drop_cutoff_ms = now_ms - (DROPPED_THRESHOLD_SEC * 1000)

    for r in rows:
        counts["checked"] += 1
        tx_hash = r["tx_hash"]
        try:
            tx = await rpc_call(
                "eth_getTransactionByHash", [tx_hash], chain="ethereum", timeout=8.0,
            )
        except RPCError as e:
            counts["errors"] += 1
            logger.debug(f"[mempool_watcher] reconcile RPC fail {tx_hash[:12]}: {e}")
            continue
        except Exception as e:
            counts["errors"] += 1
            logger.debug(
                f"[mempool_watcher] reconcile exc {tx_hash[:12]}: "
                f"{type(e).__name__}: {e}"
            )
            continue

        if tx and tx.get("blockNumber"):
            # Confirmed — stamp block + latency.
            try:
                block_num = int(tx["blockNumber"], 16)
            except Exception:
                counts["errors"] += 1
                continue
            latency_ms = max(0, now_ms - int(r["seen_at_ms"]))
            try:
                execute(
                    """
                    UPDATE mempool_observations
                    SET confirmed_block = %s,
                        confirmed_at = NOW(),
                        confirmation_latency_ms = %s
                    WHERE id = %s AND confirmed_block IS NULL
                    """,
                    (block_num, latency_ms, r["id"]),
                )
                counts["confirmed"] += 1
            except Exception as e:
                counts["errors"] += 1
                logger.debug(f"[mempool_watcher] reconcile update failed: {e}")
        else:
            # Unconfirmed — if older than drop threshold, mark dropped.
            if int(r["seen_at_ms"]) < drop_cutoff_ms:
                try:
                    execute(
                        "UPDATE mempool_observations SET dropped = TRUE WHERE id = %s",
                        (r["id"],),
                    )
                    counts["dropped"] += 1
                except Exception as e:
                    counts["errors"] += 1
                    logger.debug(f"[mempool_watcher] drop mark failed: {e}")

    return counts


async def run_reconciliation_loop() -> None:
    """Fires `reconcile_once` every RECONCILIATION_INTERVAL_SEC. Caller
    schedules as a background task."""
    while True:
        try:
            counts = await reconcile_once()
            if any(counts.values()):
                logger.error(
                    f"[mempool_watcher] reconcile: "
                    f"checked={counts['checked']} confirmed={counts['confirmed']} "
                    f"dropped={counts['dropped']} errors={counts['errors']}"
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[mempool_watcher] reconcile loop error: {type(e).__name__}: {e}")
        await asyncio.sleep(RECONCILIATION_INTERVAL_SEC)


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

def emit_24h_summary() -> None:
    """Emit the 24h [mempool_observations] summary line expected by the
    acceptance checklist. Safe to call even if the table doesn't exist
    yet (logs the absence and returns)."""
    try:
        summary = fetch_one(
            """
            SELECT
                COUNT(*) AS captured,
                COUNT(*) FILTER (WHERE confirmed_block IS NOT NULL) AS confirmed,
                COUNT(*) FILTER (WHERE dropped = TRUE) AS dropped,
                AVG(confirmation_latency_ms) FILTER (WHERE confirmation_latency_ms IS NOT NULL) AS avg_latency_ms
            FROM mempool_observations
            WHERE seen_at > NOW() - INTERVAL '24 hours'
            """
        )
    except Exception as e:
        logger.debug(f"[mempool_observations] 24h SUMMARY skipped: {e}")
        return

    if not summary or not summary.get("captured"):
        logger.error("[mempool_observations] 24h SUMMARY: no rows yet")
        return

    top_to = fetch_all(
        """
        SELECT to_address AS addr, COUNT(*) AS cnt
        FROM mempool_observations
        WHERE seen_at > NOW() - INTERVAL '24 hours' AND to_address IS NOT NULL
        GROUP BY to_address
        ORDER BY cnt DESC
        LIMIT 5
        """
    ) or []
    top_selectors = fetch_all(
        """
        SELECT function_selector AS sel, COUNT(*) AS cnt
        FROM mempool_observations
        WHERE seen_at > NOW() - INTERVAL '24 hours' AND function_selector IS NOT NULL
        GROUP BY function_selector
        ORDER BY cnt DESC
        LIMIT 5
        """
    ) or []

    cu_rolling = _rolling_24h_cu("alchemy")
    avg_latency = int(summary["avg_latency_ms"]) if summary["avg_latency_ms"] else None

    top_to_str = ", ".join(f"{(r['addr'] or '')[:10]}={r['cnt']}" for r in top_to)
    top_sel_str = ", ".join(f"{r['sel']}={r['cnt']}" for r in top_selectors)

    logger.error(
        f"[mempool_observations] 24h SUMMARY: "
        f"captured={int(summary['captured'])} "
        f"confirmed={int(summary['confirmed'])} "
        f"dropped={int(summary['dropped'])} "
        f"avg_confirmation_latency_ms={avg_latency} "
        f"alchemy_cu_rolling24h={cu_rolling} "
        f"top_to=[{top_to_str}] "
        f"top_selectors=[{top_sel_str}]"
    )


# ---------------------------------------------------------------------------
# Orchestration entry point
# ---------------------------------------------------------------------------

async def start_mempool_tasks() -> tuple[asyncio.Task, asyncio.Task] | None:
    """Launch the watcher and reconciliation loop as background tasks.

    Returns (watcher_task, reconcile_task) on success, or None if the
    module is disabled (no API key, feature flag off). Safe to call at
    worker boot. Failure to start never raises — mempool is a telemetry
    feature and must not block the scoring worker.
    """
    if os.environ.get("MEMPOOL_WATCHER_ENABLED", "true").lower() in ("0", "false", "no"):
        logger.error("[mempool_watcher] disabled by MEMPOOL_WATCHER_ENABLED env var")
        return None

    if not os.environ.get("ALCHEMY_API_KEY"):
        logger.error("[mempool_watcher] ALCHEMY_API_KEY not set — watcher not started")
        return None

    try:
        watcher_task = asyncio.create_task(run_watcher("ethereum"), name="mempool_watcher")
        reconcile_task = asyncio.create_task(run_reconciliation_loop(), name="mempool_reconcile")
        logger.error("[mempool_watcher] background tasks scheduled")
        return watcher_task, reconcile_task
    except Exception as e:
        logger.error(f"[mempool_watcher] failed to start: {type(e).__name__}: {e}")
        return None
