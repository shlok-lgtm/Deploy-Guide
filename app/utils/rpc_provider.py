"""
RPC provider abstraction — Alchemy primary, Dwellir secondary.

Introduced by the Dwellir integration PR. Wraps JSON-RPC calls for every
chain we touch and routes each method to the provider best suited to
serve it, with automatic fallback to the alternate provider on 429/5xx
or "method not found" errors. Every call is tracked in
`rpc_provider_usage` so we can see usage, fallback rates, and cap risk
before committing to additional pipelines.

Capability-probe-first: `probe_rpc_capabilities()` runs once at worker
startup and records what the Dwellir free tier actually supports in
`rpc_capabilities`. Results inform LLL pipeline decisions — do NOT build
pipelines that assume trace/debug availability until the probe confirms.

Environment variables:
    ALCHEMY_API_KEY     — existing; used for all chains when present
    DWELLIR_API_KEY     — new; if absent Dwellir routing is disabled and
                          every call pins to Alchemy with a warning logged
                          once per run
    DWELLIR_ETH_URL     — optional full URL (includes api key). If set
                          used directly; otherwise composed from
                          DWELLIR_API_KEY.
    DWELLIR_BASE_URL    — same for base chain

Public surface:
    RPCProvider         — namespace class carrying constants + router
    call(method, params, chain) — async; returns the JSON-RPC `result`
                                  field or raises RuntimeError with the
                                  last error from whichever provider(s)
                                  tried
    probe_rpc_capabilities(chain="ethereum") — async; runs once at boot,
                                  persists to rpc_capabilities
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Iterable

import httpx

from app.database import execute, fetch_one

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class RPCProvider:
    """Namespace for provider constants + router config."""

    ALCHEMY = "alchemy"
    DWELLIR = "dwellir"

    # Methods for which Dwellir is the declared primary. Alchemy's free tier
    # either doesn't support these or restricts them; Dwellir's free tier
    # claims support (verified at runtime by probe_rpc_capabilities).
    DWELLIR_PREFERRED: frozenset[str] = frozenset({
        "debug_traceTransaction",
        "debug_traceCall",
        "debug_traceBlockByNumber",
        "debug_traceBlockByHash",
        "trace_transaction",
        "trace_block",
        "trace_call",
        "trace_filter",
        "trace_get",
        "trace_replayBlockTransactions",
        "trace_replayTransaction",
    })

    # HTTP status codes that should trigger a fallback attempt on the
    # alternate provider before giving up.
    FALLBACK_STATUSES: frozenset[int] = frozenset({429, 500, 502, 503, 504})

    # How far behind head we consider a block "archive-historical". eth_call
    # reads against such blocks are routed to Dwellir, where archive is
    # unrestricted; Alchemy free tier rate-limits or 400s on older blocks.
    ARCHIVE_BLOCK_DEPTH = 128

    # Alchemy chain → subdomain map.
    ALCHEMY_CHAIN_MAP: dict[str, str] = {
        "ethereum": "eth-mainnet",
        "base": "base-mainnet",
        "arbitrum": "arb-mainnet",
    }


# ---------------------------------------------------------------------------
# URL resolution
# ---------------------------------------------------------------------------

def _alchemy_url(chain: str) -> str | None:
    key = os.environ.get("ALCHEMY_API_KEY", "")
    subdomain = RPCProvider.ALCHEMY_CHAIN_MAP.get(chain)
    if not (key and subdomain):
        return None
    return f"https://{subdomain}.g.alchemy.com/v2/{key}"


def _dwellir_url(chain: str) -> str | None:
    # Prefer full URL override if operator supplied it (keeps the api key
    # out of URL construction logic). Otherwise compose from DWELLIR_API_KEY.
    env_full = {
        "ethereum": "DWELLIR_ETH_URL",
        "base":     "DWELLIR_BASE_URL",
    }.get(chain)
    if env_full:
        full = os.environ.get(env_full)
        if full:
            return full

    key = os.environ.get("DWELLIR_API_KEY", "")
    if not key:
        return None
    chain_segment = {
        "ethereum": "api-ethereum-mainnet",
        "base":     "api-base-mainnet",
    }.get(chain)
    if not chain_segment:
        return None
    return f"https://{chain_segment}.n.dwellir.com/{key}"


def _provider_url(provider: str, chain: str) -> str | None:
    if provider == RPCProvider.ALCHEMY:
        return _alchemy_url(chain)
    if provider == RPCProvider.DWELLIR:
        return _dwellir_url(chain)
    return None


# ---------------------------------------------------------------------------
# Cached head-block lookup (per-chain, 60s TTL)
# ---------------------------------------------------------------------------

_HEAD_BLOCK_CACHE: dict[str, tuple[float, int]] = {}
_HEAD_BLOCK_TTL_SECONDS = 60.0


async def _get_cached_head_block(chain: str, client: httpx.AsyncClient) -> int | None:
    now = time.monotonic()
    cached = _HEAD_BLOCK_CACHE.get(chain)
    if cached and (now - cached[0]) < _HEAD_BLOCK_TTL_SECONDS:
        return cached[1]
    # Fetch via Alchemy (always available if we're routing anything). Skip
    # failure is non-fatal — we fall back to Alchemy routing for eth_call.
    url = _alchemy_url(chain) or _dwellir_url(chain)
    if not url:
        return None
    try:
        resp = await client.post(
            url,
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
            timeout=5,
        )
        hex_block = resp.json().get("result", "0x0")
        block_num = int(hex_block, 16)
        _HEAD_BLOCK_CACHE[chain] = (now, block_num)
        return block_num
    except Exception as e:
        logger.debug(f"[rpc] head-block fetch failed for {chain}: {e}")
        return None


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

def route(method: str, params: list, chain: str = "ethereum",
          head_block: int | None = None) -> str:
    """Return 'alchemy' or 'dwellir' based on method + params.

    Pure function; does no network I/O. The caller supplies head_block when
    archive-depth routing is needed (the async call() fetches it internally
    once per 60s).
    """
    # 1. Trace/debug methods go to Dwellir if Dwellir is configured;
    # otherwise fall through to Alchemy so the fallback chain still gets a
    # chance (it'll almost certainly fail too, but the user sees a clear
    # error rather than a silent KeyError).
    if method in RPCProvider.DWELLIR_PREFERRED and _dwellir_url(chain):
        return RPCProvider.DWELLIR

    # 2. Archive-historical eth_call — block tag older than head - 128 →
    # Dwellir. The last element of params for eth_call is the block tag
    # ('latest', 'pending', 'earliest', or a hex block number).
    if method == "eth_call" and len(params) >= 2 and head_block is not None:
        block_tag = params[1]
        if isinstance(block_tag, str) and block_tag.startswith("0x") and _dwellir_url(chain):
            try:
                block_num = int(block_tag, 16)
                if (head_block - block_num) > RPCProvider.ARCHIVE_BLOCK_DEPTH:
                    return RPCProvider.DWELLIR
            except ValueError:
                pass

    # 3. Default: Alchemy.
    return RPCProvider.ALCHEMY


# ---------------------------------------------------------------------------
# Usage tracking — hourly upserts into rpc_provider_usage
# ---------------------------------------------------------------------------

def _track(provider: str, method: str, chain: str, status: str,
           fallback_reason: str | None = None) -> None:
    """Hourly counter upsert. Non-fatal on error — tracking must never
    break an RPC call path."""
    try:
        execute(
            """
            INSERT INTO rpc_provider_usage
                (provider, method, chain, status, fallback_reason, hour, calls)
            VALUES (%s, %s, %s, %s, %s, date_trunc('hour', NOW()), 1)
            ON CONFLICT (provider, method, chain, status, hour) DO UPDATE
            SET calls = rpc_provider_usage.calls + 1,
                fallback_reason = COALESCE(EXCLUDED.fallback_reason,
                                            rpc_provider_usage.fallback_reason)
            """,
            (provider, method, chain, status, fallback_reason),
        )
    except Exception as e:
        logger.debug(f"[rpc] usage tracking skipped: {e}")


def _track_success(provider: str, method: str, chain: str) -> None:
    _track(provider, method, chain, "ok")


def _track_fallback(primary: str, fallback_provider: str, method: str,
                    chain: str, reason: str) -> None:
    _track(primary, method, chain, "fallback", fallback_reason=reason[:200])
    _track(fallback_provider, method, chain, "ok")


def _track_failure(primary: str, fallback_provider: str, method: str,
                   chain: str, reason: str) -> None:
    _track(primary, method, chain, "error", fallback_reason=reason[:200])
    _track(fallback_provider, method, chain, "error", fallback_reason=reason[:200])


# ---------------------------------------------------------------------------
# JSON-RPC primitive
# ---------------------------------------------------------------------------

class RPCError(RuntimeError):
    """Raised when a JSON-RPC call returns a non-retriable error or both
    providers failed. Carries the HTTP status and the RPC error message
    from the last attempt."""

    def __init__(self, message: str, status: int | None = None,
                 rpc_code: int | None = None):
        super().__init__(message)
        self.status = status
        self.rpc_code = rpc_code


async def _call_provider(provider: str, method: str, params: list, chain: str,
                         client: httpx.AsyncClient, timeout: float = 20.0) -> Any:
    """One-shot JSON-RPC call against a specific provider. Raises RPCError
    on HTTP non-2xx or on a JSON-RPC `error` field in the response body.
    The caller is responsible for deciding whether to fall back."""
    url = _provider_url(provider, chain)
    if not url:
        raise RPCError(f"no URL configured for provider={provider} chain={chain}")

    try:
        resp = await client.post(
            url,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=timeout,
        )
    except httpx.TimeoutException as e:
        raise RPCError(f"timeout: {e}", status=599)
    except httpx.HTTPError as e:
        raise RPCError(f"network: {e}", status=598)

    if resp.status_code >= 400:
        raise RPCError(
            f"HTTP {resp.status_code}: {resp.text[:200]}",
            status=resp.status_code,
        )

    try:
        payload = resp.json()
    except ValueError:
        raise RPCError(f"non-JSON response: {resp.text[:200]}", status=resp.status_code)

    if "error" in payload and payload["error"] is not None:
        err = payload["error"]
        err_msg = err.get("message", "") if isinstance(err, dict) else str(err)
        err_code = err.get("code") if isinstance(err, dict) else None
        raise RPCError(
            f"rpc error {err_code}: {err_msg}",
            status=resp.status_code,
            rpc_code=err_code,
        )

    return payload.get("result")


def _should_fallback(err: RPCError) -> bool:
    """Decide whether to try the alternate provider."""
    if err.status in RPCProvider.FALLBACK_STATUSES:
        return True
    msg = str(err).lower()
    if "method not found" in msg or "not supported" in msg:
        return True
    # JSON-RPC code -32601 = method not found
    if err.rpc_code == -32601:
        return True
    return False


# ---------------------------------------------------------------------------
# Public call() — async, with routing + fallback
# ---------------------------------------------------------------------------

async def call(method: str, params: list | None = None, chain: str = "ethereum",
               client: httpx.AsyncClient | None = None,
               timeout: float = 20.0) -> Any:
    """Dispatch a JSON-RPC method through the router.

    Returns the JSON-RPC `result` payload on success. On failure (both
    providers erred), raises RPCError with the last error.

    If `client` is None, a temporary httpx.AsyncClient is created for the
    duration of the call. Callers making many calls should pass a shared
    client for connection reuse.
    """
    params = params or []
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=timeout)

    try:
        head_block = None
        if method == "eth_call" and len(params) >= 2 \
                and isinstance(params[1], str) and params[1].startswith("0x"):
            head_block = await _get_cached_head_block(chain, client)

        primary = route(method, params, chain, head_block=head_block)
        primary_url = _provider_url(primary, chain)
        if not primary_url:
            # Router picked a provider we're not configured for — try the
            # other. If neither is configured we'll raise below.
            primary = (
                RPCProvider.ALCHEMY if primary == RPCProvider.DWELLIR
                else RPCProvider.DWELLIR
            )
        fallback = (
            RPCProvider.DWELLIR if primary == RPCProvider.ALCHEMY
            else RPCProvider.ALCHEMY
        )

        # Primary attempt.
        try:
            result = await _call_provider(primary, method, params, chain, client, timeout)
            _track_success(primary, method, chain)
            return result
        except RPCError as e:
            if not _should_fallback(e) or not _provider_url(fallback, chain):
                # Unrecoverable (or no fallback configured). Record and re-raise.
                _track_failure(primary, fallback, method, chain, reason=str(e))
                raise
            logger.warning(
                f"[rpc] {primary} failed for {method} on {chain} "
                f"(status={e.status} rpc_code={e.rpc_code}): {str(e)[:200]}. "
                f"Trying {fallback}."
            )
            try:
                result = await _call_provider(
                    fallback, method, params, chain, client, timeout,
                )
                _track_fallback(primary, fallback, method, chain, reason=str(e))
                return result
            except RPCError as e2:
                _track_failure(primary, fallback, method, chain, reason=str(e2))
                raise RPCError(
                    f"both providers failed: {primary}={e}; {fallback}={e2}",
                    status=e2.status,
                    rpc_code=e2.rpc_code,
                )
    finally:
        if own_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# Capability probe
# ---------------------------------------------------------------------------

async def _fetch_probe_tx_hash(client: httpx.AsyncClient, chain: str) -> str | None:
    """Grab a real, recent tx hash so debug_trace/trace probes hit a
    real block. Returns None if the chain has no transactions in the
    latest block (unlikely)."""
    url = _alchemy_url(chain) or _dwellir_url(chain)
    if not url:
        return None
    try:
        block_resp = await client.post(
            url,
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber",
                  "params": ["latest", False]},
            timeout=8,
        )
        block = block_resp.json().get("result") or {}
        txs = block.get("transactions") or []
        return txs[0] if txs else None
    except Exception as e:
        logger.debug(f"[rpc_probe] failed to fetch sample tx hash: {e}")
        return None


async def _fetch_archive_block_tag(client: httpx.AsyncClient, chain: str) -> str | None:
    """Return a block tag older than the archive threshold so the eth_call
    probe exercises the archive-read code path. Returns hex string."""
    head = await _get_cached_head_block(chain, client)
    if head is None:
        return None
    archive_block = head - (RPCProvider.ARCHIVE_BLOCK_DEPTH * 2)
    if archive_block <= 0:
        return None
    return hex(archive_block)


def _record_capability(provider: str, chain: str, method: str, status: str,
                       error_message: str | None = None,
                       sample_params: list | None = None) -> None:
    try:
        execute(
            """
            INSERT INTO rpc_capabilities
                (provider, chain, method, status, error_message, sample_params, tested_at)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
            """,
            (
                provider, chain, method, status,
                error_message[:200] if error_message else None,
                json.dumps(sample_params) if sample_params is not None else None,
            ),
        )
    except Exception as e:
        logger.debug(f"[rpc_probe] capability row skipped: {e}")


# A known USDC mainnet address — totalSupply() is a cheap, deterministic
# eth_call with zero argument encoding, safe to run at startup.
_USDC_ADDRESS_ETHEREUM = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
_USDC_TOTALSUPPLY_CALLDATA = "0x18160ddd"


async def probe_rpc_capabilities(chain: str = "ethereum") -> dict[str, str]:
    """One-shot probe at worker startup. Tests what Dwellir's free tier
    actually supports for this chain and records the result in
    `rpc_capabilities`. Never raises — probe failures are logged and
    the function returns whatever capabilities it did detect.

    Returns a dict {method_label: 'ok' | 'FAIL: <reason>'} suitable for
    structured logging.
    """
    capabilities: dict[str, str] = {}

    if not os.environ.get("DWELLIR_API_KEY") and not os.environ.get("DWELLIR_ETH_URL"):
        logger.error(
            "[rpc_probe] neither DWELLIR_API_KEY nor DWELLIR_ETH_URL set — "
            "Dwellir integration disabled; all calls will pin to Alchemy"
        )
        return capabilities

    if not _dwellir_url(chain):
        logger.error(f"[rpc_probe] Dwellir URL unresolvable for chain={chain} — skipping probe")
        return capabilities

    async with httpx.AsyncClient(timeout=20) as client:
        # Discover a real recent tx hash and a real archive block tag so
        # the probe runs against real data rather than synthetic inputs.
        recent_tx = await _fetch_probe_tx_hash(client, chain)
        archive_block = await _fetch_archive_block_tag(client, chain)

        probes: list[tuple[str, str, list]] = [
            ("eth_blockNumber (connectivity)", "eth_blockNumber", []),
            (
                "eth_call @latest (current state)",
                "eth_call",
                [{"to": _USDC_ADDRESS_ETHEREUM, "data": _USDC_TOTALSUPPLY_CALLDATA}, "latest"],
            ),
        ]
        if archive_block:
            probes.append((
                f"eth_call @{archive_block} (archive read)",
                "eth_call",
                [{"to": _USDC_ADDRESS_ETHEREUM, "data": _USDC_TOTALSUPPLY_CALLDATA}, archive_block],
            ))
        else:
            logger.error("[rpc_probe] archive block tag unavailable — skipping archive probe")
        if recent_tx:
            probes.append((
                "debug_traceTransaction (callTracer)",
                "debug_traceTransaction",
                [recent_tx, {"tracer": "callTracer"}],
            ))
            probes.append((
                "trace_transaction",
                "trace_transaction",
                [recent_tx],
            ))
        else:
            logger.error("[rpc_probe] recent tx hash unavailable — skipping trace probes")

        for label, method, params in probes:
            try:
                await _call_provider(
                    RPCProvider.DWELLIR, method, params, chain, client, timeout=15,
                )
                capabilities[label] = "ok"
                _record_capability(
                    RPCProvider.DWELLIR, chain, method, "ok", None, params,
                )
                logger.error(f"[rpc_probe] dwellir {label}: OK")
            except RPCError as e:
                err_str = str(e)[:100]
                capabilities[label] = f"FAIL: {err_str}"
                _record_capability(
                    RPCProvider.DWELLIR, chain, method, "fail", err_str, params,
                )
                logger.error(f"[rpc_probe] dwellir {label}: FAIL {err_str}")
            except Exception as e:
                err_str = f"{type(e).__name__}: {e}"[:100]
                capabilities[label] = f"FAIL: {err_str}"
                _record_capability(
                    RPCProvider.DWELLIR, chain, method, "fail", err_str, params,
                )
                logger.error(f"[rpc_probe] dwellir {label}: FAIL {err_str}")

    trace_ok = any(
        v == "ok" for k, v in capabilities.items()
        if k.startswith("debug_trace") or k.startswith("trace_")
    )
    archive_ok = any(
        v == "ok" for k, v in capabilities.items()
        if "archive" in k.lower()
    )
    logger.error(
        f"[rpc_probe] SUMMARY chain={chain} trace={trace_ok} archive={archive_ok} "
        f"(see rpc_capabilities table for per-method details)"
    )

    return capabilities


# ---------------------------------------------------------------------------
# Sync wrapper for legacy callers — lets code that's not in an async
# context still use the router. Uses asyncio.run() so it's only safe
# outside an existing event loop.
# ---------------------------------------------------------------------------

def call_sync(method: str, params: list | None = None, chain: str = "ethereum",
              timeout: float = 20.0) -> Any:
    return asyncio.run(call(method, params, chain=chain, timeout=timeout))
