"""
Basis Protocol — MCP Server (HTTP transport)
=============================================
Thin adapter: exposes 8 MCP tools that call the hub's REST API internally.
"""

import asyncio
import json
import logging
import threading
import time
from datetime import datetime, timezone

import httpx
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

_mcp_log_buffer: list[dict] = []
_mcp_log_lock = threading.Lock()


def _log_mcp_tool_call(tool_name: str, args_summary: dict, response_time_ms: int, success: bool):
    """Buffer MCP tool call for async DB insert."""
    entry = {
        "tool_name": tool_name,
        "args_summary": json.dumps(args_summary)[:500] if args_summary else None,
        "response_time_ms": response_time_ms,
        "success": success,
    }
    with _mcp_log_lock:
        _mcp_log_buffer.append(entry)
        if len(_mcp_log_buffer) >= 20:
            _flush_mcp_log()


def _flush_mcp_log():
    """Bulk insert buffered MCP tool calls."""
    with _mcp_log_lock:
        if not _mcp_log_buffer:
            return
        batch = list(_mcp_log_buffer)
        _mcp_log_buffer.clear()

    try:
        from app.database import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                for entry in batch:
                    cur.execute(
                        """INSERT INTO mcp_tool_calls (tool_name, args_summary, response_time_ms, success)
                           VALUES (%s, %s, %s, %s)""",
                        (entry["tool_name"], entry["args_summary"], entry["response_time_ms"], entry["success"])
                    )
            conn.commit()
    except Exception as e:
        logger.debug(f"MCP tool log flush failed (table may not exist yet): {e}")

mcp = FastMCP(
    name="basis-protocol",
    instructions="Verifiable risk intelligence for on-chain finance.",
    streamable_http_path="/",
    stateless_http=True,
    json_response=True,
)

API_BASE = "http://localhost:5000"


async def _api_get(path: str) -> dict:
    """Call a hub API endpoint internally."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{API_BASE}{path}", timeout=15.0)
        if resp.status_code == 404:
            return {"error": "not_found"}
        resp.raise_for_status()
        return resp.json()


@mcp.tool()
async def get_stablecoin_scores(min_grade: str = None, sort_by: str = "score_desc") -> str:
    """Get current SII scores for all scored stablecoins. Use before any decision involving stablecoins."""
    _start = time.time()
    _success = True
    try:
        data = await _api_get("/api/scores")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        _log_mcp_tool_call("get_stablecoin_scores", {"min_grade": min_grade, "sort_by": sort_by}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def get_stablecoin_detail(coin: str) -> str:
    """Full score breakdown for a specific stablecoin including category scores and methodology version."""
    _start = time.time()
    _success = True
    try:
        data = await _api_get(f"/api/scores/{coin.lower()}")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        _log_mcp_tool_call("get_stablecoin_detail", {"coin": coin}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def get_wallet_risk(address: str) -> str:
    """Get risk profile for a specific Ethereum wallet — composite risk score, concentration, coverage quality."""
    _start = time.time()
    _success = True
    try:
        data = await _api_get(f"/api/wallets/{address.lower()}")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        _log_mcp_tool_call("get_wallet_risk", {"address": address}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def get_wallet_holdings(address: str) -> str:
    """Detailed holdings breakdown for an Ethereum wallet with per-asset SII scores."""
    _start = time.time()
    _success = True
    try:
        data = await _api_get(f"/api/wallets/{address.lower()}")
        return json.dumps(data.get("holdings", data), indent=2)
    except Exception:
        _success = False
        raise
    finally:
        _log_mcp_tool_call("get_wallet_holdings", {"address": address}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def get_riskiest_wallets(limit: int = 20) -> str:
    """Wallets with the most capital at risk — lowest risk scores weighted by total value."""
    _start = time.time()
    _success = True
    try:
        data = await _api_get(f"/api/wallets/riskiest?limit={limit}")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        _log_mcp_tool_call("get_riskiest_wallets", {"limit": limit}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def get_scoring_backlog(limit: int = 20) -> str:
    """Unscored stablecoin assets ranked by total capital exposure across all indexed wallets."""
    _start = time.time()
    _success = True
    try:
        data = await _api_get(f"/api/backlog?limit={limit}")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        _log_mcp_tool_call("get_scoring_backlog", {"limit": limit}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def check_transaction_risk(from_address: str, to_address: str, asset_symbol: str) -> str:
    """Composite risk assessment for a stablecoin transaction — evaluates asset, sender, and receiver."""
    _start = time.time()
    _success = True
    try:
        asset_task = _api_get(f"/api/scores/{asset_symbol.lower()}")
        sender_task = _api_get(f"/api/wallets/{from_address.lower()}")
        receiver_task = _api_get(f"/api/wallets/{to_address.lower()}")
        asset, sender, receiver = await asyncio.gather(asset_task, sender_task, receiver_task)
        result = {
            "asset": asset,
            "sender": sender,
            "receiver": receiver,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return json.dumps(result, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        _log_mcp_tool_call("check_transaction_risk", {"from": from_address, "to": to_address, "asset": asset_symbol}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def get_methodology() -> str:
    """Returns the current SII formula, category weights, grade scale, and version information."""
    _start = time.time()
    _success = True
    try:
        data = await _api_get("/api/methodology")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        _log_mcp_tool_call("get_methodology", {}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def get_divergence_signals() -> str:
    """Check for divergence signals before executing transactions.

    Detects capital-flow / quality mismatches:
    - Asset quality: stablecoin score declining while capital flows in
    - Wallet concentration: HHI rising while wallet value grows
    - Quality-flow: score declining with net inflows from wallet graph

    Call this BEFORE executing any stablecoin swap, deposit, or rebalance
    to check if capital is flowing toward deteriorating assets.
    """
    _start = time.time()
    _success = True
    try:
        data = await _api_get("/api/divergence")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        _log_mcp_tool_call("get_divergence_signals", {}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def query_template(template_name: str, params: dict = None) -> str:
    """Run a pre-built query template against the Basis risk database.

    Available templates:
    - high_risk_whales: Wallets with high value AND poor risk grades
    - contagion_hotspots: Wallets with the most counterparty connections
    - stablecoin_concentration: Per-stablecoin holder concentration
    - score_movers: Assets whose SII score changed most over N days
    - disclosure_gaps: Issuers with the oldest attestation documents
    - cross_chain_exposure: Wallets active on multiple chains

    Call GET /api/query/templates for full parameter documentation.
    """
    _start = time.time()
    _success = True
    try:
        body = {"template": template_name}
        if params:
            body["params"] = params
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{API_BASE}/api/query/template", json=body, timeout=15.0)
            resp.raise_for_status()
            return json.dumps(resp.json(), indent=2)
    except Exception:
        _success = False
        raise
    finally:
        _log_mcp_tool_call("query_template", {"template": template_name}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def get_treasury_events(wallet_address: str = None, event_type: str = None, days: int = 30) -> str:
    """Get recent behavioral events from labeled treasury wallets.

    Detects: TWAP conversions, protocol rebalancing, concentration drift,
    quality shifts, and large transfers (>$1M) from known treasury wallets.

    Parameters:
        wallet_address: Filter to a specific wallet (optional)
        event_type: Filter by type: twap_conversion, rebalance, concentration_drift, quality_shift, large_transfer
        days: Lookback period in days (default 30)
    """
    _start = time.time()
    _success = True
    try:
        params = []
        if wallet_address:
            params.append(f"wallet={wallet_address}")
        if event_type:
            params.append(f"type={event_type}")
        qs = "&".join(params)
        path = f"/api/treasury/events?{qs}&limit=50" if qs else "/api/treasury/events?limit=50"
        data = await _api_get(path)
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        _log_mcp_tool_call("get_treasury_events", {"wallet": wallet_address, "type": event_type, "days": days}, int((time.time() - _start) * 1000), _success)
