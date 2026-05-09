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
        logger.warning(f"MCP tool log flush failed (table may not exist yet): {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="mcp_server_flush_log_failure",
                error_message=str(e)[:500],
                cycle_phase="mcp_server_flush_mcp_log",
            )
        except Exception:
            pass

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
async def get_stablecoin_scores(sort_by: str = "score_desc") -> str:
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
        await asyncio.to_thread(_log_mcp_tool_call, "get_stablecoin_scores", {"sort_by": sort_by}, int((time.time() - _start) * 1000), _success)


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
        await asyncio.to_thread(_log_mcp_tool_call, "get_stablecoin_detail", {"coin": coin}, int((time.time() - _start) * 1000), _success)


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
        await asyncio.to_thread(_log_mcp_tool_call, "get_wallet_risk", {"address": address}, int((time.time() - _start) * 1000), _success)


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
        await asyncio.to_thread(_log_mcp_tool_call, "get_wallet_holdings", {"address": address}, int((time.time() - _start) * 1000), _success)


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
        await asyncio.to_thread(_log_mcp_tool_call, "get_riskiest_wallets", {"limit": limit}, int((time.time() - _start) * 1000), _success)


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
        await asyncio.to_thread(_log_mcp_tool_call, "get_scoring_backlog", {"limit": limit}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def check_transaction_risk(from_address: str, to_address: str, asset_symbol: str) -> str:
    """Composite risk assessment for a stablecoin transaction — evaluates asset, sender, and receiver."""
    _start = time.time()
    _success = True
    try:
        asset_task = await _api_get(f"/api/scores/{asset_symbol.lower()}")
        sender_task = await _api_get(f"/api/wallets/{from_address.lower()}")
        receiver_task = await _api_get(f"/api/wallets/{to_address.lower()}")
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
        await asyncio.to_thread(_log_mcp_tool_call, "check_transaction_risk", {"from": from_address, "to": to_address, "asset": asset_symbol}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def get_methodology() -> str:
    """Returns the current SII formula, category weights, score scale, and version information."""
    _start = time.time()
    _success = True
    try:
        data = await _api_get("/api/methodology")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        await asyncio.to_thread(_log_mcp_tool_call, "get_methodology", {}, int((time.time() - _start) * 1000), _success)


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
        await asyncio.to_thread(_log_mcp_tool_call, "get_divergence_signals", {}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def query_template(template_name: str, params: dict = None) -> str:
    """Run a pre-built query template against the Basis risk database.

    Available templates:
    - high_risk_whales: Wallets with high value AND poor risk scores
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
        await asyncio.to_thread(_log_mcp_tool_call, "query_template", {"template": template_name}, int((time.time() - _start) * 1000), _success)


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
        await asyncio.to_thread(_log_mcp_tool_call, "get_treasury_events", {"wallet": wallet_address, "type": event_type, "days": days}, int((time.time() - _start) * 1000), _success)


# =============================================================================
# Universal Data Layer MCP Tools
# =============================================================================

@mcp.tool()
async def basis_liquidity_depth(asset_id: str) -> str:
    """Per-asset, per-venue liquidity profile for any stablecoin.

    Returns DEX pool depth (Uniswap, Curve, etc.) and CEX ticker data
    (Binance, Coinbase, etc.) with bid/ask depth, spread, volume, and
    trade counts. Use before any large stablecoin transaction to check
    venue liquidity.

    Parameters:
        asset_id: Stablecoin ID (e.g. "usdc", "usdt", "dai")
    """
    _start = time.time()
    _success = True
    try:
        data = await _api_get(f"/api/data/liquidity/{asset_id.lower()}")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        await asyncio.to_thread(_log_mcp_tool_call, "basis_liquidity_depth", {"asset_id": asset_id}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def basis_yield_data(protocol: str = None) -> str:
    """Pool-level yield, TVL, and utilization for any DeFi protocol.

    Returns APY, base APY, reward APY, TVL, and pool metadata for all
    tracked lending/vault pools. Use to evaluate yield sustainability
    and protocol health.

    Parameters:
        protocol: Protocol slug (e.g. "aave-v3", "compound-v3"). Omit for all protocols.
    """
    _start = time.time()
    _success = True
    try:
        path = f"/api/data/yields?protocol={protocol}" if protocol else "/api/data/yields"
        data = await _api_get(path)
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        await asyncio.to_thread(_log_mcp_tool_call, "basis_yield_data", {"protocol": protocol}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def basis_governance_activity(protocol: str, days: int = 90) -> str:
    """Governance proposal counts, voter participation, and pass rates.

    Returns full proposal history with vote counts, quorum status, and
    voter concentration data. Use to assess DAO health and governance quality.

    Parameters:
        protocol: Protocol name (e.g. "aavedao", "comp-vote", "lido-snapshot")
        days: Lookback period in days (default 90)
    """
    _start = time.time()
    _success = True
    try:
        data = await _api_get(f"/api/data/governance/{protocol.lower()}?days={days}")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        await asyncio.to_thread(_log_mcp_tool_call, "basis_governance_activity", {"protocol": protocol, "days": days}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def basis_bridge_flows(bridge_id: str = None) -> str:
    """Directional bridge volume per chain pair.

    Shows where capital is flowing: "$50M Ethereum → Arbitrum, $12M back."
    Use to assess cross-chain liquidity routing and chain health.

    Parameters:
        bridge_id: Specific bridge ID (optional). Omit for aggregate flows.
    """
    _start = time.time()
    _success = True
    try:
        path = f"/api/data/bridge-flows?bridge_id={bridge_id}" if bridge_id else "/api/data/bridge-flows"
        data = await _api_get(path)
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        await asyncio.to_thread(_log_mcp_tool_call, "basis_bridge_flows", {"bridge_id": bridge_id}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def basis_exchange_health(exchange_id: str = None) -> str:
    """Exchange trust score, volume, and reserve status.

    Returns CoinGecko trust scores, 24h volume, year established, and
    stablecoin-specific trading pair data for top exchanges.

    Parameters:
        exchange_id: CoinGecko exchange ID (e.g. "binance", "coinbase-exchange"). Omit for all.
    """
    _start = time.time()
    _success = True
    try:
        path = f"/api/data/exchanges?exchange_id={exchange_id}" if exchange_id else "/api/data/exchanges"
        data = await _api_get(path)
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        await asyncio.to_thread(_log_mcp_tool_call, "basis_exchange_health", {"exchange_id": exchange_id}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def basis_correlation(matrix_type: str = "sii_30d") -> str:
    """Cross-entity correlation matrix.

    Shows which assets move together. When USDC depegs, which protocols
    lose TVL in sync? Use for portfolio construction and systemic risk assessment.

    Parameters:
        matrix_type: "sii_30d", "sii_90d", or "cross_90d"
    """
    _start = time.time()
    _success = True
    try:
        data = await _api_get(f"/api/data/correlations?matrix_type={matrix_type}")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        await asyncio.to_thread(_log_mcp_tool_call, "basis_correlation", {"matrix_type": matrix_type}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def basis_volatility(asset_id: str) -> str:
    """Realized volatility, drawdown, and recovery time for any asset.

    Returns 1d/7d/30d/90d realized vol, max drawdown, and correlation
    with BTC/ETH. Use for risk-adjusted position sizing.

    Parameters:
        asset_id: Asset ID (e.g. "usdc", "usdt")
    """
    _start = time.time()
    _success = True
    try:
        data = await _api_get(f"/api/data/volatility/{asset_id.lower()}")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        await asyncio.to_thread(_log_mcp_tool_call, "basis_volatility", {"asset_id": asset_id}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def basis_incidents(entity_id: str = None) -> str:
    """Structured event history: exploits, depegs, oracle failures.

    Returns timeline of incidents with severity, affected entities, and
    resolution status. Use for due diligence and insurance risk assessment.

    Parameters:
        entity_id: Entity ID to filter by (optional). Omit for all recent incidents.
    """
    _start = time.time()
    _success = True
    try:
        path = f"/api/data/incidents?entity_id={entity_id}" if entity_id else "/api/data/incidents"
        data = await _api_get(path)
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        await asyncio.to_thread(_log_mcp_tool_call, "basis_incidents", {"entity_id": entity_id}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def basis_peg_monitor(stablecoin_id: str, hours: int = 24) -> str:
    """5-minute peg resolution data for micro-depeg detection.

    Returns 5-minute price snapshots and deviation from $1.00. Catches
    micro-depegs that are invisible at hourly resolution. Early warning
    signal for peg instability.

    Parameters:
        stablecoin_id: Stablecoin ID (e.g. "usdc", "usdt")
        hours: Lookback period in hours (default 24)
    """
    _start = time.time()
    _success = True
    try:
        data = await _api_get(f"/api/data/peg-5m/{stablecoin_id.lower()}?hours={hours}")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        await asyncio.to_thread(_log_mcp_tool_call, "basis_peg_monitor", {"stablecoin_id": stablecoin_id, "hours": hours}, int((time.time() - _start) * 1000), _success)


@mcp.tool()
async def basis_data_catalog() -> str:
    """Every data type available in the universal data layer.

    Returns per data type: description, freshness, history depth, update
    frequency, provenance status, row count. Use to discover what data
    is available for building custom risk indices.
    """
    _start = time.time()
    _success = True
    try:
        data = await _api_get("/api/data/catalog")
        return json.dumps(data, indent=2)
    except Exception:
        _success = False
        raise
    finally:
        await asyncio.to_thread(_log_mcp_tool_call, "basis_data_catalog", {}, int((time.time() - _start) * 1000), _success)
