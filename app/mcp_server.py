"""
Basis Protocol — MCP Server (HTTP transport)
=============================================
Thin adapter: exposes 8 MCP tools that call the hub's REST API internally.
"""

import asyncio
import json
from datetime import datetime, timezone

import httpx
from mcp.server.fastmcp import FastMCP

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
    data = await _api_get("/api/scores")
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_stablecoin_detail(coin: str) -> str:
    """Full score breakdown for a specific stablecoin including category scores and methodology version."""
    data = await _api_get(f"/api/scores/{coin.lower()}")
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_wallet_risk(address: str) -> str:
    """Get risk profile for a specific Ethereum wallet — composite risk score, concentration, coverage quality."""
    data = await _api_get(f"/api/wallets/{address.lower()}")
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_wallet_holdings(address: str) -> str:
    """Detailed holdings breakdown for an Ethereum wallet with per-asset SII scores."""
    data = await _api_get(f"/api/wallets/{address.lower()}")
    return json.dumps(data.get("holdings", data), indent=2)


@mcp.tool()
async def get_riskiest_wallets(limit: int = 20) -> str:
    """Wallets with the most capital at risk — lowest risk scores weighted by total value."""
    data = await _api_get(f"/api/wallets/riskiest?limit={limit}")
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_scoring_backlog(limit: int = 20) -> str:
    """Unscored stablecoin assets ranked by total capital exposure across all indexed wallets."""
    data = await _api_get(f"/api/backlog?limit={limit}")
    return json.dumps(data, indent=2)


@mcp.tool()
async def check_transaction_risk(from_address: str, to_address: str, asset_symbol: str) -> str:
    """Composite risk assessment for a stablecoin transaction — evaluates asset, sender, and receiver."""
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


@mcp.tool()
async def get_methodology() -> str:
    """Returns the current SII formula, category weights, grade scale, and version information."""
    data = await _api_get("/api/methodology")
    return json.dumps(data, indent=2)
