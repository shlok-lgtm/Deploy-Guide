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
    data = await _api_get("/api/divergence")
    return json.dumps(data, indent=2)


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
    body = {"template": template_name}
    if params:
        body["params"] = params
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{API_BASE}/api/query/template", json=body, timeout=15.0)
        resp.raise_for_status()
        return json.dumps(resp.json(), indent=2)
