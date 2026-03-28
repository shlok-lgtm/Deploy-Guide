"""
Basis Protocol - API Server
============================
Clean FastAPI server. Reads from database only. No data collection.
"""

import atexit
import json
import logging
import os
import time
import uuid as uuid_mod
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import STABLECOIN_REGISTRY, CORS_ORIGINS
from app.database import (
    init_pool, close_pool, health_check as db_health_check,
    fetch_one, fetch_all,
)
from app.scoring import (
    SII_V1_WEIGHTS, STRUCTURAL_SUBWEIGHTS, FORMULA_VERSION,
    score_to_grade, COMPONENT_NORMALIZATIONS,
)
from app.specs.methodology_versions import METHODOLOGY_VERSIONS, WALLET_METHODOLOGY_VERSIONS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Basis Protocol API",
    description="Standardized risk surfaces for on-chain finance",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Rate limiting + usage tracking middleware
# Registered AFTER CORSMiddleware so it runs FIRST on each incoming request
# (FastAPI executes middleware in reverse registration order).
# =============================================================================

@app.middleware("http")
async def rate_limit_and_track(request: Request, call_next):
    start_time = time.time()
    path = request.url.path

    # Only process /api/* and /mcp paths — pass everything else straight through
    if not path.startswith("/api") and not path.startswith("/mcp"):
        return await call_next(request)

    from app.rate_limiter import rate_limiter, PUBLIC_RATE_LIMIT, KEYED_RATE_LIMIT
    from app.usage_tracker import validate_api_key, hash_api_key, log_request

    query_key = request.query_params.get("apikey")
    header_key = request.headers.get("x-api-key")
    api_key_id: Optional[int] = None
    api_key_hash: Optional[str] = None

    # Try query param first; if invalid or absent, fall back to header
    for candidate in filter(None, [query_key, header_key]):
        api_key_id = validate_api_key(candidate)
        api_key_hash = hash_api_key(candidate)
        if api_key_id:
            break

    ip = request.client.host if request.client else "unknown"
    ua = request.headers.get("user-agent", "")[:500]

    # Admin endpoints — exempt from rate limiting but still logged
    if path.startswith("/api/admin"):
        response = await call_next(request)
        elapsed_ms = int((time.time() - start_time) * 1000)
        log_request(
            endpoint=path, method=request.method,
            status_code=response.status_code, response_time_ms=elapsed_ms,
            ip=ip, api_key_id=api_key_id, api_key_hash=api_key_hash, user_agent=ua,
        )
        return response

    # Determine rate limit tier
    if api_key_id:
        identifier = f"key:{api_key_id}"
        limit = KEYED_RATE_LIMIT
    else:
        identifier = f"ip:{ip}"
        limit = PUBLIC_RATE_LIMIT

    allowed, remaining = rate_limiter.is_allowed(identifier, limit)

    if not allowed:
        elapsed_ms = int((time.time() - start_time) * 1000)
        log_request(
            endpoint=path, method=request.method,
            status_code=429, response_time_ms=elapsed_ms,
            ip=ip, api_key_id=api_key_id, api_key_hash=api_key_hash, user_agent=ua,
        )
        origin = request.headers.get("origin", "")
        cors_header = "*" if (not origin or "*" in CORS_ORIGINS) else (origin if origin in CORS_ORIGINS else "")
        rl_headers = {
            "Retry-After": "60",
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Limit": str(limit),
        }
        if cors_header:
            rl_headers["Access-Control-Allow-Origin"] = cors_header
        return JSONResponse(
            status_code=429,
            content={"error": "Rate limit exceeded", "retry_after_seconds": 60},
            headers=rl_headers,
        )

    response = await call_next(request)
    elapsed_ms = int((time.time() - start_time) * 1000)
    log_request(
        endpoint=path, method=request.method,
        status_code=response.status_code, response_time_ms=elapsed_ms,
        ip=ip, api_key_id=api_key_id, api_key_hash=api_key_hash, user_agent=ua,
    )
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-RateLimit-Limit"] = str(limit)
    if path.startswith("/api/scores") or path.startswith("/api/wallets"):
        response.headers["Basis-Methodology-Version"] = FORMULA_VERSION
    return response


# =============================================================================
# Lifecycle
# =============================================================================

@app.on_event("startup")
async def startup():
    init_pool()
    # Register governance intelligence routes
    try:
        from app.governance import register_gov_routes, apply_gov_migration
        apply_gov_migration()
        register_gov_routes(app)
        logger.info("Governance intelligence routes registered")
    except Exception as e:
        logger.warning(f"Governance module not available: {e}")
    # Register content engine routes
    try:
        from app.content_engine import register_content_routes
        register_content_routes(app)
        logger.info("Content engine routes registered")
    except Exception as e:
        logger.warning(f"Content engine not available: {e}")
    # Register wallet indexer routes
    try:
        from app.indexer.api import register_wallet_routes
        register_wallet_routes(app)
        logger.info("Wallet indexer routes registered")
    except Exception as e:
        logger.warning(f"Wallet indexer not available: {e}")
    # Register verification agent routes
    try:
        from app.agent.api import register_agent_routes
        register_agent_routes(app)
        logger.info("Verification agent routes registered")
    except Exception as e:
        logger.warning(f"Verification agent not available: {e}")
    # Register publisher page routes (wallet, asset, assessment, pulse HTML pages)
    try:
        from app.publisher.page_renderer import register_page_routes
        register_page_routes(app)
        logger.info("Publisher page routes registered")
    except Exception as e:
        logger.warning(f"Publisher pages not available: {e}")
    # MCP HTTP endpoint
    try:
        from app.mcp_server import mcp as mcp_server
        import asyncio

        if hasattr(mcp_server, "streamable_http_app"):
            mcp_asgi = mcp_server.streamable_http_app()
            app.state.mcp_task = asyncio.get_event_loop().create_task(
                _run_mcp_session_manager(mcp_server.session_manager)
            )
        elif hasattr(mcp_server, "asgi_app"):
            mcp_asgi = mcp_server.asgi_app()
            app.state.mcp_task = asyncio.get_event_loop().create_task(
                _run_mcp_session_manager(mcp_server.session_manager)
            )
        else:
            mcp_asgi = None

        if mcp_asgi is not None:
            @app.post("/mcp")
            @app.get("/mcp")
            @app.delete("/mcp")
            async def mcp_endpoint(request: Request):
                return await _delegate_to_asgi(mcp_asgi, request)

            app.mount("/mcp", mcp_asgi)
            logger.info("MCP HTTP endpoint registered at /mcp")
        else:
            from fastapi.responses import JSONResponse

            @app.post("/mcp")
            @app.get("/mcp")
            @app.delete("/mcp")
            async def mcp_endpoint_fallback(request: Request):
                return JSONResponse(
                    {"error": "MCP transport not available: SDK lacks streamable_http_app/asgi_app"},
                    status_code=503,
                )

            logger.warning("MCP SDK has no ASGI app method; /mcp registered with 503 fallback")
    except ImportError as e:
        logger.warning(f"MCP endpoint not available: {e}")
    except Exception as e:
        logger.warning(f"MCP endpoint registration failed: {e}")

    # SPA catch-all must be registered LAST so it doesn't shadow dynamic routes
    _register_spa_catch_all(app)
    logger.info("Basis Protocol API started")


async def _run_mcp_session_manager(session_manager):
    """Keep the MCP session manager running via its public run() context manager."""
    import asyncio
    async with session_manager.run():
        await asyncio.sleep(float("inf"))


async def _delegate_to_asgi(asgi_app, request: Request):
    """Forward a FastAPI request to an ASGI sub-app with a streaming response."""
    from starlette.responses import StreamingResponse
    import asyncio

    body = await request.body()
    send_queue: asyncio.Queue = asyncio.Queue()
    _body_sent = False

    async def receive():
        nonlocal _body_sent
        if not _body_sent:
            _body_sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message):
        await send_queue.put(message)

    scope = dict(request.scope)
    scope["path"] = "/"
    scope["root_path"] = request.scope.get("root_path", "") + "/mcp"

    asgi_task = asyncio.ensure_future(asgi_app(scope, receive, send))
    asgi_task.add_done_callback(lambda _: send_queue.put_nowait({"type": "http.response.body", "body": b"", "more_body": False}))

    start_message = await send_queue.get()
    status_code = start_message.get("status", 200)
    headers = {k.decode(): v.decode() for k, v in start_message.get("headers", [])}

    async def body_generator():
        while True:
            msg = await send_queue.get()
            if msg["type"] == "http.response.body":
                chunk = msg.get("body", b"")
                if chunk:
                    yield chunk
                if not msg.get("more_body", False):
                    break

    return StreamingResponse(
        body_generator(),
        status_code=status_code,
        headers=headers,
    )


@app.on_event("shutdown")
async def shutdown():
    try:
        from app.usage_tracker import flush as _flush_usage
        _flush_usage()
    except Exception as e:
        logger.warning(f"Usage tracker shutdown flush error: {e}")
    close_pool()
    logger.info("Basis Protocol API stopped")


# Atexit fallback for non-graceful shutdowns (SIGKILL, Replit restarts, etc.)
try:
    from app.usage_tracker import flush as _flush_usage_atexit
    atexit.register(_flush_usage_atexit)
except Exception:
    pass


# =============================================================================
# Frontend — Serve built React app from root
# =============================================================================

FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend", "dist")

if os.path.isdir(FRONTEND_DIR):
    app.mount("/assets", StaticFiles(directory=os.path.join(FRONTEND_DIR, "assets")), name="static-assets")


# =============================================================================
# Methodology version helper
# =============================================================================

def check_methodology_version(requested_version, current_version="v1.0.0"):
    """Validate a methodology_version query param. Returns True if pinned, False if omitted."""
    if requested_version is None:
        return False  # not pinned
    if requested_version == current_version:
        return True  # pinned
    raise HTTPException(status_code=404, detail={
        "error": "version_not_found",
        "requested": requested_version,
        "available": [current_version],
        "message": f"Requested methodology version not available. Current version: {current_version}"
    })


# =============================================================================
# 1. GET /api/health
# =============================================================================

@app.get("/api/health")
async def get_health():
    """System health check — is the database up, how many stablecoins scored."""
    db_status = db_health_check()
    
    scores_result = fetch_one("SELECT COUNT(*) as count FROM scores")
    scored_count = scores_result["count"] if scores_result else 0
    
    latest_result = fetch_one(
        "SELECT MAX(computed_at) as latest FROM scores"
    )
    latest_score = latest_result["latest"] if latest_result else None
    
    return {
        "status": "healthy" if db_status["status"] == "healthy" else "degraded",
        "database": db_status,
        "scores": {
            "stablecoins_scored": scored_count,
            "stablecoins_registered": len(STABLECOIN_REGISTRY),
            "last_computed": latest_score.isoformat() if latest_score else None,
        },
        "formula_version": FORMULA_VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# 2. GET /api/scores — All stablecoin scores (main rankings table)
# =============================================================================

@app.get("/api/scores")
async def get_scores(methodology_version: Optional[str] = Query(default=None)):
    """Get current SII scores for all stablecoins."""
    pinned = check_methodology_version(methodology_version)
    rows = fetch_all("""
        SELECT s.*, st.name, st.symbol, st.issuer, st.contract AS token_contract
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        ORDER BY s.overall_score DESC
    """)
    
    results = []
    for row in rows:
        results.append({
            "id": row["stablecoin_id"],
            "name": row["name"],
            "symbol": row["symbol"],
            "issuer": row["issuer"],
            "token_contract": row.get("token_contract"),
            "score": float(row["overall_score"]),
            "grade": row["grade"],
            "price": float(row["current_price"]) if row.get("current_price") else None,
            "market_cap": row.get("market_cap"),
            "volume_24h": row.get("volume_24h"),
            "daily_change": float(row["daily_change"]) if row.get("daily_change") else None,
            "weekly_change": float(row["weekly_change"]) if row.get("weekly_change") else None,
            "categories": {
                "peg": float(row["peg_score"]) if row.get("peg_score") else None,
                "liquidity": float(row["liquidity_score"]) if row.get("liquidity_score") else None,
                "flows": float(row["mint_burn_score"]) if row.get("mint_burn_score") else None,
                "distribution": float(row["distribution_score"]) if row.get("distribution_score") else None,
                "structural": float(row["structural_score"]) if row.get("structural_score") else None,
            },
            "structural_breakdown": {
                "reserves": float(row["reserves_score"]) if row.get("reserves_score") else None,
                "contract": float(row["contract_score"]) if row.get("contract_score") else None,
                "oracle": float(row["oracle_score"]) if row.get("oracle_score") else None,
                "governance": float(row["governance_score"]) if row.get("governance_score") else None,
                "network": float(row["network_score"]) if row.get("network_score") else None,
            },
            "component_count": row.get("component_count"),
            "formula_version": row.get("formula_version"),
            "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        })
    
    return {
        "stablecoins": results,
        "count": len(results),
        "formula_version": FORMULA_VERSION,
        "methodology_version": FORMULA_VERSION,
        "methodology_version_pinned": pinned,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# 3. GET /api/scores/{coin} — Detailed score for one stablecoin
# =============================================================================

@app.get("/api/scores/{coin}")
async def get_score_detail(coin: str, methodology_version: Optional[str] = Query(default=None)):
    """Get detailed SII score breakdown for a specific stablecoin."""
    pinned = check_methodology_version(methodology_version)
    row = fetch_one("""
        SELECT s.*, st.name, st.symbol, st.issuer, st.contract AS token_contract, st.attestation_config, st.regulatory_licenses
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        WHERE s.stablecoin_id = %s
    """, (coin,))
    
    if not row:
        # Check if the stablecoin exists but isn't scored yet
        exists = fetch_one("SELECT id FROM stablecoins WHERE id = %s", (coin,))
        if exists:
            raise HTTPException(status_code=404, detail=f"Stablecoin '{coin}' exists but has no scores yet")
        raise HTTPException(status_code=404, detail=f"Stablecoin '{coin}' not found")
    
    # Get latest component readings
    components = fetch_all("""
        SELECT component_id, category, raw_value, normalized_score, data_source, collected_at
        FROM component_readings
        WHERE stablecoin_id = %s
          AND collected_at > NOW() - INTERVAL '48 hours'
        ORDER BY category, component_id
    """, (coin,))
    
    return {
        "id": row["stablecoin_id"],
        "name": row["name"],
        "symbol": row["symbol"],
        "issuer": row["issuer"],
        "token_contract": row.get("token_contract"),
        "score": float(row["overall_score"]),
        "grade": row["grade"],
        "price": float(row["current_price"]) if row.get("current_price") else None,
        "market_cap": row.get("market_cap"),
        "volume_24h": row.get("volume_24h"),
        "categories": {
            "peg": {"score": float(row["peg_score"]) if row.get("peg_score") else None, "weight": SII_V1_WEIGHTS["peg_stability"]},
            "liquidity": {"score": float(row["liquidity_score"]) if row.get("liquidity_score") else None, "weight": SII_V1_WEIGHTS["liquidity_depth"]},
            "flows": {"score": float(row["mint_burn_score"]) if row.get("mint_burn_score") else None, "weight": SII_V1_WEIGHTS["mint_burn_dynamics"]},
            "distribution": {"score": float(row["distribution_score"]) if row.get("distribution_score") else None, "weight": SII_V1_WEIGHTS["holder_distribution"]},
            "structural": {"score": float(row["structural_score"]) if row.get("structural_score") else None, "weight": SII_V1_WEIGHTS["structural_risk_composite"]},
        },
        "structural_breakdown": {
            "reserves": {"score": float(row["reserves_score"]) if row.get("reserves_score") else None, "weight": STRUCTURAL_SUBWEIGHTS["reserves_collateral"]},
            "contract": {"score": float(row["contract_score"]) if row.get("contract_score") else None, "weight": STRUCTURAL_SUBWEIGHTS["smart_contract_risk"]},
            "oracle": {"score": float(row["oracle_score"]) if row.get("oracle_score") else None, "weight": STRUCTURAL_SUBWEIGHTS["oracle_integrity"]},
            "governance": {"score": float(row["governance_score"]) if row.get("governance_score") else None, "weight": STRUCTURAL_SUBWEIGHTS["governance_operations"]},
            "network": {"score": float(row["network_score"]) if row.get("network_score") else None, "weight": STRUCTURAL_SUBWEIGHTS["network_chain_risk"]},
        },
        "components": [
            {
                "id": c["component_id"],
                "category": c["category"],
                "raw_value": c["raw_value"],
                "normalized_score": round(c["normalized_score"], 2) if c["normalized_score"] else None,
                "data_source": c["data_source"],
                "collected_at": c["collected_at"].isoformat() if c["collected_at"] else None,
            }
            for c in components
        ],
        "attestation": row.get("attestation_config"),
        "regulatory_licenses": row.get("regulatory_licenses"),
        "component_count": row.get("component_count"),
        "formula_version": row.get("formula_version"),
        "methodology_version": FORMULA_VERSION,
        "methodology_version_pinned": pinned,
        "daily_change": float(row["daily_change"]) if row.get("daily_change") else None,
        "weekly_change": float(row["weekly_change"]) if row.get("weekly_change") else None,
        "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
    }


# =============================================================================
# 4. GET /api/scores/{coin}/history — Historical scores
# =============================================================================

@app.get("/api/scores/{coin}/history")
async def get_score_history(
    coin: str,
    days: int = Query(default=90, ge=1, le=365),
):
    """Get historical SII scores for a stablecoin."""
    rows = fetch_all("""
        SELECT score_date, overall_score, grade, peg_score, liquidity_score,
               mint_burn_score, distribution_score, structural_score,
               daily_change, component_count
        FROM score_history
        WHERE stablecoin = %s
          AND score_date > CURRENT_DATE - INTERVAL '%s days'
        ORDER BY score_date ASC
    """, (coin, days))
    
    return {
        "stablecoin": coin,
        "days": days,
        "history": [
            {
                "date": str(row["score_date"]),
                "score": float(row["overall_score"]),
                "grade": row["grade"],
                "categories": {
                    "peg": float(row["peg_score"]) if row.get("peg_score") else None,
                    "liquidity": float(row["liquidity_score"]) if row.get("liquidity_score") else None,
                    "flows": float(row["mint_burn_score"]) if row.get("mint_burn_score") else None,
                    "distribution": float(row["distribution_score"]) if row.get("distribution_score") else None,
                    "structural": float(row["structural_score"]) if row.get("structural_score") else None,
                },
                "daily_change": float(row["daily_change"]) if row.get("daily_change") else None,
                "component_count": row.get("component_count"),
            }
            for row in rows
        ],
        "count": len(rows),
    }


# =============================================================================
# 5. GET /api/compare — Compare stablecoins side by side
# =============================================================================

@app.get("/api/compare")
async def compare_scores(
    coins: str = Query(description="Comma-separated stablecoin IDs", examples=["usdc,usdt,dai"]),
):
    """Compare SII scores for multiple stablecoins."""
    coin_list = [c.strip().lower() for c in coins.split(",") if c.strip()]
    
    if not coin_list:
        raise HTTPException(status_code=400, detail="Provide at least one stablecoin ID")
    if len(coin_list) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 stablecoins per comparison")
    
    placeholders = ",".join(["%s"] * len(coin_list))
    rows = fetch_all(f"""
        SELECT s.*, st.name, st.symbol, st.issuer, st.contract AS token_contract
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        WHERE s.stablecoin_id IN ({placeholders})
        ORDER BY s.overall_score DESC
    """, tuple(coin_list))
    
    return {
        "comparison": [
            {
                "id": row["stablecoin_id"],
                "name": row["name"],
                "symbol": row["symbol"],
                "token_contract": row.get("token_contract"),
                "score": float(row["overall_score"]),
                "grade": row["grade"],
                "categories": {
                    "peg": float(row["peg_score"]) if row.get("peg_score") else None,
                    "liquidity": float(row["liquidity_score"]) if row.get("liquidity_score") else None,
                    "flows": float(row["mint_burn_score"]) if row.get("mint_burn_score") else None,
                    "distribution": float(row["distribution_score"]) if row.get("distribution_score") else None,
                    "structural": float(row["structural_score"]) if row.get("structural_score") else None,
                },
            }
            for row in rows
        ],
        "count": len(rows),
        "requested": coin_list,
    }


# =============================================================================
# 6. GET /api/methodology — Formula and weights
# =============================================================================

@app.get("/api/methodology")
async def get_methodology(methodology_version: Optional[str] = Query(default=None)):
    """Get the SII methodology — formula, weights, and component specifications."""
    pinned = check_methodology_version(methodology_version)
    return {
        "version": FORMULA_VERSION,
        "formula": "SII = 0.30×Peg + 0.25×Liquidity + 0.15×MintBurn + 0.10×Distribution + 0.20×Structural",
        "structural_formula": "Structural = 0.30×Reserves + 0.20×SmartContract + 0.15×Oracle + 0.20×Governance + 0.15×Network",
        "weights": {
            "top_level": SII_V1_WEIGHTS,
            "structural": STRUCTURAL_SUBWEIGHTS,
        },
        "grade_scale": {
            "A+": "90-100", "A": "85-90", "A-": "80-85",
            "B+": "75-80", "B": "70-75", "B-": "65-70",
            "C+": "60-65", "C": "55-60", "C-": "50-55",
            "D": "45-50", "F": "0-45",
        },
        "components": {
            comp_id: {
                "category": spec["category"],
                "weight_in_category": spec["weight"],
            }
            for comp_id, spec in COMPONENT_NORMALIZATIONS.items()
        },
        "total_components": 102,
        "automated_components": 83,
        "data_sources": [
            "CoinGecko Pro", "DeFiLlama", "Etherscan", "Curve Finance",
            "Issuer attestation reports", "On-chain contract analysis",
        ],
        "methodology_version": FORMULA_VERSION,
        "methodology_version_pinned": pinned,
    }


# =============================================================================
# 6b. GET /api/methodology/versions — Version history and governance
# =============================================================================

@app.get("/api/methodology/versions")
async def get_methodology_versions():
    """Get methodology version history, governance rules, and wallet scoring versions."""
    return {
        "sii": METHODOLOGY_VERSIONS,
        "wallet": WALLET_METHODOLOGY_VERSIONS,
    }


# =============================================================================
# 7. GET /api/config — Stablecoin registry
# =============================================================================

@app.get("/api/config")
async def get_config():
    """Get the stablecoin registry configuration."""
    return {
        "stablecoins": {
            sid: {
                "name": cfg["name"],
                "symbol": cfg["symbol"],
                "issuer": cfg["issuer"],
                "coingecko_id": cfg["coingecko_id"],
                "contract": cfg.get("contract"),
                "attestation": cfg.get("attestation"),
            }
            for sid, cfg in STABLECOIN_REGISTRY.items()
        },
        "count": len(STABLECOIN_REGISTRY),
    }


# =============================================================================
# Namespace spec — URL addressing scheme
# =============================================================================

@app.get("/api/namespace")
async def get_namespace():
    """Addressing namespace specification for Basis Protocol URLs."""
    return {
        "version": "1.0.0",
        "stability": "permanent",
        "patterns": {
            "wallet": "/wallet/{address}",
            "asset": "/asset/{symbol}",
            "assessment": "/assessment/{uuid}",
            "pulse": "/pulse/{YYYY-MM-DD}",
        },
        "content_negotiation": {
            "text/html": "Returns rendered HTML page with JSON-LD",
            "application/json": "Returns structured JSON data",
        },
        "guarantees": [
            "URLs will not change structure",
            "URLs will not 404 for previously published entities",
            "JSON-LD schema will remain backward-compatible",
            "API alternate links are stable",
        ],
    }


# =============================================================================
# 8. GET /api/events — Crisis events timeline
# =============================================================================

@app.get("/api/events")
async def get_events(
    limit: int = Query(default=50, ge=1, le=500),
):
    """Get crisis events and annotations timeline."""
    rows = fetch_all("""
        SELECT id, event_date, event_name, event_type, affected_stablecoins,
               description, severity
        FROM score_events
        ORDER BY event_date DESC
        LIMIT %s
    """, (limit,))
    
    return {
        "events": [
            {
                "date": str(row["event_date"]),
                "name": row["event_name"],
                "type": row["event_type"],
                "affected": row["affected_stablecoins"],
                "description": row["description"],
                "severity": row["severity"],
            }
            for row in rows
        ],
        "count": len(rows),
    }


# =============================================================================
# 9. GET /api/deviations/{coin} — Peg deviation history
# =============================================================================

@app.get("/api/deviations/{coin}")
async def get_deviations(
    coin: str,
    limit: int = Query(default=50, ge=1, le=500),
):
    """Get historical peg deviation events for a stablecoin."""
    # Map stablecoin ID to coingecko_id
    cfg = STABLECOIN_REGISTRY.get(coin)
    if not cfg:
        raise HTTPException(status_code=404, detail=f"Stablecoin '{coin}' not found")
    
    cg_id = cfg["coingecko_id"]
    
    rows = fetch_all("""
        SELECT event_start, event_end, duration_hours, max_deviation_pct,
               avg_deviation_pct, direction, recovery_complete,
               market_cap_at_start, volume_during_event
        FROM deviation_events
        WHERE coingecko_id = %s
        ORDER BY event_start DESC
        LIMIT %s
    """, (cg_id, limit))
    
    return {
        "stablecoin": coin,
        "coingecko_id": cg_id,
        "deviations": [
            {
                "start": row["event_start"].isoformat() if row["event_start"] else None,
                "end": row["event_end"].isoformat() if row["event_end"] else None,
                "duration_hours": row["duration_hours"],
                "max_deviation_pct": row["max_deviation_pct"],
                "avg_deviation_pct": row["avg_deviation_pct"],
                "direction": row["direction"],
                "recovered": row["recovery_complete"],
                "market_cap": row["market_cap_at_start"],
                "volume": row["volume_during_event"],
            }
            for row in rows
        ],
        "count": len(rows),
    }


# =============================================================================
# Admin — Authentication helper
# =============================================================================

def _check_admin_key(request: Request):
    admin_key = os.environ.get("ADMIN_KEY", "")
    provided = request.query_params.get("key", "")
    if not admin_key or provided != admin_key:
        raise HTTPException(status_code=401, detail="Unauthorized")


# =============================================================================
# 10. GET /api/admin/governance/stats
# =============================================================================

@app.get("/api/admin/governance/stats")
async def admin_governance_stats(request: Request):
    _check_admin_key(request)

    total_docs = fetch_one("SELECT COUNT(*) as count FROM gov_documents")
    total_coin_mentions = fetch_one("SELECT COUNT(*) as count FROM gov_stablecoin_mentions")
    total_metric_mentions = fetch_one("SELECT COUNT(*) as count FROM gov_metric_mentions")

    sources = fetch_all("""
        SELECT source, COUNT(*) as count
        FROM gov_documents
        GROUP BY source
        ORDER BY count DESC
    """)

    top_coins = fetch_all("""
        SELECT
            sm.stablecoin,
            COUNT(*) as count,
            COUNT(*) FILTER (WHERE sm.sentiment = 'negative') as negative,
            COUNT(*) FILTER (WHERE sm.sentiment = 'neutral') as neutral,
            COUNT(*) FILTER (WHERE sm.sentiment = 'positive') as positive,
            COUNT(*) FILTER (WHERE sm.sentiment = 'concerned') as concerned
        FROM gov_stablecoin_mentions sm
        GROUP BY sm.stablecoin
        ORDER BY count DESC
        LIMIT 15
    """)

    recent_docs = fetch_all("""
        SELECT
            d.title, d.source, d.published_at,
            array_agg(DISTINCT sm.stablecoin) FILTER (WHERE sm.stablecoin IS NOT NULL) as stablecoins_mentioned
        FROM gov_documents d
        LEFT JOIN gov_stablecoin_mentions sm ON d.id = sm.document_id
        GROUP BY d.id, d.title, d.source, d.published_at
        ORDER BY d.published_at DESC NULLS LAST
        LIMIT 20
    """)

    return {
        "total_documents": total_docs["count"] if total_docs else 0,
        "total_stablecoin_mentions": total_coin_mentions["count"] if total_coin_mentions else 0,
        "total_metric_mentions": total_metric_mentions["count"] if total_metric_mentions else 0,
        "sources": [{"source": r["source"], "count": r["count"]} for r in sources],
        "top_stablecoin_mentions": [
            {
                "stablecoin": r["stablecoin"],
                "count": r["count"],
                "sentiment_breakdown": {
                    "negative": r["negative"],
                    "neutral": r["neutral"],
                    "positive": r["positive"],
                    "concerned": r["concerned"],
                },
            }
            for r in top_coins
        ],
        "recent_documents": [
            {
                "title": r["title"],
                "source": r["source"],
                "published_at": r["published_at"].isoformat() if r.get("published_at") else None,
                "stablecoins_mentioned": r["stablecoins_mentioned"] or [],
            }
            for r in recent_docs
        ],
    }


# =============================================================================
# 11. GET /api/admin/freshness
# =============================================================================

@app.get("/api/admin/freshness")
async def admin_freshness(request: Request):
    _check_admin_key(request)

    stablecoins_data = []
    for sid in STABLECOIN_REGISTRY:
        score_row = fetch_one(
            "SELECT computed_at FROM scores WHERE stablecoin_id = %s", (sid,)
        )

        components = fetch_all("""
            SELECT
                category,
                COUNT(*) as count,
                ROUND(AVG(normalized_score)::numeric, 1) as avg_score
            FROM component_readings
            WHERE stablecoin_id = %s
              AND collected_at > NOW() - INTERVAL '48 hours'
            GROUP BY category
        """, (sid,))

        sources_rows = fetch_all("""
            SELECT DISTINCT data_source
            FROM component_readings
            WHERE stablecoin_id = %s
              AND collected_at > NOW() - INTERVAL '48 hours'
        """, (sid,))

        categories = {}
        total_count = 0
        null_cats = []
        for c in components:
            cat_key = c["category"]
            cat_map = {
                "peg_stability": "peg",
                "liquidity_depth": "liquidity",
                "mint_burn_dynamics": "flows",
                "holder_distribution": "distribution",
                "structural_risk_composite": "structural",
            }
            short = cat_map.get(cat_key, cat_key)
            categories[short] = {
                "count": c["count"],
                "avg_score": float(c["avg_score"]) if c["avg_score"] else None,
            }
            total_count += c["count"]
            if c["avg_score"] is None:
                null_cats.append(short)

        for expected in ["peg", "liquidity", "flows", "distribution", "structural"]:
            if expected not in categories:
                null_cats.append(expected)

        stablecoins_data.append({
            "id": sid,
            "last_scored": score_row["computed_at"].isoformat() if score_row and score_row.get("computed_at") else None,
            "component_count": total_count,
            "categories": categories,
            "null_categories": null_cats,
            "sources": [r["data_source"] for r in sources_rows],
        })

    latest_score = fetch_one("SELECT MAX(computed_at) as latest FROM scores")
    last_run = latest_score["latest"] if latest_score else None
    interval_min = int(os.environ.get("COLLECTION_INTERVAL", "60"))

    return {
        "stablecoins": stablecoins_data,
        "last_worker_run": last_run.isoformat() if last_run else None,
        "next_expected": (last_run + timedelta(minutes=interval_min)).isoformat() if last_run else None,
    }


# =============================================================================
# 12. GET /api/admin/health
# =============================================================================

@app.get("/api/admin/health")
async def admin_health(request: Request):
    _check_admin_key(request)

    db_status = db_health_check()

    scores_result = fetch_one("SELECT COUNT(*) as count FROM scores")
    scored_count = scores_result["count"] if scores_result else 0

    latest_result = fetch_one("SELECT MAX(computed_at) as latest FROM scores")
    latest_score = latest_result["latest"] if latest_result else None

    last_crawl = fetch_one("""
        SELECT source, completed_at, documents_found, documents_new, errors, status
        FROM gov_crawl_logs
        ORDER BY started_at DESC
        LIMIT 1
    """)

    source_coverage = fetch_all("""
        SELECT data_source, COUNT(DISTINCT stablecoin_id) as stablecoin_count, COUNT(*) as reading_count
        FROM component_readings
        WHERE collected_at > NOW() - INTERVAL '48 hours'
        GROUP BY data_source
        ORDER BY reading_count DESC
    """)

    table_counts = fetch_all("""
        SELECT relname as table_name, n_live_tup as row_count
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
        ORDER BY n_live_tup DESC
    """)

    stale_count = fetch_one("""
        SELECT COUNT(*) as count FROM component_readings
        WHERE is_stale = TRUE AND collected_at > NOW() - INTERVAL '48 hours'
    """)

    error_count = fetch_one("""
        SELECT COUNT(*) as count FROM component_readings
        WHERE error_message IS NOT NULL AND collected_at > NOW() - INTERVAL '48 hours'
    """)

    return {
        "status": "healthy" if db_status["status"] == "healthy" else "degraded",
        "database": db_status,
        "scores": {
            "stablecoins_scored": scored_count,
            "stablecoins_registered": len(STABLECOIN_REGISTRY),
            "last_computed": latest_score.isoformat() if latest_score else None,
        },
        "formula_version": FORMULA_VERSION,
        "last_governance_crawl": {
            "source": last_crawl["source"] if last_crawl else None,
            "completed_at": last_crawl["completed_at"].isoformat() if last_crawl and last_crawl.get("completed_at") else None,
            "documents_found": last_crawl["documents_found"] if last_crawl else 0,
            "documents_new": last_crawl["documents_new"] if last_crawl else 0,
            "errors": last_crawl["errors"] if last_crawl else 0,
            "status": last_crawl["status"] if last_crawl else None,
        } if last_crawl else None,
        "component_coverage": [
            {
                "source": r["data_source"],
                "stablecoin_count": r["stablecoin_count"],
                "reading_count": r["reading_count"],
            }
            for r in source_coverage
        ],
        "scoring_issues": {
            "stale_readings": stale_count["count"] if stale_count else 0,
            "error_readings": error_count["count"] if error_count else 0,
        },
        "table_row_counts": {r["table_name"]: r["row_count"] for r in table_counts},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# 13. GET /api/admin/content/signals
# =============================================================================

@app.get("/api/admin/content/signals")
async def admin_content_signals(request: Request):
    _check_admin_key(request)

    hot_docs = fetch_all("""
        SELECT
            d.id, d.title, d.source, d.published_at,
            COUNT(sm.id) as mention_count,
            array_agg(DISTINCT sm.stablecoin) FILTER (WHERE sm.stablecoin IS NOT NULL) as stablecoins,
            MODE() WITHIN GROUP (ORDER BY sm.sentiment) as dominant_sentiment
        FROM gov_documents d
        JOIN gov_stablecoin_mentions sm ON d.id = sm.document_id
        WHERE d.published_at > NOW() - INTERVAL '14 days'
        GROUP BY d.id, d.title, d.source, d.published_at
        ORDER BY COUNT(sm.id) DESC
        LIMIT 10
    """)

    if not hot_docs:
        hot_docs = fetch_all("""
            SELECT
                d.id, d.title, d.source, d.published_at,
                COUNT(sm.id) as mention_count,
                array_agg(DISTINCT sm.stablecoin) FILTER (WHERE sm.stablecoin IS NOT NULL) as stablecoins,
                MODE() WITHIN GROUP (ORDER BY sm.sentiment) as dominant_sentiment
            FROM gov_documents d
            JOIN gov_stablecoin_mentions sm ON d.id = sm.document_id
            GROUP BY d.id, d.title, d.source, d.published_at
            ORDER BY COUNT(sm.id) DESC
            LIMIT 10
        """)

    signals = []
    for doc in hot_docs:
        coins = doc.get("stablecoins") or []
        sii_data = {}
        scores_list = []
        for coin_name in coins:
            coin_id = coin_name.lower()
            score_row = fetch_one("""
                SELECT overall_score, peg_score, liquidity_score, mint_burn_score,
                       distribution_score, structural_score
                FROM scores WHERE stablecoin_id = %s
            """, (coin_id,))
            if score_row:
                sii_data[coin_id + "_score"] = float(score_row["overall_score"])
                scores_list.append(float(score_row["overall_score"]))
                cats = {
                    "peg": float(score_row["peg_score"]) if score_row.get("peg_score") else 100,
                    "liquidity": float(score_row["liquidity_score"]) if score_row.get("liquidity_score") else 100,
                    "flows": float(score_row["mint_burn_score"]) if score_row.get("mint_burn_score") else 100,
                    "distribution": float(score_row["distribution_score"]) if score_row.get("distribution_score") else 100,
                    "structural": float(score_row["structural_score"]) if score_row.get("structural_score") else 100,
                }
                weakest = min(cats, key=cats.get)
                sii_data["weakest_category"] = weakest

        if len(scores_list) >= 2:
            sii_data["gap"] = round(max(scores_list) - min(scores_list), 1)

        topic = doc["title"] or "Untitled"
        coins_str = ", ".join(c.upper() for c in coins[:3]) if coins else "stablecoins"
        suggested = f"{topic} — comparing {coins_str}"
        if sii_data.get("weakest_category"):
            suggested += f" (weakest: {sii_data['weakest_category']})"

        signals.append({
            "governance_topic": topic,
            "source": doc["source"],
            "published_at": doc["published_at"].isoformat() if doc.get("published_at") else None,
            "mention_count": doc["mention_count"],
            "sentiment": doc.get("dominant_sentiment", "neutral"),
            "relevant_sii_data": sii_data,
            "suggested_angle": suggested,
        })

    return {"signals": signals}


# =============================================================================
# 14a. GET /developers — Public API/Pricing Page
# =============================================================================

DEVELOPERS_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Basis Protocol API — Risk primitives for on-chain finance</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#f5f2ec;color:#0a0a0a;font-family:'IBM Plex Sans',sans-serif;line-height:1.6}
.container{max-width:960px;margin:0 auto;padding:48px 24px}
h1{font-family:'IBM Plex Mono',monospace;font-size:1.8rem;font-weight:600;margin-bottom:4px}
.subtitle{color:#6a6a6a;font-size:1rem;margin-bottom:48px}
.tiers{display:grid;grid-template-columns:repeat(3,1fr);gap:20px;margin-bottom:56px}
@media(max-width:768px){.tiers{grid-template-columns:1fr}}
.tier{background:#fff;border:1px solid #d5d0c8;border-radius:6px;padding:28px 24px;display:flex;flex-direction:column}
.tier.featured{border-color:#c0392b;border-width:2px}
.tier h2{font-family:'IBM Plex Mono',monospace;font-size:1.1rem;font-weight:600;margin-bottom:4px}
.tier .price{font-size:0.85rem;color:#6a6a6a;margin-bottom:16px;min-height:40px}
.tier ul{list-style:none;flex:1;margin-bottom:20px}
.tier li{font-size:0.85rem;color:#3a3a3a;padding:4px 0;padding-left:16px;position:relative}
.tier li::before{content:"\\2713";position:absolute;left:0;color:#6a6a6a;font-size:0.75rem}
.tier .btn{display:inline-block;text-align:center;padding:10px 20px;border-radius:4px;font-family:'IBM Plex Mono',monospace;font-size:0.85rem;font-weight:500;text-decoration:none;cursor:pointer;border:1px solid #0a0a0a;background:transparent;color:#0a0a0a;transition:all 0.15s}
.tier .btn:hover{background:#0a0a0a;color:#f5f2ec}
.tier.featured .btn{background:#c0392b;color:#fff;border-color:#c0392b}
.tier.featured .btn:hover{background:#a5311f}
.pricing-note{font-size:0.75rem;color:#6a6a6a;margin-top:4px}
h3{font-family:'IBM Plex Mono',monospace;font-size:1.1rem;font-weight:600;margin-bottom:20px;padding-bottom:8px;border-bottom:1px solid #d5d0c8}
.endpoint-group{margin-bottom:28px}
.endpoint-group h4{font-family:'IBM Plex Sans',sans-serif;font-size:0.9rem;font-weight:600;color:#3a3a3a;margin-bottom:8px}
.endpoint{font-family:'IBM Plex Mono',monospace;font-size:0.8rem;color:#3a3a3a;padding:3px 0}
.endpoint .method{color:#c0392b;font-weight:500}
.footer-note{margin-top:40px;padding-top:20px;border-top:1px solid #d5d0c8;font-family:'IBM Plex Mono',monospace;font-size:0.8rem;color:#6a6a6a}
.footer-note code{background:#eae6de;padding:2px 6px;border-radius:3px;font-size:0.78rem}
/* Key generation form */
.keygen-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.4);z-index:100;align-items:center;justify-content:center}
.keygen-overlay.active{display:flex}
.keygen-box{background:#fff;border:1px solid #d5d0c8;border-radius:6px;padding:32px;max-width:420px;width:90%;position:relative}
.keygen-box h3{border:none;margin-bottom:16px;padding-bottom:0}
.keygen-box .close{position:absolute;top:12px;right:16px;cursor:pointer;font-size:1.2rem;color:#6a6a6a;background:none;border:none}
.keygen-box label{display:block;font-size:0.85rem;color:#3a3a3a;margin-bottom:4px;font-weight:500}
.keygen-box input{width:100%;padding:8px 12px;border:1px solid #d5d0c8;border-radius:4px;font-family:'IBM Plex Sans',sans-serif;font-size:0.9rem;margin-bottom:14px}
.keygen-box input:focus{outline:none;border-color:#c0392b}
.keygen-box .submit-btn{width:100%;padding:10px;background:#c0392b;color:#fff;border:none;border-radius:4px;font-family:'IBM Plex Mono',monospace;font-size:0.9rem;cursor:pointer;font-weight:500}
.keygen-box .submit-btn:hover{background:#a5311f}
.keygen-box .submit-btn:disabled{background:#999;cursor:not-allowed}
.key-result{display:none;margin-top:16px}
.key-result .key-display{background:#0a0a0a;color:#4ade80;padding:12px 16px;border-radius:4px;font-family:'IBM Plex Mono',monospace;font-size:0.82rem;word-break:break-all;margin:8px 0}
.key-result .copy-btn{padding:6px 14px;border:1px solid #d5d0c8;border-radius:4px;background:#fff;font-family:'IBM Plex Mono',monospace;font-size:0.8rem;cursor:pointer}
.key-result .copy-btn:hover{background:#eae6de}
.key-result .warning{font-size:0.8rem;color:#c0392b;margin-top:10px;font-weight:500}
.keygen-error{color:#c0392b;font-size:0.85rem;margin-top:8px;display:none}
.back-link{font-family:'IBM Plex Mono',monospace;font-size:0.85rem;color:#6a6a6a;text-decoration:none;display:inline-block;margin-bottom:24px}
.back-link:hover{color:#0a0a0a}
</style>
</head>
<body>
<div class="container">
<a href="/" class="back-link">&larr; Back to dashboard</a>
<h1>Basis Protocol API</h1>
<p class="subtitle">Risk primitives for on-chain finance</p>

<div class="tiers">
  <div class="tier">
    <h2>Free</h2>
    <div class="price">No key required</div>
    <ul>
      <li>10 requests / minute</li>
      <li>All public endpoints</li>
      <li>Stablecoin scores (SII)</li>
      <li>Protocol scores (PSI)</li>
      <li>Wallet risk profiles</li>
      <li>Composition queries (CQI)</li>
      <li>Daily pulse</li>
    </ul>
    <a href="/docs" class="btn">Explore the API &rarr;</a>
  </div>

  <div class="tier featured">
    <h2>Pro</h2>
    <div class="price">Free during beta<br><span class="pricing-note">Pricing starts when enterprise tiers launch</span></div>
    <ul>
      <li>120 requests / minute</li>
      <li>Everything in Free</li>
      <li>Higher rate limits</li>
      <li>API key for usage tracking</li>
      <li>Priority support</li>
    </ul>
    <button class="btn" onclick="openKeygen()">Get API Key</button>
  </div>

  <div class="tier">
    <h2>Enterprise</h2>
    <div class="price">Contact us</div>
    <ul>
      <li>Custom rate limits</li>
      <li>Historical data access</li>
      <li>Dedicated support</li>
      <li>SLA guarantees</li>
      <li>Custom index definitions</li>
      <li>Priority webhook delivery</li>
    </ul>
    <a href="mailto:shlok@basisprotocol.xyz" class="btn">Contact &rarr;</a>
  </div>
</div>

<h3>API Reference</h3>

<div class="endpoint-group">
  <h4>Stablecoin Integrity Index (SII)</h4>
  <div class="endpoint"><span class="method">GET</span> /api/scores &mdash; all scored stablecoins</div>
  <div class="endpoint"><span class="method">GET</span> /api/scores/{symbol} &mdash; detailed breakdown</div>
</div>

<div class="endpoint-group">
  <h4>Protocol Solvency Index (PSI)</h4>
  <div class="endpoint"><span class="method">GET</span> /api/psi/scores &mdash; all scored protocols</div>
  <div class="endpoint"><span class="method">GET</span> /api/psi/scores/{slug} &mdash; detailed breakdown</div>
</div>

<div class="endpoint-group">
  <h4>Collateral Quality Index (CQI)</h4>
  <div class="endpoint"><span class="method">GET</span> /api/compose/cqi?asset=usdc&amp;protocol=aave &mdash; single pair</div>
  <div class="endpoint"><span class="method">GET</span> /api/compose/cqi/matrix &mdash; all pairs</div>
</div>

<div class="endpoint-group">
  <h4>Wallet Risk</h4>
  <div class="endpoint"><span class="method">GET</span> /api/wallets/{address} &mdash; risk profile</div>
  <div class="endpoint"><span class="method">GET</span> /api/wallets/{address}/profile &mdash; full reputation primitive</div>
</div>

<div class="endpoint-group">
  <h4>Evidence Layer (CDA)</h4>
  <div class="endpoint"><span class="method">GET</span> /api/cda/issuers &mdash; all issuers</div>
  <div class="endpoint"><span class="method">GET</span> /api/cda/issuers/{symbol}/latest &mdash; latest attestation</div>
</div>

<div class="endpoint-group">
  <h4>Network State</h4>
  <div class="endpoint"><span class="method">GET</span> /api/pulse/latest &mdash; daily pulse</div>
  <div class="endpoint"><span class="method">GET</span> /api/methodology &mdash; formula and weights</div>
</div>

<p style="font-size:0.85rem;color:#6a6a6a;margin-top:8px">Full interactive docs: <a href="/docs" style="color:#c0392b">/docs</a></p>

<div class="footer-note">
  <strong>Authentication:</strong> pass your API key as <code>?apikey=YOUR_KEY</code> or <code>X-Api-Key: YOUR_KEY</code> header
</div>
</div>

<!-- Key generation modal -->
<div class="keygen-overlay" id="keygenOverlay">
  <div class="keygen-box">
    <button class="close" onclick="closeKeygen()">&times;</button>
    <h3>Generate API Key</h3>
    <form id="keygenForm" onsubmit="submitKeygen(event)">
      <label for="kg-name">App name</label>
      <input type="text" id="kg-name" placeholder="My App" required minlength="2" maxlength="100">
      <label for="kg-email">Email</label>
      <input type="email" id="kg-email" placeholder="dev@example.com" required>
      <button type="submit" class="submit-btn" id="kg-submit">Generate Key</button>
    </form>
    <div class="keygen-error" id="kg-error"></div>
    <div class="key-result" id="kg-result">
      <p style="font-size:0.9rem;font-weight:600;">Your API key:</p>
      <div class="key-display" id="kg-key"></div>
      <button class="copy-btn" onclick="copyKey()">Copy to clipboard</button>
      <p class="warning">Store this key securely. It will not be shown again.</p>
    </div>
  </div>
</div>

<script>
function openKeygen(){
  document.getElementById('keygenOverlay').classList.add('active');
  document.getElementById('keygenForm').style.display='block';
  document.getElementById('kg-result').style.display='none';
  document.getElementById('kg-error').style.display='none';
  document.getElementById('kg-name').value='';
  document.getElementById('kg-email').value='';
}
function closeKeygen(){
  document.getElementById('keygenOverlay').classList.remove('active');
}
document.getElementById('keygenOverlay').addEventListener('click',function(e){
  if(e.target===this) closeKeygen();
});
async function submitKeygen(e){
  e.preventDefault();
  var btn=document.getElementById('kg-submit');
  var err=document.getElementById('kg-error');
  btn.disabled=true; btn.textContent='Generating...';
  err.style.display='none';
  try{
    var res=await fetch('/api/keys/generate',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({
        name:document.getElementById('kg-name').value.trim(),
        email:document.getElementById('kg-email').value.trim()
      })
    });
    var data=await res.json();
    if(!res.ok){
      err.textContent=data.detail||data.error||'Something went wrong';
      err.style.display='block';
      btn.disabled=false; btn.textContent='Generate Key';
      return;
    }
    document.getElementById('kg-key').textContent=data.api_key;
    document.getElementById('keygenForm').style.display='none';
    document.getElementById('kg-result').style.display='block';
  }catch(ex){
    err.textContent='Network error. Please try again.';
    err.style.display='block';
    btn.disabled=false; btn.textContent='Generate Key';
  }
}
function copyKey(){
  var key=document.getElementById('kg-key').textContent;
  navigator.clipboard.writeText(key).then(function(){
    var btn=document.querySelector('.copy-btn');
    btn.textContent='Copied!';
    setTimeout(function(){btn.textContent='Copy to clipboard';},2000);
  });
}
</script>
</body>
</html>"""


@app.get("/developers")
async def developers_page():
    return HTMLResponse(content=DEVELOPERS_HTML)


# =============================================================================
# 14b. POST /api/keys/generate — Self-serve API key generation
# =============================================================================

import time as _time
from collections import defaultdict as _defaultdict

_keygen_requests: dict[str, list[float]] = _defaultdict(list)
_keygen_lock = __import__("threading").Lock()
_KEYGEN_LIMIT = 5
_KEYGEN_WINDOW = 3600  # 1 hour


def _check_keygen_rate(ip: str) -> bool:
    """Returns True if the IP is within the key-generation rate limit."""
    now = _time.time()
    cutoff = now - _KEYGEN_WINDOW
    with _keygen_lock:
        _keygen_requests[ip] = [t for t in _keygen_requests[ip] if t > cutoff]
        if len(_keygen_requests[ip]) >= _KEYGEN_LIMIT:
            return False
        _keygen_requests[ip].append(now)
        return True


@app.post("/api/keys/generate")
async def generate_api_key(request: Request):
    from app.usage_tracker import create_api_key
    from app.database import fetch_one, get_conn

    # Parse body
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    name = (body.get("name") or "").strip()
    email = (body.get("email") or "").strip()

    # Validate
    if not name or len(name) < 2 or len(name) > 100:
        raise HTTPException(status_code=400, detail="name is required (2-100 characters)")
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="A valid email is required")

    # Rate limit key generation per IP
    ip = request.client.host if request.client else "unknown"
    if not _check_keygen_rate(ip):
        raise HTTPException(status_code=429, detail="Too many key requests. Max 5 per hour.")

    # Check for duplicate active key with same name
    existing = fetch_one(
        "SELECT id FROM api_keys WHERE name = %s AND is_active = TRUE",
        (name,),
    )
    if existing:
        raise HTTPException(status_code=409, detail="An active key with this name already exists. Choose a different name.")

    # Ensure email/tier columns exist (idempotent migration)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS email VARCHAR(255)")
                cur.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS tier VARCHAR(20) DEFAULT 'pro'")
                conn.commit()
    except Exception:
        pass  # columns may already exist

    # Create key
    key_string = create_api_key(name)

    # Store email and tier
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE api_keys SET email = %s, tier = %s WHERE key = %s",
                    (email, "pro", key_string),
                )
                conn.commit()
    except Exception as e:
        logger.warning(f"Failed to store email for key: {e}")

    return {
        "api_key": key_string,
        "name": name,
        "tier": "pro",
        "rate_limit": "120 requests/minute",
        "message": "Store this key securely. It will not be shown again.",
        "docs": "/docs",
        "pricing_note": "Free during beta. Enterprise tiers coming soon.",
    }


# =============================================================================
# 15. GET /admin — Admin Panel HTML
# =============================================================================

ADMIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Basis Protocol — Admin Panel</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#06080d;color:#c8cdd8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;padding:20px;line-height:1.5}
h1{color:#22d3a7;font-size:1.6rem;margin-bottom:8px}
h2{color:#22d3a7;font-size:1.2rem;margin:24px 0 12px;border-bottom:1px solid #1a1f2e;padding-bottom:6px}
h3{color:#a0a8b8;font-size:0.95rem;margin:14px 0 8px}
.header{display:flex;align-items:center;gap:12px;margin-bottom:20px;border-bottom:1px solid #1a1f2e;padding-bottom:12px}
.header .dot{width:10px;height:10px;border-radius:50%;display:inline-block}
.dot.green{background:#22d3a7}.dot.red{background:#ef4444}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
@media(max-width:900px){.grid{grid-template-columns:1fr}}
.card{background:#0d1117;border:1px solid #1a1f2e;border-radius:8px;padding:16px}
table{width:100%;border-collapse:collapse;font-size:0.85rem;margin-top:8px}
th{text-align:left;color:#8b95a5;font-weight:500;padding:6px 8px;border-bottom:1px solid #1a1f2e}
td{padding:6px 8px;border-bottom:1px solid #111520}
.tag{display:inline-block;padding:2px 8px;border-radius:10px;font-size:0.75rem;margin:1px 2px}
.tag-neg{background:#3b1420;color:#f87171}.tag-pos{background:#0d2818;color:#4ade80}
.tag-neu{background:#1a1f2e;color:#8b95a5}.tag-con{background:#2d1b00;color:#f59e0b}
.stale{color:#ef4444;font-weight:600}
.fresh{color:#22d3a7}
.stat{font-size:1.8rem;font-weight:700;color:#22d3a7}
.stat-label{font-size:0.8rem;color:#8b95a5}
.stats-row{display:flex;gap:24px;flex-wrap:wrap;margin:8px 0}
.signal{background:#0d1117;border:1px solid #1a1f2e;border-radius:8px;padding:14px;margin-bottom:10px}
.signal-title{color:#e2e8f0;font-weight:600;margin-bottom:4px}
.signal-meta{font-size:0.8rem;color:#8b95a5;margin-bottom:6px}
.signal-angle{color:#3b82f6;font-size:0.9rem;font-style:italic}
.sii-chip{display:inline-block;background:#1a1f2e;padding:2px 8px;border-radius:4px;font-size:0.8rem;margin:2px}
.loading{color:#8b95a5;padding:20px;text-align:center}
.error{color:#ef4444;padding:10px}
</style>
</head>
<body>
<div class="header">
  <h1>BASIS Admin Panel</h1>
  <span id="statusDot" class="dot"></span>
  <span id="statusText" style="font-size:0.85rem;color:#8b95a5"></span>
</div>

<div class="grid">
  <div class="card" id="healthCard"><div class="loading">Loading health...</div></div>
  <div class="card" id="statsCard"><div class="loading">Loading stats...</div></div>
</div>

<h2>Data Freshness</h2>
<div class="card" id="freshnessCard"><div class="loading">Loading freshness...</div></div>

<h2>Governance Intelligence</h2>
<div class="grid">
  <div class="card" id="govStatsCard"><div class="loading">Loading governance...</div></div>
  <div class="card" id="govDocsCard"><div class="loading">Loading documents...</div></div>
</div>

<h2>Content Signals</h2>
<div id="signalsCard"><div class="loading">Loading signals...</div></div>

<h2>Protocol State</h2>
<div class="grid">
  <div class="card" id="pulseCard"><div class="loading">Loading pulse...</div></div>
  <div class="card" id="psiCard"><div class="loading">Loading PSI...</div></div>
</div>
<div class="grid" style="margin-top:16px">
  <div class="card" id="cdaCard"><div class="loading">Loading CDA...</div></div>
  <div class="card" id="eventsCard"><div class="loading">Loading events...</div></div>
</div>

<script>
const KEY = new URLSearchParams(window.location.search).get('key') || '';
const api = (path) => fetch('/api/admin/' + path + '?key=' + encodeURIComponent(KEY)).then(r => {
  if (!r.ok) throw new Error(r.status === 401 ? 'Unauthorized' : 'API error ' + r.status);
  return r.json();
});

function fmtDate(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});
}

function sentTag(s, count) {
  const cls = {negative:'tag-neg',positive:'tag-pos',neutral:'tag-neu',concerned:'tag-con'}[s] || 'tag-neu';
  return '<span class="tag '+cls+'">'+s+' ('+count+')</span>';
}

async function loadHealth() {
  try {
    const d = await api('health');
    document.getElementById('statusDot').className = 'dot ' + (d.status === 'healthy' ? 'green' : 'red');
    document.getElementById('statusText').textContent = d.status;
    let h = '<h3>System Health</h3><div class="stats-row">';
    h += '<div><div class="stat">'+d.scores.stablecoins_scored+'</div><div class="stat-label">Scored</div></div>';
    h += '<div><div class="stat">'+d.scores.stablecoins_registered+'</div><div class="stat-label">Registered</div></div>';
    h += '</div>';
    h += '<table><tr><th>Metric</th><th>Value</th></tr>';
    h += '<tr><td>Last Scored</td><td>'+fmtDate(d.scores.last_computed)+'</td></tr>';
    h += '<tr><td>Formula</td><td>'+d.formula_version+'</td></tr>';
    h += '<tr><td>Stale Readings</td><td>'+(d.scoring_issues.stale_readings||0)+'</td></tr>';
    h += '<tr><td>Error Readings</td><td>'+(d.scoring_issues.error_readings||0)+'</td></tr>';
    if (d.last_governance_crawl) {
      h += '<tr><td>Last Crawl</td><td>'+fmtDate(d.last_governance_crawl.completed_at)+' ('+d.last_governance_crawl.source+')</td></tr>';
    }
    h += '</table>';
    document.getElementById('healthCard').innerHTML = h;

    let s = '<h3>Component Coverage</h3><table><tr><th>Source</th><th>Coins</th><th>Readings</th></tr>';
    (d.component_coverage||[]).forEach(c => {
      s += '<tr><td>'+c.source+'</td><td>'+c.stablecoin_count+'</td><td>'+c.reading_count+'</td></tr>';
    });
    s += '</table><h3>Table Sizes</h3><table><tr><th>Table</th><th>Rows</th></tr>';
    Object.entries(d.table_row_counts||{}).forEach(([t,n]) => {
      s += '<tr><td>'+t+'</td><td>'+n.toLocaleString()+'</td></tr>';
    });
    s += '</table>';
    document.getElementById('statsCard').innerHTML = s;
  } catch(e) { document.getElementById('healthCard').innerHTML = '<div class="error">'+e.message+'</div>'; }
}

async function loadFreshness() {
  try {
    const d = await api('freshness');
    let h = '<p style="font-size:0.85rem;color:#8b95a5;margin-bottom:8px">Last worker: '+fmtDate(d.last_worker_run)+' &bull; Next: '+fmtDate(d.next_expected)+'</p>';
    h += '<table><tr><th>Coin</th><th>Last Scored</th><th>Components</th><th>Peg</th><th>Liquidity</th><th>Flows</th><th>Distribution</th><th>Structural</th><th>Sources</th></tr>';
    (d.stablecoins||[]).forEach(s => {
      h += '<tr><td><b>'+s.id.toUpperCase()+'</b></td>';
      h += '<td>'+fmtDate(s.last_scored)+'</td>';
      h += '<td>'+s.component_count+'</td>';
      ['peg','liquidity','flows','distribution','structural'].forEach(cat => {
        const c = s.categories[cat];
        if (!c) { h += '<td class="stale">MISSING</td>'; return; }
        const cls = c.avg_score === null ? 'stale' : 'fresh';
        h += '<td class="'+cls+'">'+(c.avg_score !== null ? c.avg_score.toFixed(1) : 'NULL')+' <span style="color:#8b95a5;font-size:0.75rem">('+c.count+')</span></td>';
      });
      h += '<td style="font-size:0.8rem">'+s.sources.join(', ')+'</td></tr>';
    });
    h += '</table>';
    if (d.stablecoins.some(s => s.null_categories.length > 0)) {
      h += '<p class="stale" style="margin-top:8px;font-size:0.85rem">&#9888; Some categories have null or missing data</p>';
    }
    document.getElementById('freshnessCard').innerHTML = h;
  } catch(e) { document.getElementById('freshnessCard').innerHTML = '<div class="error">'+e.message+'</div>'; }
}

async function loadGovernance() {
  try {
    const d = await api('governance/stats');
    let h = '<h3>Overview</h3><div class="stats-row">';
    h += '<div><div class="stat">'+d.total_documents+'</div><div class="stat-label">Documents</div></div>';
    h += '<div><div class="stat">'+d.total_stablecoin_mentions.toLocaleString()+'</div><div class="stat-label">Coin Mentions</div></div>';
    h += '<div><div class="stat">'+d.total_metric_mentions.toLocaleString()+'</div><div class="stat-label">Metric Mentions</div></div>';
    h += '</div>';
    h += '<h3>Sources</h3><table><tr><th>Forum</th><th>Docs</th></tr>';
    (d.sources||[]).forEach(s => { h += '<tr><td>'+s.source+'</td><td>'+s.count+'</td></tr>'; });
    h += '</table>';
    h += '<h3>Top Mentioned Stablecoins</h3>';
    (d.top_stablecoin_mentions||[]).forEach(m => {
      h += '<div style="margin:4px 0"><b>'+m.stablecoin.toUpperCase()+'</b> ('+m.count+') ';
      const sb = m.sentiment_breakdown;
      if (sb.negative) h += sentTag('negative', sb.negative);
      if (sb.concerned) h += sentTag('concerned', sb.concerned);
      if (sb.neutral) h += sentTag('neutral', sb.neutral);
      if (sb.positive) h += sentTag('positive', sb.positive);
      h += '</div>';
    });
    document.getElementById('govStatsCard').innerHTML = h;

    let rd = '<h3>Recent Documents</h3><table><tr><th>Title</th><th>Source</th><th>Date</th><th>Coins</th></tr>';
    (d.recent_documents||[]).slice(0,15).forEach(doc => {
      rd += '<tr><td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+((doc.title||'').substring(0,60))+'</td>';
      rd += '<td>'+doc.source+'</td>';
      rd += '<td>'+fmtDate(doc.published_at)+'</td>';
      rd += '<td>'+(doc.stablecoins_mentioned||[]).join(', ')+'</td></tr>';
    });
    rd += '</table>';
    document.getElementById('govDocsCard').innerHTML = rd;
  } catch(e) { document.getElementById('govStatsCard').innerHTML = '<div class="error">'+e.message+'</div>'; }
}

async function loadSignals() {
  try {
    const d = await api('content/signals');
    if (!d.signals || d.signals.length === 0) {
      document.getElementById('signalsCard').innerHTML = '<div class="card"><p style="color:#8b95a5">No content signals available</p></div>';
      return;
    }
    let h = '';
    d.signals.forEach(s => {
      h += '<div class="signal">';
      h += '<div class="signal-title">'+s.governance_topic+'</div>';
      h += '<div class="signal-meta">'+s.source+' &bull; '+fmtDate(s.published_at)+' &bull; '+s.mention_count+' mentions &bull; ';
      h += sentTag(s.sentiment, '')+' </div>';
      const sii = s.relevant_sii_data || {};
      Object.entries(sii).forEach(([k,v]) => {
        if (typeof v === 'number') h += '<span class="sii-chip">'+k+': '+v+'</span> ';
        else if (typeof v === 'string') h += '<span class="sii-chip">'+k+': '+v+'</span> ';
      });
      h += '<div class="signal-angle" style="margin-top:6px">'+s.suggested_angle+'</div>';
      h += '</div>';
    });
    document.getElementById('signalsCard').innerHTML = h;
  } catch(e) { document.getElementById('signalsCard').innerHTML = '<div class="error">'+e.message+'</div>'; }
}

async function loadPulse() {
  try {
    const d = await fetch('/api/pulse/latest').then(r=>r.json());
    const today = new Date().toISOString().slice(0,10);
    const pd = d.pulse_date || 'none';
    const cur = pd === today;
    const hash = (d.content_hash || '').slice(0,18);
    let h = '<h3>Pulse Status</h3>';
    h += '<div class="stats-row"><div><span class="dot '+(cur?'green':'red')+'"></span> ';
    h += '<b>'+pd+'</b> '+(cur?'<span class="fresh">(current)</span>':'<span class="stale">(STALE)</span>')+'</div></div>';
    if (hash) h += '<div style="font-size:0.8rem;color:#8b95a5;margin-top:4px">Hash: '+hash+'…</div>';
    const s = typeof d.summary==='string'?JSON.parse(d.summary):d.summary;
    if (s && s.network_state) {
      const n = s.network_state;
      h += '<table><tr><th>Metric</th><th>Value</th></tr>';
      h += '<tr><td>Wallets indexed</td><td>'+(n.wallets_indexed||'—')+'</td></tr>';
      h += '<tr><td>Avg risk</td><td>'+(n.avg_risk_score!=null?n.avg_risk_score.toFixed(1):'—')+'</td></tr>';
      h += '<tr><td>Tracked USD</td><td>'+(n.total_tracked_usd?'$'+(n.total_tracked_usd/1e9).toFixed(1)+'B':'—')+'</td></tr>';
      h += '</table>';
    }
    document.getElementById('pulseCard').innerHTML = h;
  } catch(e) { document.getElementById('pulseCard').innerHTML = '<div class="error">Pulse: '+e.message+'</div>'; }
}

async function loadPSI() {
  try {
    const d = await fetch('/api/psi/scores').then(r=>r.json());
    const protos = d.protocols || d || [];
    let h = '<h3>PSI Scoring</h3>';
    h += '<div class="stats-row"><div><div class="stat">'+protos.length+'</div><div class="stat-label">Protocols Scored</div></div></div>';
    if (protos.length) {
      h += '<table><tr><th>Protocol</th><th>Score</th><th>Grade</th></tr>';
      protos.sort((a,b)=>(b.overall_score||0)-(a.overall_score||0)).forEach(p => {
        h += '<tr><td>'+(p.protocol_name||p.protocol_slug)+'</td>';
        h += '<td>'+(p.overall_score!=null?p.overall_score.toFixed(1):'—')+'</td>';
        h += '<td>'+(p.grade||'—')+'</td></tr>';
      });
      h += '</table>';
    }
    document.getElementById('psiCard').innerHTML = h;
  } catch(e) { document.getElementById('psiCard').innerHTML = '<div class="error">PSI: '+e.message+'</div>'; }
}

async function loadCDA() {
  try {
    const d = await fetch('/api/cda/issuers').then(r=>r.json());
    const issuers = d.issuers || [];
    const now = Date.now();
    const WEEK = 7*24*60*60*1000;
    let h = '<h3>CDA Pipeline</h3>';
    h += '<div class="stats-row"><div><div class="stat">'+issuers.length+'</div><div class="stat-label">Issuers Tracked</div></div></div>';
    if (issuers.length) {
      h += '<table><tr><th>Asset</th><th>Issuer</th><th>Last Attestation</th></tr>';
      issuers.forEach(i => {
        const la = i.last_attestation_date || i.extracted_at;
        const stale = la && (now - new Date(la).getTime()) > WEEK;
        h += '<tr><td><b>'+(i.asset_symbol||'—')+'</b></td>';
        h += '<td>'+(i.issuer_name||'—')+'</td>';
        h += '<td class="'+(stale?'stale':'fresh')+'">'+(la?fmtDate(la):'—')+'</td></tr>';
      });
      h += '</table>';
    }
    document.getElementById('cdaCard').innerHTML = h;
  } catch(e) { document.getElementById('cdaCard').innerHTML = '<div class="error">CDA: '+e.message+'</div>'; }
}

async function loadEvents() {
  try {
    const d = await fetch('/api/assessment-events?severity=notable&limit=10').then(r=>r.json());
    const evts = d.events || [];
    let h = '<h3>Assessment Events (24h)</h3>';
    const counts = {};
    evts.forEach(e => { counts[e.severity] = (counts[e.severity]||0)+1; });
    h += '<div style="margin:6px 0">';
    Object.entries(counts).forEach(([s,c]) => {
      const cls = s==='critical'?'tag-neg':s==='alert'?'tag-con':'tag-neu';
      h += '<span class="tag '+cls+'">'+s+': '+c+'</span> ';
    });
    if (!evts.length) h += '<span style="color:#8b95a5">No notable events</span>';
    h += '</div>';
    if (evts.length) {
      h += '<table><tr><th>Severity</th><th>Trigger</th><th>Wallet</th><th>Score</th></tr>';
      evts.slice(0,10).forEach(e => {
        const addr = e.wallet_address||e.wallet||'';
        const short = addr ? addr.slice(0,8)+'…'+addr.slice(-6) : '—';
        h += '<tr><td><span class="tag '+(e.severity==='critical'?'tag-neg':e.severity==='alert'?'tag-con':'tag-neu')+'">'+e.severity+'</span></td>';
        h += '<td>'+(e.trigger||e.event_type||'—')+'</td>';
        h += '<td style="font-family:monospace;font-size:0.8rem">'+short+'</td>';
        h += '<td>'+(e.score!=null?Number(e.score).toFixed(1):'—')+'</td></tr>';
      });
      h += '</table>';
    }
    document.getElementById('eventsCard').innerHTML = h;
  } catch(e) { document.getElementById('eventsCard').innerHTML = '<div class="error">Events: '+e.message+'</div>'; }
}

loadHealth(); loadFreshness(); loadGovernance(); loadSignals();
loadPulse(); loadPSI(); loadCDA(); loadEvents();
</script>
</body>
</html>"""


# =============================================================================
# CDA Vendor Integration — Test endpoint (Phase 1)
# =============================================================================

@app.get("/api/cda/test-vendors")
async def test_vendors():
    """Test both vendor integrations. For development only."""
    from app.services import parallel_client, reducto_client

    results = {}

    # Test Parallel Extract on Circle
    results["parallel_extract"] = await parallel_client.extract(
        "https://www.circle.com/transparency",
        objective="Find reserve attestation data, PDF report links, total reserves amount"
    )

    parallel_ok = "error" not in results["parallel_extract"]
    results["parallel_status"] = "ok" if parallel_ok else "failed"
    results["reducto_status"] = "not_tested_yet"

    return results


@app.get("/api/cda/issuer-registry")
async def get_issuer_registry():
    """Return current CDA issuer registry."""
    rows = fetch_all("SELECT * FROM cda_issuer_registry ORDER BY asset_symbol")
    return {"issuers": rows, "count": len(rows)}


@app.get("/api/cda/quality")
async def cda_quality():
    """Data quality dashboard — freshness, confidence, and status per issuer."""
    from datetime import datetime, timezone

    issuers = fetch_all(
        "SELECT * FROM cda_issuer_registry WHERE is_active = TRUE ORDER BY asset_symbol"
    )

    # Get latest extraction per asset (prefer PDF over web)
    latest_extractions = fetch_all("""
        SELECT DISTINCT ON (asset_symbol)
            asset_symbol, structured_data, confidence_score, extracted_at,
            extraction_method, source_type
        FROM cda_vendor_extractions
        WHERE structured_data IS NOT NULL
        ORDER BY asset_symbol,
            CASE WHEN source_type = 'pdf_attestation' THEN 0 ELSE 1 END,
            extracted_at DESC
    """)
    ext_map = {e["asset_symbol"]: e for e in latest_extractions}

    quality = []
    summary = {
        "total_issuers": len(issuers),
        "with_data": 0, "on_chain_only": 0, "no_data": 0,
        "current": 0, "stale": 0, "overdue": 0,
    }

    now = datetime.now(timezone.utc)

    for iss in issuers:
        symbol = iss["asset_symbol"]
        entry = {
            "asset": symbol,
            "issuer": iss["issuer_name"],
            "collection_method": iss["collection_method"],
        }

        if iss["collection_method"] == "nav_oracle":
            entry["has_data"] = False
            entry["status"] = "on_chain_only"
            summary["on_chain_only"] += 1
            quality.append(entry)
            continue

        ext = ext_map.get(symbol)
        if not ext:
            entry["has_data"] = False
            entry["status"] = "no_data"
            summary["no_data"] += 1
            quality.append(entry)
            continue

        entry["has_data"] = True
        entry["latest_extraction"] = ext["extracted_at"].isoformat() if ext.get("extracted_at") else None
        entry["confidence"] = float(ext["confidence_score"]) if ext.get("confidence_score") else None
        entry["extraction_method"] = ext.get("extraction_method")
        summary["with_data"] += 1

        sd = ext.get("structured_data", {}) or {}
        att_date_str = sd.get("attestation_date")
        reserves = sd.get("total_reserves_usd")

        entry["attestation_date"] = att_date_str
        entry["total_reserves_usd"] = reserves

        if att_date_str:
            try:
                att_date = datetime.fromisoformat(str(att_date_str).replace("Z", "+00:00"))
                if att_date.tzinfo is None:
                    att_date = att_date.replace(tzinfo=timezone.utc)
                days = (now - att_date).days
                entry["days_since_attestation"] = days
                if days <= 45:
                    entry["status"] = "current"
                    summary["current"] += 1
                elif days <= 90:
                    entry["status"] = "stale"
                    summary["stale"] += 1
                else:
                    entry["status"] = "overdue"
                    summary["overdue"] += 1
            except (ValueError, TypeError):
                entry["status"] = "no_data"
        else:
            entry["status"] = "no_data"

        quality.append(entry)

    return {"quality": quality, "summary": summary}


@app.get("/api/cda/attestations/{asset_symbol}")
async def cda_attestations(asset_symbol: str):
    """Extraction history for one asset."""
    rows = fetch_all(
        """
        SELECT id, source_url, source_type, extraction_method, extraction_vendor,
               confidence_score, structured_data, extraction_warnings, extracted_at
        FROM cda_vendor_extractions
        WHERE UPPER(asset_symbol) = %s
        ORDER BY extracted_at DESC
        LIMIT 50
        """,
        (asset_symbol.upper(),),
    )
    return {"asset": asset_symbol.upper(), "extractions": rows}


@app.post("/api/admin/collect-cda")
async def trigger_cda_collection(key: str = Query(default=None)):
    """Manually trigger CDA collection pipeline. Requires admin key."""
    admin_key = os.environ.get("ADMIN_KEY", "")
    if not admin_key or key != admin_key:
        raise HTTPException(status_code=401, detail="Unauthorized — provide ?key=YOUR_ADMIN_KEY")

    import asyncio as _asyncio
    from app.services.cda_collector import run_collection
    _asyncio.create_task(run_collection())
    return {"status": "collection_started"}


@app.get("/api/cda/monitors")
async def get_monitors():
    """List active CDA monitor watches."""
    rows = fetch_all(
        "SELECT * FROM cda_monitors WHERE is_active = TRUE ORDER BY asset_symbol"
    )
    return {"monitors": rows, "count": len(rows)}


@app.post("/api/admin/setup-monitors")
async def setup_cda_monitors(key: str = Query(default=None)):
    """Create Parallel Monitor watches for all web_extract issuers. Requires admin key."""
    admin_key = os.environ.get("ADMIN_KEY", "")
    if not admin_key or key != admin_key:
        raise HTTPException(status_code=401, detail="Unauthorized — provide ?key=YOUR_ADMIN_KEY")

    from app.services.cda_collector import setup_monitors
    count = await setup_monitors()
    return {"status": "ok", "monitors_created": count}


@app.post("/api/cda/webhook/monitor")
async def handle_monitor_alert(request: Request):
    """
    Webhook for Parallel Monitor alerts.
    When an issuer updates their transparency page, trigger immediate CDA collection.
    """
    body = await request.json()

    # Parallel may send monitor_id, id, or watch_id — check all
    monitor_id = body.get("monitor_id") or body.get("id") or body.get("watch_id") or ""

    if not monitor_id:
        return {"status": "no_monitor_id"}

    issuer = fetch_one(
        "SELECT * FROM cda_issuer_registry WHERE parallel_monitor_id = %s",
        (monitor_id,),
    )

    if issuer:
        import asyncio as _asyncio
        from app.services.cda_collector import collect_single_issuer
        _asyncio.create_task(collect_single_issuer(issuer["asset_symbol"]))

        execute(
            "UPDATE cda_monitors SET last_alert_at = NOW() WHERE parallel_monitor_id = %s",
            (monitor_id,),
        )

        return {"status": "collected", "asset": issuer["asset_symbol"]}

    return {"status": "unknown_monitor", "monitor_id": monitor_id}


# =============================================================================
# CDA Public Evidence Layer API
# =============================================================================

@app.get("/api/cda")
async def cda_overview():
    """Public documentation/overview of the CDA evidence layer."""
    return {
        "name": "Basis Protocol Evidence Layer (CDA)",
        "version": "1.0.0",
        "description": "Structured, timestamped disclosure data for stablecoin issuers. Build your own index on this evidence.",
        "endpoints": [
            "GET /api/cda/issuers",
            "GET /api/cda/issuers/{symbol}/latest",
            "GET /api/cda/issuers/{symbol}/history",
            "GET /api/cda/coverage"
        ],
        "license": "Open data. Attribution requested.",
        "update_frequency": "Daily before SII scoring cycle"
    }


@app.get("/api/cda/issuers")
async def cda_issuers():
    """List all CDA-tracked issuers with last attestation date."""
    rows = fetch_all("""
        SELECT r.asset_symbol, r.issuer_name, r.transparency_url,
               r.collection_method, r.created_at,
               (
                   SELECT MAX(e.extracted_at)
                   FROM cda_vendor_extractions e
                   WHERE e.asset_symbol = r.asset_symbol
               ) AS last_attestation
        FROM cda_issuer_registry r
        WHERE r.is_active = TRUE
        ORDER BY r.asset_symbol
    """)
    issuers = []
    for r in rows:
        issuers.append({
            "asset_symbol": r["asset_symbol"],
            "issuer_name": r["issuer_name"],
            "transparency_url": r["transparency_url"],
            "collection_method": r["collection_method"],
            "last_attestation": r["last_attestation"].isoformat() if r.get("last_attestation") else None,
        })
    return {"issuers": issuers, "count": len(issuers)}


@app.get("/api/cda/issuers/{symbol}/latest")
async def cda_issuer_latest(symbol: str):
    """Most recent attestation for a specific issuer, with evidence hash."""
    import hashlib
    import json as _json

    row = fetch_one("""
        SELECT id, asset_symbol, source_url, source_type, extraction_method,
               extraction_vendor, structured_data, confidence_score,
               extraction_warnings, extracted_at
        FROM cda_vendor_extractions
        WHERE UPPER(asset_symbol) = %s
          AND structured_data IS NOT NULL
        ORDER BY extracted_at DESC
        LIMIT 1
    """, (symbol.upper(),))

    if not row:
        raise HTTPException(status_code=404, detail=f"No attestation data found for {symbol.upper()}")

    attestation = {
        "id": row["id"],
        "asset_symbol": row["asset_symbol"],
        "source_url": row["source_url"],
        "source_type": row["source_type"],
        "extraction_method": row["extraction_method"],
        "extraction_vendor": row["extraction_vendor"],
        "structured_data": row["structured_data"],
        "confidence_score": float(row["confidence_score"]) if row.get("confidence_score") else None,
        "extraction_warnings": row.get("extraction_warnings"),
        "extracted_at": row["extracted_at"].isoformat() if row.get("extracted_at") else None,
    }

    raw_data = _json.dumps(attestation, sort_keys=True, separators=(',', ':'), default=str)
    evidence_hash = '0x' + hashlib.sha256(raw_data.encode()).hexdigest()
    attestation["evidence_hash"] = evidence_hash

    return attestation


@app.get("/api/cda/issuers/{symbol}/history")
async def cda_issuer_history(symbol: str, days: int = Query(default=90, ge=1, le=365)):
    """Attestation history for a specific issuer."""
    rows = fetch_all("""
        SELECT id, asset_symbol, source_url, source_type, extraction_method,
               extraction_vendor, structured_data, confidence_score,
               extraction_warnings, extracted_at
        FROM cda_vendor_extractions
        WHERE UPPER(asset_symbol) = %s
          AND extracted_at > NOW() - INTERVAL '%s days'
        ORDER BY extracted_at DESC
    """, (symbol.upper(), days))

    attestations = []
    for r in rows:
        attestations.append({
            "id": r["id"],
            "asset_symbol": r["asset_symbol"],
            "source_url": r["source_url"],
            "source_type": r["source_type"],
            "extraction_method": r["extraction_method"],
            "extraction_vendor": r["extraction_vendor"],
            "structured_data": r["structured_data"],
            "confidence_score": float(r["confidence_score"]) if r.get("confidence_score") else None,
            "extraction_warnings": r.get("extraction_warnings"),
            "extracted_at": r["extracted_at"].isoformat() if r.get("extracted_at") else None,
        })

    return {
        "asset": symbol.upper(),
        "days": days,
        "attestations": attestations,
        "count": len(attestations),
    }


@app.get("/api/cda/coverage")
async def cda_coverage():
    """Coverage summary of the CDA evidence layer."""
    issuers = fetch_all(
        "SELECT asset_symbol, collection_method, asset_category FROM cda_issuer_registry WHERE is_active = TRUE ORDER BY asset_symbol"
    )

    # Count by collection method
    method_counts: dict = {}
    fiat_covered = []
    for iss in issuers:
        method = iss.get("collection_method") or "unknown"
        method_counts[method] = method_counts.get(method, 0) + 1
        category = (iss.get("asset_category") or "").lower()
        if category in ("fiat_backed", "fiat-backed", "fiat") or method != "nav_oracle":
            fiat_covered.append(iss["asset_symbol"])

    # Total known fiat-backed from stablecoin registry
    all_coins = fetch_all("SELECT id, symbol FROM stablecoins")
    total_fiat = len(all_coins)

    gaps = []
    covered_symbols = {i["asset_symbol"] for i in issuers}
    for coin in all_coins:
        sym = coin["symbol"].upper() if coin.get("symbol") else coin["id"].upper()
        if sym not in covered_symbols:
            gaps.append(f"{sym} — not yet in CDA pipeline")

    return {
        "fiat_backed": {
            "covered": len(fiat_covered),
            "total": total_fiat,
            "issuers": sorted(fiat_covered),
            "gaps": gaps,
        },
        "crypto_backed": {
            "note": "On-chain data used directly via smart contract reads, no attestation pipeline needed"
        },
        "collection_methods": method_counts,
        "update_frequency": "daily",
        "vendors": ["parallel.ai", "reducto", "firecrawl"],
    }


# =============================================================================
# Severity spec + assessment events
# =============================================================================

@app.get("/api/specs/severity")
async def get_severity_spec():
    """Published severity taxonomy for assessment events."""
    from app.specs.severity_v1 import SEVERITY_V1
    return SEVERITY_V1


@app.get("/api/specs/wallet-profile")
async def get_wallet_profile_spec():
    """Published wallet profile schema — reputation primitive definition."""
    from app.specs.wallet_profile_v1 import WALLET_PROFILE_SCHEMA_V1
    return WALLET_PROFILE_SCHEMA_V1


@app.get("/api/assessment-events")
async def get_assessment_events(
    severity: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
):
    """Assessment events with optional severity filtering."""
    from app.specs.severity_v1 import SEVERITY_ORDER

    if severity and severity not in SEVERITY_ORDER:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown severity '{severity}'. Valid: {', '.join(SEVERITY_ORDER.keys())}",
        )

    ordinal = SEVERITY_ORDER.get(severity, 0) if severity else 0

    rows = fetch_all("""
        SELECT * FROM assessment_events
        WHERE severity_ordinal >= %s
        ORDER BY created_at DESC
        LIMIT %s
    """, (ordinal, limit))

    events = []
    for row in rows:
        d = dict(row)
        for k, v in d.items():
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
            elif isinstance(v, (Decimal,)):
                d[k] = float(v)
            elif isinstance(v, uuid_mod.UUID):
                d[k] = str(v)
        events.append(d)

    return {
        "events": events,
        "count": len(events),
        "filter": {"severity_gte": severity} if severity else {},
        "taxonomy_version": "1.0.0",
    }


@app.get("/api/assessments/recent")
async def recent_assessments(
    limit: int = Query(default=10, ge=1, le=100),
    verified: Optional[bool] = Query(default=None),
):
    """Recent assessment events with verification status."""
    rows = fetch_all("""
        SELECT ae.id, ae.wallet_address, ae.wallet_risk_score, ae.severity,
               ae.created_at, ae.inputs_hash,
               iv.inputs_hash AS iv_hash, iv.holdings, iv.stablecoin_scores
        FROM assessment_events ae
        LEFT JOIN assessment_input_vectors iv ON iv.assessment_id = ae.id
        ORDER BY ae.created_at DESC
        LIMIT %s
    """, (limit,))

    assessments = []
    verified_count = 0
    for r in rows:
        has_vector = r.get("iv_hash") is not None
        v_status = "inputs_not_available"

        if has_vector:
            # Quick re-verify: recompute score from stored vector
            holdings = r["holdings"] if isinstance(r.get("holdings"), list) else []
            scores_map = r["stablecoin_scores"] if isinstance(r.get("stablecoin_scores"), dict) else {}
            scored = []
            for h in holdings:
                sym = h.get("symbol", "")
                val = float(h.get("value_usd", 0))
                sii = h.get("sii_score")
                if sii is None and sym in scores_map:
                    sii = scores_map[sym].get("score")
                if sii is not None and val > 0:
                    scored.append((val, float(sii)))
            if scored:
                total_val = sum(v for v, _ in scored)
                if total_val > 0:
                    recomputed = round(sum(v * s for v, s in scored) / total_val, 2)
                    stored = float(r["wallet_risk_score"]) if r.get("wallet_risk_score") else None
                    if stored is not None and abs(recomputed - stored) < 0.01:
                        v_status = "verified"
                        verified_count += 1
                    else:
                        v_status = "mismatch"
                else:
                    v_status = "verified"
                    verified_count += 1
            else:
                v_status = "verified"
                verified_count += 1

        assessments.append({
            "id": str(r["id"]),
            "wallet_address": r.get("wallet_address"),
            "risk_score": float(r["wallet_risk_score"]) if r.get("wallet_risk_score") else None,
            "severity": r.get("severity"),
            "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
            "verification": v_status,
            "inputs_hash": r.get("inputs_hash"),
        })

    total = len(assessments)
    return {
        "assessments": assessments,
        "verified_count": verified_count,
        "total_count": total,
        "verification_rate": round(verified_count / total, 2) if total > 0 else 0,
    }


@app.get("/api/assessments/{assessment_id}/inputs")
async def get_assessment_inputs(assessment_id: str):
    """Computation attestation: retrieve input hash and summary for an assessment event."""
    row = fetch_one("""
        SELECT id, inputs_hash, inputs_summary, content_hash, methodology_version,
               wallet_risk_score, severity, created_at
        FROM assessment_events
        WHERE id = %s::uuid
    """, (assessment_id,))

    if not row:
        raise HTTPException(status_code=404, detail="Assessment event not found")

    has_hash = row.get("inputs_hash") is not None
    return {
        "assessment_id": str(row["id"]),
        "inputs_hash": row.get("inputs_hash"),
        "inputs_summary": row.get("inputs_summary"),
        "content_hash": row.get("content_hash"),
        "methodology_version": row.get("methodology_version"),
        "wallet_risk_score": float(row["wallet_risk_score"]) if row.get("wallet_risk_score") else None,
        "severity": row.get("severity"),
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "verification_status": "hash_available" if has_hash else "hash_not_available",
        "note": "Full input vector retrieval available in v2. Hash can be verified against on-chain anchor.",
    }


@app.get("/api/assessments/{assessment_id}/verify")
async def verify_assessment(assessment_id: str):
    """Full computation attestation: verify an assessment by re-deriving the score from stored inputs."""
    from app.computation_attestation import compute_inputs_hash
    from app.scoring import score_to_grade

    # 1. Fetch assessment event
    ae = fetch_one("""
        SELECT id, wallet_address, wallet_risk_score, content_hash, inputs_hash,
               methodology_version, severity, created_at
        FROM assessment_events
        WHERE id = %s::uuid
    """, (assessment_id,))
    if not ae:
        raise HTTPException(status_code=404, detail="Assessment event not found")

    stored_score = float(ae["wallet_risk_score"]) if ae.get("wallet_risk_score") else None

    # 2. Fetch input vector
    iv = fetch_one("""
        SELECT wallet_address, holdings, stablecoin_scores, formula_version, inputs_hash, computed_at
        FROM assessment_input_vectors
        WHERE assessment_id = %s::uuid
    """, (assessment_id,))

    if not iv:
        return {
            "assessment_id": str(ae["id"]),
            "wallet_address": ae.get("wallet_address"),
            "verification": {
                "status": "inputs_not_available",
                "stored_score": stored_score,
                "recomputed_score": None,
                "match": None,
                "formula_version": ae.get("methodology_version"),
            },
            "inputs": None,
            "hashes": {
                "inputs_hash": ae.get("inputs_hash"),
                "content_hash": ae.get("content_hash"),
                "recomputed_inputs_hash": None,
            },
        }

    holdings = iv["holdings"] if isinstance(iv["holdings"], list) else []
    stablecoin_scores = iv["stablecoin_scores"] if isinstance(iv["stablecoin_scores"], dict) else {}

    # 3. Re-compute wallet risk score (value-weighted average of SII scores)
    scored_holdings = []
    for h in holdings:
        symbol = h.get("symbol", "")
        value_usd = float(h.get("value_usd", 0))
        sii = h.get("sii_score")
        if sii is None and symbol in stablecoin_scores:
            sii = stablecoin_scores[symbol].get("score")
        if sii is not None and value_usd > 0:
            scored_holdings.append({"symbol": symbol, "value_usd": value_usd, "sii_score": float(sii)})

    recomputed_score = None
    if scored_holdings:
        total_val = sum(h["value_usd"] for h in scored_holdings)
        if total_val > 0:
            recomputed_score = round(
                sum(h["value_usd"] * h["sii_score"] for h in scored_holdings) / total_val,
                2,
            )

    # 4. Re-compute inputs hash
    component_scores_for_hash = {}
    for h in holdings:
        sym = h.get("symbol", "")
        sii = h.get("sii_score")
        if sym and sii is not None:
            component_scores_for_hash[sym] = float(sii)
    # Note: recomputed hash won't match stored hash exactly because compute_inputs_hash
    # includes a timestamp. We include it for transparency but verify score, not hash.
    try:
        recomputed_hash, _ = compute_inputs_hash(
            component_scores=component_scores_for_hash,
            wallet_holdings=holdings,
            formula_version=iv["formula_version"],
        )
    except Exception:
        recomputed_hash = None

    # 5. Determine verification status
    if recomputed_score is not None and stored_score is not None:
        match = abs(recomputed_score - stored_score) < 0.01
        status = "verified" if match else "mismatch"
    else:
        match = None
        status = "inputs_not_available"

    # Build holdings output with percentages
    total_value = sum(float(h.get("value_usd", 0)) for h in holdings)
    holdings_output = []
    for h in holdings:
        val = float(h.get("value_usd", 0))
        symbol = h.get("symbol", "")
        sii = h.get("sii_score")
        if sii is None and symbol in stablecoin_scores:
            sii = stablecoin_scores[symbol].get("score")
        holdings_output.append({
            "symbol": symbol,
            "value_usd": val,
            "pct_of_wallet": round(val / total_value, 4) if total_value > 0 else 0,
            "sii_score": float(sii) if sii is not None else None,
        })

    return {
        "assessment_id": str(ae["id"]),
        "wallet_address": ae.get("wallet_address"),
        "verification": {
            "status": status,
            "stored_score": stored_score,
            "recomputed_score": recomputed_score,
            "match": match,
            "formula_version": iv["formula_version"],
        },
        "inputs": {
            "holdings_count": len(holdings),
            "holdings": holdings_output,
            "stablecoin_scores_at_time": stablecoin_scores,
        },
        "hashes": {
            "inputs_hash": ae.get("inputs_hash"),
            "content_hash": ae.get("content_hash"),
            "recomputed_inputs_hash": recomputed_hash,
        },
    }


# =============================================================================
# PSI (Protocol Solvency Index) API
# =============================================================================

@app.get("/api/psi/scores")
async def psi_scores():
    """All scored protocols — latest score per protocol."""
    rows = fetch_all("""
        SELECT DISTINCT ON (protocol_slug)
            id, protocol_slug, protocol_name, overall_score, grade,
            category_scores, component_scores, raw_values,
            formula_version, computed_at
        FROM psi_scores
        ORDER BY protocol_slug, computed_at DESC
    """)
    results = []
    for row in rows:
        results.append({
            "protocol_slug": row["protocol_slug"],
            "protocol_name": row["protocol_name"],
            "score": float(row["overall_score"]) if row.get("overall_score") else None,
            "grade": row["grade"],
            "category_scores": row.get("category_scores"),
            "formula_version": row.get("formula_version"),
            "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        })
    return {
        "protocols": results,
        "count": len(results),
        "index": "psi",
        "version": "v0.1.0",
    }


@app.get("/api/psi/scores/{slug}")
async def psi_score_detail(slug: str):
    """Detailed PSI breakdown for one protocol."""
    row = fetch_one("""
        SELECT id, protocol_slug, protocol_name, overall_score, grade,
               category_scores, component_scores, raw_values,
               formula_version, computed_at
        FROM psi_scores
        WHERE protocol_slug = %s
        ORDER BY computed_at DESC
        LIMIT 1
    """, (slug,))
    if not row:
        raise HTTPException(status_code=404, detail=f"Protocol '{slug}' not found in PSI scores")
    return {
        "protocol_slug": row["protocol_slug"],
        "protocol_name": row["protocol_name"],
        "score": float(row["overall_score"]) if row.get("overall_score") else None,
        "grade": row["grade"],
        "category_scores": row.get("category_scores"),
        "component_scores": row.get("component_scores"),
        "raw_values": row.get("raw_values"),
        "formula_version": row.get("formula_version"),
        "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
    }


@app.get("/api/psi/definition")
async def psi_definition():
    """Return the full PSI v0.1 index definition."""
    from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
    return PSI_V01_DEFINITION


@app.get("/api/indices")
async def list_indices():
    """List all available index definitions."""
    from app.index_definitions.sii_v1 import SII_V1_DEFINITION
    from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
    return {
        "indices": [
            {
                "id": SII_V1_DEFINITION["index_id"],
                "version": SII_V1_DEFINITION["version"],
                "name": SII_V1_DEFINITION["name"],
                "entity_type": SII_V1_DEFINITION["entity_type"],
                "components": len(SII_V1_DEFINITION["components"]),
                "status": "live",
            },
            {
                "id": PSI_V01_DEFINITION["index_id"],
                "version": PSI_V01_DEFINITION["version"],
                "name": PSI_V01_DEFINITION["name"],
                "entity_type": PSI_V01_DEFINITION["entity_type"],
                "components": len(PSI_V01_DEFINITION["components"]),
                "status": "live",
            },
        ]
    }


# =============================================================================
# Structured Query API
# =============================================================================

@app.post("/api/query")
async def query_wallets(request: Request):
    """Structured query against the wallet risk graph."""
    from app.query_engine import execute_query

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")

    result = execute_query(body)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.get("/api/query/schema")
async def query_schema():
    """Documentation of available query filters and options."""
    from app.query_engine import QUERY_SCHEMA
    return QUERY_SCHEMA


# =============================================================================
# Daily Pulse API
# =============================================================================

@app.get("/api/pulse/latest")
async def pulse_latest():
    """Latest daily pulse — full risk surface snapshot."""
    import hashlib
    row = fetch_one("SELECT * FROM daily_pulses ORDER BY pulse_date DESC LIMIT 1")
    if not row:
        raise HTTPException(status_code=404, detail="No pulse data available yet. Run the scoring cycle first.")
    summary = row.get("summary", {})
    if isinstance(summary, str):
        import json as _json
        summary = _json.loads(summary)
    canonical = json.dumps(summary, sort_keys=True, separators=(",", ":"), default=str)
    content_hash = "0x" + hashlib.sha256(canonical.encode()).hexdigest()
    return {
        "pulse_date": row["pulse_date"].isoformat() if hasattr(row["pulse_date"], "isoformat") else str(row["pulse_date"]),
        "summary": summary,
        "content_hash": content_hash,
        "page_url": row.get("page_url"),
    }


@app.get("/api/pulse/history")
async def pulse_history(days: int = Query(default=30, ge=1, le=365)):
    """List of recent pulse dates with page URLs (not full summaries)."""
    rows = fetch_all(
        "SELECT pulse_date, created_at, page_url FROM daily_pulses ORDER BY pulse_date DESC LIMIT %s",
        (days,),
    )
    return {
        "pulses": [
            {
                "pulse_date": r["pulse_date"].isoformat() if hasattr(r["pulse_date"], "isoformat") else str(r["pulse_date"]),
                "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
                "page_url": r.get("page_url"),
            }
            for r in rows
        ],
        "count": len(rows),
    }


@app.get("/api/pulse/{date_str}")
async def pulse_by_date(date_str: str):
    """Daily pulse for a specific date (YYYY-MM-DD)."""
    import hashlib
    import re
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    row = fetch_one("SELECT * FROM daily_pulses WHERE pulse_date = %s", (date_str,))
    if not row:
        raise HTTPException(status_code=404, detail=f"No pulse found for {date_str}")
    summary = row.get("summary", {})
    if isinstance(summary, str):
        import json as _json
        summary = _json.loads(summary)
    canonical = json.dumps(summary, sort_keys=True, separators=(",", ":"), default=str)
    content_hash = "0x" + hashlib.sha256(canonical.encode()).hexdigest()
    return {
        "pulse_date": row["pulse_date"].isoformat() if hasattr(row["pulse_date"], "isoformat") else str(row["pulse_date"]),
        "summary": summary,
        "content_hash": content_hash,
        "page_url": row.get("page_url"),
    }


# =============================================================================
# Divergence Detection API
# =============================================================================

@app.get("/api/divergence")
async def divergence_all():
    """Combined divergence signals — capital-flow / quality mismatches."""
    from app.divergence import detect_all_divergences
    return detect_all_divergences()


@app.get("/api/divergence/assets")
async def divergence_assets():
    """Asset quality divergence: score declining while capital flows in."""
    from app.divergence import detect_asset_divergence
    return {"signals": detect_asset_divergence(), "type": "asset_quality"}


@app.get("/api/divergence/wallets")
async def divergence_wallets():
    """Wallet concentration divergence: HHI rising while value grows."""
    from app.divergence import detect_wallet_concentration_divergence
    return {"signals": detect_wallet_concentration_divergence(), "type": "wallet_concentration"}


@app.get("/api/specs/divergence")
async def divergence_spec():
    """Divergence signal type definitions and severity thresholds."""
    from app.divergence import DIVERGENCE_SPEC
    return DIVERGENCE_SPEC


# =============================================================================
# CQI (Collateral Quality Index) — Composition API
# =============================================================================

@app.get("/api/compose/cqi")
async def compose_cqi(asset: str = Query(...), protocol: str = Query(...)):
    """Compute Collateral Quality Index for an asset-in-protocol pair."""
    from app.composition import compute_cqi
    result = compute_cqi(asset, protocol)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@app.get("/api/compose/cqi/matrix")
async def compose_cqi_matrix():
    """CQI for all stablecoin x protocol combinations."""
    from app.composition import compute_cqi_matrix
    return compute_cqi_matrix()


@app.get("/api/specs/composition")
async def get_composition_spec():
    """Published composition grammar specification."""
    from app.specs.composition_grammar_v1 import COMPOSITION_GRAMMAR_V1
    return COMPOSITION_GRAMMAR_V1


@app.get("/admin")
async def admin_panel(request: Request):
    admin_key = os.environ.get("ADMIN_KEY", "")
    provided = request.query_params.get("key", "")
    if not admin_key or provided != admin_key:
        raise HTTPException(status_code=401, detail="Unauthorized — provide ?key=YOUR_ADMIN_KEY")
    return HTMLResponse(content=ADMIN_HTML)


# =============================================================================
# Admin: usage stats + API key management
# =============================================================================

@app.get("/api/admin/usage")
async def admin_usage(request: Request, days: int = 7):
    _check_admin_key(request)
    from app.usage_tracker import get_usage_stats
    return get_usage_stats(days=days)


@app.post("/api/admin/apikeys")
async def admin_create_key(request: Request, name: str = Query(...)):
    _check_admin_key(request)
    from app.usage_tracker import create_api_key
    key = create_api_key(name)
    return {
        "api_key": key,
        "name": name,
        "message": "Store this key — it will not be shown again.",
    }


@app.get("/api/admin/apikeys")
async def admin_list_keys(request: Request):
    _check_admin_key(request)
    from app.usage_tracker import list_api_keys
    return {"keys": list_api_keys()}


# =============================================================================
# Admin: API Budget Allocator
# =============================================================================

@app.get("/api/admin/budget")
async def admin_budget_status(request: Request):
    """Returns today's API budget allocation and usage."""
    _check_admin_key(request)
    from app.budget.manager import ApiBudgetManager
    budget = ApiBudgetManager()
    return budget.get_status()


@app.post("/api/admin/run-daily-cycle")
async def trigger_daily_cycle(request: Request):
    """Trigger the full daily scoring + indexing cycle in background."""
    _check_admin_key(request)
    import asyncio
    from app.budget.daily_cycle import run_daily_cycle
    asyncio.create_task(run_daily_cycle())
    return {"status": "started", "message": "Daily cycle triggered in background"}


# =============================================================================
# SPA Catch-All — registered at end of startup so dynamic routes take priority
# =============================================================================

def _register_spa_catch_all(app_instance):
    """Register the SPA catch-all AFTER all other routes so it doesn't shadow them."""
    @app_instance.get("/{full_path:path}")
    async def serve_spa(request: Request, full_path: str):
        if full_path.startswith("api/") or full_path.startswith("docs") or full_path.startswith("openapi") or full_path.startswith("admin") or full_path.startswith("developers"):
            raise HTTPException(status_code=404, detail="Not found")
        index_path = os.path.join(FRONTEND_DIR, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
        return {"name": "Basis Protocol API", "version": FORMULA_VERSION, "docs": "/docs"}
