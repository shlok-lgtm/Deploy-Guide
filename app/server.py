"""
Basis Protocol - API Server
============================
Clean FastAPI server. Reads from database only. No data collection.
"""

import atexit
import logging
import os
import time
from datetime import datetime, timezone, timedelta
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

    api_key_str = request.query_params.get("apikey") or request.headers.get("x-api-key")
    api_key_id: Optional[int] = None
    api_key_hash: Optional[str] = None

    if api_key_str:
        api_key_id = validate_api_key(api_key_str)
        api_key_hash = hash_api_key(api_key_str)

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
async def get_scores():
    """Get current SII scores for all stablecoins."""
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# 3. GET /api/scores/{coin} — Detailed score for one stablecoin
# =============================================================================

@app.get("/api/scores/{coin}")
async def get_score_detail(coin: str):
    """Get detailed SII score breakdown for a specific stablecoin."""
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
async def get_methodology():
    """Get the SII methodology — formula, weights, and component specifications."""
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
# 14. GET /admin — Admin Panel HTML
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

loadHealth(); loadFreshness(); loadGovernance(); loadSignals();
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
# SPA Catch-All — registered at end of startup so dynamic routes take priority
# =============================================================================

def _register_spa_catch_all(app_instance):
    """Register the SPA catch-all AFTER all other routes so it doesn't shadow them."""
    @app_instance.get("/{full_path:path}")
    async def serve_spa(request: Request, full_path: str):
        if full_path.startswith("api/") or full_path.startswith("docs") or full_path.startswith("openapi") or full_path.startswith("admin"):
            raise HTTPException(status_code=404, detail="Not found")
        index_path = os.path.join(FRONTEND_DIR, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
        return {"name": "Basis Protocol API", "version": FORMULA_VERSION, "docs": "/docs"}
