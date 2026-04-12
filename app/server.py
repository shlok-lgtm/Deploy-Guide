"""
Basis Protocol - API Server
============================
Clean FastAPI server. Reads from database only. No data collection.
"""

import atexit
import hashlib
import hmac
import json
import logging
import os
import re
import time
import uuid as uuid_mod
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request, Response
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
    COMPONENT_NORMALIZATIONS,
)
from app.specs.methodology_versions import METHODOLOGY_VERSIONS, WALLET_METHODOLOGY_VERSIONS, PSI_METHODOLOGY_VERSIONS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Log filter: redact API keys from httpx / urllib / requests log output
# Catches patterns like apikey=XXX, api-key=XXX, x-cg-pro-api-key=XXX,
# api_key=XXX in both URLs and headers.
# ---------------------------------------------------------------------------
_API_KEY_RE = re.compile(
    r'((?:apikey|api-key|api_key|x-cg-pro-api-key|x-api-key)=)([^&\s\'"]+)',
    re.IGNORECASE,
)

class _RedactApiKeysFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.args:
            # Format the message early so we can redact it
            try:
                record.msg = record.msg % record.args
                record.args = None
            except Exception:
                pass
        record.msg = _API_KEY_RE.sub(r'\1***', str(record.msg))
        return True

_redact_filter = _RedactApiKeysFilter()
for _logger_name in ("httpx", "httpcore", "urllib3", "requests"):
    logging.getLogger(_logger_name).addFilter(_redact_filter)

class _ResponseCache:
    """Simple TTL cache for expensive endpoint responses."""
    def __init__(self):
        self._store: dict = {}

    def get(self, key: str, ttl: int = 60):
        cached = self._store.get(key)
        if cached and (time.time() - cached[1]) < ttl:
            return cached[0]
        return None

    def set(self, key: str, value):
        self._store[key] = (value, time.time())

_cache = _ResponseCache()


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

# --- x402 Agent Payment Layer ---
try:
    from app.payments import create_x402_middleware, paid_router
    _x402_cls, _x402_kwargs = create_x402_middleware()
    app.add_middleware(_x402_cls, **_x402_kwargs)
    app.include_router(paid_router)
    logger.info("x402 payment layer enabled on /api/paid/*")
except ImportError:
    logger.warning("x402 not installed — paid endpoints disabled. Run: pip install x402")
except Exception as e:
    logger.warning(f"x402 setup failed: {e} — paid endpoints disabled")


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

    # Resolve real client IP — prefer X-Forwarded-For over direct connection
    # (GCP/Railway/Replit proxies set the direct connection to their own IPs like 35.191.*)
    forwarded_for = request.headers.get("x-forwarded-for", "")
    if forwarded_for:
        # X-Forwarded-For: client, proxy1, proxy2 — first value is the real client
        ip = forwarded_for.split(",")[0].strip()
    else:
        ip = request.client.host if request.client else "unknown"
    ua = request.headers.get("user-agent", "")[:500]
    accept_header = request.headers.get("accept", "")[:255]
    referer = request.headers.get("referer", "")[:500]

    # Classify internal vs external traffic
    INTERNAL_UA_PATTERNS = [
        "python-httpx",        # MCP server's internal calls to hub API
        "basis-keeper",        # Oracle keeper
        "basis-worker",        # Background worker
        "uvicorn",             # Internal health checks
        "headlesschrome",      # Replit internal rendering
        "claudebot",           # Anthropic web crawler
        "python-requests",     # Internal Python HTTP calls
        "replit",              # Replit internal traffic
    ]
    INTERNAL_IP_PREFIXES = [
        "35.191.",             # GCP load balancer (Replit proxy)
        "10.81.",              # Replit internal network
        "10.",                 # Any private 10.x.x.x network
        "127.0.0.",            # Localhost
    ]
    is_internal = (
        any(pat in ua.lower() for pat in INTERNAL_UA_PATTERNS)
        or any(ip.startswith(prefix) for prefix in INTERNAL_IP_PREFIXES)
    )
    # Parse entity from path
    entity_type = None
    entity_id = None
    if "/api/scores/" in path:
        parts = path.split("/api/scores/")
        if len(parts) > 1 and parts[1]:
            entity_type = "stablecoin"
            entity_id = parts[1].split("/")[0].split("?")[0]
    elif "/api/wallets/" in path:
        parts = path.split("/api/wallets/")
        if len(parts) > 1 and parts[1]:
            entity_type = "wallet"
            entity_id = parts[1].split("/")[0].split("?")[0][:42]
    elif "/api/protocols/" in path or "/api/psi/" in path:
        for prefix in ["/api/protocols/", "/api/psi/scores/"]:
            if prefix in path:
                parts = path.split(prefix)
                if len(parts) > 1 and parts[1]:
                    entity_type = "protocol"
                    entity_id = parts[1].split("/")[0].split("?")[0]
    elif path.startswith("/mcp"):
        entity_type = "mcp"

    # Admin endpoints — exempt from rate limiting but still logged
    if path.startswith("/api/admin") or path.startswith("/api/ops"):
        response = await call_next(request)
        elapsed_ms = int((time.time() - start_time) * 1000)
        log_request(
            endpoint=path, method=request.method,
            status_code=response.status_code, response_time_ms=elapsed_ms,
            ip=ip, api_key_id=api_key_id, api_key_hash=api_key_hash, user_agent=ua,
            accept_header=accept_header, referer=referer, is_internal=is_internal,
            entity_type=entity_type, entity_id=entity_id,
        )
        return response

    # Determine rate limit tier
    # First-party dashboard requests get the keyed tier. The dashboard's
    # apiFetch() helper sends X-Basis-Dashboard: 1 on every call. We also
    # check Referer/Origin as a fallback for any fetch that slips through.
    is_dashboard = request.headers.get("x-basis-dashboard") == "1"
    if not is_dashboard:
        referer = request.headers.get("referer", "")
        origin_hdr = request.headers.get("origin", "")
        is_dashboard = any(
            h and ("basisprotocol.xyz" in h or "localhost" in h or "127.0.0.1" in h)
            for h in (referer, origin_hdr)
        )

    if api_key_id:
        identifier = f"key:{api_key_id}"
        limit = KEYED_RATE_LIMIT
    elif is_dashboard:
        identifier = f"dashboard:{ip}"
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
            accept_header=accept_header, referer=referer, is_internal=is_internal,
            entity_type=entity_type, entity_id=entity_id,
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
        accept_header=accept_header, referer=referer, is_internal=is_internal,
        entity_type=entity_type, entity_id=entity_id,
    )
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-RateLimit-Limit"] = str(limit)
    if path.startswith("/api/scores") or path.startswith("/api/wallets"):
        response.headers["Basis-Methodology-Version"] = FORMULA_VERSION
    return response


# =============================================================================
# Lifecycle
# =============================================================================

def _seed_cda_issuer_registry():
    """Ensure cda_issuer_registry has entries for all known stablecoins."""
    from app.database import execute
    ON_CHAIN_AUDITORS = {"N/A (on-chain)", "N/A (algorithmic)", "N/A"}
    DISCLOSURE_TYPE_MAP = {
        "usde": "synthetic-derivative",
        "dai": "overcollateralized",
        "frax": "algorithmic",
        "usdy": "rwa-tokenized",
    }
    SOURCE_URLS_MAP = {
        "usde": [
            {"url": "https://app.ethena.fi/dashboards/transparency", "type": "dashboard", "description": "Real-time transparency dashboard"},
            {"url": "https://docs.ethena.fi/resources/custodian-attestations", "type": "attestation_page", "description": "Custodian attestation reports"},
        ],
    }
    seeded = 0
    for sid, cfg in STABLECOIN_REGISTRY.items():
        auditor = cfg.get("attestation", {}).get("auditor", "")
        category = "crypto-backed" if auditor in ON_CHAIN_AUDITORS else "fiat-backed"
        method = "nav_oracle" if category == "crypto-backed" else "web_extract"
        url = cfg.get("attestation", {}).get("transparency_url")
        disc_type = DISCLOSURE_TYPE_MAP.get(sid, "fiat-reserve" if category == "fiat-backed" else "unknown")
        src_urls = SOURCE_URLS_MAP.get(sid)
        try:
            if src_urls:
                import json as _j
                execute(
                    """
                    INSERT INTO cda_issuer_registry
                        (asset_symbol, issuer_name, coingecko_id, transparency_url,
                         collection_method, asset_category, disclosure_type, source_urls)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (asset_symbol) DO UPDATE SET
                        disclosure_type = EXCLUDED.disclosure_type,
                        asset_category = EXCLUDED.asset_category,
                        source_urls = EXCLUDED.source_urls
                    """,
                    (cfg["symbol"], cfg.get("issuer", "Unknown"), cfg.get("coingecko_id"),
                     url, method, category, disc_type, _j.dumps(src_urls)),
                )
            else:
                execute(
                    """
                    INSERT INTO cda_issuer_registry
                        (asset_symbol, issuer_name, coingecko_id, transparency_url,
                         collection_method, asset_category, disclosure_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (asset_symbol) DO UPDATE SET
                        disclosure_type = EXCLUDED.disclosure_type,
                        asset_category = EXCLUDED.asset_category
                    """,
                    (cfg["symbol"], cfg.get("issuer", "Unknown"), cfg.get("coingecko_id"),
                     url, method, category, disc_type),
                )
            seeded += 1
        except Exception as e:
            logger.warning(f"CDA seed failed for {cfg['symbol']}: {e}")
    logger.info(f"CDA issuer registry seeded ({seeded} upserts attempted)")

    # Populate source_urls for issuers that have transparency_url but no source_urls
    # (skips issuers that already have source_urls set, e.g. from SOURCE_URLS_MAP)
    try:
        issuers_needing_sources = fetch_all(
            """
            SELECT asset_symbol, transparency_url, attestation_page_url, disclosure_type
            FROM cda_issuer_registry
            WHERE (source_urls IS NULL OR source_urls = '[]'::jsonb)
              AND transparency_url IS NOT NULL
              AND is_active = TRUE
            """
        )
        import json as _j
        populated = 0
        for iss in issuers_needing_sources:
            sources = []
            t_url = iss.get("transparency_url")
            a_url = iss.get("attestation_page_url")

            if t_url:
                if any(k in t_url.lower() for k in ("dashboard", "stats", "facts.")):
                    sources.append({"url": t_url, "type": "dashboard", "description": "Transparency dashboard"})
                else:
                    sources.append({"url": t_url, "type": "attestation_page", "description": "Transparency page"})

            if a_url and a_url != t_url:
                sources.append({"url": a_url, "type": "attestation_page", "description": "Attestation reports"})

            if sources:
                # Only set if still NULL (double-check to avoid race with SOURCE_URLS_MAP seed)
                execute(
                    """UPDATE cda_issuer_registry SET source_urls = %s
                       WHERE asset_symbol = %s AND (source_urls IS NULL OR source_urls = '[]'::jsonb)""",
                    (_j.dumps(sources), iss["asset_symbol"]),
                )
                populated += 1
        if populated:
            logger.info(f"CDA: Populated source_urls for {populated} issuers")
    except Exception as e:
        logger.warning(f"CDA source_urls population failed: {e}")


@app.on_event("startup")
async def startup():
    init_pool()
    # Seed CDA issuer registry from config
    try:
        _seed_cda_issuer_registry()
    except Exception as e:
        logger.warning(f"CDA registry seed skipped: {e}")
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
    # Clean stale temporal reconstruction cache entries
    try:
        from app.database import execute as _exec
        _exec("DELETE FROM temporal_reconstructions WHERE components_available = 0")
        logger.info("Cleared stale temporal reconstruction cache entries")
    except Exception:
        pass
    # Ensure historical_protocol_data table exists on this database
    try:
        from app.database import execute as _exec2, fetch_one as _fo
        _exec2("""
            CREATE TABLE IF NOT EXISTS historical_protocol_data (
                id SERIAL PRIMARY KEY,
                protocol_slug VARCHAR(64) NOT NULL,
                record_date DATE NOT NULL,
                tvl NUMERIC,
                fees_24h NUMERIC,
                revenue_24h NUMERIC,
                token_price NUMERIC,
                token_mcap NUMERIC,
                token_volume NUMERIC,
                chain_count INTEGER,
                data_source VARCHAR(32) DEFAULT 'defillama+coingecko',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(protocol_slug, record_date)
            )
        """)
        _exec2(
            "CREATE INDEX IF NOT EXISTS idx_hist_protocol_slug_date "
            "ON historical_protocol_data(protocol_slug, record_date)"
        )
        count = _fo("SELECT COUNT(*) as cnt FROM historical_protocol_data")
        logger.info(f"historical_protocol_data: {count['cnt']} rows")
    except Exception as e:
        logger.warning(f"historical_protocol_data check failed: {e}")
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
    # Register Operations Hub routes
    try:
        from app.ops.routes import register_ops_routes
        register_ops_routes(app)

        logger.info("Operations Hub routes registered")
    except Exception as e:
        logger.warning(f"Operations Hub not available: {e}")
    # Register Squads Guard routes (webhook + scoring for Solana multisigs)
    try:
        from squads_guard.router import router as squads_router
        app.include_router(squads_router, prefix="/api/squads")
        logger.info("Squads Guard routes registered at /api/squads")
    except Exception as e:
        logger.warning(f"Squads Guard not available: {e}")
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

    # Startup notification — catches restart loops
    try:
        from app.ops.tools.alerter import send_alert
        await send_alert("service_restart", "API server started.")
    except Exception:
        pass


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
    """System health check — powered by the integrity layer."""
    cached = _cache.get("health", ttl=60)
    if cached:
        return cached

    db_status = db_health_check()
    db_ok = db_status.get("status") == "healthy"

    try:
        from app.integrity import check_all
        result = check_all()
    except Exception:
        return {
            "status": "unhealthy",
            "database": db_status,
            "domains": {},
            "formula_version": FORMULA_VERSION,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    overall = result["status"] if db_ok else "unhealthy"

    response_data = {
        "status": overall,
        "database": db_status,
        "domains": result["domains"],
        "checked_at": result["checked_at"],
        "formula_version": FORMULA_VERSION,
        "timestamp": result["checked_at"],
    }
    _cache.set("health", response_data)
    return response_data


@app.get("/api/integrity")
async def get_integrity():
    """Full data integrity status across all domains."""
    cached = _cache.get("integrity", ttl=60)
    if cached:
        return cached
    from app.integrity import check_all
    result = check_all()
    _cache.set("integrity", result)
    return result


@app.get("/api/integrity/{domain}")
async def get_integrity_domain(domain: str):
    """Data integrity status for a specific domain."""
    from app.integrity import check_domain
    result = check_domain(domain)
    if result.get("warnings") and any(w.get("rule") == "unknown_domain" for w in result["warnings"]):
        raise HTTPException(status_code=404, detail=f"Unknown domain: {domain}")
    return result


# =============================================================================
# 2. GET /api/scores — All stablecoin scores (main rankings table)
# =============================================================================

@app.get("/api/scores")
async def get_scores(response: Response, methodology_version: Optional[str] = Query(default=None)):
    """Get current SII scores for all stablecoins."""
    cache_key = f"scores:{methodology_version or 'latest'}"
    cached = _cache.get(cache_key, ttl=30)
    if cached:
        response.headers["Cache-Control"] = "public, max-age=30"
        response.headers["X-Cache"] = "HIT"
        return cached
    pinned = check_methodology_version(methodology_version)
    rows = fetch_all("""
        SELECT s.*, st.name, st.symbol, st.issuer, st.contract AS token_contract
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        ORDER BY s.overall_score DESC
    """)
    
    from app.scoring_engine import compute_confidence_tag
    SII_COMPONENTS_TOTAL = len(COMPONENT_NORMALIZATIONS)

    results = []
    for row in rows:
        comp_count = row.get("component_count") or 0
        coverage = round(comp_count / max(SII_COMPONENTS_TOTAL, 1), 2)
        # Determine missing categories from null category scores
        sii_cat_map = {"peg": "peg_score", "liquidity": "liquidity_score", "flows": "mint_burn_score",
                       "distribution": "distribution_score", "structural": "structural_score"}
        missing = [cat for cat, col in sii_cat_map.items() if not row.get(col)]
        conf = compute_confidence_tag(5 - len(missing), 5, coverage, missing)

        results.append({
            "id": row["stablecoin_id"],
            "name": row["name"],
            "symbol": row["symbol"],
            "issuer": row["issuer"],
            "token_contract": row.get("token_contract"),
            "score": float(row["overall_score"]),
            "confidence": conf["confidence"],
            "confidence_tag": conf["tag"],
            "missing_categories": conf["missing_categories"],
            "component_coverage": coverage,
            "components_populated": comp_count,
            "components_total": SII_COMPONENTS_TOTAL,
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
            "component_count": comp_count,
            "formula_version": row.get("formula_version"),
            "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        })
    
    # Count active data sources
    data_sources = {"CoinGecko", "DeFiLlama", "Etherscan/Blockscout", "Snapshot"}
    if os.environ.get("HELIUS_API_KEY"):
        data_sources.add("Helius")
    try:
        cda_count = fetch_one(
            "SELECT COUNT(*) as cnt FROM cda_vendor_extractions WHERE extracted_at > NOW() - INTERVAL '7 days'"
        )
        if cda_count and cda_count["cnt"] > 0:
            data_sources.add("Parallel.ai+Reducto")
    except Exception:
        pass

    scores_result = {
        "stablecoins": results,
        "count": len(results),
        "formula_version": FORMULA_VERSION,
        "methodology_version": FORMULA_VERSION,
        "methodology_version_pinned": pinned,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data_source_count": len(data_sources),
        "sii_component_count": len(COMPONENT_NORMALIZATIONS),
    }
    _cache.set(cache_key, scores_result)
    response.headers["Cache-Control"] = "public, max-age=30"
    response.headers["X-Cache"] = "MISS"
    return scores_result


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
    
    # Get latest component readings (deduplicated — one per component)
    components = fetch_all("""
        SELECT DISTINCT ON (component_id)
          component_id, category, raw_value, normalized_score, data_source, collected_at
        FROM component_readings
        WHERE stablecoin_id = %s
          AND collected_at > NOW() - INTERVAL '48 hours'
        ORDER BY component_id, collected_at DESC
    """, (coin,))
    
    from app.scoring_engine import compute_confidence_tag
    SII_COMPONENTS_TOTAL = len(COMPONENT_NORMALIZATIONS)
    comp_count = row.get("component_count") or 0
    detail_coverage = round(comp_count / max(SII_COMPONENTS_TOTAL, 1), 2)
    detail_cat_map = {"peg": "peg_score", "liquidity": "liquidity_score", "flows": "mint_burn_score",
                      "distribution": "distribution_score", "structural": "structural_score"}
    detail_missing = [cat for cat, col in detail_cat_map.items() if not row.get(col)]
    detail_conf = compute_confidence_tag(5 - len(detail_missing), 5, detail_coverage, detail_missing)

    return {
        "id": row["stablecoin_id"],
        "name": row["name"],
        "symbol": row["symbol"],
        "issuer": row["issuer"],
        "token_contract": row.get("token_contract"),
        "score": float(row["overall_score"]),
        "confidence": detail_conf["confidence"],
        "confidence_tag": detail_conf["tag"],
        "missing_categories": detail_conf["missing_categories"],
        "component_coverage": detail_coverage,
        "components_populated": comp_count,
        "components_total": SII_COMPONENTS_TOTAL,
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
                "source_type": (
                    "cda_extraction" if (c["data_source"] or "").startswith("cda_") else
                    "live_api" if c["data_source"] in ("coingecko", "etherscan", "curve", "defillama") else
                    "static_config"
                ),
                "collected_at": c["collected_at"].isoformat() if c["collected_at"] else None,
            }
            for c in components
        ],
        "attestation": row.get("attestation_config"),
        "regulatory_licenses": row.get("regulatory_licenses"),
        "component_count": comp_count,
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
# 4a. GET /api/scores/{coin}/recent — Lightweight recent scores for trend lines
# =============================================================================

@app.get("/api/scores/{coin}/recent")
async def get_recent_scores(
    coin: str,
    days: int = Query(default=7, ge=1, le=30),
):
    """Lightweight recent score history for trend display (e.g. 7-day sparkline)."""
    rows = fetch_all("""
        SELECT score_date, overall_score, grade
        FROM score_history
        WHERE stablecoin = %s
          AND score_date > CURRENT_DATE - INTERVAL '%s days'
        ORDER BY score_date ASC
    """, (coin, days))

    if not rows:
        # Verify the stablecoin exists
        exists = fetch_one("SELECT id FROM stablecoins WHERE id = %s", (coin,))
        if not exists:
            raise HTTPException(status_code=404, detail=f"Stablecoin '{coin}' not found")

    return {
        "stablecoin": coin,
        "days": days,
        "scores": [
            {
                "date": str(row["score_date"]),
                "score": round(float(row["overall_score"]), 2),
            }
            for row in rows
        ],
        "count": len(rows),
    }


# =============================================================================
# 4b. Temporal reconstruction — historical score reconstruction
# =============================================================================

@app.get("/api/scores/{coin}/at/{target_date}")
def score_at_date(coin: str, target_date: str):
    """Reconstruct SII score at a specific historical date.
    Sync handler — FastAPI runs in threadpool to avoid blocking the event loop."""
    from app.services.temporal_engine import reconstruct_score_sync
    try:
        td = datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    if td > datetime.now(timezone.utc).date():
        raise HTTPException(status_code=400, detail="Cannot reconstruct future dates.")

    return reconstruct_score_sync(coin, td)


@app.get("/api/scores/{coin}/range")
def score_range(
    coin: str,
    start: str = Query(alias="from", description="Start date YYYY-MM-DD"),
    end: str = Query(alias="to", description="End date YYYY-MM-DD"),
):
    """Reconstruct SII scores for a date range (max 365 days).
    Sync handler — FastAPI runs in threadpool."""
    from app.services.temporal_engine import reconstruct_range_sync
    try:
        from_date = datetime.strptime(start, "%Y-%m-%d").date()
        to_date = datetime.strptime(end, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    if to_date > datetime.now(timezone.utc).date():
        to_date = datetime.now(timezone.utc).date()

    if (to_date - from_date).days > 365:
        raise HTTPException(status_code=400, detail="Max 365 days per request.")

    results = reconstruct_range_sync(coin, from_date, to_date)
    return {
        "stablecoin": coin,
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "scores": results,
        "count": len(results),
    }


@app.get("/api/backtest/{coin}")
def backtest_event(
    coin: str,
    event: str = Query(description="Named crisis event ID"),
):
    """Reconstruct scores across a named crisis event window.
    Sync handler — FastAPI runs in threadpool."""
    from app.services.temporal_engine import reconstruct_range_sync, CRISIS_EVENTS

    if event not in CRISIS_EVENTS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown event '{event}'. Available: {list(CRISIS_EVENTS.keys())}",
        )

    ev = CRISIS_EVENTS[event]
    from_date = datetime.strptime(ev["from"], "%Y-%m-%d").date()
    to_date = datetime.strptime(ev["to"], "%Y-%m-%d").date()

    results = reconstruct_range_sync(coin, from_date, to_date)
    return {
        "stablecoin": coin,
        "event": event,
        "event_name": ev["name"],
        "event_description": ev["description"],
        "from": ev["from"],
        "to": ev["to"],
        "scores": results,
        "count": len(results),
    }


@app.post("/api/admin/backfill")
async def admin_backfill(request: Request, background_tasks: BackgroundTasks):
    """Trigger historical price backfill from CoinGecko. Runs in background.
    Admin-key protected."""
    _check_admin_key(request)
    try:
        body = await request.json()

        from app.services.historical_backfill import backfill_coin_sync, backfill_all_sync

        if body.get("all"):
            from_date = body.get("from", "2020-01-01")
            to_date = body.get("to")
            background_tasks.add_task(backfill_all_sync, from_date, to_date)
            return {"status": "started", "scope": "all", "from": from_date, "to": to_date or "today"}

        stablecoin_id = body.get("stablecoin_id")
        if not stablecoin_id:
            raise HTTPException(status_code=400, detail="Provide stablecoin_id or all=true")

        from app.config import STABLECOIN_REGISTRY
        cfg = STABLECOIN_REGISTRY.get(stablecoin_id)
        coingecko_id = cfg["coingecko_id"] if cfg else stablecoin_id

        from_date = body.get("from", "2020-01-01")
        to_date = body.get("to")
        background_tasks.add_task(backfill_coin_sync, coingecko_id, from_date, to_date)
        return {"status": "started", "coingecko_id": coingecko_id, "from": from_date, "to": to_date or "today"}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.get("/api/admin/backfill/status")
def backfill_status(request: Request):
    """Check backfill progress."""
    _check_admin_key(request)
    try:
        row = fetch_one("SELECT * FROM backfill_status ORDER BY id DESC LIMIT 1")
        if not row:
            return {"status": "never_run"}
        return {
            "status": row["status"],
            "started_at": row["started_at"].isoformat() if row["started_at"] else None,
            "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
            "coins_total": row["coins_total"],
            "coins_completed": row["coins_completed"],
            "records_total": row["records_total"],
            "current_coin": row["current_coin"],
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


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
        "scoring_range": "Numerical 0\u2013100. No letter grade mapping.",
        "components": {
            comp_id: {
                "category": spec["category"],
                "weight_in_category": spec["weight"],
            }
            for comp_id, spec in COMPONENT_NORMALIZATIONS.items()
        },
        "scoring_components": len(COMPONENT_NORMALIZATIONS),
        "diagnostic_components": 14,
        "data_sources": [
            "CoinGecko Pro", "DeFiLlama", "Etherscan", "Curve Finance",
            "Issuer attestation reports", "On-chain contract analysis",
        ],
        "confidence_system": {
            "description": "Scores include a confidence tag based on data coverage",
            "levels": {
                "high": ">=80% component coverage — full confidence",
                "standard": ">=60% component coverage — reliable with minor gaps",
                "limited": "<60% component coverage — score computed from partial data",
            },
            "fields": ["confidence", "confidence_tag", "missing_categories", "component_coverage"],
        },
        "methodology_version": FORMULA_VERSION,
        "methodology_version_pinned": pinned,
    }


# =============================================================================
# 6a2. GET /api/indices — All registered index definitions
# =============================================================================

@app.get("/api/indices")
async def get_all_indices():
    """Return all registered index definitions for the methodology page."""
    from app.index_definitions.sii_v1 import SII_V1_DEFINITION
    from app.index_definitions.psi_v01 import PSI_V01_DEFINITION

    indices = [SII_V1_DEFINITION, PSI_V01_DEFINITION]

    result = []
    for idx in indices:
        categories = []
        for cat_id, cat in idx["categories"].items():
            comp_count = sum(
                1 for c in idx.get("components", {}).values()
                if c.get("category") == cat_id
            )
            comp_names = [
                c["name"] for c in idx.get("components", {}).values()
                if c.get("category") == cat_id
            ]
            categories.append({
                "id": cat_id,
                "name": cat["name"],
                "weight": cat["weight"],
                "component_count": comp_count,
                "components": comp_names,
            })
        categories.sort(key=lambda x: x["weight"], reverse=True)

        formula_parts = [
            f"{cat['weight']:.2f}\u00d7{cat['name'].split('&')[0].strip().split('(')[0].strip()}"
            for cat in categories
        ]
        formula = f"{idx['index_id'].upper()} = " + " + ".join(formula_parts)

        result.append({
            "index_id": idx["index_id"],
            "version": idx["version"],
            "name": idx["name"],
            "description": idx["description"],
            "entity_type": idx["entity_type"],
            "formula": formula,
            "categories": categories,
            "total_components": len(idx.get("components", {})),
            "structural_subcategories": idx.get("structural_subcategories"),
        })

    result.append({
        "index_id": "cqi",
        "version": "v1.0.0",
        "name": "Collateral Quality Index",
        "description": "Composes SII and PSI to measure stablecoin safety within a specific protocol. Every scored stablecoin \u00d7 every scored protocol produces a CQI pair.",
        "entity_type": "composition",
        "formula": "CQI(asset, protocol) = 0.60\u00d7SII(asset) + 0.40\u00d7PSI(protocol)",
        "categories": [],
        "total_components": 0,
    })

    return {
        "indices": result,
        "count": len(result),
        "scoring_range": "Numerical 0\u2013100. No letter grade mapping.",
        "principles": [
            {"title": "Neutral", "desc": "No customer can pay to influence scores, weights, thresholds, or methodology timing."},
            {"title": "Deterministic", "desc": "Same inputs always produce the same outputs. No discretionary adjustments."},
            {"title": "Versioned", "desc": "All methodology changes are announced in advance, timestamped, and retroactively reproducible."},
            {"title": "Composable", "desc": "Every index is a JSON config against the generic scoring engine. New indices require zero code changes."},
        ],
        "data_sources": ["CoinGecko Pro", "DeFiLlama", "Etherscan", "Curve Finance", "Issuer Attestations", "On-Chain Analysis"],
    }


# =============================================================================
# 6b. GET /api/methodology/versions — Version history and governance
# =============================================================================

@app.get("/api/methodology/versions")
async def get_methodology_versions():
    """Get methodology version history, governance rules, and wallet scoring versions."""
    return {
        "sii": METHODOLOGY_VERSIONS,
        "psi": PSI_METHODOLOGY_VERSIONS,
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
    provided = (
        request.query_params.get("key", "")
        or request.headers.get("x-admin-key", "")
    )
    if not admin_key or not provided or not hmac.compare_digest(provided, admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized")


# =============================================================================
# 10. GET /api/admin/governance/stats
# =============================================================================

@app.get("/api/admin/governance/stats")
async def admin_governance_stats(request: Request):
    _check_admin_key(request)

    try:
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
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


# =============================================================================
# 11. GET /api/admin/freshness
# =============================================================================

@app.get("/api/admin/freshness")
async def admin_freshness(request: Request):
    _check_admin_key(request)

    try:
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
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


# =============================================================================
# 12. GET /api/admin/health
# =============================================================================

@app.get("/api/admin/health")
async def admin_health(request: Request):
    _check_admin_key(request)

    try:
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
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


# =============================================================================
# 13. GET /api/admin/content/signals
# =============================================================================

@app.get("/api/admin/content/signals")
async def admin_content_signals(request: Request):
    _check_admin_key(request)

    try:
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
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


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
body{background:#F3F2ED;color:#0B090A;font-family:'IBM Plex Sans',system-ui,sans-serif;line-height:1.6}
.container{max-width:960px;margin:0 auto;padding:48px 24px}
@media(max-width:768px){.container{padding:16px 12px}}
.back-link{font-family:'IBM Plex Mono',monospace;font-size:12px;color:#6a6a6a;text-decoration:none;display:inline-block;margin-bottom:20px}
.back-link:hover{color:#0B090A}

/* TabHeader-style hero */
.hero{border:1.5px solid #0B090A;margin-bottom:32px}
.hero-title{padding:18px 24px 0;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px}
@media(max-width:768px){.hero-title{padding:14px 12px 0;flex-direction:column;align-items:flex-start}}
.hero-left{display:flex;align-items:center;gap:16px}
.hero-left h1{font-family:'IBM Plex Sans',sans-serif;font-size:28px;font-weight:400;letter-spacing:-0.3px}
.hero-left h1 b{font-weight:700}
.hero-form{font-family:'IBM Plex Mono',monospace;font-size:10px;color:#9a9a9a;text-transform:uppercase;letter-spacing:2px;display:flex;align-items:center;gap:14px}
.hero-stats{padding:0 24px 14px;display:flex;flex-wrap:wrap;align-items:center;gap:0;margin-top:12px;border-top:1px solid #c8c4bc;padding-top:12px}
@media(max-width:768px){.hero-stats{padding:0 12px 14px}}
.hero-stats span{font-family:'IBM Plex Mono',monospace;font-size:10px;color:#6a6a6a;text-transform:uppercase;letter-spacing:1.5px;padding:0 12px}
@media(max-width:768px){.hero-stats span{font-size:8px;letter-spacing:0.5px;padding:2px 6px}}
.hero-stats .sep{width:1px;height:12px;background:#c8c4bc}
@media(max-width:768px){.hero-stats .sep{display:none}}
.hero-formula{border-top:1px solid #c8c4bc;padding:10px 24px;font-family:'IBM Plex Mono',monospace;font-size:11px;color:#0B090A}
@media(max-width:768px){.hero-formula{padding:8px 12px;font-size:9px}}

/* Tiers */
.tiers{display:grid;grid-template-columns:repeat(3,1fr);gap:20px;margin-bottom:40px}
@media(max-width:768px){.tiers{grid-template-columns:1fr}}
.tier{border:1px solid #0B090A;padding:24px 20px;display:flex;flex-direction:column;background:#F3F2ED}
.tier.featured{border-color:#fc988f;border-width:2px}
.tier h2{font-family:'IBM Plex Mono',monospace;font-size:14px;font-weight:600;margin-bottom:4px;text-transform:uppercase;letter-spacing:1px}
.tier .price{font-size:12px;color:#6a6a6a;margin-bottom:16px;min-height:36px;font-family:'IBM Plex Mono',monospace}
.tier ul{list-style:none;flex:1;margin-bottom:20px}
.tier li{font-size:13px;color:#3a3a3a;padding:4px 0 4px 16px;position:relative;font-family:'IBM Plex Sans',sans-serif}
.tier li::before{content:"\\2713";position:absolute;left:0;color:#9a9a9a;font-size:11px}
.tier .btn{display:inline-block;text-align:center;padding:10px 20px;font-family:'IBM Plex Mono',monospace;font-size:12px;font-weight:500;text-decoration:none;cursor:pointer;border:1px solid #0B090A;background:transparent;color:#0B090A;transition:all 0.15s;letter-spacing:0.5px}
.tier .btn:hover{background:#0B090A;color:#F3F2ED}
.tier.featured .btn{background:#fc988f;color:#0B090A;border-color:#fc988f}
.tier.featured .btn:hover{background:#e8877e}
.pricing-note{font-size:10px;color:#9a9a9a;margin-top:4px}

/* Endpoint reference */
.ref-box{border:1px solid #c8c4bc;padding:20px 24px;margin-bottom:32px}
@media(max-width:768px){.ref-box{padding:16px 12px}}
.ref-box h3{font-family:'IBM Plex Mono',monospace;font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:1.5px;color:#6a6a6a;margin-bottom:20px;border:none;padding:0}
.endpoint-group{margin-bottom:20px}
.endpoint-group h4{font-family:'IBM Plex Mono',monospace;font-size:12px;font-weight:600;color:#0B090A;margin-bottom:6px}
.endpoint{font-family:'IBM Plex Mono',monospace;font-size:12px;color:#3a3a3a;padding:4px 0;border-bottom:1px dotted #e0ddd6}
.endpoint .method{color:#fc988f;font-weight:600}
.docs-link{font-family:'IBM Plex Mono',monospace;font-size:11px;color:#6a6a6a;margin-top:8px;display:block}
.docs-link a{color:#6a6a6a;text-decoration:none;border-bottom:1px solid #c8c4bc}
.docs-link a:hover{color:#0B090A}

/* Auth note */
.auth-note{border:1px solid #c8c4bc;padding:12px 24px;font-family:'IBM Plex Mono',monospace;font-size:11px;color:#6a6a6a;margin-bottom:32px}
@media(max-width:768px){.auth-note{padding:10px 12px}}
.auth-note code{background:#eae6de;padding:2px 6px;font-size:11px}

/* Footer */
.page-footer{border-top:1px solid #c8c4bc;padding-top:16px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}
.page-footer span{font-family:'IBM Plex Mono',monospace;font-size:11px;color:#6a6a6a}
.page-footer a{font-family:'IBM Plex Mono',monospace;font-size:11px;color:#6a6a6a;text-decoration:none;border-bottom:1px solid #c8c4bc}
.page-footer a:hover{color:#0B090A}

/* Key generation modal */
.keygen-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.4);z-index:100;align-items:center;justify-content:center}
.keygen-overlay.active{display:flex}
.keygen-box{background:#F3F2ED;border:2px solid #0B090A;padding:32px;max-width:420px;width:90%;position:relative}
.keygen-box h3{font-family:'IBM Plex Mono',monospace;font-size:14px;font-weight:600;margin-bottom:16px;border:none;padding:0;text-transform:uppercase;letter-spacing:1px}
.keygen-box .close{position:absolute;top:12px;right:16px;cursor:pointer;font-size:1.2rem;color:#6a6a6a;background:none;border:none}
.keygen-box label{display:block;font-size:12px;color:#3a3a3a;margin-bottom:4px;font-weight:500;font-family:'IBM Plex Mono',monospace}
.keygen-box input{width:100%;padding:8px 12px;border:1px solid #c8c4bc;font-family:'IBM Plex Sans',sans-serif;font-size:13px;margin-bottom:14px;background:#F3F2ED}
.keygen-box input:focus{outline:none;border-color:#0B090A}
.keygen-box .submit-btn{width:100%;padding:10px;background:#0B090A;color:#F3F2ED;border:none;font-family:'IBM Plex Mono',monospace;font-size:12px;cursor:pointer;font-weight:500;letter-spacing:0.5px}
.keygen-box .submit-btn:hover{background:#2a2a2a}
.keygen-box .submit-btn:disabled{background:#999;cursor:not-allowed}
.key-result{display:none;margin-top:16px}
.key-result .key-display{background:#0B090A;color:#4ade80;padding:12px 16px;font-family:'IBM Plex Mono',monospace;font-size:12px;word-break:break-all;margin:8px 0}
.key-result .copy-btn{padding:6px 14px;border:1px solid #0B090A;background:transparent;font-family:'IBM Plex Mono',monospace;font-size:11px;cursor:pointer}
.key-result .copy-btn:hover{background:#0B090A;color:#F3F2ED}
.key-result .warning{font-size:11px;color:#c0392b;margin-top:10px;font-weight:500;font-family:'IBM Plex Mono',monospace}
.keygen-error{color:#c0392b;font-size:12px;margin-top:8px;display:none;font-family:'IBM Plex Mono',monospace}
</style>
</head>
<body>
<div class="container">
<a href="/" class="back-link">&larr; Back to dashboard</a>

<!-- Hero header -->
<div class="hero">
  <div class="hero-title">
    <div class="hero-left">
      <svg viewBox="120 0 880 720" width="60" height="49" xmlns="http://www.w3.org/2000/svg"><g stroke="none"><g fill="#20222d"><path d="M 941.49 629.18 L 946.51 637.25 L 951.99 646.13 L 954.51 650.28 L 981.98 694.03 L 984.45 698.16 Q 985.37 701.10 987.50 703.29 L 990.00 706.79 A 0.55 0.54 72.2 0 1 989.56 707.65 L 279.25 707.90 L 274.56 707.88 L 264.29 707.88 L 262.52 707.88 L 262.04 707.87 L 260.71 707.88 L 128.66 707.90 A 0.84 0.84 0.0 0 1 127.94 706.63 Q 129.14 704.66 130.44 702.38 Q 132.06 699.53 143.46 680.74 Q 197.91 590.97 201.95 584.46 C 207.70 575.21 212.96 567.68 217.55 560.11 Q 218.04 559.30 227.82 543.37 Q 241.93 520.40 384.32 286.58 Q 422.33 224.16 466.63 151.77 C 469.44 147.19 471.27 143.29 474.15 138.93 Q 478.82 131.84 484.04 122.34 Q 487.51 116.02 489.30 113.40 C 491.39 110.33 494.66 105.41 496.61 101.63 C 500.06 94.92 504.16 88.92 508.67 81.45 Q 527.86 49.63 535.73 36.72 Q 537.88 33.18 545.53 21.75 Q 547.21 19.24 548.10 17.43 C 549.30 14.99 550.17 12.86 551.61 11.60 A 1.30 1.30 0.0 0 1 553.62 11.96 C 556.47 17.30 559.17 22.44 562.35 27.21 C 566.21 33.01 573.18 43.13 578.04 51.96 Q 582.43 59.91 587.61 67.40 Q 592.93 75.06 595.11 79.26 C 597.89 84.60 601.39 89.01 605.14 95.06 C 616.63 113.57 621.25 121.53 629.78 134.60 Q 634.71 142.15 635.89 144.30 Q 638.63 149.29 641.93 154.03 C 647.35 161.80 652.21 171.61 658.01 179.94 C 660.69 183.80 662.70 187.89 665.70 192.66 Q 753.21 331.91 812.05 424.96 Q 852.98 489.67 891.97 552.28 Q 901.27 567.22 902.63 569.24 Q 908.50 577.98 912.82 585.69 Q 918.42 595.70 926.71 607.29 Q 927.72 608.69 929.18 609.97 A 2.19 2.18 -85.6 0 1 929.59 610.45 L 941.49 629.18 Z M 955.26 688.21 C 840.54 505.58 735.55 339.82 600.25 124.50 Q 584.72 99.79 552.71 48.20 A 0.38 0.38 0.0 0 0 552.07 48.20 L 161.88 687.93 A 0.32 0.31 -74.1 0 0 162.14 688.41 L 955.15 688.41 A 0.13 0.13 0.0 0 0 955.26 688.21 Z"/></g><g fill="#f8eee5"><path d="M 955.26 688.21 A 0.13 0.13 0.0 0 1 955.15 688.41 L 162.14 688.41 A 0.32 0.31 -74.1 0 1 161.88 687.93 L 552.07 48.20 A 0.38 0.38 0.0 0 1 552.71 48.20 Q 584.72 99.79 600.25 124.50 C 735.55 339.82 840.54 505.58 955.26 688.21 Z M 255.54 654.21 L 893.85 654.22 A 0.31 0.31 0.0 0 0 894.11 653.74 L 886.54 641.61 Q 883.95 637.52 881.25 633.23 Q 673.15 303.22 643.32 255.45 Q 612.22 205.65 574.86 146.13 Q 564.76 130.04 554.21 112.39 A 0.61 0.60 45.3 0 0 553.17 112.38 L 223.00 653.69 A 0.38 0.38 0.0 0 0 223.33 654.27 L 255.54 654.21 Z"/><path d="M 597.88 193.49 L 578.12 225.37 L 554.55 187.51 A 0.44 0.44 0.0 0 0 553.80 187.51 L 294.63 612.43 A 0.46 0.46 0.0 0 0 294.90 613.11 Q 297.14 613.72 299.71 613.23 L 655.21 613.38 Q 654.95 614.72 655.50 615.51 Q 666.67 631.59 677.13 648.56 L 342.21 648.60 L 340.96 648.62 L 238.82 648.62 L 233.10 648.70 A 0.36 0.36 0.0 0 1 232.79 648.16 L 553.29 122.70 A 0.39 0.39 0.0 0 1 553.95 122.70 L 597.88 193.49 Z"/><path d="M 575.21 231.45 L 574.57 231.67 A 1.45 1.43 6.9 0 0 573.83 232.24 Q 570.43 237.24 567.54 242.55 Q 561.79 253.10 554.46 262.26 L 365.83 571.98 A 0.46 0.46 0.0 0 0 366.22 572.67 L 388.97 572.62 L 628.21 572.64 Q 628.16 573.77 628.59 574.43 Q 639.50 591.00 650.46 607.78 L 304.72 608.05 Q 304.30 608.02 304.24 607.56 A 0.31 0.30 60.0 0 0 304.29 607.36 L 553.73 197.96 A 0.43 0.42 -45.4 0 1 554.45 197.95 L 575.21 231.45 Z"/><path d="M 623.37 567.14 L 387.95 567.15 L 375.80 567.16 A 0.26 0.25 -73.9 0 1 375.59 566.77 L 495.94 369.15 Q 546.99 448.28 598.42 527.83 Q 608.74 543.80 619.08 559.75 Q 621.45 563.41 623.37 567.14 Z"/></g><g fill="#27252a"><path d="M 886.54 641.61 Q 886.01 645.16 883.46 647.02 L 597.88 193.49 L 553.95 122.70 A 0.39 0.39 0.0 0 0 553.29 122.70 L 232.79 648.16 A 0.36 0.36 0.0 0 0 233.10 648.70 L 238.82 648.62 L 244.74 653.50 A 1.47 1.40 64.2 0 0 245.62 653.83 L 255.54 654.21 L 223.33 654.27 A 0.38 0.38 0.0 0 1 223.00 653.69 L 553.17 112.38 A 0.61 0.60 45.3 0 1 554.21 112.39 Q 564.76 130.04 574.86 146.13 Q 612.22 205.65 643.32 255.45 Q 673.15 303.22 881.25 633.23 Q 883.95 637.52 886.54 641.61 Z"/><path d="M 578.12 225.37 L 821.92 612.61 A 0.47 0.47 0.0 0 1 821.52 613.33 L 802.09 613.37 L 801.56 607.77 L 811.85 608.05 A 0.32 0.32 0.0 0 0 812.13 607.56 L 575.21 231.45 L 554.45 197.95 A 0.43 0.42 -45.4 0 0 553.73 197.96 L 304.29 607.36 A 0.31 0.30 60.0 0 0 304.24 607.56 Q 304.30 608.02 304.72 608.05 L 299.71 613.23 Q 297.14 613.72 294.90 613.11 A 0.46 0.46 0.0 0 1 294.63 612.43 L 553.80 187.51 A 0.44 0.44 0.0 0 1 554.55 187.51 L 578.12 225.37 Z"/><path d="M 554.46 262.26 L 749.53 571.71 A 0.52 0.52 0.0 0 1 749.09 572.50 L 738.95 572.62 Q 739.09 569.76 737.55 567.15 Q 738.75 567.29 739.31 567.08 A 0.63 0.62 64.3 0 0 739.63 566.16 L 554.81 273.32 A 0.48 0.47 44.8 0 0 554.00 273.32 L 495.94 369.15 L 375.59 566.77 A 0.26 0.25 -73.9 0 0 375.80 567.16 L 387.95 567.15 L 388.97 572.62 L 366.22 572.67 A 0.46 0.46 0.0 0 1 365.83 571.98 L 554.46 262.26 Z"/><path d="M 623.37 567.14 L 737.55 567.15 Q 739.09 569.76 738.95 572.62 L 628.21 572.64 L 388.97 572.62 L 387.95 567.15 L 623.37 567.14 Z"/><path d="M 650.46 607.78 L 801.56 607.77 L 802.09 613.37 L 655.21 613.38 L 299.71 613.23 L 304.72 608.05 L 650.46 607.78 Z"/><path d="M 886.54 641.61 L 894.11 653.74 A 0.31 0.31 0.0 0 0 893.85 654.22 L 255.54 654.21 L 245.62 653.83 A 1.47 1.40 64.2 0 0 244.74 653.50 L 238.82 648.62 L 340.96 648.62 L 342.21 648.60 L 677.13 648.56 L 883.69 648.56 A 0.41 0.40 80.3 0 0 884.07 648.02 Q 883.92 647.57 883.46 647.02 Q 886.01 645.16 886.54 641.61 Z"/></g><g fill="#fc988f"><path d="M 883.46 647.02 Q 883.92 647.57 884.07 648.02 A 0.41 0.40 80.3 0 1 883.69 648.56 L 677.13 648.56 Q 666.67 631.59 655.50 615.51 Q 654.95 614.72 655.21 613.38 L 802.09 613.37 L 821.52 613.33 A 0.47 0.47 0.0 0 0 821.92 612.61 L 578.12 225.37 L 597.88 193.49 L 883.46 647.02 Z"/><path d="M 575.21 231.45 L 812.13 607.56 A 0.32 0.32 0.0 0 1 811.85 608.05 L 801.56 607.77 L 650.46 607.78 Q 639.50 591.00 628.59 574.43 Q 628.16 573.77 628.21 572.64 L 738.95 572.62 L 749.09 572.50 A 0.52 0.52 0.0 0 0 749.53 571.71 L 554.46 262.26 Q 561.79 253.10 567.54 242.55 Q 570.43 237.24 573.83 232.24 A 1.45 1.43 6.9 0 1 574.57 231.67 L 575.21 231.45 Z"/><path d="M 737.55 567.15 L 623.37 567.14 Q 621.45 563.41 619.08 559.75 Q 608.74 543.80 598.42 527.83 Q 546.99 448.28 495.94 369.15 L 554.00 273.32 A 0.48 0.47 44.8 0 1 554.81 273.32 L 739.63 566.16 A 0.63 0.62 64.3 0 1 739.31 567.08 Q 738.75 567.29 737.55 567.15 Z"/></g></g></svg>
      <h1><b>Basis Protocol</b> API</h1>
    </div>
    <div class="hero-form">
      <span>FORM API-001 &middot; BASIS PROTOCOL</span>
    </div>
  </div>
  <div class="hero-stats">
    <span>3 Tiers</span><div class="sep"></div>
    <span>55+ Endpoints</span><div class="sep"></div>
    <span>Self-Serve Keys</span><div class="sep"></div>
    <span>Free During Beta</span>
  </div>
  <div class="hero-formula">Risk primitives for on-chain finance</div>
</div>

<!-- Tier cards -->
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

<!-- Endpoint reference -->
<div class="ref-box">
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

  <div class="docs-link">Full interactive docs: <a href="/docs">/docs</a></div>
</div>

<!-- Auth -->
<div class="auth-note">
  <strong>Authentication:</strong> pass your API key as <code>?apikey=YOUR_KEY</code> or <code>X-Api-Key: YOUR_KEY</code> header
</div>

<!-- Footer -->
<div class="page-footer">
  <span>Basis Protocol &middot; basisprotocol.xyz</span>
  <div>
    <a href="/" style="margin-right:12px">Dashboard</a>
    <a href="/docs">API Docs</a>
  </div>
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
      <p style="font-size:13px;font-weight:600;font-family:'IBM Plex Mono',monospace">Your API key:</p>
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
      h += '<table><tr><th>Protocol</th><th>Score</th></tr>';
      protos.sort((a,b)=>(b.overall_score||0)-(a.overall_score||0)).forEach(p => {
        h += '<tr><td>'+(p.protocol_name||p.protocol_slug)+'</td>';
        h += '<td>'+(p.overall_score!=null?p.overall_score.toFixed(1):'—')+'</td></tr>';
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


@app.post("/api/admin/reindex")
async def admin_reindex_batch(request: Request, background_tasks: BackgroundTasks, batch_size: int = Query(default=500)):
    """Run one batch of wallet re-indexing. Call externally via cron."""
    _check_admin_key(request)
    try:
        from app.indexer.pipeline import run_pipeline_batch

        background_tasks.add_task(run_pipeline_batch, batch_size)
        return {
            "status": "accepted",
            "batch_size": batch_size,
            "message": "Reindex started. Check GET /api/admin/reindex-status for progress.",
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.get("/api/admin/reindex-status")
async def admin_reindex_status(request: Request):
    """Return current/last reindex run metadata and DB freshness."""
    _check_admin_key(request)
    try:
        from app.indexer.pipeline import get_reindex_status
        return get_reindex_status()
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.post("/api/admin/rebuild-profiles")
async def admin_rebuild_profiles(request: Request, background_tasks: BackgroundTasks):
    """Rebuild unified cross-chain wallet profiles."""
    _check_admin_key(request)
    try:
        from app.indexer.profiles import rebuild_all_profiles
        background_tasks.add_task(rebuild_all_profiles)
        return {"status": "started"}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.post("/api/admin/decay-edges")
async def admin_decay_edges(request: Request):
    """Recalculate edge weights with time-decay multiplier."""
    _check_admin_key(request)
    try:
        from app.indexer.edges import decay_edges
        result = decay_edges()
        return result
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.post("/api/admin/prune-edges")
async def admin_prune_edges(request: Request):
    """Archive edges older than 180 days."""
    _check_admin_key(request)
    try:
        from app.indexer.edges import prune_stale_edges
        result = prune_stale_edges()
        return result
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.post("/api/admin/collect-cda")
async def trigger_cda_collection(key: str = Query(default=None)):
    """Manually trigger CDA collection pipeline. Requires admin key."""
    admin_key = os.environ.get("ADMIN_KEY", "")
    if not admin_key or not key or not hmac.compare_digest(key, admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized — provide ?key=YOUR_ADMIN_KEY")

    try:
        import asyncio as _asyncio
        from app.services.cda_collector import run_collection
        _asyncio.create_task(run_collection())
        return {"status": "collection_started"}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.post("/api/admin/seed-cda-registry")
async def seed_cda_registry(key: str = Query(default=None)):
    """Seed CDA issuer registry from STABLECOIN_REGISTRY config. Requires admin key."""
    admin_key = os.environ.get("ADMIN_KEY", "")
    if not admin_key or not key or not hmac.compare_digest(key, admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized — provide ?key=YOUR_ADMIN_KEY")
    try:
        _seed_cda_issuer_registry()
        count = fetch_one("SELECT COUNT(*) AS cnt FROM cda_issuer_registry WHERE is_active = TRUE")
        return {"status": "seeded", "active_issuers": count["cnt"] if count else 0}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


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
    if not admin_key or not key or not hmac.compare_digest(key, admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized — provide ?key=YOUR_ADMIN_KEY")

    try:
        from app.services.cda_collector import setup_monitors
        count = await setup_monitors()
        return {"status": "ok", "monitors_created": count}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


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
    """List all CDA-tracked issuers with last verification and source update timestamps."""
    rows = fetch_all("""
        SELECT r.asset_symbol, r.issuer_name, r.transparency_url,
               r.collection_method, r.disclosure_type, r.created_at,
               r.last_successful_collection,
               (
                   SELECT MAX(e.extracted_at)
                   FROM cda_vendor_extractions e
                   WHERE e.asset_symbol = r.asset_symbol
               ) AS last_attestation,
               (
                   SELECT e.structured_data
                   FROM cda_vendor_extractions e
                   WHERE e.asset_symbol = r.asset_symbol
                     AND e.structured_data IS NOT NULL
                   ORDER BY e.extracted_at DESC
                   LIMIT 1
               ) AS latest_structured_data
        FROM cda_issuer_registry r
        WHERE r.is_active = TRUE
        ORDER BY last_attestation DESC NULLS LAST, r.asset_symbol
    """)
    issuers = []
    for r in rows:
        # last_verified: when Basis last checked/extracted (extracted_at or last_successful_collection)
        last_verified = r.get("last_attestation") or r.get("last_successful_collection")
        # source_updated: when the underlying source data actually changed
        source_updated = None
        sd = r.get("latest_structured_data") or {}
        if isinstance(sd, dict):
            source_updated = (
                sd.get("attestation_date")
                or sd.get("report_date")
                or sd.get("as_of_date")
                or sd.get("publication_date")
            )
        issuers.append({
            "asset_symbol": r["asset_symbol"],
            "issuer_name": r["issuer_name"],
            "transparency_url": r["transparency_url"],
            "collection_method": r["collection_method"],
            "disclosure_type": r.get("disclosure_type", "fiat-reserve"),
            "last_attestation": r["last_attestation"].isoformat() if r.get("last_attestation") else None,
            "last_verified": last_verified.isoformat() if last_verified else None,
            "source_updated": str(source_updated) if source_updated else None,
        })
    return {"issuers": issuers, "count": len(issuers)}


def _extract_value(val):
    """Extract a simple value from potentially nested structures."""
    if isinstance(val, dict):
        return val.get("value", val.get("amount", val.get("percentage", str(val))))
    if isinstance(val, list):
        return str(val)
    return val


def _classify_attestation_quality(att: dict, disclosure_type: str = None) -> dict:
    """Add human-readable quality classification to an attestation.
    Type-aware: different disclosure types have different expected fields."""
    sd = att.get("structured_data")
    if not sd:
        att["quality"] = "empty"
        att["quality_label"] = "No data extracted"
        att["display_fields"] = []
        return att

    if isinstance(sd, str):
        import json as _j
        try:
            sd = _j.loads(sd)
        except Exception:
            att["quality"] = "empty"
            att["quality_label"] = "Parse error"
            att["display_fields"] = []
            return att

    # Unwrap Reducto citation wrappers
    def _unwrap(obj):
        if isinstance(obj, dict):
            if "value" in obj and "citations" in obj:
                return _unwrap(obj["value"])
            return {k: _unwrap(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_unwrap(item) for item in obj]
        return obj
    sd = _unwrap(sd)

    dtype = disclosure_type or "fiat-reserve"

    if dtype == "synthetic-derivative":
        display, quality, quality_label = _classify_synthetic(sd)
    elif dtype == "rwa-tokenized":
        display, quality, quality_label = _classify_rwa(sd)
    elif dtype in ("overcollateralized", "algorithmic"):
        display, quality, quality_label = _classify_onchain(sd, dtype)
    else:
        display, quality, quality_label = _classify_fiat_reserve(sd)

    att["quality"] = quality
    att["quality_label"] = quality_label
    att["display_fields"] = display
    att["disclosure_type"] = dtype
    return att


def _classify_fiat_reserve(sd: dict) -> tuple:
    """Original fiat-reserve classification logic."""
    has_reserves = sd.get("total_reserves_usd") is not None
    has_supply = sd.get("total_supply") is not None
    has_date = sd.get("attestation_date") is not None
    has_auditor = sd.get("auditor") or sd.get("auditor_name")
    has_composition = sd.get("reserve_composition") or sd.get("cash_pct") or sd.get("tbills_pct")

    display = []
    if has_reserves:
        display.append({"label": "Total Reserves", "value": _extract_value(sd["total_reserves_usd"]), "type": "currency"})
    if has_supply:
        display.append({"label": "Total Supply", "value": _extract_value(sd["total_supply"]), "type": "number"})
    if has_reserves and has_supply:
        try:
            res_val = _extract_value(sd["total_reserves_usd"])
            sup_val = _extract_value(sd["total_supply"])
            ratio = float(res_val) / float(sup_val)
            display.append({"label": "Reserve Ratio", "value": round(ratio, 4), "type": "ratio"})
        except (ValueError, ZeroDivisionError, TypeError):
            pass
    if has_date:
        display.append({"label": "Report Date", "value": _extract_value(sd["attestation_date"]), "type": "text"})
    if has_auditor:
        display.append({"label": "Auditor", "value": _extract_value(sd.get("auditor") or sd.get("auditor_name")), "type": "text"})
    if has_composition:
        comp = sd.get("reserve_composition", {})
        if isinstance(comp, dict):
            for k, v in comp.items():
                if v is not None:
                    extracted = _extract_value(v)
                    if extracted is not None and extracted != 0 and extracted != "0":
                        label = k.replace("_pct", "").replace("_", " ").title()
                        display.append({"label": label, "value": extracted, "type": "percent"})

    if has_reserves or (has_date and has_auditor):
        return display, "full", "Reserve attestation"
    elif has_date or has_auditor:
        return display, "partial", "Partial data"
    elif sd.get("content_length") or sd.get("excerpt_count"):
        return display, "metadata", "Page scraped — no reserve data extracted"
    else:
        return display, "minimal", "Minimal data"


def _classify_synthetic(sd: dict) -> tuple:
    """Classification for synthetic/derivative-backed stablecoins."""
    display = []
    has_data = False

    supply = sd.get("total_supply")
    if supply:
        display.append({"label": "Total Supply", "value": _extract_value(supply), "type": "number"})
        has_data = True

    backing = sd.get("backing_assets", {})
    if isinstance(backing, dict):
        total_backing = backing.get("total_value_usd")
        if total_backing:
            display.append({"label": "Total Backing", "value": _extract_value(total_backing), "type": "currency"})
            has_data = True
        for k in ("staked_eth_usd", "derivatives_notional_usd", "other_usd"):
            v = backing.get(k)
            if v and _extract_value(v):
                label = k.replace("_usd", "").replace("_", " ").title()
                display.append({"label": label, "value": _extract_value(v), "type": "currency"})

    cr = sd.get("collateral_ratio")
    if cr:
        display.append({"label": "Collateral Ratio", "value": _extract_value(cr), "type": "ratio"})
        has_data = True

    oi = sd.get("open_interest")
    if oi:
        display.append({"label": "Open Interest", "value": _extract_value(oi), "type": "currency"})
        has_data = True

    fr = sd.get("funding_rate")
    if fr:
        display.append({"label": "Funding Rate", "value": _extract_value(fr), "type": "percent"})

    custodians = sd.get("custodians", [])
    if isinstance(custodians, list) and custodians:
        for c in custodians:
            if isinstance(c, dict) and c.get("name"):
                label = f"Custodian: {c['name']}"
                val = c.get("assets_held_usd") or c.get("percentage")
                ctype = "currency" if c.get("assets_held_usd") else "percent"
                if val:
                    display.append({"label": label, "value": _extract_value(val), "type": ctype})
            elif isinstance(c, str):
                display.append({"label": "Custodian", "value": c, "type": "text"})
        has_data = True

    att_date = sd.get("attestation_date")
    if att_date:
        display.append({"label": "Report Date", "value": _extract_value(att_date), "type": "text"})
        has_data = True

    auditor = sd.get("auditor_name") or sd.get("auditor")
    if auditor:
        display.append({"label": "Attestor", "value": _extract_value(auditor), "type": "text"})

    if has_data and len(display) >= 3:
        return display, "full", "Custodian attestation"
    elif has_data:
        return display, "partial", "Partial custodian data"
    elif sd.get("content_length") or sd.get("excerpt_count"):
        return display, "metadata", "Page scraped — no custodian data extracted"
    else:
        return display, "empty", "No data extracted"


def _classify_rwa(sd: dict) -> tuple:
    """Classification for RWA/tokenized asset attestations."""
    display = []
    has_data = False

    nav = sd.get("nav_per_token")
    if nav:
        display.append({"label": "NAV / Token", "value": _extract_value(nav), "type": "currency"})
        has_data = True

    total = sd.get("total_assets_usd")
    if total:
        display.append({"label": "Total AUM", "value": _extract_value(total), "type": "currency"})
        has_data = True

    supply = sd.get("total_supply")
    if supply:
        display.append({"label": "Total Supply", "value": _extract_value(supply), "type": "number"})

    yld = sd.get("yield_rate")
    if yld:
        display.append({"label": "Yield / APY", "value": _extract_value(yld), "type": "percent"})

    wam = sd.get("weighted_avg_maturity_days")
    if wam:
        display.append({"label": "Avg Maturity", "value": f"{_extract_value(wam)} days", "type": "text"})

    holdings = sd.get("underlying_holdings", {})
    if isinstance(holdings, dict):
        for k, v in holdings.items():
            if v and _extract_value(v):
                label = k.replace("_pct", "").replace("_", " ").title()
                display.append({"label": label, "value": _extract_value(v), "type": "percent"})

    att_date = sd.get("attestation_date")
    if att_date:
        display.append({"label": "Report Date", "value": _extract_value(att_date), "type": "text"})
        has_data = True

    auditor = sd.get("auditor_name")
    if auditor:
        display.append({"label": "Auditor", "value": _extract_value(auditor), "type": "text"})

    if has_data and len(display) >= 3:
        return display, "full", "NAV attestation"
    elif has_data:
        return display, "partial", "Partial NAV data"
    else:
        return display, "empty", "No data extracted"


def _classify_onchain(sd: dict, dtype: str) -> tuple:
    """Classification for on-chain verified assets (DAI, FRAX)."""
    label = "Overcollateralized vault" if dtype == "overcollateralized" else "Algorithmic"
    return [], "not_applicable", f"{label} — verified on-chain"


@app.get("/api/cda/issuers/{symbol}/latest")
async def cda_issuer_latest(symbol: str):
    """Most recent attestation for a specific issuer, with evidence hash."""
    import hashlib
    import json as _json

    row = fetch_one("""
        SELECT e.id, e.asset_symbol, e.source_url, e.source_type, e.extraction_method,
               e.extraction_vendor, e.structured_data, e.confidence_score,
               e.extraction_warnings, e.extracted_at,
               r.disclosure_type, r.issuer_name
        FROM cda_vendor_extractions e
        LEFT JOIN cda_issuer_registry r ON UPPER(r.asset_symbol) = UPPER(e.asset_symbol)
        WHERE UPPER(e.asset_symbol) = %s
          AND e.structured_data IS NOT NULL
        ORDER BY e.extracted_at DESC
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
        "issuer_name": row.get("issuer_name"),
    }

    raw_data = _json.dumps(attestation, sort_keys=True, separators=(',', ':'), default=str)
    evidence_hash = '0x' + hashlib.sha256(raw_data.encode()).hexdigest()
    attestation["evidence_hash"] = evidence_hash

    # Include source URLs so frontend can show data provenance
    reg_full = fetch_one(
        "SELECT source_urls FROM cda_issuer_registry WHERE UPPER(asset_symbol) = %s",
        (symbol.upper(),)
    )
    if reg_full and reg_full.get("source_urls"):
        attestation["source_urls"] = reg_full["source_urls"]

    disclosure_type = row.get("disclosure_type", "fiat-reserve")
    return _classify_attestation_quality(attestation, disclosure_type=disclosure_type)


@app.get("/api/cda/issuers/{symbol}/history")
async def cda_issuer_history(symbol: str, days: int = Query(default=90, ge=1, le=365)):
    """Attestation history for a specific issuer."""
    reg = fetch_one(
        "SELECT disclosure_type FROM cda_issuer_registry WHERE UPPER(asset_symbol) = %s",
        (symbol.upper(),)
    )
    disc_type = reg.get("disclosure_type", "fiat-reserve") if reg else "fiat-reserve"

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

    attestations = [_classify_attestation_quality(a, disclosure_type=disc_type) for a in attestations]

    return {
        "asset": symbol.upper(),
        "days": days,
        "attestations": attestations,
        "count": len(attestations),
        "disclosure_type": disc_type,
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

    attestation_count = fetch_one("SELECT COUNT(*) as cnt FROM cda_vendor_extractions")

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
        "total_attestations": attestation_count["cnt"] if attestation_count else 0,
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
# Admin — Manual Assessment Event Creation
# =============================================================================

@app.post("/api/admin/assessment-events")
async def admin_create_assessment_event(request: Request):
    """Create a manual assessment event (e.g. exploit, incident). Admin-key protected."""
    _check_admin_key(request)
    try:
        body = await request.json()

        import hashlib

        required = ["entity_type", "entity_id", "severity", "title"]
        for field in required:
            if field not in body:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

        from app.specs.severity_v1 import SEVERITY_ORDER
        if body["severity"] not in SEVERITY_ORDER:
            raise HTTPException(status_code=400, detail=f"Invalid severity. Valid: {list(SEVERITY_ORDER.keys())}")

        # Build the assessment dict compatible with store_assessment schema
        trigger_detail = {
            "entity_type": body["entity_type"],
            "entity_id": body["entity_id"],
            "title": body["title"],
            "description": body.get("description", ""),
            "data": body.get("data", {}),
            "manual": True,
        }

        # Use entity_id as wallet_address placeholder for protocol-level events
        wallet_addr = body.get("wallet_address", f"protocol:{body['entity_id']}")

        # Compute content hash for idempotency
        canonical = json.dumps(trigger_detail, sort_keys=True, separators=(",", ":"))
        content_hash = "0x" + hashlib.sha256(canonical.encode()).hexdigest()

        assessment = {
            "wallet_address": wallet_addr,
            "chain": body.get("chain", "solana" if body.get("data", {}).get("chain") == "solana" else "ethereum"),
            "trigger_type": body.get("event_type", "manual_incident"),
            "trigger_detail": trigger_detail,
            "wallet_risk_score": None,
            "wallet_risk_score_prev": None,
            "concentration_hhi": None,
            "concentration_hhi_prev": None,
            "coverage_ratio": None,
            "total_stablecoin_value": body.get("data", {}).get("estimated_loss_usd"),
            "holdings_snapshot": body.get("holdings_snapshot", []),
            "severity": body["severity"],
            "broadcast": body["severity"] in ("alert", "critical"),
            "content_hash": content_hash,
            "methodology_version": body.get("methodology_version", "manual-v1.0.0"),
        }

        from app.agent.store import store_assessment
        event_id = store_assessment(assessment)

        if not event_id:
            return {"status": "skipped", "reason": "Duplicate event (same content_hash within 1 hour)"}

        return {
            "status": "created",
            "event_id": event_id,
            "severity": body["severity"],
            "content_hash": content_hash,
            "entity": f"{body['entity_type']}:{body['entity_id']}",
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


# =============================================================================
# PSI (Protocol Solvency Index) API
# =============================================================================

_SOLANA_PROTOCOL_SLUGS = {"drift", "jupiter-perpetual-exchange", "raydium"}


@app.get("/api/psi/scores")
async def psi_scores():
    """All scored protocols — latest score per protocol."""
    cached = _cache.get("psi_scores", ttl=30)
    if cached:
        return cached
    rows = fetch_all("""
        SELECT DISTINCT ON (protocol_slug)
            id, protocol_slug, protocol_name, overall_score, grade,
            category_scores, component_scores, raw_values,
            formula_version, computed_at
        FROM psi_scores
        ORDER BY protocol_slug, computed_at DESC
    """)
    from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
    from app.scoring_engine import compute_confidence_tag
    psi_cats_total = len(PSI_V01_DEFINITION["categories"])
    psi_comps_total = len(PSI_V01_DEFINITION["components"])

    results = []
    for row in rows:
        slug = row["protocol_slug"]
        cat_scores = row.get("category_scores") or {}
        comp_scores = row.get("component_scores") or {}
        comps_populated = len(comp_scores)
        psi_coverage = round(comps_populated / max(psi_comps_total, 1), 2)
        psi_missing = sorted(set(PSI_V01_DEFINITION["categories"].keys()) - set(cat_scores.keys()))
        psi_conf = compute_confidence_tag(psi_cats_total - len(psi_missing), psi_cats_total, psi_coverage, psi_missing)

        results.append({
            "protocol_slug": slug,
            "protocol_name": row["protocol_name"],
            "score": float(row["overall_score"]) if row.get("overall_score") else None,
            "confidence": psi_conf["confidence"],
            "confidence_tag": psi_conf["tag"],
            "missing_categories": psi_conf["missing_categories"],
            "component_coverage": psi_coverage,
            "components_populated": comps_populated,
            "components_total": psi_comps_total,
            "chain": "solana" if slug in _SOLANA_PROTOCOL_SLUGS else "ethereum",
            "category_scores": cat_scores,
            "formula_version": row.get("formula_version"),
            "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        })
    psi_result = {
        "protocols": results,
        "count": len(results),
        "index": "psi",
        "version": PSI_V01_DEFINITION["version"],
        "methodology_version": f"psi-{PSI_V01_DEFINITION['version']}",
    }
    _cache.set("psi_scores", psi_result)
    return psi_result


@app.get("/api/psi/definition")
async def psi_definition():
    """Return the full PSI index definition with methodology changelog."""
    from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
    return {
        **PSI_V01_DEFINITION,
        "methodology_version": f"psi-{PSI_V01_DEFINITION['version']}",
        "changelog": PSI_METHODOLOGY_VERSIONS["versions"],
    }


# =============================================================================
# PSI Temporal Reconstruction
# =============================================================================

@app.get("/api/psi/scores/{slug}/at/{date_str}")
async def psi_score_at_date(slug: str, date_str: str):
    """Reconstruct PSI score for a protocol at a historical date."""
    try:
        from datetime import date as date_type
        from app.services.psi_temporal_engine import reconstruct_psi_score

        target = date_type.fromisoformat(date_str)
        result = reconstruct_psi_score(slug, target)
        return result
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/api/psi/scores/{slug}/range")
async def psi_score_range(
    slug: str,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
):
    """Reconstruct daily PSI scores for a date range (max 365 days)."""
    from app.services.psi_temporal_engine import reconstruct_psi_range
    to_date = date.fromisoformat(end) if end else date.today()
    from_date = date.fromisoformat(start) if start else to_date - timedelta(days=30)
    if (to_date - from_date).days > 365:
        raise HTTPException(status_code=400, detail="Max range is 365 days")
    scores = reconstruct_psi_range(slug, from_date, to_date)
    return {
        "protocol": slug,
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "scores": scores,
        "count": len(scores),
    }


@app.get("/api/psi/scores/{slug}/backtest/{event}")
async def psi_backtest_event(slug: str, event: str):
    """Reconstruct PSI scores during a named crisis event."""
    from app.services.psi_temporal_engine import reconstruct_psi_range
    from app.services.temporal_engine import CRISIS_EVENTS
    crisis = CRISIS_EVENTS.get(event)
    if not crisis:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown event '{event}'. Available: {list(CRISIS_EVENTS.keys())}",
        )
    from_date = date.fromisoformat(crisis["from"])
    to_date = date.fromisoformat(crisis["to"])
    scores = reconstruct_psi_range(slug, from_date, to_date)
    return {
        "event": event,
        "event_name": crisis["name"],
        "event_description": crisis["description"],
        "protocol": slug,
        "from": crisis["from"],
        "to": crisis["to"],
        "scores": scores,
        "count": len(scores),
    }


@app.post("/api/admin/psi-backfill")
async def admin_psi_backfill(request: Request, background_tasks: BackgroundTasks):
    """Trigger historical data backfill for all PSI protocols. Admin-key protected."""
    _check_admin_key(request)
    try:
        body = await request.json() if request.headers.get("content-type") == "application/json" else {}
        from_date = body.get("from_date", "2026-01-01")

        from app.services.psi_backfill import backfill_all_protocols, _backfill_running
        if _backfill_running:
            return {"status": "already_running", "message": "A backfill is already in progress."}
        background_tasks.add_task(backfill_all_protocols, from_date=from_date)

        return {"status": "started", "from_date": from_date, "message": "Backfill running in background. Check logs for progress."}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.post("/api/admin/protocol-expansion")
async def admin_run_protocol_expansion(request: Request):
    """Run the PSI protocol discovery + enrichment + promotion pipeline."""
    _check_admin_key(request)
    try:
        from app.collectors.psi_collector import (
            collect_collateral_exposure,
            sync_collateral_to_backlog,
            discover_protocols,
            enrich_protocol_backlog,
            promote_eligible_protocols,
        )
        collect_collateral_exposure()
        synced = sync_collateral_to_backlog()
        discovered = discover_protocols()
        enriched = enrich_protocol_backlog()
        promoted = promote_eligible_protocols()
        return {
            "synced": synced,
            "discovered": discovered,
            "enriched": enriched,
            "promoted": promoted,
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.get("/api/psi/scores/{slug}/verify")
async def verify_psi_score(slug: str):
    """Verify the latest PSI score by re-deriving from stored raw values."""
    row = fetch_one("""
        SELECT protocol_slug, overall_score, grade, raw_values,
               inputs_hash, formula_version, computed_at
        FROM psi_scores WHERE protocol_slug = %s
        ORDER BY computed_at DESC LIMIT 1
    """, (slug,))

    if not row:
        raise HTTPException(status_code=404, detail=f"No PSI score found for {slug}")

    stored_score = float(row["overall_score"]) if row.get("overall_score") else None
    stored_hash = row.get("inputs_hash")
    raw_values = row.get("raw_values", {})

    # Re-derive hash from stored raw values
    raw_canonical = json.dumps(raw_values, sort_keys=True, default=str)
    recomputed_hash = "0x" + hashlib.sha256(raw_canonical.encode()).hexdigest()

    # Re-derive score from stored raw values
    from app.collectors.psi_collector import score_protocol_from_raw
    recomputed = score_protocol_from_raw(slug, raw_values)
    recomputed_score = recomputed.get("overall_score") if recomputed else None

    hash_match = stored_hash == recomputed_hash if stored_hash else None
    score_match = abs(stored_score - recomputed_score) < 0.01 if stored_score and recomputed_score else None

    if hash_match and score_match:
        status = "verified"
    elif hash_match is False or score_match is False:
        status = "mismatch"
    else:
        status = "no_hash_stored"

    return {
        "protocol": slug,
        "verification": {
            "status": status,
            "stored_score": stored_score,
            "recomputed_score": round(recomputed_score, 2) if recomputed_score else None,
            "score_match": score_match,
            "hash_match": hash_match,
            "formula_version": row.get("formula_version"),
        },
        "hashes": {
            "stored_inputs_hash": stored_hash,
            "recomputed_inputs_hash": recomputed_hash,
        },
        "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
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

    from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
    from app.scoring_engine import compute_confidence_tag
    psi_cat_scores = row.get("category_scores") or {}
    psi_comp_scores = row.get("component_scores") or {}
    psi_comps_populated = len(psi_comp_scores)
    psi_comps_total = len(PSI_V01_DEFINITION["components"])
    psi_det_coverage = round(psi_comps_populated / max(psi_comps_total, 1), 2)
    psi_det_missing = sorted(set(PSI_V01_DEFINITION["categories"].keys()) - set(psi_cat_scores.keys()))
    psi_det_conf = compute_confidence_tag(
        len(PSI_V01_DEFINITION["categories"]) - len(psi_det_missing),
        len(PSI_V01_DEFINITION["categories"]),
        psi_det_coverage, psi_det_missing
    )

    return {
        "protocol_slug": row["protocol_slug"],
        "protocol_name": row["protocol_name"],
        "score": float(row["overall_score"]) if row.get("overall_score") else None,
        "confidence": psi_det_conf["confidence"],
        "confidence_tag": psi_det_conf["tag"],
        "missing_categories": psi_det_conf["missing_categories"],
        "component_coverage": psi_det_coverage,
        "components_populated": psi_comps_populated,
        "components_total": psi_comps_total,
        "category_scores": psi_cat_scores,
        "component_scores": psi_comp_scores,
        "raw_values": row.get("raw_values"),
        "formula_version": row.get("formula_version"),
        "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
    }


# =============================================================================
# Governance History & Market History — temporal security signals
# =============================================================================

@app.get("/api/protocols/{slug}/governance-history")
async def protocol_governance_history(slug: str):
    """Governance config snapshot history with change detection events."""
    try:
        from app.collectors.governance_detector import get_governance_history
        return get_governance_history(slug)
    except Exception as e:
        logger.error(f"governance-history failed for {slug}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/protocols/{slug}/market-history")
async def protocol_market_history(slug: str):
    """Market listing snapshot history with diffs and velocity events."""
    try:
        from app.collectors.collateral_coverage import get_market_history
        return get_market_history(slug)
    except Exception as e:
        logger.error(f"market-history failed for {slug}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Treasury Exposure — cross-reference protocol holdings with SII scores
# =============================================================================

def _build_protocol_treasury(rows_by_slug):
    """Build treasury response for a set of holdings grouped by protocol slug."""
    protocols = []
    total_stable_usd = 0.0
    covered_usd = 0.0
    uncovered_usd = 0.0
    unscored_symbols = set()

    for slug, rows in rows_by_slug.items():
        # Get PSI score for this protocol
        psi_row = fetch_one("""
            SELECT protocol_name, overall_score FROM psi_scores
            WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1
        """, (slug,))
        psi_name = psi_row["protocol_name"] if psi_row else slug
        psi_score = float(psi_row["overall_score"]) if psi_row and psi_row.get("overall_score") else None

        treasury_total = sum(r["usd_value"] for r in rows)
        stablecoin_rows = [r for r in rows if r["is_stablecoin"]]
        scored = [r for r in stablecoin_rows if r["sii_score"] is not None]
        unscored = [r for r in stablecoin_rows if r["sii_score"] is None]

        # Aggregate by symbol (may appear on multiple chains)
        def _agg(subset):
            by_sym = {}
            for r in subset:
                sym = r["token_symbol"]
                if sym not in by_sym:
                    by_sym[sym] = {"symbol": sym, "usd_value": 0.0, "sii_score": r.get("sii_score")}
                by_sym[sym]["usd_value"] += r["usd_value"]
            for v in by_sym.values():
                v["pct_of_treasury"] = round((v["usd_value"] / treasury_total) * 100, 2) if treasury_total else 0
                if v["sii_score"] is not None:
                    v["sii_score"] = round(float(v["sii_score"]), 1)
            return sorted(by_sym.values(), key=lambda x: x["usd_value"], reverse=True)

        scored_agg = _agg(scored)
        unscored_agg = _agg(unscored)

        # Weighted avg SII
        total_scored_usd = sum(s["usd_value"] for s in scored_agg)
        weighted_sii = None
        if total_scored_usd > 0:
            weighted_sii = round(
                sum(s["usd_value"] * s["sii_score"] for s in scored_agg) / total_scored_usd, 1
            )

        lowest = min(scored_agg, key=lambda x: x["sii_score"]) if scored_agg else None
        stablecoin_pct = round(
            sum(r["usd_value"] for r in stablecoin_rows) / treasury_total * 100, 2
        ) if treasury_total else 0

        protocols.append({
            "slug": slug,
            "name": psi_name,
            "psi_score": round(psi_score, 1) if psi_score else None,
            "treasury_total_usd": round(treasury_total, 2),
            "stablecoin_holdings": scored_agg,
            "unscored_stablecoins": [{"symbol": u["symbol"], "usd_value": u["usd_value"], "pct_of_treasury": u["pct_of_treasury"]} for u in unscored_agg],
            "stablecoin_pct": stablecoin_pct,
            "weighted_avg_sii": weighted_sii,
            "lowest_sii_held": {"symbol": lowest["symbol"], "sii_score": lowest["sii_score"]} if lowest else None,
        })

        total_stable_usd += sum(r["usd_value"] for r in stablecoin_rows)
        covered_usd += sum(r["usd_value"] for r in scored)
        uncovered_usd += sum(r["usd_value"] for r in unscored)
        for u in unscored_agg:
            unscored_symbols.add(u["symbol"])

    return protocols, {
        "total_stablecoin_exposure_usd": round(total_stable_usd, 2),
        "pct_covered_by_sii": round(covered_usd / total_stable_usd * 100, 1) if total_stable_usd else 0,
        "pct_uncovered": round(uncovered_usd / total_stable_usd * 100, 1) if total_stable_usd else 0,
        "unscored_stablecoins": sorted(unscored_symbols),
    }


@app.get("/api/protocols/treasury-exposure")
async def treasury_exposure():
    """All protocols' stablecoin holdings with SII cross-reference."""
    rows = fetch_all("""
        SELECT protocol_slug, token_name, token_symbol, chain, usd_value,
               is_stablecoin, sii_score
        FROM protocol_treasury_holdings
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM protocol_treasury_holdings)
        ORDER BY protocol_slug, usd_value DESC
    """)
    if not rows:
        return {"protocols": [], "coverage_summary": {}}

    rows_by_slug = {}
    for r in rows:
        rows_by_slug.setdefault(r["protocol_slug"], []).append(r)

    protocols, coverage = _build_protocol_treasury(rows_by_slug)
    protocols.sort(key=lambda p: p["treasury_total_usd"], reverse=True)

    return {"protocols": protocols, "coverage_summary": coverage}


@app.get("/api/protocols/{slug}/treasury")
async def protocol_treasury(slug: str):
    """Single protocol treasury breakdown with SII cross-reference."""
    rows = fetch_all("""
        SELECT token_name, token_symbol, chain, usd_value, is_stablecoin, sii_score
        FROM protocol_treasury_holdings
        WHERE protocol_slug = %s
          AND snapshot_date = (
              SELECT MAX(snapshot_date) FROM protocol_treasury_holdings WHERE protocol_slug = %s
          )
        ORDER BY usd_value DESC
    """, (slug, slug))
    if not rows:
        raise HTTPException(status_code=404, detail=f"No treasury data for '{slug}'")

    protocols, coverage = _build_protocol_treasury({slug: rows})
    return protocols[0] if protocols else {}


@app.get("/api/stablecoins/{symbol}/protocol-exposure")
async def stablecoin_protocol_exposure(symbol: str):
    """Reverse lookup — which protocols hold this stablecoin?"""
    sym_upper = symbol.upper()
    rows = fetch_all("""
        SELECT h.protocol_slug, h.usd_value, h.sii_score,
               p.protocol_name, p.overall_score as psi_score
        FROM protocol_treasury_holdings h
        LEFT JOIN LATERAL (
            SELECT protocol_name, overall_score
            FROM psi_scores
            WHERE protocol_slug = h.protocol_slug
            ORDER BY computed_at DESC LIMIT 1
        ) p ON true
        WHERE UPPER(h.token_symbol) = %s
          AND h.is_stablecoin = TRUE
          AND h.snapshot_date = (SELECT MAX(snapshot_date) FROM protocol_treasury_holdings)
        ORDER BY h.usd_value DESC
    """, (sym_upper,))

    # Get SII score for this stablecoin
    sii_row = fetch_one("""
        SELECT s.overall_score FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        WHERE UPPER(st.symbol) = %s
    """, (sym_upper,))
    sii_score = round(float(sii_row["overall_score"]), 1) if sii_row and sii_row.get("overall_score") else None

    held_by = []
    total_exposure = 0.0
    for r in rows:
        usd = float(r["usd_value"])
        held_by.append({
            "protocol": r["protocol_slug"],
            "protocol_name": r.get("protocol_name"),
            "psi_score": round(float(r["psi_score"]), 1) if r.get("psi_score") else None,
            "usd_value": round(usd, 2),
        })
        total_exposure += usd

    return {
        "symbol": sym_upper,
        "sii_score": sii_score,
        "held_by": held_by,
        "total_protocol_exposure_usd": round(total_exposure, 2),
    }


# =============================================================================
# Treasury Behavioral Events
# =============================================================================

@app.get("/api/treasury/events")
async def get_treasury_events(
    wallet: Optional[str] = Query(default=None),
    type: Optional[str] = Query(default=None, alias="type"),
    severity: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
):
    """Recent treasury behavioral events."""
    conditions = ["1=1"]
    params = []

    if wallet:
        conditions.append("te.wallet_address = %s")
        params.append(wallet.lower())
    if type:
        conditions.append("te.event_type = %s")
        params.append(type)
    if severity:
        conditions.append("te.severity = %s")
        params.append(severity)

    params.append(limit)
    where = " AND ".join(conditions)

    rows = fetch_all(f"""
        SELECT te.*, tr.entity_name, tr.entity_type
        FROM wallet_graph.treasury_events te
        LEFT JOIN wallet_graph.treasury_registry tr ON tr.address = te.wallet_address
        WHERE {where}
        ORDER BY te.detected_at DESC
        LIMIT %s
    """, tuple(params))

    return {
        "events": [
            {
                "id": r["id"],
                "wallet_address": r["wallet_address"],
                "entity_name": r.get("entity_name"),
                "entity_type": r.get("entity_type"),
                "event_type": r["event_type"],
                "event_data": r["event_data"],
                "severity": r["severity"],
                "confidence": r["confidence"],
                "detected_at": r["detected_at"].isoformat() if r.get("detected_at") else None,
                "stablecoins_involved": r.get("stablecoins_involved"),
                "protocols_involved": r.get("protocols_involved"),
                "risk_score_delta": float(r["risk_score_delta"]) if r.get("risk_score_delta") else None,
            }
            for r in (rows or [])
        ],
        "count": len(rows or []),
    }


@app.get("/api/treasury/{address}/profile")
async def treasury_profile(address: str):
    """Treasury risk profile — extends wallet profile with entity metadata and events."""
    addr_lower = address.lower()
    registry = fetch_one(
        "SELECT * FROM wallet_graph.treasury_registry WHERE address = %s",
        (addr_lower,)
    )

    # Get wallet risk profile
    profile = None
    try:
        from app.wallet_profile import generate_wallet_profile
        profile = generate_wallet_profile(addr_lower)
    except Exception:
        pass

    # Get recent events
    events = fetch_all("""
        SELECT event_type, event_data, severity, confidence, detected_at
        FROM wallet_graph.treasury_events
        WHERE wallet_address = %s
        ORDER BY detected_at DESC LIMIT 20
    """, (addr_lower,))

    if not registry and not profile:
        raise HTTPException(status_code=404, detail=f"Treasury '{address}' not found")

    return {
        "address": addr_lower,
        "entity": {
            "name": registry["entity_name"] if registry else None,
            "type": registry["entity_type"] if registry else None,
            "purpose": registry.get("wallet_purpose") if registry else None,
            "label_source": registry.get("label_source") if registry else None,
        } if registry else None,
        "risk_profile": profile,
        "recent_events": [
            {
                "event_type": e["event_type"],
                "event_data": e["event_data"],
                "severity": e["severity"],
                "detected_at": e["detected_at"].isoformat() if e.get("detected_at") else None,
            }
            for e in (events or [])
        ],
    }


@app.get("/api/treasury/registry")
async def treasury_registry():
    """All registered treasury wallets."""
    from app.collectors.treasury_flows import get_registered_treasuries
    treasuries = get_registered_treasuries()
    return {
        "treasuries": [dict(t) for t in treasuries],
        "count": len(treasuries),
    }


# =============================================================================
# Collateral Exposure — what stablecoins protocols accept from users
# =============================================================================

def _build_collateral_protocol(rows):
    """Build collateral exposure response for a single protocol's rows."""
    if not rows:
        return None

    slug = rows[0]["protocol_slug"]
    psi_row = fetch_one("""
        SELECT protocol_name, overall_score FROM psi_scores
        WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1
    """, (slug,))
    psi_name = psi_row["protocol_name"] if psi_row else slug
    psi_score = round(float(psi_row["overall_score"]), 1) if psi_row and psi_row.get("overall_score") else None

    total_stable_usd = sum(float(r["tvl_usd"]) for r in rows)
    exposure_list = []
    unscored_usd = 0.0

    for r in sorted(rows, key=lambda x: x["tvl_usd"], reverse=True):
        tvl = float(r["tvl_usd"])
        sii = round(float(r["sii_score"]), 1) if r.get("sii_score") is not None else None
        entry = {
            "symbol": r["token_symbol"],
            "tvl_usd": round(tvl, 2),
            "sii_score": sii,
            "is_sii_scored": r["is_sii_scored"],
            "pct_of_total": round(tvl / total_stable_usd * 100, 2) if total_stable_usd else 0,
        }
        exposure_list.append(entry)
        if not r["is_sii_scored"]:
            unscored_usd += tvl

    # Weighted avg SII across scored holdings
    scored = [e for e in exposure_list if e["sii_score"] is not None]
    scored_usd = sum(e["tvl_usd"] for e in scored)
    weighted_sii = round(
        sum(e["tvl_usd"] * e["sii_score"] for e in scored) / scored_usd, 1
    ) if scored_usd > 0 else None

    return {
        "slug": slug,
        "name": psi_name,
        "psi_score": psi_score,
        "total_stablecoin_collateral_usd": round(total_stable_usd, 2),
        "stablecoin_exposure": exposure_list,
        "unscored_exposure_usd": round(unscored_usd, 2),
        "unscored_exposure_pct": round(unscored_usd / total_stable_usd * 100, 2) if total_stable_usd else 0,
        "weighted_avg_sii": weighted_sii,
    }


@app.get("/api/protocols/backlog")
async def protocol_backlog():
    """Protocol discovery backlog — candidates for PSI scoring."""
    try:
        rows = fetch_all("""
            SELECT slug, name, category, tvl_usd,
                   stablecoin_exposure_usd, unscored_stablecoin_exposure_usd,
                   unscored_stablecoins,
                   enrichment_status, components_available, components_total,
                   coverage_pct, first_seen_at
            FROM protocol_backlog
            ORDER BY stablecoin_exposure_usd DESC
        """)
    except Exception:
        return {"protocols": [], "summary": {}}

    protocols = []
    status_counts = {"discovered": 0, "enriching": 0, "ready": 0, "promoted": 0, "scored": 0}

    for r in rows:
        status = r.get("enrichment_status", "discovered")
        status_counts[status] = status_counts.get(status, 0) + 1
        protocols.append({
            "slug": r["slug"],
            "name": r.get("name"),
            "category": r.get("category"),
            "tvl_usd": round(float(r.get("tvl_usd") or 0), 2),
            "stablecoin_exposure_usd": round(float(r.get("stablecoin_exposure_usd") or 0), 2),
            "unscored_stablecoin_exposure_usd": round(float(r.get("unscored_stablecoin_exposure_usd") or 0), 2),
            "unscored_stablecoins": r.get("unscored_stablecoins") or [],
            "enrichment_status": status,
            "components_available": r.get("components_available") or 0,
            "coverage_pct": round(float(r.get("coverage_pct") or 0), 1),
            "first_seen_at": str(r["first_seen_at"]) if r.get("first_seen_at") else None,
        })

    from app.index_definitions.psi_v01 import TARGET_PROTOCOLS
    status_counts["scored"] += len(TARGET_PROTOCOLS)  # hardcoded protocols are always scored

    return {"protocols": protocols, "summary": status_counts}


@app.get("/api/protocols/collateral-exposure")
async def collateral_exposure():
    """All protocols' stablecoin collateral exposure with SII cross-reference."""
    rows = fetch_all("""
        SELECT protocol_slug, pool_id, token_symbol, chain, tvl_usd,
               is_stablecoin, is_sii_scored, sii_score, pool_type
        FROM protocol_collateral_exposure
        WHERE is_stablecoin = TRUE
          AND snapshot_date = (SELECT MAX(snapshot_date) FROM protocol_collateral_exposure)
        ORDER BY protocol_slug, tvl_usd DESC
    """)
    if not rows:
        return {"protocols": [], "totals": {}}

    # Group by protocol
    by_slug = {}
    for r in rows:
        by_slug.setdefault(r["protocol_slug"], []).append(r)

    protocols = []
    total_exposure = 0.0
    covered_usd = 0.0
    uncovered_usd = 0.0
    unscored_agg = {}  # symbol -> {total_usd, protocol_count}

    for slug, proto_rows in by_slug.items():
        proto = _build_collateral_protocol(proto_rows)
        if proto:
            protocols.append(proto)
            total_exposure += proto["total_stablecoin_collateral_usd"]
            covered_usd += proto["total_stablecoin_collateral_usd"] - proto["unscored_exposure_usd"]
            uncovered_usd += proto["unscored_exposure_usd"]
            for e in proto["stablecoin_exposure"]:
                if not e["is_sii_scored"]:
                    sym = e["symbol"]
                    if sym not in unscored_agg:
                        unscored_agg[sym] = {"symbol": sym, "total_exposure_usd": 0.0, "protocol_count": 0}
                    unscored_agg[sym]["total_exposure_usd"] += e["tvl_usd"]
                    unscored_agg[sym]["protocol_count"] += 1

    protocols.sort(key=lambda p: p["total_stablecoin_collateral_usd"], reverse=True)

    unscored_list = sorted(unscored_agg.values(), key=lambda x: x["total_exposure_usd"], reverse=True)
    for u in unscored_list:
        u["total_exposure_usd"] = round(u["total_exposure_usd"], 2)

    return {
        "protocols": protocols,
        "totals": {
            "total_stablecoin_exposure_usd": round(total_exposure, 2),
            "covered_by_sii_usd": round(covered_usd, 2),
            "uncovered_usd": round(uncovered_usd, 2),
            "coverage_pct": round(covered_usd / total_exposure * 100, 1) if total_exposure else 0,
            "unscored_stablecoins": unscored_list,
        },
    }


@app.get("/api/protocols/{slug}/collateral-exposure")
async def protocol_collateral_exposure(slug: str):
    """Single protocol's stablecoin collateral exposure."""
    rows = fetch_all("""
        SELECT protocol_slug, pool_id, token_symbol, chain, tvl_usd,
               is_stablecoin, is_sii_scored, sii_score, pool_type
        FROM protocol_collateral_exposure
        WHERE protocol_slug = %s AND is_stablecoin = TRUE
          AND snapshot_date = (
              SELECT MAX(snapshot_date) FROM protocol_collateral_exposure WHERE protocol_slug = %s
          )
        ORDER BY tvl_usd DESC
    """, (slug, slug))
    if not rows:
        raise HTTPException(status_code=404, detail=f"No collateral data for '{slug}'")
    return _build_collateral_protocol(rows)


@app.get("/api/stablecoins/{symbol}/collateral-exposure")
async def stablecoin_collateral_exposure(symbol: str):
    """Reverse lookup — which protocols accept this stablecoin as collateral?"""
    sym_upper = symbol.upper()
    rows = fetch_all("""
        SELECT ce.protocol_slug, ce.tvl_usd, ce.is_sii_scored, ce.sii_score,
               p.protocol_name, p.overall_score as psi_score
        FROM protocol_collateral_exposure ce
        LEFT JOIN LATERAL (
            SELECT protocol_name, overall_score
            FROM psi_scores
            WHERE protocol_slug = ce.protocol_slug
            ORDER BY computed_at DESC LIMIT 1
        ) p ON true
        WHERE UPPER(ce.token_symbol) = %s
          AND ce.is_stablecoin = TRUE
          AND ce.snapshot_date = (SELECT MAX(snapshot_date) FROM protocol_collateral_exposure)
        ORDER BY ce.tvl_usd DESC
    """, (sym_upper,))

    # Get SII score
    sii_row = fetch_one("""
        SELECT s.overall_score FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        WHERE UPPER(st.symbol) = %s
    """, (sym_upper,))
    sii_score = round(float(sii_row["overall_score"]), 1) if sii_row and sii_row.get("overall_score") else None
    is_sii_scored = sii_score is not None

    accepted_by = []
    total_exposure = 0.0
    for r in rows:
        tvl = float(r["tvl_usd"])
        accepted_by.append({
            "protocol": r["protocol_slug"],
            "protocol_name": r.get("protocol_name"),
            "psi_score": round(float(r["psi_score"]), 1) if r.get("psi_score") else None,
            "tvl_usd": round(tvl, 2),
        })
        total_exposure += tvl

    result = {
        "symbol": sym_upper,
        "is_sii_scored": is_sii_scored,
        "sii_score": sii_score,
        "accepted_by": accepted_by,
        "total_collateral_exposure_usd": round(total_exposure, 2),
    }
    if not is_sii_scored and accepted_by:
        result["risk_note"] = (
            f"This stablecoin is accepted as collateral by {len(accepted_by)} "
            f"protocol{'s' if len(accepted_by) != 1 else ''} but has no SII score."
        )
    return result


# =============================================================================
# Full Exposure — combined treasury + collateral for a single protocol
# =============================================================================

@app.get("/api/protocols/{slug}/full-exposure")
async def protocol_full_exposure(slug: str):
    """Combined treasury holdings + collateral exposure for one protocol."""
    # PSI score
    psi_row = fetch_one("""
        SELECT protocol_name, overall_score FROM psi_scores
        WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1
    """, (slug,))
    if not psi_row:
        raise HTTPException(status_code=404, detail=f"Protocol '{slug}' not found in PSI scores")
    psi_score = round(float(psi_row["overall_score"]), 1) if psi_row.get("overall_score") else None

    # Treasury holdings (from session 026)
    treasury_rows = fetch_all("""
        SELECT token_name, token_symbol, chain, usd_value, is_stablecoin, sii_score
        FROM protocol_treasury_holdings
        WHERE protocol_slug = %s AND is_stablecoin = TRUE
          AND snapshot_date = (
              SELECT MAX(snapshot_date) FROM protocol_treasury_holdings WHERE protocol_slug = %s
          )
        ORDER BY usd_value DESC
    """, (slug, slug))

    treasury_exposure = {}
    treasury_total = 0.0
    for r in treasury_rows:
        sym = r["token_symbol"]
        if sym not in treasury_exposure:
            treasury_exposure[sym] = {"symbol": sym, "usd_value": 0.0, "sii_score": None}
        treasury_exposure[sym]["usd_value"] += float(r["usd_value"])
        if r.get("sii_score") is not None:
            treasury_exposure[sym]["sii_score"] = round(float(r["sii_score"]), 1)
        treasury_total += float(r["usd_value"])

    # Collateral exposure (from this session)
    collateral_rows = fetch_all("""
        SELECT token_symbol, tvl_usd, is_sii_scored, sii_score
        FROM protocol_collateral_exposure
        WHERE protocol_slug = %s AND is_stablecoin = TRUE
          AND snapshot_date = (
              SELECT MAX(snapshot_date) FROM protocol_collateral_exposure WHERE protocol_slug = %s
          )
        ORDER BY tvl_usd DESC
    """, (slug, slug))

    collateral_exposure = {}
    collateral_total = 0.0
    for r in collateral_rows:
        sym = r["token_symbol"]
        tvl = float(r["tvl_usd"])
        collateral_exposure[sym] = {
            "symbol": sym,
            "tvl_usd": round(tvl, 2),
            "is_sii_scored": r["is_sii_scored"],
            "sii_score": round(float(r["sii_score"]), 1) if r.get("sii_score") is not None else None,
        }
        collateral_total += tvl

    # Combined unscored stablecoins
    all_syms = set(treasury_exposure.keys()) | set(collateral_exposure.keys())
    unscored = sorted(
        sym for sym in all_syms
        if (treasury_exposure.get(sym, {}).get("sii_score") is None
            and collateral_exposure.get(sym, {}).get("sii_score") is None)
    )

    # Risk summary
    combined_total = treasury_total + collateral_total
    scored_total = 0.0
    lowest_sii = None
    largest_unscored = None

    for sym in all_syms:
        t_usd = treasury_exposure.get(sym, {}).get("usd_value", 0)
        c_usd = collateral_exposure.get(sym, {}).get("tvl_usd", 0)
        sii = (treasury_exposure.get(sym, {}).get("sii_score")
               or collateral_exposure.get(sym, {}).get("sii_score"))
        if sii is not None:
            scored_total += t_usd + c_usd
            if lowest_sii is None or sii < lowest_sii.get("sii_score", 999):
                lowest_sii = {"symbol": sym, "sii_score": sii, "tvl_usd": round(c_usd, 2)}
        else:
            total_sym = t_usd + c_usd
            if largest_unscored is None or total_sym > largest_unscored.get("tvl_usd", 0):
                largest_unscored = {"symbol": sym, "tvl_usd": round(total_sym, 2)}

    return {
        "slug": slug,
        "name": psi_row["protocol_name"],
        "psi_score": psi_score,
        "treasury_stablecoin_exposure": sorted(treasury_exposure.values(), key=lambda x: x["usd_value"], reverse=True),
        "collateral_stablecoin_exposure": sorted(collateral_exposure.values(), key=lambda x: x["tvl_usd"], reverse=True),
        "combined_unscored_stablecoins": unscored,
        "risk_summary": {
            "total_stablecoin_exposure_usd": round(combined_total, 2),
            "sii_coverage_pct": round(scored_total / combined_total * 100, 1) if combined_total else 0,
            "lowest_sii_in_collateral": lowest_sii,
            "largest_unscored": largest_unscored,
        },
    }


@app.get("/api/protocols/drift/vault-balances")
async def drift_vault_balances():
    """On-chain Drift vault token balances via Helius RPC. Real-time, not DeFiLlama."""
    from app.collectors.solana import get_drift_vault_balances, DRIFT_PROGRAM_ID
    import httpx as _httpx

    async with _httpx.AsyncClient() as client:
        balances = await get_drift_vault_balances(client)

    total_stablecoin = sum(b["usd_value_approx"] or 0 for b in balances if b["is_stablecoin"])

    return {
        "protocol": "drift",
        "source": "on-chain (Helius RPC)",
        "program_id": DRIFT_PROGRAM_ID,
        "balances": balances,
        "summary": {
            "total_token_accounts": len(balances),
            "stablecoin_accounts": sum(1 for b in balances if b["is_stablecoin"]),
            "total_stablecoin_usd_approx": round(total_stablecoin, 2),
        },
        "note": "Post-exploit snapshot. Compare against DeFiLlama collateral exposure for discrepancy.",
    }


@app.get("/api/indices")
async def list_indices():
    """List all available index definitions."""
    from app.index_definitions.sii_v1 import SII_V1_DEFINITION
    from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
    from app.index_definitions.lsti_v01 import LSTI_V01_DEFINITION
    from app.index_definitions.bri_v01 import BRI_V01_DEFINITION
    from app.index_definitions.dohi_v01 import DOHI_V01_DEFINITION
    from app.index_definitions.vsri_v01 import VSRI_V01_DEFINITION
    from app.index_definitions.cxri_v01 import CXRI_V01_DEFINITION
    from app.index_definitions.tti_v01 import TTI_V01_DEFINITION

    def _idx_entry(defn, status):
        return {
            "id": defn["index_id"],
            "version": defn["version"],
            "name": defn["name"],
            "entity_type": defn["entity_type"],
            "components": len(defn["components"]),
            "status": status,
        }

    return {
        "indices": [
            _idx_entry(SII_V1_DEFINITION, "live"),
            _idx_entry(PSI_V01_DEFINITION, "live"),
            _idx_entry(LSTI_V01_DEFINITION, "accruing"),
            _idx_entry(BRI_V01_DEFINITION, "accruing"),
            _idx_entry(DOHI_V01_DEFINITION, "accruing"),
            _idx_entry(VSRI_V01_DEFINITION, "accruing"),
            _idx_entry(CXRI_V01_DEFINITION, "accruing"),
            _idx_entry(TTI_V01_DEFINITION, "accruing"),
        ]
    }


# =============================================================================
# Circle 7 Index API Endpoints (silent accumulation — internal only)
# =============================================================================

@app.get("/api/{index_id}/scores")
async def circle7_scores(index_id: str):
    """Latest scores for a Circle 7 index (lsti, bri, dohi, vsri, cxri, tti)."""
    valid_indices = {"lsti", "bri", "dohi", "vsri", "cxri", "tti"}
    if index_id not in valid_indices:
        raise HTTPException(status_code=404, detail=f"Unknown index: {index_id}")

    rows = fetch_all("""
        SELECT DISTINCT ON (entity_slug)
            entity_slug, entity_name, overall_score,
            category_scores, confidence, confidence_tag,
            scored_date, computed_at
        FROM generic_index_scores
        WHERE index_id = %s
        ORDER BY entity_slug, computed_at DESC
    """, (index_id,))

    return {
        "index_id": index_id,
        "count": len(rows),
        "scores": [
            {
                "entity": r["entity_slug"],
                "name": r["entity_name"],
                "score": float(r["overall_score"]) if r["overall_score"] else None,
                "category_scores": r["category_scores"],
                "confidence": r["confidence"],
                "confidence_tag": r["confidence_tag"],
                "scored_date": str(r["scored_date"]),
            }
            for r in rows
        ],
    }


@app.get("/api/{index_id}/scores/{entity_slug}")
async def circle7_score_detail(index_id: str, entity_slug: str):
    """Detailed score breakdown for a specific entity in a Circle 7 index."""
    valid_indices = {"lsti", "bri", "dohi", "vsri", "cxri", "tti"}
    if index_id not in valid_indices:
        raise HTTPException(status_code=404, detail=f"Unknown index: {index_id}")

    row = fetch_one("""
        SELECT entity_slug, entity_name, overall_score,
               category_scores, component_scores, raw_values,
               formula_version, inputs_hash, confidence, confidence_tag,
               scored_date, computed_at
        FROM generic_index_scores
        WHERE index_id = %s AND entity_slug = %s
        ORDER BY computed_at DESC LIMIT 1
    """, (index_id, entity_slug))

    if not row:
        raise HTTPException(status_code=404, detail=f"No score found for {entity_slug} in {index_id}")

    return {
        "index_id": index_id,
        "entity": row["entity_slug"],
        "name": row["entity_name"],
        "score": float(row["overall_score"]) if row["overall_score"] else None,
        "category_scores": row["category_scores"],
        "component_scores": row["component_scores"],
        "raw_values": row["raw_values"],
        "formula_version": row["formula_version"],
        "inputs_hash": row["inputs_hash"],
        "confidence": row["confidence"],
        "confidence_tag": row["confidence_tag"],
        "scored_date": str(row["scored_date"]),
        "computed_at": str(row["computed_at"]),
    }


@app.get("/api/{index_id}/definition")
async def circle7_definition(index_id: str):
    """Return the full index definition for a Circle 7 index."""
    definitions = {}
    try:
        from app.index_definitions.lsti_v01 import LSTI_V01_DEFINITION
        from app.index_definitions.bri_v01 import BRI_V01_DEFINITION
        from app.index_definitions.dohi_v01 import DOHI_V01_DEFINITION
        from app.index_definitions.vsri_v01 import VSRI_V01_DEFINITION
        from app.index_definitions.cxri_v01 import CXRI_V01_DEFINITION
        from app.index_definitions.tti_v01 import TTI_V01_DEFINITION
        definitions = {
            "lsti": LSTI_V01_DEFINITION,
            "bri": BRI_V01_DEFINITION,
            "dohi": DOHI_V01_DEFINITION,
            "vsri": VSRI_V01_DEFINITION,
            "cxri": CXRI_V01_DEFINITION,
            "tti": TTI_V01_DEFINITION,
        }
    except ImportError:
        pass

    defn = definitions.get(index_id)
    if not defn:
        raise HTTPException(status_code=404, detail=f"Unknown index: {index_id}")

    return {
        "definition": defn,
        "component_count": len(defn.get("components", {})),
        "category_count": len(defn.get("categories", {})),
    }


# =============================================================================
# Attribution API (Governance Events / RPI Delta)
# =============================================================================

@app.get("/api/attribution")
async def attribution_query(
    protocol: str = None,
    contributor: str = None,
    period: int = 90,
):
    """
    Attribution query endpoint.
    - ?protocol={slug}&period={days} — PSI trajectory + governance events overlay
    - ?contributor={tag} — cross-protocol contributor impact analysis
    """
    if not protocol and not contributor:
        raise HTTPException(status_code=400, detail="Provide either protocol or contributor parameter")

    if protocol:
        from app.collectors.governance_events import get_attribution_by_protocol
        return get_attribution_by_protocol(protocol, period)
    else:
        from app.collectors.governance_events import get_attribution_by_contributor
        return get_attribution_by_contributor(contributor)


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


@app.get("/api/query/templates")
async def query_templates_list():
    """List available query templates with descriptions and default params."""
    from app.query_templates import list_templates
    return {"templates": list_templates()}


@app.post("/api/query/template")
async def query_template_execute(request: Request):
    """Execute a named query template with optional parameter overrides."""
    from app.query_templates import execute_template

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    template_name = body.get("template")
    if not template_name:
        raise HTTPException(status_code=400, detail="Missing 'template' field")

    params = body.get("params", {})
    if not isinstance(params, dict):
        raise HTTPException(status_code=400, detail="'params' must be a JSON object")

    result = execute_template(template_name, params)
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
    if not admin_key or not provided or not hmac.compare_digest(provided, admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized — provide ?key=YOUR_ADMIN_KEY")
    try:
        return HTMLResponse(content=ADMIN_HTML)
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


# =============================================================================
# Admin: usage stats + API key management
# =============================================================================

@app.get("/api/admin/usage")
async def admin_usage(request: Request, days: int = 7):
    _check_admin_key(request)
    try:
        from app.usage_tracker import get_usage_stats
        return get_usage_stats(days=days)
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.post("/api/admin/apikeys")
async def admin_create_key(request: Request, name: str = Query(...)):
    _check_admin_key(request)
    try:
        from app.usage_tracker import create_api_key
        key = create_api_key(name)
        return {
            "api_key": key,
            "name": name,
            "message": "Store this key — it will not be shown again.",
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.get("/api/admin/apikeys")
async def admin_list_keys(request: Request):
    _check_admin_key(request)
    try:
        from app.usage_tracker import list_api_keys
        return {"keys": list_api_keys()}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


# =============================================================================
# Admin: API Budget Allocator
# =============================================================================

@app.get("/api/admin/budget")
async def admin_budget_status(request: Request):
    """Returns today's API budget allocation and usage."""
    _check_admin_key(request)
    try:
        from app.budget.manager import ApiBudgetManager
        budget = ApiBudgetManager()
        return budget.get_status()
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.post("/api/admin/run-daily-cycle")
async def trigger_daily_cycle(request: Request):
    """Trigger the full daily scoring + indexing cycle in background."""
    _check_admin_key(request)
    try:
        import asyncio
        from app.budget.daily_cycle import run_daily_cycle
        asyncio.create_task(run_daily_cycle())
        return {"status": "started", "message": "Daily cycle triggered in background"}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


@app.get("/api/ops/reports/recent")
async def ops_recent_reports(request: Request):
    _check_admin_key(request)
    rows = fetch_all("""
        SELECT entity_type, entity_id, template, lens, lens_version,
               report_hash, methodology_version, generated_at
        FROM report_attestations
        ORDER BY generated_at DESC
        LIMIT 20
    """)
    return {
        "reports": [
            {
                "entity_type": r["entity_type"],
                "entity_id": r["entity_id"],
                "template": r["template"],
                "lens": r.get("lens"),
                "lens_version": r.get("lens_version"),
                "report_hash": r["report_hash"],
                "methodology_version": r.get("methodology_version"),
                "generated_at": r["generated_at"].isoformat() if r.get("generated_at") else None,
            }
            for r in (rows or [])
        ]
    }


@app.post("/api/admin/keeper-log")
async def log_keeper_publish(request: Request):
    """Log a keeper publish event. Called by the keeper after each on-chain update."""
    _check_admin_key(request)
    body = await request.json()
    try:
        from app.database import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO keeper_publish_log (chain, scores_published, gas_used, tx_hash, success, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    body.get("chain", "unknown"),
                    body.get("scores_published", 0),
                    body.get("gas_used"),
                    body.get("tx_hash"),
                    body.get("success", True),
                    body.get("error_message"),
                ))
            conn.commit()
        return {"status": "logged"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# =============================================================================
# SPA Catch-All + SSR helpers for bot / AI visibility
# =============================================================================

CANONICAL_BASE_URL = os.environ.get("CANONICAL_BASE_URL", "https://basisprotocol.xyz").rstrip("/")

class _DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

BOT_UA_PATTERN = re.compile(
    r"(bot|crawl|spider|slurp|Googlebot|Bingbot|DuckDuckBot|Baiduspider|YandexBot"
    r"|facebookexternalhit|Twitterbot|LinkedInBot|Discordbot"
    r"|ChatGPT|GPTBot|Claude|ClaudeBot|Anthropic|PerplexityBot|Perplexity"
    r"|Cohere|cohere-ai|Google-Extended|CCBot|amazonbot"
    r"|python-requests|httpx|axios|curl|wget|fetch|Go-http-client"
    r"|Applebot|ia_archiver|archive\.org_bot)",
    re.IGNORECASE
)

def _is_bot(request) -> bool:
    ua = request.headers.get("user-agent", "")
    if BOT_UA_PATTERN.search(ua):
        return True
    if not ua or len(ua) < 10:
        return True
    accept = request.headers.get("accept", "")
    if "application/json" in accept and "text/html" not in accept:
        return True
    return False

def _render_rankings_html() -> str:
    """Server-rendered rankings page for bots and AI assistants."""
    rows = fetch_all("""
        SELECT s.*, st.name, st.symbol, st.issuer
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        ORDER BY s.overall_score DESC
    """)

    json_ld = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": "Basis Protocol — Stablecoin Integrity Index",
        "description": "Standardized risk scores for on-chain stablecoins. SII scores 37 components across 7 categories.",
        "url": f"{CANONICAL_BASE_URL}/",
        "dateModified": datetime.now(timezone.utc).isoformat(),
        "creator": {"@type": "Organization", "name": "Basis Protocol", "url": CANONICAL_BASE_URL},
        "hasPart": []
    }

    score_rows_html = ""
    for row in rows:
        symbol = (row.get("symbol") or row.get("stablecoin_id", "")).upper()
        name = row.get("name") or row.get("issuer") or symbol
        issuer = row.get("issuer") or ""
        score = row.get("overall_score", 0)
        price = row.get("current_price")
        price_str = f"${float(price):.4f}" if price else "—"
        peg = row.get("peg_score") or "—"
        liq = row.get("liquidity_score") or "—"
        flow = row.get("mint_burn_score") or "—"
        dist = row.get("distribution_score") or "—"
        struct = row.get("structural_score") or "—"

        score_rows_html += f"""
        <tr>
            <td><strong>{symbol}</strong><br><span class="sub">{issuer}</span></td>
            <td class="num">{float(score):.1f}</td>
            <td class="num">{price_str}</td>
            <td class="num">{peg}</td>
            <td class="num">{liq}</td>
            <td class="num">{flow}</td>
            <td class="num">{dist}</td>
            <td class="num">{struct}</td>
        </tr>"""

        json_ld["hasPart"].append({
            "@type": "FinancialProduct",
            "name": f"{symbol} Stablecoin Integrity Index",
            "additionalProperty": [
                {"@type": "PropertyValue", "name": "sii_score", "value": float(score)},
            ]
        })

    json_ld_str = json.dumps(json_ld, cls=_DecimalEncoder)
    count = len(rows)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Basis Protocol — Stablecoin Integrity Index</title>
    <meta name="description" content="Standardized risk scores for {count} on-chain stablecoins. Updated hourly. 37 scoring components across 7 categories, 5 live data sources.">
    <meta property="og:title" content="Basis Protocol — Stablecoin Integrity Index">
    <meta property="og:description" content="Live SII scores for {count} stablecoins. Deterministic methodology. Updated hourly.">
    <meta property="og:type" content="website">
    <meta property="og:url" content="{CANONICAL_BASE_URL}/">
    <link rel="canonical" href="{CANONICAL_BASE_URL}/">
    <link rel="alternate" type="application/json" href="{CANONICAL_BASE_URL}/api/scores">
    <script type="application/ld+json">{json_ld_str}</script>
    <style>
        body {{ font-family: 'Georgia', serif; max-width: 960px; margin: 0 auto; padding: 24px; background: #F3F2ED; color: #0B090A; }}
        h1 {{ font-size: 1.6rem; font-weight: 400; margin-bottom: 4px; }}
        .meta {{ font-family: monospace; font-size: 0.75rem; color: #6a6a6a; margin-bottom: 24px; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
        th {{ text-align: left; padding: 8px; border-bottom: 2px solid #0B090A; font-family: monospace; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 1px; color: #6a6a6a; }}
        td {{ padding: 8px; border-bottom: 1px dotted #ccc; }}
        .num {{ font-family: monospace; text-align: right; }}
        .sub {{ font-size: 0.75rem; color: #6a6a6a; }}
        nav {{ margin-bottom: 24px; font-family: monospace; font-size: 0.8rem; }}
        nav a {{ color: #0B090A; margin-right: 16px; text-decoration: none; }}
        footer {{ margin-top: 32px; font-family: monospace; font-size: 0.75rem; color: #6a6a6a; border-top: 1px solid #ccc; padding-top: 12px; }}
    </style>
</head>
<body>
    <h1>Basis Protocol</h1>
    <p class="meta">Stablecoin Integrity Index · {count} stablecoins · Updated hourly · {ts}</p>
    <nav>
        <a href="/">Rankings</a>
        <a href="/witness">Witness</a>
        <a href="/proof/sii/usdc">Proof</a>
        <a href="/developers">API</a>
        <a href="/terms">Terms</a>
    </nav>
    <table>
        <thead>
            <tr>
                <th>Stablecoin</th>
                <th class="num">SII</th>
                <th class="num">Price</th>
                <th class="num">Peg</th>
                <th class="num">Liq</th>
                <th class="num">Flow</th>
                <th class="num">Dist</th>
                <th class="num">Struct</th>
            </tr>
        </thead>
        <tbody>
            {score_rows_html}
        </tbody>
    </table>
    <footer>
        <p>Basis Protocol · basisprotocol.xyz · SII v1.0.0 · Methodology: deterministic, version-controlled, open</p>
        <p>API: <a href="/api/scores">/api/scores</a> · <a href="/api/cda/issuers">/api/cda/issuers</a> · <a href="/developers">Developer docs</a> · <a href="/terms">Terms</a></p>
    </footer>
</body>
</html>"""

def _render_witness_html() -> str:
    """Server-rendered witness page for bots and AI assistants."""
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
        ORDER BY last_attestation DESC NULLS LAST, r.asset_symbol
    """)

    total_attestations = fetch_one("SELECT COUNT(*) as cnt FROM cda_vendor_extractions")
    att_count = total_attestations["cnt"] if total_attestations else 0

    json_ld = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": "Basis Witness — Stablecoin Issuer Disclosure Archive",
        "description": f"Structured, timestamped, hash-verified archive of stablecoin issuer disclosures. {len(rows)} issuers tracked. {att_count} attestations archived.",
        "url": f"{CANONICAL_BASE_URL}/witness",
        "dateModified": datetime.now(timezone.utc).isoformat(),
        "creator": {"@type": "Organization", "name": "Basis Protocol", "url": CANONICAL_BASE_URL},
    }
    json_ld_str = json.dumps(json_ld, cls=_DecimalEncoder)

    issuer_rows_html = ""
    for r in rows:
        symbol = r.get("asset_symbol", "")
        issuer = r.get("issuer_name", "")
        method = r.get("collection_method", "")
        method_display = "on-chain" if method == "nav_oracle" else method
        last = r["last_attestation"].strftime("%Y-%m-%d %H:%M UTC") if r.get("last_attestation") else "—"

        issuer_rows_html += f"""
        <tr>
            <td><strong>{issuer}</strong> <span class="sub">{symbol}</span></td>
            <td class="num">{last}</td>
            <td>{method_display}</td>
        </tr>"""

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Basis Witness — Stablecoin Disclosure Archive | Basis Protocol</title>
    <meta name="description" content="Structured, timestamped, hash-verified archive of stablecoin issuer disclosures. {len(rows)} issuers tracked. {att_count} attestations archived.">
    <meta property="og:title" content="Basis Witness — Stablecoin Disclosure Archive">
    <meta property="og:description" content="{len(rows)} issuers tracked. {att_count} attestations archived. Updated daily.">
    <meta property="og:type" content="website">
    <link rel="canonical" href="{CANONICAL_BASE_URL}/witness">
    <link rel="alternate" type="application/json" href="{CANONICAL_BASE_URL}/api/cda/issuers">
    <script type="application/ld+json">{json_ld_str}</script>
    <style>
        body {{ font-family: 'Georgia', serif; max-width: 960px; margin: 0 auto; padding: 24px; background: #F3F2ED; color: #0B090A; }}
        h1 {{ font-size: 1.6rem; font-weight: 400; margin-bottom: 4px; }}
        .meta {{ font-family: monospace; font-size: 0.75rem; color: #6a6a6a; margin-bottom: 24px; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
        th {{ text-align: left; padding: 8px; border-bottom: 2px solid #0B090A; font-family: monospace; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 1px; color: #6a6a6a; }}
        td {{ padding: 8px; border-bottom: 1px dotted #ccc; }}
        .num {{ font-family: monospace; }}
        .sub {{ font-size: 0.75rem; color: #6a6a6a; }}
        nav {{ margin-bottom: 24px; font-family: monospace; font-size: 0.8rem; }}
        nav a {{ color: #0B090A; margin-right: 16px; text-decoration: none; }}
        footer {{ margin-top: 32px; font-family: monospace; font-size: 0.75rem; color: #6a6a6a; border-top: 1px solid #ccc; padding-top: 12px; }}
    </style>
</head>
<body>
    <h1>Basis Witness</h1>
    <p class="meta">Stablecoin Issuer Disclosure Archive · {len(rows)} issuers · {att_count} attestations · Updated daily · {ts}</p>
    <nav>
        <a href="/">Rankings</a>
        <a href="/witness">Witness</a>
        <a href="/proof/sii/usdc">Proof</a>
        <a href="/developers">API</a>
        <a href="/terms">Terms</a>
    </nav>
    <p>Structured, timestamped, hash-verified archive of stablecoin issuer disclosures. Basis does not modify source documents.</p>
    <table>
        <thead>
            <tr>
                <th>Issuer</th>
                <th class="num">Last Attestation</th>
                <th>Method</th>
            </tr>
        </thead>
        <tbody>
            {issuer_rows_html}
        </tbody>
    </table>
    <footer>
        <p>Basis Protocol · basisprotocol.xyz · Witness is the disclosure primitive in the Basis Protocol stack.</p>
        <p>API: <a href="/api/cda/issuers">/api/cda/issuers</a> · <a href="/api/cda/coverage">/api/cda/coverage</a> · <a href="/developers">Developer docs</a> · <a href="/terms">Terms</a></p>
    </footer>
</body>
</html>"""

def _render_proof_html(identifier: str, surface: str) -> str:
    """Server-rendered score proof page — shows every input, weight, and component."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if surface == "sii":
        row = fetch_one("""
            SELECT s.*, st.name, st.symbol, st.issuer
            FROM scores s JOIN stablecoins st ON st.id = s.stablecoin_id
            WHERE s.stablecoin_id = %s
        """, (identifier,))
        if not row:
            return f"<html><body><h1>Not found</h1><p>No SII score for '{identifier}'</p></body></html>"

        symbol = (row.get("symbol") or identifier).upper()
        name = row.get("name") or symbol
        issuer = row.get("issuer") or ""
        score = float(row.get("overall_score", 0))
        computed = row["computed_at"].strftime("%Y-%m-%d %H:%M UTC") if row.get("computed_at") else "—"
        formula_version = row.get("formula_version") or FORMULA_VERSION
        comp_count = row.get("component_count") or 0

        cats = [
            ("Peg Stability", "peg_stability", 0.30, row.get("peg_score")),
            ("Liquidity Depth", "liquidity_depth", 0.25, row.get("liquidity_score")),
            ("Mint/Burn Flows", "mint_burn_dynamics", 0.15, row.get("mint_burn_score")),
            ("Holder Distribution", "holder_distribution", 0.10, row.get("distribution_score")),
            ("Structural Risk", "structural_risk_composite", 0.20, row.get("structural_score")),
        ]

        components = fetch_all("""
            SELECT DISTINCT ON (component_id)
              component_id, category, raw_value, normalized_score, data_source, collected_at
            FROM component_readings
            WHERE stablecoin_id = %s AND collected_at > NOW() - INTERVAL '48 hours'
            ORDER BY component_id, collected_at DESC
        """, (identifier,))

        title = f"{symbol} — Score Proof"
        desc = f"Full score derivation for {symbol}. SII {score:.1f}. {comp_count} components."
        canon = f"{CANONICAL_BASE_URL}/proof/sii/{identifier}"
        json_api = f"/api/scores/{identifier}"
        history_api = f"/api/scores/{identifier}/history"
        formula_str = "SII = 0.30×Peg + 0.25×Liquidity + 0.15×Flows + 0.10×Distribution + 0.20×Structural"

    elif surface == "psi":
        row = fetch_one("""
            SELECT * FROM psi_scores
            WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1
        """, (identifier,))
        if not row:
            return f"<html><body><h1>Not found</h1><p>No PSI score for '{identifier}'</p></body></html>"

        from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
        symbol = identifier
        name = row.get("protocol_name") or identifier
        issuer = ""
        score = float(row.get("overall_score", 0))
        computed = row["computed_at"].strftime("%Y-%m-%d %H:%M UTC") if row.get("computed_at") else "—"
        formula_version = row.get("formula_version") or "psi-v0.2.0"
        cat_scores = row.get("category_scores") or {}
        comp_scores = row.get("component_scores") or {}
        raw_values = row.get("raw_values") or {}
        comp_count = len(comp_scores)

        psi_cats = PSI_V01_DEFINITION["categories"]
        cats = []
        for cat_id, cat_def in psi_cats.items():
            w = cat_def["weight"] if isinstance(cat_def, dict) else 0
            s = cat_scores.get(cat_id)
            cats.append((cat_def["name"] if isinstance(cat_def, dict) else cat_id, cat_id, w, s))

        # Build components from PSI definition + stored data
        components = []
        for comp_id, comp_def in PSI_V01_DEFINITION["components"].items():
            components.append({
                "component_id": comp_id,
                "category": comp_def.get("category", ""),
                "raw_value": raw_values.get(comp_id),
                "normalized_score": comp_scores.get(comp_id),
                "data_source": comp_def.get("data_source", ""),
                "collected_at": row.get("computed_at"),
            })

        title = f"{name} — Score Proof"
        desc = f"Full score derivation for {name}. PSI {score:.1f}. {comp_count} components."
        canon = f"{CANONICAL_BASE_URL}/proof/psi/{identifier}"
        json_api = f"/api/psi/scores/{identifier}"
        history_api = None
        formula_str = "PSI = 0.25×Balance + 0.20×Revenue + 0.20×Liquidity + 0.15×Security + 0.10×Governance + 0.10×Token"
    else:
        return "<html><body><h1>Not found</h1></body></html>"

    # Build formula table rows
    formula_rows = ""
    running_total = 0.0
    for cat_name, cat_id, weight, cat_score in cats:
        cs = float(cat_score) if cat_score else 0
        contribution = weight * cs
        running_total += contribution
        bar_w = max(0, min(100, cs))
        score_str = f"{cs:.1f}" if cat_score else "—"
        contrib_str = f"{contribution:.2f}" if cat_score else "—"
        formula_rows += f"""
        <tr>
            <td>{cat_name}</td>
            <td class="num">{weight:.2f}</td>
            <td class="num">{score_str}</td>
            <td class="num">{contrib_str}</td>
            <td><div class="bar" style="width:{bar_w}%"></div></td>
        </tr>"""

    formula_rows += f"""
        <tr style="border-top:2px solid #0B090A">
            <td><strong>Total</strong></td>
            <td></td>
            <td class="num"><strong>{score:.1f}</strong></td>
            <td class="num"><strong>{running_total:.2f}</strong></td>
            <td></td>
        </tr>"""

    # Build component readings grouped by category
    by_cat = {}
    for c in components:
        cat = c.get("category") or "other"
        by_cat.setdefault(cat, []).append(c)

    comp_rows = ""
    for cat in sorted(by_cat.keys()):
        comp_rows += f"""
        <tr class="cat-header"><td colspan="6">{cat.replace("_", " ").title()}</td></tr>"""
        for c in sorted(by_cat[cat], key=lambda x: x["component_id"]):
            raw = c.get("raw_value")
            raw_str = f"{float(raw):.4g}" if raw is not None else "—"
            norm = c.get("normalized_score")
            norm_str = f"{float(norm):.1f}" if norm is not None else "—"
            src = c.get("data_source") or "—"
            src_type = (
                "cda" if str(src).startswith("cda_") else
                "live" if src in ("coingecko", "etherscan", "curve", "defillama") else
                "static"
            )
            src_class = f"src-{src_type}"
            ts_str = c["collected_at"].strftime("%Y-%m-%d %H:%M") if c.get("collected_at") else "—"
            comp_rows += f"""
        <tr>
            <td class="comp-id">{c["component_id"]}</td>
            <td class="num">{raw_str}</td>
            <td class="num">{norm_str}</td>
            <td class="{src_class}">{src}</td>
            <td class="{src_class}">{src_type}</td>
            <td class="ts">{ts_str}</td>
        </tr>"""

    total_target = len(COMPONENT_NORMALIZATIONS) if surface == "sii" else len(PSI_V01_DEFINITION["components"]) if surface == "psi" else 0

    # Evidence links
    evidence = f'<a href="{json_api}">Raw JSON</a>'
    if history_api:
        evidence += f' · <a href="{history_api}">Score History</a>'
    evidence += ' · <a href="/witness">Witness Archive</a> · <a href="/api/methodology">Methodology Spec</a>'

    # Compute inputs hash if available
    hash_section = ""
    try:
        import hashlib
        if components:
            inputs = {c["component_id"]: c.get("raw_value") for c in components if c.get("raw_value") is not None}
            if inputs:
                canonical = json.dumps(dict(sorted(inputs.items())), sort_keys=True, default=str)
                h = hashlib.sha256(canonical.encode()).hexdigest()[:32]
                hash_section = f"""
    <div class="section">
        <h3>Re-derivation</h3>
        <p>Inputs hash: <code>{h}</code></p>
        <p>You can verify this score independently by calling <code>GET {json_api}</code> and applying the published formula to the component readings. The same inputs always produce the same output.</p>
    </div>"""
    except Exception:
        pass

    json_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": f"{title} — Basis Protocol",
        "description": desc,
        "url": canon,
        "dateModified": datetime.now(timezone.utc).isoformat(),
        "creator": {"@type": "Organization", "name": "Basis Protocol", "url": CANONICAL_BASE_URL},
    }, cls=_DecimalEncoder)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} — Basis Protocol</title>
    <meta name="description" content="{desc}">
    <meta property="og:title" content="{title} — Basis Protocol">
    <meta property="og:description" content="{desc}">
    <meta property="og:type" content="website">
    <meta property="og:url" content="{canon}">
    <link rel="canonical" href="{canon}">
    <link rel="alternate" type="application/json" href="{CANONICAL_BASE_URL}{json_api}">
    <script type="application/ld+json">{json_ld}</script>
    <style>
        body {{ font-family: 'Georgia', serif; max-width: 960px; margin: 0 auto; padding: 24px; background: #F3F2ED; color: #0B090A; }}
        h1 {{ font-size: 1.6rem; font-weight: 400; margin-bottom: 4px; }}
        h3 {{ font-family: monospace; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1.5px; color: #6a6a6a; margin: 0 0 12px; }}
        .meta {{ font-family: monospace; font-size: 0.75rem; color: #6a6a6a; margin-bottom: 6px; }}
        .intro {{ font-size: 0.9rem; color: #3a3a3a; line-height: 1.6; margin-bottom: 24px; }}
        nav {{ margin-bottom: 24px; font-family: monospace; font-size: 0.8rem; }}
        nav a {{ color: #0B090A; margin-right: 16px; text-decoration: none; }}
        .section {{ border: 1px solid #ccc; padding: 16px 20px; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.8rem; }}
        th {{ text-align: left; padding: 6px 8px; border-bottom: 2px solid #0B090A; font-family: monospace; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 1px; color: #6a6a6a; }}
        td {{ padding: 6px 8px; border-bottom: 1px dotted #ccc; }}
        .num {{ font-family: monospace; text-align: right; }}
        .comp-id {{ font-family: monospace; font-size: 0.75rem; }}
        .ts {{ font-family: monospace; font-size: 0.7rem; color: #9a9a9a; }}
        .cat-header td {{ font-family: monospace; font-size: 0.7rem; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; color: #6a6a6a; padding-top: 14px; border-bottom: 1px solid #0B090A; }}
        .src-live {{ color: #2d6b45; }}
        .src-cda {{ color: #6b5b2d; }}
        .src-static {{ color: #9a9a9a; }}
        .bar {{ height: 4px; background: #0B090A; border-radius: 1px; }}
        .score {{ font-family: monospace; font-size: 1.8rem; font-weight: 700; }}
        code {{ font-family: monospace; font-size: 0.8rem; background: #e8e6e0; padding: 1px 4px; border-radius: 2px; }}
        footer {{ margin-top: 32px; font-family: monospace; font-size: 0.75rem; color: #6a6a6a; border-top: 1px solid #ccc; padding-top: 12px; }}
    </style>
</head>
<body>
    <h1>Basis Protocol</h1>
    <p class="meta">Score Proof · {symbol.upper() if surface == "sii" else name} · {surface.upper()} · {ts}</p>
    <nav>
        <a href="/">Rankings</a>
        <a href="/witness">Witness</a>
        <a href="/proof/sii/usdc">Proof</a>
        <a href="/developers">API</a>
        <a href="/terms">Terms</a>
    </nav>

    <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:8px">
        <div>
            <span style="font-size:1.4rem;font-weight:600">{name}</span>
            <span style="font-family:monospace;color:#6a6a6a;margin-left:8px">{symbol.upper() if surface == "sii" else ""}</span>
            {"<br><span class='meta'>Issued by " + issuer + "</span>" if issuer else ""}
        </div>
        <div style="text-align:right">
            <span class="score">{score:.1f}</span>
        </div>
    </div>
    <p class="meta">Methodology {formula_version} · Computed {computed} · {comp_count} components</p>
    <p class="intro">This page shows every input used to compute this score. The methodology is open. The computation is deterministic — same inputs always produce the same output.</p>

    <div class="section">
        <h3>Formula</h3>
        <p style="font-family:monospace;font-size:0.85rem;margin:0 0 14px;color:#3a3a3a">{formula_str}</p>
        <table>
            <thead>
                <tr>
                    <th>Category</th>
                    <th class="num">Weight</th>
                    <th class="num">Score</th>
                    <th class="num">Contribution</th>
                    <th style="width:30%"></th>
                </tr>
            </thead>
            <tbody>
                {formula_rows}
            </tbody>
        </table>
    </div>

    <div class="section">
        <h3>Component Readings</h3>
        <p class="meta" style="margin-bottom:12px">{comp_count} of {total_target} target components currently scoring</p>
        <table>
            <thead>
                <tr>
                    <th>Component</th>
                    <th class="num">Raw Value</th>
                    <th class="num">Score (0-100)</th>
                    <th>Source</th>
                    <th>Type</th>
                    <th>Collected</th>
                </tr>
            </thead>
            <tbody>
                {comp_rows}
            </tbody>
        </table>
    </div>

    <div class="section">
        <h3>Evidence</h3>
        <p>{evidence}</p>
    </div>

    {hash_section}

    <footer>
        <p>Basis Protocol · basisprotocol.xyz · {surface.upper()} {formula_version} · Methodology: deterministic, version-controlled, open</p>
        <p>Score proof pages are public by design — open methodology means open proof. · <a href="/terms">Terms</a></p>
    </footer>
</body>
</html>"""


# =============================================================================
# Discovery Layer API
# =============================================================================

@app.get("/api/discovery/latest")
async def get_discovery_latest():
    """Top 20 signals from last 7 days by novelty_score."""
    rows = fetch_all("""
        SELECT id, signal_type, domain, title, description, entities,
               novelty_score, direction, magnitude, baseline, detail,
               methodology_version, detected_at, acknowledged, published
        FROM discovery_signals
        WHERE detected_at >= NOW() - INTERVAL '7 days'
        ORDER BY novelty_score DESC
        LIMIT 20
    """)
    return {"signals": rows, "count": len(rows)}


@app.get("/api/discovery/domain/{domain}")
async def get_discovery_by_domain(domain: str):
    """Signals for a specific domain."""
    rows = fetch_all("""
        SELECT id, signal_type, domain, title, description, entities,
               novelty_score, direction, magnitude, baseline, detail,
               methodology_version, detected_at, acknowledged, published
        FROM discovery_signals
        WHERE domain = %s
          AND detected_at >= NOW() - INTERVAL '7 days'
        ORDER BY novelty_score DESC
        LIMIT 50
    """, (domain,))
    return {"domain": domain, "signals": rows, "count": len(rows)}


@app.get("/api/discovery/unacknowledged")
async def get_discovery_unacknowledged():
    """Signals not yet reviewed."""
    rows = fetch_all("""
        SELECT id, signal_type, domain, title, description, entities,
               novelty_score, direction, magnitude, baseline, detail,
               methodology_version, detected_at
        FROM discovery_signals
        WHERE acknowledged = FALSE
        ORDER BY detected_at DESC
        LIMIT 50
    """)
    return {"signals": rows, "count": len(rows)}


@app.post("/api/admin/discovery/ack/{signal_id}")
async def acknowledge_discovery_signal(signal_id: int, key: str = Query(default=None)):
    """Mark a discovery signal as acknowledged. Requires admin key."""
    admin_key = os.environ.get("ADMIN_KEY", "")
    if not admin_key or not key or not hmac.compare_digest(key, admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized — provide ?key=YOUR_ADMIN_KEY")
    try:
        execute("""
            UPDATE discovery_signals SET acknowledged = TRUE WHERE id = %s
        """, (signal_id,))
        return {"status": "acknowledged", "id": signal_id}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})


# =============================================================================
# Analysis — Drift Exploit aggregation endpoint
# =============================================================================

@app.get("/api/analysis/drift-exploit")
async def drift_exploit_analysis():
    """Structured analysis of the Drift Protocol exploit — aggregates PSI, CQI, exposure, and market data."""
    from app.composition import compute_cqi
    from datetime import datetime, timezone

    result = {
        "event": None,
        "drift_psi": None,
        "stablecoin_exposure": None,
        "cqi_pairs": {},
        "contagion": {
            "connected_wallets": 0,
            "note": "Solana wallet graph not yet indexed",
        },
        "market_impact": None,
        "narrative": None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # 1. Assessment event
    try:
        event_row = fetch_one("""
            SELECT * FROM assessment_events
            WHERE wallet_address = 'protocol:drift'
            ORDER BY created_at DESC LIMIT 1
        """)
        if event_row:
            d = dict(event_row)
            for k, v in d.items():
                if hasattr(v, "isoformat"):
                    d[k] = v.isoformat()
                elif isinstance(v, Decimal):
                    d[k] = float(v)
                elif isinstance(v, uuid_mod.UUID):
                    d[k] = str(v)
            result["event"] = d
    except Exception:
        pass

    # 2. Drift PSI score
    psi_row = fetch_one("""
        SELECT protocol_slug, protocol_name, overall_score, grade,
               category_scores, component_scores, raw_values, formula_version, computed_at
        FROM psi_scores WHERE protocol_slug = 'drift'
        ORDER BY computed_at DESC LIMIT 1
    """)
    if psi_row:
        # Count rank
        all_protocols = fetch_all("""
            SELECT DISTINCT ON (protocol_slug) protocol_slug, overall_score
            FROM psi_scores ORDER BY protocol_slug, computed_at DESC
        """)
        sorted_protos = sorted(
            [r for r in all_protocols if r.get("overall_score")],
            key=lambda r: float(r["overall_score"]),
            reverse=True,
        )
        rank = next((i + 1 for i, r in enumerate(sorted_protos) if r["protocol_slug"] == "drift"), None)

        # Find missing components
        from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
        all_comp_ids = set(PSI_V01_DEFINITION["components"].keys())
        scored_comp_ids = set((psi_row.get("component_scores") or {}).keys())
        missing = list(all_comp_ids - scored_comp_ids)

        result["drift_psi"] = {
            "current_score": float(psi_row["overall_score"]) if psi_row.get("overall_score") else None,
            "category_breakdown": psi_row.get("category_scores"),
            "components_missing": missing,
            "rank_among_protocols": rank,
            "total_protocols": len(sorted_protos),
            "scored_at": psi_row["computed_at"].isoformat() if psi_row.get("computed_at") else None,
        }

    # 3. Collateral exposure
    try:
        exposure_rows = fetch_all("""
            SELECT token_symbol, tvl_usd, is_sii_scored, sii_score, pool_type
            FROM protocol_collateral_exposure
            WHERE protocol_slug = 'drift'
            AND snapshot_date = (SELECT MAX(snapshot_date) FROM protocol_collateral_exposure WHERE protocol_slug = 'drift')
        """)
        if exposure_rows:
            result["stablecoin_exposure"] = [
                {
                    "symbol": r["token_symbol"],
                    "tvl_usd": float(r["tvl_usd"]) if r.get("tvl_usd") else 0,
                    "sii_scored": r.get("is_sii_scored", False),
                    "sii_score": float(r["sii_score"]) if r.get("sii_score") else None,
                }
                for r in exposure_rows
            ]
    except Exception:
        pass

    # 4. CQI pairs
    for symbol in ["usdc", "usdt", "dai"]:
        try:
            cqi = compute_cqi(symbol, "drift")
            if "error" not in cqi:
                result["cqi_pairs"][f"{symbol}_x_drift"] = {
                    "cqi": cqi.get("cqi_score"),
                    "sii": cqi.get("inputs", {}).get("sii", {}).get("score"),
                    "psi": cqi.get("inputs", {}).get("psi", {}).get("score"),
                }
        except Exception:
            pass

    # 5. Market impact (DRIFT token from CoinGecko)
    try:
        from app.collectors.psi_collector import fetch_coingecko_token
        token_data = fetch_coingecko_token("drift-protocol")
        if token_data:
            market = token_data.get("market_data", {})
            result["market_impact"] = {
                "drift_price": market.get("current_price", {}).get("usd"),
                "drift_24h_change_pct": market.get("price_change_percentage_24h"),
                "drift_market_cap": market.get("market_cap", {}).get("usd"),
                "drift_volume_24h": market.get("total_volume", {}).get("usd"),
            }
    except Exception:
        pass

    # 6. Narrative
    psi_score = result["drift_psi"]["current_score"] if result["drift_psi"] else "N/A"
    n_components = 24 - len(result["drift_psi"]["components_missing"]) if result["drift_psi"] else "N/A"
    usdc_cqi = result["cqi_pairs"].get("usdc_x_drift", {}).get("cqi", "N/A")
    usdc_sii = result["cqi_pairs"].get("usdc_x_drift", {}).get("sii", "N/A")
    exposure_usdc = next(
        (e["tvl_usd"] for e in (result["stablecoin_exposure"] or []) if e["symbol"] == "USDC"),
        0,
    )
    exposure_usdc_m = round(exposure_usdc / 1e6, 1) if exposure_usdc else 0

    result["narrative"] = {
        "headline": f"Drift Protocol exploit: ~$270M drained. PSI score: {psi_score}. USDC×Drift CQI: {usdc_cqi}.",
        "key_finding": f"Drift held ${exposure_usdc_m}M in USDC across its vaults. USDC's SII remained stable at {usdc_sii}, but the protocol-level failure demonstrates why CQI — the composition of stablecoin quality and protocol solvency — is the relevant risk surface.",
        "basis_insight": "A high SII score for USDC did not protect depositors because protocol-level risk was the failure mode. This is exactly what CQI measures.",
        "methodology_note": f"Drift is Basis's first Solana protocol. Governance components are not yet scored (Solana uses Realms, not Snapshot). The PSI score reflects {n_components}/24 available components.",
    }

    # 7. On-chain vault balances (real-time via Helius)
    try:
        from app.collectors.solana import get_drift_vault_balances
        import httpx as _httpx
        async with _httpx.AsyncClient() as vault_client:
            vault_balances = await get_drift_vault_balances(vault_client)
        result["on_chain_vault"] = {
            "balances": vault_balances[:10],  # top 10
            "total_stablecoin_usd": round(
                sum(b["usd_value_approx"] or 0 for b in vault_balances if b["is_stablecoin"]), 2
            ),
            "source": "on-chain (Helius RPC)",
            "note": "Real-time vault state — may differ from DeFiLlama aggregation",
        }
    except Exception:
        result["on_chain_vault"] = {"error": "Helius API unavailable or HELIUS_API_KEY not set"}

    return result


# =============================================================================
# Report Primitive — Endpoints
# =============================================================================


@app.get("/api/reports/{entity_type}/{entity_id}")
async def generate_report(
    entity_type: str,
    entity_id: str,
    template: str = Query(default=None),
    lens: Optional[str] = Query(default=None),
    format: str = Query(default="html"),
):
    """Generate an attested report for an entity."""
    try:
        from app.report import assemble_report_data
        from app.report_attestation import compute_report_hash, store_report_attestation
        from app.templates import get_template
        from app.lenses import load_lens, apply_lens

        if entity_type not in ("stablecoin", "protocol", "wallet"):
            raise HTTPException(status_code=400, detail=f"Invalid entity_type: {entity_type}. Use stablecoin, protocol, or wallet.")

        # Default template per entity type
        if template is None:
            template = {"stablecoin": "compliance", "protocol": "protocol_risk", "wallet": "wallet_risk"}.get(entity_type, "protocol_risk")

        render_fn = get_template(template)
        if not render_fn:
            raise HTTPException(status_code=400, detail=f"Unknown template: {template}")

        data = assemble_report_data(entity_type, entity_id)
        if not data:
            raise HTTPException(status_code=404, detail=f"{entity_type} '{entity_id}' not found")

        # Apply lens if specified
        lens_result = None
        lens_version = None
        if lens:
            lens_config = load_lens(lens)
            if not lens_config:
                raise HTTPException(status_code=400, detail=f"Unknown lens: {lens}. Use GET /api/lenses to see available lenses.")
            lens_result = apply_lens(lens_config, data)
            lens_version = lens_config.get("lens_version")

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        report_hash = compute_report_hash(data, template, lens, lens_version, ts,
                                          state_hashes=data.get("state_hashes"))

        # Store attestation
        store_report_attestation(
            entity_type, entity_id, template, lens, lens_version,
            report_hash, data.get("score_hashes", []),
            data.get("cqi_hashes"),
            data.get("formula_version", FORMULA_VERSION),
        )

        # SBT metadata always returns JSON
        if template == "sbt_metadata" or format == "json":
            import json as _json
            from app.templates.sbt_metadata import render as render_sbt
            if template == "sbt_metadata":
                content = render_sbt(data, lens_result, report_hash, ts)
            else:
                content = _json.dumps({
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "report_data": data,
                    "lens_result": lens_result,
                    "report_hash": report_hash,
                    "generated_at": ts,
                }, default=str, indent=2)
            return JSONResponse(
                content=_json.loads(content) if isinstance(content, str) else content,
                headers={"X-Report-Hash": report_hash},
            )

        html = render_fn(data, lens_result, report_hash, ts, format)
        return HTMLResponse(
            content=html,
            headers={
                "X-Report-Hash": report_hash,
                "Cache-Control": "public, max-age=300",
            },
        )
    except HTTPException:
        raise
    except Exception:
        import traceback
        from starlette.responses import PlainTextResponse
        return PlainTextResponse(
            content=traceback.format_exc(),
            status_code=500,
            media_type="text/plain",
        )


@app.get("/api/reports/verify/{report_hash}")
async def verify_report_endpoint(report_hash: str):
    """Verify a report's attestation chain."""
    from app.report_attestation import verify_report
    return verify_report(report_hash)


@app.get("/api/reports/templates")
async def list_report_templates():
    """List available report templates."""
    from app.templates import list_templates
    return {"templates": list_templates()}


@app.get("/api/reports/lenses")
async def list_report_lenses():
    """List available regulatory lenses."""
    from app.lenses import list_lenses
    return {"lenses": list_lenses()}


@app.get("/api/reports/sbt/{token_id}")
async def sbt_metadata(token_id: int):
    """ERC-721 metadata for a Basis Rating SBT."""
    row = fetch_one(
        "SELECT entity_type, entity_id, score, grade, confidence, report_hash, method_version FROM sbt_tokens WHERE token_id = %s",
        (token_id,),
    )
    if not row:
        raise HTTPException(404, "Token not found")

    from app.report import assemble_report_data
    from app.templates.sbt_metadata import render as render_sbt
    from app.report_attestation import compute_report_hash

    data = assemble_report_data(row["entity_type"], row["entity_id"])
    if not data:
        # Fallback: minimal metadata from sbt_tokens table
        import json as _json
        return JSONResponse({
            "name": f"Basis Rating — {row['entity_id']}",
            "description": f"Score: {row['score']}",
            "attributes": [
                {"trait_type": "Score", "value": float(row["score"]) if row.get("score") else 0},
            ],
            "report_hash": row.get("report_hash"),
        })

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    report_hash = row.get("report_hash") or compute_report_hash(data, "sbt_metadata", None, None, ts)
    content = render_sbt(data, None, report_hash, ts)
    import json as _json
    return JSONResponse(_json.loads(content))


# =============================================================================
# Lens Management Endpoints
# =============================================================================


@app.get("/api/lenses")
async def list_all_lenses():
    """List all available regulatory lenses (built-in + custom)."""
    from app.lenses import list_lenses
    lenses = list_lenses()
    return {"lenses": lenses, "count": len(lenses)}


@app.get("/api/lenses/{lens_id}")
async def get_lens_detail(lens_id: str):
    """Get full lens config including criteria."""
    from app.lenses import load_lens_from_db, load_lens, _compute_content_hash
    from app.database import fetch_one as _lf1

    # Try DB first for full metadata
    try:
        row = _lf1(
            "SELECT * FROM lens_configs WHERE lens_id = %s", (lens_id,)
        )
    except Exception:
        row = None

    if row:
        criteria = row["criteria"]
        if isinstance(criteria, str):
            import json as _json
            criteria = _json.loads(criteria)
        return {
            "lens_id": row["lens_id"],
            "name": row["name"],
            "version": row["version"],
            "author": row["author"],
            "description": row.get("description"),
            "criteria": criteria,
            "content_hash": row.get("content_hash"),
            "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
            "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
        }

    # Fallback: JSON file lens
    config = load_lens(lens_id)
    if not config:
        raise HTTPException(status_code=404, detail=f"Lens '{lens_id}' not found")

    criteria_body = {
        "framework": config.get("framework"),
        "classification": config.get("classification", {}),
    }
    return {
        "lens_id": config.get("lens_id"),
        "name": config.get("framework"),
        "version": config.get("lens_version"),
        "author": "basis-protocol",
        "description": config.get("description", ""),
        "criteria": criteria_body,
        "content_hash": _compute_content_hash(criteria_body),
        "created_at": None,
        "updated_at": None,
    }


@app.post("/api/lenses")
async def create_lens(request: Request):
    """Create a custom regulatory lens."""
    from app.lenses import _compute_content_hash
    from app.database import fetch_one as _lf2

    body = await request.json()

    # Validate required fields
    for field in ("lens_id", "name", "criteria"):
        if field not in body or not body[field]:
            raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

    lens_id = body["lens_id"].strip()
    if not lens_id or len(lens_id) > 64:
        raise HTTPException(status_code=400, detail="lens_id must be 1-64 characters")

    criteria = body["criteria"]
    if not isinstance(criteria, dict):
        raise HTTPException(status_code=400, detail="criteria must be a JSON object")

    classification = criteria.get("classification")
    if not classification or not isinstance(classification, dict):
        raise HTTPException(status_code=400, detail="criteria must contain a 'classification' object with at least one group")

    # Validate that each group has criteria array
    for group_id, group in classification.items():
        group_criteria = group.get("criteria")
        if not group_criteria or not isinstance(group_criteria, list):
            raise HTTPException(
                status_code=400,
                detail=f"Group '{group_id}' must contain a 'criteria' array",
            )
        for c in group_criteria:
            if "name" not in c or "threshold" not in c or "logic" not in c:
                raise HTTPException(
                    status_code=400,
                    detail=f"Each criterion in group '{group_id}' must have 'name', 'threshold', and 'logic'",
                )
            if c["logic"] not in ("category_score_above", "sub_score_above"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid logic '{c['logic']}'. Use 'category_score_above' or 'sub_score_above'",
                )

    content_hash = _compute_content_hash(criteria)

    try:
        row = _lf2(
            """INSERT INTO lens_configs (lens_id, name, version, author, description, criteria, content_hash)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               RETURNING id, lens_id, name, version, author, description, content_hash, created_at""",
            (
                lens_id,
                body["name"],
                body.get("version", "1.0"),
                body.get("author", "custom"),
                body.get("description", ""),
                json.dumps(criteria),
                content_hash,
            ),
        )
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(status_code=409, detail=f"Lens '{lens_id}' already exists")
        raise HTTPException(status_code=500, detail=f"Failed to create lens: {e}")

    # Clear cache so new lens is picked up
    from app.lenses import _LENS_CACHE
    _LENS_CACHE.pop(lens_id, None)

    return JSONResponse(
        status_code=201,
        content={
            "lens_id": row["lens_id"],
            "name": row["name"],
            "version": row["version"],
            "author": row["author"],
            "description": row.get("description"),
            "content_hash": row["content_hash"],
            "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        },
    )


@app.get("/api/lenses/{lens_id}/test")
async def test_lens(lens_id: str):
    """Run a lens against all scored stablecoins, return pass/fail results."""
    from app.lenses import load_lens, apply_lens
    from app.report import assemble_report_data

    lens_config = load_lens(lens_id)
    if not lens_config:
        raise HTTPException(status_code=404, detail=f"Lens '{lens_id}' not found")

    # Get all scored stablecoins
    rows = fetch_all("""
        SELECT s.stablecoin_id, st.name, st.symbol
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        ORDER BY s.overall_score DESC
    """)

    results = []
    pass_count = 0
    for row in rows:
        symbol = row["symbol"] or row["stablecoin_id"]
        data = assemble_report_data("stablecoin", symbol)
        if not data:
            results.append({
                "entity_id": symbol,
                "name": row.get("name", symbol),
                "pass": None,
                "error": "Could not assemble report data",
            })
            continue

        lens_result = apply_lens(lens_config, data)
        passed = lens_result.get("overall_pass", False)
        if passed:
            pass_count += 1

        results.append({
            "entity_id": symbol,
            "name": row.get("name", symbol),
            "pass": passed,
            "classification": lens_result.get("classification", {}),
        })

    total = len(results)
    return {
        "lens_id": lens_id,
        "lens_version": lens_config.get("lens_version") or lens_config.get("version"),
        "framework": lens_config.get("framework"),
        "results": results,
        "summary": {
            "total": total,
            "pass": pass_count,
            "fail": total - pass_count,
            "pass_rate": round(pass_count / total, 2) if total > 0 else 0,
        },
    }


@app.get("/api/state-root/latest")
async def state_root_latest():
    """Latest state root — all attestation hashes across all domains."""
    import json as _json
    pulse = fetch_one("""
        SELECT summary, pulse_date FROM daily_pulses
        ORDER BY pulse_date DESC LIMIT 1
    """)
    if not pulse:
        raise HTTPException(404, "No state root available")
    summary = pulse.get("summary")
    if isinstance(summary, str):
        summary = _json.loads(summary)
    state_root = summary.get("state_root") if summary else None
    if not state_root:
        raise HTTPException(404, "No state root in latest pulse")
    return {
        "pulse_date": str(pulse.get("pulse_date", "")),
        "state_root": state_root,
    }


# =============================================================================
# Provenance Proofs
# =============================================================================

@app.post("/api/provenance/register")
async def provenance_register(request: Request):
    _check_admin_key(request)
    body = await request.json()
    required = ["source_domain", "source_endpoint", "response_hash",
                "attestation_hash", "proof_url", "attestor_pubkey",
                "proved_at", "cycle_hour"]
    for field in required:
        if field not in body:
            raise HTTPException(status_code=400, detail=f"Missing field: {field}")

    from app.database import fetch_one as _prov_fetch
    row = _prov_fetch(
        """INSERT INTO provenance_proofs
           (source_domain, source_endpoint, response_hash, attestation_hash,
            proof_url, attestor_pubkey, proved_at, cycle_hour)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING id""",
        (body["source_domain"], body["source_endpoint"],
         body["response_hash"], body["attestation_hash"],
         body["proof_url"], body["attestor_pubkey"],
         body["proved_at"], body["cycle_hour"]),
    )
    return {"status": "registered", "id": row["id"] if row else None}


@app.get("/api/provenance/latest")
async def provenance_latest():
    from app.database import fetch_all as _prov_all, fetch_one as _prov_one
    cycle = _prov_one("SELECT MAX(cycle_hour) AS ch FROM provenance_proofs")
    if not cycle or not cycle.get("ch"):
        return {"proofs": [], "cycle_hour": None, "count": 0}
    rows = _prov_all(
        "SELECT * FROM provenance_proofs WHERE cycle_hour = %s ORDER BY id",
        (cycle["ch"],),
    )
    return {
        "proofs": [dict(r) for r in rows],
        "cycle_hour": str(cycle["ch"]),
        "count": len(rows),
    }


@app.get("/api/provenance/{domain}/{date}")
async def provenance_by_domain_date(domain: str, date: str):
    from app.database import fetch_all as _prov_all
    rows = _prov_all(
        """SELECT * FROM provenance_proofs
           WHERE source_domain = %s AND proved_at::date = %s::date
           ORDER BY proved_at""",
        (domain, date),
    )
    return {
        "proofs": [dict(r) for r in rows],
        "source_domain": domain,
        "date": date,
        "count": len(rows),
    }


@app.get("/api/provenance/verify/{attestation_hash}")
async def provenance_verify(attestation_hash: str):
    from app.database import fetch_one as _prov_one
    row = _prov_one(
        "SELECT * FROM provenance_proofs WHERE attestation_hash = %s",
        (attestation_hash,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Proof not found")
    return {
        "proof": dict(row),
        "verification_note": "Download proof from proof_url and verify against attestor_pubkey",
    }


@app.get("/api/provenance/summary")
async def provenance_summary():
    from app.database import fetch_one as _prov_one, fetch_all as _prov_all
    total = _prov_one("SELECT COUNT(*) AS n FROM provenance_proofs")
    sources = _prov_all("SELECT DISTINCT source_domain FROM provenance_proofs ORDER BY 1")
    date_range = _prov_one(
        "SELECT MIN(proved_at) AS first, MAX(proved_at) AS last FROM provenance_proofs"
    )
    today = _prov_one(
        "SELECT COUNT(*) AS n FROM provenance_proofs WHERE proved_at::date = CURRENT_DATE"
    )
    this_hour = _prov_one(
        "SELECT COUNT(*) AS n FROM provenance_proofs WHERE proved_at > NOW() - INTERVAL '1 hour'"
    )
    return {
        "total_proofs": total["n"] if total else 0,
        "sources": [r["source_domain"] for r in (sources or [])],
        "date_range": {
            "first": str(date_range["first"]) if date_range and date_range.get("first") else None,
            "last": str(date_range["last"]) if date_range and date_range.get("last") else None,
        },
        "proofs_today": today["n"] if today else 0,
        "proofs_this_hour": this_hour["n"] if this_hour else 0,
    }


@app.get("/api/provenance/cda-sources")
async def provenance_cda_sources():
    from app.database import fetch_all as _prov_all
    rows = _prov_all(
        "SELECT asset_symbol, issuer, source_url, content_type, discovered_at FROM cda_source_urls WHERE active = TRUE ORDER BY discovered_at DESC"
    )
    return {
        "sources": [dict(r) for r in (rows or [])],
        "count": len(rows or []),
    }


@app.get("/api/provenance/attestor-pubkey")
async def provenance_attestor_pubkey():
    pubkey = os.environ.get("ATTESTOR_PUBLIC_KEY", "")
    return {
        "pubkey": pubkey,
        "algorithm": "secp256k1",
        "note": "Basis self-hosted attestor. Verify provenance proofs against this key.",
    }


# =============================================================================

def _register_spa_catch_all(app_instance):
    """Register the SPA catch-all AFTER all other routes so it doesn't shadow them."""
    @app_instance.get("/{full_path:path}")
    async def serve_spa(request: Request, full_path: str):
        if full_path.startswith("api/") or full_path.startswith("docs") or full_path.startswith("openapi") or full_path.startswith("admin") or full_path.startswith("developers"):
            raise HTTPException(status_code=404, detail="Not found")

        # Fast path: static assets — skip all SSR attempts
        if full_path.startswith("assets/") or full_path == "favicon.ico":
            index_path = os.path.join(FRONTEND_DIR, "index.html")
            if os.path.exists(index_path):
                return FileResponse(index_path, headers={"Cache-Control": "no-cache"})
            return Response(status_code=404)

        # Proof pages are server-rendered for ALL visitors (not just bots)
        try:
            if full_path.startswith("proof/sii/"):
                symbol = full_path.split("proof/sii/")[1].split("/")[0].split("?")[0]
                if symbol:
                    return HTMLResponse(
                        content=_render_proof_html(symbol.lower(), "sii"),
                        headers={"Cache-Control": "public, max-age=300", "Basis-URL-Stability": "permanent"}
                    )
            elif full_path.startswith("proof/psi/"):
                slug = full_path.split("proof/psi/")[1].split("/")[0].split("?")[0]
                if slug:
                    return HTMLResponse(
                        content=_render_proof_html(slug.lower(), "psi"),
                        headers={"Cache-Control": "public, max-age=300", "Basis-URL-Stability": "permanent"}
                    )
        except Exception as e:
            logger.warning(f"Proof page render failed for /{full_path}: {e}")

        # Report pages are server-rendered for ALL visitors
        try:
            if full_path.startswith("report/"):
                parts = full_path.split("/")
                if len(parts) >= 3:
                    r_entity_type = parts[1]
                    r_entity_id = parts[2].split("?")[0]
                    # Parse query params from URL
                    import urllib.parse as _urlparse
                    qs = _urlparse.parse_qs(_urlparse.urlparse(str(request.url)).query)
                    r_template = qs.get("template", [None])[0]
                    r_lens = qs.get("lens", [None])[0]
                    r_format = qs.get("format", ["html"])[0]
                    response = await generate_report(
                        r_entity_type, r_entity_id,
                        template=r_template, lens=r_lens, format=r_format,
                    )
                    return response
        except HTTPException:
            pass  # Fall through to SPA
        except Exception as e:
            logger.warning(f"Report page render failed for /{full_path}: {e}")

        # Serve server-rendered HTML for bots/AI on key routes
        if _is_bot(request):
            try:
                if full_path in ("", "/"):
                    return HTMLResponse(
                        content=_render_rankings_html(),
                        headers={"Cache-Control": "public, max-age=300", "Basis-URL-Stability": "permanent"}
                    )
                elif full_path == "witness":
                    return HTMLResponse(
                        content=_render_witness_html(),
                        headers={"Cache-Control": "public, max-age=300", "Basis-URL-Stability": "permanent"}
                    )
            except Exception as e:
                logger.warning(f"SSR render failed for /{full_path}: {e}")
                # Fall through to SPA

        index_path = os.path.join(FRONTEND_DIR, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path, headers={"Cache-Control": "no-cache"})
        return {"name": "Basis Protocol API", "version": FORMULA_VERSION, "docs": "/docs"}
