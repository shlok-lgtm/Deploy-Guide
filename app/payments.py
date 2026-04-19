"""
x402 Agent Payment Layer
========================
Middleware + paid route definitions for machine-native micropayments.
Agents pay USDC on Base per-request via the x402 protocol (HTTP 402).
Free endpoints are completely unaffected.
"""

import os
import json
import logging
import hashlib
import base64
import random
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

from fastapi import APIRouter, Request, HTTPException, Query
from typing import Optional

import jwt
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from x402 import x402ResourceServer
from x402.http.facilitator_client import HTTPFacilitatorClient
from x402.http.middleware.fastapi import PaymentMiddlewareASGI
from x402.http.types import RouteConfig, PaymentOption
from x402.mechanisms.evm.exact import ExactEvmServerScheme
from x402.schemas import SupportedKind, SupportedResponse

from functools import wraps
from app.database import fetch_one, fetch_all, execute

logger = logging.getLogger("payments")


def _log_payment(request, endpoint: str, price_usd: float = 0.001):
    """Best-effort payment logging."""
    try:
        from app.database import execute
        execute(
            "INSERT INTO payment_log (endpoint, price_usd, protocol, ip_address) VALUES (%s, %s, 'x402', %s)",
            (endpoint, price_usd, request.client.host if request.client else "unknown"),
        )
    except Exception:
        pass

# --- Configuration ---
BASIS_WALLET = os.environ.get("BASIS_PAYMENT_WALLET", "")

# CDP credentials — pass to x402.org facilitator to unlock mainnet
CDP_API_KEY_ID = os.environ.get("CDP_API_KEY_ID", "")
CDP_API_KEY_SECRET = os.environ.get("CDP_API_KEY_SECRET", "")

X402_FACILITATOR = os.environ.get(
    "X402_FACILITATOR_URL", "https://x402.org/facilitator"
)

_USE_CDP = bool(CDP_API_KEY_ID and CDP_API_KEY_SECRET)

if _USE_CDP:
    X402_NETWORK = os.environ.get("X402_NETWORK", "eip155:8453")   # Base mainnet
else:
    X402_NETWORK = os.environ.get("X402_NETWORK", "eip155:84532")  # Base Sepolia testnet


def _load_cdp_private_key() -> Ed25519PrivateKey | None:
    """Load Ed25519 private key from CDP_API_KEY_SECRET (base64-encoded 64-byte seed+pub)."""
    if not CDP_API_KEY_SECRET:
        return None
    try:
        raw = base64.b64decode(CDP_API_KEY_SECRET)
        if len(raw) == 64:
            return Ed25519PrivateKey.from_private_bytes(raw[:32])
        logger.error("CDP key secret decoded to %d bytes, expected 64", len(raw))
        return None
    except Exception as e:
        logger.error("Failed to decode CDP key secret: %s", e)
        return None


_CDP_PRIVATE_KEY = _load_cdp_private_key() if _USE_CDP else None


def _cdp_jwt(method: str, path: str) -> str:
    """Build a CDP JWT for a specific facilitator endpoint."""
    parsed = urlparse(X402_FACILITATOR)
    host = parsed.hostname or "x402.org"
    uri = f"{method} {host}{path}"
    now = int(time.time())
    nonce = "".join([str(random.randint(0, 9)) for _ in range(16)])
    payload = {
        "sub": CDP_API_KEY_ID,
        "iss": "cdp",
        "nbf": now,
        "exp": now + 120,
        "uris": [uri],
    }
    headers = {
        "kid": CDP_API_KEY_ID,
        "typ": "JWT",
        "nonce": nonce,
    }
    return jwt.encode(payload, _CDP_PRIVATE_KEY, algorithm="EdDSA", headers=headers)


def _cdp_create_headers() -> dict[str, dict[str, str]]:
    """Build per-endpoint CDP auth headers for the x402.org facilitator."""
    parsed = urlparse(X402_FACILITATOR)
    base_path = parsed.path.rstrip("/")
    return {
        "verify": {"Authorization": f"Bearer {_cdp_jwt('POST', f'{base_path}/verify')}"},
        "settle": {"Authorization": f"Bearer {_cdp_jwt('POST', f'{base_path}/settle')}"},
        "supported": {"Authorization": f"Bearer {_cdp_jwt('GET', f'{base_path}/supported')}"},
    }


class _MainnetFacilitatorClient:
    """Wraps HTTPFacilitatorClient to inject mainnet into its supported networks.

    The x402.org /supported endpoint doesn't advertise mainnet even with CDP auth,
    but verify/settle work on mainnet when CDP auth headers are present.
    This wrapper adds the mainnet network so route validation passes.
    """

    def __init__(self, inner: HTTPFacilitatorClient, network: str):
        self._inner = inner
        self._network = network

    def get_supported(self) -> SupportedResponse:
        try:
            resp = self._inner.get_supported()
        except Exception:
            resp = SupportedResponse(kinds=[])

        networks = {k.network for k in resp.kinds}
        if self._network not in networks:
            resp.kinds.append(
                SupportedKind(x402_version=2, scheme="exact", network=self._network)
            )
        return resp

    async def verify(self, payload, requirements):
        return await self._inner.verify(payload, requirements)

    async def settle(self, payload, requirements):
        return await self._inner.settle(payload, requirements)


def _route(price: str, description: str) -> RouteConfig:
    """Helper to create a RouteConfig with standard options."""
    return RouteConfig(
        accepts=PaymentOption(
            scheme="exact",
            pay_to=BASIS_WALLET,
            price=price,
            network=X402_NETWORK,
        ),
        description=description,
        mime_type="application/json",
    )


def create_x402_middleware():
    """Create the x402 payment middleware for FastAPI.

    Returns (middleware_class, kwargs) for app.add_middleware().
    """
    if not BASIS_WALLET:
        raise ValueError("BASIS_PAYMENT_WALLET env var not set")

    if _USE_CDP:
        inner = HTTPFacilitatorClient({
            "url": X402_FACILITATOR,
            "create_headers": _cdp_create_headers,
        })
        facilitator = _MainnetFacilitatorClient(inner, X402_NETWORK)
        logger.info("x402: CDP auth on %s, network %s", X402_FACILITATOR, X402_NETWORK)
    else:
        facilitator = HTTPFacilitatorClient({"url": X402_FACILITATOR})
        logger.info("x402: using x402.org facilitator (network %s)", X402_NETWORK)

    server = x402ResourceServer(facilitator_clients=facilitator)
    server.register(X402_NETWORK, ExactEvmServerScheme())

    routes = {}
    from app.paid_endpoints import PAID_ENDPOINTS
    for ep in PAID_ENDPOINTS:
        key = f"{ep['method']} {ep['url']}"
        routes[key] = _route(ep["price"], ep["description"])

    return PaymentMiddlewareASGI, {"routes": routes, "server": server}


# =============================================================================
# Paid Route Handlers
# These call the same DB queries / functions as the free endpoints.
# The x402 middleware handles 402 challenge/response before these run.
# =============================================================================

def _log_payment(endpoint: str, request: Request, price_usd: str) -> None:
    """Log a successful payment to the payment_log table."""
    try:
        # x402 middleware sets these headers after successful verification
        payer = request.headers.get("x-payer-address") or request.headers.get("x-payment-from") or ""
        tx_hash = request.headers.get("x-payment-tx-hash") or request.headers.get("x-transaction-hash") or ""
        ip = request.client.host if request.client else ""

        # Parse price — strip '$' prefix
        price_val = float(price_usd.replace("$", "")) if price_usd else 0

        execute("""
            INSERT INTO payment_log (endpoint, price_usd, protocol, payer_address, tx_hash, verified, ip_address)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            endpoint,
            price_val,
            "x402",
            payer or None,
            tx_hash or None,
            True,  # only called after successful middleware verification
            ip or None,
        ))
    except Exception as e:
        logger.warning(f"Payment logging failed for {endpoint}: {e}")


# Price lookup for logging
_ROUTE_PRICES = {
    "/api/paid/sii/rankings": "$0.005",
    "/api/paid/psi/scores": "$0.005",
    "/api/paid/pulse/latest": "$0.002",
    "/api/paid/discovery/latest": "$0.005",
}
_DEFAULT_PRICE = "$0.001"


paid_router = APIRouter(prefix="/api/paid", tags=["paid"])


@paid_router.get("/sii/rankings")
async def paid_sii_rankings(request: Request):
    """Paid: All stablecoin SII scores."""
    _log_payment("/api/paid/sii/rankings", request, "$0.005")
    from app.scoring import FORMULA_VERSION
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
            "score": float(row["overall_score"]),
            "price": float(row["current_price"]) if row.get("current_price") else None,
            "categories": {
                "peg": float(row["peg_score"]) if row.get("peg_score") else None,
                "liquidity": float(row["liquidity_score"]) if row.get("liquidity_score") else None,
                "flows": float(row["mint_burn_score"]) if row.get("mint_burn_score") else None,
                "distribution": float(row["distribution_score"]) if row.get("distribution_score") else None,
                "structural": float(row["structural_score"]) if row.get("structural_score") else None,
            },
            "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        })
    _log_payment(request, request.url.path)
    return {
        "stablecoins": results,
        "count": len(results),
        "methodology_version": FORMULA_VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tier": "paid",
    }


@paid_router.get("/sii/{coin}")
async def paid_sii_detail(request: Request, coin: str):
    """Paid: Single stablecoin detail."""
    _log_payment(f"/api/paid/sii/{coin}", request, "$0.001")
    from app.scoring import SII_V1_WEIGHTS, FORMULA_VERSION
    row = fetch_one("""
        SELECT s.*, st.name, st.symbol, st.issuer, st.contract AS token_contract
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        WHERE s.stablecoin_id = %s
    """, (coin,))
    if not row:
        raise HTTPException(status_code=404, detail=f"Stablecoin '{coin}' not found")
    components = fetch_all("""
        SELECT DISTINCT ON (component_id)
          component_id, category, raw_value, normalized_score, data_source, collected_at
        FROM component_readings
        WHERE stablecoin_id = %s AND collected_at > NOW() - INTERVAL '48 hours'
        ORDER BY component_id, collected_at DESC
    """, (coin,))
    _log_payment(request, request.url.path)
    return {
        "id": row["stablecoin_id"],
        "name": row["name"],
        "symbol": row["symbol"],
        "score": float(row["overall_score"]),
        "price": float(row["current_price"]) if row.get("current_price") else None,
        "categories": {
            "peg": {"score": float(row["peg_score"]) if row.get("peg_score") else None, "weight": SII_V1_WEIGHTS["peg_stability"]},
            "liquidity": {"score": float(row["liquidity_score"]) if row.get("liquidity_score") else None, "weight": SII_V1_WEIGHTS["liquidity_depth"]},
            "flows": {"score": float(row["mint_burn_score"]) if row.get("mint_burn_score") else None, "weight": SII_V1_WEIGHTS["mint_burn_dynamics"]},
            "distribution": {"score": float(row["distribution_score"]) if row.get("distribution_score") else None, "weight": SII_V1_WEIGHTS["holder_distribution"]},
            "structural": {"score": float(row["structural_score"]) if row.get("structural_score") else None, "weight": SII_V1_WEIGHTS["structural_risk_composite"]},
        },
        "components": [
            {
                "id": c["component_id"], "category": c["category"],
                "raw_value": c["raw_value"],
                "normalized_score": round(c["normalized_score"], 2) if c["normalized_score"] else None,
                "data_source": c["data_source"],
                "collected_at": c["collected_at"].isoformat() if c["collected_at"] else None,
            }
            for c in components
        ],
        "methodology_version": FORMULA_VERSION,
        "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        "tier": "paid",
    }


@paid_router.get("/psi/scores")
async def paid_psi_all(request: Request):
    """Paid: All PSI scores."""
    _log_payment("/api/paid/psi/scores", request, "$0.005")
    rows = fetch_all("""
        SELECT DISTINCT ON (protocol_slug)
            protocol_slug, protocol_name, overall_score, grade,
            category_scores, formula_version, computed_at
        FROM psi_scores ORDER BY protocol_slug, computed_at DESC
    """)
    from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
    _log_payment(request, request.url.path)
    return {
        "protocols": [
            {
                "protocol_slug": r["protocol_slug"],
                "protocol_name": r["protocol_name"],
                "score": float(r["overall_score"]) if r.get("overall_score") else None,
                "category_scores": r.get("category_scores"),
                "computed_at": r["computed_at"].isoformat() if r.get("computed_at") else None,
            }
            for r in rows
        ],
        "count": len(rows),
        "version": PSI_V01_DEFINITION["version"],
        "tier": "paid",
    }


@paid_router.get("/psi/scores/{slug}")
async def paid_psi_detail(request: Request, slug: str):
    """Paid: Single PSI detail."""
    _log_payment(f"/api/paid/psi/scores/{slug}", request, "$0.001")
    row = fetch_one("""
        SELECT protocol_slug, protocol_name, overall_score, grade,
               category_scores, component_scores, raw_values, formula_version, computed_at
        FROM psi_scores WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1
    """, (slug,))
    if not row:
        raise HTTPException(status_code=404, detail=f"Protocol '{slug}' not found")
    _log_payment(request, request.url.path)
    return {
        "protocol_slug": row["protocol_slug"],
        "protocol_name": row["protocol_name"],
        "score": float(row["overall_score"]) if row.get("overall_score") else None,
        "category_scores": row.get("category_scores"),
        "component_scores": row.get("component_scores"),
        "raw_values": row.get("raw_values"),
        "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        "tier": "paid",
    }


@paid_router.get("/cqi")
async def paid_cqi(request: Request, asset: str = Query(...), protocol: str = Query(...)):
    """Paid: CQI score for an asset-in-protocol pair."""
    _log_payment("/api/paid/cqi", request, "$0.001")
    from app.composition import compute_cqi
    result = compute_cqi(asset, protocol)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    result["tier"] = "paid"
    _log_payment(request, request.url.path)
    return result


@paid_router.get("/rqs/{slug}")
async def paid_rqs(slug: str):
    """Paid: Reserve Quality Score for a protocol's stablecoin treasury."""
    from app.composition import compute_rqs_for_protocol
    result = compute_rqs_for_protocol(slug)
    if "error" in result and "rqs_score" not in result:
        raise HTTPException(status_code=404, detail=result["error"])
    result["tier"] = "paid"
    return result


@paid_router.get("/pulse/latest")
async def paid_pulse(request: Request):
    """Paid: Latest daily pulse."""
    _log_payment("/api/paid/pulse/latest", request, "$0.002")
    row = fetch_one("SELECT * FROM daily_pulses ORDER BY pulse_date DESC LIMIT 1")
    if not row:
        raise HTTPException(status_code=404, detail="No pulse data available")
    summary = row.get("summary", {})
    if isinstance(summary, str):
        summary = json.loads(summary)
    canonical = json.dumps(summary, sort_keys=True, separators=(",", ":"), default=str)
    content_hash = "0x" + hashlib.sha256(canonical.encode()).hexdigest()
    _log_payment(request, request.url.path)
    return {
        "pulse_date": row["pulse_date"].isoformat() if hasattr(row["pulse_date"], "isoformat") else str(row["pulse_date"]),
        "summary": summary,
        "content_hash": content_hash,
        "tier": "paid",
    }


@paid_router.get("/discovery/latest")
async def paid_discovery(request: Request):
    """Paid: Latest cross-domain discovery signals."""
    _log_payment("/api/paid/discovery/latest", request, "$0.005")
    rows = fetch_all("""
        SELECT id, signal_type, domain, title, description, entities,
               novelty_score, direction, magnitude, baseline, detail,
               methodology_version, detected_at, acknowledged, published
        FROM discovery_signals
        WHERE detected_at >= NOW() - INTERVAL '7 days'
        ORDER BY novelty_score DESC LIMIT 20
    """)
    _log_payment(request, request.url.path)
    return {"signals": rows, "count": len(rows), "tier": "paid"}


@paid_router.get("/wallets/{address}/profile")
async def paid_wallet_profile(request: Request, address: str):
    """Paid: Full wallet risk profile with behavioral signals."""
    _log_payment(f"/api/paid/wallets/{address}/profile", request, "$0.005")
    from app.wallet_profile import generate_wallet_profile
    profile = generate_wallet_profile(address)
    if not profile:
        raise HTTPException(status_code=404, detail="Wallet not found in index")

    addr = address.lower()
    top_connections = fetch_all("""
        SELECT
            CASE WHEN from_address = %s THEN to_address ELSE from_address END AS counterparty,
            weight, total_value_usd
        FROM wallet_graph.wallet_edges
        WHERE from_address = %s OR to_address = %s
        ORDER BY weight DESC LIMIT 5
    """, (addr, addr, addr))
    edge_count = fetch_one(
        "SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges WHERE from_address = %s OR to_address = %s",
        (addr, addr),
    )
    profile["connections_summary"] = {
        "total_connections": edge_count["cnt"] if edge_count else 0,
        "top_counterparties": [
            {"address": c["counterparty"], "weight": round(float(c["weight"]), 4) if c.get("weight") else 0, "value": c["total_value_usd"]}
            for c in top_connections
        ],
    }
    profile["tier"] = "paid"
    _log_payment(request, request.url.path)
    return profile


@paid_router.get("/report/{entity_type}/{entity_id}")
async def paid_report(
    request: Request,
    entity_type: str,
    entity_id: str,
    template: str = "protocol_risk",
    lens: str = None,
):
    """Paid: Attested risk report with optional regulatory lens."""
    _log_payment(f"/api/paid/report/{entity_type}/{entity_id}", request, "$0.01")
    from app.report import assemble_report_data
    from app.report_attestation import compute_report_hash, store_report_attestation
    from app.templates import get_template
    from app.lenses import load_lens, apply_lens
    from app.scoring import FORMULA_VERSION

    if entity_type not in ("stablecoin", "protocol", "wallet"):
        raise HTTPException(status_code=400, detail=f"Invalid entity_type: {entity_type}")

    render_fn = get_template(template)
    if not render_fn:
        raise HTTPException(status_code=400, detail=f"Unknown template: {template}")

    data = assemble_report_data(entity_type, entity_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"{entity_type} '{entity_id}' not found")

    lens_result = None
    lens_version = None
    if lens:
        lens_config = load_lens(lens)
        if lens_config:
            lens_result = apply_lens(lens_config, data)
            lens_version = lens_config.get("lens_version")

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    report_hash = compute_report_hash(data, template, lens, lens_version, ts,
                                      state_hashes=data.get("state_hashes"))
    store_report_attestation(
        entity_type, entity_id, template, lens, lens_version,
        report_hash, data.get("score_hashes", []),
        data.get("cqi_hashes"), data.get("formula_version", FORMULA_VERSION),
    )

    import json as _json
    _log_payment(request, request.url.path)
    return {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "report_data": _json.loads(_json.dumps(data, default=str)),
        "lens_result": lens_result,
        "report_hash": report_hash,
        "generated_at": ts,
        "tier": "paid",
    }


@paid_router.get("/rpi/scores")
async def paid_rpi_all():
    """Paid: All RPI scores."""
    rows = fetch_all("""
        SELECT DISTINCT ON (protocol_slug)
            protocol_slug, protocol_name, overall_score, grade,
            component_scores, methodology_version, computed_at
        FROM rpi_scores ORDER BY protocol_slug, computed_at DESC
    """)
    from app.index_definitions.rpi_v2 import RPI_V2_DEFINITION
    return {
        "protocols": [
            {
                "protocol_slug": r["protocol_slug"],
                "protocol_name": r["protocol_name"],
                "score": float(r["overall_score"]) if r.get("overall_score") else None,
                "grade": r.get("grade"),
                "component_scores": r.get("component_scores"),
                "computed_at": r["computed_at"].isoformat() if r.get("computed_at") else None,
            }
            for r in rows
        ],
        "count": len(rows),
        "version": RPI_V2_DEFINITION["version"],
        "tier": "paid",
    }


@paid_router.get("/rpi/scores/{slug}")
async def paid_rpi_detail(slug: str):
    """Paid: Single RPI detail."""
    row = fetch_one("""
        SELECT protocol_slug, protocol_name, overall_score, grade,
               component_scores, raw_values, inputs_hash,
               methodology_version, computed_at
        FROM rpi_scores WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1
    """, (slug,))
    if not row:
        raise HTTPException(status_code=404, detail=f"Protocol '{slug}' not found in RPI scores")
    return {
        "protocol_slug": row["protocol_slug"],
        "protocol_name": row["protocol_name"],
        "score": float(row["overall_score"]) if row.get("overall_score") else None,
        "grade": row.get("grade"),
        "component_scores": row.get("component_scores"),
        "raw_values": row.get("raw_values"),
        "inputs_hash": row.get("inputs_hash"),
        "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        "tier": "paid",
    }
