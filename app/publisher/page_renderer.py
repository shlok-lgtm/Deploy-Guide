"""
Publisher — Page Renderer
==========================
Generates HTML pages with embedded JSON-LD for wallets, assets,
assessments, and daily pulses. Served on-demand from the database.
"""

import json
import logging
import os
from decimal import Decimal

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response

from app.database import fetch_one, fetch_all

logger = logging.getLogger(__name__)


class _DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


# Jinja2 setup — lazy import to avoid hard dependency at module level
_jinja_env = None


def _get_jinja_env():
    global _jinja_env
    if _jinja_env is None:
        from jinja2 import Environment, FileSystemLoader
        template_dir = os.path.join(os.path.dirname(__file__), "..", "..", "templates")
        _jinja_env = Environment(
            loader=FileSystemLoader(os.path.abspath(template_dir)),
            autoescape=True,
        )
    return _jinja_env


CANONICAL_BASE_URL = os.environ.get("CANONICAL_BASE_URL", "https://basisprotocol.xyz").rstrip("/")


def _page_response(html_content: str) -> HTMLResponse:
    """Wrap HTML content with URL stability headers."""
    response = HTMLResponse(content=html_content)
    response.headers["Basis-URL-Stability"] = "permanent"
    response.headers["Basis-Protocol-Version"] = "v1.0.0"
    response.headers["Cache-Control"] = "public, max-age=300"
    return response


async def update_wallet_page(assessment: dict) -> None:
    """Update wallet page data (on-demand rendering — no-op for now)."""
    pass


async def update_asset_pages(assessment: dict) -> None:
    """Update asset page data (on-demand rendering — no-op for now)."""
    pass


async def create_assessment_page(assessment: dict) -> None:
    """Create assessment page (on-demand rendering — no-op for now)."""
    pass


def register_page_routes(app: FastAPI) -> None:
    """Register HTML page routes for wallets, assets, assessments, and pulses."""

    @app.get("/wallet/{address}")
    async def wallet_page(request: Request, address: str):
        """Rendered HTML wallet risk page with JSON-LD."""
        # Content negotiation: JSON clients get redirected to API
        if "application/json" in request.headers.get("accept", ""):
            from fastapi.responses import RedirectResponse
            return RedirectResponse(url=f"/api/wallets/{address}", status_code=307)

        # Fetch current risk data
        risk = fetch_one("""
            SELECT * FROM wallet_graph.wallet_risk_scores
            WHERE wallet_address = %s
            ORDER BY computed_at DESC LIMIT 1
        """, (address.lower(),))
        if not risk:
            raise HTTPException(status_code=404, detail="Wallet not found")

        holdings = fetch_all("""
            SELECT symbol, value_usd, pct_of_wallet, is_scored,
                   sii_score, sii_grade
            FROM wallet_graph.wallet_holdings
            WHERE wallet_address = %s
            AND indexed_at = (
                SELECT MAX(indexed_at) FROM wallet_graph.wallet_holdings
                WHERE wallet_address = %s
            )
            ORDER BY value_usd DESC
        """, (address.lower(), address.lower()))

        recent_assessments = fetch_all("""
            SELECT id::text, created_at, trigger_type, severity,
                   wallet_risk_score, wallet_risk_grade
            FROM assessment_events
            WHERE wallet_address = %s
            ORDER BY created_at DESC LIMIT 10
        """, (address.lower(),))

        context = {
            "address": address.lower(),
            "risk": dict(risk),
            "holdings": [dict(h) for h in holdings],
            "assessments": [dict(a) for a in recent_assessments],
            "json_ld": _wallet_json_ld(address.lower(), risk, holdings),
        }

        try:
            env = _get_jinja_env()
            template = env.get_template("wallet.html")
            return _page_response(template.render(**context))
        except Exception as e:
            logger.error(f"Template rendering failed: {e}")
            return _page_response(_fallback_wallet_html(context))

    @app.get("/asset/{symbol}")
    async def asset_page(request: Request, symbol: str):
        """Rendered HTML asset page with JSON-LD."""
        # Content negotiation: JSON clients get redirected to API
        if "application/json" in request.headers.get("accept", ""):
            from fastapi.responses import RedirectResponse
            return RedirectResponse(url=f"/api/scores/{symbol}", status_code=307)

        score = fetch_one("""
            SELECT * FROM scores WHERE stablecoin_id = %s
        """, (symbol.lower(),))
        if not score:
            raise HTTPException(status_code=404, detail="Asset not found")

        history = fetch_all("""
            SELECT score_date, overall_score, grade
            FROM score_history
            WHERE stablecoin = %s
            ORDER BY score_date DESC LIMIT 30
        """, (symbol.lower(),))

        context = {
            "symbol": symbol.upper(),
            "score": dict(score),
            "history": [dict(h) for h in history],
            "json_ld": _asset_json_ld(symbol, score),
        }

        try:
            env = _get_jinja_env()
            template = env.get_template("asset.html")
            return _page_response(template.render(**context))
        except Exception as e:
            logger.error(f"Template rendering failed: {e}")
            return _page_response(_fallback_asset_html(context))

    @app.get("/assessment/{assessment_id}")
    async def assessment_page(request: Request, assessment_id: str):
        """Rendered HTML assessment event page with JSON-LD."""
        row = fetch_one("""
            SELECT * FROM assessment_events WHERE id::text = %s
        """, (assessment_id,))
        if not row:
            raise HTTPException(status_code=404, detail="Assessment not found")

        # Content negotiation: return JSON directly (no dedicated API endpoint)
        if "application/json" in request.headers.get("accept", ""):
            data = dict(row)
            # Convert non-serializable types
            for k, v in data.items():
                if hasattr(v, "isoformat"):
                    data[k] = v.isoformat()
                elif isinstance(v, Decimal):
                    data[k] = float(v)
            return JSONResponse(content=data)

        context = {
            "assessment": dict(row),
            "json_ld": _assessment_json_ld(row),
        }

        try:
            env = _get_jinja_env()
            template = env.get_template("assessment.html")
            return _page_response(template.render(**context))
        except Exception as e:
            logger.error(f"Template rendering failed: {e}")
            return _page_response(_fallback_assessment_html(context))

    @app.get("/pulse/{pulse_date}")
    async def pulse_page(request: Request, pulse_date: str):
        """Rendered HTML daily pulse page."""
        row = fetch_one("""
            SELECT * FROM daily_pulses WHERE pulse_date = %s
        """, (pulse_date,))
        if not row:
            raise HTTPException(status_code=404, detail="No pulse for that date")

        summary = row.get("summary", {})
        if isinstance(summary, str):
            summary = json.loads(summary)

        # Content negotiation: return JSON directly (no dedicated API endpoint)
        if "application/json" in request.headers.get("accept", ""):
            data = dict(row)
            for k, v in data.items():
                if hasattr(v, "isoformat"):
                    data[k] = v.isoformat()
                elif isinstance(v, Decimal):
                    data[k] = float(v)
            return JSONResponse(content=data)

        context = {
            "pulse_date": pulse_date,
            "summary": summary,
            "json_ld": _pulse_json_ld(pulse_date, summary),
        }

        try:
            env = _get_jinja_env()
            template = env.get_template("pulse.html")
            return _page_response(template.render(**context))
        except Exception as e:
            logger.error(f"Template rendering failed: {e}")
            return _page_response(_fallback_pulse_html(context))

    @app.get("/sitemap.xml")
    async def sitemap_xml():
        """Dynamic XML sitemap of all published entities."""
        urls = []

        # Active stablecoins
        try:
            coins = fetch_all("SELECT symbol FROM stablecoins WHERE is_active = TRUE")
            for row in coins:
                urls.append(f"{CANONICAL_BASE_URL}/asset/{row['symbol']}")
        except Exception as e:
            logger.warning(f"Sitemap: failed to fetch stablecoins: {e}")

        # Top wallets by value
        try:
            wallets = fetch_all("""
                SELECT DISTINCT ON (wallet_address) wallet_address
                FROM wallet_graph.wallet_risk_scores
                ORDER BY wallet_address, total_stablecoin_value DESC
                LIMIT 1000
            """)
            for row in wallets:
                urls.append(f"{CANONICAL_BASE_URL}/wallet/{row['wallet_address']}")
        except Exception as e:
            logger.warning(f"Sitemap: failed to fetch wallets: {e}")

        # Notable+ assessment events
        try:
            assessments = fetch_all("""
                SELECT id::text FROM assessment_events
                WHERE severity IN ('notable', 'alert', 'critical')
                ORDER BY created_at DESC LIMIT 500
            """)
            for row in assessments:
                urls.append(f"{CANONICAL_BASE_URL}/assessment/{row['id']}")
        except Exception as e:
            logger.warning(f"Sitemap: failed to fetch assessments: {e}")

        # Daily pulses
        try:
            pulses = fetch_all("""
                SELECT pulse_date FROM daily_pulses ORDER BY pulse_date DESC
            """)
            for row in pulses:
                date_str = row["pulse_date"]
                if hasattr(date_str, "isoformat"):
                    date_str = date_str.isoformat()
                urls.append(f"{CANONICAL_BASE_URL}/pulse/{date_str}")
        except Exception as e:
            logger.warning(f"Sitemap: failed to fetch pulses: {e}")

        xml_lines = ['<?xml version="1.0" encoding="UTF-8"?>',
                     '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
        for url in urls:
            xml_lines.append(f"  <url><loc>{url}</loc></url>")
        xml_lines.append("</urlset>")

        return Response(content="\n".join(xml_lines), media_type="application/xml")

    @app.get("/robots.txt")
    async def robots_txt():
        """Robots.txt with sitemap reference."""
        content = (
            "User-agent: *\n"
            "Allow: /wallet/\n"
            "Allow: /asset/\n"
            "Allow: /assessment/\n"
            "Allow: /pulse/\n"
            "Disallow: /api/admin/\n"
            "Disallow: /admin\n"
            f"Sitemap: {CANONICAL_BASE_URL}/sitemap.xml\n"
        )
        return PlainTextResponse(content=content)

    logger.info("Page routes registered: /wallet, /asset, /assessment, /pulse, /sitemap.xml, /robots.txt")


# --- JSON-LD Generators ---

def _wallet_json_ld(address: str, risk: dict, holdings: list) -> str:
    ld = {
        "@context": "https://schema.org",
        "@type": "FinancialProduct",
        "name": f"Wallet Risk Profile: {address[:10]}...{address[-4:]}",
        "description": f"Stablecoin risk profile for Ethereum wallet {address}",
        "additionalProperty": [
            {"@type": "PropertyValue", "name": "risk_score", "value": risk.get("risk_score")},
            {"@type": "PropertyValue", "name": "risk_grade", "value": risk.get("risk_grade")},
            {"@type": "PropertyValue", "name": "concentration_hhi", "value": risk.get("concentration_hhi")},
            {"@type": "PropertyValue", "name": "total_value", "value": risk.get("total_stablecoin_value")},
            {"@type": "PropertyValue", "name": "holdings_count", "value": len(holdings)},
        ],
    }
    return json.dumps(ld, cls=_DecimalEncoder)


def _asset_json_ld(symbol: str, score: dict) -> str:
    ld = {
        "@context": "https://schema.org",
        "@type": "FinancialProduct",
        "name": f"Stablecoin Integrity Index: {symbol.upper()}",
        "additionalProperty": [
            {"@type": "PropertyValue", "name": "sii_score", "value": score.get("overall_score")},
            {"@type": "PropertyValue", "name": "grade", "value": score.get("grade")},
            {"@type": "PropertyValue", "name": "formula_version", "value": score.get("formula_version")},
        ],
    }
    return json.dumps(ld, cls=_DecimalEncoder)


def _assessment_json_ld(row: dict) -> str:
    ld = {
        "@context": "https://schema.org",
        "@type": "Event",
        "name": f"Assessment: {row.get('wallet_address', '')[:10]}...",
        "startDate": row["created_at"].isoformat() if hasattr(row.get("created_at"), "isoformat") else str(row.get("created_at")),
        "additionalProperty": [
            {"@type": "PropertyValue", "name": "trigger_type", "value": row.get("trigger_type")},
            {"@type": "PropertyValue", "name": "severity", "value": row.get("severity")},
            {"@type": "PropertyValue", "name": "content_hash", "value": row.get("content_hash")},
            {"@type": "PropertyValue", "name": "wallet_risk_score", "value": row.get("wallet_risk_score")},
        ],
    }
    return json.dumps(ld, cls=_DecimalEncoder)


def _pulse_json_ld(pulse_date: str, summary: dict) -> str:
    ld = {
        "@context": "https://schema.org",
        "@type": "Report",
        "name": f"Basis Protocol Daily Pulse: {pulse_date}",
        "datePublished": pulse_date,
        "additionalProperty": [
            {"@type": "PropertyValue", "name": "wallets_indexed", "value": summary.get("wallets_indexed")},
            {"@type": "PropertyValue", "name": "alerts_today", "value": summary.get("alerts_today")},
            {"@type": "PropertyValue", "name": "total_tracked", "value": summary.get("total_tracked")},
        ],
    }
    return json.dumps(ld, cls=_DecimalEncoder)


# --- Fallback HTML (when Jinja2 templates not available) ---

def _fallback_wallet_html(ctx: dict) -> str:
    addr = ctx["address"]
    risk = ctx["risk"]
    return f"""<!DOCTYPE html>
<html><head><title>Wallet {addr[:10]}... | Basis Protocol</title>
<script type="application/ld+json">{ctx['json_ld']}</script>
<link rel="alternate" type="application/json" href="/api/wallets/{addr}">
</head><body>
<h1>Wallet Risk Profile</h1>
<p><strong>{addr}</strong></p>
<p>Risk Score: {risk.get('risk_score', 'N/A')} ({risk.get('risk_grade', 'N/A')})</p>
<p>Concentration HHI: {risk.get('concentration_hhi', 'N/A')}</p>
<p>Total Value: ${risk.get('total_stablecoin_value', 0):,.2f}</p>
</body></html>"""


def _fallback_asset_html(ctx: dict) -> str:
    score = ctx["score"]
    return f"""<!DOCTYPE html>
<html><head><title>{ctx['symbol']} | Basis Protocol</title>
<script type="application/ld+json">{ctx['json_ld']}</script>
<link rel="alternate" type="application/json" href="/api/scores/{ctx['symbol'].lower()}">
</head><body>
<h1>{ctx['symbol']} Stablecoin Integrity Index</h1>
<p>Score: {score.get('overall_score', 'N/A')} ({score.get('grade', 'N/A')})</p>
</body></html>"""


def _fallback_assessment_html(ctx: dict) -> str:
    a = ctx["assessment"]
    return f"""<!DOCTYPE html>
<html><head><title>Assessment {str(a.get('id', ''))[:8]}... | Basis Protocol</title>
<script type="application/ld+json">{ctx['json_ld']}</script>
</head><body>
<h1>Assessment Event</h1>
<p>Wallet: {a.get('wallet_address', 'N/A')}</p>
<p>Trigger: {a.get('trigger_type', 'N/A')}</p>
<p>Severity: {a.get('severity', 'N/A')}</p>
<p>Risk Score: {a.get('wallet_risk_score', 'N/A')}</p>
<p>Content Hash: {a.get('content_hash', 'N/A')}</p>
</body></html>"""


def _fallback_pulse_html(ctx: dict) -> str:
    summary = ctx["summary"]
    return f"""<!DOCTYPE html>
<html><head><title>Pulse {ctx['pulse_date']} | Basis Protocol</title>
<script type="application/ld+json">{ctx['json_ld']}</script>
</head><body>
<h1>Daily Pulse: {ctx['pulse_date']}</h1>
<p>Wallets Indexed: {summary.get('wallets_indexed', 0)}</p>
<p>Alerts Today: {summary.get('alerts_today', 0)}</p>
<p>Total Tracked: ${summary.get('total_tracked', 0):,.2f}</p>
</body></html>"""
