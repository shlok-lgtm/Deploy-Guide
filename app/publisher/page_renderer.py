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

        holdings_raw = fetch_all("""
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

        # Filter dust holdings (display only — scoring still sees all)
        MIN_DISPLAY_VALUE_USD = 0.01
        holdings = [h for h in holdings_raw if float(h.get("value_usd") or 0) >= MIN_DISPLAY_VALUE_USD]

        # Compute display values from actual current holdings
        holdings_total_value = sum(float(h.get("value_usd") or 0) for h in holdings)
        risk_table_value = float(risk.get("total_stablecoin_value") or 0)
        display_total_value = holdings_total_value if holdings_total_value > 0 else risk_table_value
        value_discrepancy = (
            abs(risk_table_value - holdings_total_value) > max(risk_table_value * 0.1, 1000)
            if risk_table_value > 0 else False
        )

        # Recompute size tier from current holdings
        if display_total_value >= 10_000_000:
            display_size_tier = "whale"
        elif display_total_value >= 100_000:
            display_size_tier = "institutional"
        else:
            display_size_tier = "retail"

        recent_assessments = fetch_all("""
            SELECT id::text, created_at, trigger_type, severity,
                   wallet_risk_score, wallet_risk_grade
            FROM assessment_events
            WHERE wallet_address = %s
            ORDER BY created_at DESC LIMIT 10
        """, (address.lower(),))

        # Full profile: behavioral signals + quality history
        profile = None
        try:
            from app.wallet_profile import generate_wallet_profile
            profile = generate_wallet_profile(address)
        except Exception as e:
            logger.warning(f"Profile generation failed for {address[:12]}...: {e}")

        # Cross-chain unified profile
        unified = fetch_one(
            "SELECT * FROM wallet_graph.wallet_profiles WHERE LOWER(address) = LOWER(%s)",
            (address.lower(),)
        )

        # Top connections by edge weight
        connections = fetch_all("""
            SELECT
                CASE WHEN from_address = %s THEN to_address ELSE from_address END AS counterparty,
                weight, total_value_usd
            FROM wallet_graph.wallet_edges
            WHERE from_address = %s OR to_address = %s
            ORDER BY weight DESC
            LIMIT 5
        """, (address.lower(), address.lower(), address.lower()))

        context = {
            "active_tab": "wallets",
            "address": address.lower(),
            "risk": dict(risk),
            "holdings": [dict(h) for h in holdings],
            "assessments": [dict(a) for a in recent_assessments],
            "profile": profile,
            "unified": dict(unified) if unified else None,
            "connections": [dict(c) for c in connections],
            "json_ld": _wallet_json_ld(address.lower(), risk, holdings, profile),
            "display_total_value": display_total_value,
            "holdings_total_value": holdings_total_value,
            "risk_table_value": risk_table_value,
            "value_discrepancy": value_discrepancy,
            "display_size_tier": display_size_tier,
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
            "active_tab": "stablecoins",
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
            "active_tab": "",
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
            "active_tab": "",
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
        urls = [
            f"{CANONICAL_BASE_URL}/",
            f"{CANONICAL_BASE_URL}/witness",
        ]

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
            "Allow: /\n"
            "Allow: /witness\n"
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

def _wallet_json_ld(address: str, risk: dict, holdings: list, profile: dict = None) -> str:
    props = [
        {"@type": "PropertyValue", "name": "risk_score", "value": risk.get("risk_score")},
        {"@type": "PropertyValue", "name": "risk_grade", "value": risk.get("risk_grade")},
        {"@type": "PropertyValue", "name": "concentration_hhi", "value": risk.get("concentration_hhi")},
        {"@type": "PropertyValue", "name": "total_value", "value": risk.get("total_stablecoin_value")},
        {"@type": "PropertyValue", "name": "holdings_count", "value": len(holdings)},
    ]
    if profile:
        bs = profile.get("behavioral_signals", {})
        qh = profile.get("quality_history", {})
        if bs.get("days_tracked") is not None:
            props.append({"@type": "PropertyValue", "name": "days_tracked", "value": bs["days_tracked"]})
        if bs.get("score_stability_30d") is not None:
            props.append({"@type": "PropertyValue", "name": "score_stability_30d", "value": bs["score_stability_30d"]})
        if qh.get("pct_days_a_grade") is not None:
            props.append({"@type": "PropertyValue", "name": "pct_days_a_grade", "value": qh["pct_days_a_grade"]})
        if profile.get("profile_hash"):
            props.append({"@type": "PropertyValue", "name": "profile_hash", "value": profile["profile_hash"]})
    ld = {
        "@context": "https://schema.org",
        "@type": "FinancialProduct",
        "name": f"Wallet Risk Profile: {address[:10]}...{address[-4:]}",
        "description": f"Stablecoin risk profile for Ethereum wallet {address}",
        "additionalProperty": props,
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

_FALLBACK_STYLE = (
    "font-family:'IBM Plex Sans',system-ui,sans-serif;max-width:800px;"
    "margin:0 auto;padding:20px;background:#f5f2ec;color:#0a0a0a"
)
_FALLBACK_LABEL = (
    "font-family:'IBM Plex Mono',monospace;font-size:9px;"
    "text-transform:uppercase;letter-spacing:1.5px;color:#6a6a6a"
)
_FALLBACK_MONO = "font-family:'IBM Plex Mono',monospace;font-size:11px;color:#3a3a3a"


def _fallback_wallet_html(ctx: dict) -> str:
    addr = ctx["address"]
    risk = ctx["risk"]
    profile = ctx.get("profile")
    parts = [
        f"""<!DOCTYPE html>
<html><head><title>Wallet {addr[:10]}... | Basis Protocol</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;700&display=swap" rel="stylesheet">
<script type="application/ld+json">{ctx['json_ld']}</script>
<link rel="alternate" type="application/json" href="/api/wallets/{addr}">
</head><body style="{_FALLBACK_STYLE}">
<p style="{_FALLBACK_LABEL}">Wallet Risk Profile</p>
<p style="{_FALLBACK_MONO};word-break:break-all">{addr}</p>
<p style="{_FALLBACK_MONO}">Risk Score: {risk.get('risk_score', 'N/A')} ({risk.get('risk_grade', 'N/A')})</p>
<p style="{_FALLBACK_MONO}">Concentration HHI: {risk.get('concentration_hhi', 'N/A')}</p>
<p style="{_FALLBACK_MONO}">Total Value: ${risk.get('total_stablecoin_value', 0):,.2f}</p>""",
    ]
    if profile:
        bs = profile.get("behavioral_signals", {})
        qh = profile.get("quality_history", {})
        parts.append(f'<p style="{_FALLBACK_LABEL};margin-top:24px">Behavioral Signals</p>')
        parts.append(f"<p style=\"{_FALLBACK_MONO}\">Days Tracked: {bs.get('days_tracked', '—')} · "
                     f"Score Stability (30d): {bs.get('score_stability_30d', '—')} · "
                     f"Avg Score (30d): {bs.get('avg_score_30d', '—')}</p>")
        parts.append(f'<p style="{_FALLBACK_LABEL};margin-top:16px">Quality History</p>')
        parts.append(f"<p style=\"{_FALLBACK_MONO}\">A Grade: {qh.get('pct_days_a_grade', '—')}% · "
                     f"Best: {qh.get('best_score_ever', '—')} · "
                     f"Worst: {qh.get('worst_score_ever', '—')}</p>")
        if profile.get("profile_hash"):
            parts.append(f"<p style='{_FALLBACK_MONO};font-size:10px;color:#9a9a9a'>Hash: {profile['profile_hash']}</p>")
    parts.append("</body></html>")
    return "\n".join(parts)


def _fallback_asset_html(ctx: dict) -> str:
    score = ctx["score"]
    return f"""<!DOCTYPE html>
<html><head><title>{ctx['symbol']} | Basis Protocol</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;700&display=swap" rel="stylesheet">
<script type="application/ld+json">{ctx['json_ld']}</script>
<link rel="alternate" type="application/json" href="/api/scores/{ctx['symbol'].lower()}">
</head><body style="{_FALLBACK_STYLE}">
<p style="{_FALLBACK_LABEL}">Stablecoin Integrity Index</p>
<p style="font-family:'IBM Plex Mono',monospace;font-size:18px;font-weight:700">{ctx['symbol']}</p>
<p style="{_FALLBACK_MONO}">Score: {score.get('overall_score', 'N/A')} ({score.get('grade', 'N/A')})</p>
</body></html>"""


def _fallback_assessment_html(ctx: dict) -> str:
    a = ctx["assessment"]
    return f"""<!DOCTYPE html>
<html><head><title>Assessment {str(a.get('id', ''))[:8]}... | Basis Protocol</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;700&display=swap" rel="stylesheet">
<script type="application/ld+json">{ctx['json_ld']}</script>
</head><body style="{_FALLBACK_STYLE}">
<p style="{_FALLBACK_LABEL}">Assessment Event</p>
<p style="{_FALLBACK_MONO}">Wallet: {a.get('wallet_address', 'N/A')}</p>
<p style="{_FALLBACK_MONO}">Trigger: {a.get('trigger_type', 'N/A')} · Severity: {a.get('severity', 'N/A')}</p>
<p style="{_FALLBACK_MONO}">Risk Score: {a.get('wallet_risk_score', 'N/A')}</p>
<p style="{_FALLBACK_MONO};font-size:10px;color:#9a9a9a">Content Hash: {a.get('content_hash', 'N/A')}</p>
</body></html>"""


def _fallback_pulse_html(ctx: dict) -> str:
    summary = ctx["summary"]
    return f"""<!DOCTYPE html>
<html><head><title>Pulse {ctx['pulse_date']} | Basis Protocol</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;700&display=swap" rel="stylesheet">
<script type="application/ld+json">{ctx['json_ld']}</script>
</head><body style="{_FALLBACK_STYLE}">
<p style="{_FALLBACK_LABEL}">Daily Pulse</p>
<p style="font-family:'IBM Plex Mono',monospace;font-size:14px;font-weight:600">{ctx['pulse_date']}</p>
<p style="{_FALLBACK_MONO}">Wallets Indexed: {summary.get('wallets_indexed', 0)}</p>
<p style="{_FALLBACK_MONO}">Alerts Today: {summary.get('alerts_today', 0)}</p>
<p style="{_FALLBACK_MONO}">Total Tracked: ${summary.get('total_tracked', 0):,.2f}</p>
</body></html>"""
