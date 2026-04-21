"""
Incident pages — evidence artifacts for real-world risk events.

Exposes:
  GET  /api/incident/{slug}        → frozen snapshot JSON
  POST /api/incident-notify        → email capture
  GET  /incident/{slug}            → SSR shell with OG meta, falls through to SPA

Audit markdown used to render at /audits/{slug}; the route was removed
when audits/ moved to audits/internal/ as non-public QA records. The
SPA-catchall now simply serves index.html for unmatched paths.
"""

from __future__ import annotations

import html as html_lib
import logging
import re

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, Field

from app.database import execute, fetch_one

logger = logging.getLogger(__name__)

router = APIRouter()

# Paper theme tokens — kept in sync with frontend/src/App.jsx:8-20
T_PAPER = "#f5f2ec"
T_PAPER_WARM = "#f0ece3"
T_INK = "#0a0a0a"
T_INK_MID = "#3a3a3a"
T_INK_LIGHT = "#6a6a6a"
T_INK_FAINT = "#9a9a9a"
T_RULE_MID = "#c8c4bc"
T_RULE_LIGHT = "#e0ddd6"
T_SANS = "'IBM Plex Sans', system-ui, sans-serif"
T_MONO = "'IBM Plex Mono', monospace"


# =============================================================================
# API: incident snapshot JSON
# =============================================================================


# Response headers for endpoints that serve DB- or file-backed state that
# can change on deploy/populator runs. Kept in one place so every endpoint
# below uses the same values; prevents CDNs (Fastly/Cloudflare/etc.) from
# serving stale snapshot data after a populator run.
NO_CACHE_HEADERS = {
    "Cache-Control": "no-cache, no-store, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


@router.get("/api/incident/{slug}")
def get_incident(slug: str) -> JSONResponse:
    row = fetch_one(
        """
        SELECT slug, event_date, title, summary, captured_at,
               components_json, metadata_json
        FROM incident_snapshots
        WHERE slug = %s
        """,
        (slug,),
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Unknown incident: {slug}")

    return JSONResponse(
        content={
            "slug": row["slug"],
            "event_date": str(row["event_date"]),
            "title": row["title"],
            "summary": row["summary"],
            "captured_at": row["captured_at"].isoformat() if row["captured_at"] else None,
            "components": row["components_json"],
            "metadata": row["metadata_json"],
        },
        headers=NO_CACHE_HEADERS,
    )


# =============================================================================
# API: email capture
# =============================================================================


class NotifyPayload(BaseModel):
    email: EmailStr
    source: str = Field(..., max_length=64)


@router.post("/api/incident-notify")
def incident_notify(payload: NotifyPayload) -> JSONResponse:
    # Simple source sanitization — only allow slug-style strings to prevent
    # arbitrary-label pollution of the table.
    if not re.fullmatch(r"[a-z0-9][a-z0-9\-\_]{1,62}[a-z0-9]", payload.source):
        raise HTTPException(status_code=400, detail="Invalid source")

    try:
        execute(
            """
            INSERT INTO incident_subscribers (email, source)
            VALUES (%s, %s)
            ON CONFLICT (email, source) DO NOTHING
            """,
            (str(payload.email).lower(), payload.source),
        )
    except Exception as e:
        # Silent 200 — don't expose DB state. Log for ops.
        logger.warning(f"incident-notify insert failed: {e}")

    # Always return 200 silently per spec; never gate content behind email.
    return JSONResponse({"ok": True}, headers=NO_CACHE_HEADERS)


# =============================================================================
# /incident/{slug} SSR shell — injects OG meta for link unfurlers, then
# falls through to the SPA for interactive rendering. The SPA reads the
# same snapshot from /api/incident/{slug}.
# =============================================================================


def render_incident_ssr_shell(slug: str, index_html: str) -> str:
    """Inject OG meta + document title into the index.html shell for a given incident slug.

    Values come from the incident_snapshots row, so link unfurlers see the
    right title/description/image even though the page itself is SPA-rendered.
    """
    row = fetch_one(
        "SELECT title, summary FROM incident_snapshots WHERE slug = %s",
        (slug,),
    )
    title = (
        row["title"]
        if row
        else "Basis Protocol — Incident"
    )
    summary = (
        row["summary"]
        if row
        else "Evidence page for an on-chain incident."
    )
    og_image = f"/share/incident/{slug}.png"
    og_url = f"https://basisprotocol.xyz/incident/{slug}"

    meta_block = (
        f'<title>{html_lib.escape(title, quote=True)} · Basis Protocol</title>\n'
        f'<meta name="description" content="{html_lib.escape(summary, quote=True)}" />\n'
        f'<meta property="og:type" content="article" />\n'
        f'<meta property="og:title" content="{html_lib.escape(title, quote=True)}" />\n'
        f'<meta property="og:description" content="{html_lib.escape(summary, quote=True)}" />\n'
        f'<meta property="og:url" content="{og_url}" />\n'
        f'<meta property="og:image" content="{og_image}" />\n'
        f'<meta property="og:site_name" content="Basis Protocol" />\n'
        f'<meta name="twitter:card" content="summary_large_image" />\n'
        f'<meta name="twitter:title" content="{html_lib.escape(title, quote=True)}" />\n'
        f'<meta name="twitter:description" content="{html_lib.escape(summary, quote=True)}" />\n'
        f'<meta name="twitter:image" content="{og_image}" />\n'
    )

    # Replace the first <title> tag (or inject after <head>).
    if "<title>" in index_html:
        html_out = re.sub(
            r"<title>.*?</title>",
            meta_block.strip(),
            index_html,
            count=1,
            flags=re.DOTALL,
        )
    else:
        html_out = index_html.replace("<head>", "<head>\n" + meta_block, 1)
    return html_out
