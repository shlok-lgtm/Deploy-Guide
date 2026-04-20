"""
Incident pages — evidence artifacts for real-world risk events.

Exposes:
  GET  /api/incident/{slug}        → frozen snapshot JSON
  POST /api/incident-notify        → email capture
  GET  /incident/{slug}            → SSR shell with OG meta, falls through to SPA
  GET  /audits/{slug}              → rendered audit markdown (paper theme)

The /audits/{slug} handler reads a committed .md file from the audits/
directory and renders it with a minimal markdown-to-HTML converter.
No third-party markdown dependency is introduced.
"""

from __future__ import annotations

import html as html_lib
import logging
import os
import re
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, EmailStr, Field

from app.database import execute, fetch_one

logger = logging.getLogger(__name__)

router = APIRouter()

AUDITS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "audits"
)

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


@router.get("/api/incident/{slug}")
def get_incident(slug: str) -> dict:
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

    return {
        "slug": row["slug"],
        "event_date": str(row["event_date"]),
        "title": row["title"],
        "summary": row["summary"],
        "captured_at": row["captured_at"].isoformat() if row["captured_at"] else None,
        "components": row["components_json"],
        "metadata": row["metadata_json"],
    }


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
    return JSONResponse({"ok": True})


# =============================================================================
# Minimal markdown → HTML renderer, scoped to the audit file's syntax:
#   headings (# / ## / ### / ####), paragraphs, bold (**x**), italic (_x_ or *x*),
#   inline code (`x`), fenced code blocks (```lang ... ```), bullet/numbered
#   lists, GFM tables, blockquotes (>), hr (---), links [t](u).
# =============================================================================


_INLINE_CODE_RE = re.compile(r"`([^`]+)`")
_BOLD_RE = re.compile(r"\*\*([^*]+)\*\*")
_ITALIC_RE = re.compile(r"(?<![\w*])\*([^*\n]+)\*(?!\w)")
_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")


def _render_inline(text: str) -> str:
    """Apply inline transforms. Escapes first, then substitutes markers with HTML."""
    # Placeholder swap for inline code so underscores inside it survive.
    placeholders: list[str] = []

    def _stash(m: re.Match) -> str:
        placeholders.append(m.group(1))
        return f"\x00C{len(placeholders) - 1}\x00"

    text = _INLINE_CODE_RE.sub(_stash, text)
    text = html_lib.escape(text, quote=False)
    text = _BOLD_RE.sub(r"<strong>\1</strong>", text)
    text = _ITALIC_RE.sub(r"<em>\1</em>", text)
    text = _LINK_RE.sub(
        lambda m: f'<a href="{html_lib.escape(m.group(2), quote=True)}" '
        f'target="_blank" rel="noopener noreferrer">{m.group(1)}</a>',
        text,
    )

    def _unstash(m: re.Match) -> str:
        idx = int(m.group(1))
        return f'<code>{html_lib.escape(placeholders[idx], quote=False)}</code>'

    text = re.sub(r"\x00C(\d+)\x00", _unstash, text)
    return text


def _render_table(lines: list[str]) -> str:
    """lines is a group of pipe-table lines including header + separator + body."""
    if len(lines) < 2:
        return ""
    header_cells = [c.strip() for c in lines[0].strip().strip("|").split("|")]
    body = lines[2:]
    out = ['<div class="md-table-wrap"><table>']
    out.append("<thead><tr>")
    for c in header_cells:
        out.append(f"<th>{_render_inline(c)}</th>")
    out.append("</tr></thead><tbody>")
    for row in body:
        cells = [c.strip() for c in row.strip().strip("|").split("|")]
        out.append("<tr>")
        for c in cells:
            out.append(f"<td>{_render_inline(c)}</td>")
        out.append("</tr>")
    out.append("</tbody></table></div>")
    return "".join(out)


def _render_markdown(md: str) -> str:
    lines = md.splitlines()
    i = 0
    out: list[str] = []
    while i < len(lines):
        line = lines[i]

        # Fenced code block
        if line.startswith("```"):
            lang = line[3:].strip()
            j = i + 1
            while j < len(lines) and not lines[j].startswith("```"):
                j += 1
            code = "\n".join(lines[i + 1 : j])
            out.append(
                f'<pre><code class="lang-{html_lib.escape(lang, quote=True)}">'
                f'{html_lib.escape(code, quote=False)}</code></pre>'
            )
            i = j + 1
            continue

        # Horizontal rule
        if re.fullmatch(r"\s*---+\s*", line):
            out.append("<hr/>")
            i += 1
            continue

        # Heading — supports trailing {#custom-id} anchor marker (pandoc
        # convention). Without an explicit marker, no id is emitted.
        m = re.match(r"^(#{1,6})\s+(.*)$", line)
        if m:
            level = len(m.group(1))
            heading_text = m.group(2).rstrip()
            anchor_m = re.search(r"\s*\{#([A-Za-z][\w\-]*)\}\s*$", heading_text)
            if anchor_m:
                heading_id = anchor_m.group(1)
                heading_text = heading_text[: anchor_m.start()].rstrip()
                out.append(
                    f'<h{level} id="{html_lib.escape(heading_id, quote=True)}">'
                    f"{_render_inline(heading_text)}</h{level}>"
                )
            else:
                out.append(f"<h{level}>{_render_inline(heading_text)}</h{level}>")
            i += 1
            continue

        # Table (simple detection: a pipe line followed by a --- separator line)
        if "|" in line and i + 1 < len(lines) and re.match(
            r"^\s*\|?\s*:?-{3,}", lines[i + 1]
        ):
            j = i
            while j < len(lines) and "|" in lines[j] and lines[j].strip():
                j += 1
            out.append(_render_table(lines[i:j]))
            i = j
            continue

        # Blockquote
        if line.startswith(">"):
            j = i
            buf: list[str] = []
            while j < len(lines) and (lines[j].startswith(">") or lines[j].strip() == ""):
                if lines[j].startswith(">"):
                    buf.append(lines[j][1:].lstrip())
                else:
                    buf.append("")
                j += 1
            inner = _render_markdown("\n".join(buf))
            out.append(f"<blockquote>{inner}</blockquote>")
            i = j
            continue

        # Unordered list
        if re.match(r"^\s*[-*+]\s+", line):
            j = i
            items: list[str] = []
            while j < len(lines) and re.match(r"^\s*[-*+]\s+", lines[j]):
                items.append(re.sub(r"^\s*[-*+]\s+", "", lines[j]))
                j += 1
            out.append(
                "<ul>" + "".join(f"<li>{_render_inline(t)}</li>" for t in items) + "</ul>"
            )
            i = j
            continue

        # Ordered list
        if re.match(r"^\s*\d+\.\s+", line):
            j = i
            items = []
            while j < len(lines) and re.match(r"^\s*\d+\.\s+", lines[j]):
                items.append(re.sub(r"^\s*\d+\.\s+", "", lines[j]))
                j += 1
            out.append(
                "<ol>" + "".join(f"<li>{_render_inline(t)}</li>" for t in items) + "</ol>"
            )
            i = j
            continue

        # Blank
        if not line.strip():
            i += 1
            continue

        # Paragraph — gather lines until blank
        j = i
        buf = []
        while j < len(lines) and lines[j].strip() and not re.match(
            r"^(#{1,6}\s|```|\s*[-*+]\s+|\s*\d+\.\s+|>|\s*---+\s*)", lines[j]
        ):
            # Stop at table header
            if "|" in lines[j] and j + 1 < len(lines) and re.match(
                r"^\s*\|?\s*:?-{3,}", lines[j + 1]
            ):
                break
            buf.append(lines[j])
            j += 1
        if buf:
            out.append(f"<p>{_render_inline(' '.join(buf))}</p>")
        i = j

    return "\n".join(out)


# =============================================================================
# /audits/{slug} — rendered markdown page
# =============================================================================


_AUDIT_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9\-_]{1,96}[a-z0-9]$")


def render_audit_html(slug: str) -> str | None:
    """Read audits/{slug}.md and return a full HTML page. None if not found."""
    if not _AUDIT_SLUG_RE.match(slug):
        return None
    # Path safety: resolve + confirm inside AUDITS_DIR
    target = os.path.realpath(os.path.join(AUDITS_DIR, f"{slug}.md"))
    if not target.startswith(os.path.realpath(AUDITS_DIR) + os.sep):
        return None
    if not os.path.isfile(target):
        return None
    with open(target, "r", encoding="utf-8") as f:
        md = f.read()

    # Extract title from first H1 for <title>
    title_match = re.search(r"^#\s+(.*)$", md, re.MULTILINE)
    page_title = (
        html_lib.escape(title_match.group(1), quote=True)
        if title_match
        else "Basis audit"
    )

    body_html = _render_markdown(md)

    return _audit_page_shell(page_title, slug, body_html)


def _audit_page_shell(page_title: str, slug: str, body_html: str) -> str:
    """Wrap rendered markdown in the paper-theme HTML shell."""
    og_url = f"https://basisprotocol.xyz/audits/{slug}"
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{page_title} · Basis Protocol</title>
<meta name="description" content="Basis Protocol audit document." />
<meta property="og:type" content="article" />
<meta property="og:title" content="{page_title}" />
<meta property="og:url" content="{og_url}" />
<meta property="og:site_name" content="Basis Protocol" />
<meta name="twitter:card" content="summary" />
<meta name="twitter:title" content="{page_title}" />
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet" />
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  html, body {{ background: {T_PAPER}; color: {T_INK}; font-family: {T_SANS}; -webkit-font-smoothing: antialiased; }}
  body {{ overflow-x: hidden; }}
  .audit-shell {{ max-width: 860px; margin: 0 auto; padding: 32px 24px 80px; }}
  .audit-nav {{ display: flex; justify-content: space-between; align-items: center; padding-bottom: 18px; border-bottom: 1px solid {T_RULE_MID}; margin-bottom: 32px; font-family: {T_MONO}; font-size: 11px; color: {T_INK_LIGHT}; text-transform: uppercase; letter-spacing: 1px; }}
  .audit-nav a {{ color: {T_INK_LIGHT}; text-decoration: none; border-bottom: 1px solid {T_RULE_MID}; padding-bottom: 2px; }}
  .audit-nav a:hover {{ color: {T_INK}; }}
  .audit-content h1 {{ font-size: 28px; font-weight: 700; margin: 12px 0 18px; line-height: 1.22; }}
  .audit-content h2 {{ font-size: 20px; font-weight: 600; margin: 36px 0 14px; padding-top: 14px; border-top: 1px solid {T_RULE_MID}; line-height: 1.3; }}
  .audit-content h3 {{ font-size: 15px; font-weight: 600; margin: 24px 0 10px; color: {T_INK_MID}; font-family: {T_MONO}; text-transform: uppercase; letter-spacing: 0.5px; }}
  .audit-content h4 {{ font-size: 13px; font-weight: 600; margin: 18px 0 8px; color: {T_INK_MID}; }}
  .audit-content p {{ margin: 0 0 14px; font-size: 15px; line-height: 1.65; color: {T_INK_MID}; }}
  .audit-content strong {{ color: {T_INK}; font-weight: 600; }}
  .audit-content em {{ font-style: italic; }}
  .audit-content ul, .audit-content ol {{ margin: 0 0 16px 22px; font-size: 15px; line-height: 1.65; color: {T_INK_MID}; }}
  .audit-content li {{ margin-bottom: 4px; }}
  .audit-content a {{ color: {T_INK}; text-decoration: none; border-bottom: 1px solid {T_RULE_MID}; }}
  .audit-content a:hover {{ border-bottom-color: {T_INK}; }}
  .audit-content hr {{ border: none; border-top: 1px solid {T_RULE_MID}; margin: 32px 0; }}
  .audit-content code {{ font-family: {T_MONO}; font-size: 0.9em; background: {T_PAPER_WARM}; padding: 1px 5px; border: 1px solid {T_RULE_LIGHT}; }}
  .audit-content pre {{ background: {T_PAPER_WARM}; border: 1px solid {T_RULE_MID}; padding: 14px 16px; overflow-x: auto; margin: 0 0 18px; }}
  .audit-content pre code {{ background: transparent; border: none; padding: 0; font-size: 12px; line-height: 1.5; color: {T_INK}; }}
  .audit-content blockquote {{ border-left: 3px solid {T_INK}; padding: 4px 0 4px 16px; margin: 18px 0; color: {T_INK_MID}; }}
  .audit-content blockquote p {{ margin-bottom: 8px; }}
  .audit-content blockquote p:last-child {{ margin-bottom: 0; }}
  .md-table-wrap {{ overflow-x: auto; margin: 0 0 20px; border: 1px solid {T_RULE_MID}; }}
  .audit-content table {{ width: 100%; border-collapse: collapse; font-size: 13px; font-family: {T_SANS}; }}
  .audit-content th {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid {T_RULE_MID}; background: {T_PAPER_WARM}; font-family: {T_MONO}; font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: {T_INK_LIGHT}; }}
  .audit-content td {{ padding: 10px 12px; border-bottom: 1px dotted {T_RULE_MID}; vertical-align: top; color: {T_INK_MID}; line-height: 1.55; }}
  .audit-content tr:last-child td {{ border-bottom: none; }}
  .audit-foot {{ margin-top: 48px; padding-top: 18px; border-top: 1px solid {T_RULE_MID}; font-family: {T_MONO}; font-size: 10px; color: {T_INK_FAINT}; text-transform: uppercase; letter-spacing: 1px; }}
  @media (max-width: 640px) {{
    .audit-shell {{ padding: 16px 14px 64px; }}
    .audit-content h1 {{ font-size: 22px; }}
    .audit-content h2 {{ font-size: 17px; }}
    .audit-content p, .audit-content ul, .audit-content ol {{ font-size: 14px; }}
    .audit-content table {{ font-size: 12px; }}
  }}
</style>
</head>
<body>
<main class="audit-shell">
  <div class="audit-nav">
    <a href="/">← Basis Protocol</a>
    <span>AUDIT · {html_lib.escape(slug.upper(), quote=False)}</span>
  </div>
  <article class="audit-content">
    {body_html}
  </article>
  <div class="audit-foot">
    Source of record: <code>audits/{html_lib.escape(slug, quote=False)}.md</code>
  </div>
</main>
</body>
</html>
"""


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
