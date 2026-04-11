"""
Shared HTML rendering utilities for report templates.
Matches Witness/Proof design language: Georgia serif, #F3F2ED, monospace data.
"""

import os

CANONICAL_BASE_URL = os.environ.get("CANONICAL_BASE_URL", "https://basisprotocol.xyz").rstrip("/")

CSS = """
body { font-family: 'Georgia', serif; max-width: 960px; margin: 0 auto; padding: 24px 20px; background: #F3F2ED; color: #0B090A; line-height: 1.6; }
h1 { font-family: 'Georgia', serif; font-size: 1.6rem; font-weight: 400; letter-spacing: -0.3px; margin-bottom: 4px; }
h3 { font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1.5px; color: #6a6a6a; margin: 0 0 12px; }
.meta { font-family: 'IBM Plex Mono', monospace; font-size: 0.75rem; color: #6a6a6a; margin-bottom: 6px; }
nav { margin-bottom: 24px; font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }
nav a { color: #6a6a6a; text-decoration: none; margin-right: 16px; border-bottom: 1px solid #c8c4bc; }
nav a:hover { color: #0B090A; }
.section { border: 1px solid #ccc; padding: 16px 20px; margin-bottom: 20px; }
table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
th { text-align: left; padding: 6px 8px; border-bottom: 2px solid #0B090A; font-family: 'IBM Plex Mono', monospace; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 1px; color: #6a6a6a; }
td { padding: 6px 8px; border-bottom: 1px dotted #ccc; }
.num { font-family: 'IBM Plex Mono', monospace; text-align: right; }
.score { font-family: 'IBM Plex Mono', monospace; font-size: 1.8rem; font-weight: 700; }
code { font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; background: #e8e6e0; padding: 1px 4px; border-radius: 2px; }
footer { margin-top: 32px; font-family: 'IBM Plex Mono', monospace; font-size: 0.75rem; color: #6a6a6a; border-top: 1px solid #ccc; padding-top: 12px; }
.pass { color: #2d6b45; } .fail { color: #c0392b; }
.src-live { color: #2d6b45; } .src-cda { color: #6b5b2d; } .src-static { color: #9a9a9a; }
.bar { display: inline-block; height: 8px; background: #0B090A; border-radius: 1px; }
.pill { font-family: 'IBM Plex Mono', monospace; font-size: 0.65rem; padding: 2px 6px; border-radius: 2px; display: inline-block; }
.pill-pass { background: rgba(45,107,69,0.1); color: #2d6b45; }
.pill-fail { background: rgba(192,57,43,0.1); color: #c0392b; }
@media (max-width: 600px) { body { padding: 12px 10px; } .score { font-size: 1.4rem; } }
"""


def page(title: str, body: str, description: str = "", canonical: str = "",
         report_hash: str = "", timestamp: str = "") -> str:
    """Wrap body content in a full HTML page."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} — Basis Protocol</title>
<meta name="description" content="{description}">
<meta property="og:title" content="{title} — Basis Protocol">
{f'<link rel="canonical" href="{canonical}">' if canonical else ''}
<style>{CSS}</style>
</head>
<body>
<h1>Basis Protocol</h1>
<nav>
<a href="/">Rankings</a>
<a href="/witness">Witness</a>
<a href="/developers">API</a>
</nav>
{body}
</body>
</html>"""


def section(title: str, content: str) -> str:
    return f'<div class="section"><h3>{title}</h3>{content}</div>'


def score_header(name: str, score, grade: str = "", subtitle: str = "") -> str:
    """Render score header. Grade parameter kept for backward compatibility but no longer displayed."""
    s = f"{float(score):.1f}" if score is not None else "—"
    return f"""<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:8px">
<div><span style="font-size:1.4rem;font-weight:600">{name}</span>
{f'<br><span class="meta">{subtitle}</span>' if subtitle else ''}</div>
<div><span class="score">{s}</span></div>
</div>"""


def attestation_footer(report_hash: str, methodology_version: str,
                       timestamp: str, lens: str = None, lens_version: str = None) -> str:
    parts = [
        f"Report hash: <code>{report_hash}</code>",
        f'Verify: <a href="/api/reports/verify/{report_hash}">{CANONICAL_BASE_URL}/api/reports/verify/{report_hash}</a>',
        f"Generated: {timestamp}",
        f"Methodology: {methodology_version}",
    ]
    if lens:
        parts.append(f"Lens: {lens} v{lens_version or '1.0'}")
    return "<footer>" + "<br>".join(parts) + "<br>Basis Protocol</footer>"


def table(headers: list[str], rows: list[list[str]], num_cols: list[int] = None) -> str:
    """Render an HTML table."""
    num_cols = num_cols or []
    h = "<table><thead><tr>"
    for i, header in enumerate(headers):
        cls = ' class="num"' if i in num_cols else ""
        h += f"<th{cls}>{header}</th>"
    h += "</tr></thead><tbody>"
    for row in rows:
        h += "<tr>"
        for i, cell in enumerate(row):
            cls = ' class="num"' if i in num_cols else ""
            h += f"<td{cls}>{cell}</td>"
        h += "</tr>"
    h += "</tbody></table>"
    return h


def proof_link(url: str, label: str = "Proof") -> str:
    if not url:
        return "—"
    return f'<a href="{url}" style="color:#6a6a6a;font-size:0.75rem">{label}</a>'


def grade_color(grade: str) -> str:
    """Deprecated: grade display has been removed for legal reasons. Kept for backward compatibility."""
    if not grade:
        return "#6a6a6a"
    g = grade[0]
    if g == "A":
        return "#2d6b45"
    if g == "B":
        return "#3a6b2d"
    if g == "C":
        return "#6b5b2d"
    if g == "D":
        return "#6b3a2d"
    return "#c0392b"


def fmt_usd(val) -> str:
    if val is None:
        return "—"
    v = float(val)
    if v >= 1e9:
        return f"${v/1e9:.1f}B"
    if v >= 1e6:
        return f"${v/1e6:.0f}M"
    if v >= 1e3:
        return f"${v/1e3:.0f}K"
    return f"${v:,.0f}"
