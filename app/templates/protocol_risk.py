"""
Protocol Risk Report Template
===============================
PSI score + stablecoin exposure + CQI composition.
"""

from app.templates._html import (
    page, section, score_header, attestation_footer,
    table, proof_link, fmt_usd, CANONICAL_BASE_URL,
)


def render(report_data: dict, lens_result: dict = None,
           report_hash: str = "", timestamp: str = "", format: str = "html") -> str:
    """Render protocol risk report as HTML."""
    d = report_data
    name = d.get("name", d.get("entity_id", "Unknown"))
    score = d.get("score")

    body = f'<p class="meta">Protocol Risk Report · {name} · {timestamp}</p>'
    body += score_header(name, score, subtitle=f"PSI {d.get('formula_version', '')}")

    # Lens classification (if provided)
    if lens_result:
        body += _render_lens_section(lens_result)

    # Category breakdown
    cat_scores = d.get("category_scores") or {}
    if cat_scores:
        rows = []
        for cat_id, cat_score in sorted(cat_scores.items()):
            s = f"{float(cat_score):.1f}" if cat_score is not None else "—"
            bar_w = int(float(cat_score or 0) * 1.5)
            rows.append([cat_id.replace("_", " ").title(), s,
                         f'<span class="bar" style="width:{bar_w}px"></span>'])
        body += section("Category Breakdown", table(["Category", "Score", ""], rows, [1]))

    # Stablecoin exposure
    exposure = d.get("exposure") or []
    if exposure:
        rows = []
        for e in exposure:
            s = f"{float(e['sii_score']):.1f}" if e.get("sii_score") else "—"
            link = proof_link(f"/proof/sii/{e.get('stablecoin_id', '')}")
            amt = fmt_usd(e.get("exposure_usd"))
            rows.append([e.get("symbol", "?"), e.get("name", ""), amt, s, link])
        body += section("Stablecoin Exposure",
                        table(["Symbol", "Name", "Exposure", "SII", "Proof"],
                              rows, [2, 3]))

    # CQI composition
    cqi_pairs = d.get("cqi_pairs") or []
    if cqi_pairs:
        rows = []
        for p in cqi_pairs:
            cqi_s = f"{float(p['cqi_score']):.1f}" if p.get("cqi_score") else "—"
            sii_s = f"{float(p['sii_score']):.1f}" if p.get("sii_score") else "—"
            psi_s = f"{float(p['psi_score']):.1f}" if p.get("psi_score") else "—"
            link = proof_link(p.get("proof_url", ""))
            rows.append([p.get("asset", "?"), sii_s, psi_s, cqi_s, link])
        body += section("Collateral Quality Index (CQI)",
                        '<p class="meta">Geometric mean of SII and PSI scores per stablecoin held.</p>' +
                        table(["Asset", "SII", "PSI", "CQI", "Proof"], rows, [1, 2, 3]))

    # Evidence links
    evidence = f'<a href="{d.get("proof_url", "#")}" style="color:#0B090A">PSI Proof page</a><br>'
    evidence += f'<a href="/witness" style="color:#0B090A">Witness — issuer evidence</a>'
    body += section("Evidence", evidence)

    body += attestation_footer(report_hash, d.get("formula_version", ""),
                               timestamp, lens_result.get("lens_id") if lens_result else None,
                               lens_result.get("lens_version") if lens_result else None)

    return page(f"{name} — Protocol Risk Report", body,
                f"Protocol risk report for {name}. PSI {score:.1f}/100." if score else f"Protocol risk report for {name}.",
                f"{CANONICAL_BASE_URL}/report/protocol/{d.get('entity_id', '')}")


def _render_lens_section(lens_result: dict) -> str:
    framework = lens_result.get("framework", "")
    overall = lens_result.get("overall_pass", False)
    status_cls = "pass" if overall else "fail"
    status_label = "PASS" if overall else "FAIL"

    html = f'<div class="section"><h3>Regulatory Classification — {framework}</h3>'
    html += f'<p>Overall: <span class="{status_cls}" style="font-weight:700">{status_label}</span></p>'

    for group_id, group in (lens_result.get("classification") or {}).items():
        html += f'<p style="font-weight:600;margin-bottom:4px">{group_id.replace("_", " ").title()}</p>'
        for c in group.get("criteria", []):
            pill = "pill-pass" if c["passed"] else "pill-fail"
            label = "PASS" if c["passed"] else "FAIL"
            html += f'<div style="margin:2px 0"><span class="pill {pill}">{label}</span> {c["name"]}'
            if c.get("threshold"):
                html += f' <span class="meta">(threshold: {c["threshold"]})</span>'
            html += '</div>'

    html += '</div>'
    return html
