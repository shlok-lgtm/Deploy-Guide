"""
Underwriting Report Template
==============================
PSI temporal behavior, coverage recommendations,
CQI composition risk analysis.
"""

from app.templates._html import (
    page, section, score_header, attestation_footer,
    table, fmt_usd, CANONICAL_BASE_URL,
)


def render(report_data: dict, lens_result: dict = None,
           report_hash: str = "", timestamp: str = "", format: str = "html") -> str:
    d = report_data
    name = d.get("name", d.get("entity_id", "?"))
    score = d.get("score")

    body = f'<p class="meta">Underwriting Report · {name} · {timestamp}</p>'
    body += score_header(name, score, subtitle=d.get("formula_version", ""))

    # PSI category analysis
    cat_scores = d.get("category_scores") or {}
    if cat_scores:
        rows = []
        weak_cats = []
        for cat_id, cat_score in sorted(cat_scores.items()):
            s = float(cat_score) if cat_score is not None else 0
            risk = "Low" if s >= 70 else "Medium" if s >= 50 else "High"
            risk_cls = "src-live" if s >= 70 else "meta" if s >= 50 else "fail"
            rows.append([cat_id.replace("_", " ").title(), f"{s:.1f}",
                         f'<span class="{risk_cls}">{risk}</span>'])
            if s < 50:
                weak_cats.append(cat_id.replace("_", " ").title())
        body += section("Risk Factor Analysis", table(["Category", "Score", "Risk Level"], rows, [1]))

        if weak_cats:
            body += f'<div class="section" style="border-color:#c0392b"><h3 style="color:#c0392b">Attention Areas</h3>'
            body += '<ul>' + ''.join(f'<li>{c}</li>' for c in weak_cats) + '</ul>'
            body += '<p class="meta">Categories scoring below 50 warrant additional due diligence.</p></div>'

    # CQI composition risk
    cqi_pairs = d.get("cqi_pairs") or []
    if cqi_pairs:
        rows = []
        for p in cqi_pairs:
            cqi = float(p.get("cqi_score") or 0)
            sii = float(p.get("sii_score") or 0)
            risk = "Low" if cqi >= 70 else "Medium" if cqi >= 50 else "Elevated"
            rows.append([
                p.get("asset", "?"),
                f"{sii:.1f}",
                f"{cqi:.1f}",
                risk,
            ])
        body += section("Collateral Quality Risk",
                        '<p class="meta">CQI combines asset quality (SII) with protocol risk (PSI). '
                        'Low CQI on any collateral pair is a concentration risk.</p>' +
                        table(["Collateral", "SII", "CQI", "Risk"], rows, [1, 2]))

    # Coverage recommendations
    exposure = d.get("exposure") or []
    unscored_exposure = [e for e in exposure if not e.get("sii_score")]
    if unscored_exposure:
        items = ", ".join(e.get("symbol", "?") for e in unscored_exposure)
        body += section("Coverage Gaps",
                        f'<p>{len(unscored_exposure)} collateral asset(s) lack SII scores: {items}</p>'
                        '<p class="meta">Unscored collateral cannot be risk-weighted. '
                        'Consider requesting SII coverage or treating as maximum risk weight.</p>')

    # Evidence
    body += section("Evidence",
                    f'<a href="{d.get("proof_url", "#")}" style="color:#0B090A">Score Proof</a><br>'
                    f'<a href="/witness" style="color:#0B090A">Witness — evidence chain</a><br>'
                    f'<a href="/api/compose/cqi/matrix" style="color:#0B090A">Full CQI Matrix (JSON)</a>')

    body += attestation_footer(report_hash, d.get("formula_version", ""), timestamp)

    return page(f"{name} — Underwriting Report", body,
                f"Underwriting risk analysis for {name}.",
                f"{CANONICAL_BASE_URL}/report/protocol/{d.get('entity_id', '')}")
