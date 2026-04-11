"""
Compliance Report Template
============================
Requires a regulatory lens. Per-stablecoin classification with
criterion-level pass/fail, evidence trail, methodology docs.
"""

from app.templates._html import (
    page, section, score_header, attestation_footer,
    table, proof_link, CANONICAL_BASE_URL,
)


def render(report_data: dict, lens_result: dict = None,
           report_hash: str = "", timestamp: str = "", format: str = "html") -> str:
    d = report_data
    entity_id = d.get("entity_id", "?")
    name = d.get("name") or d.get("symbol") or entity_id
    score = d.get("score")

    if not lens_result:
        return page("Compliance Report — Lens Required",
                     '<div class="section"><p>Compliance reports require a regulatory lens. '
                     'Specify ?lens=SCO60, ?lens=MiCA67, or ?lens=GENIUS.</p></div>',
                     "Compliance report requires a regulatory lens.")

    framework = lens_result.get("framework", "Unknown")
    lens_id = lens_result.get("lens_id", "")
    lens_version = lens_result.get("lens_version", "1.0")
    overall = lens_result.get("overall_pass", False)

    body = f'<p class="meta">Compliance Report · {name} · {framework} · {timestamp}</p>'
    body += score_header(name, score, subtitle=f"Lens: {lens_id} v{lens_version}")

    # Classification header
    status = "ELIGIBLE" if overall else "NOT ELIGIBLE"
    status_cls = "pass" if overall else "fail"
    body += f'<div class="section"><h3>Regulatory Classification</h3>'
    body += f'<p style="font-size:1.1rem">Classification: <span class="{status_cls}" style="font-weight:700;font-size:1.2rem">{status}</span></p>'
    body += f'<p class="meta">{framework}</p>'

    # Per-criterion breakdown
    for group_id, group in (lens_result.get("classification") or {}).items():
        body += f'<h3 style="margin-top:16px">{group_id.replace("_", " ").title()}</h3>'
        body += '<table><thead><tr><th>Criterion</th><th>Status</th><th>Categories</th><th>Threshold</th></tr></thead><tbody>'
        for c in group.get("criteria", []):
            pill_cls = "pill-pass" if c["passed"] else "pill-fail"
            label = "PASS" if c["passed"] else "FAIL"
            cats = ", ".join(c.get("categories", []))
            body += f'<tr><td>{c["name"]}</td><td><span class="pill {pill_cls}">{label}</span></td>'
            body += f'<td class="meta">{cats}</td><td class="num">{c.get("threshold", "—")}</td></tr>'
        body += '</tbody></table>'

    body += '</div>'

    # Evidence trail
    evidence_html = f'<a href="/witness" style="color:#0B090A">Witness — issuer evidence chain</a><br>'
    if d.get("proof_url"):
        evidence_html += f'<a href="{d["proof_url"]}" style="color:#0B090A">Score Proof — full derivation</a><br>'
    evidence_html += f'<a href="/api/methodology" style="color:#0B090A">Methodology documentation</a>'
    body += section("Evidence Trail", evidence_html)

    # Score history (temporal stability)
    history = d.get("history") or []
    if history:
        rows = [[h.get("date", "—")[:10], f"{h['score']:.1f}"] for h in history[:14]]
        body += section("Temporal Stability (14-day)",
                        '<p class="meta">Score consistency over time strengthens classification confidence.</p>' +
                        table(["Date", "Score"], rows, [1]))

    # Methodology
    body += section("Methodology",
                    f'<p>Scoring surface: {d.get("formula_version", "")}</p>'
                    f'<p>Lens: {lens_id} v{lens_version}</p>'
                    f'<p>Classification logic: all criteria must pass for eligibility.</p>'
                    f'<p class="meta">This report is generated from deterministic scoring data and can be '
                    f'independently verified using the report hash below.</p>')

    body += attestation_footer(report_hash, d.get("formula_version", ""),
                               timestamp, lens_id, lens_version)

    return page(f"{name} — Compliance Report ({framework})", body,
                f"Compliance classification for {name} under {framework}.",
                f"{CANONICAL_BASE_URL}/report/stablecoin/{entity_id}?lens={lens_id}")
