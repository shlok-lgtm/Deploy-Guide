"""
Wallet Risk Report Template
=============================
Holdings breakdown, per-position SII, concentration, unscored exposure.
"""

from app.templates._html import (
    page, section, score_header, attestation_footer,
    table, proof_link, fmt_usd, CANONICAL_BASE_URL,
)


def render(report_data: dict, lens_result: dict = None,
           report_hash: str = "", timestamp: str = "", format: str = "html") -> str:
    d = report_data
    addr = d.get("address", d.get("entity_id", "?"))
    score = d.get("score")
    short_addr = f"{addr[:8]}...{addr[-6:]}" if len(addr) > 16 else addr

    body = f'<p class="meta">Wallet Risk Report · {short_addr} · {timestamp}</p>'
    body += score_header(short_addr, score,
                         subtitle=f"Value: {fmt_usd(d.get('holdings_value'))} · {d.get('size_tier', '').upper()}")

    # Concentration metrics
    hhi = d.get("concentration_hhi")
    hhi_label = "Concentrated" if hhi and hhi >= 5000 else "Mixed" if hhi and hhi >= 1500 else "Diversified" if hhi else "—"
    metrics = f"""<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;font-family:monospace;font-size:0.85rem">
<div>HHI: <strong>{f"{hhi:.0f}" if hhi else "—"}</strong> ({hhi_label})</div>
<div>Coverage: <strong>{d.get("coverage_quality", "—").upper()}</strong></div>
<div>Unscored: <strong>{f"{d.get('unscored_pct', 0):.1f}%" if d.get("unscored_pct") is not None else "—"}</strong></div>
<div>Dominant: <strong>{d.get("dominant_asset", "—")}</strong> ({f"{d.get('dominant_asset_pct', 0):.0f}%" if d.get("dominant_asset_pct") else "—"})</div>
<div>Scored: <strong>{d.get("num_scored", 0)}</strong></div>
<div>Unscored: <strong>{d.get("num_unscored", 0)}</strong></div>
</div>"""
    body += section("Concentration & Coverage", metrics)

    # Holdings breakdown
    holdings = d.get("holdings") or []
    if holdings:
        rows = []
        for h in holdings:
            s = f"{h['sii_score']:.1f}" if h.get("sii_score") is not None else "—"
            scored_label = '<span class="src-live">Yes</span>' if h.get("is_scored") else '<span class="src-static">No</span>'
            link = proof_link(h.get("proof_url", ""))
            rows.append([
                h.get("symbol", "?"),
                fmt_usd(h.get("value_usd")),
                f"{h.get('pct_of_wallet', 0):.1f}%",
                s,
                scored_label,
                link,
            ])
        body += section("Holdings Breakdown",
                        table(["Symbol", "Value", "% Wallet", "SII", "Scored", "Proof"],
                              rows, [1, 2, 3]))

    # Unscored exposure warning
    unscored = [h for h in holdings if not h.get("is_scored")]
    if unscored:
        items = ", ".join(h.get("symbol", "?") for h in unscored[:10])
        body += f'<div class="section" style="border-color:#c0392b"><h3 style="color:#c0392b">Unscored Exposure</h3>'
        body += f'<p>{len(unscored)} holding(s) lack SII scores: {items}</p>'
        body += '<p class="meta">Unscored holdings reduce coverage quality and may indicate higher risk.</p></div>'

    body += attestation_footer(report_hash, d.get("formula_version", ""), timestamp)

    return page(f"Wallet {short_addr} — Risk Report", body,
                f"Wallet risk report for {short_addr}. Score {score:.1f}/100." if score else "",
                f"{CANONICAL_BASE_URL}/report/wallet/{addr}")
