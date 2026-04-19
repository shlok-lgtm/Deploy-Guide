"""
Engagement Template
====================
Per-account demo artifact for cold email attachment.
Five sections: exposure map, own scores, historical reconstruction,
start today, forward to risk team.

Renders markdown by default (copy-pasteable into email drafts).
HTML optional via format parameter.

Consumes 8 composer outputs from _assemble_protocol — does NOT query
the database. Every claim cites a score hash or proof URL.
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

CANONICAL_BASE_URL = "https://basisprotocol.xyz"


def render(report_data: dict, lens_result: dict = None,
           report_hash: str = "", timestamp: str = "", format: str = "markdown") -> str:
    """Render engagement artifact. Markdown default, HTML optional."""
    d = report_data
    entity_type = d.get("entity_type", "protocol")
    name = d.get("name", d.get("entity_id", "Unknown"))
    entity_id = d.get("entity_id", "")

    if format == "html":
        return _render_html(d, lens_result, report_hash, timestamp)

    return _render_markdown(d, lens_result, report_hash, timestamp)


def _render_markdown(d: dict, lens_result: dict = None,
                     report_hash: str = "", timestamp: str = "") -> str:
    entity_type = d.get("entity_type", "protocol")
    name = d.get("name", d.get("entity_id", "Unknown"))
    entity_id = d.get("entity_id", "")
    score = d.get("score")
    ts = timestamp or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = []

    # Header block
    lines.append(f"**Account:** {name}")
    lines.append(f"**Generated:** {ts}")
    lines.append(f"**Verifiable:** {CANONICAL_BASE_URL}/proof/psi/{entity_id}")
    if report_hash:
        lines.append(f"**Report hash:** `{report_hash[:16]}...`")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Section 1 — Live exposure map
    lines.append(f"## Live exposure map for {name}")
    lines.append("")

    exposure = d.get("exposure") or []
    cqi_pairs = d.get("cqi_pairs") or []

    if entity_type == "protocol" and exposure:
        lines.append("| Asset | Exposure | SII | CQI | Proof |")
        lines.append("|-------|----------|-----|-----|-------|")
        cqi_map = {p["asset"]: p for p in cqi_pairs}
        flagged = None
        for e in exposure:
            sym = e.get("symbol", "?")
            amt = f"${e['exposure_usd']:,.0f}" if e.get("exposure_usd") else "—"
            sii = f"{e['sii_score']:.1f}" if e.get("sii_score") else "—"
            cqi_entry = cqi_map.get(sym, {})
            cqi_s = f"{cqi_entry['cqi_score']:.1f}" if cqi_entry.get("cqi_score") else "—"
            proof = f"[verify]({CANONICAL_BASE_URL}/proof/sii/{e.get('stablecoin_id', sym)})"
            lines.append(f"| {sym} | {amt} | {sii} | {cqi_s} | {proof} |")
            if e.get("sii_score") and e["sii_score"] < 60 and not flagged:
                flagged = f"{sym} scores below 60 on SII ({e['sii_score']:.1f}) — this is the weakest asset in the exposure set."
        lines.append("")
        if flagged:
            lines.append(flagged)
        else:
            lines.append("Exposure set is stable across all counterparties monitored over the last 30 days.")
    else:
        lines.append("*No exposure data captured.*")
    lines.append("")

    # Section 2 — Entity's own scores
    lines.append(f"## {name} scores")
    lines.append("")

    if score is not None:
        lines.append(f"**PSI:** {score:.1f}/100 · [proof]({CANONICAL_BASE_URL}/proof/psi/{entity_id})")

    rpi = d.get("rpi")
    if rpi and rpi.get("score") is not None:
        traj = rpi.get("trajectory") or {}
        traj_parts = []
        for label, delta in sorted(traj.items()):
            sign = "+" if delta >= 0 else ""
            traj_parts.append(f"{label} {sign}{delta:.1f}")
        traj_str = " · ".join(traj_parts) if traj_parts else ""
        line = f"**RPI:** {rpi['score']:.1f}/100"
        if traj_str:
            line += f" ({traj_str})"
        lines.append(line)

    # Component attribution — find the top mover
    comp = d.get("component_scores") or {}
    if comp:
        sorted_comps = sorted(comp.items(), key=lambda x: abs(float(x[1] or 0)), reverse=True)
        if sorted_comps:
            top_name, top_val = sorted_comps[0]
            lines.append(f"Strongest component: {top_name.replace('_', ' ')} at {float(top_val):.1f}.")

    lines.append("")

    # Section 3 — What Basis would have shown at most recent event
    lines.append(f"## What Basis would have shown")
    lines.append("")

    event_rendered = False

    # Priority: oracle stress → parameter change → governance edit → peg event
    oracle = d.get("oracle_behavior") or {}
    stress = oracle.get("stress_events") or []
    if stress:
        ev = stress[0]
        lines.append(
            f"**Oracle stress event** on {ev.get('feed', 'unknown feed')} "
            f"({ev.get('timestamp', '')[:10]}): "
            f"max deviation {ev.get('max_deviation_pct', '?')}%, "
            f"lasted {ev.get('duration_s', '?')}s."
        )
        event_rendered = True

    if not event_rendered:
        params = d.get("parameter_changes") or []
        reactive = [p for p in params if p.get("context") == "reactive"]
        if reactive:
            p = reactive[0]
            lines.append(
                f"**Reactive parameter change** on {p.get('parameter', '')} "
                f"({p.get('timestamp', '')[:10]}): "
                f"{p.get('old_value', '?')} → {p.get('new_value', '?')} {p.get('unit', '')}. "
                f"Context: {p.get('context', 'unclassified')}."
            )
            event_rendered = True

    if not event_rendered:
        gov = d.get("governance_activity") or {}
        edits = gov.get("edited_after_publication") or []
        if edits:
            ed = edits[0]
            lines.append(
                f"**Governance edit detected** on \"{ed.get('title', 'proposal')}\" — "
                f"body modified after publication. "
                f"Original hash: `{ed.get('original_hash', '?')[:12]}...`"
            )
            event_rendered = True

    if not event_rendered:
        lines.append(
            f"No material events affecting your exposure set in the last 90 days. "
            f"Basis will reconstruct any prior event on request — "
            f"query the temporal engine at `{CANONICAL_BASE_URL}/api/scores/{entity_id}/history`."
        )
    lines.append("")

    # Section 4 — Start today
    lines.append("## Start today")
    lines.append("")
    lines.append("```bash")
    lines.append(f"curl {CANONICAL_BASE_URL}/api/psi/scores/{entity_id}")
    lines.append(f"curl {CANONICAL_BASE_URL}/api/compose/cqi?protocol={entity_id}")
    lines.append(f"curl {CANONICAL_BASE_URL}/api/reports/protocol/{entity_id}")
    lines.append("```")
    lines.append("")
    lines.append("```solidity")
    lines.append(f'uint16 score = IBasisSIIOracle(oracle).score(token);')
    lines.append("```")
    lines.append("")

    # Section 5 — Forward to risk team
    lines.append("## Forward to your risk team")
    lines.append("")
    lines.append(
        f"Forward to your risk team or your external risk contributor. "
        f"Everything above is free to read; the [Proof pages]({CANONICAL_BASE_URL}/proof/psi/{entity_id}) "
        f"let them verify independently without any vendor relationship with us."
    )
    lines.append("")

    # Footer
    lines.append("---")
    lines.append("")
    lines.append(f"*Basis Protocol · {CANONICAL_BASE_URL} · Decision integrity infrastructure*")
    state_hashes = d.get("state_hashes") or []
    if state_hashes:
        lines.append(f"*State hashes: {', '.join(h[:12] + '...' for h in state_hashes[:3])}*")

    return "\n".join(lines)


def _render_html(d: dict, lens_result: dict = None,
                 report_hash: str = "", timestamp: str = "") -> str:
    """Render engagement as HTML using the shared design system."""
    from app.templates._html import page, section, table, CANONICAL_BASE_URL as BASE

    md = _render_markdown(d, lens_result, report_hash, timestamp)

    # Convert markdown to simple HTML
    import re
    html_body = ""
    for line in md.split("\n"):
        if line.startswith("## "):
            html_body += f'<h3>{line[3:]}</h3>'
        elif line.startswith("**") and line.endswith("**"):
            html_body += f'<p><strong>{line[2:-2]}</strong></p>'
        elif line.startswith("|"):
            if "---" in line:
                continue
            cells = [c.strip() for c in line.split("|")[1:-1]]
            html_body += "<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"
        elif line.startswith("```"):
            if "bash" in line or "solidity" in line:
                html_body += '<pre style="background:#e8e6e0;padding:8px;font-size:0.8rem;overflow-x:auto"><code>'
            elif line == "```":
                html_body += "</code></pre>"
        elif line.startswith("curl ") or line.startswith("uint16"):
            html_body += line + "\n"
        elif line == "---":
            html_body += '<hr style="border:none;border-top:1px solid #ccc;margin:16px 0">'
        elif line.startswith("*") and line.endswith("*") and not line.startswith("**"):
            html_body += f'<p class="meta"><em>{line[1:-1]}</em></p>'
        elif line:
            html_body += f"<p>{line}</p>"

    name = d.get("name", d.get("entity_id", "Unknown"))
    return page(
        f"{name} — Engagement",
        html_body,
        f"Engagement artifact for {name}.",
        f"{BASE}/report/protocol/{d.get('entity_id', '')}",
        form_id="ENGAGEMENT · BASIS PROTOCOL",
        stats=[f"PSI {d.get('score', 0):.1f}" if d.get("score") else "—"],
    )
