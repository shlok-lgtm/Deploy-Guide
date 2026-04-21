"""
Markdown Alternates
====================
Chrome-free markdown rendering of entity pages, rankings, proof pages,
and other canonical URLs. Selected via .md suffix or Accept: text/markdown.

Every .md alternate contains the SAME data as the HTML version.
No navigation, no scripts, no HTML fragments. Pure CommonMark.
"""

import logging
from datetime import datetime, timezone

from app.database import fetch_all, fetch_one

logger = logging.getLogger(__name__)

SITE = "https://basisprotocol.xyz"


def render_entity_markdown(entity_slug: str) -> str:
    """Render a stablecoin entity page as markdown."""
    row = fetch_one(
        """SELECT s.stablecoin_id, s.overall_score, s.grade,
                  s.peg_score, s.liquidity_score, s.mint_burn_score,
                  s.distribution_score, s.structural_score,
                  s.reserves_score, s.contract_score, s.oracle_score,
                  s.governance_score, s.network_score,
                  s.component_count, s.formula_version,
                  s.current_price, s.market_cap, s.volume_24h,
                  s.daily_change, s.weekly_change,
                  s.computed_at,
                  c.name, c.symbol, c.issuer
           FROM scores s
           JOIN stablecoins c ON s.stablecoin_id = c.id
           WHERE s.stablecoin_id = %s""",
        (entity_slug,),
    )

    if not row:
        return f"# {entity_slug}\n\nEntity not found or not yet scored."

    name = row.get("name", entity_slug)
    symbol = row.get("symbol", entity_slug.upper())
    score = row.get("overall_score", 0)
    grade = row.get("grade", "?")
    updated = row.get("computed_at", "")
    version = row.get("formula_version", "")

    lines = [
        f"# {name} ({symbol}) — SII Risk Score",
        "",
        f"**Score:** {score}",
        f"**Grade:** {grade}",
        f"**Components:** {row.get('component_count', 0)}",
        f"**Last updated:** {updated}",
        f"**Methodology version:** {version}",
        "",
    ]

    # Price context
    if row.get("current_price"):
        lines.extend([
            "## Market data",
            "",
            f"- Price: ${row['current_price']:.4f}",
            f"- Market cap: ${row.get('market_cap', 0):,.0f}" if row.get("market_cap") else "",
            f"- 24h volume: ${row.get('volume_24h', 0):,.0f}" if row.get("volume_24h") else "",
            f"- Daily change: {row.get('daily_change', 0):+.2f}" if row.get("daily_change") else "",
            f"- Weekly change: {row.get('weekly_change', 0):+.2f}" if row.get("weekly_change") else "",
            "",
        ])

    # Category breakdown
    lines.extend([
        "## Category breakdown",
        "",
        "| Category | Score |",
        "|----------|-------|",
        f"| Peg stability | {row.get('peg_score', 0):.1f} |",
        f"| Liquidity depth | {row.get('liquidity_score', 0):.1f} |",
        f"| Mint/burn dynamics | {row.get('mint_burn_score', 0):.1f} |",
        f"| Holder distribution | {row.get('distribution_score', 0):.1f} |",
        f"| Structural risk | {row.get('structural_score', 0):.1f} |",
        "",
        "### Structural sub-scores",
        "",
        "| Sub-category | Score |",
        "|-------------|-------|",
        f"| Reserves | {row.get('reserves_score', 0):.1f} |",
        f"| Smart contract | {row.get('contract_score', 0):.1f} |",
        f"| Oracle integrity | {row.get('oracle_score', 0):.1f} |",
        f"| Governance | {row.get('governance_score', 0):.1f} |",
        f"| Network/chain | {row.get('network_score', 0):.1f} |",
        "",
    ])

    # Cross-index scores
    psi_row = fetch_one(
        "SELECT overall_score, grade FROM psi_scores WHERE protocol_slug = %s ORDER BY scored_at DESC LIMIT 1",
        (entity_slug,),
    )
    if psi_row:
        lines.extend([
            "## Also scored in",
            "",
            f"- PSI (Protocol Solvency Index): {psi_row.get('overall_score', 0)} ({psi_row.get('grade', '?')})",
            "",
        ])

    # Provenance links
    lines.extend([
        "## Provenance",
        "",
        f"- Computation: {SITE}/proof/sii/{entity_slug}",
        f"- Evidence: {SITE}/witness",
        f"- API: {SITE}/api/scores/{entity_slug}",
        "",
        "## Data license",
        "",
        "All data at basisprotocol.xyz is free to reference with attribution.",
        "For commercial redistribution, contact shlok@basisprotocol.xyz.",
    ])

    return "\n".join(line for line in lines if line is not None)


def render_rankings_markdown() -> str:
    """Render the rankings page as markdown."""
    rows = fetch_all(
        """SELECT s.stablecoin_id, s.overall_score, s.grade,
                  s.daily_change, c.name, c.symbol
           FROM scores s
           JOIN stablecoins c ON s.stablecoin_id = c.id
           ORDER BY s.overall_score DESC"""
    )

    lines = [
        "# Stablecoin Integrity Index — Rankings",
        "",
        f"**Entities scored:** {len(rows or [])}",
        f"**Updated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "| Rank | Stablecoin | Score | Grade | 24h Δ |",
        "|------|-----------|-------|-------|-------|",
    ]

    for i, row in enumerate(rows or [], 1):
        delta = f"{row.get('daily_change', 0):+.1f}" if row.get("daily_change") else "—"
        lines.append(
            f"| {i} | [{row.get('name', '?')} ({row.get('symbol', '?')})]({SITE}/entity/{row['stablecoin_id']}) "
            f"| {row.get('overall_score', 0):.1f} | {row.get('grade', '?')} | {delta} |"
        )

    # PSI rankings
    psi_rows = fetch_all(
        """SELECT protocol_slug, overall_score, grade
           FROM psi_scores
           WHERE scored_at = (SELECT MAX(scored_at) FROM psi_scores)
           ORDER BY overall_score DESC"""
    )

    if psi_rows:
        lines.extend([
            "",
            "## Protocol Solvency Index",
            "",
            "| Rank | Protocol | Score | Grade |",
            "|------|----------|-------|-------|",
        ])
        for i, row in enumerate(psi_rows, 1):
            lines.append(
                f"| {i} | {row.get('protocol_slug', '?')} "
                f"| {row.get('overall_score', 0):.1f} | {row.get('grade', '?')} |"
            )

    lines.extend([
        "",
        f"Full methodology: {SITE}/api/methodology",
        f"API: {SITE}/api/scores",
    ])

    return "\n".join(lines)


def render_proof_markdown(index_name: str, entity_slug: str) -> str:
    """Render a proof page as markdown."""
    lines = [
        f"# Computation Proof — {index_name.upper()} / {entity_slug}",
        "",
    ]

    if index_name == "sii":
        row = fetch_one(
            """SELECT s.overall_score, s.grade, s.component_count,
                      s.formula_version, s.computed_at
               FROM scores s WHERE s.stablecoin_id = %s""",
            (entity_slug,),
        )
        if row:
            lines.extend([
                f"**Score:** {row.get('overall_score', 0)}",
                f"**Grade:** {row.get('grade', '?')}",
                f"**Components:** {row.get('component_count', 0)}",
                f"**Formula version:** {row.get('formula_version', '?')}",
                f"**Computed at:** {row.get('computed_at', '?')}",
                "",
            ])

        # Component readings
        readings = fetch_all(
            """SELECT component_id, category, normalized_score, data_source, collected_at
               FROM component_readings
               WHERE stablecoin_id = %s
               ORDER BY category, component_id""",
            (entity_slug,),
        )
        if readings:
            lines.extend([
                "## Component readings",
                "",
                "| Component | Category | Score | Source |",
                "|-----------|----------|-------|--------|",
            ])
            for r in readings:
                lines.append(
                    f"| {r.get('component_id', '?')} | {r.get('category', '?')} "
                    f"| {r.get('normalized_score', 0):.1f} | {r.get('data_source', '?')} |"
                )

    lines.extend([
        "",
        f"Full API: {SITE}/api/scores/{entity_slug}",
    ])

    return "\n".join(lines)


def render_methodology_markdown() -> str:
    """Render methodology page as markdown."""
    lines = [
        "# Basis SII Methodology",
        "",
        "## Formula (v1.0.0)",
        "",
        "```",
        "SII = 0.30×Peg + 0.25×Liquidity + 0.15×MintBurn + 0.10×Distribution + 0.20×Structural",
        "",
        "Structural = 0.30×Reserves + 0.20×SmartContract + 0.15×Oracle + 0.20×Governance + 0.15×Network",
        "```",
        "",
        "102 components across 11 categories. 83 automated, deterministic.",
        "Scores are 0-100, grades A+ through F.",
        "",
        "## Grade scale",
        "",
        "| Grade | Range |",
        "|-------|-------|",
        "| A+ | 95-100 |",
        "| A | 90-94.9 |",
        "| A- | 85-89.9 |",
        "| B+ | 80-84.9 |",
        "| B | 75-79.9 |",
        "| B- | 70-74.9 |",
        "| C+ | 65-69.9 |",
        "| C | 60-64.9 |",
        "| C- | 55-59.9 |",
        "| D+ | 50-54.9 |",
        "| D | 40-49.9 |",
        "| F | 0-39.9 |",
        "",
        f"Full specification: {SITE}/api/methodology",
        f"Version history: {SITE}/api/methodology/versions",
    ]

    return "\n".join(lines)
