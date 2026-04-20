"""
Composition Playground
=======================
Portfolio-level CQI aggregation, stress scenarios, and Basel SCO60 preview.
Reuses existing primitives — no new scoring logic.
"""

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from app.database import fetch_one, fetch_all

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    field: str
    message: str


def validate_portfolio(portfolio: list[dict]) -> list[ValidationError]:
    """Validate a portfolio submission. Returns empty list if valid."""
    errors = []
    if not portfolio:
        errors.append(ValidationError("portfolio", "Portfolio is empty"))
        return errors
    if len(portfolio) > 50:
        errors.append(ValidationError("portfolio", f"Portfolio has {len(portfolio)} positions (max 50)"))
        return errors
    for i, pos in enumerate(portfolio):
        if not pos.get("asset_symbol"):
            errors.append(ValidationError(f"position[{i}]", "Missing asset_symbol"))
        amount = pos.get("amount")
        if amount is None or (isinstance(amount, (int, float)) and amount <= 0):
            errors.append(ValidationError(f"position[{i}].amount", f"Invalid amount: {amount}"))
    return errors


def compute_aggregate_cqi(portfolio: list[dict]) -> dict:
    """
    Compute weighted-average CQI across a portfolio.
    Each position: SII for the asset, PSI for the protocol (if specified).
    Free-floating stablecoin holdings use SII directly.
    """
    from app.composition import compose_geometric_mean

    total_value = sum(float(p.get("amount", 0)) for p in portfolio)
    if total_value <= 0:
        return {"error": "Portfolio total value is zero"}

    positions = []
    weighted_sum = 0.0
    total_weight = 0.0

    for pos in portfolio:
        symbol = pos.get("asset_symbol", "").upper()
        amount = float(pos.get("amount", 0))
        weight = amount / total_value
        protocol = pos.get("protocol_slug")

        # Get SII score
        sii_row = fetch_one("""
            SELECT s.overall_score FROM scores s
            JOIN stablecoins st ON st.id = s.stablecoin_id
            WHERE UPPER(st.symbol) = %s
        """, (symbol,))
        sii_score = float(sii_row["overall_score"]) if sii_row and sii_row.get("overall_score") else None

        psi_score = None
        cqi_score = None

        if protocol:
            psi_row = fetch_one("""
                SELECT overall_score FROM psi_scores
                WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1
            """, (protocol,))
            psi_score = float(psi_row["overall_score"]) if psi_row and psi_row.get("overall_score") else None

        if sii_score and psi_score:
            cqi_score = compose_geometric_mean([sii_score, psi_score])
        elif sii_score:
            cqi_score = sii_score

        position_score = cqi_score or 0
        weighted_sum += position_score * weight
        total_weight += weight

        positions.append({
            "asset": symbol,
            "protocol": protocol,
            "amount": amount,
            "weight": round(weight * 100, 2),
            "sii_score": sii_score,
            "psi_score": psi_score,
            "cqi_score": cqi_score,
        })

    aggregate = round(weighted_sum / total_weight, 2) if total_weight > 0 else 0

    return {
        "aggregate_cqi": aggregate,
        "grade": _grade(aggregate),
        "position_count": len(positions),
        "positions": positions,
        "total_value": total_value,
    }


def compute_stress_scenarios(portfolio: list[dict]) -> dict:
    """
    Run stress scenarios against the portfolio.
    Reuses CQI methodology — applies shocks to component scores.
    """
    cqi = compute_aggregate_cqi(portfolio)
    if cqi.get("error"):
        return cqi

    aggregate = cqi["aggregate_cqi"]
    positions = cqi["positions"]

    scenarios = []

    # Scenario 1: Single-issuer depeg (SVB-style)
    # Assume largest stablecoin position depegs to 0
    largest = max(positions, key=lambda p: p["amount"]) if positions else None
    if largest:
        shocked_positions = [p for p in positions if p["asset"] != largest["asset"]]
        remaining_value = sum(p["amount"] for p in shocked_positions)
        post_shock = _recompute_aggregate(shocked_positions, remaining_value) if remaining_value > 0 else 0
        scenarios.append({
            "name": "Single-issuer depeg",
            "description": f"Largest position ({largest['asset']}) depegs completely",
            "pre_shock_cqi": aggregate,
            "post_shock_cqi": post_shock,
            "loss_pct": round((1 - remaining_value / cqi["total_value"]) * 100, 1) if cqi["total_value"] > 0 else 100,
            "pass": post_shock >= 60,
        })

    # Scenario 2: Algorithmic collapse (Terra-style)
    # All positions with SII < 50 go to zero
    surviving = [p for p in positions if p.get("sii_score") and p["sii_score"] >= 50]
    surviving_value = sum(p["amount"] for p in surviving)
    post_algo = _recompute_aggregate(surviving, surviving_value) if surviving_value > 0 else 0
    scenarios.append({
        "name": "Algorithmic collapse",
        "description": "All positions with SII below 50 fail",
        "pre_shock_cqi": aggregate,
        "post_shock_cqi": post_algo,
        "loss_pct": round((1 - surviving_value / cqi["total_value"]) * 100, 1) if cqi["total_value"] > 0 else 0,
        "pass": post_algo >= 60,
    })

    # Scenario 3: Protocol contagion
    # All positions with protocol exposure lose 30% of their CQI
    contagion_positions = []
    for p in positions:
        if p.get("protocol") and p.get("cqi_score"):
            shocked = {**p, "cqi_score": p["cqi_score"] * 0.7}
            contagion_positions.append(shocked)
        else:
            contagion_positions.append(p)
    post_contagion = _recompute_from_scores(contagion_positions)
    scenarios.append({
        "name": "Protocol contagion",
        "description": "All protocol-exposed positions lose 30% CQI",
        "pre_shock_cqi": aggregate,
        "post_shock_cqi": post_contagion,
        "loss_pct": round((aggregate - post_contagion) / aggregate * 100, 1) if aggregate > 0 else 0,
        "pass": post_contagion >= 60,
    })

    all_pass = all(s["pass"] for s in scenarios)

    return {
        "scenarios": scenarios,
        "all_pass": all_pass,
        "pass_count": sum(1 for s in scenarios if s["pass"]),
        "total_count": len(scenarios),
    }


def render_basel_sco60_preview(portfolio: list[dict], cqi: dict) -> str:
    """Render truncated Basel SCO60 preview — executive summary only."""
    aggregate = cqi.get("aggregate_cqi", 0)
    grade = cqi.get("grade", "—")
    count = cqi.get("position_count", 0)
    total = cqi.get("total_value", 0)

    # Determine classification
    if aggregate >= 80:
        classification = "Group 1a (low risk) — eligible for favorable capital treatment"
    elif aggregate >= 60:
        classification = "Group 1b (moderate risk) — standard capital treatment applies"
    else:
        classification = "Group 2 (higher risk) — conservative capital treatment required"

    lines = [
        "## Basel SCO60 Classification Preview",
        "",
        f"**Portfolio:** {count} positions, ${total:,.0f} total value",
        f"**Aggregate CQI:** {aggregate:.1f}/100 ({grade})",
        f"**Classification:** {classification}",
        "",
        "### Top Positions by Weight",
        "",
        "| Asset | Weight | SII | CQI |",
        "|-------|--------|-----|-----|",
    ]
    for p in sorted(cqi.get("positions", []), key=lambda x: x["weight"], reverse=True)[:5]:
        sii = f"{p['sii_score']:.1f}" if p.get("sii_score") else "—"
        cqi_s = f"{p['cqi_score']:.1f}" if p.get("cqi_score") else "—"
        lines.append(f"| {p['asset']} | {p['weight']:.1f}% | {sii} | {cqi_s} |")

    lines.append("")
    lines.append("*This is a preview. Request the full report for per-position detail, "
                  "scenario-by-scenario breakdown, and methodology appendix.*")

    return "\n".join(lines)


def render_basel_sco60_full(portfolio: list[dict], cqi: dict) -> str:
    """Render full Basel SCO60 report using the existing compliance template."""
    from app.report import assemble_report_data
    from app.lenses import load_lens, apply_lens
    from app.templates.compliance import render

    # Use the dominant asset to generate the base report
    positions = cqi.get("positions", [])
    if not positions:
        return "No positions to analyze."

    dominant = max(positions, key=lambda p: p.get("amount", 0))
    symbol = dominant.get("asset", "USDC")

    data = assemble_report_data("stablecoin", symbol, persist=False)
    if not data:
        data = {"entity_type": "stablecoin", "entity_id": symbol, "name": symbol, "score": cqi["aggregate_cqi"]}

    # Inject portfolio-level data
    data["playground_portfolio"] = portfolio
    data["playground_cqi"] = cqi

    lens = load_lens("SCO60")
    if not lens:
        return "Basel SCO60 lens not available."

    lens_result = apply_lens(lens, data)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    content_hash = hashlib.sha256(json.dumps(cqi, sort_keys=True, default=str).encode()).hexdigest()[:16]

    return render(data, lens_result, report_hash=content_hash, timestamp=ts)


def compute_content_hash(portfolio: list[dict], submitted_at: str) -> str:
    """SHA-256 of (portfolio, submitted_at) for content addressing."""
    canonical = json.dumps({"portfolio": portfolio, "submitted_at": submitted_at}, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _grade(score: float) -> str:
    if score >= 90: return "A"
    if score >= 80: return "B"
    if score >= 70: return "C"
    if score >= 60: return "D"
    return "F"


def _recompute_aggregate(positions: list[dict], total_value: float) -> float:
    if total_value <= 0 or not positions:
        return 0
    weighted = sum((p.get("cqi_score") or 0) * p["amount"] / total_value for p in positions)
    return round(weighted, 2)


def _recompute_from_scores(positions: list[dict]) -> float:
    total_amount = sum(p["amount"] for p in positions)
    if total_amount <= 0:
        return 0
    weighted = sum((p.get("cqi_score") or 0) * p["amount"] / total_amount for p in positions)
    return round(weighted, 2)
