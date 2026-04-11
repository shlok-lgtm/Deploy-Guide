"""
Report Data Assembler
======================
Assembles structured report data from existing scoring primitives.
No new scoring logic — reports compose existing SII, PSI, CQI, and wallet scores.
"""

import logging
from datetime import datetime, timezone

from app.database import fetch_one, fetch_all
from app.scoring import FORMULA_VERSION, SII_V1_WEIGHTS, STRUCTURAL_SUBWEIGHTS
from app.composition import compute_cqi, compose_geometric_mean

logger = logging.getLogger(__name__)


def assemble_report_data(entity_type: str, entity_id: str) -> dict | None:
    """
    Assemble full report data for an entity.

    Args:
        entity_type: 'stablecoin', 'protocol', or 'wallet'
        entity_id: stablecoin symbol, protocol slug, or wallet address

    Returns:
        Structured dict with all data needed to render any template,
        or None if entity not found.
    """
    if entity_type == "stablecoin":
        return _assemble_stablecoin(entity_id)
    elif entity_type == "protocol":
        return _assemble_protocol(entity_id)
    elif entity_type == "wallet":
        return _assemble_wallet(entity_id)
    return None


def _assemble_stablecoin(symbol: str) -> dict | None:
    """Assemble SII report for a stablecoin."""
    row = fetch_one("""
        SELECT s.*, st.name, st.symbol, st.issuer, st.contract AS token_contract
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        WHERE UPPER(st.symbol) = UPPER(%s)
    """, (symbol,))

    if not row:
        return None

    sid = row["stablecoin_id"]
    score = float(row.get("overall_score") or 0)

    categories = {
        "peg": {"score": _f(row.get("peg_score")), "weight": SII_V1_WEIGHTS["peg_stability"]},
        "liquidity": {"score": _f(row.get("liquidity_score")), "weight": SII_V1_WEIGHTS["liquidity_depth"]},
        "flows": {"score": _f(row.get("mint_burn_score")), "weight": SII_V1_WEIGHTS["mint_burn_dynamics"]},
        "distribution": {"score": _f(row.get("distribution_score")), "weight": SII_V1_WEIGHTS["holder_distribution"]},
        "structural": {"score": _f(row.get("structural_score")), "weight": SII_V1_WEIGHTS["structural_risk_composite"]},
    }

    structural_breakdown = {
        "reserves": {"score": _f(row.get("reserves_score")), "weight": STRUCTURAL_SUBWEIGHTS["reserves_collateral"]},
        "contract": {"score": _f(row.get("contract_score")), "weight": STRUCTURAL_SUBWEIGHTS["smart_contract_risk"]},
        "oracle": {"score": _f(row.get("oracle_score")), "weight": STRUCTURAL_SUBWEIGHTS["oracle_integrity"]},
        "governance": {"score": _f(row.get("governance_score")), "weight": STRUCTURAL_SUBWEIGHTS["governance_operations"]},
        "network": {"score": _f(row.get("network_score")), "weight": STRUCTURAL_SUBWEIGHTS["network_chain_risk"]},
    }

    components = fetch_all("""
        SELECT DISTINCT ON (component_id)
            component_id, category, raw_value, normalized_score, data_source, collected_at
        FROM component_readings
        WHERE stablecoin_id = %s AND collected_at > NOW() - INTERVAL '48 hours'
        ORDER BY component_id, collected_at DESC
    """, (sid,))

    # Protocols that hold this stablecoin
    protocols_holding = _get_protocols_for_stablecoin(symbol)

    # Score hashes from assessment events
    score_hashes = _get_score_hashes("stablecoin", sid)

    # State attestation hashes
    state_hashes = _get_state_hashes(["sii_components", "cda_extractions"], entity_id=sid)

    # History for temporal stability
    history = fetch_all("""
        SELECT overall_score, grade, score_date AS computed_at
        FROM score_history WHERE stablecoin = %s
        ORDER BY score_date DESC LIMIT 30
    """, (sid,))

    return {
        "entity_type": "stablecoin",
        "entity_id": symbol.upper(),
        "name": row.get("name", symbol),
        "symbol": (row.get("symbol") or symbol).upper(),
        "issuer": row.get("issuer"),
        "token_contract": row.get("token_contract"),
        "score": score,
        "categories": categories,
        "structural_breakdown": structural_breakdown,
        "components": [_fmt_component(c) for c in components],
        "component_count": row.get("component_count") or len(components),
        "protocols_holding": protocols_holding,
        "score_hashes": score_hashes,
        "state_hashes": state_hashes,
        "history": [{"score": float(h["overall_score"]),
                      "date": h["computed_at"].isoformat()} for h in history] if history else [],
        "formula_version": row.get("formula_version") or FORMULA_VERSION,
        "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        "proof_url": f"/proof/sii/{sid}",
    }


def _assemble_protocol(slug: str) -> dict | None:
    """Assemble PSI report for a protocol, including stablecoin exposure and CQI."""
    row = fetch_one("""
        SELECT id, protocol_slug, protocol_name, overall_score, grade,
               category_scores, component_scores, raw_values,
               formula_version, inputs_hash, computed_at
        FROM psi_scores
        WHERE protocol_slug = %s
        ORDER BY computed_at DESC LIMIT 1
    """, (slug,))

    if not row:
        return None

    score = float(row.get("overall_score") or 0)
    cat_scores = row.get("category_scores") or {}
    comp_scores = row.get("component_scores") or {}

    # Stablecoin exposure — what stablecoins does this protocol hold/accept?
    exposure = _get_stablecoin_exposure(slug)

    # CQI pairs for each stablecoin in exposure
    cqi_pairs = []
    cqi_hashes = []
    for coin in exposure:
        cqi = compute_cqi(coin["symbol"], slug)
        if "error" not in cqi:
            cqi_pairs.append({
                "asset": coin["symbol"],
                "protocol": row.get("protocol_name", slug),
                "sii_score": cqi["inputs"]["sii"]["score"],
                "psi_score": cqi["inputs"]["psi"]["score"],
                "cqi_score": cqi["cqi_score"],
                "confidence": cqi["confidence"],
                "proof_url": f"/proof/sii/{coin['stablecoin_id']}",
            })

    score_hashes = _get_score_hashes("protocol", slug)
    state_hashes = _get_state_hashes(["psi_components", "psi_discoveries"], entity_id=slug)

    return {
        "entity_type": "protocol",
        "entity_id": slug,
        "name": row.get("protocol_name", slug),
        "protocol_slug": slug,
        "score": score,
        "category_scores": cat_scores,
        "component_scores": comp_scores,
        "raw_values": row.get("raw_values") or {},
        "exposure": exposure,
        "cqi_pairs": cqi_pairs,
        "cqi_hashes": cqi_hashes,
        "score_hashes": score_hashes,
        "state_hashes": state_hashes,
        "formula_version": row.get("formula_version") or "psi-v0.2.0",
        "inputs_hash": row.get("inputs_hash"),
        "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        "proof_url": f"/proof/psi/{slug}",
    }


def _assemble_wallet(address: str) -> dict | None:
    """Assemble wallet risk report from holdings and scores."""
    addr = address.strip().lower()

    risk = fetch_one("""
        SELECT risk_score, risk_grade, concentration_hhi, concentration_grade,
               unscored_pct, coverage_quality,
               num_scored_holdings, num_unscored_holdings, num_total_holdings,
               dominant_asset, dominant_asset_pct,
               total_stablecoin_value, size_tier, formula_version, computed_at
        FROM wallet_graph.wallet_risk_scores
        WHERE LOWER(wallet_address) = %s
        ORDER BY computed_at DESC LIMIT 1
    """, (addr,))

    holdings = fetch_all("""
        SELECT token_address, symbol, chain, balance, value_usd,
               is_scored, sii_score, sii_grade, pct_of_wallet
        FROM wallet_graph.wallet_holdings
        WHERE LOWER(wallet_address) = %s
          AND indexed_at > NOW() - INTERVAL '7 days'
          AND value_usd >= 0.01
        ORDER BY value_usd DESC
    """, (addr,))

    if not risk and not holdings:
        return None

    holdings_value = sum(float(h.get("value_usd") or 0) for h in holdings)

    # Recalculate pct_of_wallet from current holdings
    for h in holdings:
        h["pct_of_wallet"] = round((float(h.get("value_usd") or 0) / holdings_value) * 100, 2) if holdings_value > 0 else 0

    # Collect proof URLs for scored holdings
    for h in holdings:
        if h.get("is_scored") and h.get("symbol"):
            sid = fetch_one(
                "SELECT id FROM stablecoins WHERE UPPER(symbol) = UPPER(%s)",
                (h["symbol"],),
            )
            h["proof_url"] = f"/proof/sii/{sid['id']}" if sid else None

    score_hashes = _get_score_hashes("wallet", addr)
    state_hashes = _get_state_hashes(["wallets", "wallet_profiles", "edges"])

    return {
        "entity_type": "wallet",
        "entity_id": addr,
        "address": addr,
        "score": float(risk["risk_score"]) if risk and risk.get("risk_score") is not None else None,
        "concentration_hhi": float(risk["concentration_hhi"]) if risk and risk.get("concentration_hhi") else None,
        "unscored_pct": float(risk["unscored_pct"]) if risk and risk.get("unscored_pct") else None,
        "coverage_quality": risk.get("coverage_quality") if risk else None,
        "dominant_asset": risk.get("dominant_asset") if risk else None,
        "dominant_asset_pct": float(risk["dominant_asset_pct"]) if risk and risk.get("dominant_asset_pct") else None,
        "size_tier": risk.get("size_tier") if risk else None,
        "holdings_value": round(holdings_value, 2),
        "holdings": [{
            "symbol": h.get("symbol"),
            "chain": h.get("chain", "ethereum"),
            "value_usd": float(h.get("value_usd") or 0),
            "pct_of_wallet": h.get("pct_of_wallet"),
            "is_scored": h.get("is_scored"),
            "sii_score": float(h["sii_score"]) if h.get("sii_score") is not None else None,
            "proof_url": h.get("proof_url"),
        } for h in holdings],
        "num_scored": risk.get("num_scored_holdings") if risk else sum(1 for h in holdings if h.get("is_scored")),
        "num_unscored": risk.get("num_unscored_holdings") if risk else sum(1 for h in holdings if not h.get("is_scored")),
        "score_hashes": score_hashes,
        "state_hashes": state_hashes,
        "formula_version": risk.get("formula_version") if risk else "wallet-v1.0.0",
        "computed_at": risk["computed_at"].isoformat() if risk and risk.get("computed_at") else None,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_state_hashes(domains: list[str], entity_id: str = None) -> dict:
    """Fetch latest state attestation hashes for relevant domains."""
    try:
        from app.state_attestation import get_latest_attestation
        hashes = {}
        for domain in domains:
            att = get_latest_attestation(domain, entity_id)
            if att:
                hashes[domain] = att["batch_hash"]
        return hashes
    except Exception:
        return {}


def _f(val) -> float | None:
    """Safe float conversion."""
    return round(float(val), 2) if val is not None else None


def _fmt_component(c: dict) -> dict:
    """Format a component reading for report data."""
    src = (c.get("data_source") or "").lower()
    if "cda" in src:
        source_type = "cda_extraction"
    elif src in ("static", "config"):
        source_type = "static_config"
    else:
        source_type = "live_api"

    return {
        "id": c["component_id"],
        "category": c["category"],
        "raw_value": c.get("raw_value"),
        "normalized_score": round(float(c["normalized_score"]), 2) if c.get("normalized_score") is not None else None,
        "data_source": c.get("data_source"),
        "source_type": source_type,
        "collected_at": c["collected_at"].isoformat() if c.get("collected_at") else None,
    }


def _get_protocols_for_stablecoin(symbol: str) -> list[dict]:
    """Find protocols that hold or accept this stablecoin."""
    try:
        rows = fetch_all("""
            SELECT DISTINCT ON (ps.protocol_slug)
                ps.protocol_slug, ps.protocol_name, ps.overall_score, ps.grade
            FROM psi_scores ps
            JOIN protocol_collateral_exposure ce ON ce.protocol_slug = ps.protocol_slug
            WHERE UPPER(ce.token_symbol) = UPPER(%s)
              AND ce.is_stablecoin = TRUE
            ORDER BY ps.protocol_slug, ps.computed_at DESC
        """, (symbol,))
        return [{"slug": r["protocol_slug"], "name": r["protocol_name"],
                 "psi_score": float(r["overall_score"]) if r.get("overall_score") else None} for r in rows]
    except Exception:
        return []


def _get_stablecoin_exposure(protocol_slug: str) -> list[dict]:
    """Get stablecoins held/accepted by a protocol."""
    try:
        rows = fetch_all("""
            SELECT ce.token_symbol AS asset_symbol, ce.tvl_usd AS exposure_usd,
                   s.overall_score, s.grade, st.id AS stablecoin_id, st.name
            FROM protocol_collateral_exposure ce
            LEFT JOIN stablecoins st ON UPPER(st.symbol) = UPPER(ce.token_symbol)
            LEFT JOIN scores s ON s.stablecoin_id = st.id
            WHERE ce.protocol_slug = %s
              AND ce.is_stablecoin = TRUE
            ORDER BY ce.tvl_usd DESC NULLS LAST
        """, (protocol_slug,))
        return [{
            "symbol": r["asset_symbol"],
            "stablecoin_id": r.get("stablecoin_id"),
            "name": r.get("name"),
            "exposure_usd": float(r["exposure_usd"]) if r.get("exposure_usd") else None,
            "sii_score": float(r["overall_score"]) if r.get("overall_score") else None,
        } for r in rows]
    except Exception:
        return []


def _get_score_hashes(entity_type: str, entity_id: str) -> list[str]:
    """Get score hashes from component_batch_hashes or assessment_events."""
    try:
        if entity_type in ("stablecoin", "protocol"):
            rows = fetch_all("""
                SELECT DISTINCT batch_hash FROM component_batch_hashes
                WHERE entity_type = %s AND entity_id = %s
                  AND batch_hash IS NOT NULL
                ORDER BY batch_hash
            """, (entity_type, entity_id))
            return [r["batch_hash"] for r in rows] if rows else []
        elif entity_type == "wallet":
            rows = fetch_all("""
                SELECT DISTINCT content_hash FROM assessment_events
                WHERE LOWER(wallet_address) = %s
                  AND content_hash IS NOT NULL
                ORDER BY content_hash
            """, (entity_id,))
            return [r["content_hash"] for r in rows] if rows else []
        return []
    except Exception:
        return []
