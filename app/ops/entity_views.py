"""
Entity View Data Assemblers
============================
Pull everything Basis knows about one entity across all domains.
Each section fails independently — a missing table or empty domain
returns {"error": "unavailable"} rather than breaking the whole page.

Three entity types:
  - stablecoin (joined by symbol)
  - protocol (joined by slug)
  - wallet (joined by address)
"""

import logging
from datetime import datetime, timezone

from app.database import fetch_one, fetch_all

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _f(val):
    """Safe float conversion."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _iso(dt):
    """Safe isoformat."""
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    try:
        return dt.isoformat()
    except Exception:
        return str(dt)


def _dict_rows(rows):
    """Convert result rows to list of dicts."""
    if not rows:
        return []
    return [dict(r) for r in rows]


def _safe_section(fn, *args, **kwargs):
    """Run a section fetcher; return {"error": ...} on failure."""
    try:
        result = fn(*args, **kwargs)
        return result if result is not None else {}
    except Exception as e:
        logger.debug(f"Entity view section {fn.__name__} failed: {e}")
        return {"error": str(e)}


# ============================================================================
# STABLECOIN ENTITY VIEW
# ============================================================================

def get_stablecoin_entity(symbol: str) -> dict | None:
    """Assemble full entity view for a stablecoin."""
    sym = symbol.strip().upper()

    # Primary lookup — must exist
    coin = fetch_one("""
        SELECT s.stablecoin_id, s.overall_score, s.grade, s.peg_score,
               s.liquidity_score, s.mint_burn_score, s.distribution_score,
               s.structural_score, s.reserves_score, s.contract_score,
               s.oracle_score, s.governance_score, s.network_score,
               s.component_count, s.formula_version, s.data_freshness_pct,
               s.current_price, s.market_cap, s.volume_24h,
               s.daily_change, s.weekly_change, s.computed_at,
               st.id AS coin_id, st.name, st.symbol, st.issuer,
               st.contract AS token_contract
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        WHERE UPPER(st.symbol) = %s
    """, (sym,))

    if not coin:
        return None

    coin_id = coin["coin_id"]

    return {
        "entity_type": "stablecoin",
        "entity_id": sym,
        "name": coin.get("name", sym),
        "symbol": sym,
        "issuer": coin.get("issuer"),
        "token_contract": coin.get("token_contract"),
        "scores": _safe_section(_stablecoin_scores, coin),
        "score_history": _safe_section(_stablecoin_history, coin_id),
        "cqi_pairs": _safe_section(_stablecoin_cqi, sym),
        "evidence": _safe_section(_stablecoin_evidence, sym, coin_id),
        "signals": _safe_section(_stablecoin_signals, sym),
        "distribution": _safe_section(_stablecoin_distribution, sym, coin_id),
        "graph": _safe_section(_stablecoin_graph, sym),
        "timeline": _safe_section(_stablecoin_timeline, sym, coin_id),
    }


def _stablecoin_scores(coin: dict) -> dict:
    return {
        "overall_score": _f(coin.get("overall_score")),
        "component_count": coin.get("component_count"),
        "formula_version": coin.get("formula_version"),
        "data_freshness_pct": _f(coin.get("data_freshness_pct")),
        "current_price": _f(coin.get("current_price")),
        "market_cap": _f(coin.get("market_cap")),
        "volume_24h": _f(coin.get("volume_24h")),
        "daily_change": _f(coin.get("daily_change")),
        "weekly_change": _f(coin.get("weekly_change")),
        "computed_at": _iso(coin.get("computed_at")),
        "categories": {
            "peg": _f(coin.get("peg_score")),
            "liquidity": _f(coin.get("liquidity_score")),
            "mint_burn": _f(coin.get("mint_burn_score")),
            "distribution": _f(coin.get("distribution_score")),
            "structural": _f(coin.get("structural_score")),
        },
        "structural_breakdown": {
            "reserves": _f(coin.get("reserves_score")),
            "contract": _f(coin.get("contract_score")),
            "oracle": _f(coin.get("oracle_score")),
            "governance": _f(coin.get("governance_score")),
            "network": _f(coin.get("network_score")),
        },
    }


def _stablecoin_history(coin_id) -> dict:
    rows = fetch_all("""
        SELECT overall_score, grade, score_date,
               peg_score, liquidity_score, mint_burn_score,
               distribution_score, structural_score,
               daily_change, weekly_change
        FROM score_history
        WHERE stablecoin = %s
        ORDER BY score_date DESC LIMIT 60
    """, (coin_id,)) or []
    return {
        "points": [
            {
                "score": _f(r["overall_score"]),
                "date": str(r["score_date"]) if r.get("score_date") else None,
                "daily_change": _f(r.get("daily_change")),
                "weekly_change": _f(r.get("weekly_change")),
            }
            for r in rows
        ]
    }


def _stablecoin_cqi(symbol: str) -> dict:
    rows = fetch_all("""
        SELECT psi_slug, sii_score, psi_score, cqi_score,
               composition_method, methodology_version, computed_at
        FROM cqi_attestations
        WHERE UPPER(sii_symbol) = %s
        ORDER BY computed_at DESC
    """, (symbol,)) or []

    # Deduplicate — keep latest per psi_slug
    seen = set()
    pairs = []
    for r in rows:
        slug = r["psi_slug"]
        if slug not in seen:
            seen.add(slug)
            pairs.append({
                "protocol": slug,
                "sii_score": _f(r["sii_score"]),
                "psi_score": _f(r["psi_score"]),
                "cqi_score": _f(r["cqi_score"]),
                "method": r.get("composition_method"),
                "computed_at": _iso(r.get("computed_at")),
            })
    return {"pairs": pairs}


def _stablecoin_evidence(symbol: str, coin_id) -> dict:
    # CDA vendor extractions
    cda = fetch_all("""
        SELECT asset_symbol, source_url, source_type, extraction_method,
               extraction_vendor, confidence_score, extracted_at
        FROM cda_vendor_extractions
        WHERE UPPER(asset_symbol) = %s
        ORDER BY extracted_at DESC LIMIT 20
    """, (symbol,)) or []

    # Static component evidence
    static = fetch_all("""
        SELECT entity_slug, component_slug, source_url,
               content_hash, capture_method, captured_at, is_stale
        FROM static_evidence
        WHERE index_id = 'sii' AND UPPER(entity_slug) = %s
        ORDER BY captured_at DESC LIMIT 20
    """, (symbol,)) or []

    # Provenance proofs (match by source_domain containing symbol)
    proofs = fetch_all("""
        SELECT source_domain, source_endpoint, response_hash,
               attestation_hash, proof_url, proved_at
        FROM provenance_proofs
        ORDER BY proved_at DESC LIMIT 20
    """) or []

    return {
        "cda_extractions": _dict_rows(cda),
        "static_evidence": _dict_rows(static),
        "provenance_proofs": _dict_rows(proofs),
    }


def _stablecoin_signals(symbol: str) -> dict:
    divergence = fetch_all("""
        SELECT detector_name, signal_direction, magnitude, severity,
               detail, cycle_timestamp
        FROM divergence_signals
        WHERE UPPER(entity_id) = %s
        ORDER BY cycle_timestamp DESC LIMIT 20
    """, (symbol,)) or []

    discovery = fetch_all("""
        SELECT signal_type, domain, title, description,
               severity, novelty_score, direction, magnitude,
               detected_at
        FROM discovery_signals
        WHERE entities::text ILIKE %s
        ORDER BY detected_at DESC LIMIT 20
    """, (f'%{symbol}%',)) or []

    return {
        "divergence": _dict_rows(divergence),
        "discovery": _dict_rows(discovery),
    }


def _stablecoin_distribution(symbol: str, coin_id) -> dict:
    # Latest distribution-related component readings
    distribution_components = fetch_all("""
        SELECT component_id, raw_value, normalized_score, data_source, collected_at
        FROM component_readings
        WHERE stablecoin_id = %s
          AND category = 'holder_distribution'
          AND collected_at > NOW() - INTERVAL '48 hours'
        ORDER BY component_id, collected_at DESC
    """, (coin_id,)) or []

    # Protocol holdings — which protocols hold this stablecoin
    protocol_exposure = fetch_all("""
        SELECT protocol_slug, tvl_usd, pool_count, pool_type, snapshot_date
        FROM protocol_collateral_exposure
        WHERE UPPER(token_symbol) = %s
        ORDER BY tvl_usd DESC
    """, (symbol,)) or []

    return {
        "distribution_components": _dict_rows(distribution_components),
        "protocol_exposure": _dict_rows(protocol_exposure),
    }


def _stablecoin_graph(symbol: str) -> dict:
    # Top wallets holding this stablecoin
    top_holders = fetch_all("""
        SELECT wallet_address, symbol, value_usd, pct_of_wallet,
               sii_score, indexed_at
        FROM wallet_graph.wallet_holdings
        WHERE UPPER(symbol) = %s AND value_usd > 0
        ORDER BY value_usd DESC LIMIT 25
    """, (symbol,)) or []

    return {"top_holders": _dict_rows(top_holders)}


def _stablecoin_timeline(symbol: str, coin_id) -> dict:
    """Unified chronological stream across all domains."""
    events = []

    # Score changes
    history = fetch_all("""
        SELECT 'score_change' AS event_type, score_date AS timestamp,
               overall_score, daily_change
        FROM score_history
        WHERE stablecoin = %s
        ORDER BY score_date DESC LIMIT 30
    """, (coin_id,)) or []
    for r in history:
        events.append({
            "event_type": "score_change",
            "timestamp": _iso(r.get("timestamp")),
            "detail": f"SII score: {_f(r.get('overall_score'))}, change: {_f(r.get('daily_change'))}",
        })

    # CDA updates
    cda = fetch_all("""
        SELECT 'cda_update' AS event_type, extracted_at AS timestamp,
               source_type, extraction_vendor, confidence_score
        FROM cda_vendor_extractions
        WHERE UPPER(asset_symbol) = %s
        ORDER BY extracted_at DESC LIMIT 20
    """, (symbol,)) or []
    for r in cda:
        events.append({
            "event_type": "cda_update",
            "timestamp": _iso(r.get("timestamp")),
            "detail": f"CDA extraction via {r.get('extraction_vendor')}, confidence {_f(r.get('confidence_score'))}",
        })

    # Divergence signals
    div = fetch_all("""
        SELECT 'divergence' AS event_type, cycle_timestamp AS timestamp,
               detector_name, signal_direction, magnitude, severity
        FROM divergence_signals
        WHERE UPPER(entity_id) = %s
        ORDER BY cycle_timestamp DESC LIMIT 20
    """, (symbol,)) or []
    for r in div:
        events.append({
            "event_type": "divergence",
            "timestamp": _iso(r.get("timestamp")),
            "detail": f"{r.get('detector_name')}: {r.get('signal_direction')} ({r.get('severity')}), magnitude {_f(r.get('magnitude'))}",
        })

    # Governance mentions — governance_events that might reference this stablecoin
    gov = fetch_all("""
        SELECT 'governance' AS event_type, event_timestamp AS timestamp,
               protocol_slug, event_type AS gov_type, title
        FROM governance_events
        WHERE LOWER(title) LIKE %s OR LOWER(description) LIKE %s
        ORDER BY event_timestamp DESC LIMIT 20
    """, (f'%{symbol.lower()}%', f'%{symbol.lower()}%')) or []
    for r in gov:
        events.append({
            "event_type": "governance",
            "timestamp": _iso(r.get("timestamp")),
            "detail": f"[{r.get('protocol_slug')}] {r.get('title')}",
        })

    # Sort all events by timestamp descending
    events.sort(key=lambda e: e.get("timestamp") or "", reverse=True)
    return {"events": events[:100]}


# ============================================================================
# PROTOCOL ENTITY VIEW
# ============================================================================

def get_protocol_entity(slug: str) -> dict | None:
    """Assemble full entity view for a protocol."""
    slug = slug.strip().lower()

    psi = fetch_one("""
        SELECT id, protocol_slug, protocol_name, overall_score, grade,
               category_scores, component_scores, raw_values,
               formula_version, computed_at
        FROM psi_scores
        WHERE protocol_slug = %s
        ORDER BY computed_at DESC LIMIT 1
    """, (slug,))

    if not psi:
        return None

    return {
        "entity_type": "protocol",
        "entity_id": slug,
        "name": psi.get("protocol_name", slug),
        "scores": _safe_section(_protocol_scores, psi),
        "score_history": _safe_section(_protocol_history, slug),
        "rpi": _safe_section(_protocol_rpi, slug),
        "cqi_matrix": _safe_section(_protocol_cqi, slug, psi),
        "collateral": _safe_section(_protocol_collateral, slug),
        "governance": _safe_section(_protocol_governance, slug),
        "evidence": _safe_section(_protocol_evidence, slug),
        "signals": _safe_section(_protocol_signals, slug),
        "timeline": _safe_section(_protocol_timeline, slug),
    }


def _protocol_scores(psi: dict) -> dict:
    cat_scores = psi.get("category_scores") or {}
    comp_scores = psi.get("component_scores") or {}

    # Confidence computation
    confidence = None
    confidence_tag = None
    try:
        from app.scoring_engine import compute_confidence_tag
        from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
        psi_comps_total = len(PSI_V01_DEFINITION["components"])
        psi_coverage = round(len(comp_scores) / max(psi_comps_total, 1), 2)
        psi_missing = sorted(set(PSI_V01_DEFINITION["categories"].keys()) - set(cat_scores.keys()))
        conf = compute_confidence_tag(
            len(PSI_V01_DEFINITION["categories"]) - len(psi_missing),
            len(PSI_V01_DEFINITION["categories"]),
            psi_coverage, psi_missing
        )
        confidence = conf.get("confidence")
        confidence_tag = conf.get("tag")
    except Exception as e:
        logger.warning(f"PSI confidence computation failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="ops_protocol_scores_psi_confidence_failure",
                error_message=str(e)[:500],
                cycle_phase="ops_entity_views",
            )
        except Exception:
            pass

    return {
        "overall_score": _f(psi.get("overall_score")),
        "formula_version": psi.get("formula_version"),
        "computed_at": _iso(psi.get("computed_at")),
        "confidence": confidence,
        "confidence_tag": confidence_tag,
        "categories": {k: _f(v) for k, v in cat_scores.items()},
        "components_populated": len(comp_scores),
    }


def _protocol_history(slug: str) -> dict:
    rows = fetch_all("""
        SELECT overall_score, grade, category_scores, computed_at, scored_date
        FROM psi_scores
        WHERE protocol_slug = %s
        ORDER BY computed_at DESC LIMIT 60
    """, (slug,)) or []
    return {
        "points": [
            {
                "score": _f(r["overall_score"]),
                "date": str(r["scored_date"]) if r.get("scored_date") else _iso(r.get("computed_at")),
                "categories": r.get("category_scores"),
            }
            for r in rows
        ]
    }


def _protocol_rpi(slug: str) -> dict:
    rpi = fetch_one("""
        SELECT overall_score, grade, component_scores, raw_values,
               methodology_version, computed_at
        FROM rpi_scores
        WHERE protocol_slug = %s
        ORDER BY computed_at DESC LIMIT 1
    """, (slug,))
    if not rpi:
        return {"available": False}
    return {
        "available": True,
        "overall_score": _f(rpi.get("overall_score")),
        "methodology_version": rpi.get("methodology_version"),
        "computed_at": _iso(rpi.get("computed_at")),
        "component_scores": rpi.get("component_scores") or {},
    }


def _protocol_cqi(slug: str, psi: dict) -> dict:
    rows = fetch_all("""
        SELECT sii_symbol, sii_score, psi_score, cqi_score,
               composition_method, computed_at
        FROM cqi_attestations
        WHERE psi_slug = %s
        ORDER BY computed_at DESC
    """, (slug,)) or []

    # Deduplicate — latest per sii_symbol
    seen = set()
    pairs = []
    for r in rows:
        sym = r["sii_symbol"]
        if sym not in seen:
            seen.add(sym)
            pairs.append({
                "asset": sym,
                "sii_score": _f(r["sii_score"]),
                "psi_score": _f(r["psi_score"]),
                "cqi_score": _f(r["cqi_score"]),
                "method": r.get("composition_method"),
                "computed_at": _iso(r.get("computed_at")),
            })
    return {"pairs": pairs}


def _protocol_collateral(slug: str) -> dict:
    # Treasury holdings
    treasury = fetch_all("""
        SELECT token_symbol, token_name, chain, usd_value,
               is_stablecoin, sii_score, sii_grade, snapshot_date
        FROM protocol_treasury_holdings
        WHERE protocol_slug = %s
        ORDER BY usd_value DESC
    """, (slug,)) or []

    # Collateral exposure
    collateral = fetch_all("""
        SELECT token_symbol, tvl_usd, pool_count, pool_type,
               is_stablecoin, is_sii_scored, sii_score, snapshot_date
        FROM protocol_collateral_exposure
        WHERE protocol_slug = %s
        ORDER BY tvl_usd DESC
    """, (slug,)) or []

    # Unscored gap — stablecoins in collateral without SII scores
    unscored = [c for c in collateral if c.get("is_stablecoin") and not c.get("is_sii_scored")]

    total_tvl = sum(_f(c.get("tvl_usd")) or 0 for c in collateral)
    unscored_tvl = sum(_f(c.get("tvl_usd")) or 0 for c in unscored)

    return {
        "treasury": _dict_rows(treasury),
        "collateral": _dict_rows(collateral),
        "total_tvl": total_tvl,
        "unscored_gap": {
            "count": len(unscored),
            "tvl": unscored_tvl,
            "pct": round(unscored_tvl / total_tvl * 100, 1) if total_tvl > 0 else 0,
            "assets": [c.get("token_symbol") for c in unscored],
        },
    }


def _protocol_governance(slug: str) -> dict:
    # Governance events
    events = fetch_all("""
        SELECT event_type, event_timestamp, title, description,
               outcome, contributor_tag, source, metadata
        FROM governance_events
        WHERE protocol_slug = %s
        ORDER BY event_timestamp DESC LIMIT 50
    """, (slug,)) or []

    # Governance proposals (RPI)
    proposals = fetch_all("""
        SELECT proposal_id, source, title, body_excerpt,
               is_risk_related, proposal_state, participation_rate,
               quorum_reached, created_at, closed_at
        FROM governance_proposals
        WHERE protocol_slug = %s
        ORDER BY created_at DESC LIMIT 30
    """, (slug,)) or []

    # Parameter changes
    param_changes = fetch_all("""
        SELECT parameter_type, function_signature, old_value, new_value,
               contract_address, chain, detected_at
        FROM parameter_changes
        WHERE protocol_slug = %s
        ORDER BY detected_at DESC LIMIT 20
    """, (slug,)) or []

    # Risk incidents
    incidents = fetch_all("""
        SELECT incident_date, title, description, severity,
               funds_at_risk_usd, funds_recovered_usd, source_url
        FROM risk_incidents
        WHERE protocol_slug = %s
        ORDER BY incident_date DESC LIMIT 20
    """, (slug,)) or []

    return {
        "events": _dict_rows(events),
        "proposals": _dict_rows(proposals),
        "parameter_changes": _dict_rows(param_changes),
        "incidents": _dict_rows(incidents),
    }


def _protocol_evidence(slug: str) -> dict:
    static = fetch_all("""
        SELECT entity_slug, component_slug, source_url,
               content_hash, capture_method, captured_at, is_stale
        FROM static_evidence
        WHERE index_id = 'psi' AND LOWER(entity_slug) = %s
        ORDER BY captured_at DESC LIMIT 20
    """, (slug,)) or []

    return {"static_evidence": _dict_rows(static)}


def _protocol_signals(slug: str) -> dict:
    divergence = fetch_all("""
        SELECT detector_name, signal_direction, magnitude, severity,
               detail, cycle_timestamp
        FROM divergence_signals
        WHERE LOWER(entity_id) = %s
        ORDER BY cycle_timestamp DESC LIMIT 20
    """, (slug,)) or []

    discovery = fetch_all("""
        SELECT signal_type, domain, title, description,
               severity, novelty_score, direction, magnitude,
               detected_at
        FROM discovery_signals
        WHERE entities::text ILIKE %s
        ORDER BY detected_at DESC LIMIT 20
    """, (f'%{slug}%',)) or []

    return {
        "divergence": _dict_rows(divergence),
        "discovery": _dict_rows(discovery),
    }


def _protocol_timeline(slug: str) -> dict:
    """Unified chronological stream for a protocol."""
    events = []

    # PSI score changes
    history = fetch_all("""
        SELECT overall_score, scored_date, computed_at
        FROM psi_scores
        WHERE protocol_slug = %s
        ORDER BY computed_at DESC LIMIT 30
    """, (slug,)) or []
    for r in history:
        events.append({
            "event_type": "score_change",
            "timestamp": _iso(r.get("scored_date") or r.get("computed_at")),
            "detail": f"PSI score: {_f(r.get('overall_score'))}",
        })

    # Governance events
    gov = fetch_all("""
        SELECT event_type, event_timestamp, title
        FROM governance_events
        WHERE protocol_slug = %s
        ORDER BY event_timestamp DESC LIMIT 30
    """, (slug,)) or []
    for r in gov:
        events.append({
            "event_type": "governance",
            "timestamp": _iso(r.get("event_timestamp")),
            "detail": f"[{r.get('event_type')}] {r.get('title')}",
        })

    # Parameter changes
    params = fetch_all("""
        SELECT parameter_type, old_value, new_value, detected_at
        FROM parameter_changes
        WHERE protocol_slug = %s
        ORDER BY detected_at DESC LIMIT 20
    """, (slug,)) or []
    for r in params:
        events.append({
            "event_type": "parameter_change",
            "timestamp": _iso(r.get("detected_at")),
            "detail": f"Parameter {r.get('parameter_type')}: {r.get('old_value')} → {r.get('new_value')}",
        })

    # Divergence signals
    div = fetch_all("""
        SELECT detector_name, signal_direction, magnitude, severity,
               cycle_timestamp
        FROM divergence_signals
        WHERE LOWER(entity_id) = %s
        ORDER BY cycle_timestamp DESC LIMIT 20
    """, (slug,)) or []
    for r in div:
        events.append({
            "event_type": "divergence",
            "timestamp": _iso(r.get("cycle_timestamp")),
            "detail": f"{r.get('detector_name')}: {r.get('signal_direction')} ({r.get('severity')})",
        })

    # Risk incidents
    inc = fetch_all("""
        SELECT incident_date, title, severity
        FROM risk_incidents
        WHERE protocol_slug = %s
        ORDER BY incident_date DESC LIMIT 20
    """, (slug,)) or []
    for r in inc:
        events.append({
            "event_type": "incident",
            "timestamp": _iso(r.get("incident_date")),
            "detail": f"[{r.get('severity')}] {r.get('title')}",
        })

    events.sort(key=lambda e: e.get("timestamp") or "", reverse=True)
    return {"events": events[:100]}


# ============================================================================
# WALLET ENTITY VIEW
# ============================================================================

def get_wallet_entity(address: str) -> dict | None:
    """Assemble full entity view for a wallet."""
    addr = address.strip().lower()

    # Check existence in risk scores or holdings
    risk = fetch_one("""
        SELECT wallet_address, risk_score, risk_grade,
               concentration_hhi, concentration_grade,
               unscored_pct, coverage_quality,
               num_scored_holdings, num_unscored_holdings, num_total_holdings,
               dominant_asset, dominant_asset_pct,
               total_stablecoin_value, size_tier, formula_version, computed_at
        FROM wallet_graph.wallet_risk_scores
        WHERE LOWER(wallet_address) = %s
        ORDER BY computed_at DESC LIMIT 1
    """, (addr,))

    holdings_check = fetch_one("""
        SELECT COUNT(*) AS cnt
        FROM wallet_graph.wallet_holdings
        WHERE LOWER(wallet_address) = %s
          AND indexed_at > NOW() - INTERVAL '14 days'
    """, (addr,))

    if not risk and (not holdings_check or holdings_check["cnt"] == 0):
        return None

    return {
        "entity_type": "wallet",
        "entity_id": addr,
        "address": addr,
        "profile": _safe_section(_wallet_profile, addr, risk),
        "holdings": _safe_section(_wallet_holdings, addr),
        "graph": _safe_section(_wallet_graph, addr),
        "activity": _safe_section(_wallet_activity, addr),
        "timeline": _safe_section(_wallet_timeline, addr),
    }


def _wallet_profile(addr: str, risk) -> dict:
    # Multi-chain profile
    profile = fetch_one("""
        SELECT is_contract, chains_active, total_value_all_chains,
               holdings_by_chain, edge_count_all_chains,
               risk_grade_aggregate, actor_type, agent_probability,
               created_at, updated_at
        FROM wallet_graph.wallet_profiles
        WHERE LOWER(address) = %s
    """, (addr,))

    # Actor classification
    actor = fetch_one("""
        SELECT actor_type, agent_probability, confidence,
               tx_count_basis, methodology_version, classified_at
        FROM wallet_graph.actor_classifications
        WHERE LOWER(wallet_address) = %s
    """, (addr,))

    # Treasury label
    treasury = fetch_one("""
        SELECT entity_name, entity_type, label_source,
               label_confidence, wallet_purpose, monitoring_enabled
        FROM wallet_graph.treasury_registry
        WHERE LOWER(address) = %s
    """, (addr,))

    result = {
        "risk_score": _f(risk["risk_score"]) if risk and risk.get("risk_score") is not None else None,
        "concentration_hhi": _f(risk["concentration_hhi"]) if risk and risk.get("concentration_hhi") else None,
        "unscored_pct": _f(risk["unscored_pct"]) if risk and risk.get("unscored_pct") else None,
        "coverage_quality": risk.get("coverage_quality") if risk else None,
        "dominant_asset": risk.get("dominant_asset") if risk else None,
        "dominant_asset_pct": _f(risk["dominant_asset_pct"]) if risk and risk.get("dominant_asset_pct") else None,
        "size_tier": risk.get("size_tier") if risk else None,
        "total_stablecoin_value": _f(risk["total_stablecoin_value"]) if risk and risk.get("total_stablecoin_value") else None,
        "formula_version": risk.get("formula_version") if risk else None,
        "computed_at": _iso(risk.get("computed_at")) if risk else None,
    }

    if profile:
        result["is_contract"] = profile.get("is_contract")
        result["chains_active"] = profile.get("chains_active")
        result["total_value_all_chains"] = _f(profile.get("total_value_all_chains"))
        result["edge_count"] = profile.get("edge_count_all_chains")

    if actor:
        result["actor_type"] = actor.get("actor_type")
        result["agent_probability"] = _f(actor.get("agent_probability"))
        result["actor_confidence"] = actor.get("confidence")

    if treasury:
        result["treasury_label"] = {
            "entity_name": treasury.get("entity_name"),
            "entity_type": treasury.get("entity_type"),
            "purpose": treasury.get("wallet_purpose"),
            "label_source": treasury.get("label_source"),
        }

    return result


def _wallet_holdings(addr: str) -> dict:
    rows = fetch_all("""
        SELECT token_address, symbol, chain, balance, value_usd,
               is_scored, sii_score, sii_grade, pct_of_wallet, indexed_at
        FROM wallet_graph.wallet_holdings
        WHERE LOWER(wallet_address) = %s
          AND indexed_at > NOW() - INTERVAL '14 days'
          AND value_usd >= 0.01
        ORDER BY value_usd DESC
    """, (addr,)) or []

    total_value = sum(_f(h.get("value_usd")) or 0 for h in rows)
    scored_count = sum(1 for h in rows if h.get("is_scored"))

    holdings = []
    for h in rows:
        val = _f(h.get("value_usd")) or 0
        holdings.append({
            "symbol": h.get("symbol"),
            "chain": h.get("chain", "ethereum"),
            "value_usd": val,
            "pct_of_wallet": round(val / total_value * 100, 2) if total_value > 0 else 0,
            "is_scored": h.get("is_scored"),
            "sii_score": _f(h.get("sii_score")),
            "indexed_at": _iso(h.get("indexed_at")),
        })

    return {
        "total_value": round(total_value, 2),
        "count": len(holdings),
        "scored_count": scored_count,
        "unscored_count": len(holdings) - scored_count,
        "items": holdings,
    }


def _wallet_graph(addr: str) -> dict:
    # Outgoing edges
    outgoing = fetch_all("""
        SELECT to_address, transfer_count, total_value_usd,
               first_transfer_at, last_transfer_at, tokens_transferred
        FROM wallet_graph.wallet_edges
        WHERE LOWER(from_address) = %s
        ORDER BY total_value_usd DESC LIMIT 25
    """, (addr,)) or []

    # Incoming edges
    incoming = fetch_all("""
        SELECT from_address, transfer_count, total_value_usd,
               first_transfer_at, last_transfer_at, tokens_transferred
        FROM wallet_graph.wallet_edges
        WHERE LOWER(to_address) = %s
        ORDER BY total_value_usd DESC LIMIT 25
    """, (addr,)) or []

    return {
        "outgoing": _dict_rows(outgoing),
        "incoming": _dict_rows(incoming),
        "total_connections": len(outgoing) + len(incoming),
    }


def _wallet_activity(addr: str) -> dict:
    # x402 payment history
    payments = fetch_all("""
        SELECT endpoint, price_usd, protocol, tx_hash,
               verified, timestamp
        FROM payment_log
        WHERE LOWER(payer_address) = %s
        ORDER BY timestamp DESC LIMIT 20
    """, (addr,)) or []

    # Assessment events for this wallet
    assessments = fetch_all("""
        SELECT trigger_type, trigger_detail, wallet_risk_score,
               wallet_risk_grade, severity, created_at
        FROM assessment_events
        WHERE LOWER(wallet_address) = %s
        ORDER BY created_at DESC LIMIT 20
    """, (addr,)) or []

    # Treasury events
    treasury_events = fetch_all("""
        SELECT event_type, event_data, severity, confidence,
               stablecoins_involved, protocols_involved,
               risk_score_before, risk_score_after, detected_at
        FROM wallet_graph.treasury_events
        WHERE LOWER(wallet_address) = %s
        ORDER BY detected_at DESC LIMIT 20
    """, (addr,)) or []

    return {
        "payments": _dict_rows(payments),
        "assessments": _dict_rows(assessments),
        "treasury_events": _dict_rows(treasury_events),
    }


def _wallet_timeline(addr: str) -> dict:
    """Unified chronological stream for a wallet."""
    events = []

    # Assessment events
    assessments = fetch_all("""
        SELECT trigger_type, wallet_risk_score, wallet_risk_score_prev,
               severity, created_at
        FROM assessment_events
        WHERE LOWER(wallet_address) = %s
        ORDER BY created_at DESC LIMIT 30
    """, (addr,)) or []
    for r in assessments:
        prev = _f(r.get("wallet_risk_score_prev"))
        curr = _f(r.get("wallet_risk_score"))
        delta = ""
        if prev is not None and curr is not None:
            delta = f" ({curr - prev:+.1f})"
        events.append({
            "event_type": "assessment",
            "timestamp": _iso(r.get("created_at")),
            "detail": f"[{r.get('severity')}] {r.get('trigger_type')}: score {curr}{delta}",
        })

    # Treasury events
    treasury = fetch_all("""
        SELECT event_type, severity, stablecoins_involved,
               risk_score_before, risk_score_after, detected_at
        FROM wallet_graph.treasury_events
        WHERE LOWER(wallet_address) = %s
        ORDER BY detected_at DESC LIMIT 20
    """, (addr,)) or []
    for r in treasury:
        events.append({
            "event_type": "treasury",
            "timestamp": _iso(r.get("detected_at")),
            "detail": f"[{r.get('severity')}] {r.get('event_type')}: {r.get('stablecoins_involved')}",
        })

    # Payment activity
    payments = fetch_all("""
        SELECT endpoint, price_usd, timestamp
        FROM payment_log
        WHERE LOWER(payer_address) = %s
        ORDER BY timestamp DESC LIMIT 20
    """, (addr,)) or []
    for r in payments:
        events.append({
            "event_type": "payment",
            "timestamp": _iso(r.get("timestamp")),
            "detail": f"x402 payment: ${_f(r.get('price_usd')) or 0:.4f} for {r.get('endpoint')}",
        })

    events.sort(key=lambda e: e.get("timestamp") or "", reverse=True)
    return {"events": events[:100]}
