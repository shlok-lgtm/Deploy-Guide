"""
Query Templates v1.5 — Pre-built Parameterized Queries
========================================================
Named templates that answer common risk intelligence questions.
All queries are parameterized (no SQL injection). Results capped at 200 rows.
"""

import logging
from typing import Any

from app.database import fetch_all

logger = logging.getLogger(__name__)

MAX_RESULTS = 200

TEMPLATES = {
    "high_risk_whales": {
        "description": "Wallets with high value AND low risk scores",
        "params": {
            "min_value": {"type": "number", "default": 1_000_000, "description": "Minimum stablecoin value (USD)"},
            "max_score": {"type": "number", "default": 60, "description": "Maximum risk score threshold (wallets scoring at or below this are included)"},
            "limit": {"type": "integer", "default": 50, "description": "Max results"},
        },
        "sql": """
            SELECT DISTINCT ON (w.address)
                w.address, w.chain, w.total_stablecoin_value, w.size_tier,
                rs.risk_score, rs.concentration_hhi,
                rs.dominant_asset, rs.dominant_asset_pct
            FROM wallet_graph.wallets w
            JOIN wallet_graph.wallet_risk_scores rs ON w.address = rs.wallet_address
            WHERE w.total_stablecoin_value >= %(min_value)s
              AND rs.risk_score IS NOT NULL
              AND rs.risk_score <= %(max_score)s
            ORDER BY w.address, rs.computed_at DESC
            LIMIT %(limit)s
        """,
    },

    "contagion_hotspots": {
        "description": "Wallets with the most counterparty connections weighted by exposure",
        "params": {
            "min_connections": {"type": "integer", "default": 5, "description": "Minimum edge count"},
            "limit": {"type": "integer", "default": 50, "description": "Max results"},
        },
        "sql": """
            SELECT
                from_address AS address,
                COUNT(*) AS connection_count,
                SUM(total_value_usd) AS total_exposure,
                AVG(weight) AS avg_weight
            FROM wallet_graph.wallet_edges
            GROUP BY from_address
            HAVING COUNT(*) >= %(min_connections)s
            ORDER BY total_exposure DESC
            LIMIT %(limit)s
        """,
    },

    "stablecoin_concentration": {
        "description": "Per-stablecoin holder concentration — which stablecoins are held by the fewest wallets",
        "params": {
            "min_holders": {"type": "integer", "default": 10, "description": "Minimum holders to include"},
        },
        "sql": """
            SELECT
                h.symbol,
                COUNT(DISTINCT h.wallet_address) AS holder_count,
                SUM(h.value_usd) AS total_value,
                MAX(h.value_usd) AS max_single_holding,
                ROUND(MAX(h.value_usd) / NULLIF(SUM(h.value_usd), 0) * 100, 2) AS top_holder_pct
            FROM wallet_graph.wallet_holdings h
            WHERE h.indexed_at > NOW() - INTERVAL '7 days'
              AND h.value_usd > 0
            GROUP BY h.symbol
            HAVING COUNT(DISTINCT h.wallet_address) >= %(min_holders)s
            ORDER BY top_holder_pct DESC
        """,
    },

    "score_movers": {
        "description": "Stablecoins whose SII score changed the most over N days",
        "params": {
            "days": {"type": "integer", "default": 7, "description": "Lookback period in days"},
            "min_change": {"type": "number", "default": 2.0, "description": "Minimum absolute score change"},
            "limit": {"type": "integer", "default": 20, "description": "Max results"},
        },
        "sql": """
            WITH latest AS (
                SELECT stablecoin, overall_score, score_date
                FROM score_history
                WHERE score_date = (SELECT MAX(score_date) FROM score_history)
            ),
            previous AS (
                SELECT DISTINCT ON (stablecoin)
                    stablecoin, overall_score, score_date
                FROM score_history
                WHERE score_date <= (SELECT MAX(score_date) FROM score_history) - %(days)s
                ORDER BY stablecoin, score_date DESC
            )
            SELECT
                l.stablecoin,
                l.overall_score AS current_score,
                p.overall_score AS previous_score,
                ROUND((l.overall_score - p.overall_score)::numeric, 2) AS score_change,
                l.score_date AS as_of
            FROM latest l
            JOIN previous p ON l.stablecoin = p.stablecoin
            WHERE ABS(l.overall_score - p.overall_score) >= %(min_change)s
            ORDER BY ABS(l.overall_score - p.overall_score) DESC
            LIMIT %(limit)s
        """,
    },

    "disclosure_gaps": {
        "description": "Issuers with the oldest attestation documents — disclosure freshness",
        "params": {
            "min_days_stale": {"type": "integer", "default": 30, "description": "Minimum days since last extraction"},
            "limit": {"type": "integer", "default": 20, "description": "Max results"},
        },
        "sql": """
            SELECT
                r.asset_symbol,
                r.issuer_name,
                r.collection_method,
                MAX(e.extracted_at) AS last_extraction,
                EXTRACT(DAY FROM NOW() - MAX(e.extracted_at))::integer AS days_stale,
                COUNT(e.id) AS total_extractions
            FROM cda_issuer_registry r
            LEFT JOIN cda_vendor_extractions e ON UPPER(r.asset_symbol) = UPPER(e.asset_symbol)
            WHERE r.is_active = TRUE
            GROUP BY r.asset_symbol, r.issuer_name, r.collection_method
            HAVING MAX(e.extracted_at) IS NULL
               OR EXTRACT(DAY FROM NOW() - MAX(e.extracted_at)) >= %(min_days_stale)s
            ORDER BY days_stale DESC NULLS FIRST
            LIMIT %(limit)s
        """,
    },

    "cross_chain_exposure": {
        "description": "Wallets active on multiple chains with aggregate risk view",
        "params": {
            "min_chains": {"type": "integer", "default": 2, "description": "Minimum number of chains active"},
            "limit": {"type": "integer", "default": 50, "description": "Max results"},
        },
        "sql": """
            SELECT
                address,
                chains_active,
                total_value_all_chains,
                holdings_by_chain,
                edge_count_all_chains
            FROM wallet_graph.wallet_profiles
            WHERE jsonb_array_length(chains_active) >= %(min_chains)s
            ORDER BY total_value_all_chains DESC
            LIMIT %(limit)s
        """,
    },
}


def list_templates() -> list[dict]:
    """Return all available templates with descriptions and default params."""
    result = []
    for name, tmpl in TEMPLATES.items():
        params = {}
        for pname, pspec in tmpl["params"].items():
            params[pname] = {
                "type": pspec["type"],
                "default": pspec["default"],
                "description": pspec.get("description", ""),
            }
        result.append({
            "name": name,
            "description": tmpl["description"],
            "params": params,
        })
    return result


def execute_template(template_name: str, params: dict[str, Any] = None) -> dict:
    """
    Execute a named query template with optional parameter overrides.
    Returns {template, params_used, results, count}.
    """
    tmpl = TEMPLATES.get(template_name)
    if not tmpl:
        available = list(TEMPLATES.keys())
        return {"error": f"Unknown template: {template_name}. Available: {available}"}

    # Merge defaults with provided params
    merged = {}
    for pname, pspec in tmpl["params"].items():
        if params and pname in params:
            merged[pname] = params[pname]
        else:
            merged[pname] = pspec["default"]

    # Validate types
    for pname, pspec in tmpl["params"].items():
        val = merged[pname]
        expected = pspec["type"]
        if expected == "number" and not isinstance(val, (int, float)):
            return {"error": f"Parameter '{pname}' must be a number, got {type(val).__name__}"}
        if expected == "integer" and not isinstance(val, int):
            return {"error": f"Parameter '{pname}' must be an integer, got {type(val).__name__}"}
        if expected == "string" and not isinstance(val, str):
            return {"error": f"Parameter '{pname}' must be a string, got {type(val).__name__}"}

    # Enforce limit cap
    if "limit" in merged:
        merged["limit"] = min(int(merged["limit"]), MAX_RESULTS)

    try:
        rows = fetch_all(tmpl["sql"], merged)
        # Cap results
        rows = rows[:MAX_RESULTS]

        # Convert to serializable dicts
        results = []
        for row in rows:
            entry = {}
            for k, v in dict(row).items():
                if hasattr(v, "isoformat"):
                    entry[k] = v.isoformat()
                elif isinstance(v, float) and v != v:  # NaN
                    entry[k] = None
                else:
                    entry[k] = v
            results.append(entry)

        return {
            "template": template_name,
            "params_used": merged,
            "results": results,
            "count": len(results),
        }
    except Exception as e:
        logger.error(f"Template query failed [{template_name}]: {e}")
        return {"error": f"Query execution failed: {str(e)}"}


