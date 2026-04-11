"""
Query Engine — Structured queries against the wallet risk graph.
================================================================
Provides parameterized, safe querying with hard limits to prevent
expensive operations. All filters are whitelist-validated.
"""

import logging
import time
from typing import Any

from app.database import fetch_all, fetch_one

logger = logging.getLogger(__name__)

# Hard limits
MAX_LIMIT = 200
ALLOWED_SORT_FIELDS = [
    "risk_score", "concentration_hhi", "total_stablecoin_value",
    "computed_at", "dominant_asset_pct",
]
ALLOWED_ENTITIES = ["wallets"]

# Filter definitions: (request_field, db_column)
_RANGE_FILTERS = [
    ("risk_score", "wrs.risk_score"),
    ("concentration_hhi", "wrs.concentration_hhi"),
    ("total_value_usd", "wrs.total_stablecoin_value"),
    ("dominant_asset_pct", "wrs.dominant_asset_pct"),
    ("num_holdings", "wrs.num_total_holdings"),
]
_STRING_FILTERS = [
    ("size_tier", "wrs.size_tier"),
    ("dominant_asset", "wrs.dominant_asset"),
    ("coverage_quality", "wrs.coverage_quality"),
]
_SORT_COL_MAP = {
    "risk_score": "wrs.risk_score",
    "concentration_hhi": "wrs.concentration_hhi",
    "total_stablecoin_value": "wrs.total_stablecoin_value",
    "computed_at": "wrs.computed_at",
    "dominant_asset_pct": "wrs.dominant_asset_pct",
}


def execute_query(query: dict) -> dict:
    """Execute a structured query against the wallet graph."""
    start = time.time()

    entity = query.get("entity", "wallets")
    if entity not in ALLOWED_ENTITIES:
        return {"error": f"Unknown entity: {entity}. Allowed: {ALLOWED_ENTITIES}"}

    filters = query.get("filters", {})
    sort = query.get("sort", {"field": "risk_score", "order": "desc"})
    limit = min(max(query.get("limit", 50), 1), MAX_LIMIT)
    offset = max(query.get("offset", 0), 0)
    include_holdings = query.get("include_holdings", False)

    # Validate sort
    sort_field = sort.get("field", "risk_score")
    if sort_field not in ALLOWED_SORT_FIELDS:
        sort_field = "risk_score"
    sort_order = "ASC" if sort.get("order", "desc").lower() == "asc" else "DESC"
    sort_col = _SORT_COL_MAP.get(sort_field, "wrs.risk_score")

    # Build WHERE clauses (all parameterized)
    where_clauses: list[str] = []
    params: list[Any] = []

    # Numeric range filters
    for field, db_col in _RANGE_FILTERS:
        if field not in filters:
            continue
        val = filters[field]
        if isinstance(val, dict):
            if "min" in val:
                where_clauses.append(f"{db_col} >= %s")
                params.append(val["min"])
            if "max" in val:
                where_clauses.append(f"{db_col} <= %s")
                params.append(val["max"])
        elif isinstance(val, (int, float)):
            where_clauses.append(f"{db_col} = %s")
            params.append(val)

    # String exact-match filters
    for field, db_col in _STRING_FILTERS:
        if field not in filters:
            continue
        val = filters[field]
        if isinstance(val, str):
            where_clauses.append(f"UPPER({db_col}) = UPPER(%s)")
            params.append(val)

    # Grade filter (string or list)
    if "risk_grade" in filters:
        val = filters["risk_grade"]
        if isinstance(val, str):
            where_clauses.append("wrs.risk_grade = %s")
            params.append(val)
        elif isinstance(val, list) and val:
            placeholders = ", ".join(["%s"] * len(val))
            where_clauses.append(f"wrs.risk_grade IN ({placeholders})")
            params.extend(val)

    where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

    # Subquery: latest score per wallet
    latest_scores_cte = """
        SELECT DISTINCT ON (wallet_address) *
        FROM wallet_graph.wallet_risk_scores
        ORDER BY wallet_address, computed_at DESC
    """

    # Count total matching
    count_sql = f"""
        SELECT COUNT(*) AS total
        FROM ({latest_scores_cte}) wrs
        WHERE {where_sql}
    """
    count_row = fetch_one(count_sql, tuple(params))
    total_matching = count_row["total"] if count_row else 0

    # Fetch results
    data_sql = f"""
        SELECT wrs.wallet_address, wrs.risk_score, wrs.risk_grade,
               wrs.concentration_hhi, wrs.total_stablecoin_value,
               wrs.size_tier, wrs.dominant_asset, wrs.dominant_asset_pct,
               wrs.coverage_quality, wrs.num_total_holdings, wrs.computed_at
        FROM ({latest_scores_cte}) wrs
        WHERE {where_sql}
        ORDER BY {sort_col} {sort_order} NULLS LAST
        LIMIT %s OFFSET %s
    """
    rows = fetch_all(data_sql, tuple(params + [limit, offset]))

    elapsed_ms = int((time.time() - start) * 1000)

    results = []
    for r in rows:
        results.append({
            "address": r["wallet_address"],
            "risk_score": float(r["risk_score"]) if r.get("risk_score") is not None else None,
            "concentration_hhi": float(r["concentration_hhi"]) if r.get("concentration_hhi") is not None else None,
            "total_value_usd": float(r["total_stablecoin_value"]) if r.get("total_stablecoin_value") is not None else None,
            "size_tier": r.get("size_tier"),
            "dominant_asset": r.get("dominant_asset"),
            "dominant_asset_pct": float(r["dominant_asset_pct"]) if r.get("dominant_asset_pct") is not None else None,
            "coverage_quality": r.get("coverage_quality"),
            "num_holdings": r.get("num_total_holdings"),
        })

    # Optionally include holdings (capped at 20 wallets)
    if include_holdings and results:
        for entry in results[:20]:
            holdings = fetch_all("""
                SELECT symbol, value_usd, pct_of_wallet, sii_score
                FROM wallet_graph.wallet_holdings
                WHERE wallet_address = %s
                  AND indexed_at = (
                      SELECT MAX(indexed_at)
                      FROM wallet_graph.wallet_holdings
                      WHERE wallet_address = %s
                  )
                ORDER BY value_usd DESC
            """, (entry["address"], entry["address"]))
            entry["holdings"] = [
                {
                    "symbol": h["symbol"],
                    "value_usd": float(h["value_usd"]) if h.get("value_usd") else None,
                    "pct": float(h["pct_of_wallet"]) if h.get("pct_of_wallet") else None,
                    "sii_score": float(h["sii_score"]) if h.get("sii_score") else None,
                }
                for h in holdings
            ]

    return {
        "query": {
            "entity": entity,
            "filters_applied": len(where_clauses),
            "total_matching": total_matching,
            "returned": len(results),
            "offset": offset,
            "sort": {"field": sort_field, "order": sort_order.lower()},
        },
        "results": results,
        "meta": {
            "query_time_ms": elapsed_ms,
            "version": "query-v1.0.0",
        },
    }


# Schema documentation returned by GET /api/query/schema
QUERY_SCHEMA = {
    "version": "query-v1.0.0",
    "entities": {
        "wallets": {
            "description": "Query the wallet risk graph",
            "filters": {
                "risk_score": {
                    "type": "range",
                    "description": "Risk score 0-100",
                    "example": {"min": 60, "max": 85},
                },
                "concentration_hhi": {
                    "type": "range",
                    "description": "Herfindahl index 0-10000",
                    "example": {"min": 3000},
                },
                "total_value_usd": {
                    "type": "range",
                    "description": "Total stablecoin value in USD",
                    "example": {"min": 1000000},
                },
                "dominant_asset_pct": {
                    "type": "range",
                    "description": "% of wallet in dominant asset",
                    "example": {"min": 80},
                },
                "num_holdings": {
                    "type": "range",
                    "description": "Number of stablecoin holdings",
                    "example": {"min": 2},
                },
                "size_tier": {
                    "type": "exact",
                    "values": ["whale", "institutional", "retail"],
                },
                "dominant_asset": {
                    "type": "exact",
                    "description": "Symbol of dominant holding",
                    "example": "USDT",
                },
                "coverage_quality": {
                    "type": "exact",
                    "values": ["full", "high", "partial", "low"],
                },
            },
            "sort_fields": ALLOWED_SORT_FIELDS,
            "max_limit": MAX_LIMIT,
            "options": {
                "include_holdings": {
                    "type": "boolean",
                    "description": "Include holdings for each wallet (capped at 20 wallets)",
                    "default": False,
                },
            },
        },
    },
}
