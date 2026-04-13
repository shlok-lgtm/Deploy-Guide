"""
State Growth Dashboard
=======================
Comprehensive dashboard of accumulated state across ALL tables —
both core platform tables and the universal data layer.

GET /api/ops/state-growth-live (live queries across all tables)
GET /api/ops/state-growth (existing — reads from daily_pulses history)
"""

import logging
from datetime import datetime, timezone

from app.database import fetch_all, fetch_one

logger = logging.getLogger(__name__)

# All tables to track — core + universal data layer + wallet graph + ops
# Grouped by category for the dashboard
TRACKED_TABLES = {
    # === Core scoring ===
    "scores":                   {"time_col": "calculated_at",  "avg_row_bytes": 400, "category": "core"},
    "score_history":            {"time_col": "created_at",     "avg_row_bytes": 300, "category": "core"},
    "component_readings":       {"time_col": "collected_at",   "avg_row_bytes": 150, "category": "core"},
    "psi_scores":               {"time_col": "scored_at",      "avg_row_bytes": 300, "category": "core"},
    "rpi_scores":               {"time_col": "computed_at",    "avg_row_bytes": 300, "category": "core"},
    "generic_index_scores":     {"time_col": "computed_at",    "avg_row_bytes": 300, "category": "core"},

    # === CDA / Compliance ===
    "cda_vendor_extractions":   {"time_col": "extracted_at",   "avg_row_bytes": 2000, "category": "cda"},
    "cda_source_urls":          {"time_col": "discovered_at",  "avg_row_bytes": 200,  "category": "cda"},

    # === Wallet graph ===
    "wallet_graph.wallets":             {"time_col": "created_at",     "avg_row_bytes": 100, "category": "wallet"},
    "wallet_graph.wallet_holdings":     {"time_col": "updated_at",     "avg_row_bytes": 120, "category": "wallet"},
    "wallet_graph.wallet_risk_scores":  {"time_col": "computed_at",    "avg_row_bytes": 200, "category": "wallet"},
    "wallet_graph.wallet_edges":        {"time_col": "updated_at",     "avg_row_bytes": 150, "category": "wallet"},
    "wallet_graph.wallet_profiles":     {"time_col": "computed_at",    "avg_row_bytes": 500, "category": "wallet"},
    "wallet_graph.unscored_assets":     {"time_col": "last_seen_at",   "avg_row_bytes": 150, "category": "wallet"},
    "wallet_graph.edge_build_status":   {"time_col": "last_built_at",  "avg_row_bytes": 80,  "category": "wallet"},
    "wallet_graph.treasury_events":     {"time_col": "detected_at",    "avg_row_bytes": 300, "category": "wallet"},

    # === Universal Data Layer — Tier 1-8 ===
    "liquidity_depth":          {"time_col": "snapshot_at",    "avg_row_bytes": 300, "category": "data_layer"},
    "yield_snapshots":          {"time_col": "snapshot_at",    "avg_row_bytes": 250, "category": "data_layer"},
    "governance_proposals":     {"time_col": "collected_at",   "avg_row_bytes": 2000, "category": "data_layer"},
    "governance_voters":        {"time_col": "collected_at",   "avg_row_bytes": 150, "category": "data_layer"},
    "bridge_flows":             {"time_col": "snapshot_at",    "avg_row_bytes": 200, "category": "data_layer"},
    "exchange_snapshots":       {"time_col": "snapshot_at",    "avg_row_bytes": 500, "category": "data_layer"},
    "correlation_matrices":     {"time_col": "computed_at",    "avg_row_bytes": 5000, "category": "data_layer"},
    "volatility_surfaces":      {"time_col": "computed_at",    "avg_row_bytes": 200, "category": "data_layer"},
    "incident_events":          {"time_col": "created_at",     "avg_row_bytes": 1000, "category": "data_layer"},
    "peg_snapshots_5m":         {"time_col": "timestamp",      "avg_row_bytes": 80,  "category": "data_layer"},
    "mint_burn_events":         {"time_col": "collected_at",   "avg_row_bytes": 300, "category": "data_layer"},
    "entity_snapshots_hourly":  {"time_col": "snapshot_at",    "avg_row_bytes": 1000, "category": "data_layer"},
    "contract_surveillance":    {"time_col": "scanned_at",     "avg_row_bytes": 2000, "category": "data_layer"},
    "wallet_behavior_tags":     {"time_col": "computed_at",    "avg_row_bytes": 200, "category": "data_layer"},
    "dex_pool_ohlcv":           {"time_col": "timestamp",      "avg_row_bytes": 150, "category": "data_layer"},
    "market_chart_history":     {"time_col": "timestamp",      "avg_row_bytes": 100, "category": "data_layer"},

    # === Protocol discovery + pool wallets ===
    "protocol_pool_wallets":    {"time_col": "discovered_at",  "avg_row_bytes": 100, "category": "discovery"},
    "protocol_backlog":         {"time_col": "last_seen_at",   "avg_row_bytes": 200, "category": "discovery"},
    "protocol_collateral_exposure": {"time_col": "snapshot_date", "avg_row_bytes": 150, "category": "discovery"},
    "discovery_signals":        {"time_col": "detected_at",    "avg_row_bytes": 500, "category": "discovery"},

    # === Assessment + Pulse ===
    "assessment_events":        {"time_col": "detected_at",    "avg_row_bytes": 500, "category": "assessment"},
    "daily_pulses":             {"time_col": "generated_at",   "avg_row_bytes": 5000, "category": "assessment"},

    # === Governance (legacy) ===
    "governance_documents":     {"time_col": "created_at",     "avg_row_bytes": 2000, "category": "governance"},
    "governance_events":        {"time_col": "created_at",     "avg_row_bytes": 500,  "category": "governance"},

    # === Infrastructure ===
    "state_attestations":       {"time_col": "cycle_timestamp", "avg_row_bytes": 200, "category": "infra"},
    "provenance_proofs":        {"time_col": "proved_at",       "avg_row_bytes": 300, "category": "infra"},
    "coherence_violations":     {"time_col": "created_at",      "avg_row_bytes": 300, "category": "infra"},
    "coherence_reports":        {"time_col": "created_at",      "avg_row_bytes": 1000, "category": "infra"},
    "collector_cycle_stats":    {"time_col": "created_at",      "avg_row_bytes": 100, "category": "infra"},
    "data_catalog":             {"time_col": "updated_at",      "avg_row_bytes": 500, "category": "infra"},
    "api_usage_tracker":        {"time_col": "recorded_at",     "avg_row_bytes": 150, "category": "infra"},
    "api_usage_hourly":         {"time_col": "hour",            "avg_row_bytes": 200, "category": "infra"},
    "integrity_checks":         {"time_col": "checked_at",      "avg_row_bytes": 300, "category": "infra"},
    "report_attestations":      {"time_col": "generated_at",    "avg_row_bytes": 400, "category": "infra"},

    # === RPI pipeline ===
    "rpi_components":           {"time_col": "collected_at",   "avg_row_bytes": 200, "category": "rpi"},
    "rpi_protocol_config":      {"time_col": "created_at",     "avg_row_bytes": 300, "category": "rpi"},
    "rpi_score_history":        {"time_col": "computed_at",    "avg_row_bytes": 200, "category": "rpi"},
    "rpi_doc_scores":           {"time_col": "scored_at",      "avg_row_bytes": 300, "category": "rpi"},

    # === Exchange health ===
    "exchange_health_checks":   {"time_col": "checked_at",     "avg_row_bytes": 100, "category": "exchange"},

    # === Historical / reconstruction ===
    "historical_prices":        {"time_col": "recorded_at",    "avg_row_bytes": 80,  "category": "historical"},
    "score_events":             {"time_col": "created_at",     "avg_row_bytes": 300, "category": "historical"},
    "data_provenance":          {"time_col": "recorded_at",    "avg_row_bytes": 200, "category": "historical"},
}


def _safe_count(query: str, params: tuple = ()) -> int:
    try:
        row = fetch_one(query, params)
        return int(row["cnt"]) if row and row.get("cnt") else 0
    except Exception:
        return 0


def _safe_fetch(query: str, params: tuple = ()):
    try:
        return fetch_one(query, params)
    except Exception:
        return None


def get_state_growth() -> dict:
    """Comprehensive state growth dashboard — live queries across all tables."""
    now = datetime.now(timezone.utc)

    # =========================================================================
    # 1. Per-table row counts and growth (grouped by category)
    # =========================================================================
    tables = {}
    by_category = {}
    total_rows = 0
    total_bytes = 0
    total_rows_24h = 0

    for table_name, config in TRACKED_TABLES.items():
        tc = config["time_col"]
        arb = config["avg_row_bytes"]
        cat = config.get("category", "other")

        row_count = _safe_count(f"SELECT COUNT(*) as cnt FROM {table_name}")
        rows_24h = _safe_count(
            f"SELECT COUNT(*) as cnt FROM {table_name} WHERE {tc} >= NOW() - INTERVAL '24 hours'"
        )
        rows_7d = _safe_count(
            f"SELECT COUNT(*) as cnt FROM {table_name} WHERE {tc} >= NOW() - INTERVAL '7 days'"
        )

        growth_rate = round(rows_7d / 7, 1) if rows_7d else 0
        est_monthly_bytes = growth_rate * 30 * arb

        entry = {
            "row_count": row_count,
            "rows_24h": rows_24h,
            "rows_7d": rows_7d,
            "growth_rate_per_day": growth_rate,
            "est_monthly_mb": round(est_monthly_bytes / 1_000_000, 2),
            "category": cat,
        }
        tables[table_name] = entry

        by_category.setdefault(cat, {"tables": 0, "rows": 0, "rows_24h": 0})
        by_category[cat]["tables"] += 1
        by_category[cat]["rows"] += row_count
        by_category[cat]["rows_24h"] += rows_24h

        total_rows += row_count
        total_rows_24h += rows_24h
        total_bytes += row_count * arb

    # =========================================================================
    # 2. Wallet graph growth
    # =========================================================================
    wallet_total = _safe_count("SELECT COUNT(*) as cnt FROM wallet_graph.wallets")
    wallet_24h = _safe_count(
        "SELECT COUNT(*) as cnt FROM wallet_graph.wallets WHERE created_at >= NOW() - INTERVAL '24 hours'"
    )
    wallet_7d = _safe_count(
        "SELECT COUNT(*) as cnt FROM wallet_graph.wallets WHERE created_at >= NOW() - INTERVAL '7 days'"
    )
    wallet_30d = _safe_count(
        "SELECT COUNT(*) as cnt FROM wallet_graph.wallets WHERE created_at >= NOW() - INTERVAL '30 days'"
    )
    wallets_with_scores = _safe_count("SELECT COUNT(*) as cnt FROM wallet_graph.wallet_risk_scores")
    wallets_with_edges = _safe_count(
        "SELECT COUNT(DISTINCT source_address) as cnt FROM wallet_graph.wallet_edges"
    )
    wallets_with_tags = _safe_count(
        "SELECT COUNT(DISTINCT wallet_address) as cnt FROM wallet_behavior_tags"
    )
    wallets_with_holdings = _safe_count(
        "SELECT COUNT(DISTINCT wallet_address) as cnt FROM wallet_graph.wallet_holdings"
    )

    daily_growth = round(wallet_7d / 7) if wallet_7d else 0
    days_to_500k = round((500_000 - wallet_total) / daily_growth) if daily_growth > 0 else None

    wallet_graph = {
        "total_wallets": wallet_total,
        "wallets_added_24h": wallet_24h,
        "wallets_added_7d": wallet_7d,
        "wallets_added_30d": wallet_30d,
        "wallets_with_balance_snapshots": wallets_with_holdings,
        "wallets_with_risk_scores": wallets_with_scores,
        "wallets_with_edges": wallets_with_edges,
        "wallets_with_behavior_tags": wallets_with_tags,
        "fully_enriched_pct": round(
            min(wallets_with_scores, wallets_with_edges, wallets_with_holdings)
            / wallet_total * 100, 1
        ) if wallet_total > 0 else 0,
        "daily_growth_rate": daily_growth,
        "target": 500_000,
        "days_to_target": days_to_500k,
        "projected_target_date": (
            str((now + __import__("datetime").timedelta(days=days_to_500k)).date())
            if days_to_500k and days_to_500k > 0 else None
        ),
    }

    # =========================================================================
    # 3. Entity coverage
    # =========================================================================
    sii_scored = _safe_count("SELECT COUNT(DISTINCT stablecoin_id) as cnt FROM scores")
    sii_total = _safe_count("SELECT COUNT(*) as cnt FROM stablecoins WHERE scoring_enabled = TRUE")
    psi_scored = _safe_count("SELECT COUNT(DISTINCT protocol_slug) as cnt FROM psi_scores")
    psi_discovered = _safe_count(
        "SELECT COUNT(*) as cnt FROM protocol_backlog WHERE enrichment_status != 'insufficient'"
    )
    rpi_scored = _safe_count("SELECT COUNT(DISTINCT protocol_slug) as cnt FROM rpi_scores")

    # Circle 7 per-index
    circle7 = {}
    for index_id in ["lsti", "bri", "dohi", "vsri", "cxri", "tti"]:
        count = _safe_count(
            "SELECT COUNT(DISTINCT entity_id) as cnt FROM generic_index_scores WHERE index_id = %s",
            (index_id,),
        )
        circle7[index_id] = count

    entity_coverage = {
        "sii": {"scored": sii_scored, "total_enabled": sii_total},
        "psi": {"scored": psi_scored, "discovered": psi_discovered},
        "rpi": {"scored": rpi_scored},
        "circle7": circle7,
        "total_scored_entities": sii_scored + psi_scored + rpi_scored + sum(circle7.values()),
    }

    # =========================================================================
    # 4. API utilization with budget context
    # =========================================================================
    api_utilization = {}
    BUDGETS = {
        "coingecko": {"daily": 16_600, "monthly": 500_000, "plan": "Analyst"},
        "etherscan": {"daily": 200_000, "monthly": None, "plan": "Standard (10/s)"},
        "blockscout": {"daily": 100_000, "monthly": None, "plan": "Free (5/s, shared)"},
        "defillama": {"daily": None, "monthly": None, "plan": "Free"},
        "snapshot": {"daily": None, "monthly": None, "plan": "Free"},
    }
    try:
        from app.api_usage_tracker import get_realtime_counters
        counters = get_realtime_counters()
        for provider, counter in counters.items():
            budget = BUDGETS.get(provider, {})
            daily_limit = budget.get("daily")
            api_utilization[provider] = {
                **counter,
                "plan": budget.get("plan"),
                "daily_budget": daily_limit,
                "daily_utilization_pct": (
                    round(counter.get("calls_today", 0) / daily_limit * 100, 1)
                    if daily_limit else None
                ),
            }
    except Exception:
        pass

    # =========================================================================
    # 5. Provenance coverage
    # =========================================================================
    provenance = {}
    try:
        from app.data_layer.provenance_scaling import get_coverage_report
        provenance = get_coverage_report()
    except Exception:
        provenance = {"sources": {"total": 0, "proven": 0}}

    # =========================================================================
    # 6. Temporal depth
    # =========================================================================
    temporal = {}
    for table_name, config in TRACKED_TABLES.items():
        tc = config["time_col"]
        row = _safe_fetch(
            f"SELECT MIN({tc}) as earliest, MAX({tc}) as latest FROM {table_name}"
        )
        if row and row.get("earliest") and row.get("latest"):
            earliest = row["earliest"]
            latest = row["latest"]
            if hasattr(earliest, "days"):
                # It's a date, not datetime
                span_days = (latest - earliest).days if hasattr(latest, "days") else 0
            elif hasattr(earliest, "timestamp"):
                span_days = (latest - earliest).days
            else:
                span_days = 0

            temporal[table_name] = {
                "earliest": str(earliest),
                "latest": str(latest),
                "span_days": span_days,
            }

    # =========================================================================
    # 7. Data quality
    # =========================================================================
    coherence_flags_24h = _safe_count(
        "SELECT COUNT(*) as cnt FROM coherence_violations WHERE created_at >= NOW() - INTERVAL '24 hours'"
    )
    unreviewed_flags = _safe_count(
        "SELECT COUNT(*) as cnt FROM coherence_violations WHERE reviewed = FALSE"
    )

    stale_types = []
    staleness_thresholds = {
        "liquidity_depth": 3, "exchange_snapshots": 3, "entity_snapshots_hourly": 3,
        "yield_snapshots": 26, "governance_proposals": 26, "bridge_flows": 26,
        "peg_snapshots_5m": 26, "mint_burn_events": 26, "contract_surveillance": 170,
        "dex_pool_ohlcv": 6, "market_chart_history": 26,
        "scores": 3, "psi_scores": 26,
    }
    for table_name, max_hours in staleness_thresholds.items():
        tc = TRACKED_TABLES.get(table_name, {}).get("time_col", "created_at")
        latest = _safe_fetch(f"SELECT MAX({tc}) as latest FROM {table_name}")
        if latest and latest.get("latest"):
            lt = latest["latest"]
            if hasattr(lt, 'tzinfo') and lt.tzinfo is None:
                lt = lt.replace(tzinfo=timezone.utc)
            if hasattr(lt, 'timestamp'):
                age_hours = (now - lt).total_seconds() / 3600
                if age_hours > max_hours:
                    stale_types.append({
                        "table": table_name,
                        "last_updated_hours_ago": round(age_hours, 1),
                        "threshold_hours": max_hours,
                    })

    data_quality = {
        "coherence_flags_24h": coherence_flags_24h,
        "unreviewed_flags": unreviewed_flags,
        "stale_data_types": stale_types,
        "stale_count": len(stale_types),
    }

    # =========================================================================
    # 8. Storage
    # =========================================================================
    total_size_mb = round(total_bytes / 1_000_000, 1)
    growth_mb_day = sum(t["est_monthly_mb"] for t in tables.values()) / 30

    # Try to get actual DB size
    db_size_mb = None
    try:
        row = _safe_fetch(
            "SELECT pg_database_size(current_database()) / 1000000 as size_mb"
        )
        if row:
            db_size_mb = round(float(row["size_mb"]), 1)
    except Exception:
        pass

    storage = {
        "estimated_total_mb": total_size_mb,
        "actual_db_size_mb": db_size_mb,
        "growth_mb_per_day": round(growth_mb_day, 2),
        "projected_monthly_mb": round(growth_mb_day * 30, 1),
        "total_rows": total_rows,
        "rows_added_24h": total_rows_24h,
        "tables_tracked": len(tables),
    }

    return {
        "generated_at": now.isoformat(),
        "summary": {
            "total_rows": total_rows,
            "rows_added_24h": total_rows_24h,
            "tables_tracked": len(tables),
            "total_scored_entities": entity_coverage["total_scored_entities"],
            "wallet_graph_size": wallet_total,
            "db_size_mb": db_size_mb,
        },
        "tables": tables,
        "by_category": by_category,
        "wallet_graph": wallet_graph,
        "entity_coverage": entity_coverage,
        "api_utilization": api_utilization,
        "provenance": provenance,
        "temporal_depth": temporal,
        "data_quality": data_quality,
        "storage": storage,
    }
