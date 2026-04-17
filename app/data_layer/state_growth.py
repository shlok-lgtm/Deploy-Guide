"""
State Growth Dashboard
=======================
Comprehensive dashboard of accumulated state across ALL tables —
both core platform tables and the universal data layer.

GET /api/ops/state-growth-live (live queries across all tables)
GET /api/ops/state-growth (existing — reads from daily_pulses history)
"""

import json
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
    "wallet_graph.wallet_holdings":     {"time_col": "indexed_at",     "avg_row_bytes": 120, "category": "wallet"},
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

    # === Oracle / security / parameters ===
    "oracle_price_readings":    {"time_col": "recorded_at",    "avg_row_bytes": 200, "category": "oracle"},
    "oracle_stress_events":     {"time_col": "event_start",    "avg_row_bytes": 500, "category": "oracle"},
    "holder_clusters":          {"time_col": "snapshot_date",  "avg_row_bytes": 400, "category": "data_layer"},
    "concentration_snapshots":  {"time_col": "snapshot_date",  "avg_row_bytes": 200, "category": "data_layer"},
    "protocol_parameter_changes":  {"time_col": "detected_at", "avg_row_bytes": 400, "category": "parameters"},
    "protocol_parameter_snapshots": {"time_col": "snapshot_date", "avg_row_bytes": 1000, "category": "parameters"},
    "contract_upgrade_history": {"time_col": "upgrade_detected_at", "avg_row_bytes": 300, "category": "security"},
    "ops.keeper_cycles":        {"time_col": "started_at",     "avg_row_bytes": 200, "category": "keeper"},
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


def _bulk_row_counts() -> dict[str, int]:
    """Fetch approximate row counts for all tables via pg_stat — instant, no scan."""
    counts = {}
    try:
        rows = fetch_all(
            "SELECT schemaname || '.' || relname AS full_name, relname, n_live_tup "
            "FROM pg_stat_user_tables"
        )
        for r in (rows or []):
            # Store under both "schema.table" and plain "table" for matching
            counts[r["full_name"]] = int(r["n_live_tup"])
            counts[r["relname"]] = int(r["n_live_tup"])
    except Exception:
        pass
    return counts


def get_state_growth() -> dict:
    """Comprehensive state growth dashboard — live queries across all tables."""
    now = datetime.now(timezone.utc)

    # =========================================================================
    # 1. Per-table row counts and growth (grouped by category)
    # =========================================================================
    # Use pg_stat_user_tables for row counts (approximate but instant).
    # Only use COUNT(*) for 24h/7d growth where we need time-filtered counts.
    pg_counts = _bulk_row_counts()

    tables = {}
    by_category = {}
    total_rows = 0
    total_bytes = 0
    total_rows_24h = 0

    for table_name, config in TRACKED_TABLES.items():
        tc = config["time_col"]
        arb = config["avg_row_bytes"]
        cat = config.get("category", "other")

        # Approximate row count from pg_stat (instant)
        row_count = pg_counts.get(table_name, 0)
        # For schema-qualified names like "wallet_graph.wallets", try both forms
        if row_count == 0 and "." in table_name:
            plain = table_name.split(".")[-1]
            row_count = pg_counts.get(plain, 0)

        # Time-filtered counts — only for tables with rows (skip empty tables)
        rows_24h = 0
        rows_7d = 0
        if row_count > 0:
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
        "SELECT COUNT(DISTINCT from_address) as cnt FROM wallet_graph.wallet_edges"
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
            "SELECT COUNT(DISTINCT entity_slug) as cnt FROM generic_index_scores WHERE index_id = %s",
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

        # Also compute live proof coverage from provenance_proofs table
        # (the registry-based report may show 0% if proof linking hasn't run)
        total_registered = _safe_count("SELECT COUNT(*) as cnt FROM provenance_sources WHERE enabled = true")
        proved_24h = _safe_count(
            "SELECT COUNT(DISTINCT source_domain) as cnt FROM provenance_proofs "
            "WHERE proved_at > NOW() - INTERVAL '24 hours'"
        )
        total_proofs = _safe_count("SELECT COUNT(*) as cnt FROM provenance_proofs")
        proofs_24h = _safe_count(
            "SELECT COUNT(*) as cnt FROM provenance_proofs "
            "WHERE proved_at > NOW() - INTERVAL '24 hours'"
        )
        provenance["live"] = {
            "registered_sources": total_registered,
            "sources_proved_24h": proved_24h,
            "coverage_pct": round(proved_24h / max(total_registered, 1) * 100, 1),
            "total_proofs": total_proofs,
            "proofs_24h": proofs_24h,
        }
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

    # =========================================================================
    # 9. Collector health — per-collector performance from collector_cycle_stats
    # =========================================================================
    collector_health = []
    try:
        rows = fetch_all("""
            SELECT DISTINCT ON (collector_name)
                collector_name, created_at, coins_ok, coins_timeout, coins_error,
                avg_latency_ms, total_components
            FROM collector_cycle_stats
            ORDER BY collector_name, created_at DESC
        """) or []
        for r in rows:
            total_runs = (r.get("coins_ok", 0) or 0) + (r.get("coins_timeout", 0) or 0) + (r.get("coins_error", 0) or 0)
            collector_health.append({
                "name": r["collector_name"],
                "last_run": r["created_at"].isoformat() if r.get("created_at") else None,
                "ok": r.get("coins_ok", 0),
                "timeout": r.get("coins_timeout", 0),
                "error": r.get("coins_error", 0),
                "success_rate": round(r.get("coins_ok", 0) / max(total_runs, 1) * 100, 1),
                "avg_latency_ms": r.get("avg_latency_ms", 0),
                "total_components": r.get("total_components", 0),
            })
    except Exception:
        pass

    # =========================================================================
    # 10. Active alerts — things needing human attention
    # =========================================================================
    active_alerts = {}
    active_alerts["oracle_stress_open"] = _safe_count(
        "SELECT COUNT(*) as cnt FROM oracle_stress_events WHERE event_end IS NULL"
    )
    active_alerts["contract_upgrades_7d"] = _safe_count(
        "SELECT COUNT(*) as cnt FROM contract_upgrade_history WHERE upgrade_detected_at >= NOW() - INTERVAL '7 days'"
    )
    active_alerts["parameter_changes_7d"] = _safe_count(
        "SELECT COUNT(*) as cnt FROM protocol_parameter_changes WHERE detected_at >= NOW() - INTERVAL '7 days'"
    )
    active_alerts["discovery_signals_24h"] = _safe_count(
        "SELECT COUNT(*) as cnt FROM discovery_signals WHERE detected_at >= NOW() - INTERVAL '24 hours'"
    )

    # =========================================================================
    # 11. Keeper status — on-chain publication state
    # =========================================================================
    keeper_status = {}
    try:
        last_cycle = _safe_fetch("""
            SELECT started_at, completed_at, duration_ms,
                   sii_updates_base, sii_updates_arb, psi_updates,
                   state_root_published, trigger_reason
            FROM ops.keeper_cycles
            ORDER BY started_at DESC LIMIT 1
        """)
        if last_cycle:
            keeper_status = {
                "last_started": last_cycle["started_at"].isoformat() if last_cycle.get("started_at") else None,
                "last_completed": last_cycle["completed_at"].isoformat() if last_cycle.get("completed_at") else None,
                "duration_ms": last_cycle.get("duration_ms"),
                "sii_updates_base": last_cycle.get("sii_updates_base", 0),
                "sii_updates_arb": last_cycle.get("sii_updates_arb", 0),
                "psi_updates": last_cycle.get("psi_updates", 0),
                "state_root_published": last_cycle.get("state_root_published", False),
            }
        keeper_status["total_cycles"] = _safe_count("SELECT COUNT(*) as cnt FROM ops.keeper_cycles")
        keeper_status["cycles_24h"] = _safe_count(
            "SELECT COUNT(*) as cnt FROM ops.keeper_cycles WHERE started_at >= NOW() - INTERVAL '24 hours'"
        )
    except Exception:
        pass

    # =========================================================================
    # 12. Scoring performance — cycle durations and throughput
    # =========================================================================
    scoring_performance = {}
    try:
        # SII cycle duration trend (last 7 days from collector_cycle_stats)
        sii_trend = fetch_all("""
            SELECT DATE(created_at) as day,
                   AVG(avg_latency_ms) as avg_ms,
                   SUM(total_components) as total_comps,
                   SUM(coins_ok) as total_ok
            FROM collector_cycle_stats
            WHERE created_at >= NOW() - INTERVAL '7 days'
            GROUP BY DATE(created_at)
            ORDER BY day DESC
        """) or []
        scoring_performance["daily_trend"] = [
            {
                "day": str(r["day"]),
                "avg_latency_ms": round(float(r["avg_ms"]), 0) if r.get("avg_ms") else 0,
                "total_components": r.get("total_comps", 0),
                "coins_scored": r.get("total_ok", 0),
            }
            for r in sii_trend
        ]

        scoring_performance["stablecoins_scored"] = sii_scored
        scoring_performance["psi_protocols_scored"] = psi_scored

        # Average components per stablecoin
        avg_comps = _safe_fetch(
            "SELECT AVG(cnt) as avg_cnt FROM ("
            "  SELECT stablecoin_id, COUNT(*) as cnt FROM component_readings"
            "  WHERE collected_at >= NOW() - INTERVAL '24 hours'"
            "  GROUP BY stablecoin_id"
            ") sub"
        )
        scoring_performance["avg_components_per_coin"] = (
            round(float(avg_comps["avg_cnt"]), 1) if avg_comps and avg_comps.get("avg_cnt") else 0
        )
    except Exception:
        pass

    # =========================================================================
    # 13. CDA freshness — per-issuer extraction staleness
    # =========================================================================
    cda_freshness = []
    try:
        issuers = fetch_all("""
            SELECT r.asset_symbol, r.issuer_name, r.is_active,
                   r.collection_method,
                   MAX(e.extracted_at) as last_extraction
            FROM cda_issuer_registry r
            LEFT JOIN cda_vendor_extractions e ON e.asset_symbol = r.asset_symbol
            GROUP BY r.asset_symbol, r.issuer_name, r.is_active, r.collection_method
            ORDER BY r.is_active DESC, last_extraction ASC NULLS FIRST
        """) or []
        for r in issuers:
            method = r.get("collection_method", "")
            no_attestation = method in ("no_attestation", "nav_oracle", "crypto_backed", "algorithmic")
            last = r.get("last_extraction")
            days_since = None
            if last:
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                days_since = round((now - last).total_seconds() / 86400, 1)
            cda_freshness.append({
                "asset": r["asset_symbol"],
                "issuer": r.get("issuer_name"),
                "active": r.get("is_active", False),
                "collection_method": method,
                "last_extraction": last.isoformat() if last else None,
                "days_since": days_since,
                "stale": False if no_attestation else (days_since is None or days_since > 30),
            })
    except Exception:
        pass

    # =========================================================================
    # 14. Component coverage — per-index: defined, automated, static, empty
    # =========================================================================
    component_coverage = {}
    try:
        # SII components per stablecoin
        sii_comp = fetch_all("""
            SELECT stablecoin_id, COUNT(*) as total,
                   COUNT(*) FILTER (WHERE value IS NOT NULL) as populated,
                   COUNT(*) FILTER (WHERE value IS NULL) as empty
            FROM component_readings
            WHERE collected_at >= NOW() - INTERVAL '24 hours'
            GROUP BY stablecoin_id
        """) or []
        component_coverage["sii"] = {
            "coins_with_readings": len(sii_comp),
            "avg_components": round(sum(r["total"] for r in sii_comp) / max(len(sii_comp), 1), 1),
            "avg_populated": round(sum(r["populated"] for r in sii_comp) / max(len(sii_comp), 1), 1),
            "avg_empty": round(sum(r["empty"] for r in sii_comp) / max(len(sii_comp), 1), 1),
        }

        # PSI components
        psi_comp_count = _safe_count(
            "SELECT COUNT(DISTINCT component_name) as cnt FROM psi_components WHERE collected_at >= NOW() - INTERVAL '48 hours'"
        )
        psi_protocols_with = _safe_count(
            "SELECT COUNT(DISTINCT protocol_slug) as cnt FROM psi_components WHERE collected_at >= NOW() - INTERVAL '48 hours'"
        )
        component_coverage["psi"] = {
            "unique_components": psi_comp_count,
            "protocols_with_readings": psi_protocols_with,
        }

        # RPI components
        rpi_comp_count = _safe_count(
            "SELECT COUNT(DISTINCT component_name) as cnt FROM rpi_components WHERE collected_at >= NOW() - INTERVAL '48 hours'"
        )
        component_coverage["rpi"] = {
            "unique_components": rpi_comp_count,
        }
    except Exception:
        pass

    # =========================================================================
    # 15. CQI contagion coverage
    # =========================================================================
    cqi_contagion = {}
    try:
        pairs_with_pools = _safe_count(
            "SELECT COUNT(DISTINCT protocol_slug) as cnt FROM protocol_pool_wallets"
        )
        pairs_total = _safe_count("SELECT COUNT(DISTINCT protocol_slug) as cnt FROM psi_scores")
        cqi_contagion = {
            "protocols_with_pool_data": pairs_with_pools,
            "protocols_total": pairs_total,
            "coverage_pct": round(pairs_with_pools / max(pairs_total, 1) * 100, 1),
            "pool_wallets_discovered": _safe_count("SELECT COUNT(*) as cnt FROM protocol_pool_wallets"),
        }
    except Exception:
        pass

    # =========================================================================
    # 16. x402 revenue
    # =========================================================================
    x402_revenue = {}
    try:
        x402_revenue["total_payments"] = _safe_count("SELECT COUNT(*) as cnt FROM payment_log")
        x402_revenue["payments_24h"] = _safe_count(
            "SELECT COUNT(*) as cnt FROM payment_log WHERE timestamp >= NOW() - INTERVAL '24 hours'"
        )
        x402_revenue["payments_7d"] = _safe_count(
            "SELECT COUNT(*) as cnt FROM payment_log WHERE timestamp >= NOW() - INTERVAL '7 days'"
        )
        x402_revenue["unique_payers"] = _safe_count(
            "SELECT COUNT(DISTINCT payer_address) as cnt FROM payment_log WHERE payer_address IS NOT NULL"
        )

        rev_total = _safe_fetch("SELECT COALESCE(SUM(price_usd), 0) as total FROM payment_log")
        x402_revenue["total_revenue_usd"] = float(rev_total["total"]) if rev_total and rev_total.get("total") else 0

        rev_7d = _safe_fetch(
            "SELECT COALESCE(SUM(price_usd), 0) as total FROM payment_log WHERE timestamp >= NOW() - INTERVAL '7 days'"
        )
        x402_revenue["revenue_7d_usd"] = float(rev_7d["total"]) if rev_7d and rev_7d.get("total") else 0

        # Top endpoints by call count
        top_endpoints = fetch_all("""
            SELECT endpoint, COUNT(*) as calls, SUM(price_usd) as revenue
            FROM payment_log
            GROUP BY endpoint
            ORDER BY calls DESC
            LIMIT 10
        """) or []
        x402_revenue["top_endpoints"] = [
            {"endpoint": r["endpoint"], "calls": r["calls"],
             "revenue_usd": round(float(r["revenue"]), 6) if r.get("revenue") else 0}
            for r in top_endpoints
        ]
    except Exception:
        pass

    # =========================================================================
    # 17. Security scanning — contract surveillance status
    # =========================================================================
    security_scanning = {}
    try:
        # Contracts monitored by entity type
        monitored = fetch_all("""
            SELECT entity_id, chain, contract_address, has_admin_keys,
                   is_upgradeable, has_pause_function, timelock_hours,
                   multisig_threshold, source_code_hash, scanned_at
            FROM contract_surveillance
            WHERE scanned_at = (
                SELECT MAX(scanned_at) FROM contract_surveillance cs2
                WHERE cs2.entity_id = contract_surveillance.entity_id
                  AND cs2.contract_address = contract_surveillance.contract_address
            )
        """) or []

        security_scanning["contracts_monitored"] = len(monitored)
        security_scanning["unique_entities"] = len(set(r["entity_id"] for r in monitored))

        # Admin key risk summary
        with_admin = [r for r in monitored if r.get("has_admin_keys")]
        short_timelock = [r for r in monitored if r.get("timelock_hours") is not None and float(r["timelock_hours"]) < 24]
        no_multisig = [r for r in monitored if r.get("multisig_threshold") is None or r.get("multisig_threshold") == ""]
        pause_no_timelock = [
            r for r in monitored
            if r.get("has_pause_function") and (r.get("timelock_hours") is None or float(r.get("timelock_hours", 0)) == 0)
        ]

        security_scanning["admin_key_risk"] = {
            "contracts_with_admin_keys": len(with_admin),
            "timelock_under_24h": len(short_timelock),
            "no_multisig": len(no_multisig),
            "pausable_without_timelock": len(pause_no_timelock),
        }

        # Scan coverage vs scored entities
        scored_entities = set()
        try:
            sii_entities = fetch_all("SELECT id FROM stablecoins WHERE scoring_enabled = TRUE") or []
            scored_entities.update(str(r["id"]) for r in sii_entities)
            psi_entities = fetch_all("SELECT DISTINCT protocol_slug FROM psi_scores") or []
            scored_entities.update(r["protocol_slug"] for r in psi_entities)
        except Exception:
            pass

        monitored_entities = set(r["entity_id"] for r in monitored)
        unmonitored = sorted(scored_entities - monitored_entities)

        security_scanning["scan_coverage"] = {
            "scored_entities": len(scored_entities),
            "monitored_entities": len(monitored_entities & scored_entities),
            "coverage_pct": round(len(monitored_entities & scored_entities) / max(len(scored_entities), 1) * 100, 1),
            "unmonitored_entities": unmonitored[:20],
        }

        # Upgrade alerts in last 7 and 30 days
        security_scanning["upgrade_alerts_7d"] = _safe_count(
            "SELECT COUNT(*) as cnt FROM contract_upgrade_history WHERE upgrade_detected_at >= NOW() - INTERVAL '7 days'"
        )
        security_scanning["upgrade_alerts_30d"] = _safe_count(
            "SELECT COUNT(*) as cnt FROM contract_upgrade_history WHERE upgrade_detected_at >= NOW() - INTERVAL '30 days'"
        )

        # Historical diff count — total source code changes since monitoring began
        security_scanning["total_diffs_detected"] = _safe_count(
            "SELECT COUNT(*) as cnt FROM contract_upgrade_history"
        )

        # Last scan per contract (most recent only)
        security_scanning["last_scan"] = _safe_fetch(
            "SELECT MAX(scanned_at) as latest FROM contract_surveillance"
        )
        if security_scanning["last_scan"] and security_scanning["last_scan"].get("latest"):
            lt = security_scanning["last_scan"]["latest"]
            security_scanning["last_scan"] = lt.isoformat() if hasattr(lt, 'isoformat') else str(lt)
        else:
            security_scanning["last_scan"] = None
    except Exception:
        pass

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
        "collector_health": collector_health,
        "active_alerts": active_alerts,
        "keeper_status": keeper_status,
        "scoring_performance": scoring_performance,
        "cda_freshness": cda_freshness,
        "component_coverage": component_coverage,
        "cqi_contagion": cqi_contagion,
        "x402_revenue": x402_revenue,
        "security_scanning": security_scanning,
    }
