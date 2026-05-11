"""
Schema self-heal — boot-time verification that every expected table exists.

Background
----------
On 2026-05-10 the Replit→owned-Neon pg_dump preserved the `migrations`
tracking table while silently skipping the DDL for 6 tables created
outside Replit's UI (migrations 055, 066, 071, 103). The application
booted cleanly because no boot-time check noticed; 838+ cycle errors
per 24h surfaced the drift over the next day.

This module runs at worker boot, after `init_pool()` and after the
inline `run_migrations()` step in main.py. It compares the actual set
of public+wallet_graph tables against `EXPECTED_TABLES` (a snapshot of
production from 2026-05-11) and fails loud if any expected table is
missing.

Design choice: detection, not auto-recreation
---------------------------------------------
The orchestrator's original sketch was "CREATE TABLE IF NOT EXISTS for
every table." That requires maintaining 161 DDL statements as a second
source of truth alongside `migrations/`. Two sources drift; recreating
a missing table with stale columns is worse than failing loud. The
contract we actually need is "tables that `migrations` claims exist
must exist." Detection plus fail-loud meets that contract and lets
migrations/ remain canonical.

If a missing table is detected the function raises `SchemaDriftError`.
The caller (main.py) is expected to translate that into a non-zero
exit so Railway marks the deploy CRASHED and rolls back, exactly
mirroring the `init_pool() failed → sys.exit(1)` shape codified for
the Neon-pooler incident.

Per v9.10, this check runs against the unpooled endpoint so it
remains valid even if pgbouncer is misconfigured at the pooler.
"""
from __future__ import annotations

import logging
import os
from typing import Iterable

import psycopg2

logger = logging.getLogger(__name__)


class SchemaDriftError(RuntimeError):
    """Raised when one or more expected tables are missing from the database."""


# Snapshot of expected tables in (schema, name) form.
# Generated from prod 2026-05-11T16:15Z via:
#   SELECT table_schema, table_name FROM information_schema.tables
#   WHERE table_schema IN ('public','wallet_graph') AND table_type='BASE TABLE'
#
# When a new table lands in a migration, ADD it here in the same PR.
# The lint added under Phase 1 Item B does not enforce this — it is a
# contributor convention.
EXPECTED_TABLES: frozenset[tuple[str, str]] = frozenset({
    ("public", "abm_campaigns"),
    ("public", "abm_drip_touches"),
    ("public", "abm_touch_log"),
    ("public", "alert_rate_limit"),
    ("public", "api_keys"),
    ("public", "api_provider_limits"),
    ("public", "api_request_log"),
    ("public", "api_usage_hourly"),
    ("public", "api_usage_tracker"),
    ("public", "assessment_events"),
    ("public", "assessment_input_vectors"),
    ("public", "backfill_runs"),
    ("public", "backfill_status"),
    ("public", "bridge_flows"),
    ("public", "cda_issuer_registry"),
    ("public", "cda_monitors"),
    ("public", "cda_source_urls"),
    ("public", "cda_validation_results"),
    ("public", "cda_vendor_extractions"),
    ("public", "coherence_reports"),
    ("public", "coherence_violations"),
    ("public", "collector_cycle_stats"),
    ("public", "component_batch_hashes"),
    ("public", "component_readings"),
    ("public", "contagion_events"),
    ("public", "contract_dependencies"),
    ("public", "contract_surveillance"),
    ("public", "correlation_matrices"),
    ("public", "cqi_attestations"),
    ("public", "cycle_errors"),
    ("public", "daily_pulses"),
    ("public", "data_catalog"),
    ("public", "data_provenance"),
    ("public", "data_source_comparisons"),
    ("public", "dependency_graph_snapshots"),
    ("public", "deviation_events"),
    ("public", "dex_pool_ohlcv"),
    ("public", "discovery_signals"),
    ("public", "dispute_transitions"),
    ("public", "disputes"),
    ("public", "divergence_signals"),
    ("public", "enforcement_records"),
    ("public", "engine_analyses"),
    ("public", "engine_artifacts"),
    ("public", "engine_events"),
    ("public", "engine_interpretation_cache"),
    ("public", "engine_prompts"),
    ("public", "engine_watchlist"),
    ("public", "entity_snapshots_hourly"),
    ("public", "exchange_health_checks"),
    ("public", "exchange_snapshots"),
    ("public", "generic_index_scores"),
    ("public", "gov_analysis_tags"),
    ("public", "gov_crawl_logs"),
    ("public", "gov_documents"),
    ("public", "gov_metric_mentions"),
    ("public", "gov_stablecoin_mentions"),
    ("public", "governance_events"),
    ("public", "governance_forum_posts"),
    ("public", "governance_proposals"),
    ("public", "governance_voters"),
    ("public", "historical_prices"),
    ("public", "historical_protocol_data"),
    ("public", "historical_rpi_data"),
    ("public", "incident_events"),
    ("public", "incident_snapshots"),
    ("public", "incident_subscribers"),
    ("public", "integrity_checks"),
    ("public", "keeper_publish_log"),
    ("public", "legacy_data_provenance"),
    ("public", "legacy_deviation_events"),
    ("public", "legacy_historical_prices"),
    ("public", "legacy_score_events"),
    ("public", "legacy_score_history"),
    ("public", "lens_configs"),
    ("public", "liquidity_depth"),
    ("public", "market_chart_history"),
    ("public", "mcp_tool_calls"),
    ("public", "mempool_observations"),
    ("public", "methodology_hashes"),
    ("public", "metrics_daily_rollup"),
    ("public", "migrations"),
    ("public", "mint_burn_events"),
    ("public", "morpho_markets"),
    ("public", "ops_alert_config"),
    ("public", "ops_alert_log"),
    ("public", "ops_coingecko_news"),
    ("public", "ops_content_items"),
    ("public", "ops_governance_proposals"),
    ("public", "ops_health_checks"),
    ("public", "ops_investor_content"),
    ("public", "ops_investor_interactions"),
    ("public", "ops_investors"),
    ("public", "ops_target_contacts"),
    ("public", "ops_target_content"),
    ("public", "ops_target_engagement_log"),
    ("public", "ops_target_exposure_reports"),
    ("public", "ops_targets"),
    ("public", "oracle_external_interactions"),
    ("public", "oracle_price_readings"),
    ("public", "oracle_reads_log"),
    ("public", "oracle_registry"),
    ("public", "oracle_stress_events"),
    ("public", "oracle_update_cadence"),
    ("public", "parameter_changes"),
    ("public", "parent_company_financials"),
    ("public", "parent_company_registry"),
    ("public", "payment_log"),
    ("public", "peg_snapshots_5m"),
    ("public", "playground_submissions"),
    ("public", "protocol_backlog"),
    ("public", "protocol_collateral_exposure"),
    ("public", "protocol_market_snapshots"),
    ("public", "protocol_parameter_changes"),
    ("public", "protocol_parameter_snapshots"),
    ("public", "protocol_parameters"),
    ("public", "protocol_pool_wallets"),
    ("public", "protocol_trace_observations"),
    ("public", "protocol_treasury_holdings"),
    ("public", "provenance_health_alerts"),
    ("public", "provenance_proofs"),
    ("public", "provenance_sources"),
    ("public", "psi_governance_snapshots"),
    ("public", "psi_scores"),
    ("public", "regulatory_registry_checks"),
    ("public", "report_attestations"),
    ("public", "risk_incidents"),
    ("public", "rpc_capabilities"),
    ("public", "rpc_provider_latency"),
    ("public", "rpc_provider_usage"),
    ("public", "sanctions_screen_targets"),
    ("public", "sanctions_screening_results"),
    ("public", "rpi_components"),
    ("public", "rpi_doc_scores"),
    ("public", "rpi_protocol_config"),
    ("public", "rpi_score_history"),
    ("public", "rpi_scores"),
    ("public", "sbt_tokens"),
    ("public", "score_events"),
    ("public", "score_history"),
    ("public", "scores"),
    ("public", "stablecoins"),
    ("public", "state_attestations"),
    ("public", "state_growth_snapshots"),
    ("public", "static_evidence"),
    ("public", "temporal_reconstructions"),
    ("public", "token_approval_snapshots"),
    ("public", "track_record_entries"),
    ("public", "track_record_followups"),
    ("public", "tti_disclosure_extractions"),
    ("public", "validator_performance_snapshots"),
    ("public", "validator_slashing_events"),
    ("public", "volatility_surfaces"),
    ("public", "wallet_behavior_tags"),
    ("public", "wallet_chain_presence"),
    ("public", "wallet_holder_discovery"),
    ("public", "yield_snapshots"),
    ("wallet_graph", "actor_classification_history"),
    ("wallet_graph", "actor_classifications"),
    ("wallet_graph", "edge_build_status"),
    ("wallet_graph", "treasury_events"),
    ("wallet_graph", "treasury_registry"),
    ("wallet_graph", "unscored_assets"),
    ("wallet_graph", "wallet_edges"),
    ("wallet_graph", "wallet_edges_archive"),
    ("wallet_graph", "wallet_holdings"),
    ("wallet_graph", "wallet_profiles"),
    ("wallet_graph", "wallet_risk_scores"),
    ("wallet_graph", "wallets"),
})


def _direct_db_url() -> str:
    """Derive the unpooled Neon endpoint URL from DATABASE_URL.

    Same shape as `app.worker._direct_db_url()` — kept local here so this
    module has no import-cycle dependency on worker.py.
    """
    url = os.environ.get("DATABASE_URL", "")
    if not url or "-pooler." not in url:
        return ""
    return url.replace("-pooler.", ".", 1)


def _query_existing_tables(conn) -> set[tuple[str, str]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ('public', 'wallet_graph')
          AND table_type = 'BASE TABLE'
        """
    )
    rows = cur.fetchall()
    cur.close()
    return {(s, t) for (s, t) in rows}


def verify_schema(
    expected: Iterable[tuple[str, str]] = EXPECTED_TABLES,
    use_unpooled: bool = True,
) -> dict:
    """Verify that every expected table exists. Raises SchemaDriftError if not.

    Returns a small summary dict on success: {verified, extra, endpoint}.
    `extra` lists tables that exist in prod but not in EXPECTED_TABLES —
    informational only (a new migration landing before EXPECTED_TABLES
    is updated). `extra` does NOT raise.
    """
    expected_set = frozenset(expected)

    url = _direct_db_url() if use_unpooled else ""
    endpoint = "unpooled"
    if not url:
        url = os.environ.get("DATABASE_URL", "")
        endpoint = "pooled (unpooled URL not derivable)"
    if not url:
        raise SchemaDriftError("DATABASE_URL not set — cannot verify schema")

    conn = psycopg2.connect(url, connect_timeout=10)
    try:
        actual = _query_existing_tables(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    missing = sorted(expected_set - actual)
    extra = sorted(actual - expected_set)

    if missing:
        # Fail loud: list every missing table so the operator can re-apply
        # the right migration. Limit to 30 in the message to avoid line-wrap
        # nightmares on Railway logs; full list is in the raise payload.
        head = ", ".join(f"{s}.{t}" for s, t in missing[:30])
        suffix = "" if len(missing) <= 30 else f" (+{len(missing) - 30} more)"
        logger.critical(
            "schema self-heal FAILED: %d expected table(s) missing from %s: %s%s",
            len(missing),
            endpoint,
            head,
            suffix,
        )
        raise SchemaDriftError(
            f"{len(missing)} expected table(s) missing: {missing}"
        )

    logger.info(
        "schema self-heal completed: %d tables verified (%s, %d extras: %s)",
        len(expected_set),
        endpoint,
        len(extra),
        ", ".join(f"{s}.{t}" for s, t in extra[:10]) or "none",
    )
    return {
        "verified": len(expected_set),
        "extra": [f"{s}.{t}" for s, t in extra],
        "endpoint": endpoint,
    }
