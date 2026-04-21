"""
Database Schema Validator
==========================
One-shot startup check: does the live DB schema match what the code expects?
Catches migration drift before per-cycle errors.

Only validates tables known to have drift history. Not exhaustive.
"""

import logging
from app.database import fetch_all

logger = logging.getLogger(__name__)

# Tables that have caused schema drift bugs, with columns the code writes to.
# Each entry: table_name → list of columns the INSERT statements reference.
EXPECTED_COLUMNS = {
    "governance_proposals": [
        "id", "protocol_slug", "proposal_id", "title",
        "proposal_source", "body", "body_hash", "author_address", "author_ens",
        "state", "vote_start", "vote_end", "scores_total", "scores_for",
        "scores_against", "scores_abstain", "quorum", "choices", "votes",
        "ipfs_hash", "discussion_url", "captured_at", "body_changed",
        "first_capture_body_hash", "content_hash", "attested_at",
    ],
    "oracle_registry": [
        "id", "oracle_address", "oracle_name", "oracle_provider", "chain",
        "asset_symbol", "quote_symbol", "decimals", "read_function",
        "is_active", "entity_type", "entity_slug", "added_at",
    ],
    "oracle_price_readings": [
        "id", "oracle_address", "oracle_name", "oracle_provider", "chain",
        "asset_symbol", "quote_symbol", "oracle_price", "oracle_price_raw",
        "oracle_decimals", "cex_price", "deviation_pct", "deviation_abs",
        "latency_seconds", "round_id", "answer_timestamp", "recorded_at",
        "is_stress_event", "content_hash", "attested_at",
        "pre_stress_event_id",
    ],
    "oracle_stress_events": [
        "id", "oracle_address", "oracle_name", "asset_symbol", "chain",
        "event_type", "event_start", "event_end", "duration_seconds",
        "max_deviation_pct", "max_latency_seconds", "reading_count",
        "concurrent_sii_score", "concurrent_psi_scores", "affected_protocols",
        "content_hash", "attested_at",
        "pre_stress_window_hours", "pre_stress_readings_tagged",
    ],
    "psi_scores": [
        "id", "protocol_slug", "protocol_name", "overall_score", "grade",
        "category_scores", "component_scores", "raw_values", "formula_version",
        "computed_at", "scored_date",
        "backfilled", "backfill_source",
    ],
    "scores": [
        "stablecoin_id", "overall_score", "grade", "peg_score", "liquidity_score",
        "mint_burn_score", "distribution_score", "structural_score",
        "reserves_score", "contract_score", "oracle_score", "governance_score",
        "network_score", "component_count", "formula_version",
        "data_freshness_pct", "current_price", "market_cap", "volume_24h",
        "daily_change", "weekly_change", "computed_at", "updated_at",
    ],
    "generic_index_scores": [
        "id", "index_id", "entity_slug", "entity_name", "overall_score",
        "category_scores", "component_scores", "raw_values", "formula_version",
        "inputs_hash", "confidence", "confidence_tag", "scored_date", "computed_at",
        "backfilled", "backfill_source",
    ],
    "governance_voters": [
        "id", "protocol", "source", "proposal_id", "voter_address",
        "voting_power", "choice", "created_at", "collected_at",
    ],
    "mint_burn_events": [
        "id", "stablecoin_id", "chain", "event_type", "amount", "tx_hash",
        "block_number", "from_address", "to_address", "timestamp", "collected_at",
    ],
    "liquidity_depth": [
        "id", "asset_id", "venue", "chain", "depth_usd_2pct", "depth_usd_5pct",
        "bid_depth", "ask_depth", "spread_bps", "raw_data", "snapshot_at",
    ],
    "contract_surveillance": [
        "id", "entity_id", "chain", "contract_address", "has_admin_keys",
        "is_upgradeable", "has_pause_function", "has_blacklist",
        "timelock_hours", "multisig_threshold", "source_code_hash",
        "analysis", "scanned_at",
    ],
    "protocol_parameter_changes": [
        "id", "protocol_slug", "parameter_name", "parameter_key",
        "asset_address", "asset_symbol", "contract_address", "chain",
        "previous_value", "new_value", "value_unit", "change_magnitude",
        "change_direction", "changed_at", "detected_at",
        "change_context", "content_hash",
    ],
    "yield_snapshots": [
        "id", "pool_id", "protocol", "chain", "asset", "apy",
        "apy_base", "apy_reward", "tvl_usd", "stable_pool", "snapshot_at",
    ],
    "peg_snapshots_5m": [
        "id", "stablecoin_id", "price", "timestamp", "deviation_bps",
    ],
    "entity_snapshots_hourly": [
        "id", "entity_id", "entity_type", "market_cap", "total_volume",
        "price_usd", "price_change_24h", "circulating_supply", "total_supply",
        "snapshot_at",
    ],
    "exchange_snapshots": [
        "id", "exchange_id", "name", "trust_score", "trust_score_rank",
        "trade_volume_24h_btc", "year_established", "country",
        "trading_pairs", "snapshot_at",
    ],
    "incident_events": [
        "id", "entity_id", "entity_type", "incident_type", "severity",
        "title", "description", "started_at", "detection_method",
        "raw_data", "created_at",
    ],
    "wallet_graph.wallets": [
        "address", "first_seen_at", "last_indexed_at", "total_stablecoin_value",
        "size_tier", "source", "is_contract", "label", "created_at",
    ],
}


def validate_schemas() -> dict:
    """
    Check that every expected column exists in the live DB.
    Returns {table: [missing_columns]} for any table with drift.
    """
    drift = {}
    tables_checked = 0
    columns_checked = 0

    for table_name, expected_cols in EXPECTED_COLUMNS.items():
        # Strip schema prefix for information_schema lookup
        bare_name = table_name.split(".")[-1]
        try:
            rows = fetch_all(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s ORDER BY ordinal_position",
                (bare_name,),
            )
            if not rows:
                drift[table_name] = {"status": "TABLE_MISSING", "missing": expected_cols}
                logger.error(f"[schema_validator] {table_name}: TABLE DOES NOT EXIST")
                continue

            actual = {r["column_name"] for r in rows}
            tables_checked += 1
            missing = []
            for col in expected_cols:
                columns_checked += 1
                if col not in actual:
                    missing.append(col)

            if missing:
                drift[table_name] = {"status": "DRIFT", "missing": missing}
                logger.error(f"[schema_validator] {table_name}: MISSING {len(missing)} columns: {missing}")
            else:
                logger.info(f"[schema_validator] {table_name}: OK ({len(expected_cols)} columns)")

        except Exception as e:
            drift[table_name] = {"status": "ERROR", "error": str(e)}
            logger.error(f"[schema_validator] {table_name}: query failed: {e}")

    if drift:
        total_missing = sum(
            len(d.get("missing", [])) for d in drift.values()
        )
        logger.error(
            f"[schema_validator] DRIFT DETECTED: {len(drift)} tables, "
            f"{total_missing} missing columns across {tables_checked} tables checked"
        )
    else:
        logger.error(
            f"[schema_validator] all schemas aligned — "
            f"{tables_checked} tables, {columns_checked} columns verified"
        )

    return drift
