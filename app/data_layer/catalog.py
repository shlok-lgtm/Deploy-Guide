"""
Data Catalog — keeps the data_catalog table in sync.
Auto-updates freshness, row counts, and history depth for all data types.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from app.database import fetch_one, execute, fetch_one_async, execute_async

logger = logging.getLogger(__name__)

# Registry of all data types in the universal data layer
DATA_TYPES = [
    {
        "data_type": "liquidity_depth",
        "description": "Per-asset, per-venue liquidity profile (DEX + CEX). Bid/ask depth, spread, volume, trade count.",
        "source_table": "liquidity_depth",
        "update_frequency": "hourly",
        "providers": ["coingecko", "geckoterminal"],
        "integrity_domain": "liquidity",
        "staleness_threshold_hours": 3,
        "used_by_indices": ["sii"],
    },
    {
        "data_type": "yield_snapshots",
        "description": "Pool-level yield, TVL, utilization for lending/vault protocols.",
        "source_table": "yield_snapshots",
        "update_frequency": "daily",
        "providers": ["defillama"],
        "integrity_domain": "yield",
        "staleness_threshold_hours": 26,
        "used_by_indices": ["psi", "vsri"],
    },
    {
        "data_type": "governance_proposals",
        "description": "Governance proposals with vote counts, quorum status, pass rates.",
        "source_table": "governance_proposals",
        "update_frequency": "daily",
        "providers": ["snapshot", "tally"],
        "integrity_domain": "governance",
        "staleness_threshold_hours": 26,
        "used_by_indices": ["dohi", "rpi"],
    },
    {
        "data_type": "governance_voters",
        "description": "Per-proposal voter data: address, voting power, choice.",
        "source_table": "governance_voters",
        "update_frequency": "daily",
        "providers": ["snapshot"],
        "integrity_domain": "governance",
        "staleness_threshold_hours": 26,
        "used_by_indices": ["dohi"],
    },
    {
        "data_type": "bridge_flows",
        "description": "Directional bridge volume per chain pair. Source → destination flow data.",
        "source_table": "bridge_flows",
        "update_frequency": "daily",
        "providers": ["defillama"],
        "integrity_domain": "bridge",
        "staleness_threshold_hours": 26,
        "used_by_indices": ["bri"],
    },
    {
        "data_type": "exchange_snapshots",
        "description": "Exchange trust score, volume, stablecoin pairs, year established.",
        "source_table": "exchange_snapshots",
        "update_frequency": "hourly",
        "providers": ["coingecko"],
        "integrity_domain": "exchange",
        "staleness_threshold_hours": 3,
        "used_by_indices": ["cxri"],
    },
    {
        "data_type": "correlation_matrices",
        "description": "Cross-entity rolling correlation matrices (30d/90d).",
        "source_table": "correlation_matrices",
        "update_frequency": "daily",
        "providers": [],
        "integrity_domain": "correlation",
        "staleness_threshold_hours": 26,
        "used_by_indices": [],
    },
    {
        "data_type": "volatility_surfaces",
        "description": "Realized volatility, max drawdown, recovery time per asset.",
        "source_table": "volatility_surfaces",
        "update_frequency": "daily",
        "providers": ["coingecko"],
        "integrity_domain": "volatility",
        "staleness_threshold_hours": 26,
        "used_by_indices": ["sii"],
    },
    {
        "data_type": "incident_events",
        "description": "Structured incident history: exploits, depegs, oracle failures.",
        "source_table": "incident_events",
        "update_frequency": "daily",
        "providers": [],
        "integrity_domain": "incidents",
        "staleness_threshold_hours": 48,
        "used_by_indices": [],
    },
    {
        "data_type": "peg_snapshots_5m",
        "description": "5-minute peg resolution: price and deviation from $1.00.",
        "source_table": "peg_snapshots_5m",
        "update_frequency": "daily",
        "providers": ["coingecko"],
        "integrity_domain": "peg",
        "staleness_threshold_hours": 26,
        "used_by_indices": ["sii"],
    },
    {
        "data_type": "mint_burn_events",
        "description": "Individual mint/burn events with tx hash, amount, timestamp.",
        "source_table": "mint_burn_events",
        "update_frequency": "daily",
        "providers": ["etherscan"],
        "integrity_domain": "mint_burn",
        "staleness_threshold_hours": 26,
        "used_by_indices": ["sii"],
    },
    {
        "data_type": "entity_snapshots_hourly",
        "description": "Hourly snapshots: market cap, volume, price, supply, dev/community data.",
        "source_table": "entity_snapshots_hourly",
        "update_frequency": "hourly",
        "providers": ["coingecko"],
        "integrity_domain": "entity_snapshot",
        "staleness_threshold_hours": 3,
        "used_by_indices": ["sii", "psi"],
    },
    {
        "data_type": "contract_surveillance",
        "description": "Smart contract analysis: admin keys, upgradeability, pause functions, timelocks.",
        "source_table": "contract_surveillance",
        "update_frequency": "weekly",
        "providers": ["etherscan"],
        "integrity_domain": "contract",
        "staleness_threshold_hours": 168,
        "used_by_indices": ["sii", "psi"],
    },
    {
        "data_type": "wallet_behavior_tags",
        "description": "Behavioral classification: accumulator, distributor, rotator, bridge user.",
        "source_table": "wallet_behavior_tags",
        "update_frequency": "daily",
        "providers": [],
        "integrity_domain": "wallet_behavior",
        "staleness_threshold_hours": 48,
        "used_by_indices": [],
    },
    {
        "data_type": "dex_pool_ohlcv",
        "description": "Pool-level candlestick data: open/high/low/close/volume per DEX pool.",
        "source_table": "dex_pool_ohlcv",
        "update_frequency": "3h",
        "providers": ["coingecko"],
        "integrity_domain": "liquidity",
        "staleness_threshold_hours": 6,
        "used_by_indices": ["sii"],
    },
    {
        "data_type": "market_chart_history",
        "description": "Historical price/mcap/volume at 5min/hourly/daily granularity for temporal reconstruction.",
        "source_table": "market_chart_history",
        "update_frequency": "daily",
        "providers": ["coingecko"],
        "integrity_domain": "temporal",
        "staleness_threshold_hours": 26,
        "used_by_indices": ["sii"],
    },
]


async def update_catalog():
    """Update the data_catalog table with current stats for all data types."""
    for dt in DATA_TYPES:
        table = dt["source_table"]

        try:
            # Get row count
            count_row = await fetch_one_async(f"SELECT COUNT(*) as cnt FROM {table}")
            row_count = count_row["cnt"] if count_row else 0

            # Get earliest and latest records
            # Use different column names based on table
            time_cols = {
                "liquidity_depth": "snapshot_at",
                "yield_snapshots": "snapshot_at",
                "governance_proposals": "captured_at",
                "governance_voters": "collected_at",
                "bridge_flows": "snapshot_at",
                "exchange_snapshots": "snapshot_at",
                "correlation_matrices": "computed_at",
                "volatility_surfaces": "computed_at",
                "incident_events": "created_at",
                "peg_snapshots_5m": "timestamp",
                "mint_burn_events": "collected_at",
                "entity_snapshots_hourly": "snapshot_at",
                "contract_surveillance": "scanned_at",
                "wallet_behavior_tags": "computed_at",
                "dex_pool_ohlcv": "timestamp",
                "market_chart_history": "timestamp",
            }
            time_col = time_cols.get(table)

            if time_col:
                range_row = await fetch_one_async(
                    f"SELECT MIN({time_col}) as earliest, MAX({time_col}) as latest FROM {table}"
                )
                earliest = range_row["earliest"] if range_row else None
                latest = range_row["latest"] if range_row else None
            else:
                logger.warning(
                    f"[catalog] no timestamp column mapped for table {table!r}; "
                    f"earliest/latest will be NULL"
                )
                earliest = None
                latest = None

            await execute_async(
                """INSERT INTO data_catalog
                   (data_type, description, source_table, update_frequency,
                    providers, provenance_status, earliest_record, latest_record,
                    row_count, used_by_indices, integrity_domain,
                    staleness_threshold_hours, updated_at)
                   VALUES (%s, %s, %s, %s, %s::jsonb, 'unproven', %s, %s, %s,
                           %s::jsonb, %s, %s, NOW())
                   ON CONFLICT (data_type) DO UPDATE SET
                       earliest_record = EXCLUDED.earliest_record,
                       latest_record = EXCLUDED.latest_record,
                       row_count = EXCLUDED.row_count,
                       updated_at = NOW()""",
                (
                    dt["data_type"], dt["description"], dt["source_table"],
                    dt["update_frequency"],
                    json.dumps(dt["providers"]),
                    earliest, latest, row_count,
                    json.dumps(dt["used_by_indices"]),
                    dt.get("integrity_domain"),
                    dt.get("staleness_threshold_hours"),
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # Table may not exist yet — skip
            logger.warning(f"Catalog update skipped for {dt['data_type']}: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="data_layer_update_catalog_update_failure",
                    error_message=str(e)[:500],
                    cycle_phase="catalog",
                )
            except Exception:
                pass

    logger.info(f"Data catalog updated: {len(DATA_TYPES)} data types")
