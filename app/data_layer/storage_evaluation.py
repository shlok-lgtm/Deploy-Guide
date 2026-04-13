"""
Time-Series Storage Evaluation
================================
Row count projections, partitioning recommendations, cost estimates.

This module provides on-demand evaluation — call it to get current
storage metrics and projections.

GET /api/ops/storage-evaluation returns all projections.
"""

import logging
from datetime import datetime, timezone

from app.database import fetch_one, fetch_all

logger = logging.getLogger(__name__)

# Projected monthly row counts at target utilization (60-70%)
PROJECTIONS = {
    "peg_snapshots_5m": {
        "description": "5-minute peg data for all stablecoins",
        "formula": "36 stablecoins × 288 points/day × 30 days",
        "monthly_rows": 36 * 288 * 30,  # ~311K
        "row_size_bytes": 80,
    },
    "liquidity_depth": {
        "description": "Per-venue liquidity (DEX+CEX) hourly",
        "formula": "36 coins × ~20 venues × 24 hours × 30 days",
        "monthly_rows": 36 * 20 * 24 * 30,  # ~518K
        "row_size_bytes": 300,
    },
    "yield_snapshots": {
        "description": "DeFiLlama pool yields daily",
        "formula": "~500 relevant pools × 30 days",
        "monthly_rows": 500 * 30,  # ~15K
        "row_size_bytes": 250,
    },
    "governance_proposals": {
        "description": "Snapshot + Tally proposals",
        "formula": "~15 spaces × ~5 proposals/month",
        "monthly_rows": 15 * 5,  # ~75
        "row_size_bytes": 2000,
    },
    "governance_voters": {
        "description": "Per-proposal voter records",
        "formula": "~75 proposals × ~200 top voters",
        "monthly_rows": 75 * 200,  # ~15K
        "row_size_bytes": 150,
    },
    "bridge_flows": {
        "description": "Directional bridge volumes daily",
        "formula": "~30 bridges × ~10 chain pairs × 30 days",
        "monthly_rows": 30 * 10 * 30,  # ~9K
        "row_size_bytes": 200,
    },
    "exchange_snapshots": {
        "description": "Exchange data hourly",
        "formula": "15 exchanges × 24 hours × 30 days",
        "monthly_rows": 15 * 24 * 30,  # ~10.8K
        "row_size_bytes": 500,
    },
    "entity_snapshots_hourly": {
        "description": "Full CoinGecko entity data hourly",
        "formula": "~50 entities × 24 hours × 30 days",
        "monthly_rows": 50 * 24 * 30,  # ~36K
        "row_size_bytes": 1000,
    },
    "mint_burn_events": {
        "description": "Per-event mint/burn from Etherscan",
        "formula": "~36 stablecoins × ~100 events/day × 30 days (variable)",
        "monthly_rows": 36 * 100 * 30,  # ~108K
        "row_size_bytes": 300,
    },
    "wallet_behavior_tags": {
        "description": "Behavioral classification tags",
        "formula": "~50K classified wallets × ~3 tags each × monthly refresh",
        "monthly_rows": 50_000 * 3,  # ~150K
        "row_size_bytes": 200,
    },
    "contract_surveillance": {
        "description": "Contract analysis weekly",
        "formula": "~50 contracts × 4 scans/month",
        "monthly_rows": 50 * 4,  # ~200
        "row_size_bytes": 2000,
    },
    "correlation_matrices": {
        "description": "Correlation matrices daily",
        "formula": "3 matrix types × 30 days",
        "monthly_rows": 3 * 30,  # ~90
        "row_size_bytes": 5000,
    },
    "volatility_surfaces": {
        "description": "Per-asset volatility daily",
        "formula": "~50 assets × 30 days",
        "monthly_rows": 50 * 30,  # ~1.5K
        "row_size_bytes": 200,
    },
    "api_usage_tracker": {
        "description": "API call tracking (raw)",
        "formula": "~5000 calls/day × 30 days",
        "monthly_rows": 5000 * 30,  # ~150K
        "row_size_bytes": 150,
    },
    "api_usage_hourly": {
        "description": "API usage rollups",
        "formula": "~8 providers × 24 hours × 30 days",
        "monthly_rows": 8 * 24 * 30,  # ~5.8K
        "row_size_bytes": 200,
    },
}


def get_storage_evaluation() -> dict:
    """
    Full storage evaluation with current stats and projections.
    Returns: current sizes, projected growth, partitioning recommendations.
    """
    tables = {}
    total_current_rows = 0
    total_projected_monthly = 0
    total_projected_monthly_bytes = 0

    for table_name, proj in PROJECTIONS.items():
        try:
            row = fetch_one(f"SELECT COUNT(*) as cnt FROM {table_name}")
            current_rows = row["cnt"] if row else 0
        except Exception:
            current_rows = 0

        monthly_rows = proj["monthly_rows"]
        monthly_bytes = monthly_rows * proj["row_size_bytes"]
        annual_rows = monthly_rows * 12
        annual_bytes = monthly_bytes * 12

        tables[table_name] = {
            "description": proj["description"],
            "formula": proj["formula"],
            "current_rows": current_rows,
            "projected_monthly_rows": monthly_rows,
            "projected_monthly_mb": round(monthly_bytes / 1_000_000, 1),
            "projected_annual_rows": annual_rows,
            "projected_annual_gb": round(annual_bytes / 1_000_000_000, 2),
            "needs_partitioning": monthly_rows > 100_000,
        }

        total_current_rows += current_rows
        total_projected_monthly += monthly_rows
        total_projected_monthly_bytes += monthly_bytes

    # Partitioning recommendations
    needs_partitioning = [
        name for name, t in tables.items() if t["needs_partitioning"]
    ]

    total_annual_gb = total_projected_monthly_bytes * 12 / 1_000_000_000

    # Cost estimate
    # Neon Postgres: Scale tier = $69/month for 50GB, $0.50/GB/month after
    # Pro tier: $19/month for 10GB
    if total_annual_gb < 10:
        recommended_tier = "Pro ($19/month, 10GB included)"
        estimated_cost = 19
    elif total_annual_gb < 50:
        recommended_tier = "Scale ($69/month, 50GB included)"
        estimated_cost = 69
    else:
        overage_gb = total_annual_gb - 50
        estimated_cost = 69 + overage_gb * 0.50
        recommended_tier = f"Scale ($69/month + ${overage_gb * 0.50:.0f}/month overage)"

    # API budget reference (for cross-check)
    api_budgets = {
        "coingecko": {"daily": 16_600, "monthly": 500_000, "plan": "Analyst"},
        "etherscan": {"daily": 200_000, "monthly": None, "plan": "Standard (10 req/s, 200K/day cap)"},
        "blockscout": {"daily": None, "monthly": None, "plan": "Free"},
        "defillama": {"daily": None, "monthly": None, "plan": "Free"},
    }

    return {
        "summary": {
            "total_current_rows": total_current_rows,
            "projected_monthly_rows": total_projected_monthly,
            "projected_monthly_gb": round(total_projected_monthly_bytes / 1_000_000_000, 2),
            "projected_annual_gb": round(total_annual_gb, 2),
        },
        "recommendation": {
            "postgres_tier": recommended_tier,
            "estimated_monthly_cost": estimated_cost,
            "partitioning_needed": needs_partitioning,
            "partitioning_strategy": (
                "Monthly range partitioning on timestamp columns for tables with >100K rows/month. "
                "PostgreSQL native partitioning (PARTITION BY RANGE) is sufficient — "
                "TimescaleDB not required at this scale. "
                "Partition key: the primary timestamp column (snapshot_at, timestamp, collected_at). "
                "Auto-create partitions monthly via pg_partman or a startup migration."
            ) if needs_partitioning else "No partitioning needed at current scale.",
            "retention_policy": (
                "Keep raw data for 90 days, then aggregate to daily rollups. "
                "Keep daily rollups indefinitely. "
                "Exception: peg_snapshots_5m — keep 30 days raw, hourly rollups for 1 year."
            ),
        },
        "tables": tables,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
