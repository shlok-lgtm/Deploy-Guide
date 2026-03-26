"""
Wallet Indexer — Backlog
========================
Tracks unscored stablecoin assets discovered during wallet scanning.
Updates demand signals (wallets_holding, total_value_held) and computes
scoring priority so we know what to score next.
"""

import logging
import os
import re
from typing import Optional

from app.database import fetch_all, fetch_one, execute, get_cursor
from app.indexer.config import UNSCORED_CONTRACTS

logger = logging.getLogger(__name__)


def upsert_unscored_asset(
    token_address: str,
    symbol: str,
    name: str,
    decimals: int,
    coingecko_id: Optional[str] = None,
    token_type: str = "unknown",
) -> None:
    """Insert or update an unscored asset in the backlog.

    token_type: 'stablecoin', 'non_stablecoin', or 'unknown'.
      - Only 'stablecoin' rows are eligible for SII promotion.
      - On conflict, token_type is only overwritten if the incoming value is
        more specific than the stored value (unknown < stablecoin|non_stablecoin).
    """
    execute(
        """
        INSERT INTO wallet_graph.unscored_assets
            (token_address, symbol, name, decimals, coingecko_id,
             token_type, first_seen_at, last_seen_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW())
        ON CONFLICT (token_address) DO UPDATE SET
            symbol = COALESCE(EXCLUDED.symbol, wallet_graph.unscored_assets.symbol),
            name = COALESCE(EXCLUDED.name, wallet_graph.unscored_assets.name),
            decimals = COALESCE(EXCLUDED.decimals, wallet_graph.unscored_assets.decimals),
            coingecko_id = COALESCE(EXCLUDED.coingecko_id, wallet_graph.unscored_assets.coingecko_id),
            token_type = CASE
                WHEN wallet_graph.unscored_assets.token_type = 'unknown'
                    THEN EXCLUDED.token_type
                ELSE wallet_graph.unscored_assets.token_type
            END,
            last_seen_at = NOW(),
            updated_at = NOW()
        """,
        (token_address.lower(), symbol, name, decimals, coingecko_id, token_type),
    )


def update_demand_signals() -> int:
    """
    Recompute demand signals for all unscored assets from current wallet_holdings.
    Returns number of assets updated.
    """
    rows = fetch_all(
        """
        SELECT
            wh.token_address,
            COUNT(DISTINCT wh.wallet_address) AS wallets_holding,
            COALESCE(SUM(wh.value_usd), 0) AS total_value_held,
            COALESCE(AVG(wh.value_usd), 0) AS avg_holding_value,
            COALESCE(MAX(wh.value_usd), 0) AS max_single_holding
        FROM wallet_graph.wallet_holdings wh
        WHERE wh.is_scored = FALSE
        GROUP BY wh.token_address
        """
    )

    if not rows:
        return 0

    # Rank by total_value_held DESC for scoring priority
    rows.sort(key=lambda r: r["total_value_held"], reverse=True)

    for rank, row in enumerate(rows, start=1):
        execute(
            """
            UPDATE wallet_graph.unscored_assets SET
                wallets_holding = %s,
                total_value_held = %s,
                avg_holding_value = %s,
                max_single_holding = %s,
                scoring_priority = %s,
                updated_at = NOW()
            WHERE token_address = %s
            """,
            (
                row["wallets_holding"],
                row["total_value_held"],
                row["avg_holding_value"],
                row["max_single_holding"],
                rank,
                row["token_address"],
            ),
        )

    logger.info(f"Updated demand signals for {len(rows)} unscored assets")
    return len(rows)


def get_backlog(limit: int = 50) -> list[dict]:
    """Get unscored asset backlog sorted by priority (total_value_held DESC)."""
    return fetch_all(
        """
        SELECT token_address, symbol, name, decimals, coingecko_id,
               wallets_holding, total_value_held, avg_holding_value,
               max_single_holding, scoring_status, scoring_priority, notes,
               first_seen_at, last_seen_at
        FROM wallet_graph.unscored_assets
        ORDER BY total_value_held DESC
        LIMIT %s
        """,
        (limit,),
    )


def get_backlog_detail(token_address: str) -> Optional[dict]:
    """Get detail for one unscored asset including which wallets hold it."""
    asset = fetch_one(
        """
        SELECT token_address, symbol, name, decimals, coingecko_id,
               wallets_holding, total_value_held, avg_holding_value,
               max_single_holding, scoring_status, scoring_priority, notes,
               first_seen_at, last_seen_at
        FROM wallet_graph.unscored_assets
        WHERE token_address = %s
        """,
        (token_address.lower(),),
    )
    if not asset:
        return None

    # Get wallets holding this asset
    holders = fetch_all(
        """
        SELECT wh.wallet_address, wh.balance, wh.value_usd, wh.indexed_at,
               w.label, w.size_tier, w.total_stablecoin_value
        FROM wallet_graph.wallet_holdings wh
        JOIN wallet_graph.wallets w ON w.address = wh.wallet_address
        WHERE wh.token_address = %s
        ORDER BY wh.value_usd DESC
        LIMIT 100
        """,
        (token_address.lower(),),
    )

    asset["holders"] = holders
    return asset


def seed_known_unscored() -> int:
    """Seed the unscored_assets table with known unscored stablecoins from config."""
    count = 0
    for addr, info in UNSCORED_CONTRACTS.items():
        upsert_unscored_asset(
            token_address=addr,
            symbol=info["symbol"],
            name=info["name"],
            decimals=info["decimals"],
            coingecko_id=info.get("coingecko_id"),
            token_type="stablecoin",
        )
        count += 1
    logger.info(f"Seeded {count} known unscored assets into backlog")
    return count


def _make_coin_id(symbol: str) -> str:
    """Derive a safe stablecoin ID from a symbol (e.g. 'crvUSD' → 'crvusd')."""
    return re.sub(r"[^a-z0-9_]", "_", symbol.lower())[:20]


def promote_eligible_assets() -> int:
    """
    Promote unscored assets above the value threshold into the stablecoins
    scoring table with scoring_enabled=TRUE.

    Threshold is controlled by BACKLOG_PROMOTE_THRESHOLD env var (default $1M).
    Sets scoring_status to 'queued' in unscored_assets after promotion.
    Returns number of assets newly promoted.
    """
    threshold = float(os.environ.get("BACKLOG_PROMOTE_THRESHOLD", "1000000"))

    eligible = fetch_all(
        """
        SELECT token_address, symbol, name, decimals, coingecko_id, total_value_held
        FROM wallet_graph.unscored_assets
        WHERE total_value_held >= %s
          AND scoring_status = 'unscored'
          AND coingecko_id IS NOT NULL
          AND token_type = 'stablecoin'
        ORDER BY total_value_held DESC
        """,
        (threshold,),
    )

    if not eligible:
        return 0

    promoted = 0
    for asset in eligible:
        coin_id = _make_coin_id(asset["symbol"])
        try:
            execute(
                """
                INSERT INTO stablecoins
                    (id, name, symbol, issuer, coingecko_id, contract, decimals, scoring_enabled)
                VALUES (%s, %s, %s, 'Unknown', %s, %s, %s, TRUE)
                ON CONFLICT (id) DO UPDATE SET scoring_enabled = TRUE
                """,
                (
                    coin_id,
                    asset["name"] or asset["symbol"],
                    asset["symbol"],
                    asset["coingecko_id"],
                    asset["token_address"],
                    asset["decimals"],
                ),
            )
            execute(
                """
                UPDATE wallet_graph.unscored_assets
                SET scoring_status = 'queued', updated_at = NOW()
                WHERE token_address = %s
                """,
                (asset["token_address"],),
            )
            promoted += 1
            logger.info(
                f"Promoted {asset['symbol']} ({asset['coingecko_id']}) "
                f"to scoring queue — ${asset['total_value_held']:,.0f} held"
            )
            # Register new issuer in CDA pipeline for disclosure collection
            try:
                import asyncio
                from app.services.cda_collector import discover_new_issuer
                asyncio.run(discover_new_issuer(asset["symbol"], asset["coingecko_id"]))
            except Exception as cda_e:
                logger.debug(f"CDA issuer discovery skipped for {asset['symbol']}: {cda_e}")
        except Exception as e:
            logger.warning(f"Failed to promote {asset['symbol']}: {e}")

    if promoted:
        logger.info(f"Promoted {promoted} backlog asset(s) to scoring (threshold: ${threshold:,.0f})")
    return promoted
