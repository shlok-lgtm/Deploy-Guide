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

    # Overlay collateral data from protocol_collateral_exposure
    collateral_by_symbol = {}
    try:
        collateral_rows = fetch_all("""
            SELECT token_symbol,
                   SUM(tvl_usd) AS total_collateral_tvl,
                   COUNT(DISTINCT protocol_slug) AS protocol_count
            FROM protocol_collateral_exposure
            WHERE is_stablecoin = TRUE
              AND is_sii_scored = FALSE
              AND snapshot_date = CURRENT_DATE
            GROUP BY token_symbol
        """)
        for cr in (collateral_rows or []):
            collateral_by_symbol[cr["token_symbol"].upper()] = {
                "total_collateral_tvl": float(cr["total_collateral_tvl"]),
                "protocol_count": int(cr["protocol_count"]),
            }
    except Exception as e:
        logger.warning(f"Could not fetch collateral data for demand signals: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="indexer_update_demand_signals_collateral_fetch_failure",
                error_message=str(e)[:500],
                cycle_phase="indexer_backlog",
            )
        except Exception:
            pass

    # Look up symbol for each token_address to match collateral data
    symbol_map = {}
    if collateral_by_symbol:
        try:
            sym_rows = fetch_all("""
                SELECT token_address, symbol
                FROM wallet_graph.unscored_assets
                WHERE symbol IS NOT NULL
            """)
            for sr in (sym_rows or []):
                symbol_map[sr["token_address"]] = sr["symbol"].upper()
        except Exception as e:
            logger.warning(f"Could not fetch unscored asset symbol map for demand signals: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="indexer_update_demand_signals_symbol_map_failure",
                    error_message=str(e)[:500],
                    cycle_phase="indexer_backlog",
                )
            except Exception:
                pass

    # Rank by combined signal: wallet holdings + collateral TVL (weighted 2x)
    for row in rows:
        addr = row["token_address"]
        sym = symbol_map.get(addr, "")
        coll = collateral_by_symbol.get(sym, {})
        row["_collateral_tvl"] = coll.get("total_collateral_tvl", 0)
        row["_protocol_count"] = coll.get("protocol_count", 0)

    rows.sort(
        key=lambda r: r["total_value_held"] + (r.get("_collateral_tvl", 0) * 2),
        reverse=True,
    )

    for rank, row in enumerate(rows, start=1):
        execute(
            """
            UPDATE wallet_graph.unscored_assets SET
                wallets_holding = %s,
                total_value_held = %s,
                avg_holding_value = %s,
                max_single_holding = %s,
                protocol_collateral_tvl = GREATEST(COALESCE(protocol_collateral_tvl, 0), %s),
                protocol_count = GREATEST(COALESCE(protocol_count, 0), %s),
                scoring_priority = %s,
                updated_at = NOW()
            WHERE token_address = %s
            """,
            (
                row["wallets_holding"],
                row["total_value_held"],
                row["avg_holding_value"],
                row["max_single_holding"],
                row["_collateral_tvl"],
                row["_protocol_count"],
                rank,
                row["token_address"],
            ),
        )

    logger.info(f"Updated demand signals for {len(rows)} unscored assets (with collateral overlay)")
    return len(rows)


def get_backlog(limit: int = 50) -> list[dict]:
    """Get unscored asset backlog sorted by priority (total_value_held DESC)."""
    return fetch_all(
        """
        SELECT token_address, symbol, name, decimals, coingecko_id,
               wallets_holding, total_value_held, avg_holding_value,
               max_single_holding, scoring_status, scoring_priority, notes,
               COALESCE(protocol_collateral_tvl, 0) AS protocol_collateral_tvl,
               COALESCE(protocol_count, 0) AS protocol_count,
               first_seen_at, last_seen_at
        FROM wallet_graph.unscored_assets
        ORDER BY (total_value_held + COALESCE(protocol_collateral_tvl, 0) * 2) DESC
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


KNOWN_ISSUERS = {
    "USDP": "Paxos",
    "GUSD": "Gemini",
    "BUSD": "Paxos",
    "LUSD": "Liquity",
    "SUSD": "Synthetix",
    "CRVUSD": "Curve",
    "GHO": "Aave",
    "DOLA": "Inverse Finance",
    "ALUSD": "Alchemix",
    "MIM": "Abracadabra",
    "EURC": "Circle",
    "USDM": "Mountain Protocol",
    "USDY": "Ondo Finance",
    "USDB": "Blast",
    "ZUSD": "Zai Finance",
    "R": "Raft",
    "WUSDM": "Mountain Protocol",
    "HUSD": "Stable Universal",
    "UST": "Terra",
    "FEI": "Fei Protocol",
    "RAI": "Reflexer",
    "BEAN": "Beanstalk",
}


def promote_eligible_assets() -> int:
    """
    Promote unscored stablecoins into the scoring table with scoring_enabled=TRUE.

    All stablecoins with a coingecko_id are eligible for promotion. The actual
    quality gate is category-completeness, enforced at scoring time in the worker
    (every v1 category must have >= 1 populated component).

    The old value-based thresholds ($1M holdings / $500K collateral) can still be
    used as an optional filter via env vars BACKLOG_VALUE_FILTER=true.

    Returns number of assets newly promoted.
    """
    use_value_filter = os.environ.get("BACKLOG_VALUE_FILTER", "").lower() in ("true", "1", "yes")

    if use_value_filter:
        threshold = float(os.environ.get("BACKLOG_PROMOTE_THRESHOLD", "1000000"))
        collateral_threshold = float(os.environ.get("BACKLOG_COLLATERAL_THRESHOLD", "500000"))
        eligible = fetch_all(
            """
            SELECT token_address, symbol, name, decimals, coingecko_id,
                   total_value_held,
                   COALESCE(protocol_collateral_tvl, 0) AS protocol_collateral_tvl,
                   COALESCE(protocol_count, 0) AS protocol_count
            FROM wallet_graph.unscored_assets
            WHERE (total_value_held >= %s OR COALESCE(protocol_collateral_tvl, 0) >= %s)
              AND scoring_status = 'unscored'
              AND coingecko_id IS NOT NULL
              AND token_type = 'stablecoin'
            ORDER BY (total_value_held + COALESCE(protocol_collateral_tvl, 0) * 2) DESC
            """,
            (threshold, collateral_threshold),
        )
    else:
        eligible = fetch_all(
            """
            SELECT token_address, symbol, name, decimals, coingecko_id,
                   total_value_held,
                   COALESCE(protocol_collateral_tvl, 0) AS protocol_collateral_tvl,
                   COALESCE(protocol_count, 0) AS protocol_count
            FROM wallet_graph.unscored_assets
            WHERE scoring_status = 'unscored'
              AND coingecko_id IS NOT NULL
              AND token_type = 'stablecoin'
            ORDER BY (total_value_held + COALESCE(protocol_collateral_tvl, 0) * 2) DESC
            """,
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT (id) DO UPDATE SET scoring_enabled = TRUE
                """,
                (
                    coin_id,
                    asset["name"] or asset["symbol"],
                    asset["symbol"],
                    KNOWN_ISSUERS.get(asset["symbol"].upper(), asset["name"] or "Unknown"),
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
            if asset.get("protocol_collateral_tvl", 0) > 0:
                logger.info(
                    f"AUTO-PROMOTE via collateral exposure: {asset['symbol']} — "
                    f"${asset['protocol_collateral_tvl']:,.0f} in {asset.get('protocol_count', 0)} protocol(s), "
                    f"${asset['total_value_held']:,.0f} in wallet holdings"
                )
            else:
                logger.info(
                    f"Promoted {asset['symbol']} ({asset['coingecko_id']}) "
                    f"to scoring queue — ${asset['total_value_held']:,.0f} held"
                )
            # Register new issuer in CDA pipeline for disclosure collection
            try:
                import asyncio
                from app.services.cda_collector import discover_new_issuer
                loop = asyncio.get_running_loop()
                loop.create_task(discover_new_issuer(asset["symbol"], asset["coingecko_id"]))
            except RuntimeError as rte:
                logger.warning(f"CDA issuer discovery deferred for {asset['symbol']} (no event loop): {rte}")
                try:
                    from app.worker import _record_cycle_error
                    _record_cycle_error(
                        error_type="indexer_promote_eligible_assets_cda_no_event_loop",
                        error_message=str(rte)[:500],
                        cycle_phase="indexer_backlog",
                    )
                except Exception:
                    pass
            except Exception as cda_e:
                logger.warning(f"CDA issuer discovery skipped for {asset['symbol']}: {cda_e}")
                try:
                    from app.worker import _record_cycle_error
                    _record_cycle_error(
                        error_type="indexer_promote_eligible_assets_cda_discovery_failure",
                        error_message=str(cda_e)[:500],
                        cycle_phase="indexer_backlog",
                    )
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"Failed to promote {asset['symbol']}: {e}")

    if promoted:
        if use_value_filter:
            logger.info(f"Promoted {promoted} backlog asset(s) to scoring (value threshold: ${threshold:,.0f})")
        else:
            logger.info(f"Promoted {promoted} backlog asset(s) to scoring (category-completeness gate)")
    return promoted
