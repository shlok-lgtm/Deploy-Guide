"""
Wallet Indexer — Pipeline
=========================
Orchestrator: seed wallets → scan holdings → score → store → update backlog.

Pipeline flow:
  1. SEED: Fetch top holders for each scored stablecoin via Etherscan
  2. SCAN: For each wallet, query balances across all known stablecoin contracts
  3. SCORE: Compute wallet risk score, HHI, coverage
  4. STORE: Upsert wallets, insert holdings + risk scores
  5. BACKLOG: Update demand signals for unscored assets
"""

import os
import asyncio
import logging
from datetime import datetime, timezone

import httpx

from app.database import fetch_all, fetch_one, execute
from app.indexer.config import ETHERSCAN_RATE_LIMIT_DELAY
from app.indexer.scanner import scan_wallet_holdings, fetch_top_holders
from app.indexer.scorer import compute_wallet_risk
from app.indexer.backlog import (
    upsert_unscored_asset,
    update_demand_signals,
    seed_known_unscored,
    promote_eligible_assets,
)

logger = logging.getLogger(__name__)


def _get_current_sii_scores() -> dict:
    """Load current SII scores and prices from the scores table."""
    rows = fetch_all(
        "SELECT stablecoin_id, overall_score, grade, current_price FROM scores"
    )
    return {
        row["stablecoin_id"]: {
            "overall_score": float(row["overall_score"]) if row["overall_score"] else None,
            "grade": row["grade"],
            "current_price": float(row["current_price"]) if row.get("current_price") else None,
        }
        for row in rows
    }


def _get_existing_wallets() -> set:
    """Get all wallet addresses already in the graph."""
    rows = fetch_all("SELECT address FROM wallet_graph.wallets")
    return {row["address"] for row in rows}


def _store_wallet(address: str, source: str, label: str = None) -> None:
    """Upsert a wallet into the wallets table."""
    execute(
        """
        INSERT INTO wallet_graph.wallets (address, source, label, created_at, updated_at)
        VALUES (%s, %s, %s, NOW(), NOW())
        ON CONFLICT (address) DO UPDATE SET
            updated_at = NOW()
        """,
        (address, source, label),
    )


def _store_holdings(wallet_address: str, holdings: list[dict]) -> None:
    """Insert wallet holdings snapshot (one per wallet/token/day)."""
    for h in holdings:
        # Delete existing row for today, then insert fresh
        execute(
            """
            DELETE FROM wallet_graph.wallet_holdings
            WHERE wallet_address = %s
              AND token_address = %s
              AND public.immutable_date(indexed_at) = CURRENT_DATE
            """,
            (wallet_address, h["token_address"]),
        )
        execute(
            """
            INSERT INTO wallet_graph.wallet_holdings
                (wallet_address, token_address, symbol, balance, value_usd,
                 is_scored, sii_score, sii_grade, pct_of_wallet, indexed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (
                wallet_address,
                h["token_address"],
                h["symbol"],
                h["balance"],
                h["value_usd"],
                h["is_scored"],
                h.get("sii_score"),
                h.get("sii_grade"),
                h.get("pct_of_wallet"),
            ),
        )


def _store_risk_score(wallet_address: str, risk: dict) -> None:
    """Insert wallet risk score snapshot (one per wallet/day)."""
    # Delete existing row for today, then insert fresh
    execute(
        """
        DELETE FROM wallet_graph.wallet_risk_scores
        WHERE wallet_address = %s
          AND public.immutable_date(computed_at) = CURRENT_DATE
        """,
        (wallet_address,),
    )
    execute(
        """
        INSERT INTO wallet_graph.wallet_risk_scores
            (wallet_address, risk_score, risk_grade,
             concentration_hhi, concentration_grade,
             unscored_pct, coverage_quality,
             num_scored_holdings, num_unscored_holdings, num_total_holdings,
             dominant_asset, dominant_asset_pct,
             total_stablecoin_value, size_tier, formula_version, computed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """,
        (
            wallet_address,
            risk.get("risk_score"),
            risk.get("risk_grade"),
            risk.get("concentration_hhi"),
            risk.get("concentration_grade"),
            risk.get("unscored_pct"),
            risk.get("coverage_quality"),
            risk.get("num_scored_holdings"),
            risk.get("num_unscored_holdings"),
            risk.get("num_total_holdings"),
            risk.get("dominant_asset"),
            risk.get("dominant_asset_pct"),
            risk.get("total_stablecoin_value"),
            risk.get("size_tier"),
            risk.get("formula_version"),
        ),
    )


def _update_wallet_summary(wallet_address: str, total_value: float, size_tier: str) -> None:
    """Update wallet summary fields after scoring."""
    execute(
        """
        UPDATE wallet_graph.wallets SET
            last_indexed_at = NOW(),
            total_stablecoin_value = %s,
            size_tier = %s,
            updated_at = NOW()
        WHERE address = %s
        """,
        (total_value, size_tier, wallet_address),
    )


def get_coverage_diagnostic() -> dict:
    """
    Query coverage breakdown: how many wallets have holdings vs scores vs silent failures.
    Returns a dict safe to log or include in API responses.
    """
    try:
        row = fetch_one(
            """
            SELECT
                COUNT(*) AS total_wallets,
                COUNT(*) FILTER (WHERE last_indexed_at IS NOT NULL) AS indexed_wallets,
                COUNT(*) FILTER (WHERE total_stablecoin_value > 0) AS wallets_with_value
            FROM wallet_graph.wallets
            """
        )
        holdings_row = fetch_one(
            """
            SELECT
                COUNT(DISTINCT wallet_address) AS wallets_with_holdings
            FROM wallet_graph.wallet_holdings
            """
        )
        no_holdings_row = fetch_one(
            """
            SELECT COUNT(*) AS wallets_no_holdings
            FROM wallet_graph.wallets w
            WHERE NOT EXISTS (
                SELECT 1 FROM wallet_graph.wallet_holdings wh
                WHERE wh.wallet_address = w.address
            )
            """
        )
        holdings_no_score_row = fetch_one(
            """
            SELECT COUNT(DISTINCT wh.wallet_address) AS holdings_no_score
            FROM wallet_graph.wallet_holdings wh
            WHERE NOT EXISTS (
                SELECT 1 FROM wallet_graph.wallet_risk_scores wrs
                WHERE wrs.wallet_address = wh.wallet_address
            )
            """
        )
        zero_value_with_holdings_row = fetch_one(
            """
            SELECT COUNT(DISTINCT wh.wallet_address) AS zero_value_with_holdings
            FROM wallet_graph.wallet_holdings wh
            JOIN wallet_graph.wallets w ON w.address = wh.wallet_address
            WHERE COALESCE(w.total_stablecoin_value, 0) = 0
            """
        )
        scored_row = fetch_one(
            """
            SELECT COUNT(DISTINCT wrs.wallet_address) AS wallets_scored
            FROM wallet_graph.wallet_risk_scores wrs
            WHERE wrs.computed_at = (
                SELECT MAX(wrs2.computed_at)
                FROM wallet_graph.wallet_risk_scores wrs2
                WHERE wrs2.wallet_address = wrs.wallet_address
            )
            AND wrs.risk_score IS NOT NULL
            """
        )

        total = (row or {}).get("total_wallets", 0)
        with_holdings = (holdings_row or {}).get("wallets_with_holdings", 0)
        no_holdings = (no_holdings_row or {}).get("wallets_no_holdings", 0)
        holdings_no_score = (holdings_no_score_row or {}).get("holdings_no_score", 0)
        zero_value = (zero_value_with_holdings_row or {}).get("zero_value_with_holdings", 0)
        scored = (scored_row or {}).get("wallets_scored", 0)

        diagnostic = {
            "total_wallets": total,
            "wallets_with_holdings": with_holdings,
            "wallets_no_holdings": no_holdings,
            "wallets_scored_latest": scored,
            "holdings_no_score": holdings_no_score,
            "zero_value_with_holdings": zero_value,
            "coverage_pct": round(scored / total * 100, 1) if total else 0,
            "silent_failure_count": holdings_no_score,
        }

        logger.info(
            f"Coverage diagnostic — total: {total}, with_holdings: {with_holdings}, "
            f"no_holdings: {no_holdings}, scored: {scored}, "
            f"holdings_no_score (silent failures): {holdings_no_score}, "
            f"zero_value_with_holdings: {zero_value}, "
            f"coverage: {diagnostic['coverage_pct']}%"
        )
        return diagnostic
    except Exception as e:
        logger.warning(f"Coverage diagnostic failed: {e}")
        return {"error": str(e)}


def _track_unscored_holdings(holdings: list[dict]) -> None:
    """Add any unscored holdings to the backlog."""
    for h in holdings:
        if not h["is_scored"]:
            upsert_unscored_asset(
                token_address=h["token_address"],
                symbol=h["symbol"],
                name=h.get("name", ""),
                decimals=h.get("decimals", 18),
            )


def _seed_from_known_holders() -> set:
    """Seed from the curated known holder list in the Etherscan collector."""
    from app.collectors.etherscan import KNOWN_HOLDERS
    addresses = set()
    for addr, label, category in KNOWN_HOLDERS:
        if addr and addr.startswith("0x"):
            addresses.add(addr)
    logger.info(f"Seeded {len(addresses)} addresses from known holder list")
    return addresses



def _get_scored_stablecoins_from_db() -> list[dict]:
    """Load all scored stablecoins with contract addresses from the database."""
    rows = fetch_all(
        """
        SELECT id, symbol, name, contract, decimals
        FROM stablecoins
        WHERE contract IS NOT NULL AND contract != ''
        """
    )
    return [
        {
            "stablecoin_id": row["id"],
            "symbol": row["symbol"],
            "name": row["name"],
            "contract": row["contract"].lower(),
            "decimals": row["decimals"] or 18,
        }
        for row in rows
    ]


async def seed_wallets(
    client: httpx.AsyncClient,
    api_key: str,
    holders_per_coin: int = None,
) -> set:
    """
    Step 1: Seed wallet addresses from multiple sources.

    Priority order:
      1. tokenholderlist API (Standard tier — top holders per coin, paginated)
      2. Curated known holders (always available, baseline)
    """
    if holders_per_coin is None:
        try:
            holders_per_coin = int(os.environ.get("INDEXER_HOLDERS_PER_COIN", "5000"))
        except (ValueError, TypeError):
            holders_per_coin = 5000

    all_addresses = set()

    # Load stablecoins from the database (not hardcoded SCORED_CONTRACTS)
    db_coins = _get_scored_stablecoins_from_db()
    logger.info(f"Loaded {len(db_coins)} stablecoins with contracts from database")

    # Primary: tokenholderlist API (Standard tier, paginated 100 per page)
    page_size = 100
    pages_needed = max(1, holders_per_coin // page_size)

    for coin in db_coins:
        sid = coin["stablecoin_id"]
        contract = coin["contract"]
        coin_holders = []

        for page in range(1, pages_needed + 1):
            holders = await fetch_top_holders(
                client, contract, api_key,
                page=page, offset=page_size,
            )
            await asyncio.sleep(ETHERSCAN_RATE_LIMIT_DELAY)

            if not holders:
                break  # No more pages for this coin

            coin_holders.extend(holders)

        for addr in coin_holders:
            if addr and addr.startswith("0x"):
                all_addresses.add(addr)

        if coin_holders:
            logger.info(f"  {sid}: {len(coin_holders)} holders from tokenholderlist")
        else:
            logger.warning(f"  {sid}: tokenholderlist returned 0 holders")

    # Always include curated known holders as baseline
    known = _seed_from_known_holders()
    all_addresses |= known

    logger.info(f"Seeded {len(all_addresses)} unique wallet addresses total")
    return all_addresses


async def index_wallet(
    client: httpx.AsyncClient,
    wallet_address: str,
    api_key: str,
    sii_scores: dict,
    source: str = "top_holder",
) -> dict:
    """
    Steps 2-4 for a single wallet: scan → score → store.
    Returns summary dict.
    """
    # Ensure wallet exists in table
    _store_wallet(wallet_address, source=source)

    # Scan holdings
    holdings = await scan_wallet_holdings(client, wallet_address, api_key, sii_scores)

    if not holdings:
        _update_wallet_summary(wallet_address, 0, "retail")
        return {"address": wallet_address, "holdings": 0, "scored": False, "reason": "no_holdings"}

    # Score
    risk = compute_wallet_risk(holdings)
    if not risk:
        logger.warning(f"Risk compute returned None for {wallet_address} ({len(holdings)} holdings)")
        _update_wallet_summary(wallet_address, 0, "retail")
        return {"address": wallet_address, "holdings": len(holdings), "scored": False, "reason": "risk_compute_failed"}

    # Store
    _store_holdings(wallet_address, holdings)
    _store_risk_score(wallet_address, risk)
    _update_wallet_summary(
        wallet_address,
        risk["total_stablecoin_value"],
        risk["size_tier"],
    )

    # Track unscored assets in backlog
    _track_unscored_holdings(holdings)

    return {
        "address": wallet_address,
        "holdings": len(holdings),
        "scored": True,
        "risk_score": risk.get("risk_score"),
        "risk_grade": risk.get("risk_grade"),
        "total_value": risk.get("total_stablecoin_value"),
        "size_tier": risk.get("size_tier"),
    }


async def run_pipeline(holders_per_coin: int = None) -> dict:
    """
    Full pipeline run: seed → scan → score → store → backlog update.

    Args:
        holders_per_coin: number of top holders to fetch per stablecoin.
                          Defaults to INDEXER_HOLDERS_PER_COIN env var (default 5000).

    Returns:
        Summary dict with counts and stats.
    """
    if holders_per_coin is None:
        try:
            holders_per_coin = int(os.environ.get("INDEXER_HOLDERS_PER_COIN", "5000"))
        except (ValueError, TypeError):
            holders_per_coin = 5000
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        logger.error("ETHERSCAN_API_KEY not set — cannot run wallet indexer")
        return {"error": "ETHERSCAN_API_KEY not set", "wallets_indexed": 0}

    started_at = datetime.now(timezone.utc)
    logger.info("=== Wallet Indexer Pipeline Starting ===")

    # Load current SII scores
    sii_scores = _get_current_sii_scores()
    logger.info(f"Loaded SII scores for {len(sii_scores)} stablecoins")

    # Seed known unscored assets
    seed_known_unscored()

    # Build known holder set for source tagging
    known_addrs = _seed_from_known_holders()

    async with httpx.AsyncClient() as client:
        # Step 1: Seed
        wallet_addresses = await seed_wallets(client, api_key, holders_per_coin)

        # Also include existing wallets for re-indexing
        existing = _get_existing_wallets()
        wallet_addresses |= existing
        logger.info(f"Total wallets to index: {len(wallet_addresses)} ({len(existing)} existing)")

        # Steps 2-4: Scan → Score → Store (sequential to respect rate limits)
        results = []
        indexed = 0
        errors = 0

        for addr in wallet_addresses:
            try:
                source = "known_holder" if addr in known_addrs else "top_holder"
                result = await index_wallet(
                    client, addr, api_key, sii_scores,
                    source=source,
                )
                results.append(result)
                indexed += 1
                if indexed % 50 == 0:
                    logger.info(f"  Progress: {indexed}/{len(wallet_addresses)} wallets indexed")
            except Exception as e:
                logger.warning(f"Error indexing {addr}: {type(e).__name__}: {e}")
                errors += 1

    # Step 5: Update backlog demand signals
    backlog_count = update_demand_signals()

    # Step 6: Promote eligible backlog assets to scoring queue
    promoted_count = promote_eligible_assets()

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    scored_count = sum(1 for r in results if r.get("scored"))
    no_holdings_count = sum(1 for r in results if r.get("reason") == "no_holdings")
    scoring_failed_count = sum(1 for r in results if r.get("reason") == "risk_compute_failed")

    # Step 7: Run coverage diagnostic to surface any silent failure paths
    coverage = get_coverage_diagnostic()

    summary = {
        "wallets_discovered": len(wallet_addresses),
        "wallets_indexed": indexed,
        "wallets_scored": scored_count,
        "wallets_no_holdings": no_holdings_count,
        "wallets_scoring_failed": scoring_failed_count,
        "errors": errors,
        "unscored_assets_tracked": backlog_count,
        "assets_promoted_to_scoring": promoted_count,
        "sii_scores_loaded": len(sii_scores),
        "elapsed_seconds": round(elapsed, 1),
        "started_at": started_at.isoformat(),
        "coverage": coverage,
    }

    logger.info(
        f"=== Pipeline Complete: {indexed} wallets indexed, "
        f"{scored_count} scored, {no_holdings_count} no holdings, "
        f"{scoring_failed_count} scoring failed, {errors} errors, "
        f"{backlog_count} unscored assets tracked, "
        f"{promoted_count} promoted to scoring, "
        f"{elapsed:.0f}s elapsed ==="
    )

    return summary
