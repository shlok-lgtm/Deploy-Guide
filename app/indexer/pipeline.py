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
from app.indexer.config import (
    SCORED_CONTRACTS,
    ETHERSCAN_RATE_LIMIT_DELAY,
    classify_size_tier,
)
from app.indexer.scanner import scan_wallet_holdings, fetch_top_holders
from app.indexer.scorer import compute_wallet_risk
from app.indexer.backlog import (
    upsert_unscored_asset,
    update_demand_signals,
    seed_known_unscored,
)

logger = logging.getLogger(__name__)


def _get_current_sii_scores() -> dict:
    """Load current SII scores from the scores table."""
    rows = fetch_all(
        "SELECT stablecoin_id, overall_score, grade FROM scores"
    )
    return {
        row["stablecoin_id"]: {
            "overall_score": float(row["overall_score"]) if row["overall_score"] else None,
            "grade": row["grade"],
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
              AND immutable_date(indexed_at) = CURRENT_DATE
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
          AND immutable_date(computed_at) = CURRENT_DATE
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


async def seed_wallets(
    client: httpx.AsyncClient,
    api_key: str,
    holders_per_coin: int = 100,
) -> set:
    """
    Step 1: Seed wallet addresses from top holders of each scored stablecoin.
    Tries Etherscan tokenholderlist API first (Pro), falls back to curated
    known holder addresses from the Etherscan collector.
    Returns set of unique wallet addresses.
    """
    all_addresses = set()

    for contract_lower, info in SCORED_CONTRACTS.items():
        sid = info["stablecoin_id"]
        logger.info(f"Fetching top {holders_per_coin} holders for {sid}…")

        holders = await fetch_top_holders(
            client, contract_lower, api_key,
            page=1, offset=holders_per_coin,
        )
        await asyncio.sleep(ETHERSCAN_RATE_LIMIT_DELAY)

        for addr in holders:
            if addr and addr.startswith("0x"):
                all_addresses.add(addr)

        logger.info(f"  {sid}: {len(holders)} holders fetched")

    # Fallback: if Pro API unavailable, seed from curated known holders
    if not all_addresses:
        logger.info("tokenholderlist API unavailable (requires Pro) — using known holder list")
        all_addresses = _seed_from_known_holders()

    logger.info(f"Seeded {len(all_addresses)} unique wallet addresses")
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
        return {"address": wallet_address, "holdings": 0, "scored": False}

    # Score
    risk = compute_wallet_risk(holdings)
    if not risk:
        _update_wallet_summary(wallet_address, 0, "retail")
        return {"address": wallet_address, "holdings": len(holdings), "scored": False}

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


async def run_pipeline(holders_per_coin: int = 100) -> dict:
    """
    Full pipeline run: seed → scan → score → store → backlog update.

    Args:
        holders_per_coin: number of top holders to fetch per stablecoin (default 100)

    Returns:
        Summary dict with counts and stats.
    """
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
                result = await index_wallet(
                    client, addr, api_key, sii_scores,
                    source=f"top_holder",
                )
                results.append(result)
                indexed += 1
                if indexed % 50 == 0:
                    logger.info(f"  Progress: {indexed}/{len(wallet_addresses)} wallets indexed")
            except Exception as e:
                logger.warning(f"Error indexing {addr[:10]}…: {e}")
                errors += 1

    # Step 5: Update backlog demand signals
    backlog_count = update_demand_signals()

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    scored_count = sum(1 for r in results if r.get("scored"))

    summary = {
        "wallets_discovered": len(wallet_addresses),
        "wallets_indexed": indexed,
        "wallets_scored": scored_count,
        "errors": errors,
        "unscored_assets_tracked": backlog_count,
        "sii_scores_loaded": len(sii_scores),
        "elapsed_seconds": round(elapsed, 1),
        "started_at": started_at.isoformat(),
    }

    logger.info(
        f"=== Pipeline Complete: {indexed} wallets indexed, "
        f"{scored_count} scored, {backlog_count} unscored assets tracked, "
        f"{elapsed:.0f}s elapsed ==="
    )

    return summary
