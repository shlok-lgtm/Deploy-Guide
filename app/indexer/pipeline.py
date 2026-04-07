"""
Wallet Indexer — Pipeline
=========================
Orchestrator: seed wallets → batch-scan holdings → score → store → update backlog.

Pipeline flow:
  1. SEED:    Fetch top holders for each scored stablecoin via Etherscan
  2. SCAN:    Contract-first batch scan — tokenbalancemulti (20 wallets/call)
             Old: 24 contracts × N wallets = ~1.07M calls (~48h for 44k wallets)
             New: 24 contracts × ⌈N/20⌉ batches = ~53k calls (~2-3h for 44k wallets)
  3. SCORE:   Compute wallet risk score, HHI, coverage (per-wallet, same as before)
  4. STORE:   Upsert wallets, insert holdings + risk scores
  5. BACKLOG: Update demand signals for unscored assets
"""

import os
import re
import asyncio
import logging
from datetime import datetime, timezone

import httpx

from app.database import fetch_all, fetch_one, execute
from app.indexer.config import BLOCK_EXPLORER_PROVIDER, EXPLORER_RATE_LIMIT_DELAY
from app.indexer.scanner import batch_scan_all_holdings, fetch_top_holders, fetch_token_list
from app.indexer.scorer import compute_wallet_risk
from app.indexer.backlog import (
    upsert_unscored_asset,
    update_demand_signals,
    seed_known_unscored,
    promote_eligible_assets,
)

# ---------------------------------------------------------------------------
# Stablecoin classification — symbol pattern match (fast, no external calls)
# ---------------------------------------------------------------------------
_STABLECOIN_PATTERN = re.compile(
    r"(usd[ct]?|usdt|usdc|busd|gusd|dai|frax|eur[os]?|eurs|gbp|chf|jpy|cnh|"
    r"tusd|usdp|susd|lusd|dola|mim|crvusd|gho|pyusd|usdd|fdusd|usdb|eurc)",
    re.IGNORECASE,
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


def _get_existing_wallets(chain: str = "ethereum") -> set:
    """Get all wallet addresses already in the graph for a given chain."""
    rows = fetch_all("SELECT address FROM wallet_graph.wallets WHERE chain = %s", (chain,))
    return {row["address"] for row in rows}


def _store_wallet(address: str, source: str, label: str = None, chain: str = "ethereum") -> None:
    """Upsert a wallet into the wallets table."""
    execute(
        """
        INSERT INTO wallet_graph.wallets (address, chain, source, label, created_at, updated_at)
        VALUES (%s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (address, chain) DO UPDATE SET
            updated_at = NOW()
        """,
        (address, chain, source, label),
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


def _update_wallet_summary(wallet_address: str, total_value: float, size_tier: str, chain: str = "ethereum") -> None:
    """Update wallet summary fields after scoring.

    NOTE: This only runs when the scoring pipeline processes this wallet.
    Wallets not recently scored retain stale total_stablecoin_value.
    Leaderboard and display queries should compute totals from wallet_holdings,
    not from this cached field.
    """
    execute(
        """
        UPDATE wallet_graph.wallets SET
            last_indexed_at = NOW(),
            total_stablecoin_value = %s,
            size_tier = %s,
            updated_at = NOW()
        WHERE address = %s AND chain = %s
        """,
        (total_value, size_tier, wallet_address, chain),
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


def _classify_token(symbol: str) -> str:
    """Classify a token as 'stablecoin' or 'non_stablecoin' by symbol pattern."""
    if symbol and _STABLECOIN_PATTERN.search(symbol):
        return "stablecoin"
    return "non_stablecoin"


def _get_known_contract_addresses() -> set:
    """
    Return the full set of already-known contract addresses (lowercased)
    from both the stablecoins table and wallet_graph.unscored_assets.
    Used to filter out contracts we've already seen during discovery.
    """
    known = set()
    try:
        rows = fetch_all(
            "SELECT LOWER(contract) AS addr FROM stablecoins WHERE contract IS NOT NULL AND contract != ''"
        )
        for r in rows:
            if r["addr"]:
                known.add(r["addr"])
    except Exception as e:
        logger.warning(f"Could not fetch known stablecoin contracts: {e}")

    try:
        rows = fetch_all("SELECT token_address FROM wallet_graph.unscored_assets")
        for r in rows:
            if r["token_address"]:
                known.add(r["token_address"].lower())
    except Exception as e:
        logger.warning(f"Could not fetch known unscored_assets: {e}")

    return known


async def discover_new_tokens(
    client: httpx.AsyncClient,
    api_key: str,
    sample_size: int = 200,
) -> tuple[dict, list]:
    """
    Phase 1 of the discovery loop: sample top whale wallets and call
    fetch_token_list() for each to find ERC-20 contracts not yet tracked.

    Steps:
      1. Query top `sample_size` wallets by total_stablecoin_value.
      2. Call fetch_token_list() for each (rate-limited).
      3. Extract unique contract addresses from the tokentx responses.
         Metadata (symbol, name, decimals) comes directly from the response —
         no extra API calls needed.
      4. Filter out contracts already in stablecoins or unscored_assets.
      5. Classify each new contract by symbol pattern.
      6. Upsert into unscored_assets with token_type.

    Returns:
        (new_contracts, sampled_wallets) where:
          new_contracts: dict of contract_addr → {symbol, name, decimals, token_type}
          sampled_wallets: list of wallet addresses that were queried (reused in Phase 2)
    """
    # Step 1: Sample top whale wallets
    rows = fetch_all(
        """
        SELECT address FROM wallet_graph.wallets
        WHERE total_stablecoin_value IS NOT NULL
        ORDER BY total_stablecoin_value DESC
        LIMIT %s
        """,
        (sample_size,),
    )
    sampled_wallets = [r["address"] for r in rows]

    if not sampled_wallets:
        logger.info("Discovery: no wallets in graph yet — skipping")
        return {}, []

    logger.info(
        f"Discovery Phase 1: fetching token lists for {len(sampled_wallets)} whale wallets"
    )

    # Step 2 + 3: Fetch token lists and collect unique contracts
    known = _get_known_contract_addresses()
    seen_this_run: dict[str, dict] = {}

    for wallet in sampled_wallets:
        txs = await fetch_token_list(client, wallet, api_key)
        await asyncio.sleep(EXPLORER_RATE_LIMIT_DELAY)

        if not txs:
            continue

        for tx in txs:
            addr = tx.get("contractAddress", "").lower()
            if not addr or addr in known or addr in seen_this_run:
                continue
            symbol = tx.get("tokenSymbol", "") or ""
            name = tx.get("tokenName", "") or ""
            try:
                decimals = int(tx.get("tokenDecimal", "18"))
            except (ValueError, TypeError):
                decimals = 18

            seen_this_run[addr] = {
                "symbol": symbol,
                "name": name,
                "decimals": decimals,
                "token_type": _classify_token(symbol),
            }

    if not seen_this_run:
        logger.info("Discovery Phase 1: no new contracts found")
        return {}, sampled_wallets

    # Step 4 is already handled above (known set filter).
    # Step 5+6: Upsert each new contract with its classified token_type
    stablecoin_count = 0
    non_stablecoin_count = 0
    for addr, info in seen_this_run.items():
        upsert_unscored_asset(
            token_address=addr,
            symbol=info["symbol"],
            name=info["name"],
            decimals=info["decimals"],
            token_type=info["token_type"],
        )
        if info["token_type"] == "stablecoin":
            stablecoin_count += 1
        else:
            non_stablecoin_count += 1

    logger.info(
        f"Discovery Phase 1 complete: {len(seen_this_run)} new contracts found "
        f"({stablecoin_count} stablecoin, {non_stablecoin_count} non_stablecoin) "
        f"from {len(sampled_wallets)} wallets"
    )
    return seen_this_run, sampled_wallets


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
            await asyncio.sleep(EXPLORER_RATE_LIMIT_DELAY)

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
    Single-wallet scan → score → store (used for ad-hoc re-scans, not the main pipeline).
    The main pipeline now uses batch_scan_all_holdings + per-wallet scoring in run_pipeline.
    Returns summary dict.
    """
    from app.indexer.scanner import scan_wallet_holdings

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


_reindex_status = {
    "last_reindex_started": None,
    "last_reindex_completed": None,
    "wallets_queued": 0,
    "wallets_processed": 0,
    "balances_updated": 0,
    "errors": 0,
    "last_error": None,
    "batch_size": 0,
    "batch_failures": 0,
}

reindex_logger = logging.getLogger("basis.reindex")


def get_reindex_status() -> dict:
    """Return current reindex run metadata plus DB freshness."""
    status = dict(_reindex_status)
    status["provider"] = BLOCK_EXPLORER_PROVIDER

    # Latest wallet_holdings row timestamp
    try:
        row = fetch_one("""
            SELECT MAX(indexed_at) AS latest
            FROM wallet_graph.wallet_holdings
        """)
        status["wallet_holdings_latest"] = (
            row["latest"].isoformat() if row and row["latest"] else None
        )
    except Exception:
        status["wallet_holdings_latest"] = None

    # Active wallet count
    try:
        row = fetch_one("SELECT COUNT(*) AS cnt FROM wallet_graph.wallets")
        status["active_wallet_count"] = row["cnt"] if row else 0
    except Exception:
        status["active_wallet_count"] = 0

    return status


def run_pipeline_batch(batch_size: int = 500) -> dict:
    """
    Incremental batch re-indexing: scan + score the oldest `batch_size` wallets.

    Unlike run_pipeline(), this does NOT seed new wallets or run discovery.
    It processes wallets already in the graph that haven't been re-indexed recently.
    Designed to be called externally via cron (every 30 min) so container restarts
    don't lose progress.

    Returns:
        Summary dict with processed/remaining counts.
    """
    import traceback

    _reindex_status["last_reindex_started"] = datetime.now(timezone.utc).isoformat()
    _reindex_status["last_reindex_completed"] = None
    _reindex_status["wallets_processed"] = 0
    _reindex_status["balances_updated"] = 0
    _reindex_status["errors"] = 0
    _reindex_status["last_error"] = None
    _reindex_status["batch_size"] = batch_size
    _reindex_status["batch_failures"] = 0

    # Resolve API key — Blockscout is free (no key needed), Etherscan requires one
    if BLOCK_EXPLORER_PROVIDER == "etherscan":
        api_key = os.environ.get("ETHERSCAN_API_KEY", "")
        if not api_key:
            msg = "ETHERSCAN_API_KEY not set but BLOCK_EXPLORER_PROVIDER=etherscan"
            reindex_logger.error(msg)
            _reindex_status["last_error"] = msg
            _reindex_status["last_reindex_completed"] = datetime.now(timezone.utc).isoformat()
            return {"error": msg, "processed": 0}
    else:
        # Blockscout primary — no key required.  Etherscan key used only for fallback.
        api_key = os.environ.get("ETHERSCAN_API_KEY", "")

    started_at = datetime.now(timezone.utc)
    reindex_logger.info(
        f"=== Wallet Batch Re-index Starting === "
        f"batch_size={batch_size}, provider={BLOCK_EXPLORER_PROVIDER}, "
        f"etherscan_key={'set (' + api_key[:6] + '...)' if api_key else 'not set (fallback disabled)'}"
    )

    # Find the oldest EVM wallets (not indexed in last 24h)
    # Solana addresses (base58, no 0x prefix) are skipped — they use a separate scanner
    try:
        stale_rows = fetch_all("""
            SELECT address FROM wallet_graph.wallets
            WHERE address LIKE '0x%%'
              AND (last_indexed_at IS NULL
                   OR last_indexed_at < NOW() - INTERVAL '24 hours')
            ORDER BY last_indexed_at ASC NULLS FIRST
            LIMIT %s
        """, (batch_size,))
    except Exception as e:
        msg = f"Failed to query stale wallets: {e}"
        reindex_logger.error(msg, exc_info=True)
        _reindex_status["last_error"] = msg
        _reindex_status["last_reindex_completed"] = datetime.now(timezone.utc).isoformat()
        return {"error": msg, "processed": 0}

    wallet_list = [r["address"] for r in stale_rows]
    _reindex_status["wallets_queued"] = len(wallet_list)

    if not wallet_list:
        reindex_logger.info("All wallets fresh — nothing to reindex")
        _reindex_status["last_reindex_completed"] = datetime.now(timezone.utc).isoformat()
        return {"processed": 0, "remaining": 0, "message": "All wallets fresh"}

    # Count total remaining for reporting (EVM only)
    remaining_row = fetch_one("""
        SELECT COUNT(*) AS cnt FROM wallet_graph.wallets
        WHERE address LIKE '0x%%'
          AND (last_indexed_at IS NULL
               OR last_indexed_at < NOW() - INTERVAL '24 hours')
    """)
    total_remaining = remaining_row["cnt"] if remaining_row else 0

    reindex_logger.info(f"Batch: {len(wallet_list)} wallets queued, {total_remaining} total stale")

    sii_scores = _get_current_sii_scores()
    indexed = 0
    scored = 0
    errors = 0
    balances_updated = 0

    async def _scan_and_score():
        nonlocal indexed, scored, errors, balances_updated
        async with httpx.AsyncClient() as client:
            reindex_logger.info("Starting batch_scan_all_holdings...")
            all_holdings, failures, api_calls, failed_addrs = await batch_scan_all_holdings(
                client, wallet_list, api_key, sii_scores
            )
            _reindex_status["batch_failures"] = failures

            reindex_logger.info(
                f"Scan complete: {len(all_holdings)} wallets with holdings, "
                f"{failures} batch failures, {api_calls} API calls, "
                f"{len(failed_addrs)} wallets deferred for retry"
            )

            if failures > 0 and len(all_holdings) == 0:
                reindex_logger.error(
                    f"ALL batches failed ({failures} failures, 0 holdings). "
                    f"Likely API key or provider misconfiguration."
                )

            for addr in wallet_list:
                try:
                    addr_lower = addr.lower()

                    # Skip wallets where the API call failed —
                    # don't update last_indexed_at so they're retried next cycle
                    if addr_lower in failed_addrs:
                        reindex_logger.info(f"Skipping {addr[:10]}… — API failed, will retry next cycle")
                        errors += 1
                        continue

                    holdings = all_holdings.get(addr_lower, [])

                    if not holdings:
                        _update_wallet_summary(addr, 0, "retail")
                        indexed += 1
                        continue

                    risk = compute_wallet_risk(holdings)
                    if not risk:
                        _update_wallet_summary(addr, 0, "retail")
                        indexed += 1
                        continue

                    _store_holdings(addr, holdings)
                    balances_updated += len(holdings)
                    _store_risk_score(addr, risk)
                    _update_wallet_summary(addr, risk["total_stablecoin_value"], risk["size_tier"])
                    _track_unscored_holdings(holdings)
                    indexed += 1
                    scored += 1
                except Exception as e:
                    reindex_logger.error(
                        f"Batch error for {addr}: {type(e).__name__}: {e}",
                        exc_info=True,
                    )
                    _reindex_status["last_error"] = f"{addr}: {type(e).__name__}: {e}"
                    errors += 1

    try:
        asyncio.run(_scan_and_score())
    except Exception as e:
        msg = f"asyncio.run(_scan_and_score) crashed: {type(e).__name__}: {e}"
        reindex_logger.error(msg, exc_info=True)
        _reindex_status["last_error"] = msg
        errors += 1

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()

    _reindex_status["wallets_processed"] = indexed
    _reindex_status["balances_updated"] = balances_updated
    _reindex_status["errors"] = errors
    _reindex_status["last_reindex_completed"] = datetime.now(timezone.utc).isoformat()

    reindex_logger.info(
        f"=== Batch Re-index Complete === "
        f"indexed={indexed}, scored={scored}, balances_updated={balances_updated}, "
        f"errors={errors}, batch_failures={_reindex_status['batch_failures']}, "
        f"elapsed={elapsed:.0f}s, remaining={total_remaining - indexed}"
    )

    return {
        "processed": indexed,
        "scored": scored,
        "balances_updated": balances_updated,
        "errors": errors,
        "batch_failures": _reindex_status["batch_failures"],
        "remaining": total_remaining - indexed,
        "elapsed_seconds": round(elapsed, 1),
    }


async def run_pipeline(holders_per_coin: int = None, force_reseed: bool = False) -> dict:
    """
    Full pipeline run: seed → scan → score → store → backlog update.

    Args:
        holders_per_coin: number of top holders to fetch per stablecoin.
                          Defaults to INDEXER_HOLDERS_PER_COIN env var (default 5000).
        force_reseed: if True, always run a full top-holder seed even when the wallet
                      table already has >1000 entries. Also overrides per-wallet resume
                      filtering so every wallet is rescanned regardless of last_indexed_at.

    Returns:
        Summary dict with counts and stats.
    """
    if holders_per_coin is None:
        try:
            holders_per_coin = int(os.environ.get("INDEXER_HOLDERS_PER_COIN", "5000"))
        except (ValueError, TypeError):
            holders_per_coin = 5000

    # Provider-sensitive API key selection
    # Blockscout is free (no key needed); Etherscan requires a key
    if BLOCK_EXPLORER_PROVIDER == "etherscan":
        api_key = os.environ.get("ETHERSCAN_API_KEY", "")
        if not api_key:
            logger.error("ETHERSCAN_API_KEY not set — cannot run wallet indexer with etherscan provider")
            return {"error": "ETHERSCAN_API_KEY not set", "wallets_indexed": 0}
    else:
        # Blockscout primary — Etherscan key used for fallback only
        api_key = os.environ.get("ETHERSCAN_API_KEY", "")

    started_at = datetime.now(timezone.utc)
    logger.info("=== Wallet Indexer Pipeline Starting ===")

    # Verify DB connection before doing any work — catches stale pool connections
    # that would otherwise surface as a cryptic error mid-pipeline.
    try:
        test = fetch_one("SELECT 1 AS alive")
        logger.info(f"Pipeline DB connection verified: {test}")
    except Exception as e:
        logger.error(f"Pipeline DB connection DEAD at startup: {e}")
        return {"error": "DB connection failed at pipeline startup", "wallets_indexed": 0}

    # Load current SII scores
    sii_scores = _get_current_sii_scores()
    logger.info(f"Loaded SII scores for {len(sii_scores)} stablecoins")

    # Seed known unscored assets
    seed_known_unscored()

    # Build known holder set for source tagging
    known_addrs = _seed_from_known_holders()

    new_contracts_discovered = 0
    tiered_scan_api_calls = 0

    # Resume logic: determine whether to seed or use existing wallets only
    existing = _get_existing_wallets()
    do_seed = force_reseed or len(existing) < 1000

    async with httpx.AsyncClient() as client:
        # Step 1: Seed (conditionally)
        if do_seed:
            logger.info(
                f"{'Force-reseed' if force_reseed else 'Full seed'} run — "
                f"fetching top holders from explorer ({len(existing)} existing wallets)"
            )
            wallet_addresses = await seed_wallets(client, api_key, holders_per_coin)
            wallet_addresses |= existing
        else:
            logger.info(f"Incremental run — {len(existing)} existing wallets, skipping seed")
            wallet_addresses = existing

        # Resume filter: skip wallets already indexed today (unless force_reseed)
        if force_reseed:
            skipped = 0
            wallet_list = list(wallet_addresses)
        else:
            try:
                indexed_today_rows = fetch_all("""
                    SELECT address FROM wallet_graph.wallets
                    WHERE last_indexed_at IS NOT NULL
                      AND last_indexed_at >= CURRENT_DATE
                """)
                already_indexed_today = {row["address"] for row in indexed_today_rows}
            except Exception as e:
                logger.warning(f"Could not query already-indexed wallets: {e} — scanning all")
                already_indexed_today = set()

            wallet_list = [w for w in wallet_addresses if w not in already_indexed_today]
            skipped = len(wallet_addresses) - len(wallet_list)
            if skipped > 0:
                logger.info(
                    f"Skipping {skipped} wallets already indexed today, "
                    f"{len(wallet_list)} remaining"
                )

        if not wallet_list:
            logger.info("All wallets already indexed today — nothing to do")
            logger.info(
                f"PIPELINE_COMPLETE status=success wallets=0 scored=0 "
                f"skipped={skipped} duration=0s"
            )
            return {
                "wallets_discovered": len(wallet_addresses),
                "wallets_indexed": 0,
                "wallets_skipped_today": skipped,
                "message": "All wallets already indexed today",
            }

        # Filter to EVM addresses only — Solana (base58, no 0x prefix) use a separate scanner
        non_evm = [w for w in wallet_list if not w.startswith("0x")]
        if non_evm:
            wallet_list = [w for w in wallet_list if w.startswith("0x")]
            logger.info(f"Filtered out {len(non_evm)} non-EVM addresses (Solana etc), {len(wallet_list)} EVM wallets remain")

        logger.info(f"Total wallets to index: {len(wallet_list)} ({skipped} skipped as already indexed today)")

        # Step 1b: Discovery — find new ERC-20 contracts from top whale wallets
        logger.info("Discovery Phase 1: scanning whale wallets for new ERC-20 contracts")
        new_contracts, sampled_wallets = await discover_new_tokens(client, api_key)
        new_contracts_discovered = len(new_contracts)

        # Step 1c: Tiered scan — batch-scan only new contracts against the 200 sampled wallets.
        # Uses batch_scan_all_holdings() with contract_override so the batching logic
        # is not duplicated. sii_scores={} because all override contracts are unscored.
        if new_contracts and sampled_wallets:
            logger.info(
                f"Discovery Phase 2: tiered scan of {new_contracts_discovered} new contracts "
                f"× {len(sampled_wallets)} sampled wallets"
            )
            tiered_holdings, tiered_failures, tiered_scan_api_calls, _ = (
                await batch_scan_all_holdings(
                    client, sampled_wallets, api_key, {},
                    contract_override=new_contracts,
                )
            )

            # Store tiered holdings with ORIGINAL wallet casing (FK requires match
            # against wallets.address which may be checksum/mixed-case from Etherscan).
            # batch_scan_all_holdings keys by lowercased address; map back to original here.
            lower_to_original = {w.lower(): w for w in sampled_wallets}
            tiered_stored = 0
            for addr_lower, h_list in tiered_holdings.items():
                if h_list:
                    original_addr = lower_to_original.get(addr_lower, addr_lower)
                    _store_holdings(original_addr, h_list)
                    tiered_stored += len(h_list)

            logger.info(
                f"Discovery Phase 2 complete: {tiered_stored} holdings stored, "
                f"{tiered_failures} batch failures, {tiered_scan_api_calls} API calls"
            )

            # Immediate demand signal update so backlog has real dollar values
            update_demand_signals()

        # Phase A: Batch-fetch all holdings (per-address, filtered to known contracts)
        logger.info("Phase A: Batch balance scan (addresstokenbalance, per-address)")
        all_holdings, scan_batch_failures, _, failed_addrs = await batch_scan_all_holdings(client, wallet_list, api_key, sii_scores)

        # Phase B: Upsert all wallets, then score + store those with holdings
        logger.info(f"Phase B: Score and store — {len(all_holdings)} wallets with holdings, {len(failed_addrs)} deferred for retry")
        results = []
        indexed = 0
        errors = 0

        for addr in wallet_list:
            try:
                addr_lower = addr.lower()
                source = "known_holder" if addr in known_addrs else "top_holder"
                _store_wallet(addr, source=source)

                # Skip wallets where the API call failed —
                # don't update last_indexed_at so they're retried next cycle
                if addr_lower in failed_addrs:
                    logger.info(f"Skipping {addr[:10]}… — API failed, will retry next cycle")
                    results.append({"address": addr, "holdings": 0, "scored": False, "reason": "api_failed"})
                    errors += 1
                    continue

                holdings = all_holdings.get(addr_lower, [])

                if not holdings:
                    _update_wallet_summary(addr, 0, "retail")
                    results.append({"address": addr, "holdings": 0, "scored": False, "reason": "no_holdings"})
                    indexed += 1
                    if indexed % 500 == 0:
                        logger.info(
                            f"  Checkpoint: {indexed}/{len(wallet_list)} wallets processed "
                            f"({skipped} skipped as already indexed today)"
                        )
                    continue

                risk = compute_wallet_risk(holdings)
                if not risk:
                    logger.warning(f"Risk compute returned None for {addr} ({len(holdings)} holdings)")
                    _update_wallet_summary(addr, 0, "retail")
                    results.append({"address": addr, "holdings": len(holdings), "scored": False, "reason": "risk_compute_failed"})
                    indexed += 1
                    if indexed % 500 == 0:
                        logger.info(
                            f"  Checkpoint: {indexed}/{len(wallet_list)} wallets processed "
                            f"({skipped} skipped as already indexed today)"
                        )
                    continue

                _store_holdings(addr, holdings)
                _store_risk_score(addr, risk)
                _update_wallet_summary(addr, risk["total_stablecoin_value"], risk["size_tier"])
                _track_unscored_holdings(holdings)

                results.append({
                    "address": addr,
                    "holdings": len(holdings),
                    "scored": True,
                    "risk_score": risk.get("risk_score"),
                    "risk_grade": risk.get("risk_grade"),
                    "total_value": risk.get("total_stablecoin_value"),
                    "size_tier": risk.get("size_tier"),
                })
                indexed += 1
                if indexed % 500 == 0:
                    logger.info(
                        f"  Checkpoint: {indexed}/{len(wallet_list)} wallets processed "
                        f"({skipped} skipped as already indexed today)"
                    )

            except Exception as e:
                logger.warning(f"Error processing {addr}: {type(e).__name__}: {e}")
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
        "wallets_skipped_today": skipped,
        "wallets_scored": scored_count,
        "wallets_no_holdings": no_holdings_count,
        "wallets_scoring_failed": scoring_failed_count,
        "scan_batch_failures": scan_batch_failures,
        "errors": errors,
        "unscored_assets_tracked": backlog_count,
        "assets_promoted_to_scoring": promoted_count,
        "sii_scores_loaded": len(sii_scores),
        "new_contracts_discovered": new_contracts_discovered,
        "tiered_scan_api_calls": tiered_scan_api_calls,
        "elapsed_seconds": round(elapsed, 1),
        "started_at": started_at.isoformat(),
        "coverage": coverage,
    }

    logger.info(
        f"=== Pipeline Complete: {indexed} wallets indexed, {skipped} skipped today, "
        f"{scored_count} scored, {no_holdings_count} no holdings, "
        f"{scoring_failed_count} scoring failed, "
        f"{scan_batch_failures} scan batch failures, {errors} errors, "
        f"{backlog_count} unscored assets tracked, "
        f"{promoted_count} promoted to scoring, "
        f"{new_contracts_discovered} new contracts discovered "
        f"({tiered_scan_api_calls} tiered scan calls), "
        f"{elapsed:.0f}s elapsed ==="
    )
    logger.info(
        f"PIPELINE_COMPLETE status=success wallets={indexed} skipped={skipped} "
        f"scored={scored_count} duration={elapsed:.0f}s"
    )

    return summary
