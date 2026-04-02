"""
Daily Cycle Orchestrator
========================
Runs the full daily scoring + indexing cycle with budget coordination.

Priority order:
  1. SII scoring cycle (non-negotiable)
  2. PSI scoring cycle (second priority)
  3. Wallet refresh (re-index existing wallets)
  4. Wallet expansion (seed new wallets with remaining budget)

Each phase checks and updates the shared budget in ops.api_budget.
"""

import logging
from app.budget.manager import ApiBudgetManager

logger = logging.getLogger(__name__)


async def run_daily_cycle():
    """
    Master daily cycle. Runs in priority order.
    Each process checks and updates the shared budget.
    """
    logger.info("=== Daily cycle starting ===")

    budget = ApiBudgetManager()
    row = budget.get_or_create_today()
    logger.info(f"Daily Etherscan budget: {row['daily_limit']} calls")

    # 1. SII scoring (non-negotiable, highest priority)
    logger.info("--- Phase 1: SII scoring ---")
    await _run_sii_phase(budget)

    # 2. PSI scoring
    logger.info("--- Phase 2: PSI scoring ---")
    await _run_psi_phase(budget)

    # 3. Wallet refresh + expansion
    logger.info("--- Phase 3: Wallet indexer ---")
    await _run_wallet_phase(budget)

    # 4. Log summary
    final = budget.get_or_create_today()
    total_used = (
        final["sii_calls_used"]
        + final["psi_calls_used"]
        + final["wallet_refresh_calls_used"]
        + final["wallet_expansion_calls_used"]
    )
    logger.info(
        f"=== Daily cycle complete === "
        f"SII: {final['sii_calls_used']}, "
        f"PSI: {final['psi_calls_used']}, "
        f"Refresh: {final['wallet_refresh_calls_used']}, "
        f"Expansion: {final['wallet_expansion_calls_used']}, "
        f"Total: {total_used}/{final['daily_limit']}, "
        f"Unused: {final['daily_limit'] - total_used}"
    )


async def _run_sii_phase(budget: ApiBudgetManager):
    """Run SII scoring cycle with budget tracking."""
    budget.mark_started("sii")

    available = budget.available_for("sii")
    if available < 5000:
        logger.error(f"Insufficient Etherscan budget for SII: {available} available")
        budget.mark_completed("sii")
        return

    try:
        # Import here to avoid circular imports
        from app.worker import run_scoring_cycle
        await run_scoring_cycle()
        # Worker doesn't yet return call counts — estimate from typical usage
        # TODO: Wire actual Etherscan call counting into worker/collectors
        logger.info("SII scoring cycle completed")
    except Exception as e:
        logger.error(f"SII scoring cycle failed: {e}")

    budget.mark_completed("sii")


async def _run_psi_phase(budget: ApiBudgetManager):
    """Run PSI scoring cycle with budget tracking."""
    budget.mark_started("psi")

    available = budget.available_for("psi")
    if available < 1000:
        logger.warning(
            f"Low Etherscan budget for PSI: {available}. "
            "PSI will use DeFiLlama/CoinGecko only, skipping on-chain verification."
        )

    try:
        from app.collectors.psi_collector import run_psi_scoring
        logger.info("Running PSI scoring...")
        results = run_psi_scoring()
        logger.info(f"PSI scoring complete: {len(results)} protocols scored")
    except Exception as e:
        logger.error(f"PSI scoring cycle failed: {e}")

    # Collect collateral exposure after PSI scoring (one HTTP call, no API key needed)
    try:
        from app.collectors.psi_collector import collect_collateral_exposure
        logger.info("Collecting protocol collateral exposure...")
        collect_collateral_exposure()
    except Exception as e:
        logger.error(f"Collateral exposure collection failed: {e}")

    # Sync collateral exposure data to auto-promote backlog
    try:
        from app.collectors.psi_collector import sync_collateral_to_backlog
        synced = sync_collateral_to_backlog()
        logger.info(f"Synced {synced} unscored collateral stablecoins to backlog")
    except Exception as e:
        logger.error(f"Collateral-to-backlog sync failed: {e}")

    # Discover, enrich, and promote protocol candidates
    try:
        from app.collectors.psi_collector import (
            discover_protocols, enrich_protocol_backlog, promote_eligible_protocols,
        )
        discovered = discover_protocols()
        enriched = enrich_protocol_backlog()
        promoted = promote_eligible_protocols()
        logger.info(
            f"Protocol backlog: {discovered} discovered, "
            f"{enriched} enriched, {promoted} promoted"
        )
    except Exception as e:
        logger.error(f"Protocol backlog cycle failed: {e}")

    # Discover chains needing coverage
    try:
        from app.collectors.psi_collector import run_chain_discovery
        chain_result = run_chain_discovery()
        if chain_result.get("specs_generated", 0) > 0:
            logger.info(
                f"Chain discovery: {chain_result['specs_generated']} new spec(s) "
                f"generated for {chain_result['chains']}"
            )
    except Exception as e:
        logger.error(f"Chain discovery error: {e}")

    budget.mark_completed("psi")


async def _run_wallet_phase(budget: ApiBudgetManager):
    """Run wallet refresh + expansion with budget tracking."""
    # Phase 3a: Refresh existing wallets
    budget.mark_started("wallet_refresh")

    refresh_available = budget.available_for("wallet_refresh")
    if refresh_available < 1000:
        logger.info(f"Etherscan budget too low for wallet refresh: {refresh_available}")
        budget.mark_completed("wallet_refresh")
    else:
        try:
            from app.indexer.pipeline import run_pipeline

            logger.info(f"Wallet refresh: {refresh_available} Etherscan calls available")
            result = await run_pipeline()

            # Record actual API calls used (from batch scan stats)
            # Pipeline tracks tiered_scan_api_calls but not total Etherscan calls yet
            # Use a conservative estimate based on wallets indexed
            calls_estimate = result.get("tiered_scan_api_calls", 0)
            wallets = result.get("wallets_indexed", 0)
            # batch_scan: ~ceil(wallets/20) * num_contracts calls
            # Rough estimate: wallets * 1.2 calls each (batched)
            if wallets > 0 and calls_estimate == 0:
                calls_estimate = max(wallets * 2, 1000)

            budget.record_calls("wallet_refresh", calls_estimate)
            logger.info(
                f"Wallet refresh complete: {wallets} wallets indexed, "
                f"~{calls_estimate} Etherscan calls recorded"
            )
        except Exception as e:
            logger.error(f"Wallet refresh failed: {e}")

        budget.mark_completed("wallet_refresh")

    # Phase 3b: Expand coverage with remaining budget
    budget.mark_started("wallet_expansion")

    expansion_available = budget.available_for("wallet_expansion")
    if expansion_available > 5000:
        try:
            from app.indexer.expander import run_wallet_expansion

            logger.info(f"Wallet expansion: {expansion_available} Etherscan calls available")
            expansion_result = await run_wallet_expansion(
                max_etherscan_calls=expansion_available
            )
            budget.record_calls(
                "wallet_expansion",
                expansion_result["etherscan_calls_used"],
            )
            logger.info(
                f"Wallet expansion: {expansion_result['new_wallets_seeded']} new wallets, "
                f"{expansion_result['etherscan_calls_used']} calls used"
            )
        except Exception as e:
            logger.error(f"Wallet expansion failed: {e}")
    else:
        logger.info(
            f"Only {expansion_available} Etherscan calls remaining — skipping expansion"
        )

    budget.mark_completed("wallet_expansion")
