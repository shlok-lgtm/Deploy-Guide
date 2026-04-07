"""
Wallet Profiles — Cross-Chain Unified View
============================================
Merges per-chain wallet data into a single profile for EOA addresses.
EOAs share the same address across EVM chains. Contracts do NOT.

The wallet_profiles table provides a merged view without replacing
per-chain data in wallets/holdings/risk_scores.
"""

import json
import logging
from datetime import datetime, timezone

from app.database import fetch_all, fetch_one, execute
from app.scoring import score_to_grade

logger = logging.getLogger(__name__)


def build_unified_profile(address: str) -> dict | None:
    """
    Build a unified cross-chain profile for a single address.
    Aggregates wallets, holdings, risk scores, and edges across all chains.
    Upserts into wallet_graph.wallet_profiles.
    """
    addr = address.strip()

    # Get per-chain wallet records (case-insensitive match)
    wallet_rows = fetch_all(
        """
        SELECT address, chain, total_stablecoin_value, is_contract, label, size_tier
        FROM wallet_graph.wallets WHERE LOWER(address) = LOWER(%s)
        """,
        (addr,),
    )

    if not wallet_rows:
        return None

    chains_active = sorted(set(r["chain"] for r in wallet_rows))
    is_contract = any(r.get("is_contract") for r in wallet_rows)

    # Compute total from actual holdings, not stale wallet summary
    holdings_value_row = fetch_one(
        """
        SELECT COALESCE(SUM(value_usd), 0) AS total_value
        FROM wallet_graph.wallet_holdings
        WHERE LOWER(wallet_address) = LOWER(%s)
          AND indexed_at > NOW() - INTERVAL '7 days'
          AND value_usd >= 0.01
        """,
        (addr,),
    )
    total_value = float(holdings_value_row["total_value"]) if holdings_value_row else 0
    # Fall back to wallet summary if no recent holdings
    if total_value == 0:
        total_value = sum(float(r["total_stablecoin_value"] or 0) for r in wallet_rows)

    # Holdings by chain
    holdings_rows = fetch_all(
        """
        SELECT chain, symbol, SUM(value_usd) AS value
        FROM wallet_graph.wallet_holdings
        WHERE LOWER(wallet_address) = LOWER(%s)
          AND indexed_at > NOW() - INTERVAL '7 days'
        GROUP BY chain, symbol
        ORDER BY chain, value DESC
        """,
        (addr,),
    )

    holdings_by_chain = {}
    for r in holdings_rows:
        chain = r["chain"] or "ethereum"
        if chain not in holdings_by_chain:
            holdings_by_chain[chain] = {}
        holdings_by_chain[chain][r["symbol"]] = round(float(r["value"] or 0), 2)

    # Edge count across all chains
    edge_row = fetch_one(
        """
        SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges
        WHERE LOWER(from_address) = LOWER(%s) OR LOWER(to_address) = LOWER(%s)
        """,
        (addr, addr),
    )
    edge_count = edge_row["cnt"] if edge_row else 0

    # Risk scores per chain — value-weighted aggregate
    risk_rows = fetch_all(
        """
        SELECT DISTINCT ON (chain)
            chain, risk_score, total_stablecoin_value
        FROM wallet_graph.wallet_risk_scores
        WHERE LOWER(wallet_address) = LOWER(%s)
        ORDER BY chain, computed_at DESC
        """,
        (addr,),
    )

    weighted_score = 0.0
    weight_sum = 0.0
    for r in risk_rows:
        score = float(r["risk_score"]) if r.get("risk_score") else None
        value = float(r["total_stablecoin_value"] or 0)
        if score is not None and value > 0:
            weighted_score += score * value
            weight_sum += value

    aggregate_score = weighted_score / weight_sum if weight_sum > 0 else None
    aggregate_grade = score_to_grade(aggregate_score) if aggregate_score is not None else None

    # Upsert into wallet_profiles
    execute(
        """
        INSERT INTO wallet_graph.wallet_profiles
            (address, is_contract, chains_active, total_value_all_chains,
             holdings_by_chain, edge_count_all_chains, risk_grade_aggregate, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (address) DO UPDATE SET
            is_contract = EXCLUDED.is_contract,
            chains_active = EXCLUDED.chains_active,
            total_value_all_chains = EXCLUDED.total_value_all_chains,
            holdings_by_chain = EXCLUDED.holdings_by_chain,
            edge_count_all_chains = EXCLUDED.edge_count_all_chains,
            risk_grade_aggregate = EXCLUDED.risk_grade_aggregate,
            updated_at = NOW()
        """,
        (
            addr,
            is_contract,
            json.dumps(chains_active),
            total_value,
            json.dumps(holdings_by_chain),
            edge_count,
            aggregate_grade,
        ),
    )

    return {
        "address": addr,
        "is_contract": is_contract,
        "chains_active": chains_active,
        "total_value_all_chains": round(total_value, 2),
        "holdings_by_chain": holdings_by_chain,
        "edge_count_all_chains": edge_count,
        "risk_grade_aggregate": aggregate_grade,
    }


def rebuild_all_profiles() -> dict:
    """
    Rebuild unified profiles for all distinct addresses across chains.
    """
    rows = fetch_all("SELECT DISTINCT address FROM wallet_graph.wallets")
    addresses = [r["address"] for r in rows]

    logger.info(f"Rebuilding profiles for {len(addresses)} distinct addresses")

    built = 0
    errors = 0
    for i, addr in enumerate(addresses):
        try:
            result = build_unified_profile(addr)
            if result:
                built += 1
        except Exception as e:
            logger.warning(f"Profile build failed for {addr[:12]}…: {e}")
            errors += 1

        if (i + 1) % 100 == 0:
            logger.info(f"Profile rebuild progress: {i + 1}/{len(addresses)} ({built} built, {errors} errors)")

    logger.info(f"Profile rebuild complete: {built} built, {errors} errors out of {len(addresses)} addresses")
    return {"total": len(addresses), "built": built, "errors": errors}
