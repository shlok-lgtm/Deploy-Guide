"""
Wallet Profiles — Cross-Chain Unified View
============================================
Merges per-chain wallet data into a single profile for EOA addresses.
EOAs share the same address across EVM chains. Contracts do NOT.

The wallet_profiles table provides a merged view without replacing
per-chain data in wallets/holdings/risk_scores.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from app.database import fetch_all, fetch_one, execute

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
            None,
        ),
    )

    return {
        "address": addr,
        "is_contract": is_contract,
        "chains_active": chains_active,
        "total_value_all_chains": round(total_value, 2),
        "holdings_by_chain": holdings_by_chain,
        "edge_count_all_chains": edge_count,
    }


def rebuild_all_profiles(limit: int = 0) -> dict:
    """
    Rebuild unified profiles for distinct addresses across chains.

    Args:
        limit: Max wallets to process (0 = unlimited, stalest first).

    The picks query (v9.12 #228 fix) restricts candidates to wallets that
    have *something to profile*: fresh holdings (last 7 days). Previously
    the work-queue was poisoned by ~700K "shell" wallets seeded by the
    expander without holdings, which produced trivial all-zero profiles
    and starved the cycle of useful work. The substrate evidence: 2000
    upserts/cycle were happening but every attestation reported
    `ran_no_results` with `built=0`, because the queue head was
    dominated by shell wallets whose `build_unified_profile()` calls
    diverged silently from the in-memory `built` counter. The new
    `built_substrate` value is sourced post-hoc from the table itself
    via cycle_start_ts, anchoring the attested value to ground truth.
    """
    import time as _time
    cycle_start_ts = datetime.now(timezone.utc)
    cycle_t0 = _time.monotonic()

    if limit > 0:
        # Stalest profiles first, restricted to wallets that have fresh
        # holdings (last 7 days). Drops "shell" wallets seeded by the
        # expander that have no holdings and would otherwise dominate
        # the NULLS FIRST queue (substrate: ~700K shell wallets vs
        # ~3K wallets with fresh holdings). Holdings is the dominant
        # signal — wallets with only edges and no holdings produce
        # trivial profiles anyway. EXPLAIN ANALYZE: 464ms with the
        # holdings filter vs 783ms unfiltered but yielding shell wallets.
        rows = fetch_all(
            """SELECT w.address, MIN(p.updated_at) AS oldest_profile
               FROM wallet_graph.wallets w
               LEFT JOIN wallet_graph.wallet_profiles p ON w.address = p.address
               WHERE EXISTS (
                   SELECT 1 FROM wallet_graph.wallet_holdings h
                   WHERE LOWER(h.wallet_address) = LOWER(w.address)
                     AND h.indexed_at > NOW() - INTERVAL '7 days'
               )
               GROUP BY w.address
               ORDER BY oldest_profile ASC NULLS FIRST
               LIMIT %s""",
            (limit,),
        )
    else:
        rows = fetch_all("SELECT DISTINCT address FROM wallet_graph.wallets")
    addresses = [r["address"] for r in rows]

    logger.info(f"Rebuilding profiles for {len(addresses)} distinct addresses (limit={limit})")

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

    # Substrate-anchor the attested `built` value: source it from the
    # table itself rather than the in-memory counter. This closes the
    # #228 gap where 2000 upserts/cycle were occurring but `built` was
    # reported as 0 due to silent intermediate path divergence. The
    # in-memory `built` is still surfaced (as `built_inmem`) for
    # observability, but the attestation and return value use the
    # substrate count, which is what the wallet_profiles table actually
    # reflects.
    built_substrate = built
    try:
        sub_row = fetch_one(
            "SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_profiles WHERE updated_at >= %s",
            (cycle_start_ts,),
        )
        if sub_row and sub_row.get("cnt") is not None:
            built_substrate = int(sub_row["cnt"])
    except Exception as _sub_err:
        logger.warning(f"wallet_profiles substrate count failed; falling back to in-memory built: {_sub_err}")

    elapsed_s = round(_time.monotonic() - cycle_t0, 1)
    logger.info(
        f"Profile rebuild complete: built_substrate={built_substrate} "
        f"(built_inmem={built}, errors={errors}, picks={len(addresses)}, elapsed={elapsed_s}s)"
    )

    # Attest wallet profiles
    try:
        from app.state_attestation import attest_state
        if built_substrate > 0:
            attest_state(
                "wallet_profiles",
                [{"built": built_substrate, "built_inmem": built, "total": len(addresses)}],
                writer_id="module.wallet_profile",
            )
        else:
            attest_state("wallet_profiles", [{"status": "ran_no_results", "profiles_built": 0}], writer_id="module.wallet_profile")
    except asyncio.CancelledError:
        raise
    except Exception as ae:
        logger.error(f"wallet_profiles attestation failed: {ae}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="wallet_profiles_attestation_failure",
                error_message=str(ae)[:500],
                cycle_phase="wallet_profiles",
            )
        except Exception:
            pass

    return {
        "total": len(addresses),
        "built": built_substrate,
        "built_inmem": built,
        "errors": errors,
    }
