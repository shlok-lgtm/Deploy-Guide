"""
Wallet Indexer — API Routes
============================
FastAPI routes for /api/wallets/* and /api/backlog/*.
Registered on the app at startup, like governance routes.
"""

import hmac
import os
import asyncio
import logging

from typing import Optional

from fastapi import FastAPI, Query, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse

from app.database import execute, fetch_all, fetch_one
from app.indexer.backlog import get_backlog, get_backlog_detail
from app.indexer.pipeline import get_coverage_diagnostic
from app.specs.methodology_versions import WALLET_METHODOLOGY_VERSIONS

logger = logging.getLogger(__name__)


def register_wallet_routes(app: FastAPI) -> None:
    """Register all wallet indexer API routes on the FastAPI app."""

    @app.get("/api/wallets/top")
    async def wallets_top(
        size_tier: str = Query(None, description="Filter by size_tier: whale, institutional, retail"),
        limit: int = Query(50, ge=1, le=500),
    ):
        """Top wallets by current holdings value (computed from wallet_holdings)."""
        rows = fetch_all(
            """
            SELECT
                wh.wallet_address AS address,
                SUM(wh.value_usd) AS total_stablecoin_value,
                w.label, w.is_contract, w.last_indexed_at, w.source,
                wrs.risk_score, wrs.risk_grade, wrs.concentration_hhi,
                wrs.coverage_quality, wrs.num_total_holdings, wrs.dominant_asset
            FROM wallet_graph.wallet_holdings wh
            JOIN wallet_graph.wallets w ON w.address = wh.wallet_address
            LEFT JOIN LATERAL (
                SELECT * FROM wallet_graph.wallet_risk_scores
                WHERE wallet_address = wh.wallet_address
                ORDER BY computed_at DESC LIMIT 1
            ) wrs ON TRUE
            WHERE wh.indexed_at > NOW() - INTERVAL '7 days'
              AND wh.value_usd >= 0.01
            GROUP BY wh.wallet_address, w.label, w.is_contract, w.last_indexed_at,
                     w.source, wrs.risk_score, wrs.risk_grade, wrs.concentration_hhi,
                     wrs.coverage_quality, wrs.num_total_holdings, wrs.dominant_asset
            HAVING SUM(wh.value_usd) > 0
            ORDER BY SUM(wh.value_usd) DESC
            LIMIT %s
            """,
            (limit,),
        )
        # Compute size_tier from current holdings value
        for row in rows:
            val = float(row.get("total_stablecoin_value") or 0)
            if val >= 10_000_000:
                row["size_tier"] = "whale"
            elif val >= 100_000:
                row["size_tier"] = "institutional"
            else:
                row["size_tier"] = "retail"
        # Apply size_tier filter in Python (computed from current holdings, not stale cache)
        if size_tier:
            rows = [r for r in rows if r.get("size_tier") == size_tier]
        return {"wallets": rows, "count": len(rows)}

    @app.get("/api/wallets/riskiest")
    async def wallets_riskiest(limit: int = Query(50, ge=1, le=500)):
        """Wallets with lowest risk scores that currently hold stablecoins."""
        rows = fetch_all(
            """
            SELECT w.address,
                   (SELECT COALESCE(SUM(value_usd), 0) FROM wallet_graph.wallet_holdings
                    WHERE wallet_address = w.address
                    AND indexed_at > NOW() - INTERVAL '7 days'
                    AND value_usd >= 0.01) AS total_stablecoin_value,
                   w.label,
                   wrs.risk_score, wrs.risk_grade, wrs.concentration_hhi,
                   wrs.unscored_pct, wrs.coverage_quality,
                   wrs.dominant_asset, wrs.dominant_asset_pct,
                   wrs.num_total_holdings, wrs.computed_at
            FROM wallet_graph.wallet_risk_scores wrs
            JOIN wallet_graph.wallets w ON w.address = wrs.wallet_address
            WHERE wrs.risk_score IS NOT NULL
              AND wrs.computed_at = (
                  SELECT MAX(wrs2.computed_at) FROM wallet_graph.wallet_risk_scores wrs2
                  WHERE wrs2.wallet_address = wrs.wallet_address
              )
              AND EXISTS (
                  SELECT 1 FROM wallet_graph.wallet_holdings wh
                  WHERE wh.wallet_address = w.address
                  AND wh.indexed_at > NOW() - INTERVAL '7 days'
                  AND wh.value_usd >= 0.01
              )
            ORDER BY wrs.risk_score ASC
            LIMIT %s
            """,
            (limit,),
        )
        # Compute size_tier from current holdings value
        for row in rows:
            val = float(row.get("total_stablecoin_value") or 0)
            if val >= 10_000_000:
                row["size_tier"] = "whale"
            elif val >= 100_000:
                row["size_tier"] = "institutional"
            else:
                row["size_tier"] = "retail"
        return {"wallets": rows, "count": len(rows)}

    @app.get("/api/wallets/debug")
    async def wallets_debug():
        """Debug: check wallet_graph schema visibility from this server instance."""
        results = {}
        try:
            results["db_info"] = fetch_one("SELECT current_database() AS db, current_user AS usr")
        except Exception as e:
            results["db_info_error"] = str(e)
        try:
            results["schema_exists"] = fetch_one(
                "SELECT COUNT(*) AS table_count FROM information_schema.tables WHERE table_schema = 'wallet_graph'"
            )
        except Exception as e:
            results["schema_error"] = str(e)
        try:
            results["wallet_count"] = fetch_one("SELECT COUNT(*) AS c FROM wallet_graph.wallets")
        except Exception as e:
            results["wallet_count_error"] = str(e)
        try:
            results["migration"] = fetch_one("SELECT name, applied_at FROM migrations WHERE name = '007_wallet_graph'")
        except Exception as e:
            results["migration_error"] = str(e)
        return results

    @app.get("/api/wallets/stats")
    async def wallets_stats():
        """Aggregate stats for the wallet risk graph."""
        stats = fetch_one(
            """
            SELECT
                COUNT(*) AS total_wallets,
                COUNT(*) FILTER (WHERE last_indexed_at IS NOT NULL) AS indexed_wallets,
                COUNT(*) FILTER (WHERE size_tier = 'whale') AS whale_count,
                COUNT(*) FILTER (WHERE size_tier = 'institutional') AS institutional_count,
                COUNT(*) FILTER (WHERE size_tier = 'retail') AS retail_count
            FROM wallet_graph.wallets
            """
        )
        # Compute total value from current holdings, not stale wallets cache
        holdings_stats = fetch_one(
            """
            SELECT
                COALESCE(SUM(value_usd), 0) AS total_value_tracked,
                COUNT(DISTINCT wallet_address) AS wallets_with_current_holdings
            FROM wallet_graph.wallet_holdings
            WHERE indexed_at > NOW() - INTERVAL '7 days'
              AND value_usd >= 0.01
            """
        )
        score_stats = fetch_one(
            """
            SELECT
                COUNT(DISTINCT wrs.wallet_address) AS wallets_scored,
                AVG(wrs.risk_score) AS avg_risk_score,
                MIN(wrs.risk_score) AS min_risk_score,
                MAX(wrs.risk_score) AS max_risk_score
            FROM wallet_graph.wallet_risk_scores wrs
            WHERE wrs.computed_at = (
                SELECT MAX(wrs2.computed_at) FROM wallet_graph.wallet_risk_scores wrs2
                WHERE wrs2.wallet_address = wrs.wallet_address
            )
            AND wrs.risk_score IS NOT NULL
            """
        )
        backlog_stats = fetch_one(
            """
            SELECT
                COUNT(*) AS unscored_assets,
                COALESCE(SUM(total_value_held), 0) AS unscored_total_value
            FROM wallet_graph.unscored_assets
            """
        )

        return {
            **(stats or {}),
            **(holdings_stats or {}),
            **(score_stats or {}),
            **(backlog_stats or {}),
        }

    @app.get("/api/wallets/coverage")
    async def wallets_coverage():
        """
        Coverage diagnostic: breakdown of wallets by holdings vs scoring status.
        Identifies silent failure paths (wallets with holdings but no risk score).
        """
        return get_coverage_diagnostic()

    @app.get("/api/wallets/{address}")
    async def wallet_profile(address: str, methodology_version: Optional[str] = Query(default=None)):
        """Full wallet profile: risk score, holdings, concentration, coverage."""
        from app.server import check_methodology_version
        current_wallet_version = WALLET_METHODOLOGY_VERSIONS["current"]
        pinned = check_methodology_version(methodology_version, current_version=current_wallet_version)
        addr = address.strip()

        # Try unified cross-chain profile first
        profile = fetch_one(
            "SELECT * FROM wallet_graph.wallet_profiles WHERE address = %s",
            (addr.lower(),),
        )

        # Fall back to single-chain lookup
        wallet = fetch_one(
            """
            SELECT address, chain, first_seen_at, last_indexed_at, total_stablecoin_value,
                   size_tier, source, is_contract, label
            FROM wallet_graph.wallets
            WHERE address = %s
            ORDER BY total_stablecoin_value DESC NULLS LAST
            LIMIT 1
            """,
            (addr,),
        )
        if not wallet and not profile:
            raise HTTPException(status_code=404, detail="Wallet not found in index")

        # Latest risk score (best chain)
        risk = fetch_one(
            """
            SELECT risk_score, risk_grade, concentration_hhi, concentration_grade,
                   unscored_pct, coverage_quality,
                   num_scored_holdings, num_unscored_holdings, num_total_holdings,
                   dominant_asset, dominant_asset_pct,
                   total_stablecoin_value, size_tier, formula_version, computed_at
            FROM wallet_graph.wallet_risk_scores
            WHERE wallet_address = %s
            ORDER BY computed_at DESC
            LIMIT 1
            """,
            (addr,),
        )

        # Latest holdings (all chains), filter dust for display
        holdings_raw = fetch_all(
            """
            SELECT token_address, symbol, chain, balance, value_usd,
                   is_scored, sii_score, sii_grade, pct_of_wallet, indexed_at
            FROM wallet_graph.wallet_holdings
            WHERE wallet_address = %s
              AND indexed_at > NOW() - INTERVAL '7 days'
            ORDER BY value_usd DESC
            """,
            (addr,),
        )
        MIN_DISPLAY_VALUE_USD = 0.01
        holdings = [h for h in holdings_raw if float(h.get("value_usd") or 0) >= MIN_DISPLAY_VALUE_USD]

        result = {
            "wallet": wallet,
            "risk": risk,
            "holdings": holdings,
            "methodology_version": current_wallet_version,
            "methodology_version_pinned": pinned,
        }

        # Add cross-chain profile data if available
        if profile:
            result["cross_chain"] = {
                "chains_active": profile.get("chains_active", []),
                "total_value_all_chains": float(profile["total_value_all_chains"]) if profile.get("total_value_all_chains") else 0,
                "holdings_by_chain": profile.get("holdings_by_chain", {}),
                "edge_count_all_chains": profile.get("edge_count_all_chains", 0),
                "risk_grade_aggregate": profile.get("risk_grade_aggregate"),
            }

        return result

    @app.get("/api/wallets/{address}/history")
    async def wallet_history(
        address: str,
        limit: int = Query(30, ge=1, le=365),
    ):
        """Daily risk score history for a wallet."""
        rows = fetch_all(
            """
            SELECT risk_score, risk_grade, concentration_hhi, unscored_pct,
                   coverage_quality, total_stablecoin_value, size_tier, computed_at
            FROM wallet_graph.wallet_risk_scores
            WHERE wallet_address = %s
            ORDER BY computed_at DESC
            LIMIT %s
            """,
            (address.strip(), limit),
        )
        return {"address": address.strip(), "history": rows}

    # -- Wallet edge/connection routes --

    @app.get("/api/wallets/{address}/connections")
    async def wallet_connections(
        address: str,
        limit: int = Query(default=20, ge=1, le=100),
        chain: Optional[str] = Query(default=None),
    ):
        """Top counterparties for a wallet sorted by edge weight. Optional ?chain= filter."""
        addr = address.lower()
        if chain:
            edges = fetch_all(
                """
                SELECT
                    CASE WHEN from_address = %s THEN to_address ELSE from_address END AS counterparty,
                    chain, transfer_count, total_value_usd, first_transfer_at, last_transfer_at,
                    weight, tokens_transferred
                FROM wallet_graph.wallet_edges
                WHERE (from_address = %s OR to_address = %s) AND chain = %s
                ORDER BY weight DESC LIMIT %s
                """,
                (addr, addr, addr, chain, limit),
            )
        else:
            edges = fetch_all(
                """
                SELECT
                    CASE WHEN from_address = %s THEN to_address ELSE from_address END AS counterparty,
                    chain, transfer_count, total_value_usd, first_transfer_at, last_transfer_at,
                    weight, tokens_transferred
                FROM wallet_graph.wallet_edges
                WHERE from_address = %s OR to_address = %s
                ORDER BY weight DESC LIMIT %s
                """,
                (addr, addr, addr, limit),
            )

        connections = []
        for edge in edges:
            cp_info = fetch_one(
                "SELECT total_stablecoin_value, size_tier, label, is_contract FROM wallet_graph.wallets WHERE address = %s",
                (edge["counterparty"],),
            )
            connections.append({
                "counterparty": edge["counterparty"],
                "chain": edge.get("chain", "ethereum"),
                "transfer_count": edge["transfer_count"],
                "total_value_usd": edge["total_value_usd"],
                "first_transfer": edge["first_transfer_at"].isoformat() if edge.get("first_transfer_at") else None,
                "last_transfer": edge["last_transfer_at"].isoformat() if edge.get("last_transfer_at") else None,
                "weight": round(float(edge["weight"]), 4) if edge.get("weight") else 0,
                "tokens": edge["tokens_transferred"],
                "counterparty_value": cp_info["total_stablecoin_value"] if cp_info else None,
                "counterparty_label": cp_info.get("label") if cp_info else None,
                "counterparty_tier": cp_info.get("size_tier") if cp_info else None,
            })

        build = fetch_one(
            "SELECT status, last_built_at FROM wallet_graph.edge_build_status WHERE wallet_address = %s",
            (addr,),
        )

        return {
            "wallet": addr,
            "connections": connections,
            "count": len(connections),
            "edge_status": build["status"] if build else "not_built",
            "edges_built_at": build["last_built_at"].isoformat() if build and build.get("last_built_at") else None,
        }

    @app.get("/api/wallets/{address}/contagion")
    async def wallet_contagion(
        address: str,
        depth: int = Query(default=2, ge=1, le=3),
        chain: Optional[str] = Query(default=None),
    ):
        """
        Recursive contagion traversal: if this wallet's holdings depeg, who's exposed?
        Uses recursive CTE to follow edges up to `depth` hops (max 3).
        Optional ?chain= filter (ethereum, base, arbitrum). Default: all chains.
        """
        if depth > 3:
            raise HTTPException(status_code=400, detail="Maximum depth is 3")

        addr = address.lower()
        MAX_NODES = 500

        # Build chain filter clause
        if chain:
            chain_clause = "AND chain = %s"
            base_params = (addr, addr, addr, addr, addr, chain, depth, chain, MAX_NODES + 1)
        else:
            chain_clause = ""
            base_params = (addr, addr, addr, addr, addr, depth, MAX_NODES + 1)

        query = f"""
            WITH RECURSIVE contagion_path AS (
                SELECT
                    CASE WHEN from_address = %s THEN to_address ELSE from_address END AS node,
                    weight,
                    total_value_usd,
                    1 AS depth,
                    ARRAY[%s,
                        CASE WHEN from_address = %s THEN to_address ELSE from_address END
                    ] AS path
                FROM wallet_graph.wallet_edges
                WHERE (from_address = %s OR to_address = %s)
                  {chain_clause}
                  AND weight > 0.05

                UNION ALL

                SELECT
                    CASE WHEN e.from_address = cp.node THEN e.to_address ELSE e.from_address END,
                    e.weight,
                    e.total_value_usd,
                    cp.depth + 1,
                    cp.path || CASE WHEN e.from_address = cp.node THEN e.to_address ELSE e.from_address END
                FROM wallet_graph.wallet_edges e
                JOIN contagion_path cp ON (e.from_address = cp.node OR e.to_address = cp.node)
                WHERE cp.depth < %s
                  {chain_clause}
                  AND NOT (CASE WHEN e.from_address = cp.node THEN e.to_address ELSE e.from_address END) = ANY(cp.path)
                  AND e.weight > 0.05
            )
            SELECT DISTINCT ON (node)
                node AS address,
                depth,
                weight AS edge_weight,
                total_value_usd AS exposure_usd,
                path
            FROM contagion_path
            ORDER BY node, depth ASC, weight DESC
            LIMIT %s
        """

        rows = fetch_all(query, base_params)

        truncated = len(rows) > MAX_NODES
        if truncated:
            rows = rows[:MAX_NODES]

        # Batch-fetch risk grades for all discovered nodes
        node_addrs = [r["address"] for r in rows]
        risk_map = {}
        if node_addrs:
            risk_rows = fetch_all(
                """
                SELECT DISTINCT ON (wallet_address)
                    wallet_address, risk_score, risk_grade
                FROM wallet_graph.wallet_risk_scores
                WHERE wallet_address = ANY(%s)
                ORDER BY wallet_address, computed_at DESC
                """,
                (node_addrs,),
            )
            risk_map = {r["wallet_address"]: r for r in risk_rows}

        nodes = []
        total_exposed = 0.0
        for r in rows:
            risk = risk_map.get(r["address"])
            exposure = float(r["exposure_usd"]) if r["exposure_usd"] else 0
            total_exposed += exposure
            nodes.append({
                "address": r["address"],
                "depth": r["depth"],
                "edge_weight": round(float(r["edge_weight"]), 4) if r["edge_weight"] else 0,
                "exposure_usd": exposure,
                "risk_grade": risk["risk_grade"] if risk else None,
                "path": r["path"],
            })

        # Sort by depth then weight descending
        nodes.sort(key=lambda n: (n["depth"], -n["edge_weight"]))

        result = {
            "source": addr,
            "depth": depth,
            "nodes": nodes,
            "total_exposed_usd": round(total_exposed, 2),
            "node_count": len(nodes),
        }
        if truncated:
            result["truncated"] = True

        return result

    # -- Wallet profile routes --

    @app.get("/api/wallets/{address}/profile")
    async def wallet_profile_full(address: str):
        """Full wallet risk profile — reputation primitive with behavioral signals."""
        from app.wallet_profile import generate_wallet_profile
        profile = generate_wallet_profile(address)
        if not profile:
            raise HTTPException(status_code=404, detail="Wallet not found in index")

        addr = address.lower()
        top_connections = fetch_all(
            """
            SELECT
                CASE WHEN from_address = %s THEN to_address ELSE from_address END AS counterparty,
                weight, total_value_usd
            FROM wallet_graph.wallet_edges
            WHERE from_address = %s OR to_address = %s
            ORDER BY weight DESC
            LIMIT 5
            """,
            (addr, addr, addr),
        )
        edge_count = fetch_one(
            "SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges WHERE from_address = %s OR to_address = %s",
            (addr, addr),
        )
        profile["connections_summary"] = {
            "total_connections": edge_count["cnt"] if edge_count else 0,
            "top_counterparties": [
                {"address": c["counterparty"], "weight": round(float(c["weight"]), 4) if c.get("weight") else 0, "value": c["total_value_usd"]}
                for c in top_connections
            ],
        }

        return profile

    @app.get("/api/wallets/{address}/profile/hash")
    async def wallet_profile_hash(address: str):
        """Just the profile hash and timestamp — lightweight verification."""
        from app.wallet_profile import generate_wallet_profile
        profile = generate_wallet_profile(address)
        if not profile:
            raise HTTPException(status_code=404, detail="Wallet not found in index")
        return {
            "address": profile["address"],
            "profile_hash": profile["profile_hash"],
            "computed_at": profile["computed_at"],
        }

    # -- Backlog routes --

    @app.get("/api/backlog")
    async def backlog_list(limit: int = Query(50, ge=1, le=500)):
        """Unscored asset backlog, sorted by priority (total_value_held DESC)."""
        rows = get_backlog(limit)
        return {"backlog": rows, "count": len(rows)}

    @app.get("/api/backlog/{token_address}")
    async def backlog_detail(token_address: str):
        """Detail for one unscored asset: which wallets hold it, how much."""
        detail = get_backlog_detail(token_address)
        if not detail:
            raise HTTPException(status_code=404, detail="Asset not found in backlog")
        return detail

    # -- Admin trigger --

    @app.post("/api/admin/index-wallets")
    async def admin_index_wallets(
        background_tasks: BackgroundTasks,
        key: str = Query(..., description="Admin key"),
        holders_per_coin: int = Query(5000, ge=10, le=10000),
        reseed: bool = Query(False, description="Force full re-seed of top holders (overrides resume logic)"),
    ):
        """Manually trigger a wallet indexing run (admin-only). Returns immediately; runs in background."""
        admin_key = os.environ.get("ADMIN_KEY", "")
        if not admin_key or not key or not hmac.compare_digest(key, admin_key):
            raise HTTPException(status_code=403, detail="Invalid admin key")

        from app.indexer.pipeline import run_pipeline

        async def _run():
            try:
                await run_pipeline(holders_per_coin=holders_per_coin, force_reseed=reseed)
            except Exception as e:
                logger.error(f"Background wallet indexing failed: {e}")
                logger.error(f"PIPELINE_COMPLETE status=error reason={type(e).__name__}: {e}")

        background_tasks.add_task(_run)
        return {
            "status": "started",
            "holders_per_coin": holders_per_coin,
            "reseed": reseed,
            "message": "Wallet indexing running in background — check /api/wallets/stats for progress",
        }

    @app.get("/api/graph/stats")
    async def graph_stats():
        """Edge graph statistics, build progress, and coverage metrics."""
        edge_stats = fetch_one(
            """
            SELECT
                COUNT(*) AS total_edges,
                COALESCE(SUM(total_value_usd), 0) AS total_value,
                COALESCE(AVG(weight), 0) AS avg_weight,
                MIN(last_transfer_at) AS oldest_edge,
                MAX(last_transfer_at) AS newest_edge
            FROM wallet_graph.wallet_edges
            """
        )
        build_stats = fetch_one(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'complete') AS built,
                COUNT(*) FILTER (WHERE status = 'pending' OR status IS NULL) AS pending
            FROM wallet_graph.edge_build_status
            """
        )
        wallets_total_row = fetch_one("SELECT COUNT(*) AS cnt FROM wallet_graph.wallets")
        wallets_total = wallets_total_row["cnt"] if wallets_total_row else 0

        # Per-chain breakdown
        chain_rows = fetch_all(
            """
            SELECT chain, COUNT(*) AS cnt, COALESCE(SUM(total_value_usd), 0) AS value
            FROM wallet_graph.wallet_edges GROUP BY chain ORDER BY cnt DESC
            """
        )
        by_chain = {r["chain"]: {"edges": r["cnt"], "value": float(r["value"])} for r in chain_rows}

        # Coverage metrics
        wallets_with_edges_row = fetch_one(
            """
            SELECT COUNT(DISTINCT addr) AS cnt FROM (
                SELECT from_address AS addr FROM wallet_graph.wallet_edges
                UNION SELECT to_address FROM wallet_graph.wallet_edges
            ) sub
            """
        )
        wallets_with_edges = wallets_with_edges_row["cnt"] if wallets_with_edges_row else 0
        total_edges = edge_stats["total_edges"] if edge_stats else 0
        coverage_pct = round(wallets_with_edges / wallets_total * 100, 2) if wallets_total > 0 else 0
        avg_connections = round(total_edges / wallets_with_edges, 2) if wallets_with_edges > 0 else 0

        # Recent activity
        recent_row = fetch_one(
            "SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges WHERE last_transfer_at > NOW() - INTERVAL '24 hours'"
        )
        edges_last_24h = recent_row["cnt"] if recent_row else 0

        # Archive count
        try:
            archive_row = fetch_one("SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_edges_archive")
            archived = archive_row["cnt"] if archive_row else 0
        except Exception:
            archived = 0

        # Profile stats
        try:
            profile_row = fetch_one("SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_profiles")
            profiles_total = profile_row["cnt"] if profile_row else 0
            multi_row = fetch_one(
                "SELECT COUNT(*) AS cnt FROM wallet_graph.wallet_profiles WHERE jsonb_array_length(chains_active) > 1"
            )
            multi_chain = multi_row["cnt"] if multi_row else 0
        except Exception:
            profiles_total = 0
            multi_chain = 0

        return {
            "edges": {
                "total": total_edges,
                "total_value_transferred": float(edge_stats["total_value"]) if edge_stats else 0,
                "avg_weight": round(float(edge_stats["avg_weight"]), 4) if edge_stats else 0,
                "oldest_edge": edge_stats["oldest_edge"].isoformat() if edge_stats and edge_stats.get("oldest_edge") else None,
                "newest_edge": edge_stats["newest_edge"].isoformat() if edge_stats and edge_stats.get("newest_edge") else None,
                "edges_last_24h": edges_last_24h,
                "by_chain": by_chain,
                "archived_edges": archived,
            },
            "coverage": {
                "total_wallets": wallets_total,
                "wallets_with_edges": wallets_with_edges,
                "coverage_pct": coverage_pct,
                "avg_connections_per_wallet": avg_connections,
            },
            "profiles": {
                "unified_profiles": profiles_total,
                "multi_chain_wallets": multi_chain,
            },
            "build_progress": {
                "wallets_built": build_stats["built"] if build_stats else 0,
                "wallets_pending": build_stats["pending"] if build_stats else 0,
                "wallets_total": wallets_total,
            },
        }

    @app.post("/api/admin/build-edges")
    async def trigger_edge_build(
        request: Request,
        key: str = Query(default=None),
        max_wallets: int = Query(default=50, ge=1, le=500),
        priority: str = Query(default="value"),
        chain: str = Query(default="ethereum"),
    ):
        """Trigger edge building for wallets on a specific chain (admin-only)."""
        admin_key = os.environ.get("ADMIN_KEY", "")
        provided = key or request.headers.get("x-admin-key", "")
        if not admin_key or not provided or not hmac.compare_digest(provided, admin_key):
            raise HTTPException(status_code=403, detail="Invalid admin key")
        from app.indexer.edges import run_edge_builder
        from app.indexer.config import SUPPORTED_CHAINS
        if chain != "all" and chain not in SUPPORTED_CHAINS:
            raise HTTPException(status_code=400, detail=f"Unsupported chain. Use: {SUPPORTED_CHAINS} or 'all'")
        if chain == "all":
            results = {}
            for c in SUPPORTED_CHAINS:
                results[c] = await run_edge_builder(max_wallets=max_wallets, priority=priority, chain=c)
            return {"chains": results}
        result = await run_edge_builder(max_wallets=max_wallets, priority=priority, chain=chain)
        return result

    @app.post("/api/admin/solana-discover-drift")
    async def admin_discover_drift_wallets(
        request: Request,
        key: str = Query(default=None),
    ):
        """Discover Drift depositors and build their Solana wallet graph edges."""
        admin_key = os.environ.get("ADMIN_KEY", "")
        provided = key or request.headers.get("x-admin-key", "")
        if not admin_key or not provided or not hmac.compare_digest(provided, admin_key):
            raise HTTPException(status_code=403, detail="Invalid admin key")

        try:
            from app.indexer.solana_edges import discover_drift_depositors, run_solana_edge_builder
            import httpx as _httpx

            async with _httpx.AsyncClient() as client:
                depositors = await discover_drift_depositors(client)

            if not depositors:
                return {"status": "no depositors found", "wallets": 0}

            # Register wallets
            for addr in depositors:
                execute(
                    """
                    INSERT INTO wallet_graph.wallets (address, chain, source, label, created_at, updated_at)
                    VALUES (%s, 'solana', 'drift-discovery', 'drift-depositor', NOW(), NOW())
                    ON CONFLICT (address, chain) DO NOTHING
                    """,
                    (addr,),
                )

            # Build edges
            result = await run_solana_edge_builder(depositors, max_pages_per_wallet=3)

            return {
                "depositors_discovered": len(depositors),
                "edge_build_result": result,
            }
        except Exception as _e:
            import traceback
            return JSONResponse(
                status_code=500,
                content={
                    "error": str(_e),
                    "type": type(_e).__name__,
                    "traceback": traceback.format_exc(),
                },
            )

    @app.post("/api/admin/merge-wallets")
    async def admin_merge_wallets(
        request: Request,
    ):
        """Manually link wallets across chains as belonging to the same entity."""
        admin_key = os.environ.get("ADMIN_KEY", "")
        provided = request.query_params.get("key", "") or request.headers.get("x-admin-key", "")
        if not admin_key or not provided or not hmac.compare_digest(provided, admin_key):
            raise HTTPException(status_code=403, detail="Invalid admin key")

        body = await request.json()
        addresses = body.get("addresses", [])  # list of {address, chain}
        entity_label = body.get("label", "")

        if len(addresses) < 2:
            raise HTTPException(status_code=400, detail="Provide at least 2 addresses to merge")

        import uuid as _uuid
        entity_id = body.get("entity_id", str(_uuid.uuid4()))

        merged = 0
        for addr_info in addresses:
            execute(
                """
                UPDATE wallet_graph.wallets
                SET label = %s, updated_at = NOW()
                WHERE address = %s AND chain = %s
                """,
                (f"entity:{entity_id}:{entity_label}", addr_info["address"], addr_info["chain"]),
            )
            merged += 1

        return {
            "entity_id": entity_id,
            "label": entity_label,
            "wallets_merged": merged,
        }
