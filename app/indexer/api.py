"""
Wallet Indexer — API Routes
============================
FastAPI routes for /api/wallets/* and /api/backlog/*.
Registered on the app at startup, like governance routes.
"""

import os
import asyncio
import logging

from typing import Optional

from fastapi import FastAPI, Query, HTTPException, BackgroundTasks

from app.database import fetch_all, fetch_one
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
        """Top wallets by total stablecoin value."""
        if size_tier:
            rows = fetch_all(
                """
                SELECT w.address, w.total_stablecoin_value, w.size_tier, w.label,
                       w.is_contract, w.last_indexed_at, w.source,
                       wrs.risk_score, wrs.risk_grade, wrs.concentration_hhi,
                       wrs.coverage_quality, wrs.num_total_holdings, wrs.dominant_asset
                FROM wallet_graph.wallets w
                LEFT JOIN LATERAL (
                    SELECT * FROM wallet_graph.wallet_risk_scores
                    WHERE wallet_address = w.address
                    ORDER BY computed_at DESC LIMIT 1
                ) wrs ON TRUE
                WHERE w.size_tier = %s
                ORDER BY w.total_stablecoin_value DESC NULLS LAST
                LIMIT %s
                """,
                (size_tier, limit),
            )
        else:
            rows = fetch_all(
                """
                SELECT w.address, w.total_stablecoin_value, w.size_tier, w.label,
                       w.is_contract, w.last_indexed_at, w.source,
                       wrs.risk_score, wrs.risk_grade, wrs.concentration_hhi,
                       wrs.coverage_quality, wrs.num_total_holdings, wrs.dominant_asset
                FROM wallet_graph.wallets w
                LEFT JOIN LATERAL (
                    SELECT * FROM wallet_graph.wallet_risk_scores
                    WHERE wallet_address = w.address
                    ORDER BY computed_at DESC LIMIT 1
                ) wrs ON TRUE
                ORDER BY w.total_stablecoin_value DESC NULLS LAST
                LIMIT %s
                """,
                (limit,),
            )
        return {"wallets": rows, "count": len(rows)}

    @app.get("/api/wallets/riskiest")
    async def wallets_riskiest(limit: int = Query(50, ge=1, le=500)):
        """Wallets with lowest risk scores (most at-risk capital)."""
        rows = fetch_all(
            """
            SELECT w.address, w.total_stablecoin_value, w.size_tier, w.label,
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
            ORDER BY wrs.risk_score ASC
            LIMIT %s
            """,
            (limit,),
        )
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
                COALESCE(SUM(total_stablecoin_value), 0) AS total_value_tracked,
                COUNT(*) FILTER (WHERE size_tier = 'whale') AS whale_count,
                COUNT(*) FILTER (WHERE size_tier = 'institutional') AS institutional_count,
                COUNT(*) FILTER (WHERE size_tier = 'retail') AS retail_count
            FROM wallet_graph.wallets
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
        wallet = fetch_one(
            """
            SELECT address, first_seen_at, last_indexed_at, total_stablecoin_value,
                   size_tier, source, is_contract, label
            FROM wallet_graph.wallets
            WHERE address = %s
            """,
            (addr,),
        )
        if not wallet:
            raise HTTPException(status_code=404, detail="Wallet not found in index")

        # Latest risk score
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

        # Latest holdings
        holdings = fetch_all(
            """
            SELECT token_address, symbol, balance, value_usd,
                   is_scored, sii_score, sii_grade, pct_of_wallet, indexed_at
            FROM wallet_graph.wallet_holdings
            WHERE wallet_address = %s
              AND indexed_at = (
                  SELECT MAX(indexed_at) FROM wallet_graph.wallet_holdings
                  WHERE wallet_address = %s
              )
            ORDER BY value_usd DESC
            """,
            (addr, addr),
        )

        return {
            "wallet": wallet,
            "risk": risk,
            "holdings": holdings,
            "methodology_version": current_wallet_version,
            "methodology_version_pinned": pinned,
        }

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
    async def wallet_connections(address: str, limit: int = Query(default=20, ge=1, le=100)):
        """Top counterparties for a wallet sorted by edge weight."""
        addr = address.lower()
        edges = fetch_all(
            """
            SELECT
                CASE WHEN from_address = %s THEN to_address ELSE from_address END AS counterparty,
                transfer_count, total_value_usd, first_transfer_at, last_transfer_at,
                weight, tokens_transferred
            FROM wallet_graph.wallet_edges
            WHERE from_address = %s OR to_address = %s
            ORDER BY weight DESC
            LIMIT %s
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
    async def wallet_contagion(address: str, depth: int = Query(default=1, ge=1, le=2)):
        """If this wallet's holdings depeg, who's exposed?"""
        addr = address.lower()
        direct = fetch_all(
            """
            SELECT
                CASE WHEN from_address = %s THEN to_address ELSE from_address END AS counterparty,
                weight, total_value_usd
            FROM wallet_graph.wallet_edges
            WHERE (from_address = %s OR to_address = %s)
              AND weight > 0.1
            ORDER BY weight DESC
            LIMIT 50
            """,
            (addr, addr, addr),
        )

        counterparties = []
        for edge in direct:
            cp = edge["counterparty"]
            risk = fetch_one(
                """SELECT risk_score, risk_grade, total_stablecoin_value
                FROM wallet_graph.wallet_risk_scores
                WHERE wallet_address = %s ORDER BY computed_at DESC LIMIT 1""",
                (cp,),
            )
            entry = {
                "address": cp,
                "edge_weight": round(float(edge["weight"]), 4) if edge.get("weight") else 0,
                "transfer_value_usd": edge["total_value_usd"],
                "risk_score": float(risk["risk_score"]) if risk and risk.get("risk_score") else None,
                "risk_grade": risk["risk_grade"] if risk else None,
                "holdings_value_usd": float(risk["total_stablecoin_value"]) if risk and risk.get("total_stablecoin_value") else None,
            }

            if depth >= 2:
                secondary = fetch_all(
                    """
                    SELECT COUNT(*) AS cnt, COALESCE(SUM(total_value_usd), 0) AS exposure
                    FROM wallet_graph.wallet_edges
                    WHERE (from_address = %s OR to_address = %s)
                      AND from_address != %s AND to_address != %s
                      AND weight > 0.1
                    """,
                    (cp, cp, addr, addr),
                )
                entry["secondary_connections"] = secondary[0]["cnt"] if secondary else 0
                entry["secondary_exposure_usd"] = float(secondary[0]["exposure"]) if secondary else 0

            counterparties.append(entry)

        return {
            "source_wallet": addr,
            "depth": depth,
            "direct_counterparties": len(counterparties),
            "direct_exposure_usd": sum(c["transfer_value_usd"] or 0 for c in counterparties),
            "counterparties": counterparties,
        }

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
        if not admin_key or key != admin_key:
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
        """Edge graph statistics and build progress."""
        edge_stats = fetch_one(
            """
            SELECT
                COUNT(*) AS total_edges,
                COALESCE(SUM(total_value_usd), 0) AS total_value,
                COALESCE(AVG(weight), 0) AS avg_weight,
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
        wallets_total = fetch_one("SELECT COUNT(*) AS cnt FROM wallet_graph.wallets")
        return {
            "edges": {
                "total": edge_stats["total_edges"] if edge_stats else 0,
                "total_value_transferred": float(edge_stats["total_value"]) if edge_stats else 0,
                "avg_weight": round(float(edge_stats["avg_weight"]), 4) if edge_stats else 0,
                "newest_edge": edge_stats["newest_edge"].isoformat() if edge_stats and edge_stats.get("newest_edge") else None,
            },
            "build_progress": {
                "wallets_built": build_stats["built"] if build_stats else 0,
                "wallets_pending": build_stats["pending"] if build_stats else 0,
                "wallets_total": wallets_total["cnt"] if wallets_total else 0,
            },
        }

    @app.post("/api/admin/build-edges")
    async def trigger_edge_build(
        key: str = Query(default=None),
        max_wallets: int = Query(default=50, ge=1, le=500),
        priority: str = Query(default="value"),
    ):
        """Trigger edge building for wallets (admin-only)."""
        admin_key = os.environ.get("ADMIN_KEY", "")
        if not admin_key or key != admin_key:
            raise HTTPException(status_code=403, detail="Invalid admin key")
        from app.indexer.edges import run_edge_builder
        result = await run_edge_builder(max_wallets=max_wallets, priority=priority)
        return result
