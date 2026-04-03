"""
Discovery Layer — Orchestrator
================================
Triggers dbt run after each scoring cycle.
Reads materialized signals from dbt models.
Stores top signals in discovery_signals for lifecycle tracking.
Runs Python-only detectors (graph topology) as post-dbt enrichment.

This module contains NO analytical logic. All transformations live in dbt models.
"""

import json
import logging
import subprocess
import os
from typing import List, Dict
from urllib.parse import urlparse

from app.database import fetch_all, execute

logger = logging.getLogger(__name__)

DISCOVERY_VERSION = "discovery-v0.1.0"
DBT_PROJECT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dbt")


def _ensure_pg_env():
    """Parse DATABASE_URL into PG* env vars if they aren't already set."""
    if os.environ.get("PGHOST"):
        return
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        return
    parsed = urlparse(db_url)
    os.environ.setdefault("PGHOST", parsed.hostname or "")
    os.environ.setdefault("PGPORT", str(parsed.port or 5432))
    os.environ.setdefault("PGUSER", parsed.username or "")
    os.environ.setdefault("PGPASSWORD", parsed.password or "")
    os.environ.setdefault("PGDATABASE", parsed.path.lstrip("/") if parsed.path else "")


def run_dbt():
    """Execute dbt run against the discovery project."""
    _ensure_pg_env()
    logger.info("Running dbt models...")
    result = subprocess.run(
        ["dbt", "run", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR],
        capture_output=True, text=True, timeout=300
    )
    if result.returncode != 0:
        logger.error(f"dbt run failed:\n{result.stderr}")
        raise RuntimeError(f"dbt run failed: {result.stderr[-500:]}")
    logger.info("dbt run complete")
    return result.stdout


def collect_dbt_signals() -> List[Dict]:
    """Read signals from the materialized disc_all_signals table."""
    try:
        rows = fetch_all("""
            SELECT signal_type, domain, title, description, entities,
                   novelty_score, direction, magnitude, baseline, detail
            FROM discovery.disc_all_signals
            WHERE novelty_score > 0.5
            ORDER BY novelty_score DESC
        """)
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning(f"Could not read dbt signals: {e}")
        return []


def run_graph_detectors() -> List[Dict]:
    """
    Python-only graph topology analysis using networkx.
    These can't be expressed in SQL.
    """
    signals = []

    try:
        import networkx as nx

        edges = fetch_all("""
            SELECT from_address, to_address, weight, total_value_usd
            FROM wallet_graph.wallet_edges
            WHERE weight > 0.1
              AND last_transfer_at >= NOW() - INTERVAL '30 days'
        """)

        if len(edges) < 50:
            logger.info("Graph too small for topology analysis — skipping")
            return signals

        G = nx.DiGraph()
        for e in edges:
            G.add_edge(e["from_address"], e["to_address"], weight=e["weight"], value=e["total_value_usd"])

        # Bridge node detection: high centrality + low risk score
        betweenness = nx.betweenness_centrality(G, k=min(100, len(G.nodes())))
        top_central = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[:10]

        for addr, centrality in top_central:
            risk = fetch_all("""
                SELECT risk_score FROM wallet_graph.wallet_risk_scores
                WHERE wallet_address = %s
                ORDER BY computed_at DESC LIMIT 1
            """, (addr,))
            if risk and risk[0]["risk_score"] and risk[0]["risk_score"] < 70:
                signals.append({
                    "signal_type": "risky_bridge",
                    "domain": "graph",
                    "title": f"High-centrality wallet {addr[:10]}... has low risk score ({risk[0]['risk_score']:.0f})",
                    "description": f"Betweenness centrality: {centrality:.4f}. This wallet connects otherwise separate clusters.",
                    "entities": [addr],
                    "novelty_score": centrality * 10,
                    "direction": "break",
                    "magnitude": centrality,
                    "baseline": 0,
                    "detail": {"centrality": centrality, "risk_score": risk[0]["risk_score"]},
                })

        # Degree distribution concentration (Gini)
        degrees = [d for _, d in G.degree()]
        if len(degrees) > 10:
            sorted_degrees = sorted(degrees)
            n = len(sorted_degrees)
            cumulative = sum((2 * i - n - 1) * d for i, d in enumerate(sorted_degrees, 1))
            gini = cumulative / (n * sum(sorted_degrees)) if sum(sorted_degrees) > 0 else 0

            signals.append({
                "signal_type": "concentration_topology",
                "domain": "graph",
                "title": f"Graph Gini coefficient: {gini:.3f}",
                "description": f"Degree concentration across {len(G.nodes())} nodes, {len(G.edges())} edges.",
                "entities": [],
                "novelty_score": max(0, gini - 0.5) * 5,
                "direction": "shift",
                "magnitude": gini,
                "baseline": 0.5,
                "detail": {"nodes": len(G.nodes()), "edges": len(G.edges()), "gini": gini},
            })

    except ImportError:
        logger.warning("networkx not installed — skipping graph detectors")
    except Exception as e:
        logger.warning(f"Graph detector failed: {e}")

    return signals


def store_signals(signals: List[Dict]):
    """Persist signals to discovery_signals table for lifecycle tracking."""
    stored = 0
    for s in signals:
        try:
            execute("""
                INSERT INTO discovery_signals
                (signal_type, domain, title, description, entities,
                 novelty_score, direction, magnitude, baseline,
                 detail, methodology_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                s.get("signal_type"),
                s.get("domain"),
                s.get("title"),
                s.get("description"),
                json.dumps(s.get("entities", [])),
                s.get("novelty_score", 0),
                s.get("direction"),
                s.get("magnitude"),
                s.get("baseline"),
                json.dumps(s.get("detail", {})),
                DISCOVERY_VERSION,
            ))
            stored += 1
        except Exception as e:
            logger.warning(f"Failed to store signal: {e}")
    return stored


def run_discovery_cycle():
    """Full discovery cycle: dbt run -> collect signals -> graph detectors -> store."""
    logger.info("Starting discovery cycle")

    # 1. Run dbt transformations
    try:
        run_dbt()
    except Exception as e:
        logger.warning(f"dbt run failed — continuing with graph detectors only: {e}")

    # 2. Collect dbt-materialized signals
    dbt_signals = collect_dbt_signals()
    logger.info(f"dbt signals collected: {len(dbt_signals)}")

    # 3. Run Python-only graph detectors
    graph_signals = run_graph_detectors()
    logger.info(f"Graph signals collected: {len(graph_signals)}")

    # 4. Combine and sort
    all_signals = dbt_signals + graph_signals
    all_signals.sort(key=lambda s: s.get("novelty_score", 0), reverse=True)

    # 5. Store
    stored = store_signals(all_signals)
    logger.info(f"Discovery cycle complete: {len(all_signals)} detected, {stored} stored")

    return all_signals
