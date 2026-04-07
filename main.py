"""
Basis Protocol — Replit Entry Point
=====================================
Runs the API server + background worker in a single process.
Replit runs `python main.py` — this handles everything.
"""

import asyncio
import threading
import time
import os
import logging
import signal
import sys

import uvicorn

from app.database import init_pool, close_pool, health_check as db_health_check
from app.server import app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")

# Worker interval (minutes) — how often to re-score all stablecoins
WORKER_INTERVAL = int(os.environ.get("COLLECTION_INTERVAL", "60"))


def free_port(port: int) -> None:
    """Kill any process holding the given port by reading /proc/net/tcp."""
    try:
        # Find the inode(s) listening on this port
        target_hex = f"{port:04X}"
        inodes = set()
        for proto_file in ("/proc/net/tcp", "/proc/net/tcp6"):
            try:
                with open(proto_file) as f:
                    for line in f.readlines()[1:]:
                        parts = line.split()
                        if len(parts) < 10:
                            continue
                        local_addr = parts[1]
                        state = parts[3]
                        inode = parts[9]
                        local_port_hex = local_addr.split(":")[1]
                        if local_port_hex.upper() == target_hex and state == "0A":
                            inodes.add(inode)
            except FileNotFoundError:
                pass

        if not inodes:
            return

        # Find PIDs that own those inodes
        own_pid = str(os.getpid())
        for pid_dir in os.listdir("/proc"):
            if not pid_dir.isdigit() or pid_dir == own_pid:
                continue
            fd_dir = f"/proc/{pid_dir}/fd"
            try:
                for fd in os.listdir(fd_dir):
                    try:
                        link = os.readlink(f"{fd_dir}/{fd}")
                        if "socket:[" in link:
                            inode = link.split("[")[1].rstrip("]")
                            if inode in inodes:
                                logger.info(f"Killing stale process {pid_dir} holding port {port}")
                                os.kill(int(pid_dir), signal.SIGKILL)
                                time.sleep(0.5)
                                break
                    except (OSError, PermissionError):
                        pass
            except (OSError, PermissionError):
                pass
    except Exception as e:
        logger.warning(f"free_port({port}) failed: {e}")


def run_worker_loop():
    """Background thread: runs scoring cycle on an interval."""
    # Wait for server to be up
    time.sleep(10)
    logger.info(f"Worker thread started (interval: {WORKER_INTERVAL} min)")

    # One-time wallet seeding: populate on fresh deployments where tables are empty
    try:
        from app.database import fetch_one
        result = fetch_one("SELECT COUNT(*) AS c FROM wallet_graph.wallets")
        count = result["c"] if result else 0
        if count == 0:
            logger.info("Wallet tables empty — running initial seeding...")
            from app.indexer.pipeline import run_pipeline
            asyncio.run(run_pipeline())  # uses INDEXER_HOLDERS_PER_COIN env var (default 5000)
            logger.info("Initial wallet seeding complete")
        else:
            logger.info(f"Wallets already seeded ({count} wallets) — skipping initial seed")
    except Exception as e:
        logger.warning(f"Wallet seeding skipped: {e}")

    from app.worker import run_scoring_cycle

    cda_interval_hours = int(os.environ.get("CDA_COLLECTION_INTERVAL_HOURS", "24"))
    last_cda_at = 0  # Run CDA on first cycle
    last_expansion_at = 0  # Run PSI expansion on first cycle
    last_wallet_expansion_at = 0  # Run wallet expansion on first cycle
    # Seed edge build timestamp from DB so restarts don't re-trigger a fresh build
    last_edge_build_at = 0
    try:
        from app.database import fetch_one as _fone
        _edge_row = _fone("SELECT EXTRACT(EPOCH FROM MAX(last_built_at)) AS ts FROM wallet_graph.edge_build_status")
        if _edge_row and _edge_row["ts"]:
            last_edge_build_at = float(_edge_row["ts"])
            _hours_ago = (time.time() - last_edge_build_at) / 3600
            logger.info(f"Edge build last ran {_hours_ago:.1f}h ago — {'will rebuild' if _hours_ago >= 10 else 'fresh, skipping'}")
    except Exception as e:
        logger.warning(f"Could not read edge build timestamp: {e}")

    while True:
        # CDA collection before scoring so fresh off-chain data is available
        hours_since_cda = (time.time() - last_cda_at) / 3600
        if hours_since_cda >= cda_interval_hours:
            try:
                logger.info("Running CDA collection pipeline...")
                from app.services.cda_collector import run_collection
                asyncio.run(run_collection())
                last_cda_at = time.time()
                logger.info("CDA collection complete")
            except Exception as e:
                logger.warning(f"CDA collection failed: {e}")

        try:
            asyncio.run(run_scoring_cycle())
        except Exception as e:
            logger.error(f"Worker cycle error: {e}")

        # Wallet batch re-indexing — rescan stale wallets every cycle
        # Uses addresstokenbalance (Etherscan V2) or Blockscout per-address API
        try:
            from app.indexer.pipeline import run_pipeline_batch
            logger.info("Running wallet batch re-index (500 stalest wallets)...")
            reindex_result = run_pipeline_batch(batch_size=500)
            logger.info(
                f"Wallet re-index complete: {reindex_result.get('processed', 0)} processed, "
                f"{reindex_result.get('scored', 0)} scored, "
                f"{reindex_result.get('errors', 0)} errors, "
                f"{reindex_result.get('remaining', '?')} remaining"
            )
        except Exception as e:
            logger.warning(f"Wallet batch re-index failed: {e}")

        # PSI scoring — uses DeFiLlama only, no explorer API budget
        # Sleep to avoid API contention with SII cycle
        time.sleep(60)
        try:
            from app.collectors.psi_collector import run_psi_scoring
            logger.info("Running PSI scoring cycle...")
            psi_results = run_psi_scoring()
            logger.info(f"PSI scoring complete: {len(psi_results)} protocols scored")
        except Exception as e:
            logger.warning(f"PSI scoring failed: {e}")

        # PSI expansion pipeline — daily gate (discover → enrich → promote)
        hours_since_expansion = (time.time() - last_expansion_at) / 3600
        if hours_since_expansion >= 24:
            try:
                from app.collectors.psi_collector import (
                    collect_collateral_exposure,
                    sync_collateral_to_backlog,
                    discover_protocols,
                    enrich_protocol_backlog,
                    promote_eligible_protocols,
                )
                logger.info("Running PSI expansion pipeline...")
                collect_collateral_exposure()
                synced = sync_collateral_to_backlog()
                discovered = discover_protocols()
                enriched = enrich_protocol_backlog()
                promoted = promote_eligible_protocols()
                last_expansion_at = time.time()
                logger.info(
                    f"PSI expansion: {synced} stablecoins synced, {discovered} discovered, "
                    f"{enriched} enriched, {promoted} promoted"
                )
            except Exception as e:
                logger.warning(f"PSI expansion pipeline failed: {e}")

        # Wallet expansion — daily gate (seed new addresses from under-covered stablecoins)
        hours_since_wallet_expansion = (time.time() - last_wallet_expansion_at) / 3600
        if hours_since_wallet_expansion >= 24:
            last_wallet_expansion_at = time.time()  # Set unconditionally — prevent retry storms on failure

            try:
                from app.indexer.expander import run_wallet_expansion
                logger.info("Running wallet expansion pipeline...")
                expansion_result = asyncio.run(run_wallet_expansion(max_etherscan_calls=50))
                logger.info(
                    f"Wallet expansion complete: {expansion_result.get('new_wallets_seeded', 0)} seeded, "
                    f"{expansion_result.get('etherscan_calls_used', 0)} Etherscan calls used"
                )
            except Exception as e:
                logger.warning(f"Wallet expansion failed: {e}")

            # Profile rebuild — piggyback on same daily gate so new wallets get profiled
            try:
                from app.indexer.profiles import rebuild_all_profiles
                logger.info("Rebuilding wallet profiles...")
                profile_result = rebuild_all_profiles()
                logger.info(
                    f"Profile rebuild complete: {profile_result.get('built', 0)} built, "
                    f"{profile_result.get('errors', 0)} errors out of {profile_result.get('total', 0)} addresses"
                )
            except Exception as e:
                logger.warning(f"Profile rebuild failed: {e}")

        # Verification agent cycle — runs after every scoring cycle
        try:
            from app.agent.watcher import run_agent_cycle
            result = run_agent_cycle()
            if result:
                logger.info(f"Agent cycle: {result.get('assessments', 0)} assessments")
        except Exception as e:
            logger.error(f"Agent cycle error: {e}")

        # Edge building — time-based gate (~10 hours) instead of cycle counter.
        # Uses DB staleness so container restarts don't reset the schedule.
        hours_since_edge_build = (time.time() - last_edge_build_at) / 3600
        edge_stale = hours_since_edge_build >= 10
        if edge_stale:
            try:
                from app.indexer.edges import run_edge_builder
                logger.info("Running edge builder (top 200 unbuilt wallets by value)...")
                edge_result = asyncio.run(run_edge_builder(max_wallets=200, priority="value"))
                last_edge_build_at = time.time()
                logger.info(
                    f"Edge builder complete: {edge_result.get('wallets_processed', 0)} wallets, "
                    f"{edge_result.get('total_edges_created', 0)} edges"
                )
            except Exception as e:
                logger.warning(f"Edge building failed: {e}")

            # Decay + prune after edge building
            try:
                from app.indexer.edges import decay_edges, prune_stale_edges
                decay_result = decay_edges()
                logger.info(f"Edge decay: {decay_result.get('edges_decayed', 0)} edges recalculated")
                prune_result = prune_stale_edges()
                logger.info(f"Edge prune: {prune_result.get('edges_archived', 0)} archived")
            except Exception as e:
                logger.warning(f"Edge decay/prune failed: {e}")

        logger.info(f"Worker sleeping {WORKER_INTERVAL} minutes...")
        time.sleep(WORKER_INTERVAL * 60)


def run_migrations():
    """Apply database migrations if not already applied."""
    from app.database import fetch_one, run_migration, get_conn

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '001_initial_schema'")
        if result:
            logger.info("Migration 001_initial_schema already applied ✓")
        else:
            raise Exception("not applied")
    except Exception:
        logger.info("Applying initial migration...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "001_initial_schema.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Initial migration applied successfully ✓")
            else:
                logger.error("Failed to apply initial migration")
                return
        else:
            logger.warning(f"Migration file not found: {migration_path}")
            return

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '002_import_governance'")
        if result:
            logger.info("Migration 002_import_governance already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 002: import governance data...")
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "migration_002",
            os.path.join(os.path.dirname(__file__), "migrations", "002_import_governance.py")
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        success = mod.run(get_conn)
        if success:
            logger.info("Migration 002_import_governance applied ✓")
        else:
            logger.warning("Migration 002_import_governance had issues")

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '006_add_usd1'")
        if result:
            logger.info("Migration 006_add_usd1 already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 006: add USD1 stablecoin...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "006_add_usd1.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Migration 006_add_usd1 applied ✓")
            else:
                logger.error("Failed to apply migration 006_add_usd1")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '007_wallet_graph'")
        if result:
            logger.info("Migration 007_wallet_graph already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 007: wallet graph schema...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "007_wallet_graph.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Migration 007_wallet_graph applied ✓")
            else:
                logger.error("Failed to apply migration 007_wallet_graph")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '011_token_type_column'")
        if result:
            logger.info("Migration 011_token_type_column already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 011: token_type column on unscored_assets...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "011_token_type_column.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Migration 011_token_type_column applied ✓")
            else:
                logger.error("Failed to apply migration 011_token_type_column")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '012_widen_symbol_columns'")
        if result:
            logger.info("Migration 012_widen_symbol_columns already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 012: widen symbol/dominant_asset columns to VARCHAR(50)...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "012_widen_symbol_columns.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Migration 012_widen_symbol_columns applied ✓")
            else:
                logger.error("Failed to apply migration 012_widen_symbol_columns")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '013_api_usage'")
        if result:
            logger.info("Migration 013_api_usage already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 013: API usage tracking tables...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "013_api_usage.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Migration 013_api_usage applied ✓")
            else:
                logger.error("Failed to apply migration 013_api_usage")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '014_assessment_events'")
        if result:
            logger.info("Migration 014_assessment_events already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 014: assessment events + daily pulses...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "014_assessment_events.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Migration 014_assessment_events applied ✓")
            else:
                logger.error("Failed to apply migration 014_assessment_events")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    # Auto-apply remaining SQL migrations (015+) not yet recorded
    import glob as _glob
    import re as _re

    migrations_dir = os.path.join(os.path.dirname(__file__), "migrations")
    sql_files = sorted(_glob.glob(os.path.join(migrations_dir, "*.sql")))

    for sql_path in sql_files:
        basename = os.path.basename(sql_path)
        # Extract migration name: "021_wallet_edges.sql" -> "021_wallet_edges"
        name = basename.replace(".sql", "")

        # Skip migrations already handled above (001-014)
        match = _re.match(r"^(\d+)", name)
        if match and int(match.group(1)) <= 14:
            continue

        try:
            already = fetch_one("SELECT 1 FROM migrations WHERE name = %s", (name,))
            if already:
                continue
        except Exception:
            pass

        logger.info(f"Applying migration: {name}...")
        success = run_migration(sql_path)
        # Record migration — even on failure, the objects may already exist
        # (e.g., table created manually before migration system was added)
        try:
            from app.database import execute as _exec
            _exec(
                "INSERT INTO migrations (name) VALUES (%s) ON CONFLICT DO NOTHING",
                (name,),
            )
        except Exception:
            pass
        if success:
            logger.info(f"Migration {name} applied ✓")
        else:
            logger.warning(f"Migration {name} failed (objects may already exist — recorded anyway)")


def main():
    # 1. Initialize database
    logger.info("Initializing database pool...")
    init_pool()

    db_status = db_health_check()
    if db_status.get("status") == "healthy":
        logger.info("Database connected ✓")
    else:
        logger.info("Database tables may not exist yet — running migrations...")

    # 1b. Auto-apply migrations
    run_migrations()

    db_status = db_health_check()
    if db_status.get("status") == "healthy":
        logger.info(f"Database ready: {db_status.get('stablecoin_count', 0)} stablecoins registered ✓")
    else:
        logger.warning(f"Database issue after migrations: {db_status}")

    # 2. Start worker thread
    worker_enabled = os.environ.get("WORKER_ENABLED", "true").lower() == "true"
    if worker_enabled:
        worker_thread = threading.Thread(target=run_worker_loop, daemon=True)
        worker_thread.start()
        logger.info("Background worker thread started")
    else:
        logger.info("Worker disabled (set WORKER_ENABLED=true to enable)")

    # 3. Start API server (always port 5000 — mapped to :80 by Replit)
    port = 5000
    free_port(port)
    logger.info(f"Starting API on port {port}")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
