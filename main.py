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

    indexer_interval_hours = int(os.environ.get("INDEXER_INTERVAL_HOURS", "24"))
    last_indexed_at = time.time()  # treat startup as last index time

    while True:
        try:
            asyncio.run(run_scoring_cycle())
        except Exception as e:
            logger.error(f"Worker cycle error: {e}")

        # Periodic wallet re-indexing (default every 24h, tunable via INDEXER_INTERVAL_HOURS)
        hours_since_index = (time.time() - last_indexed_at) / 3600
        if hours_since_index >= indexer_interval_hours:
            try:
                logger.info(f"Scheduled wallet re-indexing ({hours_since_index:.1f}h since last run)...")
                from app.indexer.pipeline import run_pipeline
                asyncio.run(run_pipeline())
                last_indexed_at = time.time()
                logger.info("Scheduled wallet re-indexing complete")
            except Exception as e:
                logger.warning(f"Scheduled wallet re-indexing failed: {e}")

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
