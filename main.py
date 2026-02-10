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


def run_worker_loop():
    """Background thread: runs scoring cycle on an interval."""
    # Wait for server to be up
    time.sleep(10)
    logger.info(f"Worker thread started (interval: {WORKER_INTERVAL} min)")

    from app.worker import run_scoring_cycle

    while True:
        try:
            asyncio.run(run_scoring_cycle())
        except Exception as e:
            logger.error(f"Worker cycle error: {e}")
        
        logger.info(f"Worker sleeping {WORKER_INTERVAL} minutes...")
        time.sleep(WORKER_INTERVAL * 60)


def main():
    # 1. Initialize database
    logger.info("Initializing database pool...")
    init_pool()

    db_status = db_health_check()
    if db_status.get("status") == "healthy":
        logger.info("Database connected ✓")
    else:
        logger.warning(f"Database connection issue — API will start but scores may be empty: {db_status}")

    # 2. Start worker thread
    worker_enabled = os.environ.get("WORKER_ENABLED", "true").lower() == "true"
    if worker_enabled:
        worker_thread = threading.Thread(target=run_worker_loop, daemon=True)
        worker_thread.start()
        logger.info("Background worker thread started")
    else:
        logger.info("Worker disabled (set WORKER_ENABLED=true to enable)")

    # 3. Start API server
    port = int(os.environ.get("PORT", os.environ.get("API_PORT", "5000")))
    logger.info(f"Starting API on port {port}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
