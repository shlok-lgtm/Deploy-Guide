"""
Task Registry
=============
Register pipeline tasks declaratively with cadence assignment.
Tasks auto-discover, auto-instrument, and run at the right frequency.

To add a new task:
  1. Write the task function (async)
  2. Add one entry to get_tasks() below
  3. Done. It runs at the assigned cadence with timeout and error handling.
"""

import asyncio
import time
import logging
from dataclasses import dataclass
from typing import Callable, Awaitable, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class Cadence(Enum):
    FAST = "fast"      # Every cycle (~60 min) — critical, must stay fresh
    SLOW = "slow"      # Every 3rd cycle (~3 hours) — enrichment, can be slow
    DAILY = "daily"    # Once per day — gated by DB timestamp
    RARE = "rare"      # Every 6th cycle (~6 hours)


@dataclass
class Task:
    name: str
    fn: Optional[Callable[..., Awaitable]]  # async function to call
    cadence: Cadence
    timeout: int = 900            # seconds (default 15 min)
    description: str = ""
    enabled: bool = True


# =========================================================================
# Task definitions — THE registry. Add new tasks here.
# =========================================================================

def get_tasks() -> list[Task]:
    """Build task list. Imports are deferred to avoid circular deps."""
    return [
        # === FAST: Every cycle, 15-min timeout ===
        Task(
            name="sii_scoring",
            fn=None,  # Set by worker.py — needs httpx client + stablecoin list
            cadence=Cadence.FAST,
            timeout=900,
            description="Score all stablecoins across all collectors",
        ),
        Task(
            name="psi_scoring",
            fn=None,  # Set by worker.py
            cadence=Cadence.FAST,
            timeout=600,
            description="Score protocols (PSI)",
        ),
        Task(
            name="verification_agent",
            fn=None,  # Set by worker.py
            cadence=Cadence.FAST,
            timeout=120,
            description="Run verification agent cycle",
        ),
        Task(
            name="health_sweep",
            fn=None,  # Set by worker.py
            cadence=Cadence.FAST,
            timeout=120,
            description="Run health checks and dispatch alerts",
        ),
        Task(
            name="pulse_generation",
            fn=None,  # Set by worker.py — daily gated internally
            cadence=Cadence.FAST,
            timeout=60,
            description="Generate daily state commitment",
        ),
        Task(
            name="daily_digest",
            fn=None,  # Set by worker.py — daily gated internally
            cadence=Cadence.FAST,
            timeout=30,
            description="Send daily operational summary email",
        ),

        # === SLOW: Every 3rd cycle, various timeouts ===
        Task(
            name="rpi_scoring",
            fn=None,  # Set by worker.py — daily gated internally
            cadence=Cadence.SLOW,
            timeout=900,
            description="Score protocols on governance (RPI)",
        ),
        Task(
            name="cda_collection",
            fn=None,  # Set by worker.py — daily gated internally
            cadence=Cadence.SLOW,
            timeout=600,
            description="Collect CDA issuer disclosures",
        ),
        Task(
            name="wallet_reindex",
            fn=None,  # Set by worker.py
            cadence=Cadence.SLOW,
            timeout=600,
            description="Re-index 500 stalest wallets",
        ),
        Task(
            name="wallet_expansion",
            fn=None,  # Set by worker.py — daily gated internally
            cadence=Cadence.SLOW,
            timeout=1800,
            description="Expand wallet coverage + rebuild profiles",
        ),
        Task(
            name="treasury_flows",
            fn=None,  # Set by worker.py
            cadence=Cadence.SLOW,
            timeout=120,
            description="Detect treasury flow events",
        ),
        Task(
            name="edge_building",
            fn=None,  # Set by worker.py
            cadence=Cadence.SLOW,
            timeout=1800,
            description="Build transfer edges across 4 chains",
        ),
        Task(
            name="actor_classification",
            fn=None,  # Set by worker.py
            cadence=Cadence.SLOW,
            timeout=300,
            description="Classify wallet actors",
        ),
        Task(
            name="discovery",
            fn=None,  # Set by worker.py
            cadence=Cadence.SLOW,
            timeout=120,
            description="Run discovery signal cycle",
        ),
        Task(
            name="provenance_attestation",
            fn=None,  # Set by worker.py
            cadence=Cadence.SLOW,
            timeout=60,
            description="Attest provenance proofs",
        ),
        Task(
            name="psi_expansion",
            fn=None,  # Set by worker.py — daily gated internally
            cadence=Cadence.SLOW,
            timeout=300,
            description="Expand PSI protocol coverage",
        ),

        # === RARE: Every 6th cycle ===
        Task(
            name="governance_crawl",
            fn=None,  # Set by worker.py
            cadence=Cadence.RARE,
            timeout=300,
            description="Crawl governance proposals",
        ),
    ]


async def run_tasks(tasks: list[Task], label: str = "tasks"):
    """Run a list of tasks sequentially with per-task timeout and error handling."""
    start = time.time()
    active = [t for t in tasks if t.enabled and t.fn is not None]
    logger.info(f"=== {label} start ({len(active)} tasks) ===")

    for task in tasks:
        if not task.enabled or task.fn is None:
            continue
        task_start = time.time()
        try:
            await asyncio.wait_for(task.fn(), timeout=task.timeout)
            elapsed = time.time() - task_start
            logger.info(f"  {task.name}: completed in {elapsed:.1f}s")
        except asyncio.TimeoutError:
            logger.error(f"  {task.name}: exceeded {task.timeout}s timeout")
        except Exception as e:
            logger.warning(f"  {task.name} failed: {e}")

    elapsed = time.time() - start
    logger.info(f"=== {label} complete in {elapsed:.0f}s ===")
