"""
PSI Discovery Monitor — Module-Canonical Wrapper (v9.12 #198 / v9.13 #193)
==========================================================================
Single owner of `attest_state("psi_discoveries", ...)`.

Pre-refactor, three writers diverged in payload + gating
(worker.py path A, main.py path B, enrichment_worker.py path C). Per
docs/audits/2026-05-12-psi-discoveries-design-questions.md and operator
sign-off on issue #200 (Q1=C, Q2=C, Q3=A, Q5=A; Q4/Q6 deferred):

- Depth (ii): expansion workload stays in worker.py:run_slow_cycle;
  this wrapper owns only the freshness gate + attest call.
- Payload: {status, synced, discovered, enriched, promoted}.
  `hours_since_last_expansion` is dropped (it churned batch_hash).
- Gate: SELECT MAX(snapshot_date) FROM protocol_collateral_exposure,
  24h threshold. Gate-closed branch emits status="skipped_fresh"
  (prevents the 29-day silence #157 fixed).
- entity_id always NULL; methodology_version defaults to FORMULA_VERSION.

The slow-cycle heartbeat at worker.py:2602-2652 also writes this
domain — preserved untouched during rollout (Q4 deferred).
"""

import asyncio
import logging
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

_PSI_DISCOVERY_GATE_HOURS = 24


async def run_psi_discovery_monitor_scheduled(
    cycle_ts: Optional[datetime] = None,
    *,
    status: str = "skipped_unknown",
    synced: int = 0,
    discovered: int = 0,
    enriched: int = 0,
    promoted: int = 0,
) -> dict:
    """Three terminal branches: skipped_fresh, ran, error.

    Args:
        cycle_ts: informational cycle timestamp for logging.
        status: caller's workload status (ran | skipped_fresh | error |
            skipped_unknown).
        synced/discovered/enriched/promoted: workload result counts.

    Returns:
        The payload dict that was attested.
    """
    from app.database import fetch_one_async
    from app.state_attestation import attest_state

    # Gate query — verbatim from pre-refactor path A semantics
    # (worker.py:2330-2339).
    hours_since: float = float(_PSI_DISCOVERY_GATE_HOURS + 1)  # default: stale
    try:
        last_expansion = await fetch_one_async(
            "SELECT MAX(snapshot_date) AS latest FROM protocol_collateral_exposure"
        )
        last_date = last_expansion["latest"] if last_expansion else None
        if last_date and isinstance(last_date, date):
            hours_since = float((date.today() - last_date).days * 24)
    except Exception as e:
        logger.warning(f"[psi_discovery_monitor] freshness query failed: {e}")

    # Branch selection
    if status == "error":
        out_payload = {
            "status": "error",
            "synced": synced, "discovered": discovered,
            "enriched": enriched, "promoted": promoted,
        }
    elif hours_since < _PSI_DISCOVERY_GATE_HOURS:
        # Gate closed — emit skipped_fresh heartbeat (Q3=A).
        out_payload = {
            "status": "skipped_fresh",
            "synced": 0, "discovered": 0, "enriched": 0, "promoted": 0,
        }
    else:
        out_payload = {
            "status": "ran" if status == "ran" else status,
            "synced": synced, "discovered": discovered,
            "enriched": enriched, "promoted": promoted,
        }

    try:
        await asyncio.to_thread(attest_state, "psi_discoveries", [out_payload], None, "module.psi_discovery_monitor")
    except Exception as ae:
        logger.warning(f"[psi_discovery_monitor] attest failed: {ae}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="psi_discoveries_attestation_failure",
                error_message=str(ae)[:500],
                cycle_phase="psi_expansion",
            )
        except Exception:
            pass

    logger.info(
        f"[psi_discovery_monitor] status={out_payload['status']} "
        f"synced={synced} discovered={discovered} "
        f"enriched={enriched} promoted={promoted}"
    )
    return out_payload
