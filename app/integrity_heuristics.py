"""
Integrity Work-Availability Heuristics
========================================
Distinguish "broken" (work should have happened but didn't) from
"quiet" (genuinely no upstream work was due — fresh enough).

See docs/integrity_heuristics.md for per-domain justification.

Public API
----------
classify_freshness(domain, age_hours, expected_hours) -> "ok" | "broken" | "quiet"
work_should_have_happened(domain) -> bool
HEURISTIC_VERSION
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Callable

from app.database import fetch_one

logger = logging.getLogger(__name__)

HEURISTIC_VERSION = "integrity_heuristics_v1"


# ---------------------------------------------------------------------------
# Per-domain "work should have happened" heuristics.
#
# Each heuristic returns:
#   True  -> upstream work was due in the relevant window; if the domain is
#            still stale, that is genuinely "broken"
#   False -> no upstream work was due; a stale domain is "quiet"
# ---------------------------------------------------------------------------


def _safe_fetch_one(query: str, params: tuple | None = None) -> dict | None:
    """fetch_one wrapper that swallows DB errors and logs at debug level.

    A heuristic that fails to evaluate is treated as "work was due" so we
    fall back to the conservative legacy behaviour of alerting.
    """
    try:
        return fetch_one(query, params)
    except Exception as exc:
        logger.debug(f"integrity_heuristics: query failed ({exc}); defaulting to 'work due'")
        return None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _is_recent(ts, max_age: timedelta) -> bool:
    if ts is None:
        return False
    if hasattr(ts, "tzinfo") and ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (_now() - ts) <= max_age


def _psi_discoveries_work_due() -> bool:
    """psi_expansion is gated on protocol_collateral_exposure freshness (24h)
    and only attests when something was discovered or promoted.

    No work due if: the gate is closed (snapshot < 24h old) AND the backlog
    has nothing pending promotion.
    """
    snap = _safe_fetch_one(
        "SELECT MAX(snapshot_date)::timestamptz AS latest FROM protocol_collateral_exposure"
    )
    snap_recent = snap and _is_recent(snap.get("latest"), timedelta(hours=24))

    if not snap_recent:
        # Gate is open — psi_expansion should have run and attested.
        return True

    # Gate is closed. Check if backlog has anything to promote.
    pending = _safe_fetch_one(
        """
        SELECT COUNT(*) AS cnt FROM psi_protocol_backlog
        WHERE promotion_eligible = TRUE
          AND COALESCE(promoted, FALSE) = FALSE
        """
    )
    if pending is None:
        # Table missing or query failed — rely on snapshot freshness alone.
        return False

    return (pending.get("cnt") or 0) > 0


def _provenance_work_due() -> bool:
    """Provenance proofs are continuous; quiet if a proof landed in the last 4h."""
    row = _safe_fetch_one("SELECT MAX(proved_at) AS latest FROM provenance_proofs")
    if not row or row.get("latest") is None:
        # No proofs at all — work is overdue.
        return True
    return not _is_recent(row.get("latest"), timedelta(hours=4))


def _actors_work_due() -> bool:
    """Actor classification is continuous; quiet if the cycle ran in the last hour."""
    row = _safe_fetch_one(
        """
        SELECT MAX(created_at) AS latest
        FROM collector_cycle_stats
        WHERE collector_name = %s
        """,
        ("actor_classification",),
    )
    if not row or row.get("latest") is None:
        return True
    return not _is_recent(row.get("latest"), timedelta(hours=1))


def _rpi_components_work_due() -> bool:
    """rpi_scoring is gated on rpi_scores.computed_at (24h)."""
    row = _safe_fetch_one("SELECT MAX(computed_at) AS latest FROM rpi_scores")
    if not row or row.get("latest") is None:
        return True
    return not _is_recent(row.get("latest"), timedelta(hours=24))


def _sii_components_work_due() -> bool:
    """SII scoring is continuous; quiet if scores were written in the last hour."""
    row = _safe_fetch_one("SELECT MAX(computed_at) AS latest FROM scores")
    if not row or row.get("latest") is None:
        return True
    return not _is_recent(row.get("latest"), timedelta(hours=1))


def _divergence_signals_work_due() -> bool:
    """Divergence detector runs every 4h; quiet if it ran within that window."""
    row = _safe_fetch_one(
        """
        SELECT MAX(created_at) AS latest
        FROM collector_cycle_stats
        WHERE collector_name = %s
        """,
        ("divergence_detection",),
    )
    if not row or row.get("latest") is None:
        return True
    return not _is_recent(row.get("latest"), timedelta(hours=4))


# Registry: domain -> heuristic function. Domains absent from this map fall
# through to the legacy "stale = broken" behaviour.
_HEURISTICS: dict[str, Callable[[], bool]] = {
    "psi_discoveries": _psi_discoveries_work_due,
    "provenance": _provenance_work_due,
    "actors": _actors_work_due,
    "rpi_components": _rpi_components_work_due,
    "sii_components": _sii_components_work_due,
    "divergence_signals": _divergence_signals_work_due,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def has_heuristic(domain: str) -> bool:
    """Return True if a work-availability heuristic is registered for this domain."""
    return domain in _HEURISTICS


def work_should_have_happened(domain: str) -> bool:
    """Decide whether upstream work was due since the last attestation.

    Returns True for any domain without a registered heuristic so we
    preserve the conservative legacy alert behaviour.
    """
    fn = _HEURISTICS.get(domain)
    if fn is None:
        return True
    try:
        return bool(fn())
    except Exception as exc:
        logger.debug(f"integrity_heuristics: heuristic for {domain} raised ({exc}); defaulting to 'work due'")
        return True


def classify_freshness(domain: str, age_hours: float | None, expected_hours: float) -> str:
    """Classify a domain's freshness as 'ok' | 'broken' | 'quiet'.

    Args:
        domain: Canonical attestation domain name.
        age_hours: Hours since the latest attestation, or None if there is none.
        expected_hours: Expected attestation cadence in hours.

    Behaviour:
        - age_hours is None and no heuristic: 'broken' (legacy behaviour for
          missing attestations).
        - age_hours is None and heuristic says no work due: 'quiet'.
        - age_hours <= 2 * expected_hours: 'ok'.
        - age_hours > 2 * expected_hours and heuristic says work was due: 'broken'.
        - age_hours > 2 * expected_hours and heuristic says no work due: 'quiet'.
    """
    if age_hours is not None and age_hours <= expected_hours * 2:
        return "ok"

    if work_should_have_happened(domain):
        return "broken"
    return "quiet"
