"""Publication gate (migration 112).

Single source of truth for the SII/PSI visibility boundary. The
automation boundary is *publication*, not scoring — newly-discovered
entities are scored and generate score history immediately, but
carry published=FALSE and MUST NOT appear on any user-facing surface
until an operator flips the flag.

Two enforcement primitives, both built on the migration-112 schema:

1. Views — `stablecoins_published`, `psi_scores_published`. Public
   read paths SELECT from these instead of the raw tables. A new
   (unpublished) row is automatically excluded.

2. Existence guards — `is_sii_published()` / `is_psi_published()`
   for endpoints that take an entity id in the path (e.g. temporal
   reconstruction `/api/scores/{coin}/at/{date}`). The guard raises
   HTTP 404, which is the correct surface (we deny the entity's
   existence on the public API while it's unpublished).

Ops/admin routes intentionally SELECT from the raw tables so the
architect can see and approve unpublished entities. The CI gate in
`tests/test_publication_gate_lint.py` enforces that no new public
read path references the raw tables.
"""

from __future__ import annotations

from fastapi import HTTPException

from app.database import fetch_one, fetch_one_async


def is_sii_published(coin_id: str) -> bool:
    """Sync check. Returns True only if the SII coin exists AND is published.

    Use in sync code paths (temporal reconstruction handlers run in
    threadpool). For async code, prefer `is_sii_published_async`.
    """
    row = fetch_one(
        "SELECT 1 FROM stablecoins WHERE id = %s AND published = TRUE",
        (coin_id,),
    )
    return row is not None


async def is_sii_published_async(coin_id: str) -> bool:
    """Async check — see `is_sii_published`."""
    row = await fetch_one_async(
        "SELECT 1 FROM stablecoins WHERE id = %s AND published = TRUE",
        (coin_id,),
    )
    return row is not None


def is_psi_published(protocol_slug: str) -> bool:
    """Sync check. Returns True only if a publication-state row exists
    for the protocol AND is marked published.

    A protocol with no row in protocol_publication_state is treated as
    unpublished. Migration 112 backfilled the existing PSI corpus to
    published=TRUE; auto-discovery (Phase 2) inserts new rows with
    published=FALSE.
    """
    row = fetch_one(
        "SELECT 1 FROM protocol_publication_state "
        "WHERE protocol_slug = %s AND published = TRUE",
        (protocol_slug,),
    )
    return row is not None


async def is_psi_published_async(protocol_slug: str) -> bool:
    """Async check — see `is_psi_published`."""
    row = await fetch_one_async(
        "SELECT 1 FROM protocol_publication_state "
        "WHERE protocol_slug = %s AND published = TRUE",
        (protocol_slug,),
    )
    return row is not None


async def require_sii_published(coin_id: str) -> None:
    """Raise HTTP 404 if the SII coin is missing or unpublished.

    Use at the top of any public endpoint that takes a coin id and
    reaches into tables/joins that bypass `stablecoins_published`
    (e.g. temporal reconstruction reading `historical_prices`
    directly via the temporal_engine).
    """
    if not await is_sii_published_async(coin_id):
        raise HTTPException(status_code=404, detail=f"Unknown stablecoin: {coin_id}")


async def require_psi_published(protocol_slug: str) -> None:
    """Raise HTTP 404 if the PSI protocol is missing or unpublished."""
    if not await is_psi_published_async(protocol_slug):
        raise HTTPException(status_code=404, detail=f"Unknown protocol: {protocol_slug}")
