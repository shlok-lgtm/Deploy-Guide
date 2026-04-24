"""
Component 1 router: GET /api/engine/coverage/{identifier}

Auth: PUBLIC per Step 0 doc §5. Rate limiting is applied by the global
middleware in app/server.py (10 req/min per IP for public, 120/min keyed),
not per-route via a Depends — mirroring the existing codebase convention.

Handler lives in app/engine/coverage.py. This file is strictly the route
declaration + 404 translation.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.engine.coverage import get_entity_coverage
from app.engine.schemas import CoverageResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/engine/coverage/{identifier}", response_model=CoverageResponse)
def get_coverage(
    identifier: str,
    include_related: bool = Query(True),
    date_range_start: Optional[date] = Query(None),
    date_range_end: Optional[date] = Query(None),
) -> CoverageResponse:
    """Return Basis's coverage of an entity across all indexes."""
    result = get_entity_coverage(
        identifier=identifier,
        include_related=include_related,
        date_range_start=date_range_start,
        date_range_end=date_range_end,
    )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No Basis coverage found for identifier '{identifier}'",
        )
    return result
