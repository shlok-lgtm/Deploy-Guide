"""
Umbrella router for all /api/engine/* endpoints.

Per Step 0 doc §7 (Revised Execution Plan) the engine exposes one top-level
router composed of per-component sub-routers. This file aggregates them.
app/server.py imports only this module; individual sub-router files are not
referenced from server.py directly.

Current state:
  - coverage_router  registered (Component 1 / S0)
  - analyze_router   registered (Component 2a / S2a — skeleton with stub
                                 interpretation; S2b adds real Signal,
                                 S2c adds LLM interpretation)
  - budget_router    registered (Component 2c / S2c — admin GET /budget
                                 for cost observability)
  - render_router    registered (Component 3 / S3 — POST /render plus
                                 artifact GET endpoints)

Future sessions add:
  - admin-style endpoints for events and watchlist (Component 4)
"""

from __future__ import annotations

from fastapi import APIRouter

from app.engine.analyze_router import router as analyze_router
from app.engine.budget_router import router as budget_router
from app.engine.coverage_router import router as coverage_router
from app.engine.render_router import router as render_router

router = APIRouter()
router.include_router(coverage_router)
router.include_router(analyze_router)
router.include_router(budget_router)
router.include_router(render_router)
