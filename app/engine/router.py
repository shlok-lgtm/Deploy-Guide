"""
Umbrella router for all /api/engine/* endpoints.

Per Step 0 doc §7 (Revised Execution Plan) the engine exposes one top-level
router composed of per-component sub-routers. This file aggregates them.
app/server.py imports only this module; individual sub-router files are not
referenced from server.py directly.

Current state (Component 1 / S0):
  - coverage_router  registered

Future sessions add:
  - analyze_router   (Component 2)
  - render_router    (Component 3)
  - admin-style endpoints for events and watchlist (Component 4)
"""

from __future__ import annotations

from fastapi import APIRouter

from app.engine.coverage_router import router as coverage_router

router = APIRouter()
router.include_router(coverage_router)

# When C2 lands:
# from app.engine.analyze_router import router as analyze_router
# router.include_router(analyze_router)

# When C3 lands:
# from app.engine.render_router import router as render_router
# router.include_router(render_router)
