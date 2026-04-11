"""
Operations Hub API routes — all under /api/ops/*
Protected by X-Admin-Key (same pattern as existing admin endpoints).
"""
import os
import json
import logging
from datetime import datetime
import traceback as _traceback_mod
from fastapi import APIRouter, BackgroundTasks, Request, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional

from app.database import fetch_one, fetch_all, execute

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ops", tags=["ops"])


def _check_admin_key(request: Request):
    import hmac
    admin_key = os.environ.get("ADMIN_KEY", "")
    provided = (
        request.query_params.get("key", "")
        or request.headers.get("x-admin-key", "")
    )
    if not admin_key or not provided or not hmac.compare_digest(provided, admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized")


# =============================================================================
# Migration — apply ops schema
# =============================================================================

@router.post("/migrate")
async def run_migration(request: Request):
    _check_admin_key(request)
    try:
        from app.database import run_migration
        import os
        migration_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "migrations", "031_ops_hub.sql"
        )
        run_migration(migration_path)
        return {"status": "ok", "migration": "031_ops_hub"}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Seed
# =============================================================================

@router.post("/seed")
async def seed_data(request: Request):
    _check_admin_key(request)
    try:
        from app.ops.seed import seed_all
        counts = seed_all()
        return {"status": "ok", "inserted": counts}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Seed failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Targets
# =============================================================================

@router.get("/targets")
async def list_targets(
    request: Request,
    tier: Optional[int] = None,
    track: Optional[str] = None,
    stage: Optional[str] = None,
):
    _check_admin_key(request)
    try:
        conditions = []
        params = []
        if tier is not None:
            conditions.append("tier = %s")
            params.append(tier)
        if track:
            conditions.append("track = %s")
            params.append(track)
        if stage:
            conditions.append("pipeline_stage = %s")
            params.append(stage)

        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        rows = fetch_all(f"SELECT * FROM ops_targets{where} ORDER BY tier, name", params or None)
        return {"targets": rows}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/targets/{target_id}")
async def get_target(request: Request, target_id: int):
    _check_admin_key(request)
    try:
        target = fetch_one("SELECT * FROM ops_targets WHERE id = %s", (target_id,))
        if not target:
            raise HTTPException(status_code=404, detail="Target not found")

        contacts = fetch_all(
            "SELECT * FROM ops_target_contacts WHERE target_id = %s", (target_id,)
        )
        content = fetch_all(
            "SELECT * FROM ops_target_content WHERE target_id = %s ORDER BY scraped_at DESC LIMIT 20",
            (target_id,),
        )
        engagement = fetch_all(
            "SELECT * FROM ops_target_engagement_log WHERE target_id = %s ORDER BY created_at DESC LIMIT 20",
            (target_id,),
        )
        exposure = fetch_one(
            "SELECT * FROM ops_target_exposure_reports WHERE target_id = %s ORDER BY generated_at DESC LIMIT 1",
            (target_id,),
        )

        return {
            "target": target,
            "contacts": contacts,
            "recent_content": content,
            "engagement_log": engagement,
            "latest_exposure": exposure,
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.put("/targets/{target_id}/stage")
async def update_target_stage(request: Request, target_id: int):
    _check_admin_key(request)
    try:
        body = await request.json()
        new_stage = body.get("stage")
        if not new_stage:
            raise HTTPException(status_code=400, detail="stage required")

        valid_stages = [
            "not_started", "recognition", "familiarity", "direct",
            "evaluating", "trying", "binding", "archived",
        ]
        if new_stage not in valid_stages:
            raise HTTPException(status_code=400, detail=f"Invalid stage. Must be one of: {valid_stages}")

        execute(
            "UPDATE ops_targets SET pipeline_stage = %s, last_action_at = NOW(), updated_at = NOW() WHERE id = %s",
            (new_stage, target_id),
        )
        return {"status": "ok", "target_id": target_id, "new_stage": new_stage}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/targets/{target_id}/engagement")
async def log_engagement(request: Request, target_id: int):
    _check_admin_key(request)
    try:
        body = await request.json()
        action_type = body.get("action_type")
        if not action_type:
            raise HTTPException(status_code=400, detail="action_type required")

        # Verify target exists
        target = fetch_one("SELECT id FROM ops_targets WHERE id = %s", (target_id,))
        if not target:
            raise HTTPException(status_code=404, detail="Target not found")

        execute(
            """INSERT INTO ops_target_engagement_log (target_id, contact_id, action_type, content, channel, next_action)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (
                target_id,
                body.get("contact_id"),
                action_type,
                body.get("content"),
                body.get("channel"),
                body.get("next_action"),
            ),
        )
        execute(
            "UPDATE ops_targets SET last_action_at = NOW(), updated_at = NOW() WHERE id = %s",
            (target_id,),
        )
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Engagement log failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/targets/{target_id}/notes")
async def append_notes(request: Request, target_id: int):
    _check_admin_key(request)
    try:
        body = await request.json()
        note_text = body.get("text", "")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        execute(
            "UPDATE ops_targets SET notes = COALESCE(notes, '') || %s, updated_at = NOW() WHERE id = %s",
            (f"\n[{timestamp}] {note_text}", target_id),
        )
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Action Queue
# =============================================================================

@router.get("/queue")
async def get_action_queue(request: Request):
    _check_admin_key(request)
    try:
        rows = fetch_all(
            """SELECT c.*, t.name as target_name, t.tier, t.pipeline_stage
               FROM ops_target_content c
               JOIN ops_targets t ON c.target_id = t.id
               WHERE c.bridge_found = TRUE AND c.founder_decision IS NULL
               ORDER BY c.relevance_score DESC NULLS LAST, c.scraped_at DESC""",
        )
        return {"queue": rows}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/content/{content_id}/decide")
async def decide_content(request: Request, content_id: int):
    _check_admin_key(request)
    try:
        body = await request.json()
        decision = body.get("decision")
        if decision not in ("approved", "edited", "skipped", "posted"):
            raise HTTPException(status_code=400, detail="decision must be: approved, edited, skipped, posted")

        updates = {"founder_decision": decision}
        params = [decision]

        if decision == "edited":
            edited_text = body.get("edited_text", "")
            execute(
                "UPDATE ops_target_content SET founder_decision = %s, founder_edited_text = %s WHERE id = %s",
                (decision, edited_text, content_id),
            )
        elif decision == "posted":
            execute(
                "UPDATE ops_target_content SET founder_decision = %s, posted_at = NOW() WHERE id = %s",
                (decision, content_id),
            )
        else:
            execute(
                "UPDATE ops_target_content SET founder_decision = %s WHERE id = %s",
                (decision, content_id),
            )

        return {"status": "ok", "content_id": content_id, "decision": decision}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Content Feed
# =============================================================================

@router.get("/content/feed")
async def content_feed(
    request: Request,
    target_id: Optional[int] = None,
    limit: int = Query(default=50, le=200),
):
    _check_admin_key(request)
    try:
        if target_id:
            rows = fetch_all(
                """SELECT c.*, t.name as target_name
                   FROM ops_target_content c
                   JOIN ops_targets t ON c.target_id = t.id
                   WHERE c.target_id = %s
                   ORDER BY c.scraped_at DESC LIMIT %s""",
                (target_id, limit),
            )
        else:
            rows = fetch_all(
                """SELECT c.*, t.name as target_name
                   FROM ops_target_content c
                   JOIN ops_targets t ON c.target_id = t.id
                   ORDER BY c.scraped_at DESC LIMIT %s""",
                (limit,),
            )
        return {"feed": rows}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Health
# =============================================================================

@router.get("/health")
async def get_health(request: Request):
    _check_admin_key(request)
    try:
        from app.ops.tools.health_checker import get_latest_health
        checks = get_latest_health()
        return {"health": checks}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/health/check")
async def run_health_check(request: Request, background_tasks: BackgroundTasks):
    _check_admin_key(request)
    try:
        def _run_health_checks():
            import importlib
            import app.ops.tools.health_checker as _hc_mod
            importlib.reload(_hc_mod)
            try:
                results = _hc_mod.run_all_checks()
                # Fire alerts for failures
                import asyncio
                try:
                    from app.ops.tools.alerter import check_and_alert_health, check_and_alert_engagement
                    loop = asyncio.new_event_loop()
                    loop.run_until_complete(check_and_alert_health(results))
                    loop.run_until_complete(check_and_alert_engagement())
                    loop.close()
                except Exception as alert_err:
                    logger.warning(f"Alert dispatch failed (non-fatal): {alert_err}")
            except Exception as e:
                logger.error(f"Health check run failed: {e}")

        background_tasks.add_task(_run_health_checks)
        return {"status": "accepted", "message": "Health checks running in background. GET /api/ops/health for results."}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Investors
# =============================================================================

@router.get("/investors")
async def list_investors(
    request: Request,
    tier: Optional[int] = None,
    stage: Optional[str] = None,
):
    _check_admin_key(request)
    try:
        conditions = []
        params = []
        if tier is not None:
            conditions.append("tier = %s")
            params.append(tier)
        if stage:
            conditions.append("stage = %s")
            params.append(stage)

        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        rows = fetch_all(f"SELECT * FROM ops_investors{where} ORDER BY tier, name", params or None)
        return {"investors": rows}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.put("/investors/{investor_id}/stage")
async def update_investor_stage(request: Request, investor_id: int):
    _check_admin_key(request)
    try:
        body = await request.json()
        new_stage = body.get("stage")
        if not new_stage:
            raise HTTPException(status_code=400, detail="stage required")

        valid_stages = [
            "not_started", "researching", "warm_intro_sent", "meeting_scheduled",
            "meeting_completed", "dd_in_progress", "term_sheet", "closed", "passed",
            "advisor_in_place",
        ]
        if new_stage not in valid_stages:
            raise HTTPException(status_code=400, detail=f"Invalid stage. Must be one of: {valid_stages}")

        execute(
            "UPDATE ops_investors SET stage = %s, last_action_at = NOW(), updated_at = NOW() WHERE id = %s",
            (new_stage, investor_id),
        )
        return {"status": "ok", "investor_id": investor_id, "new_stage": new_stage}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/investors/{investor_id}/interaction")
async def log_investor_interaction(request: Request, investor_id: int):
    _check_admin_key(request)
    try:
        body = await request.json()
        action_type = body.get("action_type")
        if not action_type:
            raise HTTPException(status_code=400, detail="action_type required")

        # Verify investor exists
        investor = fetch_one("SELECT id FROM ops_investors WHERE id = %s", (investor_id,))
        if not investor:
            raise HTTPException(status_code=404, detail="Investor not found")

        execute(
            """INSERT INTO ops_investor_interactions (investor_id, action_type, content, response, next_step)
               VALUES (%s, %s, %s, %s, %s)""",
            (investor_id, action_type, body.get("content"), body.get("response"), body.get("next_step")),
        )
        execute(
            "UPDATE ops_investors SET last_action_at = NOW(), updated_at = NOW() WHERE id = %s",
            (investor_id,),
        )
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Investor interaction log failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/investors/{investor_id}")
async def get_investor(request: Request, investor_id: int):
    _check_admin_key(request)
    try:
        investor = fetch_one("SELECT * FROM ops_investors WHERE id = %s", (investor_id,))
        if not investor:
            raise HTTPException(status_code=404, detail="Investor not found")
        interactions = fetch_all(
            "SELECT * FROM ops_investor_interactions WHERE investor_id = %s ORDER BY occurred_at DESC LIMIT 20",
            (investor_id,),
        )
        return {"investor": investor, "interactions": interactions}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/fundraise/dashboard")
async def fundraise_dashboard(request: Request):
    _check_admin_key(request)
    try:
        investors = fetch_all("SELECT * FROM ops_investors ORDER BY tier, name")

        # Compute seed trigger milestones from live data
        from app.ops.tools.milestone_checker import check_all_milestones
        try:
            full_milestones = check_all_milestones()
            milestones = full_milestones.get("seed_triggers", _compute_milestones())
        except Exception:
            milestones = _compute_milestones()

        return {
            "investors": investors,
            "milestones": milestones,
            "raise": {"target": "$4M seed", "valuation": "$25-40M pre", "timing": "Jun-Jul 2026"},
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


def _compute_milestones():
    """Compute seed trigger milestones from live database state."""
    milestones = []

    # 1. Renderers live (check if we can query this)
    try:
        # This is a placeholder — renderer count would come from a registry
        milestones.append({
            "name": "8+ renderers live",
            "target": 8,
            "current": None,
            "met": False,
            "auto": True,
        })
    except Exception:
        pass

    # 2. API external requests/day
    try:
        row = fetch_one(
            """SELECT COUNT(*) as cnt FROM api_request_log
               WHERE created_at > NOW() - INTERVAL '24 hours'
               AND api_key_id IS NOT NULL"""
        )
        api_count = row["cnt"] if row else 0
        milestones.append({
            "name": "API >500 external requests/day",
            "target": 500,
            "current": api_count,
            "met": api_count > 500,
            "auto": True,
        })
    except Exception:
        milestones.append({"name": "API >500 external requests/day", "target": 500, "current": 0, "met": False, "auto": True})

    # 3. Protocol teams citing scores
    try:
        row = fetch_one(
            """SELECT COUNT(*) as cnt FROM ops_target_engagement_log
               WHERE action_type IN ('comment_posted', 'forum_posted')
               AND response IS NOT NULL"""
        )
        citations = row["cnt"] if row else 0
        milestones.append({
            "name": "Protocol teams citing scores",
            "target": 1,
            "current": citations,
            "met": citations > 0,
            "auto": False,
        })
    except Exception:
        milestones.append({"name": "Protocol teams citing scores", "target": 1, "current": 0, "met": False, "auto": False})

    # 4. Snap submitted for audit (manual)
    milestones.append({
        "name": "Snap submitted for audit",
        "target": 1,
        "current": 0,
        "met": False,
        "auto": False,
    })

    # 5. DAO pilot in conversation
    try:
        row = fetch_one(
            """SELECT COUNT(*) as cnt FROM ops_targets
               WHERE tier = 1 AND pipeline_stage IN ('evaluating', 'trying', 'binding')"""
        )
        pilots = row["cnt"] if row else 0
        milestones.append({
            "name": "DAO pilot in conversation",
            "target": 1,
            "current": pilots,
            "met": pilots > 0,
            "auto": True,
        })
    except Exception:
        milestones.append({"name": "DAO pilot in conversation", "target": 1, "current": 0, "met": False, "auto": True})

    met_count = sum(1 for m in milestones if m["met"])
    return {"milestones": milestones, "met": met_count, "total": len(milestones), "threshold": 3}


# =============================================================================
# Exposure Reports
# =============================================================================

@router.post("/exposure/generate")
async def generate_exposure_report(request: Request):
    _check_admin_key(request)
    try:
        body = await request.json()
        target_id = body.get("target_id")
        if not target_id:
            raise HTTPException(status_code=400, detail="target_id required")

        from app.ops.tools.exposure import generate_exposure
        result = generate_exposure(target_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Target not found")
        if "error" in result:
            return {"status": "error", "detail": result["error"]}

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Exposure report generation failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/exposure/{target_id}/latest")
async def get_latest_exposure(request: Request, target_id: int):
    _check_admin_key(request)
    try:
        row = fetch_one(
            "SELECT * FROM ops_target_exposure_reports WHERE target_id = %s ORDER BY generated_at DESC LIMIT 1",
            (target_id,),
        )
        if not row:
            raise HTTPException(status_code=404, detail="No exposure report found")
        return row
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Scrape + Analyze
# =============================================================================

@router.post("/scrape")
async def scrape_url(request: Request):
    _check_admin_key(request)
    try:
        body = await request.json()
        target_id = body.get("target_id")
        url = body.get("url")
        source_type = body.get("source_type", "blog")

        if not target_id or not url:
            raise HTTPException(status_code=400, detail="target_id and url required")

        from app.ops.tools.scraper import scrape_target
        content_id = await scrape_target(target_id, url, source_type)
        if content_id is None:
            return {"status": "error", "detail": "Scrape returned no content"}

        # Auto-trigger analysis
        from app.ops.tools.analyzer import analyze_content
        try:
            analysis = await analyze_content(content_id)
        except Exception as e:
            logger.error(f"Auto-analysis failed after scrape: {e}")
            analysis = None

        return {"content_id": content_id, "analysis": analysis}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Scrape failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/analyze/{content_id}")
async def analyze_content_endpoint(request: Request, content_id: int):
    _check_admin_key(request)
    try:
        from app.ops.tools.analyzer import analyze_content
        result = await analyze_content(content_id)
        if result is None:
            return {"status": "error", "detail": "Analysis returned no result — Claude API may be unavailable"}
        return {"content_id": content_id, "analysis": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Monitor Webhook (called by Parallel Monitor)
# =============================================================================

@router.post("/webhook/monitor")
async def monitor_webhook(request: Request):
    """
    Webhook endpoint called by Parallel Monitor when content changes.
    No admin key required — Parallel calls this directly.
    Triggers scrape + analyze pipeline.
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event_type = body.get("event_type", body.get("type", ""))
    monitor_id = body.get("monitor_id", "")
    url = body.get("url", body.get("data", {}).get("url", ""))

    logger.info(f"Monitor webhook received: type={event_type}, monitor={monitor_id}, url={url}")

    if not url:
        return {"status": "ok", "action": "no_url"}

    # Try to match URL to a target
    # For now, store it and analyze — target matching can be refined
    from app.ops.tools.scraper import scrape_target
    from app.ops.tools.analyzer import analyze_content

    # Find the most likely target based on URL patterns
    target = _match_url_to_target(url)
    target_id = target["id"] if target else None

    if target_id:
        content_id = await scrape_target(target_id, url, "blog")
        if content_id:
            await analyze_content(content_id)
            return {"status": "ok", "action": "scraped_and_analyzed", "content_id": content_id}

    return {"status": "ok", "action": "no_target_match"}


def _match_url_to_target(url: str):
    """Match a URL to a target based on known URL patterns."""
    url_lower = url.lower()
    patterns = {
        "kpk.io": "karpatkey",
        "karpatkey": "karpatkey",
        "morpho.org": "Morpho",
        "forum.morpho": "Morpho",
        "steakhouse": "Steakhouse Financial",
        "governance.aave": "Aave governance",
        "forum.cow": "CoW DAO",
        "lido.fi": "Lido Earn",
        "agentkit": "AgentKit / Coinbase",
        "coinbase": "AgentKit / Coinbase",
    }
    for pattern, target_name in patterns.items():
        if pattern in url_lower:
            return fetch_one("SELECT id, name FROM ops_targets WHERE name = %s", (target_name,))
    return None


# =============================================================================
# Monitor Setup
# =============================================================================

@router.post("/monitors/setup")
async def setup_monitors_endpoint(request: Request):
    _check_admin_key(request)
    try:
        body = await request.json()
        webhook_base_url = body.get("webhook_base_url", "")
        if not webhook_base_url:
            raise HTTPException(status_code=400, detail="webhook_base_url required")

        from app.ops.tools.scraper import setup_monitors
        monitors = await setup_monitors(webhook_base_url)
        return {"status": "ok", "monitors": monitors}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Content Items
# =============================================================================

@router.get("/content/items")
async def list_content_items(
    request: Request,
    status: Optional[str] = None,
    type: Optional[str] = None,
):
    _check_admin_key(request)
    try:
        conditions = []
        params = []
        if status:
            conditions.append("status = %s")
            params.append(status)
        if type:
            conditions.append("type = %s")
            params.append(type)

        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        rows = fetch_all(
            f"SELECT * FROM ops_content_items{where} ORDER BY scheduled_for ASC NULLS LAST, created_at DESC",
            params or None,
        )
        return {"items": rows}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/content/items")
async def create_content_item(request: Request):
    _check_admin_key(request)
    try:
        body = await request.json()
        execute(
            """INSERT INTO ops_content_items (type, title, content, target_channel, related_target_id, status, scheduled_for)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (
                body.get("type"),
                body.get("title"),
                body.get("content"),
                body.get("target_channel"),
                body.get("related_target_id"),
                body.get("status", "draft"),
                body.get("scheduled_for"),
            ),
        )
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Drafts — DM, email, forum post generation
# =============================================================================

@router.post("/draft/dm")
async def draft_dm_endpoint(request: Request):
    _check_admin_key(request)
    try:
        body = await request.json()
        target_id = body.get("target_id")
        trigger = body.get("trigger") or body.get("trigger_context") or ""
        if not target_id or not trigger:
            raise HTTPException(status_code=400, detail="target_id and trigger (or trigger_context) required")

        from app.ops.tools.drafter import draft_dm
        result = await draft_dm(target_id, trigger)
        if "error" in result:
            detail = result["error"]
            if "Claude API" in detail:
                return {"status": "error", "detail": "Claude API unavailable — check ANTHROPIC_API_KEY or try again later", "raw": result.get("raw")}
            return {"status": "error", "detail": detail}
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Draft DM failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/draft/forum")
async def draft_forum_endpoint(request: Request):
    _check_admin_key(request)
    try:
        body = await request.json()
        forum = body.get("forum")
        topic = body.get("topic")
        if not forum or not topic:
            raise HTTPException(status_code=400, detail="forum and topic required")

        from app.ops.tools.drafter import draft_forum_post
        result = await draft_forum_post(
            forum=forum,
            topic=topic,
            target_id=body.get("target_id"),
            include_sii_data=body.get("include_sii_data", True),
        )
        if "error" in result:
            detail = result["error"]
            if "Claude API" in detail:
                return {"status": "error", "detail": "Claude API unavailable — check ANTHROPIC_API_KEY or try again later", "raw": result.get("raw")}
            return {"status": "error", "detail": detail}
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Draft forum post failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Discovery Scanner
# =============================================================================

@router.get("/discovery/scan")
async def scan_discovery_endpoint(
    request: Request,
    limit: int = Query(default=20, le=100),
    min_magnitude: float = Query(default=0.0),
):
    _check_admin_key(request)
    try:
        from app.ops.tools.discovery_scanner import scan_discovery
        result = scan_discovery(limit=limit, min_magnitude=min_magnitude)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Discovery scan failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Milestones — full tracker
# =============================================================================

@router.get("/milestones")
async def get_milestones(request: Request):
    _check_admin_key(request)
    try:
        from app.ops.tools.milestone_checker import check_all_milestones
        return check_all_milestones()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Milestone check failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Historical Backfill — scrape all existing content for a target
# =============================================================================

@router.post("/backfill")
async def backfill_target(request: Request):
    """
    Use Parallel Search to find all existing content for a target,
    then scrape + analyze each URL.
    """
    _check_admin_key(request)
    try:
        body = await request.json()
        target_id = body.get("target_id")
        query = body.get("query")  # e.g., "all blog posts on kpk.io"
        max_results = body.get("max_results", 20)

        if not target_id or not query:
            raise HTTPException(status_code=400, detail="target_id and query required")

        from app.services import parallel_client
        from app.ops.tools.scraper import scrape_target
        from app.ops.tools.analyzer import analyze_content

        # Step 1: Search for URLs
        search_result = await parallel_client.search(query, num_results=max_results)
        if "error" in search_result:
            return {"status": "error", "detail": f"Search failed: {search_result['error']}"}

        # Parse URLs from search results
        urls = []
        results_data = search_result.get("results", search_result.get("search_results", []))
        if isinstance(results_data, list):
            for item in results_data:
                if isinstance(item, dict):
                    url = item.get("url") or item.get("link") or item.get("source_url")
                    if url:
                        urls.append(url)
                elif isinstance(item, str):
                    urls.append(item)

        if not urls:
            return {"status": "ok", "scraped": 0, "message": "No URLs found for query"}

        # Step 2: Scrape + analyze each URL
        scraped = 0
        analyzed = 0
        errors = []
        for url in urls[:max_results]:
            try:
                content_id = await scrape_target(target_id, url, "blog")
                if content_id:
                    scraped += 1
                    analysis = await analyze_content(content_id)
                    if analysis:
                        analyzed += 1
            except Exception as e:
                errors.append({"url": url, "error": str(e)})

        return {
            "status": "ok",
            "query": query,
            "urls_found": len(urls),
            "scraped": scraped,
            "analyzed": analyzed,
            "errors": errors[:5],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Alerts
# =============================================================================

@router.get("/alerts")
async def get_alerts(request: Request, limit: int = Query(default=50, le=200)):
    _check_admin_key(request)
    try:
        from app.ops.tools.alerter import get_alert_log
        return {"alerts": get_alert_log(limit)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/alerts/config")
async def configure_alerts(request: Request):
    """Add or update alert channel config. Body: { channel, config, alert_types }"""
    _check_admin_key(request)
    try:
        body = await request.json()
        channel = body.get("channel")
        config = body.get("config", {})
        alert_types = body.get("alert_types", ["health_failure", "engagement_response", "milestone_change"])

        if channel not in ("telegram", "email"):
            raise HTTPException(status_code=400, detail="channel must be 'telegram' or 'email'")

        # Upsert
        existing = fetch_one("SELECT id FROM ops_alert_config WHERE channel = %s", (channel,))
        if existing:
            execute(
                "UPDATE ops_alert_config SET config = %s, alert_types = %s, enabled = TRUE WHERE id = %s",
                (json.dumps(config), alert_types, existing["id"]),
            )
        else:
            execute(
                "INSERT INTO ops_alert_config (channel, config, alert_types) VALUES (%s, %s, %s)",
                (channel, json.dumps(config), alert_types),
            )
        return {"status": "ok", "channel": channel}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/alerts/test")
async def test_alert(request: Request):
    """Send a test alert to verify configuration."""
    _check_admin_key(request)
    try:
        from app.ops.tools.alerter import send_alert
        result = await send_alert(
            "health_failure",
            "*TEST ALERT*\nBasis Operations Hub alert system is working.",
            {"test": True},
        )
        return {"status": "ok", "sent": result}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# CoinGecko News
# =============================================================================

@router.post("/news/scan")
async def scan_news_endpoint(request: Request):
    _check_admin_key(request)
    try:
        from app.ops.tools.news_monitor import scan_news
        result = await scan_news()
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"News scan failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/news/feed")
async def get_news_feed(
    request: Request,
    limit: int = Query(default=30, le=100),
    relevant_only: bool = Query(default=True),
):
    _check_admin_key(request)
    try:
        from app.ops.tools.news_monitor import get_recent_news
        return {"news": get_recent_news(limit, relevant_only)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/news/incidents")
async def get_incidents(request: Request, days: int = Query(default=7)):
    _check_admin_key(request)
    try:
        from app.ops.tools.news_monitor import get_incidents
        return {"incidents": get_incidents(days)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Analytics
# =============================================================================

@router.get("/analytics")
async def get_analytics(request: Request):
    _check_admin_key(request)
    try:
        from app.ops.tools.analytics import compute_analytics
        return compute_analytics()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analytics computation failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/analytics/compute")
async def compute_analytics_endpoint(request: Request):
    _check_admin_key(request)
    try:
        from app.ops.tools.analytics import compute_analytics
        return compute_analytics()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analytics computation failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Target Surfaces (for Tier 2 monitoring config)
# =============================================================================

@router.put("/targets/{target_id}/surfaces")
async def update_target_surfaces(request: Request, target_id: int):
    """Update surface_urls for a target. Body: { surfaces: [...urls] }"""
    _check_admin_key(request)
    try:
        body = await request.json()
        surfaces = body.get("surfaces", [])
        execute(
            "UPDATE ops_targets SET surface_urls = %s, updated_at = NOW() WHERE id = %s",
            (json.dumps(surfaces), target_id),
        )
        return {"status": "ok", "target_id": target_id, "surfaces": surfaces}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Twitter Monitoring
# =============================================================================

@router.post("/twitter/scan")
async def scan_twitter(request: Request, background_tasks: BackgroundTasks, target_id: Optional[int] = None):
    """Scan Twitter for recent tweets from target contacts' handles."""
    _check_admin_key(request)
    try:
        async def _run():
            from app.ops.tools.twitter_monitor import scan_target_tweets
            try:
                await scan_target_tweets(target_id=target_id)
            except Exception as e:
                logger.error(f"Twitter scan failed: {e}")

        import asyncio
        background_tasks.add_task(lambda: asyncio.run(_run()))
        return {"status": "accepted", "message": "Twitter scan running in background. GET /api/ops/twitter/feed for results."}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/twitter/feed")
async def get_twitter_feed(
    request: Request,
    target_id: Optional[int] = None,
    limit: int = Query(default=30, le=100),
):
    """Get recent tweets from ops_target_content."""
    _check_admin_key(request)
    try:
        from app.ops.tools.twitter_monitor import get_recent_tweets
        return {"tweets": get_recent_tweets(limit=limit, target_id=target_id)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/twitter/keywords")
async def scan_twitter_keywords(request: Request):
    """Search Twitter for stablecoin-related tweets by keyword."""
    _check_admin_key(request)
    try:
        body = await request.json()
        keywords = body.get("keywords")
        max_results = body.get("max_results", 20)
        from app.ops.tools.twitter_monitor import scan_keyword_tweets
        result = await scan_keyword_tweets(keywords=keywords, max_results=max_results)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Twitter keyword scan failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Governance Monitoring (Snapshot / Tally)
# =============================================================================

@router.post("/governance/scan")
async def scan_governance(request: Request, background_tasks: BackgroundTasks, target_id: Optional[int] = None):
    """Scan Snapshot + Tally for governance proposals from target DAOs."""
    _check_admin_key(request)
    try:
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        days_back = body.get("days_back", 14) if body else 14
        tid = body.get("target_id", target_id) if body else target_id

        async def _run():
            from app.ops.tools.governance_monitor import scan_all_governance
            try:
                await scan_all_governance(target_id=tid, days_back=days_back)
            except Exception as e:
                logger.error(f"Governance scan failed: {e}")

        import asyncio
        background_tasks.add_task(lambda: asyncio.run(_run()))
        return {"status": "accepted", "message": "Governance scan running in background. GET /api/ops/governance/feed for results."}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/governance/feed")
async def get_governance_feed(
    request: Request,
    target_id: Optional[int] = None,
    stablecoin_only: bool = Query(default=False),
    limit: int = Query(default=30, le=100),
):
    """Get recent governance proposals."""
    _check_admin_key(request)
    try:
        from app.ops.tools.governance_monitor import get_recent_proposals
        return {"proposals": get_recent_proposals(limit=limit, stablecoin_only=stablecoin_only, target_id=target_id)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/governance/snapshot/{space_id}")
async def get_snapshot_space(request: Request, space_id: str):
    """Get proposals for a specific Snapshot space."""
    _check_admin_key(request)
    try:
        from app.database import fetch_all as fa
        proposals = fa(
            """SELECT * FROM ops_governance_proposals
               WHERE platform = 'snapshot' AND space_or_org = %s
               ORDER BY fetched_at DESC LIMIT 20""",
            (space_id,),
        ) or []
        return {"space": space_id, "proposals": proposals}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Investor Content Monitoring
# =============================================================================

@router.post("/investors/content/scan")
async def scan_investor_content_endpoint(request: Request, background_tasks: BackgroundTasks):
    """Scan VC blogs and tweets for investor content."""
    _check_admin_key(request)
    try:
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        investor_id = body.get("investor_id") if body else None

        async def _run():
            from app.ops.tools.investor_monitor import scan_investor_content
            try:
                await scan_investor_content(investor_id=investor_id)
            except Exception as e:
                logger.error(f"Investor content scan failed: {e}")

        import asyncio
        background_tasks.add_task(lambda: asyncio.run(_run()))
        return {"status": "accepted", "message": "Investor content scan running in background. GET /api/ops/investors/content/feed for results."}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/investors/content/feed")
async def get_investor_content_feed(
    request: Request,
    investor_id: Optional[int] = None,
    analyzed_only: bool = Query(default=False),
    limit: int = Query(default=30, le=100),
):
    """Get recent investor content."""
    _check_admin_key(request)
    try:
        from app.ops.tools.investor_monitor import get_investor_content
        return {"content": get_investor_content(limit=limit, investor_id=investor_id, analyzed_only=analyzed_only)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/investors/content/{content_id}/analyze")
async def analyze_investor_content_endpoint(request: Request, content_id: int):
    """Analyze investor content for thesis alignment."""
    _check_admin_key(request)
    try:
        from app.ops.tools.investor_monitor import analyze_investor_content
        result = await analyze_investor_content(content_id)
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
        return {"content_id": content_id, "analysis": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Investor content analysis failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/investors/content/signals")
async def get_investor_timing_signals(request: Request, limit: int = Query(default=10, le=50)):
    """Get investor content with timing signals — high-priority outreach opportunities."""
    _check_admin_key(request)
    try:
        from app.ops.tools.investor_monitor import get_timing_signals
        return {"signals": get_timing_signals(limit=limit)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Migration 033 — run new tables
# =============================================================================

@router.post("/migrate/033")
async def run_migration_033(request: Request):
    _check_admin_key(request)
    try:
        from app.database import run_migration
        import os
        migration_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "migrations", "033_ops_session6_expansion.sql"
        )
        run_migration(migration_path)
        return {"status": "ok", "migration": "033_ops_session6_expansion"}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# ─── Chain Expansion Endpoints ────────────────────────────────────────


@router.get("/chain-candidates")
async def ops_chain_candidates(request: Request):
    """View chain expansion candidates and their specs."""
    _check_admin_key(request)
    try:
        from app.collectors.psi_collector import discover_chain_candidates

        candidates = discover_chain_candidates()

        # Check which have specs and serialise sets for JSON
        for c in candidates:
            spec_path = os.path.join("docs", "collector_specs", f"{c['chain'].lower()}_collector_spec.md")
            c["spec_exists"] = os.path.exists(spec_path)
            c["spec_path"] = spec_path if c["spec_exists"] else None
            c["protocols"] = list(c.get("protocols", []))[:20]
            c["stablecoins"] = list(c.get("stablecoins", []))

        return {"candidates": candidates, "count": len(candidates)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/chain-expand")
async def ops_chain_expand(request: Request):
    """Trigger chain discovery + spec generation manually."""
    _check_admin_key(request)
    try:
        from app.collectors.psi_collector import run_chain_discovery
        result = run_chain_discovery()
        return result
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/chain-spec/{chain}")
async def ops_chain_spec(request: Request, chain: str):
    """Read a generated chain collector spec."""
    _check_admin_key(request)
    try:
        spec_path = os.path.join("docs", "collector_specs", f"{chain.lower()}_collector_spec.md")
        if not os.path.exists(spec_path):
            raise HTTPException(status_code=404, detail=f"No spec for {chain}. Run chain discovery first.")
        with open(spec_path, "r") as f:
            return {"chain": chain, "spec": f.read()}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# State Growth
# =============================================================================

@router.get("/state-growth")
async def state_growth(request: Request, days: int = Query(default=14, ge=1, le=90)):
    """Day-over-day state accumulation from daily pulses."""
    _check_admin_key(request)
    try:
        rows = fetch_all(
            """SELECT pulse_date, summary
               FROM daily_pulses
               ORDER BY pulse_date DESC
               LIMIT %s""",
            (days,),
        )
        if not rows:
            return {"days": [], "summary": {}}

        # Parse summaries, newest first
        parsed = []
        for r in rows:
            s = r["summary"]
            if isinstance(s, str):
                s = json.loads(s)
            parsed.append({"date": str(r["pulse_date"]), "summary": s})

        # Build the list of tracked fields from state_accumulation + network_state
        def _extract_fields(summary: dict) -> dict:
            fields = {}
            sa = summary.get("state_accumulation", {})
            ns = summary.get("network_state", {})
            # state_accumulation fields (except total_records, computed separately)
            for k, v in sa.items():
                if k != "total_records":
                    fields[k] = v if isinstance(v, (int, float)) else 0
            # network_state fields
            for k in ("wallets_indexed", "wallets_scored", "edge_count",
                       "stablecoins_scored", "protocols_scored"):
                fields[k] = ns.get(k, 0) if isinstance(ns.get(k), (int, float)) else 0
            return fields

        # Build day entries (newest first)
        day_entries = []
        for i, p in enumerate(parsed):
            fields = _extract_fields(p["summary"])
            sa = p["summary"].get("state_accumulation", {})
            total = sa.get("total_records", sum(fields.values()))

            # Delta vs previous day (next in list since sorted desc)
            prev_fields = {}
            prev_total = 0
            if i + 1 < len(parsed):
                prev_fields = _extract_fields(parsed[i + 1]["summary"])
                prev_sa = parsed[i + 1]["summary"].get("state_accumulation", {})
                prev_total = prev_sa.get("total_records", sum(prev_fields.values()))

            breakdown = {}
            for k, v in fields.items():
                breakdown[k] = {
                    "value": v,
                    "delta": v - prev_fields.get(k, 0) if prev_fields else None,
                }

            day_entries.append({
                "date": p["date"],
                "total_records": total,
                "delta": total - prev_total if prev_fields else None,
                "breakdown": breakdown,
            })

        # Summary stats
        newest = day_entries[0] if day_entries else {}
        total_now = newest.get("total_records", 0)

        # 7-day growth
        growth_7d = 0
        if len(day_entries) > 1:
            window = day_entries[:min(8, len(day_entries))]
            oldest_in_window = window[-1].get("total_records", 0)
            growth_7d = total_now - oldest_in_window

        # Average daily growth
        deltas = [d["delta"] for d in day_entries if d.get("delta") is not None]
        avg_daily = round(sum(deltas) / len(deltas)) if deltas else 0

        # Fastest growing and stalled fields
        if len(day_entries) >= 2 and day_entries[0].get("breakdown"):
            field_totals = {}
            for d in day_entries:
                for k, v in d.get("breakdown", {}).items():
                    if v.get("delta") is not None:
                        field_totals[k] = field_totals.get(k, 0) + v["delta"]
            fastest = max(field_totals, key=field_totals.get) if field_totals else None
            stalled = sorted([k for k, v in field_totals.items() if v == 0])
        else:
            fastest = None
            stalled = []

        # Treasury status
        treasury_status = {}
        try:
            reg_count = fetch_one(
                "SELECT COUNT(*) as cnt FROM wallet_graph.treasury_registry WHERE monitoring_enabled = TRUE"
            )
            total_events = fetch_one(
                "SELECT COUNT(*) as cnt FROM wallet_graph.treasury_events"
            )
            events_24h = fetch_one(
                "SELECT COUNT(*) as cnt FROM wallet_graph.treasury_events WHERE detected_at > NOW() - INTERVAL '24 hours'"
            )
            severity_breakdown = fetch_all(
                "SELECT severity, COUNT(*) as cnt FROM wallet_graph.treasury_events GROUP BY severity ORDER BY severity"
            ) or []
            treasury_status = {
                "registered_treasuries": reg_count["cnt"] if reg_count else 0,
                "total_events": total_events["cnt"] if total_events else 0,
                "events_24h": events_24h["cnt"] if events_24h else 0,
                "by_severity": {r["severity"]: r["cnt"] for r in severity_breakdown},
            }
        except Exception as e:
            treasury_status = {"error": str(e)}

        return {
            "days": day_entries,
            "summary": {
                "total_records_now": total_now,
                "total_growth_7d": growth_7d,
                "avg_daily_growth": avg_daily,
                "fastest_growing": fastest,
                "stalled": stalled,
            },
            "treasury": treasury_status,
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


# =============================================================================
# Keeper wallet monitor
# =============================================================================

@router.get("/keeper/status")
async def keeper_status(request: Request):
    """Keeper wallet balance and gas burn across Base and Arbitrum."""
    _check_admin_key(request)
    try:
        from app.ops.tools.keeper_monitor import get_keeper_status
        result = await get_keeper_status()
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Keeper status error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/keeper/history")
async def keeper_history(request: Request, limit: int = Query(default=50, ge=1, le=200)):
    """Recent keeper transactions with gas costs."""
    _check_admin_key(request)
    try:
        from app.ops.tools.keeper_monitor import get_keeper_history
        result = await get_keeper_history(limit=limit)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Keeper history error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/coverage-report")
async def coverage_report(request: Request):
    """Data availability report — what's scored, what's blocked, and why."""
    _check_admin_key(request)
    try:
        from datetime import timezone

        # SII coverage
        sii_discovered = fetch_one(
            "SELECT COUNT(*) as c FROM wallet_graph.unscored_assets WHERE token_type = 'stablecoin' AND coingecko_id IS NOT NULL"
        )
        sii_scored = fetch_one(
            "SELECT COUNT(*) as c FROM stablecoins WHERE scoring_enabled = TRUE"
        )
        sii_scores = fetch_all(
            "SELECT s.component_count FROM scores s"
        ) or []

        sii_components_total = 39  # from COMPONENT_NORMALIZATIONS
        sii_by_confidence = {"high": 0, "standard": 0, "limited": 0}
        for row in sii_scores:
            cc = row.get("component_count") or 0
            cov = cc / max(sii_components_total, 1)
            if cov >= 0.80:
                sii_by_confidence["high"] += 1
            elif cov >= 0.60:
                sii_by_confidence["standard"] += 1
            else:
                sii_by_confidence["limited"] += 1

        # SII blocked analysis — check which categories are commonly missing
        # We look at unscored assets that have been attempted but skipped
        sii_category_gaps = {}
        try:
            # Check recent component_readings for non-scored stablecoins
            gap_rows = fetch_all("""
                SELECT cr.stablecoin_id, cr.category, COUNT(*) as cnt
                FROM component_readings cr
                LEFT JOIN scores s ON s.stablecoin_id = cr.stablecoin_id
                WHERE s.stablecoin_id IS NULL
                  AND cr.collected_at > NOW() - INTERVAL '7 days'
                GROUP BY cr.stablecoin_id, cr.category
            """) or []
            # Invert: find which v1 categories are missing for each stablecoin
            # This is approximate — just report totals
        except Exception:
            pass

        # PSI coverage
        psi_discovered = fetch_one(
            "SELECT COUNT(*) as c FROM protocol_backlog"
        )
        psi_scored_count = fetch_one(
            "SELECT COUNT(DISTINCT protocol_slug) as c FROM psi_scores"
        )
        psi_category_complete = fetch_one(
            "SELECT COUNT(*) as c FROM protocol_backlog WHERE enrichment_status IN ('ready', 'promoted')"
        )
        psi_scores = fetch_all(
            "SELECT DISTINCT ON (protocol_slug) component_scores FROM psi_scores ORDER BY protocol_slug, computed_at DESC"
        ) or []

        psi_components_total = 27  # from PSI_V01_DEFINITION
        psi_by_confidence = {"high": 0, "standard": 0, "limited": 0}
        for row in psi_scores:
            cs = row.get("component_scores") or {}
            cov = len(cs) / max(psi_components_total, 1)
            if cov >= 0.80:
                psi_by_confidence["high"] += 1
            elif cov >= 0.60:
                psi_by_confidence["standard"] += 1
            else:
                psi_by_confidence["limited"] += 1

        # PSI blocked — which categories are most commonly missing
        psi_blocked = {}
        try:
            blocked_rows = fetch_all("""
                SELECT slug, name, coverage_pct, components_available, components_total
                FROM protocol_backlog
                WHERE enrichment_status IN ('discovered', 'enriching')
                ORDER BY stablecoin_exposure_usd DESC LIMIT 50
            """) or []
            # Just report count
            psi_blocked["count"] = len(blocked_rows)
        except Exception:
            psi_blocked["count"] = 0

        # CQI pairs
        sii_count = len(sii_scores)
        psi_count = psi_scored_count["c"] if psi_scored_count else 0
        cqi_pairs = sii_count * psi_count

        return {
            "sii": {
                "discovered": sii_discovered["c"] if sii_discovered else 0,
                "scored": len(sii_scores),
                "scoring_enabled": sii_scored["c"] if sii_scored else 0,
                "by_confidence": sii_by_confidence,
                "gate": "category_completeness",
                "gate_description": "Every v1 category (peg, liquidity, flows, distribution, structural) must have >= 1 populated component",
            },
            "psi": {
                "discovered": psi_discovered["c"] if psi_discovered else 0,
                "category_complete": psi_category_complete["c"] if psi_category_complete else 0,
                "scored": psi_count,
                "by_confidence": psi_by_confidence,
                "blocked": psi_blocked,
                "gate": "category_completeness",
                "gate_description": "Every PSI category (balance_sheet, revenue, liquidity, security, governance, token_health) must have >= 1 populated component",
            },
            "cqi_pairs": cqi_pairs,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.error(f"Coverage report error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/protocol/{slug}/deep-dive")
async def protocol_deep_dive(slug: str, request: Request):
    """Aggregated deep dive for a single protocol — ops-only."""
    _check_admin_key(request)
    try:
        from datetime import timezone
        from app.scoring_engine import compute_confidence_tag

        # PSI score
        psi_row = fetch_one("""
            SELECT id, protocol_slug, protocol_name, overall_score, grade,
                   category_scores, component_scores, raw_values,
                   formula_version, computed_at
            FROM psi_scores
            WHERE protocol_slug = %s
            ORDER BY computed_at DESC LIMIT 1
        """, (slug,))
        if not psi_row:
            raise HTTPException(status_code=404, detail=f"Protocol '{slug}' not found in PSI scores")

        from app.index_definitions.psi_v01 import PSI_V01_DEFINITION
        cat_scores = psi_row.get("category_scores") or {}
        comp_scores = psi_row.get("component_scores") or {}
        raw_vals = psi_row.get("raw_values") or {}
        psi_comps_total = len(PSI_V01_DEFINITION["components"])
        psi_coverage = round(len(comp_scores) / max(psi_comps_total, 1), 2)
        psi_missing = sorted(set(PSI_V01_DEFINITION["categories"].keys()) - set(cat_scores.keys()))
        psi_conf = compute_confidence_tag(
            len(PSI_V01_DEFINITION["categories"]) - len(psi_missing),
            len(PSI_V01_DEFINITION["categories"]),
            psi_coverage, psi_missing
        )

        # Category breakdown with component details
        category_breakdown = {}
        for cat_id, cat_def in PSI_V01_DEFINITION["categories"].items():
            cat_comps = {
                cid: cdef for cid, cdef in PSI_V01_DEFINITION["components"].items()
                if cdef["category"] == cat_id
            }
            components_detail = []
            for cid, cdef in cat_comps.items():
                components_detail.append({
                    "id": cid,
                    "name": cdef.get("name", cid),
                    "weight": cdef.get("weight", 0),
                    "raw_value": raw_vals.get(cid),
                    "normalized_score": comp_scores.get(cid),
                    "data_source": cdef.get("data_source", "unknown"),
                    "status": "available" if cid in comp_scores else "unavailable",
                })
            populated = sum(1 for c in components_detail if c["status"] == "available")
            category_breakdown[cat_id] = {
                "name": cat_def["name"] if isinstance(cat_def, dict) else cat_id,
                "weight": cat_def["weight"] if isinstance(cat_def, dict) else 0,
                "score": cat_scores.get(cat_id),
                "components_populated": populated,
                "components_total": len(cat_comps),
                "components": components_detail,
            }

        # Score history (last 30 days)
        score_history = fetch_all("""
            SELECT overall_score, grade, category_scores, computed_at, scored_date
            FROM psi_scores
            WHERE protocol_slug = %s
            ORDER BY computed_at DESC LIMIT 30
        """, (slug,)) or []

        # Stablecoin exposure (treasury + collateral)
        treasury = []
        try:
            treasury = fetch_all("""
                SELECT token_symbol, token_address, usd_value, is_stablecoin,
                       sii_score, sii_grade
                FROM protocol_treasury_holdings
                WHERE protocol_slug = %s
                ORDER BY usd_value DESC
            """, (slug,)) or []
        except Exception:
            pass

        collateral = []
        try:
            collateral = fetch_all("""
                SELECT stablecoin_symbol, tvl_usd, pool_count
                FROM protocol_collateral_exposure
                WHERE protocol_slug = %s
                ORDER BY tvl_usd DESC
            """, (slug,)) or []
        except Exception:
            pass

        # CQI matrix row for this protocol
        cqi_row = []
        try:
            from app.composition import compose_geometric_mean, _sii_confidence, _psi_confidence, _lower_confidence
            psi_score_val = float(psi_row["overall_score"]) if psi_row.get("overall_score") else None
            psi_c = _psi_confidence(comp_scores)
            stablecoins = fetch_all("""
                SELECT st.symbol, s.overall_score, s.grade, s.component_count
                FROM scores s JOIN stablecoins st ON st.id = s.stablecoin_id
                WHERE s.overall_score IS NOT NULL
                ORDER BY s.overall_score DESC
            """) or []
            for coin in stablecoins:
                sii = float(coin["overall_score"]) if coin.get("overall_score") else None
                if sii and psi_score_val:
                    cqi = compose_geometric_mean([sii, psi_score_val])
                    sii_c = _sii_confidence(coin.get("component_count") or 0)
                    cqi_c = _lower_confidence(sii_c, psi_c)
                    cqi_row.append({
                        "asset": coin["symbol"],
                        "sii_score": sii,
                        "sii_grade": coin["grade"],
                        "sii_confidence": sii_c["confidence"],
                        "psi_score": psi_score_val,
                        "cqi_score": cqi,
                        "cqi_grade": score_to_grade(cqi) if cqi else None,
                        "cqi_confidence": cqi_c["confidence"],
                    })
            cqi_row.sort(key=lambda x: x.get("cqi_score", 0), reverse=True)
        except Exception as e:
            logger.debug(f"CQI matrix row failed for {slug}: {e}")

        # Discovery signals
        discovery_signals = []
        try:
            discovery_signals = fetch_all("""
                SELECT signal_type, severity, details, discovered_at
                FROM discovery_signals
                WHERE entity_type = 'protocol' AND entity_id = %s
                ORDER BY discovered_at DESC LIMIT 10
            """, (slug,)) or []
        except Exception:
            pass

        from app.scoring import score_to_grade

        return {
            "protocol_slug": slug,
            "protocol_name": psi_row["protocol_name"],
            "score": float(psi_row["overall_score"]) if psi_row.get("overall_score") else None,
            "grade": psi_row["grade"],
            "confidence": psi_conf["confidence"],
            "confidence_tag": psi_conf["tag"],
            "missing_categories": psi_conf["missing_categories"],
            "component_coverage": psi_coverage,
            "components_populated": len(comp_scores),
            "components_total": psi_comps_total,
            "formula_version": psi_row.get("formula_version"),
            "computed_at": psi_row["computed_at"].isoformat() if psi_row.get("computed_at") else None,
            "category_breakdown": category_breakdown,
            "score_history": [
                {
                    "score": float(h["overall_score"]) if h.get("overall_score") else None,
                    "grade": h["grade"],
                    "category_scores": h.get("category_scores"),
                    "date": str(h["scored_date"]) if h.get("scored_date") else (h["computed_at"].isoformat() if h.get("computed_at") else None),
                }
                for h in score_history
            ],
            "stablecoin_exposure": {
                "treasury": [dict(t) for t in treasury],
                "collateral": [dict(c) for c in collateral],
            },
            "cqi_matrix_row": cqi_row,
            "risk_summary": {
                "lowest_category": min(cat_scores.items(), key=lambda x: x[1])[0] if cat_scores else None,
                "lowest_category_score": min(cat_scores.values()) if cat_scores else None,
            },
            "discovery_signals": [dict(s) for s in discovery_signals],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Protocol deep-dive error for {slug}: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/seed-metrics")
async def seed_metrics(request: Request):
    """Aggregated metrics for seed fundraise conversations. No auth required for read-only summary."""
    try:
        from datetime import timezone

        # Real-time: today's requests
        today = fetch_one(
            "SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE is_internal = FALSE) as external FROM api_request_log WHERE timestamp >= NOW() - INTERVAL '1 day'"
        )

        # 7-day trend from rollup table
        trend_7d = fetch_all(
            "SELECT date, total_api_requests, external_api_requests, unique_external_ips, mcp_tool_calls, jsonld_requests FROM metrics_daily_rollup ORDER BY date DESC LIMIT 7"
        ) or []

        # 30-day totals
        month = fetch_one(
            "SELECT SUM(total_api_requests) as total, SUM(external_api_requests) as external, SUM(mcp_tool_calls) as mcp_tools FROM metrics_daily_rollup WHERE date >= NOW()::date - INTERVAL '30 days'"
        )

        # Active API keys
        active_keys = fetch_one(
            "SELECT COUNT(DISTINCT api_key_id) as c FROM api_request_log WHERE timestamp > NOW() - INTERVAL '7 days' AND api_key_id IS NOT NULL"
        )

        # MCP tool breakdown (last 7 days)
        mcp_tools = fetch_all(
            "SELECT tool_name, COUNT(*) as calls FROM mcp_tool_calls WHERE timestamp > NOW() - INTERVAL '7 days' GROUP BY tool_name ORDER BY calls DESC"
        ) or []

        # Oracle keeper publishes (last 7 days)
        keeper = fetch_all(
            "SELECT chain, COUNT(*) as publishes, MAX(timestamp) as last_publish FROM keeper_publish_log WHERE timestamp > NOW() - INTERVAL '7 days' GROUP BY chain"
        ) or []

        # Top external consumers (by IP, last 7 days)
        top_consumers = fetch_all(
            """SELECT ip_address, LEFT(user_agent, 80) as ua, COUNT(*) as requests
               FROM api_request_log
               WHERE timestamp > NOW() - INTERVAL '7 days' AND is_internal = FALSE
               GROUP BY ip_address, LEFT(user_agent, 80)
               ORDER BY requests DESC LIMIT 10"""
        ) or []

        # Most queried entities
        top_entities = fetch_all(
            """SELECT entity_type, entity_id, COUNT(*) as lookups
               FROM api_request_log
               WHERE timestamp > NOW() - INTERVAL '7 days' AND entity_id IS NOT NULL AND is_internal = FALSE
               GROUP BY entity_type, entity_id
               ORDER BY lookups DESC LIMIT 15"""
        ) or []

        # Channels live count
        channels_live = {
            "api": True,
            "dashboard": True,
            "mcp_server": True,
            "oracle_base": bool(os.environ.get("BASE_ORACLE_ADDRESS")),
            "oracle_arbitrum": bool(os.environ.get("ARBITRUM_ORACLE_ADDRESS")),
            "assessment_pages": True,
            "tradingview": True,
            "snap": "audit_queue",
            "safe_guard": "built",
            "social_bots": False,
            "dune_dashboard": False,
            "weekly_digest": False,
        }

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "realtime": {
                "requests_today": today["total"] if today else 0,
                "external_requests_today": today["external"] if today else 0,
            },
            "trend_7d": [dict(r) for r in trend_7d] if trend_7d else [],
            "month_totals": {
                "total_requests": int(month["total"]) if month and month["total"] else 0,
                "external_requests": int(month["external"]) if month and month["external"] else 0,
                "mcp_tool_calls": int(month["mcp_tools"]) if month and month["mcp_tools"] else 0,
            },
            "active_api_keys_7d": active_keys["c"] if active_keys else 0,
            "mcp_tool_breakdown": [dict(r) for r in mcp_tools],
            "keeper_publishes": [dict(r) for r in keeper],
            "top_external_consumers": [dict(r) for r in top_consumers],
            "top_entities": [dict(r) for r in top_entities],
            "channels": channels_live,
            "channels_live_count": sum(1 for v in channels_live.values() if v is True),
        }
    except Exception as e:
        logger.error(f"Seed metrics computation failed: {e}")
        return {"error": str(e)}


# =============================================================================
# ABM Engine — Account-Based Marketing campaigns
# =============================================================================

from app.ops.abm_config import ABM_ICP_TYPES, ABM_DRIP_TEMPLATES, ABM_STATE_LABELS


@router.get("/abm/config")
async def abm_config(request: Request):
    _check_admin_key(request)
    try:
        return {
            "icp_types": ABM_ICP_TYPES,
            "state_labels": ABM_STATE_LABELS,
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/abm/campaigns")
async def abm_list_campaigns(request: Request):
    _check_admin_key(request)
    try:
        rows = fetch_all(
            "SELECT * FROM abm_campaigns ORDER BY updated_at DESC", None
        )
        campaigns = []
        for r in rows:
            c = dict(r)
            # Parse JSONB fields if they come back as strings
            for k in ("stablecoins", "lenses", "pain_points"):
                if isinstance(c.get(k), str):
                    c[k] = json.loads(c[k])
            campaigns.append(c)
        return {"campaigns": campaigns}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.get("/abm/campaigns/{campaign_id}")
async def abm_get_campaign(campaign_id: int, request: Request):
    _check_admin_key(request)
    try:
        campaign = fetch_one("SELECT * FROM abm_campaigns WHERE id = %s", (campaign_id,))
        if not campaign:
            return JSONResponse(status_code=404, content={"error": "Campaign not found"})
        c = dict(campaign)
        for k in ("stablecoins", "lenses", "pain_points"):
            if isinstance(c.get(k), str):
                c[k] = json.loads(c[k])

        touches = fetch_all(
            "SELECT * FROM abm_drip_touches WHERE campaign_id = %s ORDER BY day, id", (campaign_id,)
        )
        log = fetch_all(
            "SELECT * FROM abm_touch_log WHERE campaign_id = %s ORDER BY created_at DESC", (campaign_id,)
        )
        return {
            "campaign": c,
            "touches": [dict(t) for t in touches],
            "log": [dict(l) for l in log],
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/abm/campaigns")
async def abm_create_campaign(request: Request):
    _check_admin_key(request)
    try:
        body = await request.json()
        mode = body.get("mode", "icp")
        icp_type = body.get("icp_type", "")
        org = body.get("org", "")
        person = body.get("person")
        title = body.get("title")
        stablecoins = body.get("stablecoins", [])
        lenses = body.get("lenses", [])
        pain_points = body.get("pain_points", [])
        entry_piece = body.get("entry_piece")
        named_target_id = body.get("named_target_id")

        if not org:
            return JSONResponse(status_code=400, content={"error": "org is required"})
        if not icp_type:
            return JSONResponse(status_code=400, content={"error": "icp_type is required"})

        # Fill defaults from ICP config if not provided
        icp_cfg = ABM_ICP_TYPES.get(icp_type, {})
        if not stablecoins:
            stablecoins = icp_cfg.get("default_coins", [])
        if not lenses:
            lenses = icp_cfg.get("lenses", [])
        if not pain_points:
            pain_points = icp_cfg.get("pain_points", [])

        # Pre-generate report for the first stablecoin + first lens
        report_hash = None
        if stablecoins:
            try:
                from app.report import assemble_report_data
                report_data = assemble_report_data("stablecoin", stablecoins[0])
                if report_data:
                    import hashlib
                    report_json = json.dumps(report_data, sort_keys=True, default=str)
                    report_hash = hashlib.sha256(report_json.encode()).hexdigest()[:16]
            except Exception as re:
                logger.warning(f"ABM report pre-generation failed: {re}")

        # Insert campaign
        execute(
            """INSERT INTO abm_campaigns
               (mode, icp_type, org, person, title, stablecoins, lenses, pain_points,
                entry_piece, state, named_target_id, report_hash)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s)""",
            (mode, icp_type, org, person, title,
             json.dumps(stablecoins), json.dumps(lenses), json.dumps(pain_points),
             entry_piece, named_target_id, report_hash)
        )

        # Fetch the newly created campaign
        campaign = fetch_one(
            "SELECT * FROM abm_campaigns WHERE org = %s AND icp_type = %s ORDER BY id DESC LIMIT 1",
            (org, icp_type)
        )
        campaign_id = campaign["id"]

        # Insert drip touches from template
        drip_template = ABM_DRIP_TEMPLATES.get(icp_type, [])
        first_coin = stablecoins[0] if stablecoins else ""
        for touch in drip_template:
            subj = touch["subj"].replace("{org}", org).replace("{coin}", first_coin)
            desc = touch.get("desc", "")
            execute(
                """INSERT INTO abm_drip_touches
                   (campaign_id, day, channel, subject, description, is_gate)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (campaign_id, touch["day"], touch["ch"], subj, desc, touch["gate"])
            )

        # Add initial touch log entry
        execute(
            "INSERT INTO abm_touch_log (campaign_id, note) VALUES (%s, %s)",
            (campaign_id, f"Campaign created — {icp_type} for {org}")
        )

        return {"status": "ok", "campaign_id": campaign_id, "report_hash": report_hash}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.put("/abm/campaigns/{campaign_id}/state")
async def abm_update_state(campaign_id: int, request: Request):
    _check_admin_key(request)
    try:
        body = await request.json()
        new_state = body.get("state")
        if new_state is None or not isinstance(new_state, int) or new_state < 0 or new_state > 8:
            return JSONResponse(status_code=400, content={"error": "state must be integer 0-8"})

        campaign = fetch_one("SELECT id, state FROM abm_campaigns WHERE id = %s", (campaign_id,))
        if not campaign:
            return JSONResponse(status_code=404, content={"error": "Campaign not found"})

        old_state = campaign["state"]
        execute(
            "UPDATE abm_campaigns SET state = %s, updated_at = NOW() WHERE id = %s",
            (new_state, campaign_id)
        )

        old_label = ABM_STATE_LABELS.get(old_state, str(old_state))
        new_label = ABM_STATE_LABELS.get(new_state, str(new_state))
        execute(
            "INSERT INTO abm_touch_log (campaign_id, note) VALUES (%s, %s)",
            (campaign_id, f"State changed: {old_label} -> {new_label}")
        )

        return {"status": "ok", "old_state": old_state, "new_state": new_state}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/abm/campaigns/{campaign_id}/log")
async def abm_add_log(campaign_id: int, request: Request):
    _check_admin_key(request)
    try:
        body = await request.json()
        note = body.get("note", "").strip()
        if not note:
            return JSONResponse(status_code=400, content={"error": "note is required"})

        campaign = fetch_one("SELECT id FROM abm_campaigns WHERE id = %s", (campaign_id,))
        if not campaign:
            return JSONResponse(status_code=404, content={"error": "Campaign not found"})

        execute(
            "INSERT INTO abm_touch_log (campaign_id, note) VALUES (%s, %s)",
            (campaign_id, note)
        )
        execute(
            "UPDATE abm_campaigns SET updated_at = NOW() WHERE id = %s",
            (campaign_id,)
        )
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.put("/abm/drip/{touch_id}")
async def abm_update_drip(touch_id: int, request: Request):
    _check_admin_key(request)
    try:
        body = await request.json()
        status = body.get("status", "")
        response_text = body.get("response")
        if status not in ("pending", "sent", "skipped"):
            return JSONResponse(status_code=400, content={"error": "status must be pending, sent, or skipped"})

        touch = fetch_one("SELECT * FROM abm_drip_touches WHERE id = %s", (touch_id,))
        if not touch:
            return JSONResponse(status_code=404, content={"error": "Touch not found"})

        sent_at = "NOW()" if status == "sent" else "NULL"
        if response_text is not None:
            execute(
                f"UPDATE abm_drip_touches SET status = %s, sent_at = {sent_at}, response = %s WHERE id = %s",
                (status, response_text, touch_id)
            )
        else:
            execute(
                f"UPDATE abm_drip_touches SET status = %s, sent_at = {sent_at} WHERE id = %s",
                (status, touch_id)
            )

        # Log the touch action
        campaign_id = touch["campaign_id"]
        execute(
            "INSERT INTO abm_touch_log (campaign_id, note) VALUES (%s, %s)",
            (campaign_id, f"Drip touch day {touch['day']} ({touch['channel']}): {status}")
        )
        execute(
            "UPDATE abm_campaigns SET updated_at = NOW() WHERE id = %s",
            (campaign_id,)
        )
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.delete("/abm/campaigns/{campaign_id}")
async def abm_delete_campaign(campaign_id: int, request: Request):
    _check_admin_key(request)
    try:
        campaign = fetch_one("SELECT id, org FROM abm_campaigns WHERE id = %s", (campaign_id,))
        if not campaign:
            return JSONResponse(status_code=404, content={"error": "Campaign not found"})

        # CASCADE handles drip_touches and touch_log
        execute("DELETE FROM abm_campaigns WHERE id = %s", (campaign_id,))
        return {"status": "ok", "deleted": campaign_id}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


@router.post("/migrate/049")
async def run_migration_049(request: Request):
    _check_admin_key(request)
    try:
        from app.database import run_migration
        migration_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "migrations", "049_abm_campaigns.sql"
        )
        run_migration(migration_path)
        return {"status": "ok", "migration": "049_abm_campaigns"}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": _traceback_mod.format_exc()})


def register_ops_routes(app):
    """Register the ops router with the main FastAPI app."""
    app.include_router(router)
    logger.info("Operations Hub routes registered")
