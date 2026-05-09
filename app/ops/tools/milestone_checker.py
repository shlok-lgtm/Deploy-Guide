"""
Milestone checker — tracks seed triggers, kill signals, and adoption metrics
against live database state.
"""
import logging
from datetime import datetime, timezone
from app.database import fetch_one, fetch_all

logger = logging.getLogger(__name__)


def check_all_milestones() -> dict:
    """
    Compute all milestone categories from live data.
    Returns structured dict with seed_triggers, kill_signals, adoption_metrics.
    """
    return {
        "seed_triggers": _check_seed_triggers(),
        "kill_signals": _check_kill_signals(),
        "adoption_metrics": _check_adoption_metrics(),
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


def _check_seed_triggers() -> dict:
    """Check the 5 seed trigger milestones. Need 3/5 to activate fundraise."""
    milestones = []

    # 1. 8+ renderers live
    # Renderers are tracked via different systems — check what's available
    renderer_count = None
    try:
        # Check if there's a renderer registry or count from API keys
        row = fetch_one(
            "SELECT COUNT(DISTINCT description) as cnt FROM api_keys WHERE description ILIKE '%renderer%'"
        )
        renderer_count = row["cnt"] if row else 0
    except Exception as e:
        logger.warning(f"milestone_checker: renderer count query failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="ops_milestone_seed_renderer_count_failure",
                error_message=str(e)[:500],
                cycle_phase="ops_milestone_checker",
            )
        except Exception:
            pass
        renderer_count = None
    milestones.append({
        "name": "8+ renderers live",
        "target": 8,
        "current": renderer_count,
        "met": renderer_count is not None and renderer_count >= 8,
        "auto": renderer_count is not None,
        "source": "api_keys (renderer descriptions)",
    })

    # 2. API >500 external requests/day
    api_daily = 0
    try:
        row = fetch_one(
            """SELECT COUNT(*) as cnt FROM api_request_log
               WHERE created_at > NOW() - INTERVAL '24 hours'
               AND api_key_id IS NOT NULL"""
        )
        api_daily = row["cnt"] if row else 0
    except Exception as e:
        logger.warning(f"milestone_checker: api_daily query failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="ops_milestone_seed_api_daily_failure",
                error_message=str(e)[:500],
                cycle_phase="ops_milestone_checker",
            )
        except Exception:
            pass
    milestones.append({
        "name": "API >500 external requests/day",
        "target": 500,
        "current": api_daily,
        "met": api_daily > 500,
        "auto": True,
        "source": "api_request_log",
    })

    # 3. Protocol teams citing scores
    citations = 0
    try:
        row = fetch_one(
            """SELECT COUNT(*) as cnt FROM ops_target_engagement_log
               WHERE action_type IN ('comment_posted', 'forum_posted')
               AND response IS NOT NULL AND response != ''"""
        )
        citations = row["cnt"] if row else 0
    except Exception as e:
        logger.warning(f"milestone_checker: citations query failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="ops_milestone_seed_citations_failure",
                error_message=str(e)[:500],
                cycle_phase="ops_milestone_checker",
            )
        except Exception:
            pass
    milestones.append({
        "name": "Protocol teams citing scores",
        "target": 1,
        "current": citations,
        "met": citations > 0,
        "auto": False,
        "source": "ops_target_engagement_log (responses to our posts)",
    })

    # 4. Snap submitted for audit (manual flag)
    milestones.append({
        "name": "Snap submitted for audit",
        "target": 1,
        "current": 0,
        "met": False,
        "auto": False,
        "source": "manual",
    })

    # 5. DAO pilot in conversation
    pilots = 0
    try:
        row = fetch_one(
            """SELECT COUNT(*) as cnt FROM ops_targets
               WHERE tier <= 2
               AND pipeline_stage IN ('evaluating', 'trying', 'binding')"""
        )
        pilots = row["cnt"] if row else 0
    except Exception as e:
        logger.warning(f"milestone_checker: pilots query failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="ops_milestone_seed_pilots_failure",
                error_message=str(e)[:500],
                cycle_phase="ops_milestone_checker",
            )
        except Exception:
            pass
    milestones.append({
        "name": "DAO pilot in conversation",
        "target": 1,
        "current": pilots,
        "met": pilots > 0,
        "auto": True,
        "source": "ops_targets pipeline_stage",
    })

    met_count = sum(1 for m in milestones if m["met"])
    return {
        "milestones": milestones,
        "met": met_count,
        "total": len(milestones),
        "threshold": 3,
        "activated": met_count >= 3,
    }


def _check_kill_signals() -> dict:
    """
    Check kill signal conditions from the Constitution.
    M6/M9/M12/M18 checkpoints.
    """
    signals = []

    # M6: <100 daily external API + no protocol references
    api_daily = 0
    try:
        row = fetch_one(
            """SELECT COUNT(*) as cnt FROM api_request_log
               WHERE created_at > NOW() - INTERVAL '24 hours'
               AND api_key_id IS NOT NULL"""
        )
        api_daily = row["cnt"] if row else 0
    except Exception as e:
        logger.warning(f"milestone_checker: kill_signal api_daily query failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="ops_milestone_kill_api_daily_failure",
                error_message=str(e)[:500],
                cycle_phase="ops_milestone_checker",
            )
        except Exception:
            pass

    protocol_refs = 0
    try:
        row = fetch_one(
            """SELECT COUNT(*) as cnt FROM ops_target_engagement_log
               WHERE response IS NOT NULL AND response != ''"""
        )
        protocol_refs = row["cnt"] if row else 0
    except Exception as e:
        logger.warning(f"milestone_checker: kill_signal protocol_refs query failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="ops_milestone_kill_protocol_refs_failure",
                error_message=str(e)[:500],
                cycle_phase="ops_milestone_checker",
            )
        except Exception:
            pass

    signals.append({
        "name": "M6: <100 API calls/day + no protocol references",
        "timeline": "M6",
        "conditions": {
            "api_calls_daily": api_daily,
            "api_threshold": 100,
            "protocol_references": protocol_refs,
        },
        "triggered": api_daily < 100 and protocol_refs == 0,
        "evaluable": True,  # Always evaluable from day 1
        "status": "safe" if api_daily >= 100 or protocol_refs > 0 else "at_risk",
    })

    # M9: CDA zero external consumers + no AI citations
    cda_consumers = 0
    try:
        row = fetch_one(
            """SELECT COUNT(DISTINCT api_key_id) as cnt FROM api_request_log
               WHERE path LIKE '/api/cda%'
               AND created_at > NOW() - INTERVAL '30 days'
               AND api_key_id IS NOT NULL"""
        )
        cda_consumers = row["cnt"] if row else 0
    except Exception as e:
        logger.warning(f"milestone_checker: cda_consumers query failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="ops_milestone_kill_cda_consumers_failure",
                error_message=str(e)[:500],
                cycle_phase="ops_milestone_checker",
            )
        except Exception:
            pass

    signals.append({
        "name": "M9: CDA zero external consumers + no AI citations",
        "timeline": "M9",
        "conditions": {
            "cda_external_consumers_30d": cda_consumers,
        },
        "triggered": cda_consumers == 0,
        "evaluable": True,
        "status": "safe" if cda_consumers > 0 else "at_risk",
    })

    # M12: No versions pinned + no third-party renderers
    signals.append({
        "name": "M12: No versions pinned + no third-party renderers",
        "timeline": "M12",
        "conditions": {},
        "triggered": False,
        "evaluable": False,
        "status": "not_yet_evaluable",
    })

    # M18: Full failure (6 criteria)
    signals.append({
        "name": "M18: Full failure criteria",
        "timeline": "M18",
        "conditions": {},
        "triggered": False,
        "evaluable": False,
        "status": "not_yet_evaluable",
    })

    return {"signals": signals}


def _check_adoption_metrics() -> dict:
    """
    Track adoption metrics from Business Plan Part 5.
    M6 and M12 targets.
    """
    metrics = []

    # External API lookups/day
    api_daily = 0
    try:
        row = fetch_one(
            """SELECT COUNT(*) as cnt FROM api_request_log
               WHERE created_at > NOW() - INTERVAL '24 hours'
               AND api_key_id IS NOT NULL"""
        )
        api_daily = row["cnt"] if row else 0
    except Exception as e:
        logger.warning(f"milestone_checker: adoption api_daily query failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="ops_milestone_adoption_api_daily_failure",
                error_message=str(e)[:500],
                cycle_phase="ops_milestone_checker",
            )
        except Exception:
            pass
    metrics.append({
        "name": "External API lookups/day",
        "current": api_daily,
        "m6_target": 1000,
        "m12_target": 10000,
        "source": "api_request_log",
    })

    # Repeat consumers (>30 days)
    repeat_consumers = 0
    try:
        row = fetch_one(
            """SELECT COUNT(DISTINCT api_key_id) as cnt FROM api_request_log
               WHERE api_key_id IN (
                   SELECT api_key_id FROM api_request_log
                   WHERE created_at < NOW() - INTERVAL '30 days'
                   AND api_key_id IS NOT NULL
               )
               AND created_at > NOW() - INTERVAL '7 days'
               AND api_key_id IS NOT NULL"""
        )
        repeat_consumers = row["cnt"] if row else 0
    except Exception as e:
        logger.warning(f"milestone_checker: repeat_consumers query failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="ops_milestone_adoption_repeat_consumers_failure",
                error_message=str(e)[:500],
                cycle_phase="ops_milestone_checker",
            )
        except Exception:
            pass
    metrics.append({
        "name": "Repeat consumers (>30 days)",
        "current": repeat_consumers,
        "m6_target": 10,
        "m12_target": 50,
        "source": "api_request_log",
    })

    # CDA cited by external entity
    metrics.append({
        "name": "CDA cited by external entity",
        "current": 0,
        "m6_target": 1,
        "m12_target": 5,
        "source": "manual + search monitoring",
    })

    # Third-party renderer
    metrics.append({
        "name": "Third-party renderer",
        "current": 0,
        "m6_target": 0,
        "m12_target": 2,
        "source": "manual",
    })

    # Methodology pinned in contract
    metrics.append({
        "name": "Methodology pinned in contract",
        "current": 0,
        "m6_target": 0,
        "m12_target": 1,
        "source": "on-chain verification",
    })

    # Pulse cited in governance
    pulse_citations = 0
    try:
        row = fetch_one(
            """SELECT COUNT(*) as cnt FROM ops_target_engagement_log
               WHERE action_type = 'forum_posted'
               AND channel IN ('aave_forum', 'morpho_forum', 'cow_forum', 'ens_forum')"""
        )
        pulse_citations = row["cnt"] if row else 0
    except Exception as e:
        logger.warning(f"milestone_checker: pulse_citations query failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="ops_milestone_adoption_pulse_citations_failure",
                error_message=str(e)[:500],
                cycle_phase="ops_milestone_checker",
            )
        except Exception:
            pass
    metrics.append({
        "name": "Pulse cited in governance",
        "current": pulse_citations,
        "m6_target": 0,
        "m12_target": 3,
        "source": "ops_target_engagement_log",
    })

    return {"metrics": metrics}
