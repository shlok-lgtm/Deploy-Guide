"""
Health checker — direct database queries against existing Basis tables.
All queries are read-only SELECT with statement_timeout.
"""
import logging
import os
import time
import json
import httpx
from datetime import datetime, timezone
from app.database import fetch_one, fetch_all, execute, get_cursor

logger = logging.getLogger(__name__)

TIMEOUT_MS = 5000  # 5-second statement timeout for all ops reads

# Use the public URL for HTTP checks (localhost doesn't work behind Replit proxy)
_PUBLIC_BASE = os.environ.get("PUBLIC_URL", "https://basisprotocol.xyz")


def _now_utc():
    return datetime.now(timezone.utc)


def _age_hours(ts):
    """Compute age in hours, handling both naive and aware datetimes."""
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (_now_utc() - ts).total_seconds() / 3600


def _safe_query(sql, params=None):
    """Execute a read-only query with statement timeout. Returns None on error."""
    try:
        with get_cursor(dict_cursor=True) as cur:
            cur.execute("SET LOCAL statement_timeout = %s", (TIMEOUT_MS,))
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows] if rows else []
    except Exception as e:
        logger.warning(f"Health check query failed: {e}")
        return None


def _safe_fetch_one(sql, params=None):
    rows = _safe_query(sql, params)
    if rows and len(rows) > 0:
        return rows[0]
    return None


def check_sii_freshness():
    """Check if SII scores are updating hourly."""
    row = _safe_fetch_one("SELECT MAX(computed_at) as last_scored FROM scores")
    if row is None:
        return {"system": "sii_scoring", "status": "down", "details": {"error": "query_failed"}}

    last_scored = row.get("last_scored")
    if not last_scored:
        return {"system": "sii_scoring", "status": "down", "details": {"error": "no_scores"}}

    age = _age_hours(last_scored)
    count = _safe_fetch_one("SELECT COUNT(DISTINCT stablecoin_id) as cnt FROM scores")
    coin_count = count["cnt"] if count else 0

    status = "healthy" if age < 2 else ("degraded" if age < 4 else "down")
    return {
        "system": "sii_scoring",
        "status": status,
        "details": {
            "last_scored": last_scored.isoformat(),
            "age_hours": round(age, 1),
            "stablecoin_count": coin_count,
        },
    }


def check_psi_freshness():
    """Check if PSI scores are current."""
    row = _safe_fetch_one("SELECT MAX(computed_at) as last_scored FROM psi_scores")
    if row is None:
        return {"system": "psi_scoring", "status": "down", "details": {"error": "query_failed_or_no_table"}}

    last_scored = row.get("last_scored")
    if not last_scored:
        return {"system": "psi_scoring", "status": "down", "details": {"error": "no_scores"}}

    age = _age_hours(last_scored)
    status = "healthy" if age < 2 else ("degraded" if age < 4 else "down")
    return {
        "system": "psi_scoring",
        "status": status,
        "details": {"last_scored": last_scored.isoformat(), "age_hours": round(age, 1)},
    }


def check_cda_freshness():
    """Check if CDA pipeline ran today."""
    row = _safe_fetch_one(
        "SELECT MAX(extracted_at) as last_extracted FROM cda_vendor_extractions"
    )
    if row is None:
        return {"system": "cda_pipeline", "status": "down", "details": {"error": "query_failed_or_no_table"}}

    last_extracted = row.get("last_extracted")
    if not last_extracted:
        return {"system": "cda_pipeline", "status": "down", "details": {"error": "no_extractions"}}

    age = _age_hours(last_extracted)
    today_count = _safe_fetch_one(
        "SELECT COUNT(DISTINCT asset_symbol) as cnt FROM cda_vendor_extractions WHERE extracted_at > NOW() - INTERVAL '24 hours'"
    )
    issuer_count = today_count["cnt"] if today_count else 0

    status = "healthy" if age < 24 else ("degraded" if age < 48 else "down")
    return {
        "system": "cda_pipeline",
        "status": status,
        "details": {
            "last_extracted": last_extracted.isoformat(),
            "age_hours": round(age, 1),
            "issuers_today": issuer_count,
        },
    }


def check_wallet_freshness():
    """Check wallet indexer freshness via wallets.last_indexed_at (updated every reindex cycle)."""
    row = _safe_fetch_one(
        "SELECT MAX(last_indexed_at) as last_indexed FROM wallet_graph.wallets"
    )
    if row is None:
        return {"system": "wallet_indexer", "status": "down", "details": {"error": "query_failed_or_no_table"}}

    last_indexed = row.get("last_indexed")
    if not last_indexed:
        return {"system": "wallet_indexer", "status": "down", "details": {"error": "no_indexed_wallets"}}

    age = _age_hours(last_indexed)
    active_count = _safe_fetch_one(
        "SELECT COUNT(*) as cnt FROM wallet_graph.wallets WHERE last_indexed_at IS NOT NULL"
    )
    active = active_count["cnt"] if active_count else 0

    # Worker cycle takes 60-120 min + sleep 60 min; healthy within 4h, degraded 4-8h, down 8h+
    status = "healthy" if age < 4 else ("degraded" if age < 8 else "down")
    return {
        "system": "wallet_indexer",
        "status": status,
        "details": {
            "last_indexed": last_indexed.isoformat(),
            "age_hours": round(age, 1),
            "active_wallets": active,
        },
    }


def check_graph_freshness():
    """Check wallet edge graph freshness."""
    row = _safe_fetch_one(
        "SELECT MAX(created_at) as last_built FROM wallet_graph.wallet_edges"
    )
    if row is None:
        return {"system": "graph_edges", "status": "down", "details": {"error": "query_failed_or_no_table"}}

    last_built = row.get("last_built")
    if not last_built:
        return {"system": "graph_edges", "status": "down", "details": {"error": "no_edges"}}

    age = _age_hours(last_built)
    chain_stats = _safe_query(
        "SELECT chain, COUNT(*) as cnt FROM wallet_graph.wallet_edges GROUP BY chain"
    )

    status = "healthy" if age < 48 else ("degraded" if age < 72 else "down")
    return {
        "system": "graph_edges",
        "status": status,
        "details": {
            "last_built": last_built.isoformat(),
            "age_hours": round(age, 1),
            "chains": {r["chain"]: r["cnt"] for r in (chain_stats or [])},
        },
    }


def check_api_health():
    """Check API responsiveness via public URL."""
    url = f"{_PUBLIC_BASE}/api/health"
    start = time.time()
    try:
        resp = httpx.get(url, timeout=20)
        latency_ms = round((time.time() - start) * 1000)
        status = "healthy" if resp.status_code == 200 and latency_ms < 3000 else "degraded"
        return {
            "system": "api",
            "status": status,
            "details": {"status_code": resp.status_code, "latency_ms": latency_ms, "url": url},
        }
    except Exception as e:
        return {"system": "api", "status": "down", "details": {"error": str(e), "url": url}}


def check_database():
    """Check database connectivity."""
    try:
        row = _safe_fetch_one("SELECT 1 as ok")
        table_count = _safe_fetch_one(
            "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_schema IN ('public', 'wallet_graph', 'ops')"
        )
        return {
            "system": "database",
            "status": "healthy" if row else "down",
            "details": {"tables": table_count["cnt"] if table_count else 0},
        }
    except Exception as e:
        return {"system": "database", "status": "down", "details": {"error": str(e)}}


def check_discovery_freshness():
    """Check discovery signal freshness."""
    row = _safe_fetch_one(
        "SELECT MAX(detected_at) as last_detected FROM discovery_signals"
    )
    if row is None:
        return {"system": "discovery", "status": "down", "details": {"error": "query_failed_or_no_table"}}

    last_detected = row.get("last_detected")
    if not last_detected:
        return {"system": "discovery", "status": "down", "details": {"error": "no_signals"}}

    age = _age_hours(last_detected)
    status = "healthy" if age < 24 else ("degraded" if age < 48 else "down")
    return {
        "system": "discovery",
        "status": status,
        "details": {"last_detected": last_detected.isoformat(), "age_hours": round(age, 1)},
    }


def check_coingecko_usage():
    """Estimate CoinGecko API usage from budget allocator."""
    row = _safe_fetch_one(
        "SELECT * FROM ops.api_budget WHERE provider = 'coingecko' ORDER BY budget_date DESC LIMIT 1"
    )
    if row is None:
        return {"system": "coingecko_api", "status": "healthy", "details": {"note": "no_budget_data"}}

    used = row.get("calls_used", 0)
    limit = row.get("daily_limit", 16666)
    pct = round(used / limit * 100, 1) if limit > 0 else 0

    status = "healthy" if pct < 80 else ("degraded" if pct < 95 else "down")
    return {
        "system": "coingecko_api",
        "status": status,
        "details": {"calls_used": used, "daily_limit": limit, "percent_used": pct},
    }


def check_integrity():
    """Check integrity status via direct DB query (not HTTP — avoids self-call latency)."""
    try:
        from app.integrity import check_all
        data = check_all()
        domains = data.get("domains", {})
        failing = [d for d, v in domains.items() if v.get("status") not in ("fresh", "healthy", "empty")]
        status = "healthy" if not failing else ("degraded" if len(failing) < 3 else "down")
        return {
            "system": "integrity",
            "status": status,
            "details": {
                "total_domains": len(domains),
                "healthy_domains": len(domains) - len(failing),
                "failing": failing,
            },
        }
    except Exception as e:
        return {"system": "integrity", "status": "down", "details": {"error": str(e)}}


def check_treasury_freshness():
    """Check treasury flow detection freshness and registry health."""
    # Check registry — table may not exist yet (migration 041 pending)
    registry = _safe_fetch_one(
        "SELECT COUNT(*) as cnt, MAX(added_at) as newest FROM wallet_graph.treasury_registry WHERE monitoring_enabled = TRUE"
    )
    if registry is None:
        # Table doesn't exist yet — not a failure, feature pending migration
        return {
            "system": "treasury_flows",
            "status": "healthy",
            "details": {"note": "treasury tables not yet created — migration 041 pending"},
        }

    registry_count = registry.get("cnt", 0)
    if registry_count == 0:
        return {
            "system": "treasury_flows",
            "status": "healthy",
            "details": {"note": "no monitoring-enabled treasuries in registry", "monitored": 0},
        }

    registry_newest = registry.get("newest")
    registry_age = _age_hours(registry_newest) if registry_newest else None

    # Check events — table may not exist yet
    events = _safe_fetch_one(
        "SELECT COUNT(*) as cnt, MAX(detected_at) as latest FROM wallet_graph.treasury_events"
    )
    if events is None:
        # Events table missing but registry exists — pending migration
        return {
            "system": "treasury_flows",
            "status": "healthy",
            "details": {"note": "treasury_events table not yet created", "monitored_treasuries": registry_count},
        }

    event_count = events.get("cnt", 0)
    last_event = events.get("latest")
    event_age = _age_hours(last_event) if last_event else None

    # Status logic:
    # healthy: events exist with age < 48h, OR no events but registry just seeded (< 48h)
    # degraded: > 48h since last event and registry > 48h old
    if event_age is not None and event_age < 48:
        status = "healthy"
    elif event_count == 0 and registry_age is not None and registry_age < 48:
        status = "healthy"  # just seeded, no events yet is fine
    elif event_count == 0:
        status = "healthy"  # no events collected yet, not a failure
    else:
        status = "degraded"

    return {
        "system": "treasury_flows",
        "status": status,
        "details": {
            "monitored_treasuries": registry_count,
            "total_events": event_count,
            "last_event": last_event.isoformat() if last_event else None,
            "event_age_hours": round(event_age, 1) if event_age else None,
            "registry_age_hours": round(registry_age, 1) if registry_age else None,
        },
    }


def check_keeper_freshness():
    """Check if keeper is publishing on-chain by querying ops.keeper_cycles."""
    row = _safe_fetch_one(
        "SELECT MAX(completed_at) as latest FROM ops.keeper_cycles WHERE completed_at IS NOT NULL"
    )
    if row is None:
        return {"system": "keeper", "status": "healthy", "details": {"note": "keeper_cycles table not yet created"}}
    latest = row.get("latest")
    if not latest:
        return {"system": "keeper", "status": "healthy", "details": {"note": "no completed cycles recorded"}}
    age = _age_hours(latest)
    status = "healthy" if age < 2 else ("degraded" if age < 4 else "down")
    return {"system": "keeper", "status": status, "details": {"last_completed": latest.isoformat(), "age_hours": round(age, 1)}}


def check_report_endpoints():
    """Smoke test report endpoints return 200."""
    url = f"{_PUBLIC_BASE}/api/reports/stablecoin/usdc?template=sbt_metadata"
    try:
        resp = httpx.get(url, timeout=10)
        status = "healthy" if resp.status_code == 200 else "down"
        return {"system": "report_endpoints", "status": status, "details": {"status_code": resp.status_code}}
    except Exception as e:
        return {"system": "report_endpoints", "status": "down", "details": {"error": str(e)}}


def check_stablecoin_coverage():
    """Verify scored stablecoins match expected count."""
    EXPECTED_COUNT = 36
    row = _safe_fetch_one("SELECT COUNT(*) as cnt FROM stablecoins WHERE scoring_enabled = TRUE")
    if row is None:
        return {"system": "stablecoin_coverage", "status": "down", "details": {"error": "query_failed"}}
    actual = row.get("cnt", 0)
    status = "healthy" if actual == EXPECTED_COUNT else "degraded"
    return {"system": "stablecoin_coverage", "status": status, "details": {"expected": EXPECTED_COUNT, "actual": actual}}


def check_db_connections():
    """Check database connection pool utilization."""
    row = _safe_fetch_one("SELECT count(*) as cnt FROM pg_stat_activity WHERE datname = current_database()")
    if row is None:
        return {"system": "db_connections", "status": "down", "details": {"error": "query_failed"}}
    count = row.get("cnt", 0)
    status = "healthy" if count < 20 else ("degraded" if count < 40 else "down")
    return {"system": "db_connections", "status": status, "details": {"active_connections": count}}


def check_collector_health():
    """Check if any collectors are consistently failing."""
    rows = _safe_query("""
        SELECT collector_name,
               SUM(coins_timeout + coins_error) as failures,
               SUM(coins_ok + coins_timeout + coins_error) as total
        FROM collector_cycle_stats
        WHERE cycle_timestamp > NOW() - INTERVAL '6 hours'
        GROUP BY collector_name
        HAVING SUM(coins_timeout + coins_error) > SUM(coins_ok) * 0.5
        ORDER BY failures DESC
    """)
    if rows and len(rows) > 0:
        worst = rows[0]
        return {
            "system": "collectors",
            "status": "degraded",
            "details": {
                "degraded_collectors": len(rows),
                "worst": worst["collector_name"],
                "failure_rate": f"{worst['failures']}/{worst['total']}",
            },
        }
    return {"system": "collectors", "status": "healthy", "details": {}}


ALL_CHECKS = [
    check_sii_freshness,
    check_psi_freshness,
    check_cda_freshness,
    check_wallet_freshness,
    check_graph_freshness,
    check_api_health,
    check_database,
    check_discovery_freshness,
    check_coingecko_usage,
    check_integrity,
    check_treasury_freshness,
    check_keeper_freshness,
    check_report_endpoints,
    check_stablecoin_coverage,
    check_db_connections,
    check_collector_health,
]


def run_all_checks():
    """Run all health checks, store results, prune old records."""
    results = []
    for check_fn in ALL_CHECKS:
        try:
            result = check_fn()
            results.append(result)
            execute(
                "INSERT INTO ops_health_checks (system, status, details) VALUES (%s, %s, %s)",
                (result["system"], result["status"], json.dumps(result["details"])),
            )
        except Exception as e:
            logger.error(f"Health check {check_fn.__name__} failed: {e}")
            results.append({"system": check_fn.__name__, "status": "down", "details": {"error": str(e)}})

    # Prune records older than 7 days
    try:
        execute("DELETE FROM ops_health_checks WHERE checked_at < NOW() - INTERVAL '7 days'")
    except Exception:
        pass

    return results


def get_latest_health():
    """Get the most recent health check per system."""
    rows = _safe_query("""
        SELECT DISTINCT ON (system) system, status, details, checked_at
        FROM ops_health_checks
        ORDER BY system, checked_at DESC
    """)
    return rows or []
