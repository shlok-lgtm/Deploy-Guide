"""
Exchange API Health Checker
============================
Simple HTTP health monitor for CXRI-scored exchanges.
Checks API endpoint availability and response times.

Components produced:
  - api_availability:  Rolling 24h uptime percentage based on HTTP pings

Data source: Direct HTTP GET to public exchange API endpoints.
"""

import json
import logging
import time
from datetime import datetime, timezone

import requests

from app.database import execute, fetch_all, fetch_one
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)


# Exchange API health endpoints — all public, no auth needed
EXCHANGE_HEALTH_ENDPOINTS = {
    "binance": "https://api.binance.com/api/v3/ping",
    "coinbase": "https://api.exchange.coinbase.com/time",
    "kraken": "https://api.kraken.com/0/public/SystemStatus",
    "okx": "https://www.okx.com/api/v5/public/time",
    "bybit": "https://api.bybit.com/v5/market/time",
    "kucoin": "https://api.kucoin.com/api/v1/timestamp",
    "gate-io": "https://api.gateio.ws/api/v4/spot/time",
    "bitget": "https://api.bitget.com/api/v2/public/time",
}

HEALTH_CHECK_TIMEOUT = 5  # seconds


# =============================================================================
# Health check
# =============================================================================

def check_exchange(exchange_slug: str, endpoint: str) -> dict:
    """
    Perform a single health check on an exchange API.
    Returns {"exchange": str, "status_code": int, "response_time_ms": int,
             "is_healthy": bool, "checked_at": str}.
    """
    checked_at = datetime.now(timezone.utc).isoformat()
    try:
        start = time.monotonic()
        resp = requests.get(endpoint, timeout=HEALTH_CHECK_TIMEOUT)
        elapsed_ms = int((time.monotonic() - start) * 1000)

        return {
            "exchange": exchange_slug,
            "status_code": resp.status_code,
            "response_time_ms": elapsed_ms,
            "is_healthy": 200 <= resp.status_code < 300,
            "checked_at": checked_at,
        }
    except requests.exceptions.Timeout:
        return {
            "exchange": exchange_slug,
            "status_code": 0,
            "response_time_ms": HEALTH_CHECK_TIMEOUT * 1000,
            "is_healthy": False,
            "checked_at": checked_at,
            "error": "timeout",
        }
    except Exception as e:
        return {
            "exchange": exchange_slug,
            "status_code": 0,
            "response_time_ms": 0,
            "is_healthy": False,
            "checked_at": checked_at,
            "error": str(e),
        }


def check_all_exchanges() -> list[dict]:
    """
    Check all exchange API endpoints.
    Returns list of health check results.
    """
    results = []
    for slug, endpoint in EXCHANGE_HEALTH_ENDPOINTS.items():
        result = check_exchange(slug, endpoint)
        results.append(result)
        # Small delay between checks to avoid triggering rate limits
        time.sleep(0.2)
    return results


# =============================================================================
# Storage
# =============================================================================

def _ensure_health_table():
    """Create exchange_health_checks table if it doesn't exist."""
    try:
        execute("""
            CREATE TABLE IF NOT EXISTS exchange_health_checks (
                id SERIAL PRIMARY KEY,
                exchange_slug TEXT NOT NULL,
                status_code INTEGER,
                response_time_ms INTEGER,
                is_healthy BOOLEAN DEFAULT FALSE,
                error TEXT,
                checked_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # Index for efficient rolling queries
        execute("""
            CREATE INDEX IF NOT EXISTS idx_exchange_health_slug_time
            ON exchange_health_checks (exchange_slug, checked_at DESC)
        """)
    except Exception as e:
        logger.debug(f"Table creation skipped (may already exist): {e}")


def store_health_checks(results: list[dict]):
    """Store health check results in the database."""
    _ensure_health_table()
    for r in results:
        try:
            execute(
                """
                INSERT INTO exchange_health_checks
                    (exchange_slug, status_code, response_time_ms, is_healthy, error, checked_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    r["exchange"],
                    r.get("status_code", 0),
                    r.get("response_time_ms", 0),
                    r.get("is_healthy", False),
                    r.get("error"),
                    r.get("checked_at", datetime.now(timezone.utc).isoformat()),
                ),
            )
        except Exception as e:
            logger.warning(f"Failed to store health check for {r['exchange']}: {e}")


def compute_rolling_uptime(exchange_slug: str, hours: int = 24) -> dict:
    """
    Compute rolling uptime statistics for an exchange over the given hours.
    Returns {"uptime_pct": float, "avg_response_ms": float, "check_count": int}.
    """
    try:
        row = fetch_one(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN is_healthy THEN 1 ELSE 0 END) as healthy,
                AVG(response_time_ms) as avg_ms
            FROM exchange_health_checks
            WHERE exchange_slug = %s
              AND checked_at > NOW() - INTERVAL '%s hours'
            """,
            (exchange_slug, hours),
        )
        if not row or not row.get("total"):
            return {"uptime_pct": 0, "avg_response_ms": 0, "check_count": 0}

        total = row["total"]
        healthy = row.get("healthy", 0) or 0
        avg_ms = row.get("avg_ms", 0) or 0

        return {
            "uptime_pct": round(healthy / total * 100, 2) if total > 0 else 0,
            "avg_response_ms": round(float(avg_ms), 0),
            "check_count": total,
        }
    except Exception as e:
        logger.debug(f"compute_rolling_uptime failed for {exchange_slug}: {e}")
        return {"uptime_pct": 0, "avg_response_ms": 0, "check_count": 0}


# =============================================================================
# Normalization
# =============================================================================

def normalize_api_availability(uptime_pct: float, avg_response_ms: float) -> float:
    """
    Normalize: 100% uptime + <200ms avg = 100, 99.5% = 80, <99% = 50.
    """
    # Uptime component (0-80)
    if uptime_pct >= 100:
        uptime_score = 80.0
    elif uptime_pct >= 99.5:
        uptime_score = 60.0 + (uptime_pct - 99.5) / 0.5 * 20.0
    elif uptime_pct >= 99.0:
        uptime_score = 40.0 + (uptime_pct - 99.0) / 0.5 * 20.0
    else:
        uptime_score = max(0, uptime_pct / 99.0 * 40.0)

    # Response time component (0-20)
    if avg_response_ms <= 200:
        speed_score = 20.0
    elif avg_response_ms <= 500:
        speed_score = 10.0 + (500 - avg_response_ms) / 300 * 10.0
    elif avg_response_ms <= 1000:
        speed_score = 5.0 + (1000 - avg_response_ms) / 500 * 5.0
    else:
        speed_score = 0.0

    return round(uptime_score + speed_score, 2)


# =============================================================================
# Main runner
# =============================================================================

def run_exchange_health_monitoring() -> list[dict]:
    """
    Run exchange health checks for all CXRI exchanges.
    Called from worker fast cycle (hourly).
    Returns list of result dicts with availability scores.
    """
    # 1. Check all exchanges
    check_results = check_all_exchanges()

    # 2. Store results
    store_health_checks(check_results)

    # 3. Compute rolling uptime and normalize
    results = []
    for r in check_results:
        slug = r["exchange"]
        try:
            uptime_data = compute_rolling_uptime(slug, hours=24)
            score = normalize_api_availability(
                uptime_data["uptime_pct"],
                uptime_data["avg_response_ms"],
            )
        except Exception as e:
            logger.debug(f"Uptime computation failed for {slug}: {e}")
            uptime_data = {"uptime_pct": 0, "avg_response_ms": 0, "check_count": 0}
            score = 0.0

        # Store normalized score in generic_index_scores
        try:
            execute(
                """
                INSERT INTO generic_index_scores (index_id, entity_slug, entity_name,
                    overall_score, category_scores, component_scores, raw_values,
                    formula_version, confidence, scored_date)
                VALUES ('exchange_health', %s, %s, %s, %s, %s, %s, 'v1.0.0', 'standard', CURRENT_DATE)
                ON CONFLICT (index_id, entity_slug, scored_date)
                DO UPDATE SET
                    overall_score = EXCLUDED.overall_score,
                    component_scores = EXCLUDED.component_scores,
                    raw_values = EXCLUDED.raw_values,
                    computed_at = NOW()
                """,
                (
                    slug, slug, score,
                    json.dumps({"operational_track_record": score}),
                    json.dumps({"api_availability": score}),
                    json.dumps({
                        "uptime_pct": uptime_data["uptime_pct"],
                        "avg_response_ms": uptime_data["avg_response_ms"],
                        "check_count": uptime_data["check_count"],
                        "latest_healthy": r.get("is_healthy"),
                        "latest_ms": r.get("response_time_ms"),
                    }),
                ),
            )
        except Exception as db_err:
            logger.warning(f"Failed to store exchange health score for {slug}: {db_err}")

        results.append({
            "exchange_slug": slug,
            "is_healthy": r.get("is_healthy"),
            "response_time_ms": r.get("response_time_ms"),
            "uptime_24h": uptime_data["uptime_pct"],
            "avg_response_ms": uptime_data["avg_response_ms"],
            "availability_score": score,
        })

    # Attest
    try:
        from app.state_attestation import attest_state
        if results:
            attest_state("exchange_health", [
                {"slug": r["exchange_slug"], "score": r["availability_score"]}
                for r in results
            ])
    except Exception:
        pass

    return results
