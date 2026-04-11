"""
Daily Metrics Rollup
=====================
Computes and stores daily aggregate metrics from api_request_log.
Called once per day from the daily cycle.
"""

import json
import logging
from datetime import date, timedelta
from app.database import fetch_one, fetch_all, get_conn

logger = logging.getLogger(__name__)


def compute_and_store_daily_rollup(target_date: date = None):
    """Compute metrics for target_date (default: yesterday) and upsert into metrics_daily_rollup."""
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    d = target_date.isoformat()
    logger.info(f"Computing daily rollup for {d}")

    try:
        # Total requests
        total = fetch_one(
            "SELECT COUNT(*) as c FROM api_request_log WHERE timestamp::date = %s", (d,)
        )
        total_requests = total["c"] if total else 0

        # Internal vs external
        internal = fetch_one(
            "SELECT COUNT(*) as c FROM api_request_log WHERE timestamp::date = %s AND is_internal = TRUE", (d,)
        )
        internal_requests = internal["c"] if internal else 0
        external_requests = total_requests - internal_requests

        # Unique IPs
        unique_ips_row = fetch_one(
            "SELECT COUNT(DISTINCT ip_address) as c FROM api_request_log WHERE timestamp::date = %s", (d,)
        )
        unique_ips = unique_ips_row["c"] if unique_ips_row else 0

        unique_ext_ips_row = fetch_one(
            "SELECT COUNT(DISTINCT ip_address) as c FROM api_request_log WHERE timestamp::date = %s AND is_internal = FALSE", (d,)
        )
        unique_external_ips = unique_ext_ips_row["c"] if unique_ext_ips_row else 0

        # Unique API keys
        unique_keys_row = fetch_one(
            "SELECT COUNT(DISTINCT api_key_id) as c FROM api_request_log WHERE timestamp::date = %s AND api_key_id IS NOT NULL", (d,)
        )
        unique_api_keys = unique_keys_row["c"] if unique_keys_row else 0

        # MCP requests (from api_request_log)
        mcp_row = fetch_one(
            "SELECT COUNT(*) as c FROM api_request_log WHERE timestamp::date = %s AND endpoint LIKE '/mcp%%'", (d,)
        )
        mcp_requests = mcp_row["c"] if mcp_row else 0

        # MCP tool calls (from mcp_tool_calls table)
        mcp_tools_row = fetch_one(
            "SELECT COUNT(*) as c FROM mcp_tool_calls WHERE timestamp::date = %s", (d,)
        )
        mcp_tool_calls = mcp_tools_row["c"] if mcp_tools_row else 0

        # Avg response time
        avg_rt = fetch_one(
            "SELECT AVG(response_time_ms) as avg_ms FROM api_request_log WHERE timestamp::date = %s AND response_time_ms IS NOT NULL", (d,)
        )
        avg_response_time = float(avg_rt["avg_ms"]) if avg_rt and avg_rt["avg_ms"] else 0

        # Errors
        errors = fetch_one(
            "SELECT COUNT(*) as c FROM api_request_log WHERE timestamp::date = %s AND status_code >= 400", (d,)
        )
        error_count = errors["c"] if errors else 0

        # Top endpoints
        top_ep = fetch_all(
            """SELECT endpoint, COUNT(*) as cnt FROM api_request_log
               WHERE timestamp::date = %s AND is_internal = FALSE
               GROUP BY endpoint ORDER BY cnt DESC LIMIT 10""", (d,)
        ) or []
        top_endpoints = json.dumps([{"endpoint": r["endpoint"], "count": r["cnt"]} for r in top_ep])

        # Top user agents (external only)
        top_ua = fetch_all(
            """SELECT LEFT(user_agent, 100) as ua, COUNT(*) as cnt FROM api_request_log
               WHERE timestamp::date = %s AND is_internal = FALSE
               GROUP BY LEFT(user_agent, 100) ORDER BY cnt DESC LIMIT 10""", (d,)
        ) or []
        top_user_agents = json.dumps([{"user_agent": r["ua"], "count": r["cnt"]} for r in top_ua])

        # JSON-LD requests
        jsonld = fetch_one(
            "SELECT COUNT(*) as c FROM api_request_log WHERE timestamp::date = %s AND accept_header LIKE '%%ld+json%%'", (d,)
        )
        jsonld_requests = jsonld["c"] if jsonld else 0

        # Report requests
        report_row = fetch_one(
            "SELECT COUNT(*) as c FROM api_request_log WHERE timestamp::date = %s "
            "AND (endpoint LIKE '/api/reports/%%' OR endpoint LIKE '/api/paid/report/%%')", (d,)
        )
        report_requests = report_row["c"] if report_row else 0

        # x402 payments
        try:
            x402_row = fetch_one(
                "SELECT COUNT(*) as c, COALESCE(SUM(price_usd), 0) as rev FROM payment_log WHERE timestamp::date = %s", (d,)
            )
            x402_payments = x402_row["c"] if x402_row else 0
            x402_revenue = float(x402_row["rev"]) if x402_row else 0.0
        except Exception:
            x402_payments = 0
            x402_revenue = 0.0

        # State attestations created
        try:
            sa_row = fetch_one(
                "SELECT COUNT(*) as c FROM state_attestations WHERE cycle_timestamp::date = %s", (d,)
            )
            state_attestations = sa_row["c"] if sa_row else 0
        except Exception:
            state_attestations = 0

        # Report attestations created
        try:
            ra_row = fetch_one(
                "SELECT COUNT(*) as c FROM report_attestations WHERE generated_at::date = %s", (d,)
            )
            report_attestations = ra_row["c"] if ra_row else 0
        except Exception:
            report_attestations = 0

        # Query language requests
        query_row = fetch_one(
            "SELECT COUNT(*) as c FROM api_request_log WHERE timestamp::date = %s AND endpoint = '/api/query'", (d,)
        )
        query_requests = query_row["c"] if query_row else 0

        # CQI composition requests
        cqi_row = fetch_one(
            "SELECT COUNT(*) as c FROM api_request_log WHERE timestamp::date = %s AND endpoint LIKE '/api/compose/cqi%%'", (d,)
        )
        cqi_requests = cqi_row["c"] if cqi_row else 0

        # Upsert
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO metrics_daily_rollup
                        (date, total_api_requests, external_api_requests, internal_api_requests,
                         unique_ips, unique_external_ips, unique_api_keys,
                         mcp_requests, mcp_tool_calls, avg_response_time_ms, error_count,
                         top_endpoints, top_user_agents, jsonld_requests,
                         report_requests, x402_payments, x402_revenue_usd,
                         state_attestations, report_attestations, query_requests, cqi_requests)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date) DO UPDATE SET
                        total_api_requests = EXCLUDED.total_api_requests,
                        external_api_requests = EXCLUDED.external_api_requests,
                        internal_api_requests = EXCLUDED.internal_api_requests,
                        unique_ips = EXCLUDED.unique_ips,
                        unique_external_ips = EXCLUDED.unique_external_ips,
                        unique_api_keys = EXCLUDED.unique_api_keys,
                        mcp_requests = EXCLUDED.mcp_requests,
                        mcp_tool_calls = EXCLUDED.mcp_tool_calls,
                        avg_response_time_ms = EXCLUDED.avg_response_time_ms,
                        error_count = EXCLUDED.error_count,
                        top_endpoints = EXCLUDED.top_endpoints,
                        top_user_agents = EXCLUDED.top_user_agents,
                        jsonld_requests = EXCLUDED.jsonld_requests,
                        report_requests = EXCLUDED.report_requests,
                        x402_payments = EXCLUDED.x402_payments,
                        x402_revenue_usd = EXCLUDED.x402_revenue_usd,
                        state_attestations = EXCLUDED.state_attestations,
                        report_attestations = EXCLUDED.report_attestations,
                        query_requests = EXCLUDED.query_requests,
                        cqi_requests = EXCLUDED.cqi_requests
                """, (d, total_requests, external_requests, internal_requests,
                      unique_ips, unique_external_ips, unique_api_keys,
                      mcp_requests, mcp_tool_calls, avg_response_time, error_count,
                      top_endpoints, top_user_agents, jsonld_requests,
                      report_requests, x402_payments, x402_revenue,
                      state_attestations, report_attestations, query_requests, cqi_requests))
            conn.commit()

        logger.info(f"Daily rollup stored for {d}: {total_requests} total, {external_requests} external")
        return {"date": d, "total": total_requests, "external": external_requests}

    except Exception as e:
        logger.error(f"Daily rollup failed for {d}: {e}")
        return {"date": d, "error": str(e)}
